-module(duplex).

-behaviour(gen_statem).

-export([start_link/3,
         start_link/4,
         send_recv/2,
         send_recv/3]).

-export([init/1,
         backoff/3,
         passive/3,
         active/3,
         half_duplex/3,
         semi_duplex/3,
         full_duplex/3,
         closing/3,
         code_change/4,
         terminate/3]).

-type debug_opt() :: trace | log | statistics | debug | {logfile, string()}.
-type start_opt() :: {debug, [debug_opt()]} | {timeout, timeout()} |
                     {spawn_opt, [proc_lib:spawn_option()]}.

-callback init(Arg) ->
    {Mode, Info, Broker, Backoff} | {stop, Reason} | ignore when
      Arg :: term(),
      Mode :: half_duplex | full_duplex,
      Info :: term(),
      Broker :: sbroker:broker(),
      Backoff :: backoff:backoff(),
      Reason :: term().

-callback handle_open(Info) ->
    {ok, Buffer, Socket} | {error, Reason} when
      Info :: term(),
      Buffer :: term(),
      Socket :: term(),
      Reason :: term().

-callback handle_activate(Buffer, Socket) ->
    {active | passive, NBuffer} | {close, Reason} when
      Buffer :: term(),
      Socket :: term(),
      NBuffer :: term(),
      Reason :: term().

-callback handle_info(Msg, Buffer, Socket) ->
    {active | passive, NBuffer} | {close, Reason} when
      Msg :: term(),
      Buffer :: term(),
      Socket :: term(),
      NBuffer :: term(),
      Reason :: term().

-callback handle_deactivate(Buffer, Socket) ->
    {passive, NBuffer} | {close, Reason} when
      Buffer :: term(),
      Socket :: term(),
      NBuffer :: term(),
      Reason :: term().

-callback handle_send(Req, Socket) ->
    {send_recv | recv, NReq} | {result, Result} | {close, Reason, Result} when
      Req :: term(),
      Socket :: term(),
      NReq :: term(),
      Result :: term(),
      Reason :: term().

-callback handle_send_recv(Req, Buffer, Socket) ->
    {recv, NReq, NBuffer} |
    {result, Result, NBuffer} |
    {close, Reason, Result} when
      Req :: term(),
      Buffer :: term(),
      Socket :: term(),
      NReq :: term(),
      NBuffer :: term(),
      Result :: term(),
      Reason :: term().

-callback handle_recv(Req, Buffer, Socket) ->
    {result, Result, NBuffer} |
    {close, Reason, Result} when
      Req :: term(),
      Buffer :: term(),
      Socket :: term(),
      Result :: term(),
      NBuffer :: term(),
      Reason :: term().

-callback handle_closed(Req, Socket) -> {result, Result} when
      Req :: term(),
      Socket :: term(),
      Result :: term().

-callback handle_close(Reason, Socket) -> term() when
      Reason :: term(),
      Socket :: term().

-optional_callbacks([handle_info/3,
                     handle_deactivate/2,
                     handle_send_recv/3,
                     handle_recv/3]).

-record(client, {ref :: reference(),
                 pid :: pid(),
                 mon :: reference() | 'DOWN',
                 timer :: reference() | infinity}).

-record(data, {name :: {pid(), module()} | {local, atom()} | {global, term()} |
                {via, module(),term()},
               mod :: module(),
               args :: term(),
               info :: term(),
               socket=undefined :: term(),
               buffer=undefined :: term(),
               mode :: half_duplex | full_duplex,
               broker :: pid() | {atom(), node()},
               broker_mon :: reference(),
               broker_ref=make_ref() :: reference(),
               backoff :: backoff:backoff(),
               backoff_timer=undefined :: reference() | undefined,
               send=undefined :: undefined | #client{} | unknown,
               recv=undefined :: undefined | #client{}}).

-spec start_link(Mod, Args, Opts) -> {ok, Pid} | ignore | {error, Reason} when
      Mod :: module(),
      Args :: term(),
      Opts :: [start_opt()],
      Pid :: pid(),
      Reason :: term().
start_link(Mod, Args, Opts) ->
    gen_statem:start_link(?MODULE, {self, Mod, Args}, Opts).

-spec start_link(Name, Mod, Args, Opts) ->
    {ok, Pid} | ignore | {error, Reason} when
      Name :: {local, atom()} | {global, term()} | {via, module(), term()},
      Mod :: module(),
      Args :: term(),
      Opts :: [start_opt()],
      Pid :: pid(),
      Reason :: term().
start_link(Name, Mod, Args, Opts) ->
    gen_statem:start_link(Name, ?MODULE, {Name, Mod, Args}, Opts).

send_recv(Broker, Req) ->
    send_recv(Broker, Req, infinity).

send_recv(Broker, Req, Timeout) ->
    case sbroker:ask(Broker, {self(), Timeout}) of
        {go, Ref, {Mod, Socket, Conn}, _, _} ->
            send(Mod, Req, Socket, Conn, Ref);
        {drop, _} = Drop ->
            {error, Drop}
    end.

init({self, Mod, Args}) ->
    init({{self(), Mod}, Mod, Args});
init({Name, Mod, Args}) ->
    try Mod:init(Args) of
        Result ->
            handle_init(Result, Name, Mod, Args)
    catch
        throw:Result ->
            handle_init(Result, Name, Mod, Args)
    end.

backoff(internal, open, #data{mod=Mod, info=Info} = Data) ->
    try Mod:handle_open(Info) of
        Result ->
            handle_open(Result, Data)
    catch
        throw:Result ->
            handle_open(Result, Data)
    end;
backoff(info, {timeout, TRef, _}, #data{backoff_timer=TRef} = Data)
  when is_reference(TRef) ->
    {keep_state, Data#data{backoff_timer=undefined}, open_next()};
backoff(Type, Event, Data) ->
    handle_event(Type, Event, backoff, Data).

passive(info, {BRef, {go, Ref, {Pid, Timeout}, _, SojournTime}},
        #data{broker_ref=BRef} = Data) ->
    handle_go(Ref, Pid, Timeout, SojournTime, Data);
passive(internal, activate,
        #data{mod=Mod, buffer=Buffer, socket=Socket} = Data) ->
    try Mod:handle_activate(Buffer, Socket) of
        Result ->
            handle_active(Result, Data)
    catch
        throw:Result ->
            handle_active(Result, Data)
    end;
passive(Type, Event, Data) ->
    handle_event(Type, Event, passive, Data).

active(info, {BRef, {go, Ref, {Pid, Timeout}, _, SojournTime}},
       #data{broker_ref=BRef, mod=Mod, buffer=Buffer, socket=Socket} = Data) ->
    Client = start_client(Ref, Pid, Timeout, SojournTime),
    try Mod:handle_deactivate(Buffer, Socket) of
        Result ->
            handle_deactivate(Result, Client, Data)
    catch
        throw:Result ->
            handle_deactivate(Result, Client, Data)
    end;
active(Type, Event, Data) ->
    handle_event(Type, Event, active, Data).

half_duplex(cast, {done, Ref, Buffer},
        #data{send=#client{ref=Ref} = Client} = Data) ->
    NData = Data#data{send=undefined, recv=undefined, buffer=Buffer},
    Result = ask_r(NData),
    cancel_client(Client),
    Result;
half_duplex(cast, {close, Ref, Reason},
            #data{send=#client{ref=Ref} = Client} = Data) ->
    cancel_client(Client),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next(Reason)};
half_duplex(info, {timeout, TRef, {Pid, Timeout}},
            #data{send=#client{timer=TRef} = Client} = Data)
  when is_reference(TRef) ->
    cancel_client(Client#client{timer=infinity}),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({timeout, Pid, Timeout})};
half_duplex(info, {'DOWN', MRef, _, Pid, Reason},
        #data{send=#client{mon=MRef} = Client} = Data) ->
    cancel_client(Client#client{mon='DOWN'}),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({'DOWN', Pid, Reason})};
half_duplex(Type, Event, Data) ->
    handle_event(Type, Event, half_duplex, Data).

semi_duplex(info, {BRef, {go, Ref, {Pid, Timeout}, _, SojournTime}},
            #data{broker_ref=BRef} = Data) ->
    Send = start_client(Ref, Pid, Timeout, SojournTime),
    {next_state, full_duplex, Data#data{send=Send, broker_ref=Ref}};
semi_duplex(cast, {done, Ref, Buffer},
            #data{send=#client{ref=Ref} = Client, broker_ref=BRef} = Data) ->
    cancel_client(Client),
    NData = Data#data{send=undefined, recv=undefined, buffer=Buffer},
    receive
        {BRef, {go, NRef, {Pid, Timeout}, _, SojournTime}} ->
            handle_go(NRef, Pid, Timeout, SojournTime, NData)
    after
        1 ->
            {next_state, passive, NData, activate_next()}
    end;
semi_duplex(cast, {close, Ref, Reason},
            #data{send=#client{ref=Ref} = Client} = Data) ->
    cancel_client(Client),
    NData = Data#data{send=unknown, recv=undefined},
    {next_state, closing, NData, close_next(Reason)};
semi_duplex(info, {timeout, TRef, {Pid, Timeout}},
            #data{send=#client{timer=TRef} = Client} = Data)
  when is_reference(TRef) ->
    cancel_client(Client#client{timer=infinity}),
    NData = Data#data{send=unknown, recv=undefined},
    {next_state, closing, NData, close_next({timeout, Pid, Timeout})};
semi_duplex(info, {'DOWN', MRef, _, Pid, Reason},
        #data{send=#client{mon=MRef} = Client} = Data) ->
    cancel_client(Client#client{mon='DOWN'}),
    NData = Data#data{send=unknown, recv=undefined},
    {next_state, closing, NData, close_next({'DOWN', Pid, Reason})};
semi_duplex(Type, Event, Data) ->
    handle_event(Type, Event, semi_duplex, Data).

full_duplex(cast, {done, Ref, Buffer},
            #data{recv=#client{ref=Ref} = Recv, send=Send,
                  broker=Broker} = Data) ->
    continue(Send, full_duplex, Broker, Buffer),
    cancel_client(Recv),
    {next_state, semi_duplex, Data#data{recv=Send}};
full_duplex(cast, {close, Ref, Reason},
            #data{recv=#client{ref=Ref} = Recv, send=Send} = Data) ->
    closed_send(Send),
    cancel_client(Recv),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next(Reason)};
full_duplex(info, {timeout, TRef, {Pid, Timeout}},
            #data{send=#client{timer=TRef} = Send, recv=Recv} = Data)
  when is_reference(TRef) ->
    closed_send(Send#client{timer=infinity}),
    cancel_client(Recv),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({timeout, Pid, Timeout})};
full_duplex(info, {timeout, TRef, {Pid, Timeout}},
            #data{recv=#client{timer=TRef}=Recv, send=Send} = Data)
  when is_reference(TRef) ->
    closed_send(Send),
    cancel_client(Send),
    cancel_client(Recv#client{timer=infinity}),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({timeout, Pid, Timeout})};
full_duplex(info, {'DOWN', MRef, _, Pid, Reason},
            #data{send=#client{mon=MRef}=Send, recv=Recv} = Data) ->
    cancel_client(Send#client{mon='DOWN'}),
    cancel_client(Recv),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({'DOWN', Pid, Reason})};
full_duplex(info, {'DOWN', MRef, _, Pid, Reason},
            #data{recv=#client{mon=MRef}=Recv, send=Send} = Data) ->
    closed_send(Send),
    cancel_client(Recv#client{mon='DOWN'}),
    NData = Data#data{send=undefined, recv=undefined},
    {next_state, closing, NData, close_next({'DOWN', Pid, Reason})};
full_duplex(Type, Event, Data) ->
    handle_event(Type, Event, full_duplex, Data).

closing(internal, {close, Reason},
        #data{recv=undefined, mod=Mod, socket=Socket} = Data) ->
    NData = cancel_send(Data#data{socket=undefined}),
    _ = try
            Mod:handle_close(Reason, Socket)
        catch
            throw:_ ->
                ok
        end,
    report(Reason, socket_closed, Data),
    {next_state, backoff, NData, open_next()}.

code_change(_, State, Data, _) ->
    {state_functions, State, Data}.

terminate(_, closing, _) ->
    ok;
terminate(_, backoff, _) ->
    ok;
terminate(Reason, _, #data{mod=Mod, socket=Socket}) ->
    Mod:handle_close(Reason, Socket).

%% Internal

send(Mod, Req, Socket, Conn, Ref) ->
    try Mod:handle_send(Req, Socket) of
        Result ->
            handle_send(Result, Mod, Socket, Conn, Ref)
    catch
        throw:Result ->
            handle_send(Result, Mod, Socket, Conn, Ref);
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            send_exception(Class, Reason, Stack, Conn, Ref)
    end.

handle_send({Next, NReq}, Mod, Socket, Conn, Ref)
  when Next == send_recv; Next == recv ->
    MRef = monitor(process, Conn),
    receive
        {?MODULE, Ref, {Mode, Buffer, Broker}} ->
            demonitor(MRef, [flush]),
            handle(Next, Mod, NReq, Buffer, Socket, Mode, Conn, Ref, Broker);
        {?MODULE, Ref, closed} ->
            demonitor(MRef, [flush]),
            closed(NReq, Socket, Mod);
        {'DOWN', MRef, _, _, _} ->
            closed(NReq, Socket, Mod)
    end;
handle_send({result, Res}, Mod, Socket, Conn, Ref) ->
    MRef = monitor(process, Conn),
    receive
        {?MODULE, Ref, {Mode, Buffer, Broker}} ->
            demonitor(MRef, [flush]),
            handle(result, Mod, Res, Buffer, Socket, Mode, Conn, Ref, Broker);
        {?MODULE, Ref, closed} ->
            demonitor(MRef, [flush]),
            Res;
        {'DOWN', MRef, _, _, _} ->
            Res
    end;
handle_send({close, Reason, Result}, _, _, Conn, Ref) ->
    MRef = monitor(process, Conn),
    receive
        {?MODULE, Ref, {_, _, _}} ->
            demonitor(MRef, [flush]),
            close(Conn, Ref, Reason),
            Result;
        {?MODULE, Ref, closed} ->
            demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, _, _, _} ->
            Result
    end;
handle_send(Other, _, _, Conn, Ref) ->
    Reason = {bad_return_value, Other},
    try
        exit(Reason)
    catch
        exit:Reason ->
            Stack = erlang:get_stacktrace(),
            send_exception(exit, Reason, Stack, Conn, Ref)
    end.

closed(Req, Socket, Mod) ->
    try Mod:handle_closed(Req, Socket) of
        Result ->
            handle_closed(Result)
    catch
        throw:Result ->
            handle_closed(Result)
    end.

handle_closed({result, Result}) ->
    Result;
handle_closed(Other) ->
    exit({bad_return_value, Other}).

send_exception(Class, Reason, Stack, Conn, Ref) ->
    MRef = monitor(process, Conn),
    exception(Conn, Ref, Class, Reason, Stack),
    receive
        {?MODULE, Ref, {_, _, _}} ->
            demonitor(MRef, [flush]),
            erlang:raise(Class, Reason, Stack);
        {?MODULE, Ref, closed} ->
            demonitor(MRef, [flush]),
            erlang:raise(Class, Reason, Stack);
        {'DOWN', MRef, _, _, _} ->
            erlang:raise(Class, Reason, Stack)
    end.

handle(send_recv, Mod, Req, Buffer, Socket, Mode, Conn, Ref, Broker) ->
    try Mod:handle_send_recv(Req, Buffer, Socket) of
        Result ->
            handle_send_recv(Result, Mod, Socket, Mode, Conn, Ref, Broker)
    catch
        throw:Result ->
            handle_send_recv(Result, Mod, Socket, Mode, Conn, Ref, Broker);
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            exception(Conn, Ref, Class, Reason, Stack),
            erlang:raise(Class, Reason, Stack)
    end;
handle(Other, Mod, Req, Buffer, Socket, full_duplex, Conn, Ref, Broker) ->
    Info = {Mod, Socket, Conn},
    To = {Conn, Ref},
    {await, Ref, Broker} = sbroker:async_ask_r(Broker, Info, To),
    handle(Other, Mod, Req, Buffer, Socket, Conn, Ref);
handle(Other, Mod, Req, Buffer, Socket, half_duplex, Conn, Ref, _) ->
    handle(Other, Mod, Req, Buffer, Socket, Conn, Ref).

handle(recv, Mod, Req, Buffer, Socket, Conn, Ref) ->
    try Mod:handle_recv(Req, Buffer, Socket) of
        Result ->
            handle_recv(Result, Mod, Socket, Conn, Ref)
    catch
        throw:Result ->
            handle_recv(Result, Mod, Socket, Conn, Ref);
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            exception(Conn, Ref, Class, Reason, Stack),
            erlang:raise(Class, Reason, Stack)
    end;
handle(result, _, Result, Buffer, _, Conn, Ref) ->
    done(Conn, Ref, Buffer),
    Result;
handle(close, _, Result, Reason, _, Conn, Ref) ->
    close(Conn, Ref, Reason),
    Result.

handle_send_recv({Next, Req, Buffer}, Mod, Socket, Mode, Conn, Ref, Broker)
  when Next == recv; Next == result ->
    handle(Next, Mod, Req, Buffer, Socket, Mode, Conn, Ref, Broker);
handle_send_recv({close, Reason, Result}, Mod, Socket, _, Conn, Ref, _) ->
    handle(close, Mod, Result, Reason, Socket, Conn, Ref);
handle_send_recv(Other, _, _, _, Conn, Ref, _) ->
    Reason = {bad_return_value, Other},
    try
        exit(Reason)
    catch
        exit:Reason ->
            Stack = erlang:get_stacktrace(),
            exception(Conn, Ref, exit, Reason, Stack),
            erlang:raise(exit, Reason, Stack)
    end.

handle_recv({result, Req, Buffer}, Mod, Socket, Conn, Ref) ->
    handle(result, Mod, Req, Buffer, Socket, Conn, Ref);
handle_recv({close, Reason, Result}, Mod, Socket, Conn, Ref) ->
    handle(close, Mod, Result, Reason, Socket, Conn, Ref);
handle_recv(Other, _, _, Conn, Ref) ->
    Reason = {bad_return_value, Other},
    try
        exit(Reason)
    catch
        exit:Reason ->
            Stack = erlang:get_stacktrace(),
            exception(Conn, Ref, exit, Reason, Stack),
            erlang:raise(exit, Reason, Stack)
    end.

done(Conn, Ref, Buffer) ->
    gen_statem:cast(Conn, {done, Ref, Buffer}).

close(Conn, Ref, Reason) ->
    gen_statem:cast(Conn, {close, Ref, Reason}).

exception(Conn, Ref, Class, Reason, Stack) ->
    gen_statem:cast(Conn, {exception, Ref, Class, Reason, Stack}).

handle_init({Mode, Info, Broker, Backoff}, Name, Mod, Args)
  when Mode == half_duplex; Mode == full_duplex ->
    NBroker = lookup(Broker),
    MRef = monitor(process, NBroker),
    Data = #data{name=Name, mod=Mod, args=Args, mode=Mode, broker=NBroker,
                 broker_mon=MRef, backoff=Backoff, info=Info},
    {state_functions, backoff, Data, open_next()};
handle_init({stop, _} = Stop, _, _, _) ->
    Stop;
handle_init(ignore, _, _, _) ->
    ignore;
handle_init(Other, _, _, _) ->
    {stop, {bad_return_value, Other}}.

lookup(Name) when is_atom(Name) ->
    whereis(Name);
lookup(Pid) when is_pid(Pid) ->
    Pid;
lookup({Name, Node}) when is_atom(Name), Node == node() ->
    whereis(Name);
lookup({Name, Node} = Process) when is_atom(Name), is_atom(Node) ->
    Process;
lookup({global, Name}) ->
    global:whereis_name(Name);
lookup({via, Mod, Name}) ->
    Mod:whereis_name(Name).

open_next() ->
    {next_event, internal, open}.

close_next(Reason) ->
    {next_event, internal, {close, Reason}}.

handle_open({ok, Buffer, Socket}, #data{backoff=Backoff} = Data) ->
    {_, NBackoff} = backoff:succeed(Backoff),
    ask_r(Data#data{socket=Socket, buffer=Buffer, backoff=NBackoff});
handle_open({error, Reason}, #data{backoff=Backoff} = Data) ->
    report(Reason, failed_to_open_socket, Data),
    TRef = backoff:fire(Backoff),
    {_, NBackoff} = backoff:fail(Backoff),
    {keep_state, Data#data{backoff=NBackoff, backoff_timer=TRef}};
handle_open(Other, _) ->
    {stop, {bad_return_value, Other}}.

ask_r(#data{send=undefined, recv=undefined, mod=Mod, socket=Socket,
            broker=Broker} = Data) ->
    Conn = self(),
    Info = {Mod, Socket, Conn},
    case sbroker:dynamic_ask_r(Broker, Info) of
        {go, Ref, {Pid, Timeout}, _, SojournTime} ->
            handle_go(Ref, Pid, Timeout, SojournTime, Data);
        {await, BRef, Broker} ->
            demonitor(BRef, [flush]),
            {next_state, passive, Data#data{broker_ref=BRef}, activate_next()}
    end.

activate_next() ->
    {next_event, internal, activate}.

handle_active({NState, NBuffer}, Data)
  when NState == active; NState == passive ->
    {next_state, NState, Data#data{buffer=NBuffer}};
handle_active({close, Reason}, Data) ->
    {next_state, closing, Data#data{send=unknown}, close_next(Reason)};
handle_active(Other, _) ->
    exit({bad_return_value, Other}).

handle_deactivate({passive, NBuffer}, #client{ref=Ref} = Client,
              #data{mode=Mode, broker=Broker} = Data) ->
    continue(Client, Mode, Broker, NBuffer),
    NData = Data#data{send=Client, recv=Client, broker_ref=Ref,
                      buffer=undefined},
    {next_state, go_next_state(Mode), NData};
handle_deactivate({close, Reason}, Client, Data) ->
    closed_send(Client),
    {next_state, closing, Data, close_next(Reason)};
handle_deactivate(Other, _, _) ->
    exit({bad_return_value, Other}).

handle_go(Ref, Pid, Timeout, SojournTime,
          #data{mode=Mode, broker=Broker, buffer=Buffer} = Data) ->
    continue(Pid, Ref, Mode, Broker, Buffer),
    Client = start_client(Ref, Pid, Timeout, SojournTime),
    NData = Data#data{send=Client, recv=Client, broker_ref=Ref,
                      buffer=undefined},
    {next_state, go_next_state(Mode), NData}.

go_next_state(half_duplex) ->
    half_duplex;
go_next_state(full_duplex) ->
    semi_duplex.

handle_event(info, {Ref, {go, _,  {Pid, _}, _, _}}, State, Data)
  when is_reference(Ref) ->
    closed_send(Ref, Pid),
    {next_state, State, Data};
handle_event(info, {Ref, {drop, _}}, State, Data) when is_reference(Ref) ->
    {next_state, State, Data};
handle_event(cast, {exception, Ref, Class, Reason, Stack}, _,
             #data{send=#client{ref=Ref}}) ->
    {stop, {Class, Reason, Stack}};
handle_event(cast, {exception, Ref, Class, Reason, Stack}, _,
             #data{recv=#client{ref=Ref}}) ->
    {stop, {Class, Reason, Stack}};
handle_event(info, {'DOWN', MRef, _, Pid, Reason}, _, #data{broker_mon=MRef}) ->
    {stop, {shutdown, {'DOWN', Pid, Reason}}};
handle_event(info, Info, active,
             #data{mod=Mod, buffer=Buffer, socket=Socket} = Data) ->
    try Mod:handle_info(Info, Buffer, Socket) of
        Result ->
            handle_active(Result, Data)
    catch
        throw:Result ->
            handle_active(Result, Data)
    end;
handle_event(info, Info, State, #data{name=Name} = Data) ->
    error_logger:error_msg("Duplex connection ~p discarding message: ~p",
                           [Name, Info]),
    {next_state, State, Data};
handle_event(cast, {done, Ref, _}, _, _) when is_reference(Ref) ->
    keep_state_and_data;
handle_event(cast, {close, Ref, _}, _, _) when is_reference(Ref) ->
    keep_state_and_data;
handle_event(cast, {exception, Ref, _, _, _}, _, _) when is_reference(Ref) ->
    keep_state_and_data.

start_client(Ref, Pid, Timeout, SojournTime) ->
    MRef = monitor(process, Pid),
    TRef = start_timer(Pid, Timeout, SojournTime),
    #client{ref=Ref, pid=Pid, mon=MRef, timer=TRef}.

cancel_client(#client{mon='DOWN', timer=TRef}) ->
    cancel_timer(TRef);
cancel_client(#client{mon=MRef, timer=TRef}) ->
    demonitor(MRef, [flush]),
    cancel_timer(TRef).

start_timer(_, infinity, _) ->
    infinity;
start_timer(Pid, Timeout, SojournTime) ->
    SoFar = erlang:convert_time_unit(SojournTime, native, milli_seconds),
    RemTimeout = max(0, Timeout-SoFar),
    erlang:start_timer(RemTimeout, self(), {Pid, Timeout}).

cancel_timer(infinity) ->
    ok;
cancel_timer(TRef) ->
    case erlang:cancel_timer(TRef) of
        false ->
            flush_timer(TRef);
        _ ->
            ok
    end.

flush_timer(TRef) ->
    receive
        {timeout, TRef, _} ->
            ok
    after
        0 ->
            error(badtimer, [TRef])
    end.

cancel_send(#data{send=unknown, broker=Broker, broker_ref=BRef} = Data) ->
    _ = sbroker:cancel(Broker, BRef, infinity),
    Data#data{send=undefined, broker_ref=make_ref()};
cancel_send(#data{send=undefined} = Data) ->
    Data#data{broker_ref=make_ref()}.

closed_send(#client{ref=Ref, pid=Pid} = Send) ->
    closed_send(Ref, Pid),
    cancel_client(Send).

closed_send(Ref, Pid) ->
    _ = Pid ! {?MODULE, Ref, closed},
    ok.

continue(#client{ref=Ref, pid=Pid}, Mode, Broker, Buffer) ->
    continue(Pid, Ref, Mode, Broker, Buffer).

continue(Pid, Ref, Mode, Broker, Buffer) ->
    _ = Pid ! {?MODULE, Ref, {Mode, Buffer, Broker}},
    ok.

report(Reason, Context, #data{name=Name, socket=Socket, mode=Mode}) ->
    Report = [{duplex_connection, Name},
              {errorContext, Context},
              {reason, Reason},
              {socket, Socket},
              {socket_mode, Mode}],
    error_logger:error_report(duplex_connect_report, Report).
