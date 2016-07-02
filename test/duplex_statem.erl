%%-------------------------------------------------------------------
%%
%% Copyright (c) 2016, James Fish <james@fishcakez.com>
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%-------------------------------------------------------------------
-module(duplex_statem).

-include_lib("proper/include/proper.hrl").
-define(TIMEOUT, 1000).

-export([quickcheck/0]).
-export([quickcheck/1]).
-export([check/1]).
-export([check/2]).

-export([initial_state/0]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).
-export([cleanup/1]).

-export([start_link/1]).
-export([start_link/2]).
-export([init/1]).
-export([handle_open/1]).
-export([handle_activate/2]).
-export([handle_info/3]).
-export([handle_deactivate/2]).
-export([handle_send/2]).
-export([handle_send_recv/3]).
-export([handle_recv/3]).
-export([handle_closed/2]).
-export([handle_close/2]).

-export([recv_closed/1]).
-export([open/1]).
-export([activate/2]).
-export([info/2]).
-export([deactivate/3]).
-export([start_client/2]).
-export([shutdown_client/1]).
-export([send/2]).
-export([recv/2]).
-export([send_recv/2]).

-record(state, {sbroker, duplex, mode, buffer, owner, opens=0, send=[], recv=[],
                result=[], done=[], send_closed=[], recv_closed=[], close=[]}).

quickcheck() ->
    quickcheck([]).

quickcheck(Opts) ->
    proper:quickcheck(prop_duplex(), Opts).

check(CounterExample) ->
    check(CounterExample, []).

check(CounterExample, Opts) ->
    proper:check(prop_duplex(), CounterExample, Opts).

prop_duplex() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(begin
                          {History, State, Result} = run_commands(?MODULE, Cmds),
                          cleanup(State),
                          ?WHENFAIL(begin
                                        io:format("History~n~p", [History]),
                                        io:format("State~n~p", [State]),
                                        io:format("Result~n~p", [Result])
                                    end,
                                    aggregate(command_names(Cmds), Result =:= ok))
                      end)).


initial_state() ->
    #state{}.

command(#state{duplex=undefined} = State) ->
    {call, ?MODULE, start_link, start_link_args(State)};
command(#state{buffer=undefined, recv_closed=[_|_]} = State) ->
    frequency([{1, {call, ?MODULE, start_client, start_client_args(State)}},
               {4, {call, ?MODULE, recv_closed, recv_closed_args(State)}}]);
command(#state{buffer=undefined} = State) ->
    frequency([{1, {call, ?MODULE, open, open_args(State)}},
               {1, {call, ?MODULE, start_client, start_client_args(State)}}]);
command(#state{owner=activate} = State) ->
    {call, ?MODULE, activate, activate_args(State)};
command(#state{owner=active, send=[], recv=[]} = State) ->
    frequency([{1, {call, ?MODULE, deactivate, deactivate_args(State)}},
               {4, {call, ?MODULE, info, info_args(State)}},
               {8, {call, ?MODULE, start_client, start_client_args(State)}}]);
command(#state{send=[], recv=[]} = State) ->
    frequency([{1, {call, ?MODULE, info, info_args(State)}},
               {4, {call, ?MODULE, start_client, start_client_args(State)}}]);
command(#state{recv=[]} = State) ->
    frequency([{1, {call, ?MODULE, shutdown_client,
                    shutdown_client_args(State)}},
               {4, {call, ?MODULE, start_client, start_client_args(State)}},
               {8, {call, ?MODULE, send, send_args(State)}}]);
command(#state{send=[Client | _], recv=[Client | _]} = State) ->
    frequency([{1, {call, ?MODULE, shutdown_client,
                    shutdown_client_args(State)}},
               {4, {call, ?MODULE, start_client, start_client_args(State)}},
               {8, {call, ?MODULE, send_recv, send_recv_args(State)}}]);
command(#state{} = State) ->
    frequency([{1, {call, ?MODULE, shutdown_client,
                    shutdown_client_args(State)}},
               {4, {call, ?MODULE, start_client, start_client_args(State)}},
               {8, {call, ?MODULE, send, send_args(State)}},
               {16, {call, ?MODULE, recv, recv_args(State)}}]).

precondition(State, {call, _, start_link, Args}) ->
    start_link_pre(State, Args);
precondition(#state{duplex=undefined}, _) ->
    false;
precondition(#state{buffer=undefined}, {call, _, start_client, _}) ->
    true;
precondition(State, {call, _, recv_closed, Args}) ->
    recv_closed_pre(State, Args);
precondition(#state{buffer=undefined, recv_closed=[_|_]}, _) ->
    false;
precondition(State, {call, _, open, Args}) ->
    open_pre(State, Args);
precondition(#state{buffer=undefined}, _) ->
    false;
precondition(State, {call, _, activate, Args}) ->
    activate_pre(State, Args);
precondition(#state{owner=activate}, _) ->
    false;
precondition(State, {call, _, info, Args}) ->
    info_pre(State, Args);
precondition(State, {call, _, deactivate, Args}) ->
    deactivate_pre(State, Args);
precondition(State, {call, _, shutdown_client, Args}) ->
    shutdown_client_pre(State, Args);
precondition(State, {call, _, send, Args}) ->
    send_pre(State, Args);
precondition(State, {call, _, send_recv, Args}) ->
    send_recv_pre(State, Args);
precondition(State, {call, _, recv, Args}) ->
    recv_pre(State, Args);
precondition(_State, _Call) ->
    true.

next_state(State, Value, {call, _, start_link, Args}) ->
    start_link_next(State, Value, Args);
next_state(State, Value, {call, _, recv_closed, Args}) ->
    recv_closed_next(State, Value, Args);
next_state(State, Value, {call, _, open, Args}) ->
    open_next(State, Value, Args);
next_state(State, Value, {call, _, activate, Args}) ->
    activate_next(State, Value, Args);
next_state(State, Value, {call, _, info, Args}) ->
    info_next(State, Value, Args);
next_state(State, Value, {call, _, deactivate, Args}) ->
    deactivate_next(State, Value, Args);
next_state(State, Value, {call, _, start_client, Args}) ->
    start_client_next(State, Value, Args);
next_state(State, Value, {call, _, shutdown_client, Args}) ->
    client_next(shutdown_client_next(State, Value, Args));
next_state(State, Value, {call, _, send, Args}) ->
    client_next(send_next(State, Value, Args));
next_state(State, Value, {call, _, send_recv, Args}) ->
    client_next(send_recv_next(State, Value, Args));
next_state(State, Value, {call, _, recv, Args}) ->
    client_next(recv_next(State, Value, Args));
next_state(State, _Value, _Call) ->
    State.

postcondition(State, {call, _, start_link, Args}, Result) ->
    start_link_post(State, Args, Result);
postcondition(State, {call, _, recv_closed, Args}, Result) ->
    recv_closed_post(State, Args, Result);
postcondition(State, {call, _, open, Args}, Result) ->
    open_post(State, Args, Result);
postcondition(State, {call, _, activate, Args}, Result) ->
    activate_post(State, Args, Result);
postcondition(State, {call, _, info, Args}, Result) ->
    info_post(State, Args, Result);
postcondition(State, {call, _, deactivate, Args}, Result) ->
    deactivate_post(State, Args, Result);
postcondition(State, {call, _, start_client, Args}, Result) ->
    start_client_post(State, Args, Result);
postcondition(State, {call, _, shutdown_client, Args}, Result) ->
    shutdown_client_post(State, Args, Result);
postcondition(State, {call, _, send, Args}, Result) ->
    send_post(State, Args, Result);
postcondition(State, {call, _, send_recv, Args}, Result) ->
    send_recv_post(State, Args, Result);
postcondition(State, {call, _, recv, Args}, Result) ->
    recv_post(State, Args, Result);
postcondition(_State, _Call, _Result) ->
    true.

cleanup(#state{sbroker=undefined, duplex=undefined}) ->
    application:unset_env(duplex, handle_open),
    application:unset_env(duplex, handle_deactivate),
    flush_acks();
cleanup(#state{sbroker=Broker, duplex=undefined} = State) ->
    sys:terminate(Broker, normal),
    cleanup(State#state{sbroker=undefined});
cleanup(#state{duplex=Duplex} = State) ->
    Trap = process_flag(trap_exit, true),
    exit(Duplex, shutdown),
    receive
        {'EXIT', Duplex, shutdown} ->
            process_flag(trap_exit, Trap),
            cleanup(State#state{duplex=undefined})
    after
        ?TIMEOUT ->
            process_flag(trap_exit, Trap),
            exit(timeout)
    end.

init({sbroker, Tester}) ->
    Ask = {duplex_statem_queue, {Tester, ask, {out, drop, infinity}}},
    AskR = {duplex_statem_queue, {Tester, ask_r, {out, drop, infinity}}},
    {ok, {Ask, AskR, []}};
init({duplex, Mode, Info, Broker}) ->
    {Mode, Info, Broker, backoff:init(1, 1, self(), ?MODULE)}.

handle_open(_) ->
    case application:get_env(duplex, handle_open) of
        {ok, {ok, Buffer, Pid} = Res} ->
            application:unset_env(duplex, handle_open),
            _ = Pid ! {handle_open, self(), Buffer},
            Res;
        undefined ->
            {error, eagain}
    end.

handle_activate(Buffer, Pid) ->
    handle(Pid, handle_activate, undefined, Buffer).

handle_info({handle_info, Pid, Res}, Buffer, Pid) ->
    _ = Pid ! {handle_info, self(), Buffer},
    case Res of
        {close, _} ->
            Res;
        _ when is_atom(Res) ->
            {Res, Buffer+1}
    end.

handle_deactivate(Buffer, Pid) ->
    case application:get_env(duplex, handle_deactivate) of
        {ok, {close, _} = Res} ->
            application:unset_env(duplex, handle_deactivate),
            _ = Pid ! {handle_deactivate, self(), Buffer},
            Res;
        undefined ->
            _ = Pid ! {handle_deactivate, self(), Buffer},
            {passive, Buffer+1}
    end.

handle_send(Req, Pid) ->
    handle(Pid, handle_send, Req).

handle_send_recv(Req, Buffer, Pid) ->
    handle(Pid, handle_send_recv, Req, Buffer).

handle_recv(Req, Buffer, Pid) ->
    handle(Pid, handle_recv, Req, Buffer).

handle_closed(Req, _Pid) ->
    {result, {closed, Req}}.

handle_close(Reason, Pid) ->
    _ = Pid ! {handle_close, self(), Reason},
    ok.

handle(Pid, Fun, Req) ->
    receive
        {Fun, Pid, exit} ->
            _ = Pid ! {Fun, self(), Req},
            exit(oops);
        {Fun, Pid, Result} ->
            _ = Pid ! {Fun, self(), Req},
            Result
    end.

handle(Pid, Fun, Req, Buffer) ->
    receive
        {Fun, Pid, bad} ->
            _ = Pid ! {Fun, self(), Req, Buffer},
            bad;
        {Fun, Pid, exit} ->
            _ = Pid ! {Fun, self(), Req, Buffer},
            exit(oops);
        {Fun, Pid, {recv, NReq}} when Fun == handle_send_recv ->
            _ = Pid ! {Fun, self(), Req, Buffer},
            {recv, NReq, Buffer+1};
        {Fun, Pid, {result, Result}} ->
            _ = Pid ! {Fun, self(), Req, Buffer},
            {result, Result, Buffer+1};
        {Fun, Pid, {close, _, _} = Return} ->
            _ = Pid ! {Fun, self(), Req, Buffer},
            Return;
        {Fun, Pid, {close, _} = Return} when Fun == handle_activate ->
            _ = Pid ! {Fun, self(), Buffer},
            Return;
        {Fun, Pid, Res} when Fun == handle_activate ->
            _ = Pid ! {Fun, self(), Buffer},
            {Res, Buffer+1}
    end.

start_link(Mode) ->
    {ok, Broker} = sbroker:start_link(?MODULE, {sbroker, self()}, []),
    start_link(Mode, Broker).

start_link(Mode, Broker) ->
    Arg = {duplex, Mode, self(), Broker},
    {ok, Pid} = duplex:start_link(?MODULE, Arg, []),
    {ok, Pid, Broker}.

start_link_args(#state{sbroker=undefined}) ->
    [oneof([half_duplex, full_duplex])];
start_link_args(#state{sbroker=Broker}) ->
    [oneof([half_duplex, full_duplex]), Broker].

start_link_pre(#state{sbroker=Broker, duplex=Duplex}, [_]) ->
    Broker == undefined andalso Duplex == undefined;
start_link_pre(#state{sbroker=Broker, duplex=Duplex}, [_, Broker2]) ->
    Broker == Broker2 andalso Duplex == undefined.

start_link_next(State, Value, [Mode | _]) ->
    Duplex = {call, erlang, element, [2, Value]},
    Broker = {call, erlang, element, [3, Value]},
    State#state{sbroker=Broker, duplex=Duplex, mode=Mode}.

start_link_post(_, _, Result) ->
    case Result of
        {ok, Pid, Broker} when is_pid(Pid), is_pid(Broker) ->
            true;
        _ ->
            false
    end.

recv_closed(Clients) ->
    [recv(Client, Res) || {Client, Res} <- Clients].

recv_closed_args(#state{recv_closed=RecvClosed}) ->
    Return = frequency([{1, {close, oneof([x, y, z]), oneof([a, b, c])}},
                        {8, {result, foo}}]),
    [[{Client, Return} || {Client, _} <- RecvClosed]].

recv_closed_pre(#state{recv_closed=RecvClosed}, [Clients]) ->
    [Client || {Client, _} <- RecvClosed] == [Client || {Client, _} <- Clients].

recv_closed_next(State, _, _) ->
    State#state{recv_closed=[], result=[], close=[]}.

recv_closed_post(#state{result=Results, close=Closes,
                        duplex=Duplex}, [Clients], _) ->
    Post = fun({Client, {result, Res}}) ->
                   result_post(Client, Res);
              ({Client, {close, _, Res}}) ->
                   result_post(Client, Res)
           end,
    Reasons = [Reason || {_, {close, Reason, _}} <- Clients] ++ Closes,
    lists:all(Post, Clients) andalso result_post(Results) andalso
    (Closes == [] orelse close_post(Duplex, hd(Reasons))).

open(Buffer) ->
    application:set_env(duplex, handle_open, {ok, Buffer, self()}).

open_args(#state{opens=Opens}) ->
    [(Opens*100)+1].

open_pre(#state{buffer=Buffer}, _) ->
    Buffer == undefined.

open_next(#state{opens=Opens, send=[]} = State, _, [Buffer]) ->
    State#state{buffer=Buffer, opens=Opens+1, owner=activate};
open_next(#state{opens=Opens} = State, _, [Buffer]) ->
    State#state{buffer=Buffer, opens=Opens+1, owner=passive}.

open_post(#state{duplex=Duplex}, [Buffer], _) ->
    receive
        {handle_open, Duplex, Buffer} ->
            true;
        {handle_open, Duplex, Buffer2} ->
            ct:pal("Buffer~nExpected: ~p~nObserved: ~p", [Buffer, Buffer2]),
            false
    after
        ?TIMEOUT ->
            ct:pal("open timeout"),
            false
    end.

activate(Duplex, Result) ->
    Duplex ! {handle_activate, self(), Result},
    ok.

activate_args(#state{duplex=Duplex}) ->
    [Duplex, oneof([passive, active, {close, oneof([x, y, x])}])].

activate_pre(#state{owner=Owner}, _) ->
    Owner == activate.

activate_next(#state{buffer=Buffer} = State, _, [_, Owner])
  when is_atom(Owner) ->
    State#state{buffer=Buffer+1, owner=Owner};
activate_next(#state{send=[], recv=[]} = State, _, [_, {close, _}]) ->
    State#state{buffer=undefined}.

activate_post(#state{duplex=Duplex, buffer=Buffer}, [_, Result], _) ->
    receive
        {handle_activate, Duplex, Buffer} when is_atom(Result) ->
            true;
        {handle_activate, Duplex, Buffer} when element(1, Result) == close ->
            {close, Reason} = Result,
            close_post(Duplex, Reason);
        {handle_activate, Duplex, Buffer2} ->
            ct:pal("Buffer~nExpected: ~p~nObserved: ~p", [Buffer, Buffer2]),
            false
    after
        ?TIMEOUT ->
            ct:pal("activate timeout"),
            false
    end.

info(Duplex, Result) ->
    Duplex ! {handle_info, self(), Result},
    ok.

info_args(#state{duplex=Duplex}) ->
    [Duplex, oneof([passive, active, {close, oneof([x, y, x])}])].

info_pre(#state{owner=Owner}, _) ->
    Owner == active.

info_next(#state{buffer=Buffer} = State, _, [_, Owner]) when is_atom(Owner) ->
    State#state{buffer=Buffer+1, owner=Owner};
info_next(#state{send=[], recv=[]} = State, _, [_, {close, _}]) ->
    State#state{buffer=undefined}.

info_post(#state{duplex=Duplex, buffer=Buffer}, [_, Result], _) ->
    receive
        {handle_info, Duplex, Buffer} when is_atom(Result) ->
            true;
        {handle_info, Duplex, Buffer} when element(1, Result) == close ->
            {close, Reason} = Result,
            close_post(Duplex, Reason);
        {handle_info, Duplex, Buffer2} ->
            ct:pal("Buffer~nExpected: ~p~nObserved: ~p", [Buffer, Buffer2]),
            false
    after
        ?TIMEOUT ->
            ct:pal("info timeout"),
            false
    end.

deactivate(Result, Broker, Req) ->
    application:set_env(duplex, handle_deactivate, Result),
    start_client(Broker, Req).

deactivate_args(State) ->
    [{close, oneof([x, y, x])} | start_client_args(State)].

deactivate_pre(#state{send=[], recv=[], owner=active}, _) ->
    true;
deactivate_pre(_, _) ->
    false.

deactivate_next(#state{send_closed=SendClosed} = State, _,
                [{close, _}, Client, Req]) ->
    State#state{buffer=undefined, send_closed=SendClosed++[{Client, Req}]}.

deactivate_post(#state{duplex=Duplex, buffer=Buffer} = State,
                [{close, Reason} | Args], Client) ->
    deactivate_post(Duplex, Buffer) andalso close_post(Duplex, Reason) andalso
    start_client_post(State, Args, Client).

start_client(Broker, Req) ->
    Tester = self(),
    spawn(fun() -> client_init(Broker, Req, Tester) end).

start_client_args(#state{sbroker=Broker}) ->
    [Broker, oneof([a, b, c])].

start_client_next(#state{send=Sends} = State, Client, [_, Req]) ->
    State#state{send=Sends ++ [{Client, Req}]}.

start_client_post(_, _, _) ->
    receive
        client_ack ->
            receive
                client_ack ->
                    ct:pal("leaking client acks"),
                    false
            after
                0 ->
                    true
            end
    after
        ?TIMEOUT ->
            ct:pal("no client ack"),
            false
    end.

shutdown_client(Client) ->
    timer:sleep(50),
    MRef = monitor(process, Client),
    exit(Client, shutdown),
    receive
        {'DOWN', MRef, _, _, shutdown} ->
            ok
    after
        ?TIMEOUT ->
            demonitor(MRef, [flush]),
            exit(timeout)
    end.

shutdown_client_args(#state{send=[{Client, _} | _], recv=[], result=[]}) ->
    [Client];
shutdown_client_args(#state{send=[{Send, _} | _], recv=[{Recv, _} | _],
                            result=[]}) ->
    [oneof([Send, Recv])];
shutdown_client_args(#state{recv=[{Recv, _} | _], result=[{Client, _} | _]}) ->
    [oneof([Recv, Client])];
shutdown_client_args(#state{send=[], recv=[{Client, _} | _]}) ->
    [Client].

shutdown_client_pre(#state{recv=[{Client, _} | _]}, [Client]) ->
    true;
shutdown_client_pre(#state{result=[{Client, _} | _]}, [Client]) ->
    true;
shutdown_client_pre(#state{mode=full_duplex, send=[{Client, _} | _],
                           recv=Recvs, result=[]}, [Client])
  when length(Recvs) =< 1 ->
    true;
shutdown_client_pre(#state{mode=half_duplex, send=[{Client, _} | _], recv=[]},
                    [Client]) ->
    true;
shutdown_client_pre(_, _) ->
    false.


shutdown_client_next(#state{owner=active, buffer=Buffer,
                            send=[{Client, _} | _]} = State, Value, [Client])->
    shutdown_client_next(State#state{owner=passive, buffer=Buffer+1}, Value,
                         [Client]);
shutdown_client_next(#state{send=[{Client, _} | Sends],
                            recv=[{Client, _}]} = State, _, [Client]) ->
    State#state{buffer=undefined, send=Sends, recv=[], result=[]};
shutdown_client_next(#state{send=[{Client, _} | Sends], recv=Recv,
                            recv_closed=RecvClosed} = State, _, [Client]) ->
    State#state{buffer=undefined, send=Sends, recv=[],
                recv_closed=RecvClosed ++ Recv};
shutdown_client_next(#state{mode=full_duplex, recv=[{Client, _}],
                            send=[Send | Sends], result=[],
                            send_closed=SendClosed} = State, _, [Client]) ->
    State#state{buffer=undefined, recv=[], send=Sends,
                send_closed=SendClosed ++ [Send]};
shutdown_client_next(#state{recv=[{Client, _} | Recvs], send=Sends} = State, _,
                     [Client]) ->
    NSends = [Send || Send <- Sends, not lists:member(Send, Recvs)],
    State#state{buffer=undefined, recv=[], result=[], send=NSends};
shutdown_client_next(#state{result=[{Client, _} | Results], recv=Recv,
                            recv_closed=RecvClosed} = State, _, [Client]) ->
    State#state{buffer=undefined, result=Results, recv=[],
                recv_closed=RecvClosed ++ Recv}.

shutdown_client_post(#state{owner=active, duplex=Duplex, buffer=Buffer,
                            send=[{Client, _} | _]} = State, [Client] = Args,
                     Result) ->
    deactivate_post(Duplex, Buffer) andalso
    shutdown_client_post(State#state{owner=passive, buffer=Buffer+1}, Args,
                         Result);
shutdown_client_post(#state{duplex=Duplex, result=[{Client, _} | Results]},
                     [Client], _) ->
    Reason = {'DOWN', Client, shutdown},
    close_post(Duplex, Reason) andalso result_post(Results);
shutdown_client_post(#state{duplex=Duplex, result=Results}, [Client], _) ->
    Reason = {'DOWN', Client, shutdown},
    close_post(Duplex, Reason) andalso result_post(Results).

send(Client, {Bad, Duplex}) when Bad == bad; Bad == exit ->
    Trap = process_flag(trap_exit, true),
    Client ! {handle_send, self(), Bad},
    receive
        {'EXIT', Duplex, Reason} ->
            process_flag(trap_exit, Trap),
            Reason
    after
        ?TIMEOUT ->
            process_flag(trap_exit, Trap),
            exit(timeout)
    end;
send(Client, Result) ->
    Client ! {handle_send, self(), Result},
    ok.

send_args(#state{duplex=Duplex, send=[{Client, _Req} | _]}) ->
    Return = frequency([{1, {oneof([bad, exit]), Duplex}},
                        {2, {result, foo}},
                        {2, {close, oneof([x, y, z]), oneof([a, b, c])}},
                        {8, {send_recv, oneof([a, b, c])}},
                        {8, {recv, oneof([a, b, c])}}]),
    [Client, Return];
send_args(_) ->
    [undefined, undefined].

send_pre(#state{send=[{Client, _} | _], recv=[{Client, _} | _]}, _) ->
    false;
send_pre(#state{owner=Owner, send=Sends, recv=Recvs, result=Results}, _)
  when (Owner == activate orelse Owner == active) andalso
       (Sends =/= [] orelse Recvs =/= [] orelse Results =/= []) ->
    false;
send_pre(#state{mode=half_duplex, send=[{Client, _} | _], recv=[]},
         [Client, _]) ->
    true;
send_pre(#state{mode=full_duplex, send=[{Client, _} | _], recv=Recvs,
         result=[]}, [Client, _]) when length(Recvs) =< 1 ->
    true;
send_pre(_, _) ->
    false.

send_next(#state{owner=active, buffer=Buffer} = State, Value, Args) ->
    send_next(State#state{owner=passive, buffer=Buffer+1}, Value, Args);
send_next(#state{owner=passive, send=[{Client, _} | Sends], recv=[]} = State, _,
          [Client, {result, _}]) ->
    State#state{send=Sends};
send_next(#state{send=[{Client, _} | Sends], result=Results} = State, _,
          [Client, {result, Res}]) ->
    State#state{send=Sends, result=Results++[{Client, Res}]};
send_next(#state{send=[{Client, _} | Sends], recv=Recvs} = State, _,
                       [Client, {recv, Req}]) ->
    State#state{send=Sends, recv=Recvs ++ [{Client, Req}]};
send_next(#state{send=[{Client, _} | Sends], recv=Recvs} = State, _,
                       [Client, {send_recv, Req}]) ->
    State#state{send=[{Client, Req} | Sends], recv=Recvs ++ [{Client, Req}]};
send_next(#state{owner=passive, send=[{Client, _} | Sends], recv=[]} = State, _,
          [Client, {close, _, _}]) ->
    State#state{buffer=undefined, send=Sends};
send_next(#state{send=[{Client, _} | Sends], recv=Recvs, result=Results,
                 recv_closed=RecvClosed} = State, _,
          [Client, {close, Reason, Res}]) ->
    State#state{buffer=undefined, send=Sends, recv=[],
                recv_closed=RecvClosed ++ Recvs,
                result=Results++[{Client, Res}], close=[Reason]};
send_next(#state{owner=passive, send=[{Client, _} | Sends], recv=[]} = State, _,
          [Client, {Bad, _}]) when Bad == bad; Bad == exit ->
    State#state{duplex=undefined, buffer=undefined, send=Sends};
send_next(#state{send=[{Client, _} | Sends], recv=Recvs,
                 recv_closed=RecvClosed} = State, _,
          [Client, {Bad, _}]) when Bad == bad; Bad == exit ->
    State#state{duplex=undefined, buffer=undefined, send=Sends, recv=[],
                recv_closed=RecvClosed ++ Recvs}.

send_post(#state{owner=active, duplex=Duplex, buffer=Buffer} = State, Args,
          Result) ->
    deactivate_post(Duplex, Buffer) andalso
    send_post(State#state{owner=passive, buffer=Buffer+1}, Args, Result);
send_post(#state{send=[{Client, Req} | _], recv=Recvs, result=Results,
                 owner=Owner, duplex=Duplex}, [Client, Return], _) ->
    receive
        {handle_send, Client, Req}
          when element(1, Return) == result, Recvs == [], Results == [],
               Owner == passive ->
            {result, Res} = Return,
            result_post(Client, Res);
        {handle_send, Client, Req}
          when element(1, Return) == result; element(1, Return) == recv;
               element(1, Return) == send_recv ->
            true;
        {handle_send, Client, Req}
          when element(1, Return) == close, Recvs == [], Results == [],
               Owner == passive ->
            {close, Reason, Res} = Return,
            close_post(Duplex, Reason) andalso result_post(Client, Res);
        {handle_send, Client, Req} when element(1, Return) == close ->
            true;
        {handle_send, Client, Req} when element(1, Return) == bad ->
            Reason = {bad_return_value, bad},
            exit_post(Duplex, Reason) andalso
            result_post(Client, {'EXIT', Reason}) andalso result_post(Results);
        {handle_send, Client, Req} when element(1, Return) == exit ->
            exit_post(Duplex, oops) andalso
            result_post(Client, {'EXIT', oops}) andalso result_post(Results)
    after
        ?TIMEOUT ->
            unexpected(Client)
    end.

send_recv(Client, {Bad, Duplex}) when Bad == bad; Bad == exit ->
    Trap = process_flag(trap_exit, true),
    Client ! {handle_send_recv, self(), Bad},
    receive
        {'EXIT', Duplex, Reason} ->
            process_flag(trap_exit, Trap),
            Reason
    after
        ?TIMEOUT ->
            process_flag(trap_exit, Trap),
            exit(timeout)
    end;
send_recv(Client, Result) ->
    Client ! {handle_send_recv, self(), Result},
    ok.

send_recv_args(#state{send=[{Client, _} | _], recv=[{Client, _} | _],
                      duplex=Duplex}) ->
    Return = frequency([{1, {oneof([bad, exit]), Duplex}},
                        {2, {close, oneof([x, y, z]), oneof([a, b, c])}},
                        {8, {result, foo}},
                        {8, {recv, oneof([a, b, c])}}]),
    [Client, Return];
send_recv_args(_) ->
    [undefined, undefined].

send_recv_pre(#state{send=[{Client, Req} | _], recv=[{Client, Req} | _]},
              [Client, _]) ->
    true;
send_recv_pre(_, _) ->
    false.

send_recv_next(#state{send=[{Client, _} | Sends], recv=[{Client, _}],
                      buffer=Buffer} = State, _, [Client, {result, _}]) ->
    State#state{buffer=Buffer+1, send=Sends, recv=[], result=[]};
send_recv_next(#state{send=[{Client, _} | Sends], recv=[{Client, _}],
                      buffer=Buffer} = State, _, [Client, {recv, Req}]) ->
    State#state{buffer=Buffer+1, send=Sends, recv=[{Client, Req}], result=[]};
send_recv_next(#state{send=[{Client, _} | Sends], recv=[{Client, _}]} = State,
               _, [Client, {close, _, _}]) ->
    State#state{buffer=undefined, send=Sends, recv=[], result=[]};
send_recv_next(#state{send=[{Client, _} | Sends], recv=[{Client, _}]} = State,
               _, [Client, {Bad, _}]) when Bad == bad; Bad == exit ->
    State#state{duplex=undefined, buffer=undefined, send=Sends, recv=[],
                result=[]}.

send_recv_post(#state{send=[{Client, Req} | _], buffer=Buffer, result=Results,
                      duplex=Duplex}, [Client, Return], _) ->
    receive
        {handle_send_recv, Client, Req, Buffer2} when Buffer2 =/= Buffer ->
            ct:pal("Buffer~nExpected: ~p~nObserved: ~p", [Buffer, Buffer2]),
            false;
        {handle_send_recv, Client, Req, _} when element(1, Return) == result ->
            {result, Res} = Return,
            result_post(Client, Res) andalso result_post(Results);
        {handle_send_recv, Client, Req, _} when element(1, Return) == recv ->
            result_post(Results);
        {handle_send_recv, Client, Req, _} when element(1, Return) == close ->
            {close, Reason, Res} = Return,
            result_post(Client, Res) andalso close_post(Duplex, Reason) andalso
            result_post(Results);
        {handle_send_recv, Client, Req, _} when element(1, Return) == bad ->
            Reason = {bad_return_value, bad},
            exit_post(Duplex, Reason) andalso
            result_post(Client, {'EXIT', Reason}) andalso result_post(Results);
        {handle_send_recv, Client, Req, _} when element(1, Return) == exit ->
            exit_post(Duplex, oops) andalso
            result_post(Client, {'EXIT', oops}) andalso result_post(Results)
    after
        ?TIMEOUT ->
            unexpected(Client)
    end.

recv(Client, {Bad, Duplex}) when Bad == bad; Bad == exit ->
    Trap = process_flag(trap_exit, true),
    Client ! {handle_recv, self(), Bad},
    receive
        {'EXIT', Duplex, Reason} ->
            process_flag(trap_exit, Trap),
            Reason
    after
        ?TIMEOUT ->
            process_flag(trap_exit, Trap),
            exit(timeout)
    end;
recv(Client, Return) ->
    Client ! {handle_recv, self(), Return},
    ok.

recv_args(#state{recv=[{Client, _Req} | _], duplex=Duplex}) ->
    Return = frequency([{1, {oneof([bad, exit]), Duplex}},
                        {2, {close, oneof([x, y, z]), oneof([a, b, c])}},
                        {16, {result, foo}}]),
    [Client, Return];
recv_args(_) ->
    [undefined, undefined].

recv_pre(#state{send=[{Client, _} | _], recv=[{Client, _} | _]}, _) ->
    false;
recv_pre(#state{recv=[{Client, _} | _]}, [Client, _]) ->
    true;
recv_pre(_, _) ->
    false.

recv_next(#state{recv=[{Client, _}], buffer=Buffer} = State, _,
          [Client, {result, _}]) ->
    State#state{buffer=Buffer+1, recv=[], result=[]};
recv_next(#state{recv=[{Client, _} | Recvs], buffer=Buffer} = State, _,
          [Client, {result, _}]) ->
    State#state{buffer=Buffer+1, recv=Recvs};
recv_next(#state{mode=full_duplex, recv=[{Client, _}],
                 send=[Send | Sends], result=[],
                 send_closed=SendClosed} = State, _, [Client, {close, _, _}]) ->
    State#state{buffer=undefined, recv=[], result=[],
                send=Sends, send_closed=SendClosed ++ [Send]};
recv_next(#state{recv=[{Client, _} | Recvs], send=Sends} = State, _,
          [Client, {close, _, _}]) ->
    NSends = [Send || Send <- Sends, not lists:member(Send, Recvs)],
    State#state{buffer=undefined, recv=[], result=[], send=NSends};
recv_next(#state{mode=full_duplex, recv=[{Client, _}],
                 send=[Send | Sends], result=[],
                 send_closed=SendClosed} = State, _, [Client, {Bad, _}])
  when Bad == bad; Bad == exit ->
    State#state{duplex=undefined, buffer=undefined, recv=[], result=[],
                send=Sends, send_closed=SendClosed ++ [Send]};
recv_next(#state{recv=[{Client, _} | Recvs], send=Sends} = State, _,
          [Client, {Bad, _}]) when Bad == bad; Bad == exit ->
    NSends = [Send || Send <- Sends, not lists:member(Send, Recvs)],
    State#state{duplex=undefined, buffer=undefined, recv=[], result=[],
                send=NSends}.

recv_post(#state{recv=[{Client, Req} | Recvs], buffer=Buffer, result=Results,
                 duplex=Duplex}, [Client, Return], _) ->
    receive
        {handle_recv, Client, Req, Buffer2} when Buffer2 =/= Buffer ->
            ct:pal("Buffer~nExpected: ~p~nObserved: ~p", [Buffer, Buffer2]),
            false;
        {handle_recv, Client, Req, _}
          when element(1, Return) == result, Recvs == [] ->
            {result, Res} = Return,
            result_post(Client, Res) andalso result_post(Results);
        {handle_recv, Client, Req, _}
          when element(1, Return) == result, Recvs =/= [] ->
            {result, Res} = Return,
            result_post(Client, Res);
        {handle_recv, Client, Req, _} when element(1, Return) == close ->
            {close, Reason, Res} = Return,
            result_post(Client, Res) andalso result_post(Results) andalso
            close_post(Duplex, Reason) andalso closed_post(Recvs);
        {handle_recv, Client, Req, _} when element(1, Return) == bad ->
            Reason = {bad_return_value, bad},
            exit_post(Duplex, Reason) andalso
            result_post(Client, {'EXIT', Reason}) andalso result_post(Results);
        {handle_recv, Client, Req, _} when element(1, Return) == exit ->
            exit_post(Duplex, oops) andalso
            result_post(Client, {'EXIT', oops}) andalso result_post(Results)
    after
        ?TIMEOUT ->
            unexpected(Client)
    end.

result_post(Results) ->
    Post = fun({Client, Result}) ->
                   result_post(Client, Result);
              ({Duplex, close, Reason}) ->
                   close_post(Duplex, Reason)
           end,
    lists:all(Post, Results).

result_post(Client, Result) ->
    receive
        {result, Client, Result} ->
            true;
        {result, Client, Result2} ->
            ct:pal("Result~nExpected: ~p~nObserved: ~p", [Result, Result2]),
            false
    after
        ?TIMEOUT ->
            unexpected(Client)
    end.

deactivate_post(Duplex, Buffer) ->
    receive
        {handle_deactivate, Duplex, Buffer} ->
            true;
        {handle_deactivate, Duplex, Buffer2} ->
            ct:pal("Buffer~nExpected: ~p~nObserved: ~p", [Buffer, Buffer2]),
            false
    after
        ?TIMEOUT ->
            ct:pal("deactivate timeout"),
            false
    end.

close_post(Duplex, Reason) ->
    receive
        {handle_close, Duplex, Reason} ->
            true;
        {handle_close, Duplex, Reason2} ->
            ct:pal("Reason~nExpected: ~p~nObserved: ~p", [Reason, Reason2]),
            false
    after
        ?TIMEOUT ->
            ct:pal("close timeout"),
            false
    end.

exit_post(Duplex, Reason) ->
    receive
        {handle_close, Duplex, {exit, Reason, [_|_]}} ->
            true;
        {handle_close, Duplex, Reason2} ->
            ct:pal("Reason~nExpected: ~p~nObserved: ~p",
                   [{exit, Reason, []}, Reason2]),
            false
    after
        ?TIMEOUT ->
            ct:pal("close timeout"),
            false
    end.

closed_post(Recvs) ->
    Post = fun({Client, Req}) -> result_post(Client, {closed, Req}) end,
    lists:all(Post, Recvs).

unexpected(Client) ->
    receive
        {_, Client, _} = Msg ->
            ct:pal("Unexpected message: ~p", [Msg]),
            false
    after
        0 ->
            ct:pal("Did not receive message from ~p", [Client]),
            false
    end.

%% Helpers

client_next(#state{send=[], recv=[], result=[]} = State) ->
    State#state{owner=activate};
client_next(State) ->
    State.

client_init(Broker, Req, Tester) ->
    Tester ! {result, self(), catch duplex:send_recv(Broker, Req)}.

flush_acks() ->
    receive
        client_ack ->
            flush_acks()
    after
        0 ->
            ok
    end.
