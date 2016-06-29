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
-export([init/1]).
-export([handle_open/1]).
-export([handle_activate/2]).
-export([handle_deactivate/2]).
-export([handle_send/2]).
-export([handle_send_recv/3]).
-export([handle_recv/3]).
-export([handle_closed/2]).
-export([handle_close/2]).

-export([recv_closed/1]).
-export([open/1]).
-export([start_client/2]).
-export([send/2]).
-export([recv/2]).
-export([send_recv/2]).
-export([noop/0]).

-record(state, {sbroker, duplex, mode, buffer, opens=0, send=[], recv=[],
                result=[], done=[], send_closed=[], recv_closed=[]}).

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

command(#state{sbroker=undefined, duplex=undefined} = State) ->
    {call, ?MODULE, start_link, start_link_args(State)};
command(#state{buffer=undefined, recv_closed=[_|_]} = State) ->
    frequency([{1, {call, ?MODULE, recv_closed, recv_closed_args(State)}},
               {1, {call, ?MODULE, start_client, start_client_args(State)}}]);
command(#state{buffer=undefined} = State) ->
    frequency([{1, {call, ?MODULE, open, open_args(State)}},
               {1, {call, ?MODULE, start_client, start_client_args(State)}}]);
command(#state{send=[], recv=[]} = State) ->
    {call, ?MODULE, start_client, start_client_args(State)};
command(#state{recv=[]} = State) ->
    frequency([{1, {call, ?MODULE, start_client, start_client_args(State)}},
               {2, {call, ?MODULE, send, send_args(State)}}]);
command(#state{send=[Client | _], recv=[Client | _]} = State) ->
    frequency([{1, {call, ?MODULE, start_client, start_client_args(State)}},
               {4, {call, ?MODULE, send_recv, send_recv_args(State)}}]);
command(#state{} = State) ->
    frequency([{1, {call, ?MODULE, start_client, start_client_args(State)}},
               {2, {call, ?MODULE, send, send_args(State)}},
               {4, {call, ?MODULE, recv, recv_args(State)}}]).


precondition(State, {call, _, start_link, Args}) ->
    start_link_pre(State, Args);
precondition(#state{sbroker=undefined, duplex=undefined}, _) ->
    false;
precondition(_, {call, _, start_client, _}) ->
    true;
precondition(State, {call, _, recv_closed, Args}) ->
    recv_closed_pre(State, Args);
precondition(#state{buffer=undefined, recv_closed=[_|_]}, _) ->
    false;
precondition(State, {call, _, open, Args}) ->
    open_pre(State, Args);
precondition(#state{buffer=undefined}, _) ->
    false;
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
next_state(State, Value, {call, _, start_client, Args}) ->
    start_client_next(State, Value, Args);
next_state(State, Value, {call, _, send, Args}) ->
    send_next(State, Value, Args);
next_state(State, Value, {call, _, send_recv, Args}) ->
    send_recv_next(State, Value, Args);
next_state(State, Value, {call, _, recv, Args}) ->
    recv_next(State, Value, Args);
next_state(State, _Value, _Call) ->
    State.

postcondition(State, {call, _, start_link, Args}, Result) ->
    start_link_post(State, Args, Result);
postcondition(State, {call, _, recv_closed, Args}, Result) ->
    recv_closed_post(State, Args, Result);
postcondition(State, {call, _, open, Args}, Result) ->
    open_post(State, Args, Result);
postcondition(State, {call, _, start_client, Args}, Result) ->
    start_client_post(State, Args, Result);
postcondition(State, {call, _, send, Args}, Result) ->
    send_post(State, Args, Result);
postcondition(State, {call, _, send_recv, Args}, Result) ->
    send_recv_post(State, Args, Result);
postcondition(State, {call, _, recv, Args}, Result) ->
    recv_post(State, Args, Result);
postcondition(_State, _Call, _Result) ->
    true.

cleanup(#state{sbroker=undefined, duplex=undefined}) ->
    application:unset_env(duplex, handle_activate),
    application:unset_env(duplex, handle_open),
    flush_acks();
cleanup(#state{sbroker=Broker, duplex=Duplex} = State) ->
    sys:terminate(Duplex, normal),
    sys:terminate(Broker, normal),
    cleanup(State#state{sbroker=undefined, duplex=undefined}).

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

handle_activate(Buffer, _Pid) ->
    %handle(Pid, handle_activate, Buffer).
    {passive, Buffer}.

handle_deactivate(Buffer, Pid) ->
    handle(Pid, handle_deactivate, Buffer).

handle_send(Req, Pid) ->
    handle(Pid, handle_send, Req).

handle_send_recv(Req, Buffer, Pid) ->
    handle(Pid, handle_send_recv, Req, Buffer).

handle_recv(Req, Buffer, Pid) ->
    handle(Pid, handle_recv, Req, Buffer).

handle_closed(Req, _Pid) ->
    {result, {closed, Req}}.

handle_close(_Reason, _Pid) ->
    ok.
    %handle(Pid, handle_close, Reason).

handle(Pid, Fun, Req) ->
    receive
        {Fun, Pid, Result} ->
            _ = Pid ! {Fun, self(), Req},
            Result
    end.

handle(Pid, Fun, Req, Buffer) ->
    receive
        {Fun, Pid, {recv, NReq}} when Fun == handle_send_recv ->
            _ = Pid ! {Fun, self(), Req},
            {recv, NReq, Buffer};
        {Fun, Pid, {result, Result}} ->
            _ = Pid ! {Fun, self(), Req},
            {result, Result, Buffer};
        {Fun, Pid, {close, _, _} = Return} ->
            _ = Pid ! {Fun, self(), Req},
            Return
    end.

start_link(Mode) ->
    {ok, Broker} = sbroker:start_link(?MODULE, {sbroker, self()}, []),
    Arg = {duplex, Mode, self(), Broker},
    {ok, Pid} = duplex:start_link(?MODULE, Arg, []),
    {ok, Pid, Broker}.

start_link_args(_) ->
    [oneof([half_duplex, full_duplex])].

start_link_pre(#state{sbroker=Broker, duplex=Duplex}, _) ->
    Broker == undefined orelse Duplex == undefined.

start_link_next(State, Value, [Mode]) ->
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
    State#state{recv_closed=[]}.

recv_closed_post(_, [Clients], _) ->
    Post = fun({Client, {result, Res}}) ->
                   result_post(Client, Res);
              ({Client, {close, _, Res}}) ->
                   result_post(Client, Res)
           end,
    lists:all(Post, Clients).

open(Buffer) ->
    application:set_env(duplex, handle_open, {ok, Buffer, self()}).

open_args(#state{opens=Opens}) ->
    [Opens+1].

open_pre(#state{buffer=Buffer}, _) ->
    Buffer == undefined.

open_next(#state{opens=Opens} = State, _, [Buffer]) ->
    State#state{buffer=Buffer, opens=Opens+1}.

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

send(Client, Result) ->
    Client ! {handle_send, self(), Result},
    ok.

send_args(#state{send=[{Client, _Req} | _]}) ->
    Return = frequency([{1, {result, foo}},
                        {1, {close, oneof([x, y, z]), oneof([a, b, c])}},
                        {8, {send_recv, oneof([a, b, c])}},
                        {8, {recv, oneof([a, b, c])}}]),
    [Client, Return];
send_args(_) ->
    [undefined, undefined].

send_pre(#state{send=[{Client, _} | _], recv=[{Client, _} | _]}, _) ->
    false;
send_pre(#state{mode=half_duplex, send=[{Client, _} | _], recv=[]},
         [Client, _]) ->
    true;
send_pre(#state{mode=full_duplex, send=[{Client, _} | _], recv=Recvs,
         result=[]}, [Client, _]) when length(Recvs) =< 1 ->
    true;
send_pre(_, _) ->
    false.

send_next(#state{send=[{Client, _} | Sends], recv=[]} = State, _,
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
send_next(#state{send=[{Client, _} | Sends], recv=[]} = State, _,
          [Client, {close, _, _}]) ->
    State#state{buffer=undefined, send=Sends};
send_next(#state{send=[{Client, _} | Sends], recv=Recvs, result=Results,
                 recv_closed=RecvClosed} = State, _, [Client, {close, _, Res}]) ->
    State#state{buffer=undefined, send=Sends, recv=[],
                recv_closed=RecvClosed ++ Recvs,
                result=Results++[{Client, Res}]}.

send_post(#state{send=[{Client, Req} | _], recv=Recvs, result=Results},
          [Client, Return], _) ->
    receive
        {handle_send, Client, Req}
          when element(1, Return) == result, Recvs == [], Results == [] ->
            {result, Res} = Return,
            result_post(Client, Res);
        {handle_send, Client, Req}
          when element(1, Return) == result; element(1, Return) == recv;
               element(1, Return) == send_recv ->
            true;
        {handle_send, Client, Req}
          when element(1, Return), Recvs == [], Results == [] ->
            {close, _, Res} = Return,
            result_post(Client, Res);
        {handle_send, Client, Req} when element(1, Return) == close ->
            result_post(Results)
    after
        ?TIMEOUT ->
            unexpected(Client)
    end.

send_recv(Client, Result) ->
    Client ! {handle_send_recv, self(), Result},
    ok.

send_recv_args(#state{send=[{Client, _} | _], recv=[{Client, _} | _]}) ->
    Return = frequency([{1, {close, oneof([x, y, z]), oneof([a, b, c])}},
                        {4, {result, foo}},
                        {4, {recv, oneof([a, b, c])}}]),
    [Client, Return];
send_recv_args(_) ->
    [undefined, undefined].

send_recv_pre(#state{send=[{Client, Req} | _], recv=[{Client, Req} | _]},
              [Client, _]) ->
    true;
send_recv_pre(_, _) ->
    false.

send_recv_next(#state{send=[{Client, _} | Sends], recv=[{Client, _}]} = State,
               _, [Client, {result, _}]) ->
    State#state{send=Sends, recv=[], result=[]};
send_recv_next(#state{send=[{Client, _} | Sends], recv=[{Client, _}]} = State,
               _, [Client, {recv, Req}]) ->
    State#state{send=Sends, recv=[{Client, Req}], result=[]};
send_recv_next(#state{send=[{Client, _} | Sends], recv=[{Client, _}]} = State,
               _, [Client, {close, _, _}]) ->
    State#state{buffer=undefined, send=Sends, recv=[], result=[]}.

send_recv_post(#state{send=[{Client, Req} | _], result=Results},
          [Client, Return], _) ->
    receive
        {handle_send_recv, Client, Req} when element(1, Return) == result ->
            {result, Res} = Return,
            result_post(Client, Res) andalso result_post(Results);
        {handle_send_recv, Client, Req} when element(1, Return) == recv ->
            result_post(Results);
        {handle_send_recv, Client, Req} when element(1, Return) == close ->
            {close, _, Res} = Return,
            result_post(Client, Res) andalso result_post(Results)
    after
        ?TIMEOUT ->
            unexpected(Client)
    end.

recv(Client, Return) ->
    Client ! {handle_recv, self(), Return},
    ok.

recv_args(#state{recv=[{Client, _Req} | _]}) ->
    Return = frequency([{1, {close, oneof([x, y, z]), oneof([a, b, c])}},
                        {8, {result, foo}}]),
    [Client, Return];
recv_args(_) ->
    [undefined, undefined].

recv_pre(#state{send=[{Client, _} | _], recv=[{Client, _} | _]}, _) ->
    false;
recv_pre(#state{recv=[{Client, _} | _]}, [Client, _]) ->
    true;
recv_pre(_, _) ->
    false.

recv_next(#state{recv=[{Client, _}]} = State, _, [Client, {result, _}]) ->
    State#state{recv=[], result=[]};
recv_next(#state{recv=[{Client, _} | Recvs]} = State, _,
          [Client, {result, _}]) ->
    State#state{recv=Recvs};
recv_next(#state{mode=full_duplex, recv=[{Client, _}],
                 send=[Send | Sends], result=[],
                 send_closed=SendClosed} = State, _, [Client, {close, _, _}]) ->
    State#state{buffer=undefined, recv=[], result=[],
                send=Sends, send_closed=SendClosed ++ [Send]};
recv_next(#state{recv=[{Client, _} | Recvs], send=Sends} = State, _,
          [Client, {close, _, _}]) ->
    NSends = [Send || Send <- Sends, not lists:member(Send, Recvs)],
    State#state{buffer=undefined, recv=[], result=[], send=NSends}.

recv_post(#state{recv=[{Client, Req} | Recvs], result=Results},
          [Client, Return], _) ->
    receive
        {handle_recv, Client, Req}
          when element(1, Return) == result, Recvs == [] ->
            {result, Res} = Return,
            result_post(Client, Res) andalso result_post(Results);
        {handle_recv, Client, Req}
          when element(1, Return) == result, Recvs =/= [] ->
            {result, Res} = Return,
            result_post(Client, Res);
        {handle_recv, Client, Req} when element(1, Return) == close ->
            {close, _, Res} = Return,
            result_post(Client, Res) andalso result_post(Results) andalso
            closed_post(Recvs)
    after
        ?TIMEOUT ->
            unexpected(Client)
    end.

result_post(Results) ->
    Post = fun({Client, Result}) -> result_post(Client, Result) end,
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

noop() ->
    ok.

%% Helpers

client_init(Broker, Req, Tester) ->
    Tester ! {result, self(), duplex:send_recv(Broker, Req)}.

flush_acks() ->
    receive
        client_ack ->
            flush_acks()
    after
        0 ->
            ok
    end.
