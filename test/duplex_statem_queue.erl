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
-module(duplex_statem_queue).

-behaviour(sbroker_queue).

-export([init/3]).
-export([handle_in/5]).
-export([handle_out/2]).
-export([handle_timeout/2]).
-export([handle_cancel/3]).
-export([handle_info/3]).
-export([code_change/4]).
-export([config_change/3]).
-export([len/1]).
-export([send_time/1]).
-export([terminate/2]).

init(Q, Time, {Tester, Queue, Args}) ->
    {State, Timeout} = sbroker_drop_queue:init(Q, Time, Args),
    {{Tester, Queue, State}, Timeout}.

handle_in(SendTime, From, Value, Time, {Tester, Queue, State}) ->
    case Queue of
        ask   -> Tester ! client_ack;
        ask_r -> ok
    end,
    {NState, Timeout} = sbroker_drop_queue:handle_in(SendTime, From, Value,
                                                     Time, State),
    {{Tester, Queue, NState}, Timeout}.

handle_out(Time, {Tester, Queue, State}) ->
    case sbroker_drop_queue:handle_out(Time, State) of
        {empty, NState} ->
            {empty, {Tester, Queue, NState}};
        {SendTime, From, Value, MRef, NState, Timeout} ->
            case Queue of
                ask_r -> Tester ! client_ack;
                ask   -> ok
            end,
            {SendTime, From, Value, MRef, {Tester, Queue, NState}, Timeout}
    end.

handle_timeout(Time, {Tester, Queue, State}) ->
    {NState, Timeout} = sbroker_drop_queue:handle_timeout(Time, State),
    {{Tester, Queue, NState}, Timeout}.

handle_cancel(Tag, Time, {Tester, Queue, State}) ->
    {Reply, NState, Timeout} = sbroker_drop_queue:handle_cancel(Tag, Time,
                                                                State),
    {Reply, {Tester, Queue, NState}, Timeout}.

handle_info(Msg, Time, {Tester, Queue, State}) ->
    {NState, Timeout} = sbroker_drop_queue:handle_info(Msg, Time, State),
    {{Tester, Queue, NState}, Timeout}.

code_change(_, _, _, _) ->
    exit(enosup).

config_change(_, _, _) ->
    exit(enosup).

len({_, _, State}) ->
    sbroker_drop_queue:len(State).

send_time({_, _, State}) ->
    sbroker_drop_queue:send_time(State).

terminate(Reason, {_, _, State}) ->
    sbroker_drop_queue:terminate(Reason, State).
