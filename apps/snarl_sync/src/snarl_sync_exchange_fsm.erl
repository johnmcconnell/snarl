%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc FSM to syncronize differences for the remote DC replication
%%%                             ┌────────┐
%%%                             │   not  │
%%%                             ▼  empty │
%%%  ┌───────────┐        ┌───────────┐  │
%%%  │   comp    │───────▶│ sync_diff │──┘
%%%  └───────────┘        └───────────┘
%%%                      empty  │
%%%                             │
%%%         ┌───────────────────┘
%%%         │                   ┌────────┐
%%%         │                   │    not │
%%%         ▼       empty       ▼   empty│
%%%   ┌───────────┐       ┌───────────┐  │
%%%┌─▶│    get    │──────▶│   push    │──┘
%%%│  └───────────┘       └───────────┘
%%%│  not   │                   │
%%%│ empty  │                   │  empty
%%%└────────┘                   │
%%%                             │
%%%                             ▼
%%%                       ┌───────────┐
%%%                       │   done    │
%%%                       └───────────┘
%%% @end
%%% Created :  7 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(snarl_sync_exchange_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/5, start/5]).

-ignore_xref([start_link/5, sync_diff/2, sync_get/2, sync_push/2]).

%% gen_fsm callbacks
-export([init/1, sync_diff/2, sync_get/2, sync_push/2, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {ip, port, socket, diff, get, push, timeout}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start(_IP, _Port, [], [], []) ->
    ok;

start(IP, Port, Diff, Get, Push) ->
    snarl_sync_exchange_sup:start_child(IP, Port, Diff, Get, Push).

start_link(IP, Port, Diff, Get, Push) ->
    gen_fsm:start_link(?MODULE, [IP, Port, Diff, Get, Push], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([IP, Port, Diff, Get, Push]) ->
    Timeout = case application:get_env(sync_recv_timeout) of
                  {ok, T} ->
                      T;
                  _ ->
                      1500
              end,
    State = #state{ip=IP, port=Port, diff=Diff, get=Get, push=Push,
                   timeout=Timeout},
    case gen_tcp:connect(IP, Port,
                         [binary, {active, false}, {packet, 4}],
                         Timeout) of
        {ok, Socket} ->
            lager:info("[sync-exchange] Connected to: ~s:~p.", [IP, Port]),
            {ok, sync_diff, State#state{socket=Socket}, 0};
        E ->
            lager:error("[sync-exchange] Initialization failed: ~p.", [E]),
            {stop, connection}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Syncs a difference between an object that exists both locally and
%% remote.
%%
%% @end
%%--------------------------------------------------------------------

%% We have to treat the accounting part differently since there is no ft_obj
%% involved:

sync_diff(
  _, State = #state{
                socket=Socket,
                timeout=Timeout,
                diff=[{Sys = snarl_accounting, {Realm, UUID}}|R]}) ->
    lager:debug("[sync-exchange] Diff: ~p", [{Sys, {Realm, UUID}}]),
    case gen_tcp:send(Socket, term_to_binary({raw, Sys, Realm, UUID})) of
        ok ->
            case gen_tcp:recv(Socket, 0, Timeout) of
                {error, E} ->
                    error_stop(recv, E, State);
                {ok, RBin} ->
                    repair_accounting(Realm, UUID, RBin),
                    {next_state, sync_diff, State#state{diff=R}, 0}
            end;
        E ->
            error_stop(send_raw, E, State)
    end;

%% This still goes for the rest
sync_diff(_, State = #state{
                        socket=Socket,
                        timeout=Timeout,
                        diff=[{Sys, {Realm, UUID}}|R]}) ->
    lager:debug("[sync-exchange:acc] Diff: ~p", [{Sys, {Realm, UUID}}]),
    case gen_tcp:send(Socket, term_to_binary({raw, Sys, Realm, UUID})) of
        ok ->
            case gen_tcp:recv(Socket, 0, Timeout) of
                {error, E} ->
                    error_stop(recv, E, State);
                {ok, RBin} ->
                    repair(Sys, Realm, UUID, RBin, R, State)
            end;
        E ->
            error_stop(send_raw, E, State)
    end;

sync_diff(_, State = #state{diff=[]}) ->
    {next_state, sync_get, State, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Syncs a difference between an object that exists remotely but not
%% locally. For this we simply read the object and repair it locally
%% only.
%%
%% @end
%%--------------------------------------------------------------------

sync_get(_, State = #state{
                       socket=Socket,
                       timeout=Timeout,
                       get=[{Sys = snarl_accounting, {Realm, UUID}}|R]}) ->
    lager:debug("[sync-exchange:acc] Get: ~p", [{Sys, {Realm, UUID}}]),
    case gen_tcp:send(Socket, term_to_binary({raw, Sys, Realm, UUID})) of
        ok ->
            case gen_tcp:recv(Socket, 0, Timeout) of
                {error, E} ->
                    error_stop(recv, E, State);
                {ok, RBin} ->
                    repair_accounting(Realm, UUID, RBin),
                    {next_state, sync_get, State#state{get=R}, 0}
            end;
        E ->
            error_stop(send_raw, E, State)
    end;

sync_get(_, State = #state{
                       socket=Socket,
                       timeout=Timeout,
                       get=[{Sys, {Realm, UUID}}|R]}) ->
    lager:debug("[sync-exchange] Get: ~p", [{Sys, {Realm, UUID}}]),
    case gen_tcp:send(Socket, term_to_binary({raw, Sys, Realm, UUID})) of
        ok ->
            case gen_tcp:recv(Socket, 0, Timeout) of
                {error, E} ->
                    error_stop(recv, E, State);
                {ok, Bin} ->
                    local_repair(Sys, Realm, UUID, Bin),
                    {next_state, sync_get, State#state{get=R}, 0}
            end;
        E ->
            error_stop(send_raw, E, State)
    end;

sync_get(_, State = #state{get=[]}) ->
    {next_state, sync_push, State, 0}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Syncs a difference between an object that exists locally but not
%% remotely. For this we simply write the object locally too, as that
%% will be propagated to all nodes.
%%
%% @end
%%--------------------------------------------------------------------

sync_push(_, State = #state{
                        socket=Socket,
                        timeout=Timeout,
                        push=[{Sys = snarl_accounting, {Realm, UUID}}|R]}) ->
    lager:debug("[sync-exchange:acc] Push: ~p", [{Sys, {Realm, UUID}}]),
    case gen_tcp:send(Socket, term_to_binary({raw, Sys, Realm, UUID})) of
        ok ->
            case gen_tcp:recv(Socket, 0, Timeout) of
                {error, E} ->
                    error_stop(recv, E, State);
                {ok, RBin} ->
                    repair_accounting(Realm, UUID, RBin),
                    {next_state, sync_push, State#state{push=R}, 0}
            end;
        E ->
            error_stop(send_raw, E, State)
    end;

sync_push(_, State = #state{push=[{Sys, {Realm, UUID}}|R]}) ->
    lager:debug("[sync-exchange] Push: ~p", [{Sys, {Realm, UUID}}]),
    case snarl_sync_element:raw(Sys, Realm, UUID) of
        {ok, Obj} ->
            local_repair(Sys, Realm, UUID, Obj),
            {next_state, sync_push, State#state{push=R}, 0};
        not_found ->
            {next_state, sync_push, State#state{push=R}, 0}
    end;

sync_push(_, State = #state{push=[]}) ->
    {stop, normal, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Repair functions
%%%===================================================================

repair(Sys, Realm, UUID, RBin, R, State) ->
    case binary_to_term(RBin) of
        {ok, RObj} ->
            case snarl_sync_element:raw(Sys, Realm, UUID) of
                {ok, LObj} ->
                    Objs = [RObj, LObj],
                    Merged = ft_obj:merge(snarl_entity_read_fsm, Objs),
                    %% We set node() not {remote, node()} sot this repair
                    %% is propagated
                    NVS = {node(), vnode(Sys), Sys},
                    snarl_entity_write_fsm:write(NVS, {Realm, UUID},
                                                 sync_repair, Merged),
                    {next_state, sync_diff, State#state{diff=R}, 0};
                _ ->
                    {next_state, sync_diff, State#state{diff=R}, 0}
            end;
        not_found ->
            {next_state, sync_diff, State#state{diff=R}, 0}
    end.

local_repair(Sys, Realm, UUID, Bin) when is_binary(Bin) ->
    case binary_to_term(Bin) of
        {ok, Obj} ->
            local_repair(Sys, Realm, UUID, Obj);
        not_found ->
            local_repair(Sys, Realm, UUID, not_found)
    end;

local_repair(Sys, Realm, UUID, not_found) ->
    NVS = {node(), vnode(Sys), Sys},
    snarl_entity_write_fsm:write(NVS, {Realm, UUID}, delete, undefined);

local_repair(Sys, Realm, UUID, Obj) ->
    NVS = {node(), vnode(Sys), Sys},
    lager:debug("[sync-exchange] repairing(~p): ~p", [NVS, Obj]),
    snarl_entity_write_fsm:write(NVS, {Realm, UUID}, sync_repair, Obj).

error_stop(Reason, Error, State) ->
    lager:error("[sync-exchange] Error: ~p", [Error]),
    {stop, Reason, State}.

%%%===================================================================
%%% Accounting related functions
%%%===================================================================

repair_accounting(Realm, UUID, RBin) ->
    RObj = case binary_to_term(RBin) of
               {ok, RObjx}  ->
                   RObjx;
               not_found ->
                   []
           end,
    case snarl_accounting:raw(Realm, UUID) of
        {ok, LObj} ->
            %% If we have local missing we update
            %% our local code first.
            repair_accounting_diff(Realm, UUID, RObj, LObj);
        _ ->
            repair_accounting_diff(Realm, UUID, RObj, [])
    end.

repair_accounting_diff(Realm, UUID, RObj, LObj) ->
    Diff =  ordsets:subtract(RObj, LObj)  ++ ordsets:subtract(LObj, RObj),
    snarl_accounting:sync_repair(Realm, UUID, Diff).

%%%===================================================================
%%% Internal functions
%%%===================================================================

vnode(snarl_2i) -> snarl_2i_vnode;
vnode(snarl_role) -> snarl_role_vnode;
vnode(snarl_user) -> snarl_user_vnode;
vnode(snarl_client) -> snarl_client_vnode;
vnode(snarl_accounting) -> snarl_accounting_vnode;
vnode(snarl_org) -> snarl_org_vnode.
