-module(snarl_2i_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3,
         handle_overload_command/3,
         handle_overload_info/2,
         handle_info/2]).

-export([
         master/0,
         aae_repair/2,
         hash_object/2
        ]).

%% Reads
-export([get/3]).

%% Writes
-export([add/4,
         delete/3,
         repair/4, sync_repair/4]).

-ignore_xref([
              start_vnode/1,
              get/3,
              add/4,
              delete/3,
              repair/4, sync_repair/4,
              handle_info/2
             ]).

-define(SERVICE, snarl_2i).

-define(MASTER, snarl_2i_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object({Type, Key}, Obj) ->
    TK = term_to_binary({Type, Key}),
    hash_object(TK, Obj);

hash_object(TK, Obj) when is_binary(TK) ->
    snarl_vnode:hash_object(TK, Obj).

aae_repair(Realm, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    snarl_2i:get(Realm, Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


repair(IdxNode, TK, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, TK, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Key},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

sync_repair(Preflist, ReqID, UUID, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   {sync_repair, ReqID, UUID, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add(Preflist, ReqID, Key, Target) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, Key, Target},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Key},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================
init([Part]) ->
    snarl_vnode:init(Part, <<"2i">>, ?SERVICE, ?MODULE, snarl_2i_state).

handle_overload_command(_Req, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload}).

handle_overload_info(_, _Idx) ->
    ok.

%%%===================================================================
%%% General
%%%===================================================================

handle_command({add, {ReqID, Coordinator} = ID, {Realm, Key}, Target},
               _Sender, State) ->
    S2i = snarl_2i_state:new(ID),
    S2i1 = snarl_2i_state:target(ID, Target, S2i),
    S2i1Obj = ft_obj:new(S2i1, Coordinator),
    snarl_vnode:put(Realm, Key, S2i1Obj, State),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _} = ID, {Realm, Key}},
               _Sender, State) ->
    snarl_vnode:change(Realm, Key, target, [not_found], ID, State),
    {reply, {ok, ReqID}, State};

handle_command(Message, Sender, State) ->
    snarl_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{} = FR, Sender, State) ->
    handle_command(FR, Sender, State);

handle_handoff_command({get, _ReqID, _Vm} = Req, Sender, State) ->
    handle_command(Req, Sender, State);

handle_handoff_command(Req, Sender, State) ->
    S1 = case handle_command(Req, Sender, State) of
             {noreply, NewState} ->
                 NewState;
             {reply, _, NewState} ->
                 NewState
         end,
    {forward, S1}.

handoff_starting(TargetNode, State) ->
    lager:warning("Starting handof to: ~p", [TargetNode]),
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    snarl_vnode:handle_handoff_data(Data, State).


encode_handoff_item(Role, Data) ->
    term_to_binary({Role, Data}).

is_empty(State) ->
    snarl_vnode:is_empty(State).

delete(State) ->
    snarl_vnode:delete(State).

handle_coverage(Req, KeySpaces, Sender, State) ->
    snarl_vnode:handle_coverage(Req, KeySpaces, Sender, State).

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(Msg, State) ->
    snarl_vnode:handle_info(Msg, State).
