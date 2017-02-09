-module(snarl_client_vnode).
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
         handle_info/2
       ]).

-export([
         master/0,
         aae_repair/2,
         hash_object/2
        ]).

%% Reads
-export([
         get/3
        ]).

%% Writes
-export([
         add/4,
         name/4,
         sync_repair/4,
         repair/4,
         add_uri/4,
         remove_uri/4,
         delete/3,
         grant/4, revoke/4, revoke_prefix/4,
         join/4, leave/4,
         secret/4,
         set_metadata/4
        ]).

-ignore_xref([
              add_uri/4,
              name/4,
              remove_uri/4,
              add/4,
              sync_repair/4,
              delete/3,
              get/3,
              grant/4,
              join/4,
              leave/4,
              secret/4,
              repair/4,
              revoke/4,
              revoke_prefix/4,
              set_metadata/4,
              handle_info/2,
              start_vnode/1
             ]).


-define(SERVICE, snarl_client).

-define(MASTER, snarl_client_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(Key, Obj) ->
    snarl_vnode:hash_object(Key, Obj).

aae_repair(Realm, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    snarl_client:get(Realm, Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


repair(IdxNode, {Realm, UUID}, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, {Realm, UUID}, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================g

get(Preflist, ReqID, {Realm, UUID}) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, {Realm, UUID}},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================


sync_repair(Preflist, ReqID, {Realm, UUID}, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   {sync_repair, ReqID, {Realm, UUID}, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add(Preflist, ReqID, {Realm, UUID}, Client) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, {Realm, UUID}, Client},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add_uri(Preflist, ReqID, {Realm, UUID}, URI) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_uri, ReqID, {Realm, UUID}, URI},
                                   {fsm, undefined, self()},
                                   ?MASTER).

remove_uri(Preflist, ReqID, {Realm, UUID}, URI) ->
    riak_core_vnode_master:command(Preflist,
                                   {remove_uri, ReqID, {Realm, UUID}, URI},
                                   {fsm, undefined, self()},
                                   ?MASTER).

set_metadata(Preflist, ReqID, {Realm, UUID}, Attributes) ->
    riak_core_vnode_master:command(Preflist,
                                   {set_metadata, ReqID, {Realm, UUID},
                                    Attributes},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, E) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, E},
                                   {fsm, undefined, self()},
                                   ?MASTER).

secret(Preflist, ReqID, {Realm, UUID}, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {secret, ReqID, {Realm, UUID}, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

name(Preflist, ReqID, {Realm, UUID}, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {name, ReqID, {Realm, UUID}, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

join(Preflist, ReqID, {Realm, UUID}, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {join, ReqID, {Realm, UUID}, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

leave(Preflist, ReqID, {Realm, UUID}, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {leave, ReqID, {Realm, UUID}, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

grant(Preflist, ReqID, {Realm, UUID}, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {grant, ReqID, {Realm, UUID}, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

revoke(Preflist, ReqID, {Realm, UUID}, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {revoke, ReqID, {Realm, UUID}, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

revoke_prefix(Preflist, ReqID, {Realm, UUID}, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {revoke_prefix, ReqID, {Realm, UUID}, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================
init([Part]) ->
    snarl_vnode:init(Part, <<"client">>, ?SERVICE, ?MODULE, ft_client).

handle_overload_command(_Req, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload}).

handle_overload_info(_, _Idx) ->
    ok.

%%%===================================================================
%%% General
%%%===================================================================

handle_command({add, {ReqID, Coordinator}=ID, {Realm, UUID}, ClientID}, _Sender,
               State) ->
    Client0 = ft_client:new(ID),
    Client1 = ft_client:client_id(ID, ClientID, Client0),
    Client2 = ft_client:name(ID, ClientID, Client1),
    Client3 = ft_client:uuid(ID, UUID, Client2),
    ClientObj = ft_obj:new(Client3, Coordinator),
    snarl_vnode:put(Realm, UUID, ClientObj, State),
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

encode_handoff_item({Realm, UUID}, Data) ->
    term_to_binary({{Realm, UUID}, Data}).

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

%%%===================================================================
%%% Internal Functions
%%%===================================================================
