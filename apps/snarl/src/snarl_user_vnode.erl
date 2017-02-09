-module(snarl_user_vnode).
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
         sync_repair/4,
         import/4,
         repair/4,
         add_key/4,
         add_yubikey/4,
         remove_yubikey/4,
         delete/3,
         grant/4, revoke/4, revoke_prefix/4,
         join/4, leave/4,
         passwd/4,
         join_org/4, leave_org/4, select_org/4,
         revoke_key/4,
         set_metadata/4,
         add_token/4,
         remove_token/4
        ]).

-ignore_xref([
              add_yubikey/4,
              remove_yubikey/4,
              add/4,
              sync_repair/4,
              add_key/4,
              find_key/3,
              delete/3,
              get/3,
              grant/4,
              import/4,
              join/4,
              leave/4,
              passwd/4,
              repair/4,
              revoke/4,
              revoke_key/4,
              revoke_prefix/4,
              set_metadata/4,
              handle_info/2,
              join_org/4, leave_org/4, select_org/4,
              start_vnode/1
             ]).


-define(SERVICE, snarl_user).

-define(MASTER, snarl_user_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(Key, Obj) ->
    snarl_vnode:hash_object(Key, Obj).

aae_repair(Realm, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    snarl_user:get(Realm, Key).

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

add(Preflist, ReqID, {Realm, UUID}, User) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, {Realm, UUID}, User},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add_key(Preflist, ReqID, {Realm, UUID}, {KeyId, Key}) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_key, ReqID, {Realm, UUID}, KeyId, Key},
                                   {fsm, undefined, self()},
                                   ?MASTER).

revoke_key(Preflist, ReqID, {Realm, UUID}, KeyId) ->
    riak_core_vnode_master:command(Preflist,
                                   {revoke_key, ReqID, {Realm, UUID}, KeyId},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add_yubikey(Preflist, ReqID, {Realm, UUID}, OTP) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_yubikey, ReqID, {Realm, UUID}, OTP},
                                   {fsm, undefined, self()},
                                   ?MASTER).

remove_yubikey(Preflist, ReqID, {Realm, UUID}, KeyId) ->
    riak_core_vnode_master:command(Preflist,
                                   {remove_yubikey, ReqID, {Realm, UUID},
                                    KeyId},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add_token(Preflist, ReqID, {Realm, UUID}, Token) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_token, ReqID, {Realm, UUID}, Token},
                                   {fsm, undefined, self()},
                                   ?MASTER).

remove_token(Preflist, ReqID, {Realm, UUID}, Token) ->
    riak_core_vnode_master:command(Preflist,
                                   {remove_token, ReqID, {Realm, UUID}, Token},
                                   {fsm, undefined, self()},
                                   ?MASTER).

set_metadata(Preflist, ReqID, {Realm, UUID}, Attributes) ->
    riak_core_vnode_master:command(Preflist,
                                   {set_metadata, ReqID, {Realm, UUID},
                                    Attributes},
                                   {fsm, undefined, self()},
                                   ?MASTER).

import(Preflist, ReqID, {Realm, UUID}, Import) ->
    riak_core_vnode_master:command(Preflist,
                                   {import, ReqID, {Realm, UUID}, Import},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, E) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, E},
                                   {fsm, undefined, self()},
                                   ?MASTER).

passwd(Preflist, ReqID, {Realm, UUID}, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {password, ReqID, {Realm, UUID}, Val},
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

join_org(Preflist, ReqID, {Realm, UUID}, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {join_org, ReqID, {Realm, UUID}, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

leave_org(Preflist, ReqID, {Realm, UUID}, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {leave_org, ReqID, {Realm, UUID}, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

select_org(Preflist, ReqID, {Realm, UUID}, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {select_org, ReqID, {Realm, UUID}, Val},
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
    snarl_vnode:init(Part, <<"user">>, ?SERVICE, ?MODULE, ft_user).

handle_overload_command(_Req, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload}).

handle_overload_info(_, _Idx) ->
    ok.

%%%===================================================================
%%% General
%%%===================================================================

handle_command({add, {ReqID, Coordinator}=ID, {Realm, UUID}, User}, _Sender,
               State) ->
    User0 = ft_user:new(ID),
    User1 = ft_user:name(ID, User, User0),
    User2 = ft_user:uuid(ID, UUID, User1),
    User3 = ft_user:grant(ID, [<<"users">>, UUID, <<"...">>], User2),
    UserObj = ft_obj:new(User3, Coordinator),
    snarl_vnode:put(Realm, UUID, UserObj, State),
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

handle_coverage({find_key, Realm, KeyID}, _KeySpaces, Sender, State) ->
    ID = snarl_vnode:mkid(findkey),
    FoldFn = fun (UUID, O, [not_found]) ->
                     U0 = ft_obj:val(O),
                     U1 = ft_user:load(ID, U0),
                     Ks = ft_user:keys(U1),
                     try
                         Ks1 = [key_to_id(K) || {_, K} <- Ks],
                         case lists:member(KeyID, Ks1) of
                             true ->
                                 [UUID];
                             _ ->
                                 [not_found]
                         end
                     catch
                         Exception:Reason ->
                             lager:warning("[key_find:~s] ~p:~p -> ~p",
                                           [UUID, Exception, Reason, Ks]),
                             [not_found]
                     end;
                 (_U, _, Res) ->
                     Res
             end,
    Prefix = snarl_vnode:mk_pfx(Realm, State),
    snarl_vnode:fold(Prefix, FoldFn, [not_found], Sender, State);

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

key_to_id(Key) ->
    case re:split(Key, " ") of
        [_, ID0, _] ->
            ID1 = base64:decode(ID0),
            crypto:hash(md5, ID1);
        _ ->
            <<>>
    end.
