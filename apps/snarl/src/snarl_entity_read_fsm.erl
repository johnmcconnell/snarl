%% @doc The coordinator for stat get operations.  The key here is to
%% generate the preflist just like in wrtie_fsm and then query each
%% replica and wait until a quorum is met.
-module(snarl_entity_read_fsm).
-behavior(gen_fsm).
-include("snarl.hrl").


-include_lib("snarl_dtrace/include/snarl_dtrace.hrl").

%% API
-export([start_link/7, start/3, start/4, start/5]).


-export([reconcile/1, different/1, needs_repair/2, repair/4, unique/1]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2, wait_for_n/2, finalize/2]).

-ignore_xref([
              code_change/4,
              different/1,
              execute/2,
              finalize/2,
              handle_event/3,
              handle_info/3,
              handle_sync_event/4,
              init/1,
              needs_repair/2,
              prepare/2,
              reconcile/1,
              repair/4,
              start/4,
              start_link/7,
              terminate/3,
              unique/1,
              wait_for_n/2,
              waiting/2,
              start/3, start/5
             ]).

-record(state, {raw = false ::boolean(),
                req_id,
                from,
                entity,
                has_send = false,
                op,
                r,
                n,
                preflist,
                num_r=0,
                size,
                timeout = 10000,
                val,
                vnode :: atom(),
                bucket,
                system :: atom(),
                replies = []}).

-type state() :: #state{}.



%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, {VNode, System}, Op, From, {Realm, Entity}, Val, Raw) ->
    gen_fsm:start_link(?MODULE, [ReqID, {VNode, System}, Op, From,
                                 {Realm, Entity}, Val, Raw], []).

start(VNodeInfo, Op, {Realm, Entity}) ->
    start(VNodeInfo, Op, {Realm, Entity}, undefined).

start(VNodeInfo, Op, {Realm, Entity}, Val) ->
    start(VNodeInfo, Op, {Realm, Entity}, Val, false).

start(VNodeInfo, Op, {Realm, Entity}, Val, Raw) ->
    ReqID = snarl_vnode:mkid(),
    snarl_entity_read_fsm_sup:start_read_fsm(
      [ReqID, VNodeInfo, Op, self(), {Realm, Entity}, Val, Raw]
     ),
    receive
        {ReqID, ok} ->
            ok;
        {ReqID, ok, Result} ->
            {ok, Result}
    after ?DEFAULT_TIMEOUT ->
            {error, timeout}
    end.

%%%===================================================================
%%% States
%%%===================================================================

-spec init(_) -> {ok, prepare, state(), 0}.
init([ReqId, {VNode, System}, Op, From, {Realm, Entity}, Val, Raw]) when
      is_atom(VNode),
      is_atom(System) ->
    ?DT_READ_ENTRY(Entity, Op),
    SD = #state{raw=Raw,
                bucket=Realm,
                req_id=ReqId,
                from=From,
                op=Op,
                n= ?N,
                r= ?R,
                val=Val,
                vnode=VNode,
                system=System,
                entity=Entity},
    {ok, prepare, SD, 0};

init([ReqId, {VNode, System}, Op, From, {Realm, Entity}]) when
      is_atom(VNode),
      is_atom(System) ->
    ?DT_READ_ENTRY(Entity, Op),
    SD = #state{req_id=ReqId,
                bucket=Realm,
                from=From,
                op=Op,
                vnode=VNode,
                system=System,
                entity=Entity},
    {ok, prepare, SD, 0}.

%% @doc Calculate the Preflist.
prepare(timeout, SD0=#state{entity=Entity,
                            bucket=Realm,
                            n = N,
                            system=System}) ->
    DocIdx = riak_core_util:chash_key({Realm, Entity}),
    Prelist = riak_core_apl:get_apl(DocIdx, N, System),
    SD = SD0#state{preflist=Prelist},
    {next_state, execute, SD, 0}.

%% @doc Execute the get reqs.
execute(timeout, SD0=#state{req_id=ReqId,
                            bucket=Realm,
                            entity=Entity,
                            op=Op,
                            val=Val,
                            vnode=VNode,
                            preflist=Prelist}) ->
    case Val of
        undefined ->
            VNode:Op(Prelist, ReqId, {Realm, Entity});
        _ ->
            VNode:Op(Prelist, ReqId, {Realm, Entity}, Val)
    end,
    {next_state, waiting, SD0}.

%% @doc Wait for R replies and then respond to From (original client
%% that called `snarl:get/2').
%% TODO: read repair...or another blog post?

waiting({ok, ReqID, IdxNode, Obj},
        SD0=#state{from=From, num_r=NumR0, replies=Replies0,
                   r=R, n = N, timeout=Timeout}) ->
    NumR = NumR0 + 1,
    Replies = [{IdxNode, Obj}|Replies0],
    SD = SD0#state{num_r=NumR, replies=Replies},
    if
        NumR >= R ->
            SD1 = case merge(Replies) of
                      not_found ->
                          %% We do not want to return not_found unless we have
                          %% all the replies
                          %% TODO: is this a good idea?
                          SD;
                      Merged ->
                          ?DT_READ_FOUND_RETURN(SD0#state.entity, SD0#state.op),
                          Reply = case SD#state.raw of
                                      false ->
                                          ft_obj:val(Merged);
                                      true ->
                                          Merged
                                  end,
                          From ! {ReqID, ok, Reply},
                          SD#state{has_send = true}
                  end,
            if
                %% If we have all repliesbut do not send yet it's not found
                NumR =:= N andalso not SD1#state.has_send ->
                    From ! {ReqID, ok, not_found},
                    {next_state, finalize, SD1#state{has_send = true}, 0};
                %% If we have all replies and also send we're good
                NumR =:= N ->
                    {next_state, finalize, SD1, 0};
                %% If we do not have all replies and not send yet we want
                %% to keep waiting
                not SD1#state.has_send ->
                    {next_state, waiting, SD1};
                %% If we have not all replies but already send the messag
                %% on we've found a object and now can wait for the rest.
                true ->
                    {next_state, wait_for_n, SD1, Timeout}
            end;
        true ->
            {next_state, waiting, SD}
    end.

wait_for_n({ok, _ReqID, IdxNode, Obj},
           SD0=#state{n = N, num_r=NumR, replies=Replies0})
  when NumR =:= N - 1 ->
    Replies = [{IdxNode, Obj}|Replies0],
    {next_state, finalize, SD0#state{num_r=N, replies=Replies}, 0};

wait_for_n({ok, _ReqID, IdxNode, Obj},
           SD0=#state{num_r=NumR0, replies=Replies0, timeout=Timeout}) ->
    NumR = NumR0 + 1,
    Replies = [{IdxNode, Obj}|Replies0],
    {next_state, wait_for_n, SD0#state{num_r=NumR, replies=Replies}, Timeout};

%% TODO partial repair?
wait_for_n(timeout, SD) ->
    {stop, timeout, SD}.

finalize(timeout, SD=#state{
                        vnode=VNode,
                        replies=Replies,
                        bucket=Realm,
                        entity=Entity}) ->
    MObj = merge(Replies),
    case needs_repair(MObj, Replies) of
        true ->
            lager:warning("[read] performing read repair on '~p'(~p) <- ~p.",
                          [Entity, MObj, Replies]),
            repair(VNode, {Realm, Entity}, MObj, Replies),
            {stop, normal, SD};
        false ->
            {stop, normal, SD}
    end.

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @pure
%%
%% @doc Given a list of `Replies' return the merged value.
-spec merge([vnode_reply()]) -> fifo:obj() | not_found.
merge(Replies) ->
    Objs = [Obj || {_, Obj} <- Replies],
    ft_obj:merge(snarl_entity_read_fsm, Objs).

%% @pure
%%
%% @doc Reconcile conflicts among conflicting values.
-type entity_list() ::
        [fifo:user()] |
        [fifo:org()] |
        [fifo:role()].
-spec reconcile(entity_list()) ->
                       fifo:user() | user:role() | fifo:org().

reconcile([V | Vs]) ->
    case {ft_user:is_a(V),
          ft_client:is_a(V),
          ft_role:is_a(V),
          ft_org:is_a(V)} of
        {true, _, _, _} ->
            reconcile_user(Vs, V);
        {_, true, _, _} ->
            reconcile_client(Vs, V);
        {_, _, true, _} ->
            reconcile_role(Vs, V);
        {_, _, _, true} ->
            reconcile_org(Vs, V);
        _ ->
            V
    end.

reconcile_role([G | R], Acc) ->
    reconcile_role(R, ft_role:merge(Acc, G));
reconcile_role(_, Acc) ->
    Acc.

reconcile_user([U | R], Acc) ->
    reconcile_user(R, ft_user:merge(Acc, U));
reconcile_user(_, Acc) ->
    Acc.

reconcile_client([U | R], Acc) ->
    reconcile_client(R, ft_client:merge(Acc, U));
reconcile_client(_, Acc) ->
    Acc.

reconcile_org([U | R], Acc) ->
    reconcile_org(R, ft_org:merge(Acc, U));
reconcile_org(_, Acc) ->
    Acc.

%% @pure
%%
%% @doc Given the merged object `MObj' and a list of `Replies'
%% determine if repair is needed.
-spec needs_repair(any(), [vnode_reply()]) -> boolean().
needs_repair(MObj, Replies) ->
    Objs = [Obj || {_, Obj} <- Replies],
    lists:any(different(MObj), Objs).

%% @pure
different(A) -> fun(B) -> not ft_obj:equal(A, B) end.

%% @impure
%%
%% @doc Repair any vnodes that do not have the correct object.
-spec repair(atom(), {binary(), binary()}, fifo:obj(), [vnode_reply()]) -> io.
repair(_, _, _, []) -> io;

repair(VNode, StatName, MObj, [{IdxNode, Obj}|T]) ->
    case ft_obj:equal(MObj, Obj) of
        true ->
            repair(VNode, StatName, MObj, T);
        false ->
            case Obj of
                not_found ->
                    VNode:repair(IdxNode, StatName, not_found, MObj);
                _ ->
                    VNode:repair(IdxNode, StatName, ft_obj:vclock(Obj), MObj)
            end,
            repair(VNode, StatName, MObj, T)
    end.

%% pure
%%
%% @doc Given a list return the set of unique values.
-spec unique([A::any()]) -> [A::any()].
unique(L) ->
    sets:to_list(sets:from_list(L)).
