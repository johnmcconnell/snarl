-module(snarl_2i).

-include_lib("riak_core/include/riak_core_vnode.hrl").
-include("snarl_ent.hrl").

-behaviour(snarl_sync_element).

-export([
         sync_repair/3,
         sync_key/1,
         list/0, list/1, list/3, list_/1,
         get/2, get/3, raw/2, raw/3,
         add/4, delete/3, delete/2,
         wipe/3, reindex/2
        ]).

-ignore_xref([
              wipe/3, raw/3,
              list/0, list/1, list/3, list_/1,
              create/3, raw/2, sync_repair/3,
              import/3, lookup/2, reindex/2
             ]).

-define(TIMEOUT, 5000).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {snarl, s2i, Met},
          Mod, Fun, Args)).

%% Public API

reindex(_, _) -> ok.

wipe(Realm, Type, Key) ->
    TK = sync_key({Type, Key}),
    ?FM(wipe, ?COVERAGE, start,
        [snarl_2i_vnode_master, snarl_2i, {wipe, Realm, TK}]).

sync_repair(Realm, TK, Obj) ->
    do_write(Realm, TK, sync_repair, Obj).

sync_key(B) when is_binary(B) ->
    B;
sync_key({_, _} = T) ->
    term_to_binary(T).


get(Realm, Type, Key) ->
    get(Realm, sync_key({Type, Key})).

-spec get(Realm::binary(), TK::binary()) ->
                 not_found |
                 {error, timeout} |
                 {ok, Target::fifo:uuid()}.
get(Realm, TK) ->
    case ?FM(get, snarl_entity_read_fsm, start,
             [{snarl_2i_vnode, snarl_2i}, get, {Realm, TK}]) of
        {ok, not_found} ->
            not_found;
        {ok, R} ->
            case snarl_2i_state:target(R) of
                not_found ->
                    not_found;
                UUID ->
                    {ok, UUID}
            end
    end.

raw(Realm, Type, Key) ->
    raw(Realm, sync_key({Type, Key})).

raw(Realm, TK) ->
    case ?FM(get, snarl_entity_read_fsm, start,
             [{snarl_2i_vnode, snarl_2i}, get, {Realm, TK}, undefined, true]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

list() ->
    ?FM(list_all, ?COVERAGE, start,
        [snarl_2i_vnode_master, snarl_2i, list]).

-spec list(Realm::binary()) -> {ok, [binary()]} |
                               not_found |
                               {error, timeout}.

list(Realm) ->
    {ok, Res} = ?FM(list, ?COVERAGE, start,
                  [snarl_2i_vnode_master, snarl_2i, {list, Realm}]),
    Res1 = [binary_to_term(R) || R <- Res],
    {ok,  Res1}.

list_(Realm) ->
    {ok, Res} =
        ?FM(list, ?COVERAGE, start,
            [snarl_2i_vnode_master, snarl_2i,
             {list, Realm, [], true, true}]),
    Res1 = [binary_to_term(R) || {_, R} <- Res],
    {ok,  Res1}.


%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list(Realm::binary(), [fifo:matcher()], boolean()) ->
                  {error, timeout} | {ok, [fifo:uuid()]}.

list(Realm, Requirements, Full)
  when Full == true orelse Full == false ->
    {ok, Res} =
        ?FM(list, ?COVERAGE, start,
            [snarl_2i_vnode_master, snarl_2i,
             {list, Realm, Requirements, Full}]),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

-spec add(Realm::binary(), Type::term(), K::binary(), Target::fifo:uuid()) ->
                 {ok, UUID::binary()} |
                 douplicate |
                 {error, timeout}.

add(Realm, Type, Key, Target) ->
    lager:info("[2i] Adding 2i key ~s/~p-~p -> ~s", [Realm, Type, Key, Target]),
    TK = sync_key({Type, Key}),
    Res = case snarl_2i:get(Realm, TK) of
              not_found ->
                  ok;
              {ok, not_found} ->
                  ok;
              {ok, _} ->
                  douplicate
          end,
    case Res of
        ok ->
            do_write(Realm, TK, add, Target);
        E ->
            {error, E}
    end.

-spec delete(Realm::binary(), Type::term(), Key::binary()) ->
                    ok |
                    not_found|
                    {error, timeout}.

delete(Realm, Type, Key) ->
    TK = sync_key({Type, Key}),
    delete(Realm, TK).

delete(Realm, TK) ->
    do_write(Realm, TK, delete).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Realm, Key, Op) ->
    ?FM(Op, snarl_entity_write_fsm, write,
        [{snarl_2i_vnode, snarl_2i}, {Realm, Key}, Op]).

do_write(Realm, Key, Op, Val) ->
    ?FM(Op, snarl_entity_write_fsm, write,
        [{snarl_2i_vnode, snarl_2i}, {Realm, Key}, Op, Val]).
