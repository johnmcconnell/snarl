-module(snarl_client).
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include("snarl_ent.hrl").

-behaviour(snarl_indexed).
-behaviour(snarl_sync_element).

-export([
         sync_repair/3,
         list/0, list/1, list/3, list/4, list_/1,
         sync_key/1,
         auth/3,
         name/3,
         type/3,
         reindex/2,
         get/2, raw/2,
         lookup/2,
         add/2, add/3,
         delete/2,
         secret/3,
         join/3, leave/3,
         grant/3, revoke/3, revoke_prefix/3,
         allowed/3,
         set_metadata/3,
         add_uri/3, remove_uri/3, uris/2,
         wipe/2
        ]).

-ignore_xref([
              wipe/2,
              list_/1
             ]).

-define(TIMEOUT, 5000).
-define(MASTER,  snarl_client_vnode_master).
-define(VNODE,   snarl_client_vnode).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {snarl, client, Met},
          Mod, Fun, Args)).

-define(ID_2I, {?MODULE, id}).

reindex(Realm, UUID) ->
    case ?MODULE:get(Realm, UUID) of
        {ok, O} ->
            snarl_2i:add(Realm, ?ID_2I, ft_client:client_id(O), UUID),
            ok;
        E ->
            E
    end.

%% Public API
wipe(Realm, UUID) ->
    ?FM(wipe, ?COVERAGE, start,
        [?MASTER, ?MODULE, {wipe, Realm, UUID}]).

sync_repair(Realm, UUID, Obj) ->
    do_write(Realm, UUID, sync_repair, Obj).

sync_key(B) when is_binary(B) ->
    B.

-spec auth(Realm::binary(), Client::binary(), Secret::binary()) ->
                  not_found |
                  {error, timeout} |
                  {ok, Client::fifo:client_id()}.

auth(Realm, Client, Secret) ->
    case ?MODULE:lookup(Realm, Client) of
        {ok, ClientR} ->
            case check_pw(ClientR, Secret) of
                true ->
                    {ok, ft_client:uuid(ClientR)};
                _ ->
                    not_found
            end;
        E ->
            E
    end.

-spec lookup(Realm::binary(), Client::binary()) ->
                     not_found |
                     {error, timeout} |
                     {ok, Client::fifo:client()}.
lookup(Realm, Client) ->
    folsom_metrics:histogram_timed_update(
      {snarl, client, lookup},
      fun() ->
              case snarl_2i:get(Realm, ?ID_2I, Client) of
                  {ok, UUID} ->
                      ?MODULE:get(Realm, UUID);
                  R ->
                      R
              end
      end).

-spec revoke_prefix(Realm::binary(),
                    Client::fifo:client_id(),
                    Prefix::fifo:permission()) ->
                           not_found |
                           {error, timeout} |
                           ok.
revoke_prefix(Realm, Client, Prefix) ->
    do_write(Realm, Client, revoke_prefix, Prefix).

-spec allowed(Realm::binary(),
              Client::fifo:uuid(),
              Permission::fifo:permission()) ->
                     not_found |
                     {error, timeout} |
                     true | false.
allowed(Realm, Client, Permission) ->
    case ?MODULE:get(Realm, Client) of
        {ok, ClientObj} ->
            test_client(Realm, ClientObj, Permission);
        E ->
            E
    end.

add_uri(Realm, Client, URI) ->
    do_write(Realm, Client, add_uri, URI).

remove_uri(Realm, Client, KeyID) ->
    do_write(Realm, Client, remove_uri, KeyID).

uris(Realm, Client) ->
    case ?MODULE:get(Realm, Client) of
        {ok, ClientObj} ->
            {ok, ft_client:uris(ClientObj)};
        E ->
            E
    end.

-spec get(Realm::binary(), Client::fifo:client_id()) ->
                  not_found |
                  {error, timeout} |
                  {ok, Client::fifo:client()}.
get(Realm, Client) ->
    case ?FM(get, ?READ_FSM, start,
             [{?VNODE, ?MODULE}, get, {Realm, Client}]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

raw(Realm, Client) ->
    case ?FM(get, ?READ_FSM, start,
             [{?VNODE, ?MODULE}, get, {Realm, Client},
              undefined, true]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

list() ->
    ?FM(list_all, ?COVERAGE, start,
        [?MASTER, ?MODULE, list]).

-spec list(Realm::binary()) ->
                  {error, timeout} |
                  {ok, Clients::[fifo:client_id()]}.
list(Realm) ->
    ?FM(list, ?COVERAGE, start,
        [?MASTER, ?MODULE, {list, Realm}]).

list_(Realm) ->
    {ok, Res} =
        ?FM(list, ?COVERAGE, start,
            [?MASTER, ?MODULE,
             {list, Realm, [], true, true}]),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec list(Realm::binary(), [fifo:matcher()], boolean()) ->
                  {error, timeout} |
                  {ok, [{integer(), fifo:uuid() | ft_client:client()}]}.

list(Realm, Requirements, Full) ->
    {ok, Res} =
        ?FM(list, ?COVERAGE, full,
            [?MASTER, ?MODULE,
             Realm, Requirements]),
    Res1 = rankmatcher:apply_scales(Res),
    Res2 = case Full of
               true ->
                   Res1;
               false ->
                   [{P, ft_client:uuid(O)} || {P, O} <- Res1]
           end,
    {ok,  lists:sort(Res2)}.

-spec list(Realm::binary(), [fifo:matcher()],
           FoldFn::snal_coverage:fold_fun(), Acc0::term()) ->
                  {error, timeout} | {ok, term()}.

list(Realm, Requirements, FoldFn, Acc0) ->
    ?FM(list_all, ?COVERAGE, full,
        [?MASTER, ?MODULE, Realm, Requirements, FoldFn, Acc0]).


-spec add(Realm::binary(), Creator::fifo:client_id(),
          ClientID::binary()) ->
                 duplicate |
                 {error, timeout} |
                 {ok, UUID::fifo:client_id()}.

add(Realm, undefined, Client) ->
    UUID = fifo_utils:uuid(client),
    lager:info("[~p:create] Creation Started.", [UUID]),
    case create(Realm, UUID, Client) of
        {ok, UUID} ->
            lager:info("[~p:create] Created.", [UUID]),
            case snarl_opt:get(clients, Realm, initial_role) of
                undefined ->
                    lager:info("[~p:create] No default role.",
                               [UUID]),
                    ok;
                Grp ->
                    lager:info("[~p:create] Assigning default role: ~s.",
                               [UUID, Grp]),
                    join(Realm, UUID, Grp)
            end,
            {ok, UUID};
        E ->
            lager:error("[create] Failed to create: ~p.", [E]),
            E
    end;

add(Realm, Creator, Client) when is_binary(Creator),
                                 is_binary(Client) ->
    case add(Realm, undefined, Client) of
        {ok, UUID}->
            snarl_user:trigger(Realm, Creator, client_create, UUID);
        E ->
            E
    end.

add(Realm, Client) ->
    add(Realm, undefined, Client).

create(Realm, UUID, Client) ->
    case lookup(Realm, Client) of
        not_found ->
            ok = do_write(Realm, UUID, add, Client),
            snarl_2i:add(Realm, ?ID_2I, Client, UUID),
            {ok, UUID};
        {ok, _ClientObj} ->
            duplicate
    end.

-spec set_metadata(Realm::binary(), Client::fifo:client_id(),
                   Attirbutes::fifo:attr_list()) ->
                          not_found |
                          {error, timeout} |
                          ok.
set_metadata(Realm, Client, Attributes) ->
    do_write(Realm, Client, set_metadata, Attributes).

-spec secret(Realm::binary(), Client::fifo:client_id(), Secret::binary()) ->
                    not_found |
                    {error, timeout} |
                    ok.
secret(Realm, Client, Secret) ->
    H = case application:get_env(snarl, hash_fun) of
            {ok, sha512} ->
                Salt = crypto:rand_bytes(64),
                Hash = hash(sha512, Salt, Secret),
                {Salt, Hash};
            _ ->
                {ok, Salt} = bcrypt:gen_salt(),
                {ok, Hash} = bcrypt:hashpw(Secret, Salt),
                {bcrypt, list_to_binary(Hash)}
        end,
    do_write(Realm, Client, secret, H).


type(Realm, Client, Type) when Type =:= confidential;
                               Type =:= public ->
    do_write(Realm, Client, type, Type).

name(Realm, Client, Name) ->
    do_write(Realm, Client, name, Name).


-spec join(Realm::binary(), Client::fifo:client_id(), Role::fifo:role_id()) ->
                  not_found |
                  {error, timeout} |
                  ok.
join(Realm, Client, Role) ->
    case snarl_role:get(Realm, Role) of
        {ok, _} ->
            do_write(Realm, Client, join, Role);
        E ->
            E
    end.

-spec leave(Realm::binary(), Client::fifo:client_id(), Role::fifo:role_id()) ->
                   not_found |
                   {error, timeout} |
                   ok.
leave(Realm, Client, Role) ->
    do_write(Realm, Client, leave, Role).

-spec delete(Realm::binary(), Client::fifo:client_id()) ->
                    not_found |
                    {error, timeout} |
                    ok.
delete(Realm, Client) ->
    case ?MODULE:get(Realm, Client) of
        {ok, O} ->
            snarl_2i:delete(Realm, ?ID_2I, ft_client:client_id(O)),
            ok;
        E ->
            E
    end,
    spawn(
      fun () ->
              Prefix = [<<"clients">>, Client],
              {ok, Clients} = ?MODULE:list(Realm),
              [?MODULE:revoke_prefix(Realm, U, Prefix) || U <- Clients],
              {ok, Roles} = snarl_role:list(Realm),
              [snarl_role:revoke_prefix(Realm, R, Prefix) || R <- Roles]
      end),
    do_write(Realm, Client, delete).

-spec grant(Realm::binary(), Client::fifo:client_id(),
            Permission::fifo:permission()) ->
                   not_found |
                   {error, timeout} |
                   ok.
grant(Realm, Client, Permission) ->
    do_write(Realm, Client, grant, Permission).

-spec revoke(Realm::binary(), Client::fifo:client_id(),
             Permission::fifo:permission()) ->
                    not_found |
                    {error, timeout} |
                    ok.
revoke(Realm, Client, Permission) ->
    do_write(Realm, Client, revoke, Permission).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Realm, Client, Op) ->
    case ?FM(Op, ?WRITE_FSM, write,
             [{?VNODE, ?MODULE}, {Realm, Client}, Op]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

do_write(Realm, Client, Op, Val) ->
    case ?FM(Op, ?WRITE_FSM, write,
             [{?VNODE, ?MODULE}, {Realm, Client}, Op, Val]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

test_roles(_Realm, _Permission, []) ->
    false;

test_roles(Realm, Permission, [Role|Roles]) ->
    case snarl_role:get(Realm, Role) of
        {ok, RoleObj} ->
            case libsnarlmatch:test_perms(
                   Permission,
                   ft_role:ptree(RoleObj)) of
                true ->
                    true;
                false ->
                    test_roles(Realm, Permission, Roles)
            end;
        _ ->
            test_roles(Realm, Permission, Roles)
    end.

test_client(Realm, ClientObj, Permission) ->
    case libsnarlmatch:test_perms(
           Permission,
           ft_client:ptree(ClientObj)) of
        true ->
            true;
        false ->
            test_roles(Realm, Permission, ft_client:roles(ClientObj))
    end.

check_pw(ClientR, Secret) ->
    case ft_client:secret(ClientR) of
        {bcrypt, Hash} ->
            HashS = binary_to_list(Hash),
            case bcrypt:hashpw(Secret, Hash) of
                {ok, HashS} ->
                    true;
                _ ->
                    false
            end;
        {S, H} ->
            case hash(sha512, S, Secret) of
                H ->
                    true;
                _ ->
                    false
            end;
        %% Unset secrets are always false
        <<>> ->
            false
    end.

-ifndef(old_hash).
hash(Hash, Salt, Secret) ->
    crypto:hash(Hash, [Salt, Secret]).
-else.
hash(sha512, Salt, Secret) ->
    crypto:sha512([Salt, Secret]).
-endif.
