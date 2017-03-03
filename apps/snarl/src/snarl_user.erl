-module(snarl_user).
-include_lib("snarl_oauth/include/snarl_oauth.hrl").
-include("snarl_ent.hrl").

-behaviour(snarl_indexed).
-behaviour(snarl_sync_element).

-export([
         trigger/4,
         sync_repair/3,
         sync_key/1,
         list/0,
         list/1,
         list_/1,
         list/3,
         list/4,
         auth/4,
         auth/3,
         reindex/2,
         find_key/2,
         get/2, raw/2,
         lookup/2,
         add/2, add/3,
         delete/2,
         passwd/3,
         join/3, leave/3,
         join_org/3, leave_org/3, select_org/3,
         grant/3, revoke/3, revoke_prefix/3,
         allowed/3,
         set_metadata/3,
         import/3,
         cache/2,
         add_token/8, add_token/9, remove_token/3,
         add_key/4, revoke_key/3,
         add_yubikey/3, remove_yubikey/3, check_yubikey/3,
         wipe/2,
         revoke_token/3
        ]).

-ignore_xref([
              import/3,
              wipe/2,
              list_/1
             ]).

-define(TIMEOUT, 5000).
-define(MASTER,  snarl_user_vnode_master).
-define(VNODE,   snarl_user_vnode).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {snarl, user, Met},
          Mod, Fun, Args)).

-define(NAME_2I, {?MODULE, name}).
-define(KEY_2I,  {?MODULE, key}).

%% Revokes a token by a given tokenID (not the token itself!)
revoke_token(Realm, User, TokenID) ->
    Ctx = #{realm => Realm, user => User},
    case ?MODULE:get(Realm, User) of
        {ok, U} ->
            case ft_user:get_token_by_id(TokenID, U) of
                #{token := T, type := access} ->
                    {ok, _} = snarl_oauth_backend:revoke_access_token(T, Ctx),
                    ok;
                #{token := T, type := refresh} ->
                    {ok, _} = snarl_oauth_backend:revoke_refresh_token(T, Ctx),
                    ok;
                _ ->
                    not_found
            end;
        E ->
            E
    end.

reindex(Realm, UUID) ->
    case ?MODULE:get(Realm, UUID) of
        {ok, O} ->
            snarl_2i:add(Realm, ?NAME_2I, ft_user:name(O), UUID),
            [snarl_2i:add(Realm, ?KEY_2I, key_to_id(K), UUID) ||
                {_, K} <- ft_user:keys(O)],
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

-spec find_key(Realm::binary(), KeyID::binary()) ->
                      not_found |
                      {error, timeout} |
                      {ok, User::fifo:user_id()}.
find_key(Realm, KeyID) ->
    folsom_metrics:histogram_timed_update(
      {snarl, user, find_key},
      snarl_2i, get, [Realm, ?KEY_2I, KeyID]).


auth(Realm, User, Passwd) ->
    case lookup(Realm, User) of
        {ok, UserR} ->
            case check_pw(UserR, Passwd) of
                true ->
                    {ok, ft_user:uuid(UserR)};
                _ ->
                    not_found
            end;
        E ->
            E
    end.

-spec auth(Realm::binary(), User::binary(), Passwd::binary(),
           OTP::binary()|undefined) ->
                  not_found |
                  {otp_required, yubikey, UUID::binary()} |
                  {error, timeout} |
                  {ok, User::fifo:user_id()}.

auth(Realm, User, Passwd, OTP) ->
    case lookup(Realm, User) of
        {ok, UserR} ->
            case check_pw(UserR, Passwd) of
                true ->
                    check_yubikey(UserR, OTP);
                _ ->
                    not_found
            end;
               E ->
            E
    end.

check_yubikey(Realm, UUID, OTP) ->
    case ?MODULE:get(Realm, UUID) of
        {ok, User} ->
            check_yubikey(User, OTP);
        E ->
            E
    end.

check_yubikey(User, undefined) ->
    UUID = ft_user:uuid(User),
    case ft_user:yubikeys(User) of
        [] ->
            {ok, UUID};
        _ ->
            {otp_required, yubikey, UUID}
    end;

check_yubikey(User, OTP) ->
    UUID = ft_user:uuid(User),
    case ft_user:yubikeys(User) of
        [] ->
            {ok, UUID};
        Ks ->
            YID = snarl_yubico:id(OTP),
            validate_key(UUID, OTP, YID, Ks)
    end.

validate_key(UUID, _OTP, <<>>, _Ks) ->
    {otp_required, yubikey, UUID};
validate_key(UUID, OTP, YID, Ks) ->
    case lists:member(YID, Ks) of
        false ->
            not_found;
        true ->
            case snarl_yubico:verify(OTP) of
                {auth, ok} ->
                    {ok, UUID};
                _ ->
                    not_found
            end
    end.

-spec lookup(Realm::binary(), User::binary()) ->
                    not_found |
                    {error, timeout} |
                    {ok, User::fifo:user()}.
lookup(Realm, User) ->
    folsom_metrics:histogram_timed_update(
      {snarl, user, lookup},
      fun() ->
              case snarl_2i:get(Realm, ?NAME_2I, User) of
                  {ok, UUID} ->
                      ?MODULE:get(Realm, UUID);
                  R ->
                      R
              end
      end).

add_token(Realm, User, TokenID, Type, Token, Expiery, Client, Scope) ->
    add_token(Realm, User, TokenID, Type, Token, Expiery, Client, Scope,
              undefined).

add_token(Realm, User, TokenID, Type, Token, Expiery, Client, Scope, Comment) ->
    do_write(Realm, User, add_token,
             {TokenID, Type, Token, Expiery, Client, Scope, Comment}).

remove_token(Realm, User, Token) ->
    do_write(Realm, User, remove_token, Token).


-spec revoke_prefix(Realm::binary(),
                    User::fifo:user_id(),
                    Prefix::fifo:permission()) ->
                           not_found |
                           {error, timeout} |
                           ok.
revoke_prefix(Realm, User, Prefix) ->
    do_write(Realm, User, revoke_prefix, Prefix).

-spec allowed(Realm::binary(),
              User::fifo:uuid(),
              Permission::fifo:permission()) ->
                     not_found |
                     {error, timeout} |
                     true | false.
allowed(Realm, User, Permission) ->
    case ?MODULE:get(Realm, User) of
        {ok, UserObj} ->
            test_user(Realm, UserObj, Permission);
        E ->
            E
    end.

add_key(Realm, User, KeyID, Key) ->
    SSHID = key_to_id(Key),
    case snarl_2i:get(Realm, ?KEY_2I, SSHID) of
        {ok, _} ->
            {error, duplicate};
        _ ->
            case do_write(Realm, User, add_key, {KeyID, Key}) of
                ok ->
                    snarl_2i:add(Realm, ?KEY_2I, SSHID, User),
                    ok;
                E ->
                    E
            end
    end.

revoke_key(Realm, User, KeyID) ->
    case ?MODULE:get(Realm, User) of
        {ok, U} ->
            Ks = ft_user:keys(U),
            case jsxd:get([KeyID], Ks) of
                {ok, Key} ->
                    snarl_2i:delete(Realm, ?KEY_2I, key_to_id(Key)),
                    do_write(Realm, User, revoke_key, KeyID);
                _ ->
                    ok
            end;
        E ->
            E
    end.

add_yubikey(Realm, User, OTP) ->
    KeyID = snarl_yubico:id(OTP),
    do_write(Realm, User, add_yubikey, KeyID).

remove_yubikey(Realm, User, KeyID) ->
    do_write(Realm, User, remove_yubikey, KeyID).



-spec cache(Realm::binary(), User::fifo:user_id()) ->
                   not_found |
                   {error, timeout} |
                   {ok, Perms::[fifo:permission()]}.
cache(Realm, User) ->
    case ?MODULE:get(Realm, User) of
        {ok, UserObj} ->
            {ok, collect_permissions(Realm, UserObj)};
        E ->
            E
    end.

collect_permissions(Realm, UserObj) ->
    lists:foldl(
      fun(Role, Permissions) ->
              case snarl_role:get(Realm, Role) of
                  {ok, RoleObj} ->
                      GrPerms = ft_role:ptree(RoleObj),
                      libsnarlmatch_tree:merge(Permissions, GrPerms);
                  _ ->
                      Permissions
              end
      end,
      ft_user:ptree(UserObj),
      ft_user:roles(UserObj)).

-spec get(Realm::binary(), User::fifo:user_id()) ->
                 not_found |
                 {error, timeout} |
                 {ok, User::fifo:user()}.
get(Realm, User) ->
    case ?FM(get, ?READ_FSM, start,
             [{?VNODE, ?MODULE}, get, {Realm, User}]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

raw(Realm, User) ->
    case ?FM(get, ?READ_FSM, start,
             [{?VNODE, ?MODULE}, get, {Realm, User}, undefined,
              true]) of
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
                  {ok, Users::[fifo:user_id()]}.
list(Realm) ->
    ?FM(list, ?COVERAGE , start,
        [?MASTER, ?MODULE, {list, Realm}]).

list_(Realm) ->
    {ok, Res} =
        ?FM(list, ?COVERAGE, raw,
            [?MASTER, ?MODULE,
             Realm, []]),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec list(Realm::binary(), [fifo:matcher()], boolean()) ->
                  {error, timeout} |
                  {ok, [{integer(), fifo:uuid() | ft_user:user()}]}.

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
                   [{P, ft_user:uuid(O)} || {P, O} <- Res1]
           end,
    {ok,  lists:sort(Res2)}.

-spec list(Realm::binary(), [fifo:matcher()],
           FoldFn::snal_coverage:fold_fun(), Acc0::term()) ->
                  {error, timeout} | {ok, term()}.

list(Realm, Requirements, FoldFn, Acc0) ->
    ?FM(list_all, ?COVERAGE, full,
        [?MASTER, ?MODULE, Realm, Requirements, FoldFn, Acc0]).

-spec add(Realm::binary(), Creator::fifo:user_id(),
          UserName::binary()) ->
                 duplicate |
                 {error, timeout} |
                 {ok, UUID::fifo:user_id()}.

add(Realm, undefined, User) ->
    UUID = fifo_utils:uuid(user),
    lager:info("[~p:create] Creation Started.", [UUID]),
    case create(Realm, UUID, User) of
        {ok, UUID} ->
            lager:info("[~p:create] Created.", [UUID]),
            case snarl_opt:get(users, Realm, initial_role) of
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

add(Realm, Creator, User) when is_binary(Creator),
                               is_binary(User) ->
    case add(Realm, undefined, User) of
        {ok, UUID} ->
            trigger(Realm, Creator, user_create, UUID);
        E ->
            E
    end.

trigger(Realm, User, Action, UUID) ->
    case ?MODULE:get(Realm, User) of
        {ok, C} ->
            case ft_user:active_org(C) of
                <<>> ->
                    lager:info("[~s:create] Creator ~s has no "
                               "active organisation.",
                               [UUID, User]),
                    {ok, UUID};
                Org ->
                    lager:info("[~s:create] Triggering ~s for organisation ~s",
                                       [UUID, Action, Org]),
                    snarl_org:trigger(Realm, Org, Action, UUID),
                    {ok, UUID}
            end;
        E ->
            lager:warning("[~s:create] Failed to get acting user ~s: ~p.",
                          [UUID, User, E]),
            {ok, UUID}
    end.

add(Realm, User) ->
    add(Realm, undefined, User).

create(Realm, UUID, User) ->
    case lookup(Realm, User) of
        not_found ->
            ok = do_write(Realm, UUID, add, User),
            snarl_2i:add(Realm, ?NAME_2I, User, UUID),
            {ok, UUID};
        {ok, _UserObj} ->
            duplicate
    end.

-spec set_metadata(Realm::binary(), User::fifo:user_id(),
                   Attirbutes::fifo:attr_list()) ->
                          not_found |
                          {error, timeout} |
                          ok.
set_metadata(Realm, User, Attributes) ->
    do_write(Realm, User, set_metadata, Attributes).

-spec passwd(Realm::binary(), User::fifo:user_id(), Passwd::binary()) ->
                    not_found |
                    {error, timeout} |
                    ok.
passwd(Realm, User, Passwd) ->
    H = case application:get_env(snarl, hash_fun) of
            {ok, sha512} ->
                Salt = crypto:strong_rand_bytes(64),
                Hash = hash(sha512, Salt, Passwd),
                {Salt, Hash};
            _ ->
                {ok, Salt} = bcrypt:gen_salt(),
                {ok, Hash} = bcrypt:hashpw(Passwd, Salt),
                {bcrypt, list_to_binary(Hash)}
        end,
    do_write(Realm, User, passwd, H).

import(Realm, User, Data) ->
    do_write(Realm, User, import, Data).

-spec join(Realm::binary(), User::fifo:user_id(), Role::fifo:role_id()) ->
                  not_found |
                  {error, timeout} |
                  ok.
join(Realm, User, Role) ->
    case snarl_role:get(Realm, Role) of
        {ok, _} ->
            do_write(Realm, User, join, Role);
        E ->
            E
    end.

-spec leave(Realm::binary(), User::fifo:user_id(), Role::fifo:role_id()) ->
                   not_found |
                   {error, timeout} |
                   ok.
leave(Realm, User, Role) ->
    do_write(Realm, User, leave, Role).


-spec join_org(Realm::binary(), User::fifo:user_id(), Org::fifo:org_id()) ->
                      not_found |
                      {error, timeout} |
                      ok.
join_org(Realm, User, Org) ->
    case snarl_org:get(Realm, Org) of
        {ok, _} ->
            do_write(Realm, User, join_org, Org);
        E ->
            E
    end.

-spec select_org(Realm::binary(), User::fifo:user_id(),
                 Org::fifo:org_id() | <<>>) ->
                        not_found |
                        {error, timeout} |
                        ok.
select_org(Realm, User, Org) ->
    case ?MODULE:get(Realm, User) of
        {ok, UserObj} ->
            Orgs = ft_user:orgs(UserObj),
            case lists:member(Org, Orgs) of
                true ->
                    do_write(Realm, User, select_org, Org);
                _ ->
                    not_found
            end;
        R  ->
            R
    end.

-spec leave_org(Realm::binary(), User::fifo:user_id(), Org::fifo:org_id()) ->
                       not_found |
                       {error, timeout} |
                       ok.
leave_org(Realm, User, Org) ->
    case ?MODULE:get(Realm, User) of
        {ok, UserObj} ->
            case ft_user:active_org(UserObj) of
                Org ->
                    do_write(Realm, User, select_org, <<"">>);
                _ ->
                    ok
            end,
            do_write(Realm, User, leave_org, Org);
        R  ->
            R
    end.

-spec delete(Realm::binary(), User::fifo:user_id()) ->
                    not_found |
                    {error, timeout} |
                    ok.
delete(Realm, User) ->
    case ?MODULE:get(Realm, User) of
        {ok, O} ->
            snarl_2i:delete(Realm, ?NAME_2I, ft_user:name(O)),
            [snarl_2i:delete(Realm, ?KEY_2I, key_to_id(K)) ||
                {_, K} <- ft_user:keys(O)],
            ok;
        E ->
            E
    end,
    spawn(
      fun () ->
              Prefix = [<<"users">>, User],
              {ok, Users} = ?MODULE:list(Realm),
              [?MODULE:revoke_prefix(Realm, U, Prefix) || U <- Users],
              {ok, Roles} = snarl_role:list(Realm),
              [snarl_role:revoke_prefix(Realm, R, Prefix) || R <- Roles],
              {ok, Orgs} = snarl_org:list(Realm),
              [snarl_org:remove_target(Realm, O, User) || O <- Orgs]
      end),
    do_write(Realm, User, delete).

-spec grant(Realm::binary(), User::fifo:user_id(),
            Permission::fifo:permission()) ->
                   not_found |
                   {error, timeout} |
                   ok.
grant(Realm, User, Permission) ->
    do_write(Realm, User, grant, Permission).

-spec revoke(Realm::binary(), User::fifo:user_id(),
             Permission::fifo:permission()) ->
                    not_found |
                    {error, timeout} |
                    ok.
revoke(Realm, User, Permission) ->
    do_write(Realm, User, revoke, Permission).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Realm, User, Op) ->
    case ?FM(Op, ?WRITE_FSM, write,
             [{?VNODE, ?MODULE}, {Realm, User}, Op]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

do_write(Realm, User, Op, Val) ->
    case ?FM(Op, ?WRITE_FSM, write,
             [{?VNODE, ?MODULE}, {Realm, User}, Op, Val]) of
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

test_user(Realm, UserObj, Permission) ->
    case libsnarlmatch:test_perms(
           Permission,
           ft_user:ptree(UserObj)) of
        true ->
            true;
        false ->
            test_roles(Realm, Permission, ft_user:roles(UserObj))
    end.

check_pw(UserR, Passwd) ->
    case ft_user:password(UserR) of
        {bcrypt, Hash} ->
            HashS = binary_to_list(Hash),
            case bcrypt:hashpw(Passwd, Hash) of
                {ok, HashS} ->
                    true;
                _ ->
                    false
            end;
        {S, H} ->
            case hash(sha512, S, Passwd) of
                H ->
                    true;
                _ ->
                    false
            end;
        %% Unset passwords are always false
        <<>> ->
            false
    end.

hash(Hash, Salt, Passwd) ->
    crypto:hash(Hash, [Salt, Passwd]).

key_to_id(Key) ->
    [_, ID0, _] = re:split(Key, " "),
    ID1 = base64:decode(ID0),
    crypto:hash(md5, ID1).
