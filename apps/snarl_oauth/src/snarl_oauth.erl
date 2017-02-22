-module(snarl_oauth).

-export([scope/1, scope/2, add_scope/3, delete_scope/2, add_permission/3,
         remove_permission/3, default/2, verify_scope/2]).

-ignore_xref([scope/1, scope/2, add_scope/3, delete_scope/2, add_permission/3,
              remove_permission/3, default/2, verify_scope/2]).

-spec scope(binary()) -> [fifo:scope_map()].
scope(Realm) ->
    case riak_core_metadata:get({<<"oauth">>, Realm}, <<"scope">>) of
        undefined ->
            [];
        Ss ->
            [update_scope(S) || S <- Ss]
    end.

-spec scope(binary(), binary()) -> [fifo:scope_map()].

scope(Realm, Subscope) ->
    Set = oauth2_priv_set:new(Subscope),
    AS = scope(Realm),
    [S || S = #{scope := Scope} <- AS,
        oauth2_priv_set:is_member(Scope, Set)].

add_scope(Realm, Scope, Description) ->
    Scopes = scope(Realm),
    Scopes1 = scopes_without(Scopes, Scope),
    Scopes2 = [#{scope => Scope, desc => Description,
                 default => false, permissions => []} | Scopes1],
    riak_core_metadata:put({<<"oauth">>, Realm}, <<"scope">>, Scopes2).

delete_scope(Realm, Scope) ->
    Scopes = scope(Realm),
    Scopes1 = scopes_without(Scopes, Scope),
    riak_core_metadata:put({<<"oauth">>, Realm}, <<"scope">>, Scopes1).

add_permission(Realm, Scope, Permission) ->
    Scopes = scope(Realm),
    case [S || S = #{scope := Name} <- Scopes, Name =:= Scope] of
        [] ->
            {error, not_found};
        [#{scope := Scope, desc := Description,
           default := Dflt, permissions := Permissions}] ->
            Scopes1 = scopes_without(Scopes, Scope),
            Permissions2 = [Permission|Permissions],
            Scopes2 = [{Scope, Description, Dflt, Permissions2} | Scopes1],
            riak_core_metadata:put({<<"oauth">>, Realm}, <<"scope">>, Scopes2)
    end.

remove_permission(Realm, Scope, Permission) ->
    Scopes = scope(Realm),
    case [S || S = #{scope := Name} <- Scopes, Name =:= Scope] of
        [] ->
            {error, not_found};
        [S = #{permissions := Permissions}] ->
            Scopes1 = scopes_without(Scopes, Scope),
            Permissions2 = [P || P <- Permissions, P =/= Permission],
            Scopes2 = [S#{permissions := Permissions2} | Scopes1],
            riak_core_metadata:put({<<"oauth">>, Realm}, <<"scope">>, Scopes2)
    end.



default(Realm, Scope) ->
    Scopes = scope(Realm),
    case [S || S = #{scope := Name} <- Scopes, Name =:= Scope] of
        [] ->
            {error, not_found};
        [#{scope := Scope, desc := Description,
           default := Dflt, permissions := Permissions}] ->
            Scopes1 = scopes_without(Scopes, Scope),
            Scopes2 = [{Scope, Description, not Dflt, Permissions} | Scopes1],
            riak_core_metadata:put({<<"oauth">>, Realm}, <<"scope">>, Scopes2)
    end.

verify_scope(Realm, Scope) ->
    snarl_oauth_backend:verify_scope(Realm, Scope).

update_scope({Scope, Description, Dflt, Permissions}) ->
    #{scope => Scope,
      desc => Description,
      default => Dflt,
      permissions => Permissions};
update_scope(S) ->
    S.

-spec scopes_without([fifo:scope_map()], binary()) ->
    [fifo:scope_map()].
scopes_without(Scopes, Scope) when is_binary(Scope) ->
    [S || S = #{scope := Name} <- Scopes, Name =/= Scope].
