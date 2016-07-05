-module(snarl_token).
-include("snarl.hrl").
-include_lib("snarl_oauth/include/snarl_oauth.hrl").

-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         get/2,
         add/2, add/3, add/4,
         delete/2,
         reindex/2,
         api_token/4,
         manual_token/5,
         ssl_cert_token/5
        ]).

-ignore_xref([
              reindex/2
             ]).


-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {snarl, token, Met},
          Mod, Fun, Args)).

%% Public API
reindex(_, _) -> ok.

-spec get(binary(), fifo:token()) ->
                 not_found |
                 {ok, fifo:user_id()}.
get(Realm, Token) ->
    R = ?FM(get, snarl_entity_read_fsm, start,
            [{snarl_token_vnode, snarl_token}, get, {Realm, Token}]),
    case R of
        {ok, not_found} ->
            not_found;
        {ok, {_Exp, Value}} ->
            {ok, Value}
    end.

api_token(Realm, User, Scope, Comment) ->
    case snarl_oauth_backend:verify_scope(Realm, Scope) of
        false ->
            {error, bad_scope};
        true ->
            case snarl_user:get(Realm, User) of
                {ok, _UserObj} ->
                    %% This os mostly copied from:
                    %% snarl_oauth:associate_access_token/3
                    AccessToken = oauth2_token:generate([]),
                    TokenID = fifo_utils:uuid(),
                    Expiery = infinity,
                    Client = undefined,
                    Context = [{<<"client">>, Client},
                               {<<"resource_owner">>, User},
                               {<<"expiry_time">>, Expiery},
                               {<<"scope">>, Scope}],
                    Type = access,
                    add(Realm,
                        {?ACCESS_TOKEN_TABLE, AccessToken},
                        Expiery,
                        Context),
                    snarl_user:add_token(
                      Realm, User, TokenID, Type, AccessToken, Expiery, Client,
                      Scope, Comment),
                    {ok, {TokenID, AccessToken}};
                E ->
                    E
            end
    end.

manual_token(Realm, User, Scope, Comment, Token) ->
    case snarl_oauth_backend:verify_scope(Realm, Scope) of
        false ->
            {error, bad_scope};
        true ->
            case snarl_user:get(Realm, User) of
                {ok, _UserObj} ->
                    %% This os mostly copied from:
                    %% snarl_oauth:associate_access_token/3
                    TokenID = fifo_utils:uuid(),
                    Expiery = infinity,
                    Client = undefined,
                    Context = [{<<"client">>, Client},
                               {<<"resource_owner">>, User},
                               {<<"expiry_time">>, Expiery},
                               {<<"scope">>, Scope}],
                    Type = access,
                    add(Realm,
                        {?ACCESS_TOKEN_TABLE, Token},
                        Expiery,
                        Context),
                    snarl_user:add_token(
                      Realm, User, TokenID, Type, Token, Expiery, Client,
                      Scope, Comment),
                    {ok, {TokenID, Token}};
                E ->
                    E
            end
    end.

ssl_cert_token(Realm, User, Scope, Comment, CSR) ->
    case snarl_oauth_backend:verify_scope(Realm, Scope) of
        false ->
            {error, bad_scope};
        true ->
            case snarl_user:get(Realm, User) of
                {ok, _UserObj} ->
                    %% This os mostly copied from:
                    %% snarl_oauth:associate_access_token/3
                    {ok, Days} = application:get_env(snarl, cert_validity),
                    {ok, Cert} = esel:sign_csr(Days, CSR),
                    Fingerprint = esel_cert:fingerprint(Cert),
                    Token = Fingerprint,
                    TokenID = fifo_utils:uuid(),
                    Expiery = Days*24*60*60 + erlang:system_time(seconds),
                    Client = undefined,
                    Context = [{<<"client">>, Client},
                               {<<"resource_owner">>, User},
                               {<<"expiry_time">>, Expiery},
                               {<<"scope">>, Scope}],
                    Type = access,
                    add(Realm,
                        {?ACCESS_TOKEN_TABLE, Token},
                        Expiery,
                        Context),
                    snarl_user:add_token(Realm, User, TokenID, Type, Token,
                                         Expiery, Client, Scope, Comment),
                    {ok, {TokenID, Cert}};
                E ->
                    E
            end
    end.

add(Realm, User) ->
    add(Realm, oauth2_token:generate([]), default, User).


add(Realm, Timeout, User) ->
    add(Realm, oauth2_token:generate([]), Timeout, User).

add(Realm, Token, Timeout, User) ->
    case do_write(Realm, Token, add, {Timeout, User}) of
        {ok, Token} ->
            {ok, Token};
        E ->
            lager:warning("[token:~s/~s] Erroor ~p.", [Realm, User, E]),
            E
    end.

delete(Realm, Token) ->
    lager:debug("[token:~s] deleted token ~s.", [Realm, Token]),
    do_write(Realm, Token, delete).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Realm, Token, Op) ->
    ?FM(Op, snarl_entity_write_fsm, write,
        [{snarl_token_vnode, snarl_token}, {Realm, Token}, Op]).

do_write(Realm, Token, Op, Val) ->
    ?FM(Op, snarl_entity_write_fsm, write,
        [{snarl_token_vnode, snarl_token}, {Realm, Token}, Op, Val]).
