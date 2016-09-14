%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2013, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 30 Dec 2013 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(snarl_sync_sup).

-behaviour(supervisor).

%% API
-export([start_child/2, start_link/0]).

-ignore_xref([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

start_child(IP, Port) ->
    supervisor:start_child(?SERVER, [IP, Port]).

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    Services =
        case snarl_sync:enabled() of
            true ->
                [{snarl_sync_worker_sup,
                  {snarl_sync_worker_sup, start_link, []},
                  permanent, 5000, supervisor, []},
                 {snarl_sync_read_sup, {snarl_sync_read_sup, start_link, []},
                  permanent, 5000, supervisor, []},
                 {snarl_sync_exchange_sup,
                  {snarl_sync_exchange_sup, start_link, []},
                  permanent, 5000, supervisor, []},
                 {snarl_sync_tree, {snarl_sync_tree, start_link, []},
                  permanent, 5000, worker, []}];
            _ ->
                []
        end,

    {ok,
     {{one_for_one, 5, 10},
      Services
     }}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
