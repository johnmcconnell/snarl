%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  7 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(snarl_sync_tree).

-behaviour(gen_server).

%% API
-export([start_link/0, insert/5, done/3, delete/3, get_tree/0, update/4,
         get_tree/1, get_tree_data/1]).

-ignore_xref([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(RECHECK_IVAL, 1000*60*60).

-record(state, {tree=[], version=0}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
get_tree(_Service) ->
    whereis(snarl_sync_tree).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_tree() ->
    gen_server:call(?SERVER, get).

get_tree_data(Service) ->
    gen_server:call(?SERVER, {get, Service}).

delete(PID, System, ID) ->
    gen_server:cast(PID, {delete, System, ID}).

done(PID, System, Vsn) ->
    gen_server:cast(PID, {done, System, Vsn}).

insert(PID, System, Vsn, ID, H) ->
    gen_server:cast(PID, {insert, System, Vsn, ID, H}).

update(PID, System, ID, Obj) ->
    gen_server:cast(PID, {update, System, ID, Obj}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    IVal = case application:get_env(sync_build_interval) of
               {ok, IValX} ->
                   IValX;
               _ ->
                   ?RECHECK_IVAL
           end,
    timer:send_interval(IVal, update_tree),
    {ok, #state{}, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(get, _From, State = #state{tree = Tree}) ->
    Tree1 = [{{S, Id}, H} || {{S, Id}, {_, H}} <- Tree],
    {reply, {ok, Tree1}, State};
handle_call({get, Service}, _From, State = #state{tree = Tree}) ->
    Tree1 = [{{S, Id}, H} || {{S, Id}, {_, H}} <- Tree, S =:= Service],
    {reply, {ok, Tree1}, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({update, Sys, ID, Obj}, State = #state{version =Vsn}) ->
    {noreply, update_tree(Sys, ID, snarl_sync:hash(ID, Obj), Vsn-1, State)};
handle_cast({delete, Sys, ID}, State = #state{tree = Tree}) ->
    Tree1 = [E || E = {{S, Id}, _} <- Tree, S =/= Sys andalso ID =/= Id],
    {noreply, State#state{tree=Tree1}};
handle_cast({done, Sys, Version}, State = #state{tree = Tree}) ->
    Tree1 = [E || E = {{S, _}, {V, _}} <- Tree, V >= Version orelse S =/= Sys],
    {noreply, State#state{tree=Tree1}};
handle_cast({insert, Sys, Vsn, ID, Hash}, State) ->
    {noreply, update_tree(Sys, ID, Hash, Vsn, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(update_tree, State = #state{version = Vsn}) ->
    Vsn1 = Vsn + 1,
    case snarl_user:list() of
        {ok, Us} ->
            snarl_sync_read_fsm:update(snarl_user, Vsn1, Us, self());
        _ ->
            ok
    end,
    case snarl_role:list() of
        {ok, Gs} ->
            snarl_sync_read_fsm:update(snarl_role, Vsn1, Gs, self());
        _ ->
            ok
    end,
    case snarl_org:list() of
        {ok, Os} ->
            snarl_sync_read_fsm:update(snarl_org, Vsn1, Os, self());
        _ ->
            ok
    end,
    case snarl_accounting:list() of
        {ok, Accs} ->
            snarl_sync_read_fsm:update(snarl_accounting, Vsn1, Accs, self());
        _ ->
            ok
    end,
    {noreply, State#state{version = Vsn1}};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
update_tree(Sys, ID, H, Vsn, State = #state{tree=Tree}) ->
    Tree1 = orddict:store({Sys, ID}, {Vsn, H}, Tree),
    State#state{tree=Tree1}.
