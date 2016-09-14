-module(snarl_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    RoleVMaster = {snarl_role_vnode_master,
                   {riak_core_vnode_master, start_link, [snarl_role_vnode]},
                   permanent, 5000, worker, [riak_core_vnode_master]},

    UserVMaster = {snarl_user_vnode_master,
                   {riak_core_vnode_master, start_link, [snarl_user_vnode]},
                   permanent, 5000, worker, [riak_core_vnode_master]},

    ClientVMaster = {snarl_client_vnode_master,
                     {riak_core_vnode_master, start_link, [snarl_client_vnode]},
                     permanent, 5000, worker, [riak_core_vnode_master]},

    WriteFSMs = {snarl_entity_write_fsm_sup,
                 {snarl_entity_write_fsm_sup, start_link, []},
                 permanent, infinity, supervisor, [snarl_entity_write_fsm_sup]},

    TokenVMaster = {snarl_token_vnode_master,
                    {riak_core_vnode_master, start_link, [snarl_token_vnode]},
                    permanent, 5000, worker, [riak_core_vnode_master]},

    OrgVMaster = {snarl_org_vnode_master,
                  {riak_core_vnode_master, start_link, [snarl_org_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},

    AccountingVMaster = {snarl_accounting_vnode_master,
                         {riak_core_vnode_master, start_link,
                          [snarl_accounting_vnode]},
                         permanent, 5000, worker, [riak_core_vnode_master]},

    S2iVMaster = {snarl_2i_vnode_master,
                  {riak_core_vnode_master, start_link, [snarl_2i_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},

    ReadFSMs = {snarl_entity_read_fsm_sup,
                {snarl_entity_read_fsm_sup, start_link, []},
                permanent, infinity, supervisor, [snarl_entity_read_fsm_sup]},

    AccountingFSMs = {snarl_accounting_read_fsm_sup,
                      {snarl_accounting_read_fsm_sup, start_link, []},
                      permanent, infinity, supervisor,
                      [snarl_accounting_read_fsm_sup]},

    riak_core_entropy_info:create_table(),

    EntropyManagerUser =
        {snarl_user_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [snarl_user, snarl_user_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerClient =
        {snarl_client_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [snarl_client, snarl_client_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerRole =
        {snarl_role_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [snarl_role, snarl_role_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerOrg =
        {snarl_org_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [snarl_org, snarl_org_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerS2i =
        {snarl_s2i_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [snarl_s2i, snarl_s2i_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerAccounting =
        {snarl_accounting_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [snarl_accounting, snarl_accounting_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},


    VNodeMasters = [RoleVMaster, UserVMaster, TokenVMaster, OrgVMaster,
                    S2iVMaster, ClientVMaster, AccountingVMaster],
    FSMs = [ReadFSMs, WriteFSMs, AccountingFSMs],
    AAE = [EntropyManagerUser, EntropyManagerRole, EntropyManagerOrg,
           EntropyManagerClient, EntropyManagerS2i, EntropyManagerAccounting],
    AdditionalServices =
        [{snarl_init, {snarl_init, start_link, []},
          permanent, 5000, worker, []}],
    spawn(fun delay_mdns_anouncement/0),
    {ok,
     {{one_for_one, 5, 10},
      VNodeMasters ++ FSMs ++ AAE ++ AdditionalServices
     }}.

%% We delay the service anouncement, first we wait
%% for sniffle to start to make sure the riak
%% core services call returns all needed services
%% then we'll go through each of the services
%% wait for startup.
%% Once they are started we enable the mdns.

delay_mdns_anouncement() ->
    riak_core:wait_for_application(snarl),
    Services = riak_core_node_watcher:services(),
    delay_mdns_anouncement(Services).
delay_mdns_anouncement([]) ->
    lager:info("[mdns] Enabling mDNS annoucements."),
    mdns_server_fsm:start();
delay_mdns_anouncement([S | R]) ->
    riak_core:wait_for_service(S),
    delay_mdns_anouncement(R).
