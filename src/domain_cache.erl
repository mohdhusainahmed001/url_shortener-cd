-module(domain_cache).

-behaviour(gen_server).

-include("onex.hrl").

-export([
  start_link/0,
  stop/0,
  is_valid_domain/2
]).
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2
]).

-define(DOMAIN_CACHE_REFRESH_TIME, 30000).    %1min
-define(PG_SAVE_INTERVAL, 30000).
-define(RETRY_PG_SAVE_INTERVAL, 5000).
-define(HTTP_TIMEOUT, 5000).

%%%=================================================================================
%%%                                     API
%%%=================================================================================

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
  gen_server:stop(?MODULE).

is_valid_domain(TucId, DomainName) ->
  case ets:lookup(?DOMAIN_CACHE, TucId) of
    []   ->
      false;
    [{_, DomainList}] ->
      case util:add_protocols_to_domain(DomainName) of
        {HttpDomain, HttpsDomain} ->
          case lists:member(HttpsDomain, DomainList) of
            true -> {true, HttpsDomain};
            false ->
              case lists:member(HttpDomain, DomainList) of
                true -> {true, HttpDomain};
                false -> false
              end
          end;
        {OnlyDomain} ->
          case lists:member(OnlyDomain, DomainList) of
            true -> {true, OnlyDomain};
            false -> false
          end
      end
  end.


%%%=================================================================================
%%%                            GENSERVER  CALL BACKS
%%%=================================================================================

init([]) ->
  case ets:lookup(?DOMAIN_CACHE, all_domains) of
    [] ->
      ?oxlog_w("No domains found in cache during init~n", []),
      shorturl_supervisor:start_link([]),
      erlang:send_after(100, self(), refresh_all_domains);
    [{all_domains, Domains}] ->
      ?oxlog_i("Found ~p domains in cache during init~n", [length(Domains)]),
      shorturl_supervisor:start_link(Domains),
      erlang:send_after(?DOMAIN_CACHE_REFRESH_TIME, self(), refresh_all_domains)
  end,
  {ok, #{}}.

handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(start_queues, State) ->
  case ets:lookup(?DOMAIN_CACHE, all_domains) of
    [] ->
      refresh_domains(),
      ?oxlog_w("No domains found in cache during start_queues~n", []),
      erlang:send_after(?RETRY_PG_SAVE_INTERVAL, self(), start_queues);
    [{all_domains, Domains}] ->
      shorturl_supervisor:start_link(Domains)
  end,
  {noreply, State};

handle_info(refresh_all_domains, State) ->
  refresh_domains(),
  erlang:send_after(?DOMAIN_CACHE_REFRESH_TIME, self(), refresh_all_domains),
  {noreply, State};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

%%%=================================================================================
%%%                             INTERNAL FUNCTIONS
%%%=================================================================================
%%%

refresh_domains() ->
 case util:get_auth_ip_port() of
    {error, Reason} ->
      ?oxlog_e("Failed to get Auth IP and PORT:- ~p~n", [Reason]),
      ok;
    {AuthIp, AuthPort} ->
      Endpoint = lists:concat(["http://", AuthIp, ":", AuthPort, "/get_domain_tucidwise"]),
      case fetch_domains_from_db(Endpoint) of
        {error, Reason} ->
          ?oxlog_e("Failed to fetch Domains from DB:- ~p~n", [Reason]),
          ok;
        Resp ->
          store_domains(Resp)
      end
  end.

fetch_domains_from_db(Endpoint) ->
  case httpc:request(get, {Endpoint, []},
                      [{ssl, [{verify, verify_none}]}, {timeout, ?HTTP_TIMEOUT}],
                      [{body_format, binary}]) of
    {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} ->
      extract_domains(json:decode(Body));
    {ok, {{_Version, StatusCode, ReasonPhrase}, _Headers, _Body}} ->
      {error, {http_error, StatusCode, ReasonPhrase}};
    {error, Reason} ->
      {error, {request_failed, Reason}}
  end.

extract_domains(Map) ->
    DomainsMap = maps:get(<<"domains">>, Map, #{}),
    IdDomainsList =
        [ {Id, DomainList}
        || {Id, DomainList} <- maps:to_list(DomainsMap)
        ],
    AllDomains =
        lists:usort(
            lists:flatten(
                [DomainList || {_Id, DomainList} <- IdDomainsList]
            )
        ),
    {AllDomains, IdDomainsList}.

store_domains({AllDomains, TucIdDomainList}) ->
  lists:foreach(fun({TucId, DomainList}) ->
    ets:insert(?DOMAIN_CACHE, {binary_to_integer(TucId), DomainList})
  end, TucIdDomainList),
  OldDomains = case ets:lookup(?DOMAIN_CACHE, all_domains) of
    [] -> [];
    [{all_domains, Domains}] -> Domains
  end,
  NewDomainsToStart = lists:subtract(AllDomains, OldDomains),
  shorturl_supervisor:start_workers(NewDomainsToStart),
  update_filler_domains(),
  ets:insert(?DOMAIN_CACHE, {all_domains, AllDomains}).

update_filler_domains() ->
  case whereis(shorturl_filler) of
    undefined ->
      ok;
    Pid when is_pid(Pid) ->
      gen_server:cast(shorturl_filler, {update_domains})
  end.

%%%=================================================================================
%%%                                     END
%%%=================================================================================
