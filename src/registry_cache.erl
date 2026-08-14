% This module is caching data from registry and saving it in the ets table(registry_cache)
-module(registry_cache).

-behaviour(gen_server).

-include("onex.hrl").

%% API

-export([
  start_link/0,
  get_all_data/0,
  get_node/1,
  get_nodes/2,
  get_porter/1
]).

%% GenCallBacks

-export([
  init/1,
  handle_call/3,
  handle_info/2,
  handle_cast/2,
  terminate/2
]).

-define(UPDATE_CACHE, 30 * 1000). %%Cache update timming is 30 sec

%%%%%%%%%%%%%%%%%%%%%%%%%% EXPORTED FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_porter(GW) ->
  case get_porter(GW, <<"hub">>, "ready") of
    {error, Reason}  -> {error, Reason};
    Porter           -> Porter
  end.
get_porter(GW, NodeName, Status) ->
  case ets:lookup(?REGISTRY_CACHE, NodeName) of
    [{NodeName, Data}] when is_map(Data) ->
        PorterList = get_porter_list(Data, Status, GW),
            case length(PorterList) of
              0 -> {error, "No Hub Available"};
              _ ->
                Index = rand:uniform(length(PorterList)),
                lists:nth(Index, PorterList)
            end;
      [] ->
        {error, {node_not_found, NodeName}};
      _  ->
        {error, invalid_data_format}
    end.
get_node(NodeName) when is_binary(NodeName) ->
  case get_nodes(NodeName, "ready") of
    {error, Reason} ->
      {error, Reason};
    FilteredData ->
      try
        RandomKey = get_random_key(FilteredData),
        case maps:get(RandomKey, FilteredData, undefined) of
          undefined ->
            {error, {node_not_found, NodeName}};
          NodeData ->
            NodeData#{"dockerid" => RandomKey}
        end
      catch
        error:badmap ->
          {error, {node_not_found, NodeName}}
      end
  end;

get_node(NodeName) ->
  {error, {invalid_datatype_expected_binary, NodeName}}.

get_nodes(NodeName, _Status) when not is_binary(NodeName) ->
  {error, {invalid_datatype_expected_binary, NodeName}};

get_nodes(_NodeName, Status) when not is_list(Status) ->
  {error, {invalid_datatype_expected_string, Status}};

get_nodes(NodeName, Status) ->
case ets:lookup(?REGISTRY_CACHE, NodeName) of
  [{NodeName, Data}] when is_map(Data) ->
    case Status of
      "all"             ->
        case maps:size(Data) of
          0 -> {error, {node_not_found, NodeName}};
          _ -> Data
        end;
      Status    ->
        FilteredData = maps:filter(
          fun(_, Entry) ->
            maps:get(<<"status">>, Entry, undefined) =:= Status
          end, Data),
        case maps:size(FilteredData) of
          0 -> {error, {invalid_status_or_empty_node_data, Status}};
          _ -> FilteredData
        end
    end;
    [] ->
      {error, {node_not_found, NodeName}};
    _  ->
      {error, invalid_data_format}
  end.


get_all_data() ->
  ets:foldr(
    fun({NodeName, NodeData}, Acc) ->
      maps:put(NodeName, NodeData, Acc)
    end,
  #{}, ?REGISTRY_CACHE).

%%%%%%%%%%%%% Genserver Callbacks %%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
  erlang:send_after(5000, self(), update_cache),
  {ok, #{}}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(update_cache, State) ->
  case catch registry_manager:fetch_registry_data() of
    {ok, ParsedData}                  ->
      store_registry_data(ParsedData),
      ?oxlog_i("Cache successfully refreshed~n", []);
    {error, {http_error, StatusCode}} ->
      ?oxlog_e("Cache refresh failed - HTTP Error ~p~n", [StatusCode]);
    {error, Reason}                   ->
      ?oxlog_e("Cache refresh failed: ~p~n", [Reason]);
    {'EXIT', {timeout, _}}            ->
      ?oxlog_e("Cache refresh failed Timeout while fetching data~n", []);
    Unexpected                        ->
      ?oxlog_e("Unexpected return from fetch_registry_data: ~p~n", [Unexpected])
  end,
  erlang:send_after(?UPDATE_CACHE, self(), update_cache),
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

%%%%%%%%%%%%% Internal Functions %%%%%%%%%%%%%%%%%%%%%%%

store_registry_data(DecodedBody) ->
  maps:foreach(fun(Type, Data) ->
    ets:insert(?REGISTRY_CACHE, {Type, Data})
  end, DecodedBody).

get_random_key(FilteredData)->
  Keys        =  maps:keys(FilteredData),
  KeyLength   =  length(Keys),
  RandomIndex =  rand:uniform(KeyLength),
  lists:nth(RandomIndex, Keys).

get_porter_list(Data, Status, GW) ->
  maps:fold(
    fun(_, Map, Porters) ->
      case Map of
        #{
          <<"status">> := Status,
          <<"meta">>   :=
            #{
              <<"gateway_id">> := GW,
              <<"smppc_id">>   := Porter
            }
        }    -> [Porter|Porters];
        _    -> Porters
      end
    end,[],Data).
