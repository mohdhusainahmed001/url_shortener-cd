%% This module contains helper functions to send update request to /publish endpoint of registry
-module(registry_helper).

-export([
  send_request/2, send/2,
  connect/1, get_meta_data/0, map_to_json/1,
  fetch_registry/4,
  ensure_connection/1
]).

-include("onex.hrl").

-define(TIMEOUT, ?REGISTRY_TIME_TO_REFRESH + 5000).

send_request(Data, State) ->
  case send(Data, State) of
    {ok, 200, _Msg}          ->
      {ok, State};
    {ok, StatusCode, Msg}    ->
      ?oxlog_e("Failed to send data with Status: ~p and Reason: ~p~n", [StatusCode, Msg]),
      {ok, State};
    {error, Reason}          ->
      ?oxlog_e("Failed to send data: ~p", [Reason]),
      {ok,State#{pid => undefined, connect_status => inactive}};
    {inactive, UpdatedState}        ->
      {ok, UpdatedState}
  end.


send(Data, #{pid := ConnPid} = State) ->
  case map_to_json(Data) of
    {ok , Json} ->
      % StreamRef = gun:put(ConnPid, ?REGISTRY_ENDPOINT, [{"Content-Type", "application/json"}], Json),
      StreamRef = gun:post(ConnPid, ?REGISTRY_ENDPOINT, ?HEADERS, Json),
      case gun:await(ConnPid, StreamRef, ?TIMEOUT) of
        {response, fin, Status, _Headers} ->
          {ok, Status, []};
        {response, nofin, Status, _Headers} ->
          case gun:await_body(ConnPid, StreamRef, ?TIMEOUT) of
            {ok, Body} ->
              {ok, Status, Body};
            {error, Reason} ->
              {error, {body_receive_failed, Reason}}
          end;
        {error, Reason} ->
          ?oxlog_e("Error: Put Request to registry failed with Reason: ~p~n", [Reason]),
          gun:close(ConnPid),
          {inactive, State#{pid => undefined, connect_status => inactive}}
      end;
  {error, Reason} ->
    ?oxlog_e("Failed to encode data: ~p~n with Reason: ~p~n", [Data, Reason]),
    {error, failed_to_encode}
  end.

connect(State) ->
  % for uncomment it for testing
  % RegistryIp = "127.0.0.1",
  % RegistryPort = 8078,
  RegistryIp = docker_func:get_registry_ip(),
  RegistryPort = docker_func:get_registry_port(),
  case gun:open(RegistryIp, RegistryPort) of
    {ok, ConnPid} ->
      case gun:await_up(ConnPid, ?TIMEOUT) of
        {ok, _Protocol}  ->
          {active, State#{pid => ConnPid, connect_status => active}};
        {error, timeout} ->
          % ?oxlog_e("Connection Timeout on server: ~p Port: ~p~n",[RegistryIp, RegistryPort]),
          {inactive, State};
        {error, Reason} ->
          ?oxlog_e("Error in gun await_up connecting to ~p: ~p~n", [RegistryIp, Reason]),
          gun:close(ConnPid),
          {inactive, State}
      end;
    {error, Reason} ->
      ?oxlog_e("Failed to open connection: ~p~n", [Reason]),
      {inactive, State}
  end.

get_meta_data() ->
  #{}.

map_to_json(Map) ->
  try json:encode(Map) of
    EncodedMap -> {ok, EncodedMap}
  catch
    _:_ -> {error, <<"bad_map">>}
  end.

fetch_registry(ConnPid, Type, DockerId, Status) ->
  Body = jiffy:encode(
    #{
      <<"data">> => #{
        <<"type">>     =>  Type,
        <<"dockerid">> =>  DockerId,
        <<"status">>   =>  Status
      }
    }),
  Headers   = [{<<"content-type">>, <<"application/json">>}],
  StreamRef = gun:post(ConnPid, "/get_registry", Headers, Body),
  case gun:await(ConnPid, StreamRef, ?TIMEOUT) of
    {response, _IsFin, 200, _Headers} ->
      case gun:await_body(ConnPid, StreamRef, ?TIMEOUT) of
        {ok, ResponseBody} ->
          try jiffy:decode(ResponseBody, [return_maps]) of
            DecodedBody  -> {ok, DecodedBody}
          catch
            Error:Reason:Stack ->
              ?oxlog_e("Failed to parse JSON data: ~p:~p~n Stacktrace: ~p~n",[Error, Reason, Stack]),
              {error, json_parse_error}
          end;
        {error, Reason}  ->
          {error, {body_receive_failed, Reason}}
      end;
    {response, _IsFin, StatusCode, _Headers} ->
      ?oxlog_e("HTTP request failed with status code: ~p~n", [StatusCode]),
      {error, {http_error, StatusCode}};
    {error, Reason}                          ->
      ?oxlog_e("Failed to fetch data: ~p~n", [Reason]),
      {error, Reason}
  end.

ensure_connection(#{pid :=ConnPid}=State) ->
    % OldPid
  ConnPidInfo=gun:info(ConnPid),
  case maps:get(socket,ConnPidInfo,undefined) of
    undefined ->
      case connect(State) of
        {active, #{pid := NewConnPid}} ->
          NewConnPid;
        {inactive, #{}}                ->
          ?oxlog_e("Connection has failed after retry~n", []),
          {error, connection_failed}
       end;
    _         ->
      ConnPid
  end.