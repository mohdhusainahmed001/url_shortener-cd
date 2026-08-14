%% This module sends a POST request to the Registry telling that this is service is 'UP'
-module(publish).

-export([send/0]).

-include("onex.hrl").

send() ->
  Ip       = docker_func:get_container_ip(),
  Port     = docker_func:get_port(),
  DockerId = docker_func:get_container_id(),
  Status   = ?UP_STATUS,
  Meta     = [],
  RegistryIp   = docker_func:get_registry_ip(),
  RegistryPort = docker_func:get_registry_port(),
  Url      = lists:concat(["http://", RegistryIp, ":", RegistryPort, ?REGISTRY_ENDPOINT]),
  Body = #{
    <<"data">> =>
      #{
        <<"ip">>              => Ip,
        <<"port">>            => Port,
        <<"type">>            => ?SERVER_TYPE,
        <<"dockerid">>        => DockerId,
        <<"time_to_refresh">> => ?REGISTRY_TIME_TO_REFRESH,
        <<"status">>          => Status,
        <<"meta">>            => Meta
      }
  },
  case registry_helper:map_to_json(Body) of
    {ok, Json}      ->
      Request     = {Url, ?HEADERS, "application/json", Json},
      SSL_Options = [{ssl, [{verify, none}]}],
      case httpc:request(post, Request, SSL_Options, []) of
        {ok, {{_, 200, _}, _, ResponseBody}} ->
          ?oxlog_e("POST done to Registry with status: ~p by server type: ~p~n", [Status, ?SERVER_TYPE]),
          {ok, ResponseBody};
        {ok, {{_, StatusCode, _}, _, ResponseBody}} ->
          ?oxlog_e("POST request to registry failed with Status: ~p~n and Response: ~p~n", [StatusCode, ResponseBody]),
          {error, {StatusCode, ResponseBody}};
        {error, Reason} ->
          ?oxlog_e("POST request to registry failed with Reason: ~p~n", [Reason]),
          {error, Reason}
      end;
    {error, Reason} ->
      ?oxlog_e("Failed to encode data: ~p~n with reason: ~p~n", [Body, Reason]),
      {error, Reason}
  end.