%% Generic HTTP client for sending POST requests with JSON payload
%% Handles JSON encoding, SSL verification, and provides detailed logging for different response scenarios

-module(http_client).

-export([send_request/2, gun_connect/4, gun_send/6, is_gun_connect_active/1]).

-include("onex.hrl").

%%%====================================HTTPC-HTTP FUNCTIONS======================================%%%
send_request(Url, Body) ->
  case util:map_to_json(Body) of
    {ok, Json}      ->
        io:format("Json Payload: ~p~n", [Json]),
       JsonBin = iolist_to_binary(Json),
        EscapedJson = binary:replace(JsonBin, <<"\"">>, <<"\\\"">>, [global]),
        WrappedJson = iolist_to_binary(["[\"", EscapedJson, "\"]"]),
      Request     = {Url, ?HEADERS, "application/json", WrappedJson},
      SSL_Options = [{ssl, [{verify, none}]}],
      case httpc:request(post, Request, SSL_Options, []) of
        {ok, {{_, 200, _}, _, ResponseBody}}        ->
          ?oxlog_i("POST done to mon by server type: ~p~n", [?SERVER_TYPE]),
          {ok, ResponseBody};
        {ok, {{_, StatusCode, _}, _, ResponseBody}} ->
          ?oxlog_e("POST request to mon failed with Status: ~p~n and Response: ~p~n", [StatusCode, ResponseBody]),
          {error, {StatusCode, ResponseBody}};
        {error, Reason}                             ->
          ?oxlog_e("POST request to mon failed with Reason: ~p~n", [Reason]),
          {error, Reason}
      end;
    {error, Reason} ->
      ?oxlog_e("Failed to encode data: ~p~n with reason: ~p~n", [Body, Reason]),
      {error, Reason}
  end.

%%%====================================GUN-HTTP FUNCTIONS======================================%%%
gun_connect(Ip, Port, OptMap, ConnectionTimeOut)->
  case gun:open(Ip, Port, OptMap) of
    {ok, ConnPid} 	->
      case gun:await_up(ConnPid, ConnectionTimeOut) of
        {ok, _Protocol} ->
          {active, ConnPid};
        {error, Reason} ->
          ?oxlog_e("Gun connection failed: ~p~n", [Reason]),
          gun:close(ConnPid),
          {inactive, undefined}
      end;
    {error, Reason} ->
      ?oxlog_e("Failed to open Gun connection: ~p~n", [Reason]),
      {inactive, undefined}
  end.

is_gun_connect_active(undefined) -> false;
is_gun_connect_active(ConnPid)   ->
  case catch gun:info(ConnPid) of
    #{socket := _} -> true;
    _Other         -> false
  end.

gun_send(ConnPid, Method, Path, Header, Payload, ConnectionTimeOut)->
  case catch json:encode(Payload) of
    {'EXIT', _Reason} ->
      {error, {invalid_body, "invalid json body"}};
    JSONPayload ->
      StreamRef = gun:request(ConnPid, Method, Path, Header, JSONPayload),
      case gun:await(ConnPid, StreamRef, ConnectionTimeOut) of
        {response, fin, 200, _Headers} 	 				->
            %% If the body is already received, handle it directly
          case gun:await_body(ConnPid, StreamRef, ConnectionTimeOut) of
            {ok, Body} 			->
              decode_json_body(Body);
            {error, Reason} ->
              {error, {body_receive_failed, Reason}}
          end;
        {response, nofin, 200, _Headers} 				->
            %% If the response is incomplete, wait for the remaining body
          case gun:await_body(ConnPid, StreamRef, ConnectionTimeOut) of
            {ok, Body} 			->
              decode_json_body(Body);
            {error, Reason} ->
              {error, {body_receive_failed, Reason}}
          end;
        {response, fin, StatusCode, _Headers} 	->
          {error, {http_error, StatusCode}};
        {response, nofin, StatusCode, _Headers} ->
          {error, {http_error, StatusCode}};
        {error, Reason} 												->
          {error, {connection_failed, Reason}}
        end
  end.

decode_json_body(Body) ->
	case json:decode(Body) of
		{error, Reason} -> {error, {json_decode_failed, Reason}};
		Data            -> {ok, Data}
	end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
