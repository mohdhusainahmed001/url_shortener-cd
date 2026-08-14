-module(short_url_handler).

-include("onex.hrl").

-export([init/2]).

-define(WHITESPACE_REGEX, re:compile("\\s+", [])).
-define(TRAILING_SLASH_REGEX, re:compile("/+$", [])).

init(Req0, Opts) ->
  % T1 = os:system_time(millisecond),
  Method = cowboy_req:method(Req0),
  Req1 =
    case Method of
      <<"POST">> ->
       {ok, Body, _} = cowboy_req:read_body(Req0),
        process_body(Req0, Body);
      _          ->
        RespBody = {[{<<"status">>, <<"Failed">>},{<<"reason">>, <<"Method_Not_Allowed">>},{<<"code">>, 405}]},
        cowboy_req:reply(405, content_type(), jiffy:encode(RespBody), Req0)
    end,
  {ok, Req1, Opts}.

process_body(Req, Body) ->
  VBody = try json:decode(Body) of
    DBody  -> {true, DBody};
    _      -> {false, <<"API_JSON_Decode_format_not_supported">>}
    catch
    _:_ ->
    {false, <<"API_Invalid_JSON">>}
    end,
  case VBody of
    {true, DecodedBody} ->
      DomainList = maps:get(<<"domains">>, DecodedBody, undefined),
      TucId      = maps:get(<<"tuc_id">>, DecodedBody, undefined),
      TenantId   = maps:get(<<"tenant_id">>, DecodedBody, undefined),
      Channel    = maps:get(<<"channel">>, DecodedBody, undefined),
      TucUsername = maps:get(<<"tuc_username">>, DecodedBody, undefined),
      validate_body(DomainList, Req, TucId, TenantId, Channel, TucUsername);
    {false, Msg}   ->
    RespBody = {[{<<"status">>, <<"Failed">>},{<<"reason">>, Msg},{<<"code">>, 400}]},
    cowboy_req:reply(400, content_type(), jiffy:encode(RespBody), Req)
  end.

validate_body(DomainList, Req, _TucId, _TenantId, _Channel, _TucUsername) when DomainList == undefined; DomainList == #{}; DomainList == []; DomainList == null ->
  RespBody = {[{<<"status">>, <<"Failed">>},{<<"reason">>, <<"Missing/Invalid Domain Object">>},{<<"code">>, 400}]},
  cowboy_req:reply(400, content_type(), jiffy:encode(RespBody), Req);

validate_body(_DomainList, Req, TucId, _TenantId, _Channel, _TucUsername) when TucId == undefined; TucId == <<>> ->
  RespBody = {[{<<"status">>, <<"Failed">>},{<<"reason">>, <<"Missing/Invalid TucId">>},{<<"code">>, 400}]},
  cowboy_req:reply(400, content_type(), jiffy:encode(RespBody), Req);

validate_body(_DomainList, Req, _TucId, _TenantId, Channel, TucUsername) when (Channel =/= <<"sms">>) andalso (TucUsername == undefined orelse TucUsername == <<>>) ->
  RespBody = {[{<<"status">>, <<"Failed">>},{<<"reason">>, <<"Missing/Invalid TucUsername">>},{<<"code">>, 400}]},
  cowboy_req:reply(400, content_type(), jiffy:encode(RespBody), Req);

validate_body(_DomainList, Req, _TucId, TenantId, _Channel, _TucUsername) when TenantId == undefined; TenantId == <<>> ->
  RespBody = {[{<<"status">>, <<"Failed">>},{<<"reason">>, <<"Missing/Invalid TenantId">>},{<<"code">>, 400}]},
  cowboy_req:reply(400, content_type(), jiffy:encode(RespBody), Req);

validate_body(_DomainList, Req, _TucId, _TenantId, Channel, _TucUsername) when Channel == undefined; Channel == <<>> ->
  RespBody = {[{<<"status">>, <<"Failed">>},{<<"reason">>, <<"Missing/Invalid Channel">>},{<<"code">>, 400}]},
  cowboy_req:reply(400, content_type(), jiffy:encode(RespBody), Req);

validate_body(DomainList, Req, TucId, TenantId, Channel, TucUsername) ->
  ResMap   = process_domains(DomainList, TucId, TenantId, Channel, TucUsername),
  RespBody = {[{<<"message">>, ResMap}]},
  cowboy_req:reply(200, content_type(), jiffy:encode(RespBody), Req).

process_domains(DomainsMap, TucId, TenantId, Channel, TucUsername) ->
  ResultList = maps:fold(fun
    (DomainName, List, Acc) when is_list(List) ->
      NormalizedDomain = util:normalize_url(DomainName),
      case domain_cache:is_valid_domain(TucId, NormalizedDomain) of
        {true, ProtocolDomain} ->
          UuidListLength   = length(List),
          case shorturl_queue_worker:dequeue_batch(ProtocolDomain, UuidListLength) of
            {ok, ShortCodes} when length(ShortCodes) == UuidListLength ->
              DomainToUse = case Channel of <<"sms">> -> NormalizedDomain; _ -> ProtocolDomain end,
              DomainMappings = create_domain_mappings(DomainToUse, List, ShortCodes, TucId, TenantId, Channel, TucUsername),
              lists:concat([DomainMappings,Acc]);
            _ ->
              [{error, #{error => <<"Unable to get shortcodes for Domain:- ", DomainName/binary>>}}]
          end;
        false ->
          ErrorUuidList = create_error_list(List, NormalizedDomain),
          lists:concat([ErrorUuidList,Acc])
      end;
    (DomainName, _List, _Acc) ->
      [{error, #{error => <<"Invalid details for domain:- ", DomainName/binary>>}}]
  end, [], DomainsMap),
  maps:from_list(ResultList).

add_channel_identifier(CodeList, Channel) ->
  Code = list_to_binary(CodeList),
  case Channel of
    <<"sms">> ->
      Code;
    <<"rcs">> ->
      <<Code/binary, "_2">>;
    _ ->
      <<Code/binary, "_1">>
  end.

create_domain_mappings(_Domain, [], _Codes, _, _, _, _) ->
  [];
create_domain_mappings(_, _UrlList, [], _, _, _, _) ->
  [];
create_domain_mappings(Domain, UrlList, Codes, TucId, TenantId, Channel, TucUsername) ->
  lists:zipwith(fun(UrlMap, Code) ->
    case check_mandatory_keys(UrlMap, Channel) of
      [] ->
        FinalCode          = add_channel_identifier(Code, Channel),
        UUID               = maps:get(<<"uuid">>, UrlMap, undefined),
        LongUrl            = maps:get(<<"long_url">>, UrlMap, undefined),
        _SenderId          = maps:get(<<"header">>, UrlMap, undefined),
        ShortUrl           = <<Domain/binary, "/", FinalCode/binary>>,
        RequestTime        = os:system_time(millisecond),
        NormalizedLongUrl  = util:normalize_url(LongUrl),
        ValuesMap          = #{<<"request_time">> => RequestTime, <<"tuc_id">> => TucId, <<"tenant_id">> => TenantId, <<"tuc_username">> => TucUsername,
                              <<"channel">> => Channel, <<"short_url">> => ShortUrl},
        NewUrlMap          = maps:merge(ValuesMap, UrlMap),
        insert_in_shorturl_queue(NewUrlMap),
        {UUID, #{<<"long_url">> => NormalizedLongUrl, <<"short_url">> => ShortUrl, <<"short_code">> =>FinalCode}};
      MissingKeys ->
        UUID = maps:get(<<"uuid">>, UrlMap, undefined),
        {UUID, #{<<"error">> => #{<<"missing_keys">> => MissingKeys}}}
    end
  end, UrlList, Codes).

create_error_list(UuidList, DomainName) ->
  lists:map(fun(UrlMap) ->
    Uuid = maps:get(<<"uuid">>, UrlMap, undefined),
    {Uuid, #{error => <<"Invalid Domain ", DomainName/binary>>}}
  end, UuidList).

insert_in_shorturl_queue(UrlMap) ->
  shorturl_queue:enqueue(UrlMap).

% get_url(Domain, Code, _SenderId, Channel) ->
  % case Channel of
  %   <<"sms">> ->
  %     <<Domain/binary, "/", SenderId/binary, "/", Code/binary>>;
  %   _ ->
      % <<Domain/binary, "/", Code/binary>>.
  % end.

check_mandatory_keys(Map, <<"rcs">>) ->
  MandatoryKeys = [<<"onex_received_ts">>, <<"bot_id">>, <<"uuid">>, <<"long_url">>, <<"country_name">>, <<"circle">>,
                   <<"operator">>, <<"template_name">>, <<"provider_id">>, <<"campaign_name">>, <<"campaign_instance_id">>,
                   <<"template_type">>, <<"msg_type">>, <<"api_key">>, <<"source">>, <<"sender_profile">>, <<"billing_category">>,
                   <<"retry_count">>, <<"repush_count">>, <<"to_number">>],
  lists:filter(fun(Key) -> not maps:is_key(Key, Map) end, MandatoryKeys);

check_mandatory_keys(Map, <<"sms">>) ->
  MandatoryKeys = [<<"uuid">>, <<"long_url">>, <<"track_location">>, <<"onex_received_ts">>, <<"circle">>, <<"operator">>, <<"header">>,
                   <<"msisdn">>, <<"campaign_id">>, <<"campaign_instance_id">>, <<"retry_count">>, <<"repush_count">>, <<"campaign_name">>, <<"sms_type">> ],
  lists:filter(fun(Key) -> not maps:is_key(Key, Map) end, MandatoryKeys);

check_mandatory_keys(Map, <<"wa">>) ->
  MandatoryKeys = [<<"uuid">>, <<"long_url">>, <<"track_location">>, <<"onex_received_ts">>,
                   <<"onex_received_ts">>, <<"waba_id">>, <<"uuid">>, <<"long_url">>, <<"country_name">>, <<"circle">>,
                   <<"operator">>, <<"template_name">>, <<"provider_id">>, <<"campaign_name">>, <<"campaign_instance_id">>,
                   <<"template_type">>, <<"api_key">>, <<"source">>, <<"phone_number">>, <<"template_category">>, <<"is_mmlite">>,
                   <<"retry_count">>, <<"repush_count">>, <<"to_number">>],
  lists:filter(fun(Key) -> not maps:is_key(Key, Map) end, MandatoryKeys).

content_type() ->
  #{<<"content-type">> => <<"application/json; charset=utf-8">>}.
