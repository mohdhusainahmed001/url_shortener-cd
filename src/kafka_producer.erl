-module(kafka_producer).

-export([create_shorturl_payload/1, create_clicker_payload/1]).

-include("onex.hrl").

%%-----------------------------------------------------------------------------------------------%%
create_shorturl_payload(
  #{<<"uuid">>             := UUID,
    <<"request_time">>     := RequestTime,
    <<"onex_received_ts">> := OnexReceivedTs,
    <<"channel">>          := Channel,
    <<"track_location">>   := LocationCheck,
    <<"short_url">>        := ShortUrl,
    <<"long_url">>         := LongUrl,
    <<"tenant_id">>        := TenantId,
    <<"tuc_id">>           := TucId}) ->
  TucTimeZone            = tuc_cache:get_tuc_timezone(TucId),
  RequestTimeTZ          = timezone_util:convert_timezone(RequestTime, timezone_util:get_server_timezone(), TucTimeZone),
  OnexReceivedTsTZ       = timezone_util:convert_timezone(OnexReceivedTs, timezone_util:get_server_timezone(), TucTimeZone),
  OnexReceivedTsDateTime = milliseconds_to_datetime_tz(OnexReceivedTsTZ),
  RequestTimeDateTime    = milliseconds_to_datetime_tz(RequestTimeTZ),
  UrlExpiryTsDateTime    = milliseconds_to_datetime_tz(RequestTimeTZ + ?EXPIRY_TS_SHORTURL),
  Key = #{
    tuc_id           => check_null(TucId),
    tenant_id        => check_null(TenantId),
    short_url        => check_null(ShortUrl),
    onex_received_ts => check_null(OnexReceivedTsDateTime)
  },
  ShortUrlPayload = #{
    tuc_id           => check_null(TucId),
    tenant_id        => check_null(check_null(TenantId)),
    short_url        => check_null(ShortUrl),
    onex_received_ts => check_null(OnexReceivedTsDateTime),
    track_location   => check_null(LocationCheck),
    onex_msg_id      => check_null(UUID),
    long_url         => check_null(LongUrl),
    channel          => check_null(Channel),
    url_expiry_ts    => check_null(UrlExpiryTsDateTime),
    gen_request_ts   => check_null(RequestTimeDateTime)
   },
  Topic = <<"odp.short.url.generate">>,
  {jiffy:encode(Key), {jiffy:encode(ShortUrlPayload),Topic, shorturl}}.

%%-----------------------------------------------------------------------------------------------%%
%%

create_clicker_payload({ShortUrlData,UaData}) ->
  Channel = maps:get(<<"channel">>, ShortUrlData, null ),
  case Channel of
    <<"rcs">> ->
      create_rcs_clicker_payload({ShortUrlData,UaData});
    <<"sms">> ->
      create_sms_clicker_payload({ShortUrlData, UaData});
    _         ->
      create_wa_clicker_payload({ShortUrlData,UaData})
  end.

create_sms_clicker_payload({ShortUrlData, UaData}) ->
  #{
    <<"tenant_id">>            := TenantId,
    <<"tuc_id">>               := TucId,
    <<"onex_received_ts">>     := OnexReceivedTs,
    <<"short_url">>            := ShortUrl,
    <<"onex_msg_id">>          := MsgId,
    <<"long_url">>             := LongUrl,
    <<"circle">>               := Circle,
    <<"operator">>             := Operator,
    <<"header">>               := Header,
    <<"msisdn">>               := MSISDN,
    <<"campaign_id">>          := CampaignId,
    <<"campaign_instance_id">> := CampaignInstanceId,
    <<"campaign_name">>        := CampaignName,
    <<"retry_count">>          := RetryCount,
    <<"repush_count">>         := RepushCount,
    <<"sms_type">>             := SmsType,
    <<"source">>               := Channel
  } = ShortUrlData,

  #{
    <<"clicked_time">> := UrlClickTs,
    <<"ip">>           := Ip,
    <<"browser">>      := Browser,
    <<"os">>           := Os
  } = UaData,
  TucTimeZone            = tuc_cache:get_tuc_timezone(TucId),
  UrlClickTz             = timezone_util:convert_timezone(UrlClickTs, timezone_util:get_server_timezone(), TucTimeZone),
  OnexReceivedTsTZ       = timezone_util:convert_timezone(OnexReceivedTs, timezone_util:get_server_timezone(), TucTimeZone),
  OnexReceivedTsDateTime = milliseconds_to_datetime_tz(OnexReceivedTsTZ),
  UrlClickTsDateTime     = milliseconds_to_datetime_tz(UrlClickTz),
  LocationLt = maps:get(<<"location_lt">>, UaData, null),
  LocationLg = maps:get(<<"location_lg">>, UaData, null),

  Key = #{
    tuc_id           => check_null(TucId),
    short_url        => check_null(ShortUrl),
    url_click_ts     => check_null(UrlClickTsDateTime),
    onex_received_ts => check_null(OnexReceivedTsDateTime)
  },

  ClickerPayload = #{
    tuc_id               => check_null(TucId),
    tenant_id            => check_null(TenantId),
    onex_received_ts     => check_null(OnexReceivedTsDateTime),
    url_click_ts         => check_null(UrlClickTsDateTime),
    short_url            => check_null(ShortUrl),
    msg_id               => check_null(MsgId),
    long_url             => check_null(LongUrl),
    location_lt          => check_null(LocationLt),
    location_lg          => check_null(LocationLg),
    ip_address           => check_null(Ip),
    browser              => check_null(Browser),
    operating_system     => check_null(Os),
    circle               => check_null(Circle),
    operator             => check_null(Operator),
    header               => check_null(Header),
    msisdn               => check_null(MSISDN),
    campaign_name        => check_null(CampaignName),
    campaign_id          => check_null(CampaignId),
    campaign_instance_id => check_null(CampaignInstanceId),
    retry_count          => check_null(RetryCount),
    repush_count         => check_null(RepushCount),
    sms_type             => check_null(SmsType),
    channel              => check_null(Channel),
    msg_type             => <<"SMS">>,
    future_retry         => false
  },

  Topic = << (integer_to_binary(TenantId))/binary, ".short.url.click">>,
  {jiffy:encode(Key), {jiffy:encode(ClickerPayload),Topic, clicker}}.

create_rcs_clicker_payload({ShortUrlData,UaData}) ->
  #{
    <<"tenant_id">>            := TenantId,
    <<"tuc_id">>               := TucId,
    <<"onex_received_ts">>     := OnexReceivedTs,
    <<"short_url">>            := ShortUrl,
    <<"bot_id">>               := BotId,
    <<"onex_msg_id">>          := MsgId,
    <<"long_url">>             := LongUrl,
    <<"country_name">>         := CountryName,
    <<"circle">>               := Circle,
    <<"operator">>             := Operator,
    <<"template_name">>        := TemplateName,
    <<"provider_id">>          := ProviderId,
    <<"campaign_name">>        := CampaignName,
    <<"campaign_instance_id">> := CampaignInstanceId,
    <<"template_type">>        := TemplateType,
    <<"msg_type">>             := MsgType,
    <<"api_key">>              := ApiKey,
    <<"source">>               := Source,
    <<"sender_profile">>       := SenderProfile,
    <<"billing_category">>     := BillingCategory,
    <<"retry_count">>          := RetryCount,
    <<"repush_count">>         := RepushCount,
    <<"tuc_username">>         := TucUsername
  } = ShortUrlData,

  #{
    <<"clicked_time">> := UrlClickTs,
    <<"ip">>           := Ip,
    <<"browser">>      := Browser,
    <<"os">>           := Os
  } = UaData,
  TucTimeZone            = tuc_cache:get_tuc_timezone(TucId),
  UrlClickTz             = timezone_util:convert_timezone(UrlClickTs, timezone_util:get_server_timezone(), TucTimeZone),
  OnexReceivedTsTZ       = timezone_util:convert_timezone(OnexReceivedTs, timezone_util:get_server_timezone(), TucTimeZone),
  OnexReceivedTsDateTime = milliseconds_to_datetime_tz(OnexReceivedTsTZ),
  UrlClickTsDateTime     = milliseconds_to_datetime_tz(UrlClickTz),
  Key = #{
    tuc_id           => check_null(TucId),
    short_url        => check_null(ShortUrl),
    url_click_ts     => check_null(UrlClickTsDateTime),
    onex_received_ts => check_null(OnexReceivedTsDateTime)
  },
  LocationLt  = maps:get(<<"location_lt">>, UaData, null),
  LocationLg  = maps:get(<<"location_lg">>, UaData, null),
  CustParam1  = maps:get(<<"cust_param_1">>, ShortUrlData, null),
  CustParam2  = maps:get(<<"cust_param_2">>, ShortUrlData, null),
  CustParam3  = maps:get(<<"cust_param_3">>, ShortUrlData, null),
  CustParam4  = maps:get(<<"cust_param_4">>, ShortUrlData, null),
  CustParam5  = maps:get(<<"cust_param_5">>, ShortUrlData, null),
  ToNumber    = maps:get(<<"to_number">>, ShortUrlData, null),
  RcsMsgId    = maps:get(<<"rcs_msg_id">>, ShortUrlData, null),
  ClickerPayload = #{
    tuc_id               => check_null(TucId),
    onex_received_ts     => check_null(OnexReceivedTsDateTime),
    url_click_ts         => check_null(UrlClickTsDateTime),
    short_url            => check_null(ShortUrl),
    bot_id               => check_null(BotId),
    onex_msg_id          => check_null(MsgId),
    long_url             => check_null(LongUrl),
    location_lt          => check_null(LocationLt),
    location_lg          => check_null(LocationLg),
    ip_address           => check_null(Ip),
    browser              => check_null(Browser),
    operating_system     => check_null(Os),
    country_name         => check_null(CountryName),
    circle               => check_null(Circle),
    operator             => check_null(Operator),
    template_name        => check_null(TemplateName),
    provider_id          => check_null(ProviderId),
    campaign_name        => check_null(CampaignName),
    campaign_instance_id => check_null(CampaignInstanceId),
    template_type        => check_null(TemplateType),
    msg_type             => check_null(MsgType),
    api_key              => check_null(ApiKey),
    source               => check_null(Source),
    sender_profile       => check_null(SenderProfile),
    billing_category     => check_null(BillingCategory),
    cust_param_1         => check_null(CustParam1),
    cust_param_2         => check_null(CustParam2),
    cust_param_3         => check_null(CustParam3),
    cust_param_4         => check_null(CustParam4),
    cust_param_5         => check_null(CustParam5),
    rcs_msg_id           => check_null(RcsMsgId),
    future_retry         => false,
    retry_count          => RetryCount,
    repush_count         => RepushCount,
    to_number            => ToNumber,
    tuc_username         => TucUsername
  },
  Topic = << (integer_to_binary(TenantId))/binary, ".rcs.short.url.click">>,
  {jiffy:encode(Key), {jiffy:encode(ClickerPayload),Topic, clicker}}.

%%------------------------------------------------------------------------------------------------%%

create_wa_clicker_payload({ShortUrlData,UaData}) ->
 
  #{
<<"tenant_id">>            := TenantId,
<<"tuc_id">>               := TucId,
<<"onex_received_ts">>     := OnexReceivedTs,
<<"short_url">>            := ShortUrl,
<<"waba_id">>              := WabaId,
<<"onex_msg_id">>          := MsgId,
<<"long_url">>             := LongUrl,
<<"country_name">>         := CountryName,
<<"circle">>               := Circle,
<<"operator">>             := Operator,
<<"template_name">>        := TemplateName,
<<"provider_id">>          := ProviderId,
<<"campaign_name">>        := CampaignName,
<<"campaign_instance_id">> := CampaignInstanceId,
<<"template_type">>        := TemplateType,
<<"api_key">>              := ApiKey,
<<"source">>               := Source,
<<"phone_number">>         := PhoneNumber,
<<"template_category">>    := TemplateCategory,
<<"retry_count">>          := RetryCount,
<<"repush_count">>         := RepushCount,
<<"tuc_username">>         := TucUsername
  } = ShortUrlData,
 
  #{
<<"clicked_time">> := UrlClickTs,
<<"ip">>           := Ip,
<<"browser">>      := Browser,
<<"os">>           := Os
  } = UaData,
  TucTimeZone            = tuc_cache:get_tuc_timezone(TucId),
  UrlClickTz             = timezone_util:convert_timezone(UrlClickTs, timezone_util:get_server_timezone(), TucTimeZone),
  OnexReceivedTsTZ       = timezone_util:convert_timezone(OnexReceivedTs, timezone_util:get_server_timezone(), TucTimeZone),
  OnexReceivedTsDateTime = milliseconds_to_datetime_tz(OnexReceivedTsTZ),
  UrlClickTsDateTime     = milliseconds_to_datetime_tz(UrlClickTz),
  CustParam1   = maps:get(<<"cust_param_1">>, ShortUrlData, null),
  CustParam2   = maps:get(<<"cust_param_2">>, ShortUrlData, null),
  CustParam3   = maps:get(<<"cust_param_3">>, ShortUrlData, null),
  CustParam4   = maps:get(<<"cust_param_4">>, ShortUrlData, null),
  CustParam5   = maps:get(<<"cust_param_5">>, ShortUrlData, null),
  TemplateType = maps:get(<<"template_type">>, ShortUrlData, null),
  MmLite       = maps:get(<<"is_mmlite">>, ShortUrlData, false),
  ContentType  = maps:get(<<"payload_type">>, ShortUrlData, null),
  PayloadType  = maps:get(<<"content_type">>, ShortUrlData, null),
  Key = #{
    tuc_id           => check_null(TucId),
    short_url        => check_null(ShortUrl),
    url_click_ts     => check_null(UrlClickTsDateTime),
    onex_received_ts => check_null(OnexReceivedTsDateTime)
  },
  LocationLt = maps:get(<<"location_lt">>, UaData, null),
  LocationLg = maps:get(<<"location_lg">>, UaData, null),
  ToNumber   = maps:get(<<"to_number">>, ShortUrlData, null),
  ClickerPayload = #{
    tuc_id               => check_null(TucId),
    onex_received_ts     => check_null(OnexReceivedTsDateTime),
    url_click_ts         => check_null(UrlClickTsDateTime),
    short_url            => check_null(ShortUrl),
    waba_id              => check_null(WabaId),
    onex_msg_id          => check_null(MsgId),
    long_url             => check_null(LongUrl),
    location_lt          => check_null(LocationLt),
    location_lg          => check_null(LocationLg),
    ip_address           => check_null(Ip),
    browser              => check_null(Browser),
    operating_system     => check_null(Os),
    country_name         => check_null(CountryName),
    circle               => check_null(Circle),
    operator             => check_null(Operator),
    template_name        => check_null(TemplateName),
    provider_id          => check_null(ProviderId),
    campaign_name        => check_null(CampaignName),
    campaign_instance_id => check_null(CampaignInstanceId),
    template_type        => check_null(TemplateType),
    api_key              => check_null(ApiKey),
    source               => check_null(Source),
    phone_number         => check_null(PhoneNumber),
    template_category    => check_null(TemplateCategory),
    cust_param_1         => check_null(CustParam1),
    cust_param_2         => check_null(CustParam2),
    cust_param_3         => check_null(CustParam3),
    cust_param_4         => check_null(CustParam4),
    cust_param_5         => check_null(CustParam5),
    meta_msg_id          => null,
    msg_type             => null,
    sender_profile       => null,
    billing_category     => null,
    future_retry         => false,
    origin_type          => null,
    pricing_type         => null,
    pricing_model        => null,
    is_mmlite            => MmLite,
    content_type         => check_null(ContentType),
    payload_type         => check_null(PayloadType),
    retry_count          => RetryCount,
    repush_count         => RepushCount,
    to_number            => ToNumber,
    tuc_username         => TucUsername
  },
  Topic = << (integer_to_binary(TenantId))/binary, ".wa.short.url.click">>,
  {jiffy:encode(Key), {jiffy:encode(ClickerPayload),Topic, clicker}}.


convert_int_to_zero_prefixed_list(Value) when Value >= 0, Value < 10 ->
  lists:concat(["0", integer_to_list(Value)]);
convert_int_to_zero_prefixed_list(Value) ->
  integer_to_list(Value).

milliseconds_to_datetime(undefined)->
  null;

milliseconds_to_datetime(null)->
  null;

milliseconds_to_datetime(Milliseconds) when is_binary(Milliseconds)->
  case string:to_integer(binary_to_list(Milliseconds)) of
    {Int, []} -> milliseconds_to_datetime(Int);
    _ -> Milliseconds
  end;

milliseconds_to_datetime(Milliseconds)->
  Seconds = Milliseconds div 1000,
  RemainingMilliseconds = Milliseconds rem 1000,
  Epoch = {Seconds div 1000000, Seconds rem 1000000, 0},
  {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:now_to_universal_time(Epoch),
  UtcSeconds = calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Min, Sec}}),
  IstSeconds = UtcSeconds + 19800,  % Add 5:30 hours in seconds for IST conversion
  {{IstYear, IstMonth, IstDay}, {IstHour, IstMin, IstSec}} = calendar:gregorian_seconds_to_datetime(IstSeconds),
  list_to_binary(integer_to_list(IstYear)++"-"++convert_int_to_zero_prefixed_list(IstMonth)++"-"++convert_int_to_zero_prefixed_list(IstDay)++" "++convert_int_to_zero_prefixed_list(IstHour)++":"++convert_int_to_zero_prefixed_list(IstMin)++":"++convert_int_to_zero_prefixed_list(IstSec)++"."++string:pad(integer_to_list(RemainingMilliseconds), 3, leading, $0)).

milliseconds_to_datetime_tz(undefined)->
  null;

milliseconds_to_datetime_tz(null)->
  null;

milliseconds_to_datetime_tz(Milliseconds) when is_binary(Milliseconds)->
  case string:to_integer(binary_to_list(Milliseconds)) of
    {Int, []} -> milliseconds_to_datetime_tz(Int);
    _ -> Milliseconds
  end;

milliseconds_to_datetime_tz(Milliseconds) ->
    Seconds = Milliseconds div 1000,
    RemainingMilliseconds = Milliseconds rem 1000,
    Epoch = {Seconds div 1000000, Seconds rem 1000000, 0},
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:now_to_universal_time(Epoch),
    list_to_binary(
        integer_to_list(Year) ++ "-" ++
        convert_int_to_zero_prefixed_list(Month) ++ "-" ++
        convert_int_to_zero_prefixed_list(Day) ++ " " ++
        convert_int_to_zero_prefixed_list(Hour) ++ ":" ++
        convert_int_to_zero_prefixed_list(Min) ++ ":" ++
        convert_int_to_zero_prefixed_list(Sec) ++ "." ++
        string:pad(integer_to_list(RemainingMilliseconds), 3, leading, $0)
    ).

check_null(Param) ->
  case Param of
    <<"undefined">> -> null;
    <<"">>          -> null;
    undefined       -> null;
    Param           -> Param
  end.
