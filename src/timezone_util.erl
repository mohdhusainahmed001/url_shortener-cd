-module(timezone_util).
-include("onex.hrl").

-export([
    get_date_and_time_from_timezone/1,
    get_timezone_options/0,
    get_timezone_options_list/0,
    get_current_timezone/0,
    % get_timezone/1,
    get_server_timezone_with_offset/0,
    get_current_timezone_without_offset/0,
    get_server_timezone/0,
    convert_timezone/3,
    parse_timezone/1,
    get_timezone_from_userdata/0,
    get_campaign_instance_id/1,
    get_schedule_datetime/1,
    get_server_timestamp_from_datetime_timezone/2,
    get_timezone_offset_ms/1
]).

get_schedule_datetime(TimeZone) ->
  DateTime    = get_date_and_time_from_timezone(TimeZone),
  Millisecond = ui_utils:datetime_to_milliseconds(DateTime),
  ui_utils:milliseconds_to_datetime(Millisecond + 16*60*1000).

parse_timezone(TimezoneStr) when is_binary(TimezoneStr) ->
  parse_timezone(binary_to_list(TimezoneStr));
parse_timezone(TimezoneStr) when is_list(TimezoneStr) ->
  case string:split(TimezoneStr, " (UTC ", leading) of
    [Timezone, OffsetWithParen] ->
      Offset = string:trim(OffsetWithParen, trailing, ")"),
      {Timezone, Offset};
     _ ->
      parse_timezone(get_server_timezone_with_offset())
  end.

get_timezone_from_userdata() ->
  case wf:user() of
    undefined -> get_server_timezone_with_offset(); 
    UserData  ->
      Timezone = proplists:get_value(timezone, UserData),
      TimezoneOffset = proplists:get_value(timezone_offset, UserData),
      lists:concat([Timezone, " (UTC ", TimezoneOffset, ")"])
  end.

% get_timezone(TimezoneWithOffset) when is_binary(TimezoneWithOffset) ->
%   get_timezone(binary_to_list(TimezoneWithOffset));
% get_timezone(TimezoneWithOffset) when is_list(TimezoneWithOffset) ->
%   hd(string:split(TimezoneWithOffset, " (", leading));
% get_timezone(_) ->
%   get_timezone(get_server_timezone_with_offset()).

get_current_timezone() ->
  TZ = case file:read_file("/etc/timezone") of
            {ok, Bin} -> string:trim(binary_to_list(Bin));
            {error, _} -> "unknown"
        end,
  Offset = string:trim(os:cmd("date +\"UTC %:z\"")),
  lists:concat([TZ, " (", Offset, ")"]).

get_current_timezone_without_offset() ->
  case file:read_file("/etc/timezone") of
    {ok, Bin} -> string:trim(binary_to_list(Bin));
    {error, _} -> "unknown"
  end.

get_timezone_options_list() ->
  persistent_term:get(timezone_options, []).

get_server_timezone_with_offset() ->
  persistent_term:get(server_timezone_with_offset, get_current_timezone()).

get_server_timezone() ->
  persistent_term:get(server_timezone, get_current_timezone_without_offset()).

get_timezone_options() ->
  Output    = string:trim(os:cmd("timedatectl list-timezones")),
  Timezones = string:split(Output, "\n", all),
  [build_timezone(TZ) || TZ <- Timezones].

build_timezone(TZ) ->
  Offset   = string:trim(os:cmd(lists:concat(["TZ=\"", TZ, "\" date +\"UTC %:z\""]))),
  Timezone = lists:concat([TZ, " (", Offset, ")"]),
  {Timezone, Timezone}.

get_date_and_time_from_timezone([])        -> erlang:localtime();
get_date_and_time_from_timezone(undefined) -> erlang:localtime();
get_date_and_time_from_timezone(Timezone)  ->
  try  util:milliseconds_to_datetime(convert_timezone(os:system_time(millisecond), get_current_timezone_without_offset(), Timezone))
  catch
    _:Reason ->
      ?oxlog_e("Error in converting date and time from timezoneee ~p~n",[Reason]),
      erlang:localtime()
  end.

% -spec convert_timezone(integer(), binary(), binary()) -> integer().
convert_timezone(undefined, _SourceTz, _TargetTz) -> undefined;
convert_timezone({error,[]}, _SourceTz, _TargetTz) -> undefined;
convert_timezone(MilliSecs, SourceTz, TargetTz) when SourceTz =:= undefined; SourceTz =:= [] ->
  convert_timezone(MilliSecs, TargetTz, TargetTz);
convert_timezone(MilliSecs, SourceTz, TargetTz) when is_list(SourceTz), is_list(TargetTz) ->
  convert_timezone(MilliSecs, list_to_binary(SourceTz), list_to_binary(TargetTz));
convert_timezone(MilliSecs, SourceTz, TargetTz) when is_list(SourceTz) ->
  convert_timezone(MilliSecs, list_to_binary(SourceTz), TargetTz);
convert_timezone(MilliSecs, SourceTz, TargetTz) when is_list(TargetTz) ->
  convert_timezone(MilliSecs, SourceTz, list_to_binary(TargetTz));
convert_timezone(MilliSecs, _SourceTz, TargetTz) ->
  %% 1. Split milliseconds into seconds + remainder
  Secs = MilliSecs div 1000,
  Ms   = MilliSecs rem 1000,
  %% 2. Convert UNIX epoch seconds -> Gregorian datetime.
  %%    This is already the true UTC datetime -- a Unix epoch has no
  %%    timezone ambiguity, so no local2utc step is needed here.
  GregorianSecs = Secs + 62167219200,
  UtcDT = calendar:gregorian_seconds_to_datetime(GregorianSecs),
  %% 3. Convert UTC -> target timezone
  {ok, TargetDT} = elocaltime:utc2local_datetime(UtcDT, TargetTz),
  %% 4. Convert target datetime -> UNIX epoch seconds
  TargetGregorianSecs = calendar:datetime_to_gregorian_seconds(TargetDT),
  TargetSecs = TargetGregorianSecs - 62167219200,
  %% 5. Reattach milliseconds
  TargetSecs * 1000 + Ms.

get_campaign_instance_id(TucTimezone) ->
  ServerTimezone        = get_server_timezone(),
  CampaignInstanceId    = convert_timezone(os:system_time(millisecond), ServerTimezone, TucTimezone), %% converting it to Tuc Timezone
  CampaignInstanceIdUTC = convert_timezone(CampaignInstanceId, TucTimezone, ?UTC_ZERO_TIMEZONE), %% converting it to UTC+0 Timezone
  {CampaignInstanceId, CampaignInstanceIdUTC}.


get_server_timestamp_from_datetime_timezone(DateTime, _Timezone) when DateTime == [] orelse DateTime == undefined -> {error, []};
get_server_timestamp_from_datetime_timezone(DateTime, Timezone) ->
  Offset = get_timezone_offset_ms(Timezone),
  BaseMs = (calendar:datetime_to_gregorian_seconds(DateTime) - 62167219200) * 1000,
  BaseMs - Offset.

get_timezone_offset_ms(Timezone) ->
  case util:milliseconds_to_datetime(convert_timezone(os:system_time(millisecond), get_current_timezone_without_offset(), Timezone)) of
    {error, _} ->
      0;
    LocalDateTime ->
      Universal    = calendar:universal_time(),
      LocalSeconds = calendar:datetime_to_gregorian_seconds(LocalDateTime),
      UTCSeconds   = calendar:datetime_to_gregorian_seconds(Universal),
      (LocalSeconds - UTCSeconds) * 1000
  end.

