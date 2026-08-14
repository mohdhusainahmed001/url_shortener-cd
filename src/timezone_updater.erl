-module(timezone_updater).
-behaviour(gen_server).
-export([start_link/0]).
-export([init/1, handle_info/2, handle_call/3, handle_cast/2, terminate/2, code_change/3]).
-export([update_timezones/0]).

-define(TIMEZONE_OPTIONS, timezone_options).
-define(SERVER_TIMEZONE, server_timezone).
-define(SERVER_TIMEZONE_WITH_OFFSET, server_timezone_with_offset).
-define(HOUR, 2).
%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
  update_server_timezone(),
  {ok, #{}}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(update_timezones, State) ->
  update_timezones(),
  schedule_next_update(),
  {noreply, State};

handle_info(_, State) ->
  {noreply, State}.

terminate(_, _) ->
  ok.

code_change(_, State, _) ->
  {ok, State}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

update_timezones() ->
  try
    Timezones = timezone_util:get_timezone_options(),
    persistent_term:put(?TIMEZONE_OPTIONS, Timezones),
    logger:info("Timezone list updated successfully.")
    catch
      Class:Reason:Stack ->
      logger:error("Failed to update timezone list: ~p:~p~n~p", [Class, Reason, Stack])
  end.

schedule_next_update() ->
  Milliseconds = milliseconds_until_next_2am(),
  erlang:send_after(Milliseconds, self(), update_timezones).

milliseconds_until_next_2am() ->
  {{Y, M, D}, {Hour, Min, Sec}} = calendar:local_time(),
  Current  = calendar:datetime_to_gregorian_seconds( {{Y, M, D}, {Hour, Min, Sec}}),
  Today2AM = calendar:datetime_to_gregorian_seconds({{Y, M, D}, {?HOUR, 0, 0}}),
  Target   =
    case Current < Today2AM of
      true  -> Today2AM;
      false ->
        Tomorrow = calendar:gregorian_days_to_date(calendar:date_to_gregorian_days({Y, M, D}) + 1),
        calendar:datetime_to_gregorian_seconds({Tomorrow, {?HOUR, 0, 0}})
    end,
  (Target - Current) * 1000.

update_server_timezone() ->
  persistent_term:put(?SERVER_TIMEZONE_WITH_OFFSET, timezone_util:get_current_timezone()),
  persistent_term:put(?SERVER_TIMEZONE, timezone_util:get_current_timezone_without_offset()).

