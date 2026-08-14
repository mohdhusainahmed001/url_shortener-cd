% This gen_server sends the put request on the /publish endpoint
-module(registry_manager).

-behaviour(gen_server).

%% API
-export([stop/0, start_link/0, send_request/1, fetch_registry_data/0]).

%% GenCallBacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("onex.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%% API %%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
  gen_server:stop(?MODULE).

send_request(Data) ->
  gen_server:call(?MODULE, {send_request, Data}).

fetch_registry_data() ->
  gen_server:call(?MODULE,fetch_registry_data).

%%%%%%%%%%%%% Genserver Callbacks %%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
  State        = init_config(),
  UpdatedState = connect_and_update_state(State),
  erlang:send_after(?REGISTRY_TIME_TO_REFRESH, self(), refresh),
  {ok, UpdatedState}.

handle_call({send_request, Data}, _From, #{connect_status := active} = State) ->
  % Data contains time_to_refresh, meta and status
  % Update state with new data if provided
  case registry_helper:ensure_connection(State) of
    {error,connection_failed} ->
      {reply, {error, "Failed to Establish Connection with Registry~n"}, State};
    _                         ->
      NewState = update_state(Data, State),
      Body     = get_body(NewState),
      case registry_helper:send(Body, NewState) of
        {ok, 200, _M}         ->
          ?oxlog_i("Successfully updated status with Registry~n", []),
          {reply, {ok, 200}, NewState};
        {ok, StatusCode, Msg} ->
          {reply,{ok, StatusCode, Msg}, NewState};
        {error, Reason}       ->
          ?oxlog_e("Error: PUT Request to registry failed with Reason: ~p~n", [Reason]),
          {reply, {error, Reason}, NewState};
        {inactive, UpdatedState} ->
          {reply, ok, UpdatedState}
      end
  end;
handle_call({send_request, Data}, _From, #{connect_status := inactive} = State) ->
  UpdatedState = connect_and_update_state(State),
  send_request(Data),
  {reply, ok, UpdatedState};
handle_call(fetch_registry_data, _From, #{connect_status := active} = State) ->
  case registry_helper:ensure_connection(State) of
    {error,connection_failed} ->
      {reply, {error, "Failed to Establish Connection with Registry~n"}, State};
    NewConnPid                ->
      ParsedData = registry_helper:fetch_registry(NewConnPid, "all", "all", "all"),
      {reply, ParsedData, State}
  end;

handle_call(fetch_registry_data, _From,#{connect_status := inactive} = State) ->
  case registry_helper:connect(State) of
    {active, UpdatedState}->
      ConnPid    = maps:get(pid, UpdatedState),
      ParsedData = registry_helper:fetch_registry(ConnPid, "all", "all", "all"),
      {reply, ParsedData, UpdatedState};
    {inactive, _} ->
      {reply, {error, connection_inactive}, State}
  end;
handle_call(stop, _From, State)     ->
  {stop, normal, stopped, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(refresh, #{connect_status := active} = State) ->
  Data = get_body(State),
  ?oxlog_i("Registry refresh started with Data:- ~p~n", [Data]),
  {ok, UpdatedState} = registry_helper:send_request(Data, State),
  ?oxlog_i("Registry refresh done for server:- ~p~n", [?SERVER_TYPE]),
  erlang:send_after(?REGISTRY_TIME_TO_REFRESH, self(), refresh),
  {noreply, UpdatedState};
handle_info(refresh, #{connect_status := inactive} = State) ->
  UpdatedState = connect_and_update_state(State),
  erlang:send_after(?REGISTRY_TIME_TO_REFRESH, self(), refresh),
  {noreply, UpdatedState};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%%%%%%%%%%%% Internal Functions %%%%%%%%%%%%%%%%%%%%%%%
init_config() ->
  #{
    time_to_refresh => ?REGISTRY_TIME_TO_REFRESH,
    status          => ?READY_STATUS,
    % uncomment for testing
  % ip              => "127.0.0.1",
  % port            => 8888,
  % dockerid        => "123456789012",
  % meta            => #{},
    ip              => docker_func:get_container_ip(),
    port            => docker_func:get_port(),
    dockerid        => docker_func:get_container_id(),
    meta            => [],

    % gun_pid       => undefined,
    % gun_status    => inactive
    pid             => undefined,
    connect_status  => inactive
  }.

connect_and_update_state(State) ->
  case registry_helper:connect(State) of
    {inactive, _S} -> State;
    {active, NewState} -> NewState
  end.

update_state(Data, State) ->
  case Data of
    [] -> State;
    _  ->
      maps:fold(fun(Key, Value, StateAcc) ->
        case Key of
          time_to_refresh -> StateAcc#{time_to_refresh => Value};
          status          -> StateAcc#{status          => Value};
          meta            -> StateAcc#{meta            => Value};
          _Other          -> StateAcc
        end
      end, State, Data)
  end.

 get_body(State) ->
  #{
    <<"data">> => #{
      <<"ip">>              => maps:get(ip, State),
      <<"port">>            => maps:get(port, State),
      <<"type">>            => ?SERVER_TYPE,
      <<"dockerid">>        => maps:get(dockerid, State),
      <<"time_to_refresh">> => maps:get(time_to_refresh, State),
      <<"status">>          => maps:get(status, State),
      <<"meta">>            => maps:get(meta, State)
      }
  }.