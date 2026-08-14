-module(shorturl_queue_reader).
-behaviour(gen_server).
-include("onex.hrl").
-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(EMPTY_QUEUE_INTERVAL, 1000).
-define(QUEUE_INTERVAL, 100).
-define(BATCH_SIZE, 900).
-define(NUM_WORKERS, 30).

%%%=================================================================================
%%% API
%%%=================================================================================
start_link() ->
    ?oxlog_i("Starting ShortUrl queue reader (single reader mode)", []),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

%%%=================================================================================
%%% GENSERVER CALLBACKS
%%%=================================================================================
init([]) ->
    %% Start worker pool
    CounterRef = counters:new(1, []),
    persistent_term:put(counterRef, CounterRef),
    erlang:send_after(10, self(), queue_reader),
    erlang:send_after(100, self(), start_readers),
    {ok, #{worker_index => 0}}.

handle_call(_Msg, _From, State) ->
    {reply, invalid_request, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(start_readers, State) ->
  start_workers(?NUM_WORKERS),
  {noreply, State};

handle_info(queue_reader, State = #{worker_index := WorkerIndex}) ->
    case shorturl_queue:dequeue_batch(?BATCH_SIZE) of
        {ok, []} ->
            erlang:send_after(?EMPTY_QUEUE_INTERVAL, self(), queue_reader),
            {noreply, State};
        {ok, Recs} ->
            NewWorkerIndex = distribute_to_workers(Recs, WorkerIndex, ?NUM_WORKERS),
            erlang:send_after(?QUEUE_INTERVAL, self(), queue_reader),
            {noreply, State#{worker_index => NewWorkerIndex}}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=================================================================================
%%% INTERNAL FUNCTIONS
%%%=================================================================================

%% Start worker processes
start_workers(NumWorkers) ->
    lists:foreach(
        fun(Index) ->
          ChildSpecMovex   =  #{id => {shorturl_worker, Index},
                      start => {shorturl_worker, start_link, [Index]},
                      restart => transient,
                      shutdown => 5000,
                      type => worker},
          case catch supervisor:start_child(url_shortener_sup, ChildSpecMovex) of
            {ok, _Pid1} -> ok;
            Else1       -> ?oxlog_e("got unknown ~p", [Else1])
          end
        end,
        lists:seq(1, NumWorkers)
    ).

%% Distribute records to workers in round-robin
distribute_to_workers(Recs, WorkerIndex, NumWorkers) ->
    WorkerNum = (WorkerIndex rem NumWorkers) + 1,
    WorkerName = list_to_atom("shorturl_worker" ++ integer_to_list(WorkerNum)),
    gen_server:cast(WorkerName, {process_batch, Recs}),
    (WorkerIndex + 1) rem NumWorkers.

%%%=================================================================================
%%% END
%%%=================================================================================
