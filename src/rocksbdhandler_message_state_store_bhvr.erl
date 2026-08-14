%%%-------------------------------------------------------------------
%% @doc Behaviour to implement for grpc service rocksbdhandler.MessageStateStore.
%% @end
%%%-------------------------------------------------------------------

%% this module was generated and should not be modified manually

-module(rocksbdhandler_message_state_store_bhvr).

%% Unary RPC
-callback put(ctx:t(), route_guide_pb:put_request()) ->
    {ok, route_guide_pb:put_response(), ctx:t()} | grpcbox_stream:grpc_error_response().

%% Unary RPC
-callback get(ctx:t(), route_guide_pb:get_request()) ->
    {ok, route_guide_pb:get_response(), ctx:t()} | grpcbox_stream:grpc_error_response().

%% Unary RPC
-callback delete(ctx:t(), route_guide_pb:delete_request()) ->
    {ok, route_guide_pb:delete_response(), ctx:t()} | grpcbox_stream:grpc_error_response().

