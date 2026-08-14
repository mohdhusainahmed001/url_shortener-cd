%%%-------------------------------------------------------------------
%% @doc Client module for grpc service rocksbdhandler.MessageStateStore.
%% @end
%%%-------------------------------------------------------------------

%% this module was generated and should not be modified manually

-module(rocksbdhandler_message_state_store_client).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("grpcbox/include/grpcbox.hrl").

-define(is_ctx(Ctx), is_tuple(Ctx) andalso element(1, Ctx) =:= ctx).

-define(SERVICE, 'rocksbdhandler.MessageStateStore').
-define(PROTO_MODULE, 'route_guide_pb').
-define(MARSHAL_FUN(T), fun(I) -> ?PROTO_MODULE:encode_msg(I, T) end).
-define(UNMARSHAL_FUN(T), fun(I) -> ?PROTO_MODULE:decode_msg(I, T) end).
-define(DEF(Input, Output, MessageType), #grpcbox_def{service=?SERVICE,
                                                      message_type=MessageType,
                                                      marshal_fun=?MARSHAL_FUN(Input),
                                                      unmarshal_fun=?UNMARSHAL_FUN(Output)}).

%% @doc Unary RPC
-spec put(route_guide_pb:put_request()) ->
    {ok, route_guide_pb:put_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response() | {error, any()}.
put(Input) ->
    put(ctx:new(), Input, #{}).

-spec put(ctx:t() | route_guide_pb:put_request(), route_guide_pb:put_request() | grpcbox_client:options()) ->
    {ok, route_guide_pb:put_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response() | {error, any()}.
put(Ctx, Input) when ?is_ctx(Ctx) ->
    put(Ctx, Input, #{});
put(Input, Options) ->
    put(ctx:new(), Input, Options).

-spec put(ctx:t(), route_guide_pb:put_request(), grpcbox_client:options()) ->
    {ok, route_guide_pb:put_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response() | {error, any()}.
put(Ctx, Input, Options) ->
    grpcbox_client:unary(Ctx, <<"/rocksbdhandler.MessageStateStore/Put">>, Input, ?DEF(put_request, put_response, <<"rocksbdhandler.PutRequest">>), Options).

%% @doc Unary RPC
-spec get(route_guide_pb:get_request()) ->
    {ok, route_guide_pb:get_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response() | {error, any()}.
get(Input) ->
    get(ctx:new(), Input, #{}).

-spec get(ctx:t() | route_guide_pb:get_request(), route_guide_pb:get_request() | grpcbox_client:options()) ->
    {ok, route_guide_pb:get_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response() | {error, any()}.
get(Ctx, Input) when ?is_ctx(Ctx) ->
    get(Ctx, Input, #{});
get(Input, Options) ->
    get(ctx:new(), Input, Options).

-spec get(ctx:t(), route_guide_pb:get_request(), grpcbox_client:options()) ->
    {ok, route_guide_pb:get_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response() | {error, any()}.
get(Ctx, Input, Options) ->
    grpcbox_client:unary(Ctx, <<"/rocksbdhandler.MessageStateStore/Get">>, Input, ?DEF(get_request, get_response, <<"rocksbdhandler.GetRequest">>), Options).

%% @doc Unary RPC
-spec delete(route_guide_pb:delete_request()) ->
    {ok, route_guide_pb:delete_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response() | {error, any()}.
delete(Input) ->
    delete(ctx:new(), Input, #{}).

-spec delete(ctx:t() | route_guide_pb:delete_request(), route_guide_pb:delete_request() | grpcbox_client:options()) ->
    {ok, route_guide_pb:delete_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response() | {error, any()}.
delete(Ctx, Input) when ?is_ctx(Ctx) ->
    delete(Ctx, Input, #{});
delete(Input, Options) ->
    delete(ctx:new(), Input, Options).

-spec delete(ctx:t(), route_guide_pb:delete_request(), grpcbox_client:options()) ->
    {ok, route_guide_pb:delete_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response() | {error, any()}.
delete(Ctx, Input, Options) ->
    grpcbox_client:unary(Ctx, <<"/rocksbdhandler.MessageStateStore/Delete">>, Input, ?DEF(delete_request, delete_response, <<"rocksbdhandler.DeleteRequest">>), Options).

