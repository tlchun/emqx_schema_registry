%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午5:09
%%%-------------------------------------------------------------------
-module(emqx_schema_tcp).
-author("root").
-include(../include/emqx_schema_registry.hrl).
-behaviour(gen_statem).

-export([start_link/3, close/1, make_parser/4, status/1]).

-export([callback_mode/0,
  init/1,
  format_status/2,
  connected/3,
  terminate/3,
  code_change/4]).

-export([pack_parse_request/1, pack_parse_result/1, unpack_parse_request/1, unpack_parse_result/1]).

%% define ip address data type
-type tcp_host() :: inet:hostname() | inet:ip_address().
%% define port data type
-type tcp_port() :: inet:port_number().
%% data parse type
-type parse_type() :: decode | encode.
%% parse code rang(0~15)
-type result_code() :: 0..15.

%% parse state struct define
-record(state,
{peer :: {tcp_host(), tcp_port()},
  socket :: gen_tcp:socket(),
  connect_timeout :: integer(),
  ping_interval :: integer(),
  msg_id = 0 :: integer(),
  callers = #{} :: map()}).

%% parse request struct define 解析请求
-record(parse_request,
{parse_type :: parse_type(),
  msg_id :: integer(),
  schema_name :: schema_name(),
  parser_opts :: binary(),
  content :: binary()}).

%% parse result struct define 解析结构
-record(parse_result, {parse_type :: parse_type(), code :: result_code(), msg_id :: integer(), content :: binary()}).

-spec start_link(tcp_host(), tcp_port(), parser_opts()) -> {ok, pid()} | {error, term()}.
start_link(Host, Port, ParserOpts) ->
  gen_statem:start_link(emqx_schema_tcp, [Host, Port, ParserOpts], []).

close(Pid) -> gen_statem:stop(Pid).

-spec make_parser(parse_type(), binary(), schema_name(), parser_opts()) -> {ok, binary()} | {error, term()}.
make_parser(ParseType, PoolName, SchemaName, ParserOpts) ->
  ParseTimeout = maps:get(parse_timeout, ParserOpts, 3),
  Opts = maps:get('3rd_party_opts', ParserOpts, <<>>),
  fun (Data) ->
    ecpool:with_client(PoolName,
      fun (Pid) ->
        case gen_statem:call(Pid,
          {ParseType, SchemaName, Opts, Data},
          {dirty_timeout, timer:seconds(ParseTimeout)})
        of
          {ok, Result} -> Result;
          {error, Reason} ->
            error({emqx_schema_tcp, Reason})
        end
      end)
  end.

status(Pid) ->
  gen_statem:call(Pid, status, {dirty_timeout, 1000}).

-spec pack_parse_request(#parse_request{}) -> binary().
pack_parse_request(#parse_request{parse_type = ParseType, schema_name = SchemaName, parser_opts = Opts, msg_id = MsgId, content = Data}) ->
  MsgType = message_type(ParseType),
  Code = 0,
  LenSchemaId = byte_size(SchemaName),
  LenOpts = byte_size(Opts),
  <<MsgType:4, Code:4, MsgId:32, LenSchemaId:16, SchemaName:LenSchemaId/binary, LenOpts:16, Opts:LenOpts/binary, Data/binary>>.

-spec pack_parse_result(#parse_result{}) -> binary().
pack_parse_result(#parse_result{parse_type = ParseType, msg_id = MsgId, code = Code, content = Data}) ->
  MsgType = message_type(ParseType),
  <<MsgType:4, Code:4, MsgId:32, Data/binary>>.

-spec unpack_parse_request(binary()) -> #parse_request{}.
unpack_parse_request(<<MsgType:4, 0:4, MsgId:32, LenSchemaId:16, SchemaName:LenSchemaId/binary, LenOpts:16, Opts:LenOpts/binary, Data/binary>>) ->
  {ok, #parse_request{parse_type = parse_type(MsgType), schema_name = SchemaName, parser_opts = Opts, msg_id = MsgId, content = Data}};
unpack_parse_request(Data) ->
  {error, {bad_formatted_parse_request, Data}}.

-spec unpack_parse_result(binary()) -> #parse_result{}.
unpack_parse_result(<<_MsgType:4, Code:4, MsgId:32, Data/binary>>) ->
  {ok, #parse_result{msg_id = MsgId, code = Code, content = Data}};
unpack_parse_result(Data) ->
  {error, {bad_formatted_result, Data}}.

callback_mode() -> [state_functions, state_enter].

init([Host, Port, ParserOpts]) ->
  ConnTimeout = maps:get(connect_timeout, ParserOpts, 3),
  case do_connect(Host, Port, ConnTimeout) of
    {ok, Sock} ->
      {ok,
        connected,
        #state{peer = {Host, Port}, socket = Sock,
          connect_timeout = ConnTimeout,
          ping_interval =
          maps:get(ping_interval, ParserOpts, 30)}};
    {error, Reason} -> {stop, Reason}
  end.

connected(enter, _OldState, State = #state{ping_interval = Interval}) ->
  logger:log(debug, "[~p] [~p] " ++ "connected, ping-interval: ~p",
    [emqx_schema_tcp, State#state.peer, Interval]),
  {keep_state_and_data,
    [{state_timeout, timer:seconds(Interval), ping}]};
connected(state_timeout, ping, #state{socket = Sock}) ->
  ok = send_ping(Sock),
  keep_state_and_data;
connected({call, Caller}, status, _Data) ->
  {keep_state_and_data, [{reply, Caller, connected}]};
connected({call, Caller}, {ParseType, SchemaName, Opts, Data}, State = #state{socket = Sock, msg_id = MsgId, callers = Callers}) when ParseType =:= decode; ParseType =:= encode ->
  logger:log(debug,
    "[~p] [~p] " ++ "sent: ~p",
    [emqx_schema_tcp, State#state.peer, Data]),
  send_parse_req(#parse_request{parse_type = ParseType,
    schema_name = SchemaName, parser_opts = Opts,
    msg_id = MsgId, content = Data},
    Sock),
  {keep_state, State#state{msg_id = next_msgid(MsgId), callers = Callers#{MsgId => Caller}}};
connected(T, C, D) -> handle_common(T, C, D).

terminate(_Reason, _State, _Data) -> ok.

code_change(_OldVsn, State, Data, _Extra) ->
  {ok, State, Data}.

format_status(_Opt, [_PDict, State, Data]) ->
  [{data, [{"State", {State, Data}}]}].

handle_common(info, {tcp_error, Sock, Reason},
    State = #state{socket = Sock}) ->
  logger:log(error,
    "[~p] [~p] " ++ "connection disconnected: ~p",
    [emqx_schema_tcp, State#state.peer, Reason]),
  {stop, {shutdown, {tcp_error, Reason}}};
handle_common(info, {tcp_closed, Sock},
    State = #state{socket = Sock}) ->
  logger:log(info,
    "[~p] [~p] " ++ "connection closed",
    [emqx_schema_tcp, State#state.peer]),
  {stop, {shutdown, tcp_closed}};
handle_common(info, {tcp, Sock, Data}, State = #state{socket = Sock, callers = Callers}) ->
  case unpack_parse_result(Data) of
    {ok,
      #parse_result{msg_id = MsgId, code = 1,
        content = Content}} ->
      logger:log(debug,
        "[~p] [~p] " ++ "received parser result: ~p",
        [emqx_schema_tcp, State#state.peer, Content]),
      {Caller, Callers0} = maps:take(MsgId, Callers),
      {keep_state,
        State#state{callers = Callers0},
        [{reply, Caller, {ok, Content}}]};
    {ok,
      #parse_result{msg_id = MsgId, code = Code,
        content = Content}} ->
      logger:log(error,
        "[~p] [~p] " ++ "received parser error result: ~p",
        [emqx_schema_tcp, State#state.peer, Content]),
      {Caller, Callers0} = maps:take(MsgId, Callers),
      Result = {error,
        {emqx_schema_3rd_party:result_code_text(Code),
          Content}},
      {keep_state,
        State#state{callers = Callers0},
        [{reply, Caller, Result}]};
    {error, Reason} ->
      logger:log(error,
        "[~p] [~p] " ++
        "received parser bad formatted result: ~p",
        [emqx_schema_tcp, State#state.peer, Reason]),
      keep_state_and_data
  end;
handle_common(Event, Msg, State) ->
  logger:log(error,
    "[~p] [~p] " ++ "unexpected message received: ~p ~p",
    [emqx_schema_tcp, State#state.peer, Event, Msg]),
  keep_state_and_data.

do_connect(Host, Port, Timeout) ->
  gen_tcp:connect(Host, Port, [{packet, 4}, {active, true}, binary], timer:seconds(Timeout)).

send_ping(_Sock) -> ok.

send_parse_req(ParseReq, Sock) ->
  gen_tcp:send(Sock, pack_parse_request(ParseReq)),
  ok.

next_msgid(MsgId) when MsgId < 4294967295 -> MsgId + 1;
next_msgid(MsgId) when MsgId < 0; MsgId >= 4294967295 ->
  0.

message_type(decode) -> 1;
message_type(encode) -> 2;
message_type(ping) -> 3.

parse_type(1) -> decode;
parse_type(2) -> encode;
parse_type(3) -> ping.

