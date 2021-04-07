%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午5:09
%%%-------------------------------------------------------------------
-module(emqx_schema_registry).
-author("root").
-include("../include/emqx_schema_registry.hrl").

-behaviour(gen_server).
-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([mnesia/1]).
-export([start_link/0]).

-export([get_schema/1,
  get_all_schemas/0,
  add_schema/1,
  delete_schema/1,
  clear_schema/0,
  sort_schema/2,
  get_parser/1,
  delete_parser/1,
  build_parser/5]).

-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-record(state, {}).

-type schema_params() :: #{name := schema_name(),
parser_type := parser_type(),
schema := schema_text(),
parser_addr => parser_addr(),
parser_opts => parser_opts(), descr => binary()}.

-spec mnesia(boot | copy) -> ok.
mnesia(boot) ->
%%  存储熟悉配置
  StoreProps = [{ets, [{read_concurrency, true}]}],
%%  方案表
  ok = ekka_mnesia:create_table(emqx_schema,
    [{disc_copies, [node()]}, {record_name, schema}, {index, []}, {attributes, record_info(fields, schema)}, {storage_properties, StoreProps}]),
%%  解析表
  ok = ekka_mnesia:create_table(emqx_parser,
    [{ram_copies, [node()]},
      {record_name, parser},
      {index, []},
      {attributes, record_info(fields, parser)},
      {storage_properties, StoreProps}]);

%% 复制解析库，复制方案库
mnesia(copy) ->
  ok = ekka_mnesia:copy_table(emqx_parser),
  ok = ekka_mnesia:copy_table(emqx_schema).

%% 启动进程
start_link() ->
  gen_server:start_link({local, emqx_schema_registry}, emqx_schema_registry, [],[]).

%% 增加方案
-spec add_schema(schema_params()) -> {ok, #schema{}} |{error, term()}.
add_schema(Params = #{name := Name, parser_type := ParserType, schema := SchemaText}) ->
  ParserAddr = maps:get(parser_addr, Params, undefined),
  ParserOpts = maps:get(parser_opts, Params, #{}),
  Descr = maps:get(descr, Params, <<>>),
  SchemaText0 = case ParserType of
                  avro -> update_avro_name(Name, SchemaText);
                  _ -> SchemaText
                end,
  Schema = #schema{name = Name, schema = SchemaText0,
    parser_type = ParserType, parser_addr = ParserAddr,
    parser_opts = ParserOpts, descr = Descr},
  insert_schema(Name, Schema),
  {ok, Schema}.

%% 获取方案
-spec get_schema(schema_name()) -> {ok, #schema{}} |{error, not_found}.
get_schema(Name) ->
  case mnesia:dirty_read(emqx_schema, Name) of
    [Schema = #schema{}] -> {ok, Schema};
    [] -> {error, not_found}
  end.

%% 获取所有方案
-spec get_all_schemas() -> [#schema{}].
get_all_schemas() -> ets:tab2list(emqx_schema).

%% 删除方案
-spec delete_schema(schema_name()) -> ok.
delete_schema(Name) ->
  trans(fun () ->
    ok = mnesia:delete(emqx_schema, Name, write),
    delete_parser(Name)
        end).

%% 获取解析
-spec get_parser(schema_name()) -> {ok, #parser{}} |
{error, not_found}.

%% 通过名字获取解析
get_parser(Name) ->
  case ets:lookup(emqx_parser, Name) of
    [] -> {error, not_found};
    [Parser] -> {ok, Parser}
  end.

%% 删除解析
-spec delete_parser(schema_name()) -> ok.
delete_parser(SchemaName) ->
  case get_parser(SchemaName) of
    {ok, #parser{destroy = Destroy}} ->
      Destroy(),
      mnesia:dirty_delete(emqx_parser, SchemaName),
      ok;
    {error, not_found} -> ok
  end.

%% 清除方案和清除解析
-spec clear_schema() -> ok.
clear_schema() ->
  {atomic, ok} = mnesia:clear_table(emqx_schema),
  {atomic, ok} = mnesia:clear_table(emqx_parser),
  ok.

%% 排序
-spec sort_schema([#schema{}], ascending | descending) -> list().
sort_schema(Schemas, ascending) ->
  lists:sort(fun (#schema{name = A}, #schema{name = B}) ->
    A =< B
             end,
    Schemas);
sort_schema(Schemas, descending) ->
  lists:sort(fun (#schema{name = A}, #schema{name = B}) ->
    A >= B
             end,
    Schemas).

init([]) ->
  mnesia:wait_for_tables([emqx_schema], timer:seconds(15)),
  gen_server:cast(self(), rebuild_parsers),
  {ok, #state{}}.

handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

handle_cast(rebuild_parsers, State) ->
  try
    N = trans(fun rebuild_parsers/0),
    logger:info("Rebuilt ~p Parsers", [N])
  catch
    Error:Reason:Trace ->
      logger:error("Rebuilt Parsers Failed", [{Error, Reason, Trace}])
  end,
  {noreply, State};
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

insert_schema(Name, Schema = #schema{schema = SchemaText, parser_type = ParserType, parser_addr = ParserAddr, parser_opts = ParserOpts}) ->
  trans(fun () ->
    try build_parser(ParserType,
      Name,
      SchemaText,
      ParserAddr,
      ParserOpts),
    mnesia:write(emqx_schema, Schema, write)
    catch
      E:R ->
        delete_parser(Name),
        error({E, R})
    end
        end).

insert_parser(Parser = #parser{}) ->
  trans(fun () -> mnesia:write(emqx_parser, Parser, write) end),
  ok.

%% 重新构建解析
rebuild_parsers() ->
  mnesia:foldl(fun (#schema{name = Name,
    schema = SchemaText, parser_type = ParserType,
    parser_opts = ParserOpts,
    parser_addr = ParserAddr},
      Count) ->
    ok = build_parser(ParserType,
      Name,
      SchemaText,
      ParserAddr,
      ParserOpts),
    Count + 1
               end,
    0,
    emqx_schema).

%% 构建解析
build_parser(ParserType, SchemaName, SchemaText, ParserAddr, ParserOpts) ->
  {Decoder, Encoder, Destroy} = emqx_schema_parser:make_parser(ParserType, SchemaName, SchemaText, ParserAddr, ParserOpts),
  insert_parser(#parser{name = SchemaName, decoder = Decoder, encoder = Encoder, destroy = Destroy}).

trans(Fun) -> trans(Fun, []).

trans(Fun, Args) ->
  case mnesia:transaction(Fun, Args, 10) of
    {atomic, R} -> R;
    {aborted, nomore} -> error(busy_try_again);
    {aborted, Reason} -> error(Reason)
  end.

update_avro_name(Name, Schema) ->
  try Schema0 = emqx_json:decode(Schema, [return_maps]),
  emqx_json:encode(Schema0#{<<"name">> => Name})
  catch
    _C:_R ->
      error({emqx_schema_badarg,
        {invalid_avro_schema, not_json}})
  end.


