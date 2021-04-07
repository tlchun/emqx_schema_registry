%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午5:08
%%%-------------------------------------------------------------------
-module(emqx_schema_api).
-author("root").
-include("../include/emqx_schema_registry.hrl").

-import(minirest, [return/1]).

%% restful 接口声明
-rest_api(#{descr => "Create a schema", func => create_schema, method => 'POST', name => create_schema, path => "/schemas/"}).
%% 方案列表
-rest_api(#{descr => "List all schemas", func => list_all_schemas, method => 'GET', name => list_all_schemas, path => "/schemas/"}).
%% 获取方案的描述
-rest_api(#{descr => "Get schemas by name", func => show_schema, method => 'GET', name => show_schema, path => "/schemas/:bin:name"}).
%% 删除方案
-rest_api(#{descr => "Delete a schema", func => delete_schema, method => 'DELETE', name => delete_schema, path => "/schemas/:bin:name"}).

-export([create_schema/2, list_all_schemas/2, show_schema/2, delete_schema/2]).
-export([make_tcp_addr/1]).
-export([format_schema/1, make_schema_params/1]).

%% 创建方案
create_schema(_Bindings, Params) ->
  try emqx_schema_registry:add_schema(make_schema_params(Params)) of
%%    成功
    {ok, Schema} -> return({ok, format_schema(Schema)});
%%    失败
    {error, Reason} ->
      return({error,
        400,
        iolist_to_binary(["Bad Arguments",
          ": ",
          io_lib:format("~0p", [Reason])])})
  catch
    error:{emqx_schema_badarg, Reason} ->
      return({error, 400, iolist_to_binary(["Bad Arguments", ": ", io_lib:format("~0p", [Reason])])});
    Error:Reason:Trace ->
      logger:error("[Schema API] Create schema failed: ~p", [{Error, Reason, Trace}]),
      return({error, 500, iolist_to_binary(["Internal error", ": ", io_lib:format("~0p", [Reason])])})
  end.

list_all_schemas(_Bindings, _Params) ->
  try Schemas = emqx_schema_registry:get_all_schemas(),
  return({ok, [format_schema(S) || S <- Schemas]})
  catch
    error:{emqx_schema_badarg, Reason} ->
      return({error, 400, iolist_to_binary(["Bad Arguments", ": ", io_lib:format("~0p", [Reason])])});
    Error:Reason:Trace ->
      logger:error("[Schema API] Get schema failed: ~p", [{Error, Reason, Trace}]),
      return({error, 500, iolist_to_binary(["Internal error", ": ", io_lib:format("~0p", [Reason])])})
  end.

%% 通过名字获取方案
show_schema(#{name := Name}, _Params) ->
  try emqx_schema_registry:get_schema(Name) of
    {ok, Schema} -> return({ok, format_schema(Schema)});
    {error, not_found} ->
      return({error, 404, list_to_binary(io_lib:format("~s Not Found", [Name]))});
    {error, Reason} ->
      return({error, 400, iolist_to_binary(["Bad Arguments", ": ", io_lib:format("~0p", [Reason])])})
  catch
    error:{emqx_schema_badarg, Reason} ->
      return({error, 400, iolist_to_binary(["Bad Arguments", ": ", io_lib:format("~0p", [Reason])])});
    Error:Reason:Trace ->
      logger:error("[Schema API] Get schema ~p failed: ~p", [Name, {Error, Reason, Trace}]),
      return({error, 500, iolist_to_binary(["Internal error", ": ", io_lib:format("~0p", [Reason])])})
  end.

%% 删除方案
delete_schema(#{name := Name}, _Params) ->
  try ok = emqx_schema_registry:delete_schema(Name),
  return(ok)
  catch
    error:{emqx_schema_badarg, Reason} ->
      return({error, 400, iolist_to_binary(["Bad Arguments", ": ", io_lib:format("~0p", [Reason])])});
    Error:Reason:Trace ->
      logger:error("[Schema API] Get schema failed: ~p",
        [{Error, Reason, Trace}]),
      return({error, 500, iolist_to_binary(["Internal error", ": ", io_lib:format("~0p", [Reason])])})
  end.

-spec make_tcp_addr(string()) -> {tcp, inet:hostname() | inet:ip_address(), inet:port_number()}.
make_tcp_addr(Address) ->
  try [Host, Port] = string:split(Address, ":"),
  {ok, Host0} = inet:parse_address(str(Host)),
  {tcp, Host0, binary_to_integer(Port)}
  catch
    _C:_R ->
      error({emqx_schema_badarg, {invalid_tcp_addr, Address}})
  end.

make_schema_params(Params) when is_list(Params) ->
  make_schema_params(maps:from_list(Params));
make_schema_params(Params = #{<<"name">> := Name, <<"parser_type">> := <<"3rd-party">>}) ->
  ParserAddr = maps:get(<<"parser_addr">>, Params, undefined),
  ParserOpts = maps:get(<<"parser_opts">>, Params, undefined),
  Descr = maps:get(<<"description">>, Params, <<>>),
  do_make_schema_params(Name, <<"3rd-party">>, <<>>, ParserAddr, ParserOpts, Descr);

make_schema_params(Params = #{<<"name">> := Name, <<"parser_type">> := ParserType, <<"schema">> := Schema}) ->
  ParserOpts = maps:get(<<"parser_opts">>, Params, undefined),
  Descr = maps:get(<<"description">>, Params, <<>>),
  do_make_schema_params(Name, ParserType, Schema, undefined, ParserOpts, Descr);
make_schema_params(Params) ->
  [case maps:is_key(Field, Params) of
     false ->
       error({emqx_schema_badarg, {required_params_missing, Field}});
     true -> ok
   end
    || Field
    <- [<<"name">>, <<"parser_type">>, <<"schema">>]].

do_make_schema_params(Name, ParserType, Schema, ParserAddr, ParserOpts, Descr) ->
  #{name => Name,
    parser_type => ensure_parser_type(ParserType),
    schema => ensure_schema(ParserType, Schema),
    parser_addr => make_parser_addr(ParserType, ParserAddr),
    parser_opts => make_parser_opts(ParserType, ParserOpts),
    descr => Descr}.

make_parser_addr(<<"3rd-party">>, undefined) ->
  error({emqx_schema_badarg,{required_params_missing, parser_addr}});
make_parser_addr(_, undefined) -> undefined;
make_parser_addr(_, ParserAddr) when is_list(ParserAddr) ->
  make_parser_addr(maps:from_list(ParserAddr));
make_parser_addr(_, ParserAddr) ->
  make_parser_addr(ParserAddr).

make_parser_addr(#{<<"server">> := TcpAddr}) ->
  make_tcp_addr(TcpAddr);
make_parser_addr(#{<<"resource_id">> := ResourceId}) ->
  {resource_id, ResourceId};
make_parser_addr(#{<<"url">> := Url}) ->
  {http, str(Url)};
make_parser_addr(#{<<"host">> := Host, <<"port">> := Port}) ->
  {ok, Host0} = inet:parse_address(str(Host)),
  {tcp, Host0, Port};
make_parser_addr(ParserAddr) ->
  error({emqx_schema_badarg, {invalid_addr, ParserAddr}}).

make_parser_opts(_, undefined) -> #{};
make_parser_opts(_, ParserOpts) ->
  do_make_parser_opts(ParserOpts, #{}).

do_make_parser_opts(ParserOpts, Opts) when is_map(ParserOpts) ->
  do_make_parser_opts(maps:to_list(ParserOpts), Opts);
do_make_parser_opts([], Opts) -> Opts;
do_make_parser_opts([{<<"3rd_party_opts">>, Val} | ParserOpts], Opts) ->
  validate_3rd_party_opts(Val),
  do_make_parser_opts(ParserOpts, Opts#{'3rd_party_opts' => Val});
do_make_parser_opts([{<<"connect_timeout">>, Val} | ParserOpts], Opts) ->
  do_make_parser_opts(ParserOpts, Opts#{connect_timeout => int(Val)});
do_make_parser_opts([{<<"parse_timeout">>, Val} | ParserOpts], Opts) ->
  do_make_parser_opts(ParserOpts, Opts#{parse_timeout => int(Val)});
do_make_parser_opts([_Opt | ParserOpts], Opts) ->
  do_make_parser_opts(ParserOpts, Opts);
do_make_parser_opts(ParserOpts, _Opts) ->
  error({emqx_schema_badarg,
    {invalid_parser_opts, ParserOpts}}).

ensure_parser_type(<<"avro">>) -> avro;
ensure_parser_type(<<"protobuf">>) -> protobuf;
ensure_parser_type(<<"json">>) -> json;
ensure_parser_type(<<"3rd-party">>) -> '3rd-party';
ensure_parser_type(T) ->
  error({emqx_schema_badarg, {unknown_parser_type, T}}).

validate_3rd_party_opts(Opts) ->
  if byte_size(Opts) > 255 ->
    error({emqx_schema_badarg, {'3rd_party_opts', {too_long, Opts}}});
    true -> ok
  end.

ensure_schema(<<"avro">>, Schema) when is_binary(Schema) ->
  Schema;
ensure_schema(<<"avro">>, Schema) ->
  try
    emqx_json:encode(Schema)
  catch
    _C:_R ->
      error({emqx_schema_badarg,{invalid_avro_schema, Schema}})
  end;
ensure_schema(_, Schema) when is_binary(Schema) ->
  Schema;
ensure_schema(_, _Schema) ->
  error({emqx_schema_badarg, {invalid_schema, must_be_string}}).

format_schema(#schema{name = Name, schema = SchemaBin,
  parser_type = ParserType, parser_addr = ParserAddr,
  parser_opts = ParserOpts, descr = Descr}) ->
  [{name, Name},
    {schema, SchemaBin},
    {parser_type, ParserType},
    {parser_addr, format_parser_addr(ParserAddr)},
    {parser_opts, ParserOpts},
    {descr, Descr}].

format_parser_addr(undefined) -> null;
format_parser_addr({resource_id, ResId}) ->
  #{<<"resource_id">> => ResId};
format_parser_addr({http, Url}) ->
  #{<<"url">> => bin(Url)};
format_parser_addr({tcp, Host, Port}) ->
  #{<<"host">> => bin(inet:ntoa(Host)),
    <<"port">> => Port}.

int(Str) when is_list(Str) -> list_to_integer(Str);
int(Bin) when is_binary(Bin) -> binary_to_integer(Bin);
int(Float) when is_float(Float) -> round(Float);
int(Int) when is_integer(Int) -> Int.

str(Str) when is_list(Str) -> Str;
str(Atom) when is_atom(Atom) -> atom_to_list(Atom);
str(Bin) when is_binary(Bin) -> binary_to_list(Bin).

bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Bin) when is_binary(Bin) -> Bin.
