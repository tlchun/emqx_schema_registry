%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午5:08
%%%-------------------------------------------------------------------
-module(emqx_schema_3rd_party).
-author("root").
-include("../include/emqx_schema_registry.hrl").

%%
-export([on_resource_create_http/2, on_get_resource_status_http/2, on_resource_destroy_http/2]).

-export([on_resource_create_tcp/2, on_get_resource_status_tcp/2, on_resource_destroy_tcp/2]).

-export([open/3,
  close/1,
  make_decoder/3,
  make_encoder/3]).

-export([connect/1]).

-export([result_code_text/1]).

-type parser_handler() :: gen_tcp:socket() | tuple().

-resource_type(#{create => on_resource_create_http,
  description =>
  #{en =>
  <<80, 97, 114, 115, 101, 114, 32, 72, 84, 84, 80>>,
    zh =>
    <<72, 84, 84, 80, 32, 231, 188, 150, 232, 167, 163,
      231, 160, 129>>},
  destroy => on_resource_destroy_http,
  name => parser_http,
  params =>
  #{connect_timeout =>
  #{default => 3,
    description =>
    #{en =>
    <<67, 111, 110, 110, 101, 99, 116, 32,
      84, 105, 109, 101, 111, 117, 116, 32,
      105, 110, 32, 83, 101, 99, 111, 110,
      100, 115>>,
      zh =>
      <<232, 191, 158, 230, 142, 165, 232, 182,
        133, 230, 151, 182, 40, 231, 167, 146,
        41>>},
    title =>
    #{en =>
    <<67, 111, 110, 110, 101, 99, 116, 32,
      84, 105, 109, 101, 111, 117, 116>>,
      zh =>
      <<232, 191, 158, 230, 142, 165, 232, 182,
        133, 230, 151, 182>>},
    type => number},
    headers =>
    #{default => #{},
      description =>
      #{en =>
      <<82, 101, 113, 117, 101, 115, 116, 32,
        72, 101, 97, 100, 101, 114>>,
        zh =>
        <<232, 175, 183, 230, 177, 130, 229, 164,
          180>>},
      schema => #{},
      title =>
      #{en =>
      <<82, 101, 113, 117, 101, 115, 116, 32,
        72, 101, 97, 100, 101, 114>>,
        zh =>
        <<232, 175, 183, 230, 177, 130, 229, 164,
          180>>},
      type => object},
    method =>
    #{default => <<80, 79, 83, 84>>,
      description =>
      #{en =>
      <<82, 101, 113, 117, 101, 115, 116, 32,
        77, 101, 116, 104, 111, 100>>,
        zh =>
        <<232, 175, 183, 230, 177, 130, 230, 150,
          185, 230, 179, 149>>},
      enum => [<<80, 85, 84>>, <<80, 79, 83, 84>>],
      title =>
      #{en =>
      <<82, 101, 113, 117, 101, 115, 116, 32,
        77, 101, 116, 104, 111, 100>>,
        zh =>
        <<232, 175, 183, 230, 177, 130, 230, 150,
          185, 230, 179, 149>>},
      type => string},
    parser_url =>
    #{description =>
    #{en =>
    <<72, 84, 84, 80, 32, 85, 82, 76, 32,
      111, 102, 32, 84, 104, 101, 32, 80, 97,
      114, 115, 101, 114, 32, 83, 101, 114,
      118, 101, 114>>,
      zh =>
      <<231, 188, 150, 232, 167, 163, 231, 160,
        129, 230, 156, 141, 229, 138, 161, 231,
        154, 132, 32, 85, 82, 76>>},
      format => url, required => true,
      title =>
      #{en =>
      <<82, 101, 113, 117, 101, 115, 116, 32,
        85, 82, 76>>,
        zh =>
        <<232, 175, 183, 230, 177, 130, 32, 85,
          82, 76>>},
      type => string}},
  status => on_get_resource_status_http,
  title =>
  #{en =>
  <<80, 97, 114, 115, 101, 114, 32, 72, 84, 84, 80>>,
    zh =>
    <<72, 84, 84, 80, 32, 231, 188, 150, 232, 167, 163,
      231, 160, 129>>}}).

-resource_type(#{create => on_resource_create_tcp,
  description =>
  #{en =>
  <<80, 97, 114, 115, 101, 114, 32, 84, 67, 80, 32>>,
    zh =>
    <<84, 67, 80, 32, 231, 188, 150, 232, 167, 163, 231,
      160, 129>>},
  destroy => on_resource_destroy_tcp, name => parser_tcp,
  params =>
  #{connect_timeout =>
  #{default => 3,
    description =>
    #{en =>
    <<67, 111, 110, 110, 101, 99, 116, 32,
      84, 105, 109, 101, 111, 117, 116, 32,
      105, 110, 32, 83, 101, 99, 111, 110,
      100, 115>>,
      zh =>
      <<232, 191, 158, 230, 142, 165, 232, 182,
        133, 230, 151, 182, 40, 231, 167, 146,
        41>>},
    title =>
    #{en =>
    <<67, 111, 110, 110, 101, 99, 116, 32,
      84, 105, 109, 101, 111, 117, 116>>,
      zh =>
      <<232, 191, 158, 230, 142, 165, 232, 182,
        133, 230, 151, 182>>},
    type => number},
    server =>
    #{description =>
    #{en =>
    <<73, 80, 32, 65, 100, 100, 114, 101,
      115, 115, 32, 97, 110, 100, 32, 80,
      111, 114, 116, 32, 111, 102, 32, 84,
      67, 80, 32, 80, 97, 114, 115, 101,
      114>>,
      zh =>
      <<231, 188, 150, 232, 167, 163, 231, 160,
        129, 230, 156, 141, 229, 138, 161, 231,
        154, 132, 32, 73, 80, 32, 229, 146,
        140, 231, 171, 175, 229, 143, 163>>},
      required => true,
      title =>
      #{en =>
      <<84, 67, 80, 32, 80, 97, 114, 115, 101,
        114, 32, 83, 101, 114, 118, 101, 114>>,
        zh =>
        <<84, 67, 80, 32, 231, 188, 150, 232,
          167, 163, 231, 160, 129, 230, 156, 141,
          229, 138, 161, 229, 156, 176, 229, 157,
          128>>},
      type => string}},
  status => on_get_resource_status_tcp,
  title =>
  #{en => <<80, 97, 114, 115, 101, 114, 32, 84, 67, 80>>,
    zh =>
    <<84, 67, 80, 32, 231, 188, 150, 232, 167, 163, 231,
      160, 129>>}}).

-spec on_resource_create_http(binary(), map()) -> map().
on_resource_create_http(ResId, Conf) ->
  {Addr, ParserOpts} = parse_action_params_http(Conf),
  #{parser_handler => open(ResId, Addr, ParserOpts), parser_opts => ParserOpts}.

-spec on_get_resource_status_http(binary(),
    map()) -> map().

on_get_resource_status_http(_ResId,
    #{parser_handler := {http, Url},
      parser_opts := ParserOpts}) ->
  ConnTimeout = maps:get(connect_timeout, ParserOpts, 3),
  #{is_alive =>
  check_connectivity(http,
    Url,
    timer:seconds(ConnTimeout))}.

-spec on_resource_destroy_http(binary(), map()) -> ok |
{error, Reason :: term()}.

on_resource_destroy_http(_ResId,
    #{parser_handler := ParseHandler}) ->
  close(ParseHandler).

-spec on_resource_create_tcp(binary(), map()) -> map().

on_resource_create_tcp(ResId, Conf) ->
  {Addr, ParserOpts} = parse_action_params_tcp(Conf),
  #{parser_handler => open(ResId, Addr, ParserOpts),
    parser_opts => ParserOpts}.

-spec on_get_resource_status_tcp(binary(),
    map()) -> map().

on_get_resource_status_tcp(_ResId,
    #{parser_handler := {tcp, PoolName}}) ->
  #{is_alive => check_connectivity(tcp, PoolName)}.

-spec on_resource_destroy_tcp(binary(), map()) -> ok |
{error, Reason :: term()}.

on_resource_destroy_tcp(_ResId,
    #{parser_handler := ParseHandler}) ->
  close(ParseHandler).

-spec open(binary(), parser_addr(),
    parser_opts()) -> parser_handler().

open(_Name, {http, Url} = Address, ParserOpts) ->
  ConnTimeout = maps:get(connect_timeout, ParserOpts, 3),
  true = check_connectivity(http,
    Url,
    timer:seconds(ConnTimeout)),
  Address;
open(Name, {tcp, Host, Port}, ParserOpts) ->
  {ok, _} = application:ensure_all_started(ecpool),
  start_pool(pool_name(Name),
    [{host, Host},
      {port, Port},
      {parse_opts, ParserOpts},
      {pool_size, 4},
      {auto_reconnect, 15}]).

-spec close(parser_addr()) -> ok | {error, term()}.

close({http, _Url}) -> ok;
close({tcp, PoolName}) ->
  ecpool:stop_sup_pool(PoolName).

-spec make_decoder(parser_handler(), schema_name(),
    parser_opts()) -> decoder().

make_decoder({http, Url}, SchemaName, ParserOpts) ->
  emqx_schema_http:make_parser(decode,
    Url,
    SchemaName,
    ParserOpts);
make_decoder({tcp, PoolName}, SchemaName, ParserOpts) ->
  emqx_schema_tcp:make_parser(decode,
    PoolName,
    SchemaName,
    ParserOpts).

-spec make_encoder(parser_handler(), schema_name(),
    parser_opts()) -> decoder().

make_encoder({http, Url}, SchemaName, ParserOpts) ->
  emqx_schema_http:make_parser(encode,
    Url,
    SchemaName,
    ParserOpts);
make_encoder({tcp, PoolName}, SchemaName, ParserOpts) ->
  emqx_schema_tcp:make_parser(encode,
    PoolName,
    SchemaName,
    ParserOpts).

check_connectivity(http, Url, Timeout) ->
  case emqx_schema_http:status(Url, Timeout) of
    connected -> true;
    _ -> false
  end.

check_connectivity(tcp, PoolName) ->
  Status = [ecpool_worker:is_connected(Worker)
    || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
  lists:any(fun (St) -> St =:= true end, Status).

parse_action_params_http(Params = #{<<"parser_url">> :=
Url}) ->
  try {{http, str(Url)},
    #{headers =>
    headers(maps:get(<<"headers">>, Params, undefined)),
      method =>
      method(maps:get(<<"method">>, Params, <<"POST">>)),
      connect_timeout =>
      int(maps:get(<<"connect_timeout">>, Params, 3))}}
  catch
    _:_ ->
      error({emqx_schema_badarg, {invalid_params, Params}})
  end.

parse_action_params_tcp(Params = #{<<"server">> :=
Server0}) ->
  Server = emqx_schema_api:make_tcp_addr(Server0),
  try {Server,
    #{connect_timeout =>
    int(maps:get(<<"connect_timeout">>, Params, 3))}}
  catch
    _:_ ->
      error({emqx_schema_badarg, {invalid_params, Params}})
  end.

method(GET) when GET == <<"GET">>; GET == <<"get">> ->
  get;
method(POST)
  when POST == <<"POST">>; POST == <<"post">> ->
  post;
method(PUT) when PUT == <<"PUT">>; PUT == <<"put">> ->
  put;
method(DEL)
  when DEL == <<"DELETE">>; DEL == <<"delete">> ->
  delete.

headers(undefined) -> [];
headers(Headers) when is_map(Headers) ->
  maps:fold(fun (K, V, Acc) -> [{str(K), str(V)} | Acc]
            end,
    [],
    Headers).

str(Str) when is_list(Str) -> Str;
str(Atom) when is_atom(Atom) -> atom_to_list(Atom);
str(Bin) when is_binary(Bin) -> binary_to_list(Bin).

int(Str) when is_list(Str) -> list_to_integer(Str);
int(Bin) when is_binary(Bin) -> binary_to_integer(Bin);
int(Float) when is_float(Float) -> round(Float);
int(Int) when is_integer(Int) -> Int.

result_code_text(1) -> <<"CODE_SUCCESS">>;
result_code_text(2) -> <<"CODE_BAD_FORMAT">>;
result_code_text(3) -> <<"CODE_INTERNAL">>;
result_code_text(4) -> <<"CODE_UNKNOWN">>.

start_pool(PoolName, Options) ->
  case ecpool:start_sup_pool(PoolName,
    emqx_schema_3rd_party,
    Options)
  of
    {ok, _} -> {tcp, PoolName};
    {error, {already_started, _Pid}} ->
      ecpool:stop_sup_pool(PoolName),
      start_pool(PoolName, Options);
    {error, Reason} ->
      logger:error("Start tcp parser ~p failed, pool: ~p, "
      "~0p",
        [PoolName, Reason]),
      error({start_tcp_parser, Reason})
  end.

pool_name(ResId) ->
  list_to_atom("backend_mysql:" ++ str(ResId)).

connect(Options) ->
  Host = proplists:get_value(host, Options),
  Port = proplists:get_value(port, Options),
  ParseOpts = proplists:get_value(parse_opts, Options),
  emqx_schema_tcp:start_link(Host, Port, ParseOpts).

