%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午5:08
%%%-------------------------------------------------------------------
-module(emqx_schema_http).
-author("root").
-include("../include/emqx_schema_registry.hrl").

-export([make_decoder/3, make_encoder/3, make_parser/4, status/2]).

make_decoder(Url, SchemaName, ParserOpts) ->
  make_parser(<<"decode">>, Url, SchemaName, ParserOpts).

make_encoder(Url, SchemaName, ParserOpts) ->
  make_parser(<<"encode">>, Url, SchemaName, ParserOpts).

make_parser(Type, Url, SchemaName, ParserOpts) ->
  ParseTimeout = maps:get(parse_timeout, ParserOpts, 5),
  Headers = maps:get(headers, ParserOpts, []),
  Method = maps:get(method, ParserOpts, post),
  Params = #{<<"type">> => Type, <<"schema_name">> => SchemaName, <<"parser_opts">> => maps:get('3rd_party_opts', ParserOpts, <<>>)},
  fun (Data) ->
    Response = http_request(Url,
      Headers,
      Method,
      Params#{<<"payload">> =>
      base64:encode(Data)},
      timer:seconds(ParseTimeout)),
    result(Response)
  end.

status(Url, Timeout) ->
  case emqx_rule_utils:http_connectivity(Url, Timeout) of
    ok -> connected;
    {error, Reason} ->
      logger:debug("[http parser] Connectivity Check for ~p failed: ~0p", [Url, Reason]),
      disconnected
  end.

http_request(Url, Headers, Method, Params, Timeout) ->
  logger:debug("[http parser] ~s to ~s, headers: ~s, "
  "body: ~p",
    [Method, Url, Headers, Params]),
  case do_http_request(Method,
    {Url,
      Headers,
      "application/json",
      emqx_json:encode(Params)},
    [{timeout, Timeout}],
    [],
    0)
  of
    {ok, {{_, _Code = 200, _OK}, _RespHeaders, RespBody}} ->
      bin(RespBody);
    {ok,
      {{_, _Code = ErrCode, Err}, _RespHeaders, RespBody}} ->
      logger:error("[http parser] HTTP request error: ~p",
        [{ErrCode, Err, RespBody}]),
      error({http_request_error, ErrCode});
    {error, Reason} ->
      logger:error("[http parser] HTTP request error: ~p",
        [Reason]),
      error({http_request_error, Reason})
  end.

do_http_request(Method, Req, HTTPOpts, Opts, Times) ->
  case httpc:request(Method, Req, HTTPOpts, Opts) of
    {error, socket_closed_remotely} when Times < 3 ->
      timer:sleep(trunc(math:pow(10, Times))),
      do_http_request(Method, Req, HTTPOpts, Opts, Times + 1);
    Other -> Other
  end.

result(Response) ->
  try emqx_json:decode(Response, [return_maps]) of
    #{<<"code">> := 1, <<"result">> := Result} ->
      base64:decode(Result);
    #{<<"code">> := ErrCode, <<"result">> := Result} ->
      logger:error("[http parser] received ~p, error: ~p",
        [emqx_schema_3rd_party:result_code_text(ErrCode),
          base64:decode(Result)]),
      error({emqx_schema_badarg,
        {http_parser_reply, ErrCode}});
    Wrong ->
      logger:error("[http parser] invalid format: ~p",
        [Wrong]),
      error({emqx_schema_badarg,
        {http_parser_reply_format, Wrong}})
  catch
    error:badarg ->
      logger:error("[http parser] not json: ~p", [Response]),
      error({emqx_schema_badarg,
        {http_parser_reply_format, Response}})
  end.

bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Bin) when is_binary(Bin) -> Bin.
