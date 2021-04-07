%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午5:08
%%%-------------------------------------------------------------------
-module(emqx_schema_parser).
-author("root").
-include("../include/emqx_schema_registry.hrl").

-export([decode/2, decode/3, encode/2, encode/3]).

-export([make_parser/5]).

-type more_args() :: [term()].
-spec decode(schema_name(), binary()) -> term().

decode(SchemaName, RawData) -> decode(SchemaName, RawData, []).

-spec decode(schema_name(), binary(), more_args()) -> term().

decode(SchemaName, RawData, VarArgs) when is_list(VarArgs) ->
  case emqx_schema_registry:get_parser(SchemaName) of
    {error, not_found} -> error(parser_not_found);
    {ok, #parser{decoder = Decoder}} ->
      erlang:apply(Decoder, [RawData | VarArgs])
  end.

-spec encode(schema_name(), term()) -> iodata().
encode(SchemaName, Term) ->
  encode(SchemaName, Term, []).

-spec encode(schema_name(), binary(), more_args()) -> iodata().
encode(SchemaName, Term, VarArgs)
  when is_list(VarArgs) ->
  case emqx_schema_registry:get_parser(SchemaName) of
    {error, not_found} -> error(parser_not_found);
    {ok, #parser{encoder = Encoder}} ->
      erlang:apply(Encoder, [Term | VarArgs])
  end.

%% avro 解析
-spec make_parser(parser_type(), schema_name(), schema_text(), parser_addr(), parser_opts()) -> {decoder(), encoder(), destroy()}.
make_parser(avro, _SchemaName, SchemaText, _ParserAddr, _ParserOpts) ->
  {avro:make_simple_decoder(SchemaText, [{map_type, map}, {record_type, map}]), avro:make_simple_encoder(SchemaText, []), fun () -> ok end};
%% protobuf 解析
make_parser(protobuf, SchemaName, SchemaText, _ParserAddr, _ParserOpts) ->
  ParserMod = make_parser_module(protobuf, SchemaName, SchemaText),
  {fun (Data, MsgName) ->
    ParserMod:decode_msg(Data, safe_atom(MsgName))
   end,
    fun (Term, MsgName) -> ParserMod:encode_msg(safe_atom_key_map(Term), safe_atom(MsgName))
    end,
    fun () -> unload(ParserMod) end};

%% 第三方解析
make_parser('3rd-party', SchemaName, _SchemaText, _ParserAddr = {resource_id, ResourceId}, ParserOpts0) ->
  case emqx_rule_engine:get_resource_params(ResourceId) of
    {ok,
      #{parser_handler := ParserHandler, parser_opts := ParserOpts}} ->
      Opts = maps:merge(ParserOpts0, ParserOpts),
      Decode = emqx_schema_3rd_party:make_decoder(ParserHandler, SchemaName, Opts),
      Encode = emqx_schema_3rd_party:make_encoder(ParserHandler, SchemaName, Opts),
      {Decode, Encode, fun () -> ok end};
    {error, Reason} ->
      error({emqx_schema_badarg, {ResourceId, Reason}})
  end;
make_parser('3rd-party', SchemaName, _SchemaText, ParserAddr, ParserOpts) ->
  ParserHandler = emqx_schema_3rd_party:open(SchemaName, ParserAddr, ParserOpts),
  Decode = emqx_schema_3rd_party:make_decoder(ParserHandler, SchemaName, ParserOpts),
  Encode = emqx_schema_3rd_party:make_encoder(ParserHandler, SchemaName, ParserOpts),
  {Decode,
    Encode,
    fun () -> emqx_schema_3rd_party:close(ParserHandler)
    end}.

%% 创建解析模块
make_parser_module(ParserType, SchemaName, Schema) ->
  {Mod, ModFileName} = parser_mod_name(SchemaName),
  case generate_code(ParserType, Mod, Schema) of
    {ok, Mod0, Code} ->
%%      编译载入模块
      ok = compile_and_load(Mod0, ModFileName, Code),
      Mod0;
    {error, Reason} ->
      logger:error("generate load code failed: ~p", [Reason]),
      error({emqx_schema_badarg, {invalid_protobuf_schema, Reason}})
  end.

%% 解析模块名称
parser_mod_name(SchemaName) ->
%%  模块名称
  ModName = "$schema_parser_" ++ binary_to_list(SchemaName),
%%  模块名
  Mod = list_to_atom(ModName),
%%  模块文件名称
  ModFileName = ModName ++ ".memory",
  {Mod, ModFileName}.

generate_code(protobuf, Mod, SchemaText) when is_binary(SchemaText) ->
  generate_code(protobuf, Mod, binary_to_list(SchemaText));
generate_code(protobuf, Mod, SchemaText) ->
  gpb_compile:string(Mod, SchemaText, [binary, strings_as_binaries, {maps, true}, {verify, always}, {maps_unset_optional, omitted}]).

compile_and_load(Mod, ModFileName, Code) ->
%%  清除旧的模块代码
  code:purge(Mod),
  {module, Mod} = code:load_binary(Mod, ModFileName, Code),
  ok.

%% 卸载
unload(Mod) ->
%%  清除旧的模块代码
  code:purge(Mod),
  code:delete(Mod).

safe_atom(Str) when is_binary(Str) ->
  try
    binary_to_existing_atom(Str, utf8)
  catch
    error:badarg -> error({not_exists, Str})
  end;
safe_atom(Str) when is_list(Str) ->
  try
    list_to_existing_atom(Str)
  catch
    error:badarg -> error({not_exists, Str})
  end;
safe_atom(Atom) when is_atom(Atom) -> Atom.

safe_atom_key_map(BinKeyMap) when is_map(BinKeyMap) ->
  maps:fold(fun (K, V, Acc) when is_binary(K) ->
    Acc#{binary_to_existing_atom(K, utf8) =>
    safe_atom_key_map(V)};
    (K, V, Acc) when is_list(K) ->
      Acc#{list_to_existing_atom(K) => safe_atom_key_map(V)};
    (K, V, Acc) when is_atom(K) ->
      Acc#{K => safe_atom_key_map(V)}
            end,
    #{},
    BinKeyMap);
safe_atom_key_map(ListV) when is_list(ListV) ->
  [safe_atom_key_map(V) || V <- ListV];
safe_atom_key_map(Val) -> Val.

