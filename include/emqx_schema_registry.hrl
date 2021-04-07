-define(APP, emqx_schema_registry).

%% 是否是星号统配符
-define(is_wildcard(W), (W) =:= <<"*">>).
%% 坏的方案
-define(BAD_SCHEMA_ARG(REASON), {emqx_schema_badarg, (REASON)}).
%% 不兼容的方案
-define(INCOMPATIBLE(REASON), {emqx_schema_incompatible, (REASON)}).

-type schema_name() :: binary().
-type schema_text() :: binary().

%% avro 持二进制序列化方式，可以便捷，快速地处理大量数据；动态语言友好，Avro提供的机制使动态语言可以方便地处理Avro数据。
%% 二进制编码和JSON编码

%% protobuf google protobuf
-type parser_type() :: avro | protobuf | json | '3rd-party'.

%% 解析地址
-type parser_addr() :: {tcp, inet:hostname() | inet:ip_address(), inet:port_number()}| {http, httpc:url()}| {resource_id, binary()}.
%% 解析配置
-type parser_opts() :: #{'3rd_party_opts' => binary(), 'connect_timeout' => integer(), 'parse_timeout' => integer(), atom() => term()}.

%% 解码类型函数
-type decoder() :: fun((Data::binary()) -> term()).
%% 编码类型函数
-type encoder() :: fun((Term::binary()) -> iodata()).
%% 销毁函数
-type destroy() :: fun(() -> ok).

%% 方案定义
-record(schema, {
    name :: schema_name(), %% 方案名称
    schema :: schema_text(),%% 方案描述
    parser_type :: parser_type(),%% 解析类型
    parser_addr :: parser_addr(), %% only for parser_type = '3rd-party' 解析地址
    parser_opts = #{} :: parser_opts(),%% 解析配置
    descr = <<>> :: binary() %% 描述
}).
%% 解析结构定义
-record(parser, {
    name :: schema_name(),%% 方案名称
    decoder :: decoder(),%% 解码函数
    encoder :: encoder(),%% 编码函数
    destroy :: destroy() %% 销魂函数
}).

%% 3rd-party result codes
-define(CODE_REQ, 0).
-define(CODE_SUCCESS, 1).
-define(CODE_BAD_FORMAT, 2).
-define(CODE_INTERNAL, 3).
-define(CODE_UNKNOWN, 4).
