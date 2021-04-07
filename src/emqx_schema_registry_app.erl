%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午5:09
%%%-------------------------------------------------------------------
-module(emqx_schema_registry_app).
-author("root").

-behaviour(application).
-emqx_plugin(emqx_schema_registry_app).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
  emqx_schema_registry_sup:start_link().

stop(_State) -> ok.
