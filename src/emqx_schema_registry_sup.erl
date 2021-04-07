%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午5:09
%%%-------------------------------------------------------------------
-module(emqx_schema_registry_sup).
-author("root").
-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
  supervisor:start_link({local, emqx_schema_registry_sup}, emqx_schema_registry_sup, []).

init([]) ->
  Registry = #{id => emqx_schema_registry,
    start => {emqx_schema_registry, start_link, []},
    restart => permanent,
    shutdown => 1000,
    type => worker,
    modules => [emqx_schema_registry]},
  {ok, {{one_for_one, 1, 5}, [Registry]}}.

