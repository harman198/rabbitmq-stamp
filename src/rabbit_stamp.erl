-module(rabbit_stamp).

-behaviour(application).

-export([start/2, stop/1]).

start(normal, []) ->
    rabbit_stamp_sup:start_link().

stop(_State) ->
    ok.
