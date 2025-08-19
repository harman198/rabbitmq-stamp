%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.

%% x-stamp: forwards to the exchange named in header <<"forward_exchange">>
%% and adds a <<"stamp">> header with a microsecond timestamp.

-module(rabbit_stamp_exchange).

-include_lib("../../rabbit_common/include/rabbit.hrl").
-include_lib("../../amqp10_common/include/amqp10_framing.hrl").

-behaviour(rabbit_exchange_type).

%% Exchange type callbacks
-export([description/0, serialise_events/0, route/2, route/3, validate/1,
         validate_binding/2, create/2, delete/2, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2, info/1, info/2]).

%% Register exchange type <<"x-stamp">> at boot
-rabbit_boot_step({?MODULE,
                   [{description, "exchange type x-stamp"},
                    {mfa,
                     {rabbit_registry,
                      register,
                      [exchange, <<120, 45, 115, 116, 97, 109, 112>>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, kernel_ready}]}).

%% --- Info/description ---

info(_X) ->
    [].

info(_X, _What) ->
    [].

description() ->
    [{description,
      <<"Forwards to header 'forward-exchange' and stamps message with "
        "microsecond timestamp">>}].

serialise_events() ->
    false.

%% --- Lifecycle ---

validate(_X) ->
    ok.

validate_binding(_X, _Binding) ->
    ok.

create(_Serial, _X) ->
    ok.

delete(_Serial, _X) ->
    ok.

policy_changed(_X1, _X2) ->
    ok.

add_binding(_Serial, _X, _B) ->
    ok.

remove_bindings(_Serial, _X, _Bs) ->
    ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

%% --- Routing ---

route(X = #exchange{}, Msg) ->
    route(X, Msg, #{}).

route(#exchange{name = _XName}, Msg, _Opts) ->
    %% Extract message headers once
    Headers = mc:routing_headers(Msg, [x_headers]),

    case maps:get(<<"forward_exchange">>, Headers, undefined) of
        undefined ->
            %% No target: route nowhere (publisher confirms will get unroutable if mandatory)
            [];
        TargetName when is_binary(TargetName) ->
            rabbit_stamp_worker:fire(TargetName, Msg),
            [];
        Other ->
            %% Bad header type: do not route
            rabbit_log:warning("x-stamp: invalid forward-exchange header: ~p", [Other]),
            []
    end.
