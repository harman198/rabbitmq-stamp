%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stamp_interceptor).

-define(STAMP, <<"stamp">>).

-include_lib("../../rabbit_common/include/rabbit.hrl").
-include_lib("../../rabbit_common/include/rabbit_framing.hrl").

-import(rabbit_basic, [header/2]).

-behaviour(rabbit_channel_interceptor).

-export([description/0, intercept/3, applies_to/0, init/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "stamp interceptor"},
                    {mfa,
                     {rabbit_registry,
                      register,
                      [channel_interceptor,
                       <<115,
                         116,
                         97,
                         109,
                         112,
                         32,
                         105,
                         110,
                         116,
                         101,
                         114,
                         99,
                         101,
                         112,
                         116,
                         111,
                         114>>,
                       ?MODULE]}},
                    {cleanup,
                     {rabbit_registry,
                      unregister,
                      [channel_interceptor,
                       <<115,
                         116,
                         97,
                         109,
                         112,
                         32,
                         105,
                         110,
                         116,
                         101,
                         114,
                         99,
                         101,
                         112,
                         116,
                         111,
                         114>>]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

init(_Ch) ->
    undefined.

description() ->
    [{description, <<"Adds current timestamp to messages as they enter RabbitMQ">>}].

intercept(#'basic.publish'{} = Method, Content, _IState) ->
    Stamp = get_timestamp(),

    DecodedContent = rabbit_binary_parser:ensure_content_decoded(Content),

    Content2 = set_content_timestamp_header(DecodedContent, Stamp),

    {Method, Content2};
intercept(Method, Content, _VHost) ->
    {Method, Content}.

applies_to() ->
    ['basic.publish'].

%%----------------------------------------------------------------------------
%%
get_timestamp() ->
    {Mega, Sec, Micro} = erlang:timestamp(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

% OverwriteTimestamps = false, do not overwrite an existing timestamp

add_header(undefined, Header) ->
    [Header];
add_header(Headers, Header) ->
    lists:keystore(element(1, Header), 1, Headers, Header).

% set_content_timestamp(#content{properties = Props0} = Content, Timestamp) ->
%     %% we need to reset properties_bin = none so the new properties
%     %% get serialized when delivering the message.
%     Props1 = Props0#'P_basic'{timestamp = Timestamp},
%     Content#content{properties = Props1, properties_bin = none}.

set_content_timestamp_header(#content{properties =
                                          #'P_basic'{headers = Headers0} = Props0} =
                                 Content,
                             Stamp) ->
    Headers1 = add_header(Headers0, {?STAMP, longstr, integer_to_binary(Stamp)}),
    Props1 = Props0#'P_basic'{headers = Headers1},
    Content#content{properties = Props1, properties_bin = none}.
