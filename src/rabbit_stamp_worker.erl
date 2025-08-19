-module(rabbit_stamp_worker).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([next/2]).

-include_lib("../../amqp_client/include/amqp_client.hrl").

-record(state, {channel, exchange}).

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

%---------------------------
% Gen Server Implementation
% --------------------------

init([]) ->
    {ok, #state{}}.

handle_call(_Msg, _From, State) ->
    {reply, unknown_command, State}.

handle_cast({next, ExchangeName, Message}, State) ->
    case rabbit:is_running() of
        true ->
            State1 = open_connection(ExchangeName),
            handle_message({next, ExchangeName, Message, State1#state.channel});
        false ->
            timer:sleep(1000),
            handle_cast({next, ExchangeName, Message}, State)
    end;
handle_cast(_, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

handle_message({next, ExchangeName, Message, Channel}) ->
    {_RoutingKey, Payload, Properties} = extract_parts(Message),
    BasicPublish = #'basic.publish'{exchange = ExchangeName, routing_key = _RoutingKey},
    Content = #amqp_msg{props = Properties, payload = Payload},
    amqp_channel:call(Channel, BasicPublish, Content),
    {noreply, #state{channel = Channel, exchange = ExchangeName}}.

terminate(_, #state{channel = undefined}) ->
    ok;
terminate(_, #state{channel = Channel}) ->
    amqp_channel:call(Channel, #'channel.close'{}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%---------------------------

open_connection(Exchange) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    %{ok, Exchange} = application:get_env(rabbitmq_stamp, exchange),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = Exchange, passive = true}),
    #state{channel = Channel, exchange = Exchange}.

next(<<ExchangeName/binary>>, Message) ->
    gen_server:cast({global, ?MODULE}, {next, ExchangeName, Message});
next(ExchangeName, Message) when is_atom(ExchangeName) ->
    ExchangeName0 =
        list_to_binary(atom_to_list(ExchangeName)), % list_to_atom(binary_to_list(ExchangeName)),
    gen_server:cast({global, ?MODULE}, {next, ExchangeName0, Message}).

%%--------------------------------------------------------------------
%% @spec extract_parts(Msg :: tuple()) ->
%%                   {RoutingKey :: binary(),
%%                    Payload    :: binary(),
%%                    Props      :: #'P_basic'{}()}
%%--------------------------------------------------------------------
extract_parts({mc,
               mc_amqpl,
               {content,
                _Tag,
                Properties = #'P_basic'{},   % bind the entire P_basic record
                _Other1,
                _Other2,
                PayloadSegments},             % a list of binaries/iodata
               #{rk := [RoutingKeyBin]}}) ->       % a map with key “rk” → list of binaries
    %% turn the (single-element) payload list into a flat binary
    Payload = iolist_to_binary(PayloadSegments),
    {RoutingKeyBin, Payload, Properties}.
