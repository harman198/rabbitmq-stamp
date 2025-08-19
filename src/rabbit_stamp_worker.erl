-module(rabbit_stamp_worker).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([fire/2]).

-include_lib("../../amqp_client/include/amqp_client.hrl").

-record(state, {channel, exchange}).

-define(RKFormat, "~4.10.0B.~2.10.0B.~2.10.0B.~1.10.0B.~2.10.0B.~2.10.0B.~2.10.0B").

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

%---------------------------
% Gen Server Implementation
% --------------------------

init([]) ->
    {ok, #state{}}.

handle_call(_Msg, _From, State) ->
    {reply, unknown_command, State}.

handle_cast({fire, ExchangeName, Message}, #state{channel = undefined} = State) ->
    rabbit_log:info("Handle before Connection Open"),
    case rabbit:is_running() of
        true ->
            State1 = open_connection(ExchangeName, State),
            rabbit_log:info("Connection Open"),
            handle_cast({fire, ExchangeName, Message}, State1);
        false ->
            timer:sleep(1000),
            handle_cast({fire, ExchangeName, Message}, State)
    end;
handle_cast({fire, ExchangeName, Message},
            #state{channel = Channel, exchange = Exchange} = State) ->
    rabbit_log:info("Handle after Connection Open"),
    rabbit_log:info("Handle Exchange ~p ExchangeName ~p Message  ~p",
                    [Exchange, ExchangeName, Message]),
    {RoutingKey, Payload, Properties} = extract_parts(Message),
    BasicPublish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    rabbit_log:info("Handle RoutingKey ~p Payload ~p Properties  ~p",
                    [RoutingKey, Payload, Properties]),
    Content = #amqp_msg{props = Properties, payload = Payload},
    amqp_channel:call(Channel, BasicPublish, Content),
    {noreply, State};
handle_cast(_, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%---------------------------

open_connection(Exchange, State) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    %{ok, Exchange} = application:get_env(rabbitmq_stamp, exchange),
    rabbit_log:info("Exchange ~p", [Exchange]),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = Exchange, passive = true}),
    State#state{channel = Channel, exchange = Exchange}.

fire(<<ExchangeName/binary>>, Message) ->
    rabbit_log:info("Fire Binary"),
    gen_server:cast({global, ?MODULE}, {fire, ExchangeName, Message});
fire(ExchangeName, Message) when is_atom(ExchangeName) ->
    rabbit_log:info("Fire Non Binary"),
    ExchangeName0 =
        list_to_binary(atom_to_list(ExchangeName)), % list_to_atom(binary_to_list(ExchangeName)),
    gen_server:cast({global, ?MODULE}, {fire, ExchangeName0, Message}).

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
