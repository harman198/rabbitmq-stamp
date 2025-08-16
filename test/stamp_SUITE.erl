-module(stamp_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("../../amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [{group, non_parallel_tests}].

groups() ->
    [{non_parallel_tests,
      [],
      [can_send_a_message_to_stamp_exchange_test, can_create_exchange_of_stamp_type_test]}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:run_setup_steps(Config1,
                                      rabbit_ct_broker_helpers:setup_steps()
                                      ++ rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_client_helpers:teardown_steps()
                                         ++ rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

suite() ->
    [{timetrap, {seconds, 60}}].

can_send_a_message_to_stamp_exchange_test(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),
    SendingXChangeName = <<"demo-exchange">>,
    RecievingXChangeName = <<"mydestinationexchange">>,

    % declare exchanges
    SendingExchangeDeclare =
        #'exchange.declare'{exchange = SendingXChangeName, type = <<"x-stamp">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, SendingExchangeDeclare),

    RecievingExchangeDeclare =
        #'exchange.declare'{exchange = RecievingXChangeName, type = <<"fanout">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, RecievingExchangeDeclare),

    % listening queue
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, #'queue.declare'{}),
    Binding =
        #'queue.bind'{queue = Queue,
                      exchange = RecievingXChangeName,
                      routing_key = <<"#">>},

    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue, no_ack = true}, self()),

    Payload = <<"foobar">>,
    Key = <<"my_routing_key">>,
    StampHeader = {<<"forward_exchange">>, longstr, RecievingXChangeName},

    Props = #'P_basic'{headers = [StampHeader]},
    Publish = #'basic.publish'{exchange = SendingXChangeName, routing_key = Key},

    Message = #amqp_msg{props = Props, payload = Payload},

    ok = amqp_channel:cast(Channel, Publish, Message),

    receive
        #'basic.consume_ok'{} ->
            ok
    end,

    loop(RecievingXChangeName),
    ok.

loop(DestExchange) ->
    receive
        {#'basic.deliver'{}, #amqp_msg{props = Props}} ->
            Headers = Props#'P_basic'.headers,
            log(" [x] Header ~p~n", Headers),
            {<<"stamp">>, long, _} = lists:nth(1, Headers),
            {<<"forward_exchange">>, longstr, DestExchange} = lists:nth(2, Headers),
            ?assert(length(Headers) =:= 2)
    after 1000 ->
        ?assert(false)
    end.

can_create_exchange_of_stamp_type_test(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),
    XChangeName = <<"demo-exchange">>,

    ExchangeDeclare = #'exchange.declare'{exchange = XChangeName, type = <<"x-stamp">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare),

    ok.

% private
log(Message, Value) ->
    ?debugFmt("~p: ~p~n", [Message, Value]).
