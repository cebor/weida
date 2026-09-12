"""The surface that needs no broker: the values, the matcher, the refusals.

Every one of these is a rule of MQTT 5.0 enforced before a byte is sent, so
none of them needs a connection — which is the point. A binding that could
only be tested against a broker would be a binding whose configuration errors
all arrived as protocol errors.
"""

import weida_mqtt


class TestMessage:
    def test_a_message_carries_what_a_publisher_sets(self):
        message = weida_mqtt.Message(
            "room/12/temperature",
            b"21.5",
            qos=2,
            retain=True,
            content_type="text/plain",
            response_topic="room/12/ack",
            correlation_data=b"req-7",
            message_expiry=60.0,
            payload_format_utf8=True,
            user_properties=[("unit", "celsius"), ("unit", "C")],
        )
        assert message.topic == "room/12/temperature"
        assert message.payload == b"21.5"
        assert message.qos == 2
        assert message.retain

    def test_the_payload_defaults_to_empty_rather_than_none(self):
        # An MQTT payload may be zero length (3.3.3); `None` is not a payload
        # and would have to mean something.
        assert weida_mqtt.Message("a/b").payload == b""

    def test_a_str_payload_is_refused(self):
        # MQTT payloads are opaque bytes and Payload Format Indicator is a
        # hint the publisher sets, not an encoding the protocol applies
        # (3.3.2.3.2). Encoding a `str` silently would be inventing one.
        try:
            weida_mqtt.Message("a/b", "not bytes")
        except (TypeError, ValueError):
            pass
        else:
            raise AssertionError("a str payload should be refused")

    def test_the_delete_is_retain_one_and_an_empty_payload(self):
        # [MQTT-3.3.1-6]: it removes the retained message, is delivered to
        # current subscribers as a normal message, and is itself not stored.
        delete = weida_mqtt.Message.delete_retained("room/12/temperature", qos=1)
        assert delete.retain
        assert delete.payload == b""
        assert delete.qos == 1

    def test_qos_outside_zero_one_two_is_refused(self):
        for qos in (3, 4, 255):
            try:
                weida_mqtt.Message("a/b", b"x", qos=qos)
            except ValueError:
                continue
            raise AssertionError(f"QoS {qos} should be refused")


class TestSubscription:
    def test_the_four_options_of_3_8_3_1(self):
        subscription = weida_mqtt.Subscription(
            "room/+/temperature",
            2,
            no_local=True,
            retain_as_published=True,
            retain_handling=2,
        )
        assert subscription.filter == "room/+/temperature"
        assert subscription.qos == 2
        assert not subscription.shared

    def test_retain_handling_three_is_a_protocol_error_and_not_a_value(self):
        # "It is a Protocol Error to send a Retain Handling of 3" (3.8.3.1),
        # so it is refused here rather than on the wire.
        try:
            weida_mqtt.Subscription("a/+", 1, retain_handling=3)
        except ValueError as refused:
            assert "3" in str(refused)
        else:
            raise AssertionError("Retain Handling 3 should be refused")

    def test_a_share_prefix_makes_a_subscription_shared(self):
        assert weida_mqtt.Subscription("$share/group/room/+", 1).shared
        assert not weida_mqtt.Subscription("room/+", 1).shared


class TestFilters:
    def test_the_specifications_own_worked_examples(self):
        # `#` matches the parent level as well as everything below it, which
        # is the case a plausible matcher gets wrong.
        assert weida_mqtt.matches("sport/tennis/player1/#", "sport/tennis/player1")
        assert weida_mqtt.matches("sport/tennis/player1/#", "sport/tennis/player1/ranking")
        assert weida_mqtt.matches("sport/#", "sport")
        # `sport/+` matches `sport/` but not `sport`: an empty level is a
        # level.
        assert weida_mqtt.matches("sport/+", "sport/")
        assert not weida_mqtt.matches("sport/+", "sport")
        assert not weida_mqtt.matches("sport/tennis/+", "sport/tennis/player1/ranking")
        # `/finance` matches `+/+` and `/+` but not `+`.
        assert weida_mqtt.matches("+/+", "/finance")
        assert weida_mqtt.matches("/+", "/finance")
        assert not weida_mqtt.matches("+", "/finance")

    def test_a_leading_wildcard_never_matches_a_dollar_topic(self):
        # [MQTT-4.7.2-1], and the rule is about the filter's *first*
        # character: `$SYS/+` is fine.
        assert not weida_mqtt.matches("#", "$SYS/broker/uptime")
        assert not weida_mqtt.matches("+/broker/uptime", "$SYS/broker/uptime")
        assert weida_mqtt.matches("$SYS/#", "$SYS/broker/uptime")
        assert weida_mqtt.matches("$SYS/+/uptime", "$SYS/broker/uptime")
        # And only `$` topics are excluded.
        assert weida_mqtt.matches("#", "SYS/broker/uptime")

    def test_a_shared_filter_matches_on_its_filter_alone(self):
        # "Neither $share nor the ShareName is considered when matching."
        assert weida_mqtt.matches("$share/g/sport/+", "sport/tennis")
        assert not weida_mqtt.matches("$share/g/sport/+", "other/tennis")

    def test_the_grammar_of_4_7_1(self):
        for legal in ("sport", "sport/tennis/#", "sport/+/player1", "+", "#", "/", "$SYS/#"):
            weida_mqtt.check_topic_filter(legal)
        for illegal in ("sport/tennis#", "sport/#/ranking", "sp+rt", "", "$share/g"):
            try:
                weida_mqtt.check_topic_filter(illegal)
            except ValueError:
                continue
            raise AssertionError(f"{illegal!r} should be refused")

    def test_a_topic_name_is_not_a_pattern(self):
        weida_mqtt.check_topic_name("room/12/temperature")
        weida_mqtt.check_topic_name("$SYS/broker/uptime")
        for illegal in ("room/+", "room/#", ""):
            try:
                weida_mqtt.check_topic_name(illegal)
            except ValueError:
                continue
            raise AssertionError(f"{illegal!r} should be refused as a name")


class TestErrors:
    def test_every_reason_code_is_its_own_class_under_one_base(self):
        # A sample across the seven groups. One class per *name* and not per
        # packet: `NotAuthorized` is 0x87 in CONNACK, PUBACK, SUBACK, UNSUBACK
        # and DISCONNECT alike, so `except NotAuthorized` catches it wherever
        # it arrived.
        for name, code in (
            ("NotAuthorized", 0x87),
            ("BadUserNameOrPassword", 0x86),
            ("RetainNotSupported", 0x9A),
            ("QosNotSupported", 0x9B),
            ("SharedSubscriptionsNotSupported", 0x9E),
            ("SubscriptionIdentifiersNotSupported", 0xA1),
            ("WildcardSubscriptionsNotSupported", 0xA2),
            ("TopicAliasInvalid", 0x94),
            ("ReceiveMaximumExceeded", 0x93),
            ("SessionTakenOver", 0x8E),
            ("KeepAliveTimeout", 0x8D),
            ("TopicFilterInvalid", 0x8F),
        ):
            klass = getattr(weida_mqtt, name)
            assert issubclass(klass, weida_mqtt.MqttError), name
            # The byte is on the instance, because the class is what a caller
            # branches on and the byte is what a caller logs.
            raised = klass("why")
            assert isinstance(raised, weida_mqtt.MqttError)
            del code  # asserted on a real raise in test_interop.py

    def test_the_failures_with_no_reason_code_are_classes_too(self):
        # MQTT gives these no byte, and a binding that left them as
        # `RuntimeError` would have thrown the branch away.
        for name in (
            "NotConnected",
            "ConnectionClosed",
            "Timeout",
            "Configuration",
            "InvalidTopic",
            "SessionPresentWithoutState",
            "QuotaExhausted",
            "AcknowledgementLengthMismatch",
        ):
            assert issubclass(getattr(weida_mqtt, name), weida_mqtt.MqttError), name


class TestOptions:
    def test_an_unusable_option_is_refused_where_it_is_written(self):
        # Authentication Data without an Authentication Method is a Protocol
        # Error ([MQTT-3.1.2-33]); refused here, so the ValueError names the
        # option rather than the broker naming a byte.
        try:
            weida_mqtt.ConnectOptions("c", authentication_data=b"x")
        except ValueError as refused:
            assert "authentication" in str(refused).lower()
        else:
            raise AssertionError("authentication_data alone should be refused")

    def test_a_will_topic_is_a_topic_name(self):
        # [MQTT-3.1.3-10]: no wildcards, never zero-length — and the Topic
        # Alias exception cannot apply, because no alias exists before the
        # CONNACK.
        for illegal in ("status/+", "status/#", ""):
            try:
                weida_mqtt.ConnectOptions("c", will=weida_mqtt.Will(illegal, b"gone"))
            except ValueError:
                continue
            raise AssertionError(f"a Will on {illegal!r} should be refused")
        weida_mqtt.ConnectOptions("c", will=weida_mqtt.Will("status/c", b"gone"))

    def test_keep_alive_zero_is_legal_and_means_off(self):
        # 3.1.2.10: zero disables the mechanism entirely — and with it the
        # server's liveness detection, which is why it is worth being able to
        # say.
        assert weida_mqtt.ConnectOptions("c", keep_alive=0.0).keep_alive == 0.0

    def test_a_negative_interval_is_refused(self):
        for kwargs in (
            {"keep_alive": -1.0},
            {"session_expiry": -1.0},
            {"connect_timeout": -1.0},
        ):
            try:
                weida_mqtt.ConnectOptions("c", **kwargs)
            except ValueError:
                continue
            raise AssertionError(f"{kwargs} should be refused")


class TestContext:
    def test_the_ambient_reactor_is_absent_in_a_plain_python_process(self):
        # `Context.current()` is the third constructor and a plain Python
        # process has no Tokio reactor, so it raises rather than quietly
        # starting a second one.
        try:
            weida_mqtt.Context.current()
        except weida_mqtt.MqttError as failed:
            assert failed.cause
        else:
            raise AssertionError("there is no ambient reactor here")

    def test_a_context_owns_a_reactor_and_shares_it(self):
        first = weida_mqtt.Context(worker_threads=1)
        second = weida_mqtt.Context.sharing(first)
        assert repr(second) == "Context()"
