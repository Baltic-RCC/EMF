import signal
from unittest import mock

import pika
import pytest

from emf.common.integrations import rabbit


@pytest.fixture(autouse=True)
def restore_signal_handlers():
    saved = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    yield
    for sig, handler in saved.items():
        signal.signal(sig, handler)


class PassThroughHandler:
    def __init__(self, success=None, error=None, suffix=b""):
        self.success = success
        self.error = error
        self.suffix = suffix
        self.calls = []

    def handle(self, body, properties, **kwargs):
        self.calls.append((body, properties))
        if self.error:
            raise self.error
        if self.success is not None:
            properties.headers["success"] = self.success
        return body + self.suffix, properties


class Converter:
    def __init__(self, error=None):
        self.error = error

    def convert(self, body):
        if self.error:
            raise self.error
        return body.upper(), "application/json"


def _message(headers=None, delivery_tag=7, body=b"payload"):
    properties = pika.BasicProperties(headers={} if headers is None else headers)
    return pika.spec.Basic.GetOk(delivery_tag=delivery_tag), properties, body


@pytest.fixture
def channel():
    return mock.MagicMock()


@pytest.fixture
def blocking_connection(channel):
    connection = mock.MagicMock()
    connection.channel.return_value = channel
    with mock.patch.object(rabbit.pika, "BlockingConnection", return_value=connection) as patched, \
            mock.patch.object(rabbit, "time"):
        yield patched


def _single_consumer(**kwargs):
    return rabbit.SingleMessageConsumer(queue="input-queue", **kwargs)


def test_single_consumer_returns_3_when_connection_fails():
    handler = PassThroughHandler()
    with mock.patch.object(rabbit.pika, "BlockingConnection", side_effect=pika.exceptions.AMQPConnectionError("down")):
        assert _single_consumer(message_handlers=[handler]).run() == 3
    assert handler.calls == []


def test_single_consumer_returns_0_on_empty_queue(blocking_connection, channel):
    channel.basic_get.return_value = (None, None, None)

    assert _single_consumer(message_handlers=[PassThroughHandler()]).run() == 0

    channel.basic_get.assert_called_once_with("input-queue", auto_ack=False)
    channel.basic_ack.assert_not_called()
    channel.basic_reject.assert_not_called()
    channel.close.assert_called_once()
    blocking_connection.return_value.close.assert_called_once()


def test_single_consumer_forwards_handler_output_then_acks(blocking_connection, channel):
    method, properties, body = _message(headers={"task": "1"})
    channel.basic_get.return_value = (method, properties, body)
    handler = PassThroughHandler(suffix=b"-handled")

    assert _single_consumer(message_handlers=[handler], forward="output-exchange").run() == 0

    assert handler.calls == [(b"payload", properties)]
    assert channel.mock_calls.index(mock.call.basic_publish(
        exchange="output-exchange", routing_key="", body=b"payload-handled", properties=properties)) < \
        channel.mock_calls.index(mock.call.basic_ack(7))
    channel.basic_reject.assert_not_called()
    blocking_connection.return_value.close.assert_called_once()


def test_single_consumer_without_forward_only_acks(blocking_connection, channel):
    channel.basic_get.return_value = _message(headers={"success": True})

    assert _single_consumer(message_handlers=[PassThroughHandler()]).run() == 0

    channel.basic_publish.assert_not_called()
    channel.basic_ack.assert_called_once_with(7)


FAILING_HANDLERS = pytest.mark.parametrize("failure", [{"success": False}, {"error": ValueError("boom")}],
                                           ids=["success-false", "handler-exception"])


@FAILING_HANDLERS
def test_single_consumer_rejects_without_requeue_on_handler_failure(blocking_connection, channel, failure):
    channel.basic_get.return_value = _message()
    handler = PassThroughHandler(**failure)

    assert _single_consumer(message_handlers=[handler], forward="output-exchange").run() == 2

    channel.basic_reject.assert_called_once_with(7, requeue=False)
    channel.basic_publish.assert_not_called()
    channel.basic_ack.assert_not_called()


@FAILING_HANDLERS
def test_single_consumer_stops_handler_chain_after_failure(blocking_connection, channel, failure):
    channel.basic_get.return_value = _message()
    failing_handler, next_handler = PassThroughHandler(**failure), PassThroughHandler()

    _single_consumer(message_handlers=[failing_handler, next_handler]).run()

    assert len(failing_handler.calls) == 1
    assert next_handler.calls == []


def test_single_consumer_runs_all_handlers_in_order(blocking_connection, channel):
    channel.basic_get.return_value = _message()
    first, second = PassThroughHandler(suffix=b"-1"), PassThroughHandler(suffix=b"-2")

    assert _single_consumer(message_handlers=[first, second], forward="out").run() == 0

    assert second.calls[0][0] == b"payload-1"
    assert channel.basic_publish.call_args.kwargs["body"] == b"payload-1-2"


def test_single_consumer_converts_message_before_handlers(blocking_connection, channel):
    channel.basic_get.return_value = _message()
    handler = PassThroughHandler()

    assert _single_consumer(message_handlers=[handler], message_converter=Converter()).run() == 0

    body, properties = handler.calls[0]
    assert body == b"PAYLOAD"
    assert properties.content_type == "application/json"


def test_single_consumer_rejects_and_skips_handlers_when_conversion_fails(blocking_connection, channel):
    channel.basic_get.return_value = _message()
    handler = PassThroughHandler()

    consumer = _single_consumer(message_handlers=[handler], message_converter=Converter(error=ValueError("bad xml")))
    assert consumer.run() == 2

    assert handler.calls == []
    channel.basic_reject.assert_called_once_with(7, requeue=False)


@pytest.mark.parametrize("failing_call", ["basic_publish", "basic_ack", "basic_reject"])
def test_single_consumer_returns_3_when_channel_operation_fails(blocking_connection, channel, failing_call):
    channel.basic_get.return_value = _message()
    getattr(channel, failing_call).side_effect = pika.exceptions.ChannelClosed(406, "closed")
    handler = PassThroughHandler(success=False if failing_call == "basic_reject" else None)

    assert _single_consumer(message_handlers=[handler], forward="out").run() == 3

    if failing_call == "basic_publish":
        channel.basic_ack.assert_not_called()


def test_single_consumer_finishes_message_when_sigterm_arrives_mid_processing(blocking_connection, channel, caplog):
    channel.basic_get.return_value = _message()
    consumer = _single_consumer(forward="out")

    class SignalledHandler(PassThroughHandler):
        def handle(self, body, properties, **kwargs):
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            return super().handle(body, properties, **kwargs)

    consumer.message_handlers = [SignalledHandler()]

    with caplog.at_level("INFO", logger=rabbit.__name__):
        assert consumer.run() == 0

    channel.basic_ack.assert_called_once_with(7)
    assert "Shutdown requested" in caplog.text


@pytest.mark.xfail(strict=True, raises=AttributeError,
                   reason="SingleMessageConsumer calls properties.headers.get() on messages published without headers")
@pytest.mark.parametrize("handler_count", [0, 1], ids=["no-handler", "pass-through-handler"])
def test_single_consumer_acks_message_without_headers(blocking_connection, channel, handler_count):
    channel.basic_get.return_value = (pika.spec.Basic.GetOk(delivery_tag=7), pika.BasicProperties(), b"payload")
    handlers = [PassThroughHandler() for _ in range(handler_count)]

    assert _single_consumer(message_handlers=handlers, forward="out").run() == 0
    channel.basic_ack.assert_called_once_with(7)


@pytest.fixture
def rmq_consumer():
    consumer = rabbit.RMQConsumer(queue="input-queue", forward="output-exchange")
    consumer._channel = mock.MagicMock()
    yield consumer
    consumer._executor.shutdown()


def _deliver(delivery_tag=5):
    return pika.spec.Basic.Deliver(delivery_tag=delivery_tag)


def test_rmq_consumer_forwards_and_acks_successful_message(rmq_consumer):
    rmq_consumer.message_handlers = [PassThroughHandler(suffix=b"-handled")]
    properties = pika.BasicProperties(headers={})

    rmq_consumer._process_messages(_deliver(), properties, b"payload")

    channel = rmq_consumer._channel
    channel.basic_publish.assert_called_once_with(exchange="output-exchange", routing_key="", body=b"payload-handled",
                                                  properties=properties)
    channel.basic_ack.assert_called_once_with(5)
    channel.basic_reject.assert_not_called()


def test_rmq_consumer_rejects_without_requeue_when_handler_sets_success_false(rmq_consumer):
    next_handler = PassThroughHandler()
    rmq_consumer.message_handlers = [PassThroughHandler(success=False), next_handler]

    rmq_consumer._process_messages(_deliver(), pika.BasicProperties(headers={}), b"payload")

    rmq_consumer._channel.basic_reject.assert_called_once_with(5, requeue=False)
    rmq_consumer._channel.basic_publish.assert_not_called()
    rmq_consumer._channel.basic_ack.assert_not_called()
    assert next_handler.calls == []


def test_rmq_consumer_requeues_message_when_handler_raises(rmq_consumer):
    rmq_consumer.message_handlers = [PassThroughHandler(error=RuntimeError("boom"))]

    rmq_consumer._process_messages(_deliver(), pika.BasicProperties(headers={}), b"payload")

    rmq_consumer._channel.basic_reject.assert_called_once_with(5, requeue=True)
    rmq_consumer._channel.basic_publish.assert_not_called()
    rmq_consumer._channel.basic_ack.assert_not_called()


def test_rmq_consumer_passes_converted_message_to_handlers(rmq_consumer):
    handler = PassThroughHandler()
    rmq_consumer.message_handlers = [handler]
    rmq_consumer.message_converter = Converter()

    rmq_consumer._process_messages(_deliver(), pika.BasicProperties(headers={}), b"payload")

    body, properties = handler.calls[0]
    assert (body, properties.content_type) == (b"PAYLOAD", "application/json")
    rmq_consumer._channel.basic_ack.assert_called_once_with(5)


@pytest.mark.xfail(strict=True, reason="RMQConsumer keeps handling a message after rejecting it for a conversion failure")
def test_rmq_consumer_skips_handlers_after_conversion_failure(rmq_consumer):
    handler = PassThroughHandler()
    rmq_consumer.message_handlers = [handler]
    rmq_consumer.message_converter = Converter(error=ValueError("bad xml"))

    rmq_consumer._process_messages(_deliver(), pika.BasicProperties(headers={}), b"payload")

    rmq_consumer._channel.basic_reject.assert_called_once_with(5, requeue=True)
    assert handler.calls == []


@pytest.mark.xfail(strict=True, reason="RMQConsumer calls properties.headers.get() on messages published without headers")
def test_rmq_consumer_acks_message_without_headers(rmq_consumer):
    rmq_consumer.message_handlers = [PassThroughHandler()]

    rmq_consumer._process_messages(_deliver(), pika.BasicProperties(), b"payload")

    rmq_consumer._channel.basic_ack.assert_called_once_with(5)


@pytest.fixture
def blocking_client():
    connection = mock.MagicMock()
    publish_channel, consume_channel = mock.MagicMock(), mock.MagicMock()
    connection.channel.side_effect = [publish_channel, consume_channel]
    with mock.patch.object(rabbit.pika, "BlockingConnection", return_value=connection):
        client = rabbit.BlockingClient(host="rabbit.test", port=5672, username="user", password="secret")
    return client, publish_channel, consume_channel


def test_blocking_client_publishes_with_headers(blocking_client):
    client, publish_channel, _ = blocking_client

    client.publish("body", "exchange", headers={"a": 1}, routing_key="key")

    kwargs = publish_channel.basic_publish.call_args.kwargs
    assert (kwargs["exchange"], kwargs["routing_key"], kwargs["body"]) == ("exchange", "key", "body")
    assert kwargs["properties"].headers == {"a": 1}


def test_blocking_client_get_single_message_converts_body(blocking_client):
    client, _, consume_channel = blocking_client
    client.message_converter = Converter()
    method, properties = pika.spec.Basic.GetOk(delivery_tag=1), pika.BasicProperties(headers={})
    consume_channel.basic_get.return_value = (method, properties, b"payload")

    assert client.get_single_message("queue") == (method, properties, b"PAYLOAD")
    assert properties.content_type == "application/json"
    consume_channel.basic_get.assert_called_once_with("queue", auto_ack=True)


def test_blocking_client_get_single_message_returns_nones_on_empty_queue(blocking_client):
    client, _, consume_channel = blocking_client
    consume_channel.basic_get.return_value = (None, None, None)

    assert client.get_single_message("queue", auto_ack=False) == (None, None, None)


@pytest.mark.parametrize("message_headers", [{"origin": "x"}, None])
def test_blocking_client_shovel_marks_message_and_acks(blocking_client, message_headers):
    client, publish_channel, consume_channel = blocking_client

    client.shovel("from-queue", "to-exchange", headers={"extra": "y"}, routing_key="rk")

    consume_kwargs = consume_channel.basic_consume.call_args.kwargs
    assert (consume_kwargs["queue"], consume_kwargs["auto_ack"]) == ("from-queue", False)
    source_channel = mock.MagicMock()
    consume_kwargs["on_message_callback"](source_channel, pika.spec.Basic.Deliver(delivery_tag=3),
                                          pika.BasicProperties(headers=message_headers), b"body")

    publish_kwargs = publish_channel.basic_publish.call_args.kwargs
    assert (publish_kwargs["exchange"], publish_kwargs["routing_key"], publish_kwargs["body"]) == ("to-exchange", "rk", b"body")
    assert publish_kwargs["properties"].headers == {**(message_headers or {}), "shovelled": True, "extra": "y"}
    source_channel.basic_ack.assert_called_once_with(delivery_tag=3)


def test_reconnecting_consumer_delay_grows_to_30_seconds_and_resets_after_consuming():
    reconnecting = rabbit.ReconnectingConsumer(queue="input-queue")
    reconnecting._consumer = mock.MagicMock(was_consuming=False)

    delays = [reconnecting._get_reconnect_delay() for _ in range(32)]
    assert delays[:3] == [1, 2, 3]
    assert delays[-3:] == [30, 30, 30]

    reconnecting._consumer.was_consuming = True
    assert reconnecting._get_reconnect_delay() == 0


@pytest.mark.parametrize("should_reconnect", [True, False])
def test_reconnecting_consumer_reconnects_only_when_requested(should_reconnect):
    reconnecting = rabbit.ReconnectingConsumer(queue="input-queue")
    reconnecting._consumer = mock.MagicMock(should_reconnect=should_reconnect, was_consuming=False)

    with mock.patch.object(rabbit, "time") as patched_time:
        reconnecting._maybe_reconnect()

    if should_reconnect:
        reconnecting._consumer.stop.assert_called_once()
        patched_time.sleep.assert_called_once_with(1)
        reconnecting._consumer.run.assert_called_once()
    else:
        reconnecting._consumer.run.assert_not_called()
        patched_time.sleep.assert_not_called()
