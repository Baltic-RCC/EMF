import os
import signal
import uuid
from types import SimpleNamespace

import pytest

from emf.common.integrations import rabbit

pytestmark = pytest.mark.integration

RMQ = {
    "host": os.environ.get("RMQ_SERVER", "localhost"),
    "port": int(os.environ.get("RMQ_PORT", "5672")),
    "username": os.environ.get("RMQ_USERNAME", "guest"),
    "password": os.environ.get("RMQ_PASSWORD", "guest"),
}


class UppercaseHandler:
    def handle(self, message, properties, **kwargs):
        properties.headers["handled-by"] = "UppercaseHandler"
        return message.upper(), properties


class UnsuccessfulHandler:
    def handle(self, message, properties, **kwargs):
        properties.headers["success"] = False
        return message, properties


class FailingHandler:
    def handle(self, message, properties, **kwargs):
        raise ValueError("handler failed")


class PassThroughHandler:
    def handle(self, message, properties, **kwargs):
        return message, properties


class UppercaseConverter:
    @staticmethod
    def convert(body):
        return body.upper(), "text/plain"


class FailingConverter:
    @staticmethod
    def convert(body):
        raise ValueError("conversion failed")


@pytest.fixture(autouse=True)
def _restore_signal_handlers():
    """SingleMessageConsumer installs its own SIGINT/SIGTERM handlers"""
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    yield
    for sig, handler in handlers.items():
        signal.signal(sig, handler)


def blocking_client(**kwargs):
    client = rabbit.BlockingClient(**RMQ, **kwargs)
    # publisher confirms make publish return only once the message is queued, before it is read back
    client.publish_channel.confirm_delivery()
    return client


@pytest.fixture
def client():
    client = blocking_client()
    yield client
    client.close()


@pytest.fixture
def names(client):
    """Input queue plus a forward exchange with one bound queue, deleted afterwards"""
    suffix = uuid.uuid4().hex[:8]
    names = SimpleNamespace(input=f"emfos-test-input-{suffix}",
                            forward_exchange=f"emfos-test-forward-{suffix}",
                            forwarded=f"emfos-test-forwarded-{suffix}")
    channel = client.publish_channel
    channel.queue_declare(names.input)
    channel.exchange_declare(names.forward_exchange, exchange_type="fanout")
    channel.queue_declare(names.forwarded)
    channel.queue_bind(names.forwarded, names.forward_exchange)
    yield names
    channel.queue_delete(names.input)
    channel.queue_delete(names.forwarded)
    channel.exchange_delete(names.forward_exchange)


def message_count(client, queue: str) -> int:
    return client.publish_channel.queue_declare(queue, passive=True).method.message_count


def single_message_consumer(names, handlers, **kwargs):
    return rabbit.SingleMessageConsumer(**{**RMQ, **kwargs}, vhost="/", queue=names.input, forward=names.forward_exchange,
                                        message_handlers=handlers, connection_attempts=1, retry_delay=0)


def test_blocking_client_publishes_and_gets_single_message(client, names):
    client.publish(b"task", exchange_name="", headers={"task-id": "1"}, routing_key=names.input)

    _, properties, body = client.get_single_message(names.input)

    assert body == b"task"
    assert properties.headers == {"task-id": "1"}
    assert message_count(client, names.input) == 0


def test_blocking_client_get_single_message_applies_converter(names):
    client = blocking_client(message_converter=UppercaseConverter)
    try:
        client.publish(b"task", exchange_name="", routing_key=names.input)
        _, properties, body = client.get_single_message(names.input)
    finally:
        client.close()

    assert (body, properties.content_type) == (b"TASK", "text/plain")


def test_blocking_client_get_single_message_on_empty_queue(client, names):
    assert client.get_single_message(names.input) == (None, None, None)


def test_single_message_consumer_forwards_handled_message_and_acks(client, names):
    client.publish(b"task", exchange_name="", headers={"task-id": "1"}, routing_key=names.input)

    exit_code = single_message_consumer(names, [UppercaseHandler()]).run()

    assert exit_code == 0
    assert message_count(client, names.input) == 0
    _, properties, body = client.get_single_message(names.forwarded)
    assert body == b"TASK"
    assert properties.headers == {"task-id": "1", "handled-by": "UppercaseHandler"}


def test_single_message_consumer_forwards_converted_message_with_content_type(client, names):
    client.publish(b"task", exchange_name="", headers={"task-id": "1"}, routing_key=names.input)

    exit_code = single_message_consumer(names, [PassThroughHandler()], message_converter=UppercaseConverter).run()

    assert exit_code == 0
    _, properties, body = client.get_single_message(names.forwarded)
    assert (body, properties.content_type, properties.headers) == (b"TASK", "text/plain", {"task-id": "1"})


@pytest.mark.parametrize("handler, converter", [
    pytest.param(UnsuccessfulHandler(), None, id="success-false"),
    pytest.param(FailingHandler(), None, id="handler-exception"),
    pytest.param(PassThroughHandler(), FailingConverter, id="conversion-exception"),
])
def test_single_message_consumer_rejects_message_without_forwarding(client, names, handler, converter):
    client.publish(b"task", exchange_name="", headers={"task-id": "1"}, routing_key=names.input)

    exit_code = single_message_consumer(names, [handler, UppercaseHandler()], message_converter=converter).run()

    assert exit_code == 2
    assert message_count(client, names.input) == 0
    assert message_count(client, names.forwarded) == 0


def test_single_message_consumer_returns_0_on_empty_queue(client, names):
    assert single_message_consumer(names, [UppercaseHandler()]).run() == 0


def test_single_message_consumer_returns_3_when_login_fails(client, names):
    client.publish(b"task", exchange_name="", headers={"task-id": "1"}, routing_key=names.input)

    exit_code = single_message_consumer(names, [UppercaseHandler()], password="wrong-password").run()

    assert exit_code == 3
    assert message_count(client, names.input) == 1


@pytest.mark.xfail(strict=True, raises=AttributeError,
                   reason="SingleMessageConsumer calls properties.headers.get without checking for None, a message without headers crashes run()")
def test_single_message_consumer_handles_message_without_headers(client, names):
    client.publish(b"task", exchange_name="", routing_key=names.input)

    exit_code = single_message_consumer(names, [PassThroughHandler()]).run()

    assert exit_code == 0
    assert client.get_single_message(names.forwarded)[2] == b"task"
