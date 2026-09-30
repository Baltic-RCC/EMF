import types
from unittest import mock

import pytest

from emf.common.integrations import edx


class StopLoop(Exception):
    pass


def _received(message_id, content):
    received = types.SimpleNamespace(messageID=message_id, content=content,
                                     __values__={"messageID": message_id, "businessType": "IEC-SCHEDULE", "content": content})
    return types.SimpleNamespace(receivedMessage=received, remainingMessagesCount=0)


def _empty():
    return types.SimpleNamespace(receivedMessage=None, remainingMessagesCount=0)


@pytest.fixture
def client():
    client = edx.EDX.__new__(edx.EDX)
    client.message_types = ["IEC-SCHEDULE"]
    client.message_handler = mock.Mock()
    client.message_converter = mock.Mock()
    client.message_converter.convert.side_effect = lambda body: (b"converted:" + body, "application/json")
    client.receive_message = mock.Mock()
    client.confirm_received_message = mock.Mock()
    return client


@pytest.fixture
def patched_time():
    with mock.patch.object(edx, "time") as patched:
        patched.sleep.side_effect = StopLoop
        yield patched


def test_run_converts_sends_and_confirms_message(client, patched_time):
    client.receive_message.side_effect = [_received("msg-1", b"xml"), _empty()]
    calls = mock.Mock()
    calls.attach_mock(client.message_handler.send, "send")
    calls.attach_mock(client.confirm_received_message, "confirm")

    with pytest.raises(StopLoop):
        client.run(retry_delay_s=3)

    assert calls.mock_calls == [
        mock.call.send(byte_string=b"converted:xml", properties={"messageID": "msg-1", "businessType": "IEC-SCHEDULE"}),
        mock.call.confirm("msg-1"),
    ]
    client.receive_message.assert_called_with("IEC-SCHEDULE")
    patched_time.sleep.assert_called_once_with(3)


def test_run_does_not_confirm_message_when_sending_fails(client, patched_time):
    client.receive_message.side_effect = [_received("msg-1", b"xml")]
    client.message_handler.send.side_effect = ConnectionError("rabbit down")

    with pytest.raises(ConnectionError):
        client.run()

    client.confirm_received_message.assert_not_called()


def test_run_confirms_message_without_handler_or_converter(client, patched_time):
    client.message_handler = None
    client.message_converter = None
    client.receive_message.side_effect = [_received("msg-1", b"xml"), _empty()]

    with pytest.raises(StopLoop):
        client.run()

    client.confirm_received_message.assert_called_once_with("msg-1")
