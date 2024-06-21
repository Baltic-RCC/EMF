from EDX import create_client
import config
from emf.common.config_parser import parse_app_properties
import logging
import time
import sys

logger = logging.getLogger(__name__)

parse_app_properties(globals(), config.paths.integrations.edx)


class EDX(create_client):

    def __init__(self,
                 server: str = EDX_SERVER,
                 username: str = EDX_USERNAME,
                 password: str = EDX_PASSWORD,
                 debug: bool = True,
                 handler=None,
                 converter=None,
                 message_types: list[str] | None = None,
                 ):
        super().__init__(server, username, password, debug)
        self.message_handler = handler
        self.message_converter = converter
        self.message_types = message_types

    def run(self, retry_delay_s=10):
        while True:
            for message_type in self.message_types:
                message = self.receive_message(message_type)

                if not message.receivedMessage:
                    logger.info(f"No messages available with message type: {message_type}, retry in {retry_delay_s}")
                    time.sleep(retry_delay_s)
                    continue
                    # break

                logger.info(f"Downloading message with ID: {message.receivedMessage.messageID}")

                # Extract data
                body = message.receivedMessage.content

                # Extract metadata
                properties = dict(message.receivedMessage.__values__)
                properties.pop('content', None)

                logger.info(f'Received message with metadata {properties}', extra=properties)

                # Convert if needed
                if self.message_converter:
                    body, content_type = self.message_converter.convert(body)
                    logger.info(f"Message converted")

                # Send message where needed
                if self.message_handler:
                    self.message_handler.send(byte_string=body, properties=properties)
                    logger.info(f"Message sent")

                # ACK/Mark message received and move to next one
                self.confirm_received_message(message.receivedMessage.messageID)
                logger.info(f"Number of messages left {message.remainingMessagesCount}")


# TEST
if __name__ == "__main__":
    from converters import iec_schedules_to_json
    from converters import opdm_metadata_to_json
    import elk_batch_send

    handler = elk_batch_send.Handler(settings.out_target_uri)
    connection = EDXconsumer(handler=handler, converter=iec_schedules_to_json, message_type="IEC-SCHEDULE")

    # start liseting
    connection.run()