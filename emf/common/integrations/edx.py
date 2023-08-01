from EDX import create_client
from emf.common.config_parser import parse_app_properties
import logging
import time
import sys

logger = logging.getLogger(__name__)

parse_app_properties(globals(), config.paths.edx_integration.edx)

class EDX(create_client):

    def __init__(self, server=EDX_SERVER, username=EDX_USERNAME, password=EDX_PASSWORD, debug=True, handler=None, converter=None, message_type=None):
        super().__init__(server, username, password, debug)
        self.message_handler = handler
        self.message_converter = converter
        self.message_type = message_type

    def run(self, retry_delay_s=10):
        while True:
            message = self.receive_message(self.message_type)
        
            if not message.receivedMessage:
                logger.debug(f"No messages available with message type: {self.message_type}, retry in {retry_delay_s}")
                time.sleep(retry_delay_s)
                continue
                #break
        
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
                self.message_handler.send(body, properties)
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
