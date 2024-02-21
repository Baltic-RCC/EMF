import logging
import pika
import config
import sys
from emf.common.config_parser import parse_app_properties

logging.basicConfig(stream=sys.stdout,
                    format="%(levelname) -10s %(asctime) -10s %(name) -35s %(funcName) -30s %(lineno) -5d: %(message)s",
                    level=logging.INFO)

logger = logging.getLogger(__name__)

parse_app_properties(globals(), config.paths.integrations.rabbit)

class BlockingClient:

    def __init__(self,
                 host: str = RMQ_SERVER,
                 port: int = int(RMQ_PORT),
                 username: str = RMQ_USERNAME,
                 password: str = RMQ_PASSWORD,
                 message_converter: object | None = None,
                 message_handler: object | None = None,
                 ):
        self.connection_params = {
            'host': host,
            'port': port,
            'credentials': pika.PlainCredentials(username, password)
        }
        self.message_converter = message_converter
        self.message_handler = message_handler
        self._connect()
        self.consuming = False

    def _connect(self):
        # Connect to RabbitMQ server
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(**self.connection_params)
        )
        self.publish_channel = self.connection.channel()
        self.consume_channel = self.connection.channel()

    def publish(self, payload, exchange_name, headers=None, routing_key=''):
        # Publish message
        self.publish_channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=payload,
            properties=pika.BasicProperties(
                headers=headers
            )
        )

    def get_single_message(self, queue, auto_ack=True):
        """
        Attempt to fetch a single message from the specified queue.

        :param queue: The name of the queue to fetch the message from.
        :param auto_ack: Whether to automatically acknowledge the message. Defaults to True.
        :return: The method frame, properties, and body of the message if available; otherwise, None.
        """

        # Stop previous consume
        if self.consuming:
            self.consume_stop()

        method_frame, properties, body = self.consume_channel.basic_get(queue, auto_ack=auto_ack)

        if method_frame:
            logger.info(f"Received message from {queue}: {properties}")

            # Convert message
            if self.message_converter:
                try:
                    body, content_type = self.message_converter.convert(body)
                    properties.content_type = content_type
                    logger.info(f"Message converted")
                except Exception as error:
                    logger.error(f"Message conversion failed: {error}")
            return method_frame, properties, body
        else:
            logger.info(f"No message available in queue {queue}")
            return None, None, None

    def consume_start(self, queue, callback=None, auto_ack=True):

        # Stop previous consume
        if self.consuming:
            self.consume_stop()

        # Set up consumer
        if not callback:
            callback = lambda ch, method, properties, body: print(f"Received message: {properties} (No callback processing)")

        self.consume_channel.basic_consume(
            queue=queue,
            on_message_callback=callback,
            auto_ack=auto_ack
        )

        print(f"Waiting for messages in {queue}. To exit press CTRL+C")

        try:
            self.consume_channel.start_consuming()
            self.consuming = True
        except KeyboardInterrupt:
            self.consume_stop()

    def shovel(self, from_queue, to_exchange, callback=None, headers=None, routing_key=''):

        def internal_callback(ch, method, properties, body):

            if callback:
                ch, method, properties, body = callback(ch, method, properties, body)

            new_headers = properties.headers if properties.headers else {}
            # Add or update the 'shovelled' flag
            new_headers['shovelled'] = True

            # If additional headers were provided, merge them with the existing ones
            if headers:
                new_headers.update(headers)

            self.publish(body, to_exchange, headers=new_headers, routing_key=routing_key)

            # Manually acknowledge the message to ensure it's only removed from the queue after successful processing
            ch.basic_ack(delivery_tag=method.delivery_tag)

        # Start consuming with the internal callback. Set auto_ack=False for manual ack in the callback.
        self.consume_start(from_queue, callback=internal_callback, auto_ack=False)

    def consume_stop(self):
        self.consume_channel.stop_consuming()
        self.consuming = False

    def close(self):

        # Stop consuming
        if self.consuming:
            self.consume_stop()

        # Close the connection
        if self.connection.is_open:
            self.connection.close()

    def __del__(self):
        # Destructor to ensure the connection is closed properly
        self.close()


if __name__ == "__main__":
    # RabbitMQ server connection details
    # host = 'your_rabbitmq_server_ip'
    # port = 5672  # default RabbitMQ port
    # username = 'your_username'
    # password = 'your_password'
    client = BlockingClient()

    # Get single message
    queue_name = 'object-storage.schedules.iec'
    # queue_name = 'object-storage.models.synchro'
    method_frame, properties, body = client.get_single_message(queue=queue_name)
