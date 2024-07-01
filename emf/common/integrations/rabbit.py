import functools
import time
import logging
import pika
import config
from typing import List
from emf.common.config_parser import parse_app_properties

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

    def publish(self, payload: str, exchange_name: str, headers: dict | None = None, routing_key: str = ''):
        # Publish message
        self.publish_channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=payload,
            properties=pika.BasicProperties(
                headers=headers
            )
        )

    def get_single_message(self, queue: str, auto_ack: bool = True):
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

    def consume_start(self, queue: str, callback: object | None = None, auto_ack: bool = True):

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

        logger.info(f"Waiting for messages in {queue}. To exit press CTRL+C")

        try:
            self.consume_channel.start_consuming()
            self.consuming = True
        except KeyboardInterrupt:
            self.consume_stop()

    def shovel(self,
               from_queue: str,
               to_exchange: str,
               callback: object | None = None,
               headers: dict | None = None,
               routing_key: str = ''):

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


class RMQConsumer:
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.
    """

    def __init__(self,
                 host: str = RMQ_SERVER,
                 port: int = int(RMQ_PORT),
                 vhost: str = RMQ_VHOST,
                 que: str | None = None,
                 username: str = RMQ_USERNAME,
                 password: str = RMQ_PASSWORD,
                 heartbeat: str | int = RMQ_HEARTBEAT_IN_SEC,
                 message_handlers: List[object] | None = None,
                 message_converter: object | None = None):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.
        """
        self.message_handlers = message_handlers
        self.message_converter = message_converter
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1

        self._host = host
        self._port = port
        self._vhost = vhost
        self._que = que
        self._username = username

        self._connection_parameters = pika.ConnectionParameters(host=self._host,
                                                                port=self._port,
                                                                virtual_host=self._vhost,
                                                                credentials=pika.PlainCredentials(username, password))
        self.set_heartbeat(heartbeat=heartbeat)

    def set_heartbeat(self, heartbeat: str | int = RMQ_HEARTBEAT_IN_SEC):
        """
        Brings heartbeat parameter out to be configured
        NB! guard is to added not to switch the heartbeat off
        :param heartbeat: new heartbeat value to send to server
        """
        if heartbeat:
            if isinstance(heartbeat, str):
                try:
                    heartbeat = int(heartbeat)
                except ValueError:
                    heartbeat = None
            # Do not switch the heartbeat off
            if heartbeat and heartbeat > 0:
                self._connection_parameters.heartbeat = heartbeat

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        logger.info(f"Connecting to {self._host}:{self._port} @ {self._vhost} as {self._username}")

        return pika.SelectConnection(
            parameters=self._connection_parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            logger.info("Connection is closing or already closed")
        else:
            logger.info("Closing connection")
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        :param pika.SelectConnection _unused_connection: The connection
        """
        logger.info("Connection opened")
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        logger.error(f"Connection open failed", exc_info=err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning(f"Connection closed, reconnect necessary: {reason}")
            self.reconnect()

    def reconnect(self):
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.
        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        logger.info("Creating a new channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use.
        :param pika.channel.Channel channel: The channel object
        """
        logger.info("Channel opened")
        self._channel = channel
        self.add_on_channel_close_callback()
        self.set_qos()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.
        """
        logger.info("Adding channel close callback")
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        logger.warning(f"Channel {channel} was closed: {reason}")
        self.close_connection()

    def set_qos(self):
        """This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.
        """
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        """Invoked by pika when the Basic.QoS method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.
        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame
        """
        logger.info(f"QOS set to: {self._prefetch_count}")
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        logger.info("Issuing consumer related RPC commands")
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self._que, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        logger.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.
        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        logger.info(f"Consumer was cancelled remotely, shutting down: {method_frame}")

        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.
        :param pika.channel.Channel _unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body
        """
        logger.info(f"Received message # {basic_deliver.delivery_tag} from {properties.app_id} meta: {properties.headers}")
        logger.debug(f"Message body: {body}")

        ack = True

        # Convert if needed
        if self.message_converter:
            try:
                body, content_type = self.message_converter.convert(body)
                properties.content_type = content_type
                logger.info(f"Message converted")
            except Exception as error:
                logger.error(f"Message conversion failed: {error}", exc_info=True)
                # ack = False

        if self.message_handlers:
            for message_handler in self.message_handlers:
                try:
                    logger.info(f"Handling message with handler: {message_handler.__class__.__name__}")
                    body = message_handler.handle(body, properties=properties)
                except Exception as error:
                    logger.error(f"Message handling failed: {error}", exc_info=True)
                    # ack = False

        if ack:
            self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.
        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
        """
        logger.info(f"Acknowledging message {delivery_tag}")
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel:
            logger.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            cb = functools.partial(self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.
        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)
        """
        self._consuming = False
        logger.info(f"RabbitMQ acknowledged the cancellation of the consumer: {userdata}")
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.
        """
        logger.info("Closing the channel")
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.
        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if not self._closing:
            self._closing = True
            logger.info(f"Stopping")
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            logger.info(f"Stopped")


class ReconnectingConsumer:
    """This is an example consumer that will reconnect if the nested
    RMQConsumer indicates that a reconnect is necessary.
    """
    def __init__(self,
                 host: str = RMQ_SERVER,
                 port: int = int(RMQ_PORT),
                 vhost: str = RMQ_VHOST,
                 que: str | None = None,
                 username: str = RMQ_USERNAME,
                 password: str = RMQ_PASSWORD,
                 message_handler: object | None = None,
                 message_converter: object | None = None):
        self._reconnect_delay = 0
        self._host = host
        self._port = port
        self._vhost = vhost
        self._que = que
        self._username = username
        self.__password = password
        self.message_handler = message_handler
        self.message_converter = message_converter
        self._consumer = RMQConsumer(self._host,
                                     self._port,
                                     self._vhost,
                                     self._que,
                                     self._username,
                                     self.__password,
                                     self.message_handler,
                                     self.message_converter)

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            logger.info(f"Reconnecting after {reconnect_delay} seconds")
            time.sleep(reconnect_delay)
            self._consumer.run()

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay

    def stop(self):
        self._consumer.stop()


if __name__ == '__main__':
    # Testing RMQ API
    import sys
    logging.basicConfig(stream=sys.stdout,
                        format="%(levelname) -10s %(asctime) -10s %(name) -35s %(funcName) -30s %(lineno) -5d: %(message)s",
                        level=logging.INFO)

    host = r'test-rscrabbit.elering.sise'
    port = 5670
    vhost = r'/'
    que = 'object-storage.schedules.iec'
    username = None
    password = None

    # Blocking client
    client = BlockingClient()
    method_frame, properties, body = client.get_single_message(queue=que)

    # Consumer
    consumer = RMQConsumer(host=host,
                           port=port,
                           vhost=vhost,
                           que=que,
                           username=username,
                           password=password,
                           message_handler=None)
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()
