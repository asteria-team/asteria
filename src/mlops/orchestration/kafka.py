"""
Wrapper for Kafka to integrate into the pipeline when
it is used as the orchestrator of choice in the MLOPs
pipeline
"""


import logging
import re
from typing import Dict, List, Union

from kafka import KafkaClient, KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from kafka.structs import TopicPartition

from ..messaging import MessageDeserializer, MLOpsMessage
from .interface import AdminInterface, ConsumerInterface, ProducerInterface
from .orchestration_discovery import Orchestration

""" Number of connection attempts to the Kafka Brokers """
connection_retries = 3

allowed_kafka_topic_chars = "^[A-Za-z0-9_-]+$"
# -----------------------------------------------
# Kafka Classes
# -----------------------------------------------


class KP(ProducerInterface):
    """Class for the Kafka Producer"""

    def __init__(self, orch: Orchestration, **kwargs):
        """
        See the docs for options and defaults for initialization:
        `KafkaProducer <https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html>`_
        """
        super().__init__()
        self.producer = _kafka_producer_connect(
            orch.connection, connection_retries, **kwargs
        )

    def send(
        self, topic: str or List[str], msg: MLOpsMessage, flush: bool = False
    ):
        """send a message to a topic(s) with the KafkaProducer"""
        try:
            if _check_connection(self.producer):
                # send records to all topics if a list of strings is passed
                assert _validate_topic(topic)
                if isinstance(topic, list):
                    [
                        self.producer.send(
                            top, msg.json_serialization()
                        ).add_callback(_send_success)
                        for top in topic
                    ]
                else:
                    self.producer.send(
                        topic, msg.json_serialization()
                    ).add_callback(_send_success)

                # flush if required
                if flush:
                    self.flush()

            else:
                logging.warning(
                    f"Unable to send {msg.message_type} from {msg.creator} due to Kafka connection issue"
                )
        except KafkaError as e:
            logging.error(f"Failed to send message to broker: {e}")
        except AssertionError as e:
            logging.error(f"Invalid Topic passed: {e}")

    def _flush(self):
        """Flush all buffered messages"""
        try:
            if _check_connection(self.producer):
                self.producer.flush()
            logging.warning(
                "Failed to flush messages due to Kafka connection issue"
            )
        except KafkaTimeoutError as e:
            logging.error(f"Failed flush Kafka messages prior to timeout: {e}")

    def close(self):
        """Close the Kafka Producer"""
        try:
            self.producer.close()
        except KafkaError as e:
            logging.error(f"Failed close Kafka producer: {e}")


class KC(ConsumerInterface):
    """Class for the Kafka Consumer"""

    def __init__(
        self, orch: Orchestration, subscription: Union[str, List[str]], **kwargs
    ):
        """
        See the docs for options and defaults for initialization:
        `KafkaConsumer <https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html>`_
        """
        super().__init__()
        try:
            assert _validate_topic(subscription)
            self.consumer = _kafka_consumer_connect(
                orch.connection, subscription, connection_retries, **kwargs
            )
        except AssertionError as e:
            logging.error(f"Invalid Topic passed: {e}")

    def subscribe(self, topic: Union[str, List[str]]):
        """Subscribe to Kafka topic(s). This overrides previous subscriptions."""
        try:
            if _check_connection(self.consumer):
                assert _validate_topic(topic)
                self.consumer.subscribe(topic)
        except KafkaError as e:
            logging.error(f"Failed to subscribe to topics: {e}")
        except AssertionError as e:
            logging.error(f"Invalid Topic passed: {e}")

    def recieve(
        self, filter_function: callable, **kwargs
    ) -> List[MLOpsMessage]:
        """Pull messages from kafka logs that consumer is subscribed to"""
        try:
            if _check_connection(self.consumer):
                timeout = int(kwargs.get("timeout", 1000))
                records = self.consumer.poll(timeout)
                return _filter_and_convert_messages(records, filter_function)
            logging.warning(
                "Failed to poll messages due to Kafka connection issue"
            )
            return []
        except KafkaError as e:
            logging.error(f"Failed to read messages with Kafka consumer: {e}")

    def close(self):
        """Close the Kafka Consumer"""
        try:
            self.consumer.close()
        except KafkaError as e:
            logging.error(f"Failed close Kafka consumer: {e}")


class KA(AdminInterface):
    """Class for the Kafka Admin Client"""

    def __init__(self, orch: Orchestration, **kwargs):
        """
        See the docs for options and defaults for initialization:
        `KafkaClient <https://kafka-python.readthedocs.io/en/master/apidoc/KafkaClient.html>`_
        """
        super().__init__()
        self.admin = _kafka_admin_connect(
            orch.connection, connection_retries, **kwargs
        )

    def validate_connection(self) -> bool:
        return _check_connection(self.admin)

    def close(self):
        """Close the Kafka Admin"""
        try:
            self.admin.close()
        except KafkaError as e:
            logging.error(f"Failed close Kafka Admin Client: {e}")


# -----------------------------------------------
# Helper Functions
# -----------------------------------------------


def _check_connection(client: Union[KP, KC, KA]) -> bool:
    """Validate that connection for client is still valid"""
    return client.bootstrap_connected()


def _send_success(metadata):
    """callback for a successful send of a message"""
    logging.info(
        f"Successfully sent a message to topic: {metadata.topic}, partition: {metadata.partition} at time {metadata.timestamp}"
    )


def _validate_topic(topic: Union[str, List[str]]) -> bool:
    """Ensure user passed topics meet kafka requirements ()"""
    if isinstance(topic, str):
        topic = [topic]
    for top in topic:
        if not bool(re.match(allowed_kafka_topic_chars, top)):
            logging.warning(
                f"Topic name: {top} contains invalid cahracters. Must be alphanumeric"
            )
            return False
    return True


def _filter_and_convert_messages(
    msg_records: Dict[TopicPartition, List[bytearray]], filter: callable
) -> List[MLOpsMessage]:
    """filter and convert messages from a kafka consumer poll"""
    poll_msgs = []
    for _, msgs in msg_records.items():
        for msg in msgs:
            # convert first for more generalizable filter capability
            mlops_data = MessageDeserializer(msg.value)
            mlops_msg = mlops_data()
            if filter(mlops_msg):
                poll_msgs.append(mlops_msg)
    return poll_msgs


# -----------------------------------------------
# Connection Helpers
# -----------------------------------------------


def _kafka_producer_connect(
    endpoint: str, con_retries: int, **kwargs
) -> KafkaProducer:
    """
    Creates a producer and connects it to the kafka broker

    :param endpoint: the connection point for a kafka broker
    :type endpoint: str
    :param retries: optional times to attempt to connect to the broker if first
            instantiation of the producer failed. Set to three if not passed
    :type retries: int
    :param **kwargs: any additional configuration requirements desired for the
            KafkaProducer. See the docs for options and defaults:
            `KafkaProducer <https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html>`_

    :return: A KafkaProducer client that publishes records to Kafka
    :rtype: KafkaProducer
    """
    try:
        for _ in range(con_retries):
            kp = KafkaProducer(
                bootstrap_servers=endpoint,
                **kwargs,
            )
            if kp is not None and kp.bootstrap_connected():
                break
        return kp
    except KafkaError as e:
        logging.error(
            f"Failed to establish KafkaProducer. Did you pass a bad parameter or endpoint? Exception: {e}"
        )


def _kafka_consumer_connect(
    endpoint: str,
    subscription: Union[str, List[str]],
    con_retries: int,
    **kwargs,
) -> KafkaConsumer:
    """
    Creates a consumer and connects it to the kafka broker

    :param endpoint: the connection point for a kafka broker
    :type endpoint: str
    :param subscription: the topic to subscribe to in Kafka. If none passed
            then subscription will have to occur later
    :type subscription: str or List[str]
    :param retries: optional times to attempt to connect to the broker if first
            instantiation of the producer failed. Set to three if not passed
    :type retries: int
    :param **kwargs: any additional configuration requirements desired for the
            KafkaConsumer. See the docs for options and defaults:
            `KafkaConsumer <https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html>`_

    :return: A KafkaConsumer client that can read records from Kafka
    :rtype: KafkaConsumer
    """
    try:
        for _ in range(con_retries):
            kc = KafkaConsumer(
                subscription,
                bootstrap_servers=endpoint,
                **kwargs,
            )
            if kc is not None and kc.bootstrap_connected():
                break
        return kc
    except KafkaError as e:
        logging.error(
            f"Failed to establish KafkaConsumer. Did you pass a bad parameter or endpoint? Exception: {e}"
        )


def _kafka_admin_connect(
    endpoint: str, con_retries: int, **kwargs
) -> KafkaClient:
    """
    Creates an admin client and connects it to the kafka broker

    :param endpoint: the connection point for a kafka broker
    :type endpoint: str
    :param retries: optional times to attempt to connect to the broker if first
            instantiation of the producer failed. Set to three
    :type retries: int
    :param **kwargs: any additional configuration requirements desired for the
            KafkaClient. See the docs for options and defaults:
            `KafkaClient <https://kafka-python.readthedocs.io/en/master/apidoc/KafkaClient.html>`_

    :return: A Kafka admin client that can access metadata information
    :rtype: KafkaClient
    """
    try:
        for _ in range(con_retries):
            ka = KafkaClient(
                bootstrap_servers=endpoint,
                **kwargs,
            )
            if ka is not None and ka.bootstrap_connected():
                break
        return ka
    except KafkaError as e:
        logging.error(
            f"Failed to establish KafkaConsumer. Did you pass a bad parameter or endpoint? Exception: {e}"
        )
