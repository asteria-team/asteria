"""
Wrapper for Kafka to integrate into the pipeline when
it is used as the orchestrator of choice in the MLOPs
pipeline
"""


import logging
from typing import List

from kafka import KafkaConsumer, KafkaProducer

from ..messaging import MLOPS_Message, json_deserializer, json_serializer

# -----------------------------------------------
# Helper Functions
# -----------------------------------------------


def _check_connection_prod(client) -> bool:
    """Validate that connection for client is still valid"""
    return client.bootstrap_connected()


# -----------------------------------------------
# Connection
# -----------------------------------------------


def _kafka_producer_connect(
    endpoint: str, con_retries: int = 3, **kwargs
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
        for trys in range(con_retries):
            kp = KafkaProducer(
                bootstrap_servers=endpoint,
                value_serializer=json_serializer,
                **kwargs,
            )
            if kp.bootstrap_connected():
                break
        return kp
    except Exception as e:
        logging.error(
            f"Failed to establish KafkaProducer. Did you pass a bad parameter or endpoint? Exception: {e}"
        )


def _kafka_consumer_connect(
    endpoint: str,
    subscription: str or List[str],
    con_retries: int = 3,
    **kwargs,
) -> KafkaConsumer:
    """
    Creates a consumer and connects it to the kafka broker

    :param endpoint: the connection point for a kafka broker
    :type endpoint: str
    :param subscription: the topic to subscribe to in Kafka
    :type subscription: str
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
        for trys in range(con_retries):
            kc = KafkaConsumer(
                subscription,
                bootstrap_servers=endpoint,
                value_deserializer=json_deserializer,
                **kwargs,
            )
            if kc.boostrap_connected():
                break
        return kc
    except Exception as e:
        logging.error(
            f"Failed to establish KafkaConsumer. Did you pass a bad parameter or endpoint? Exception: {e}"
        )


# -----------------------------------------------
# Producer
# -----------------------------------------------


def _kafka_send_message(
    prod: KafkaProducer, topic: str, msg: MLOPS_Message, flush: bool = True
):
    """"""
    pass


def _kafka_flush_messages():
    """Flush all buffered messages"""


# -----------------------------------------------
# Consumer
# -----------------------------------------------


def _kafka_subscribe():
    pass


def _kafka_poll_messages():
    pass


# -----------------------------------------------
# Admin
# -----------------------------------------------
