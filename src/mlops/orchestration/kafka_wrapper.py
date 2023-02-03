"""
Wrapper for Kafka to integrate into the pipeline when
it is used as the orchestrator of choice in the MLOPs
pipeline
"""


import logging
from typing import Any, Dict, List

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from kafka.structs import TopicPartition

from ..messaging import MLOPS_Message, json_deserializer, json_serializer

# -----------------------------------------------
# Helper Functions
# -----------------------------------------------


def _check_connection(client) -> bool:
    """Validate that connection for client is still valid"""
    return client.bootstrap_connected()


def _convert_kafka_record(
    tp: TopicPartition, msg: Dict[str, Any]
) -> MLOPS_Message:
    """
    Take elements of the Kafka record returned by a consumer poll
    and coverts it to an MLOPS_Message for easier use

    :param topic: the topic information for the message (where it was sent)
    :type topic: kafka.structs.TopicPartition
    :param msg: the resulting dictionary that contains the message data
    :type msg: Dict[str, Any]

    :return: An MLOPS_Message
    :rtype: MLOPS_Message
    """
    return MLOPS_Message(
        msg["msg_type"],
        topic=tp.topic,
        creator=msg["creator"],
        creator_type=msg["creator_type"],
        stage=msg["stage"],
        user_message=msg["user_message"],
        retries=msg["retries"],
        output=msg["output"],
        recipient=msg["recipient"],
        additional_arguments=msg["additional_arguments"],
    )


def _filter_and_convert_messages(
    msg_records: Dict[TopicPartition, List[Dict[str, Any]]], filter: callable
) -> List[MLOPS_Message]:
    """filter and convert messages from a kafka consumer poll"""
    poll_msgs = []
    for topic, msgs in msg_records.items():
        for msg in msgs:
            # convert first for more generalizable filter capability
            mlops_msg = _convert_kafka_record(topic, msg["value"])
            if filter(mlops_msg):
                poll_msgs.append(mlops_msg)
    return poll_msgs


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
    except KafkaError as e:
        logging.error(
            f"Failed to establish KafkaProducer. Did you pass a bad parameter or endpoint? Exception: {e}"
        )


def _kafka_consumer_connect(
    endpoint: str,
    subscription: str or List[str] = None,
    con_retries: int = 3,
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
        for trys in range(con_retries):
            if subscription is not None:
                kc = KafkaConsumer(
                    subscription,
                    bootstrap_servers=endpoint,
                    value_deserializer=json_deserializer,
                    **kwargs,
                )
            else:
                kc = KafkaConsumer(
                    bootstrap_servers=endpoint,
                    value_deserializer=json_deserializer,
                    **kwargs,
                )
            if kc.boostrap_connected():
                break
        return kc
    except KafkaError as e:
        logging.error(
            f"Failed to establish KafkaConsumer. Did you pass a bad parameter or endpoint? Exception: {e}"
        )


# -----------------------------------------------
# Producer
# -----------------------------------------------


def _send_success(metadata):
    """callback for a successful send of a message"""
    logging.info(
        f"Successfully sent a message to topic: {metadata.topic}, partition: {metadata.partition} at time {metadata.timestamp}"
    )


def _flush_success():
    """
    alert that messages for a producer have been pushed to
    Kafka broker successfully
    """
    logging.info("Flushed Kafka Producer")


def _kafka_send_message(
    prod: KafkaProducer,
    topic: str or List[str],
    msg: MLOPS_Message,
    flush: bool = True,
):
    """send a message to a topic(s) with the KafkaProducer"""
    try:
        if _check_connection(prod):
            # send records to all topics if a list of strings is passed
            if isinstance(topic, list):
                [
                    prod.send(top, msg.to_json()).add_callback(_send_success)
                    for top in topic
                ]
            else:
                prod.send(topic, msg.to_json()).add_callback(_send_success)

            # flush if required
            if flush:
                prod.flush().add_callback(_flush_success)

        else:
            logging.warning(
                f"Unable to send {msg.get_msg_type()} from {msg.get_creator()} due to Kafka connection issue"
            )
    except KafkaError as e:
        logging.error(f"Failed to send message to broker: {e}")


def _kafka_flush_messages(prod: KafkaProducer) -> bool:
    """Flush all buffered messages"""
    try:
        if _check_connection(prod):
            prod.flush().add_callback(_flush_success)
            return True
        else:
            logging.warning(
                "Failed to flush messages due to Kafka connection issue"
            )
            return False
    except KafkaTimeoutError as e:
        logging.error(f"Failed flush Kafka messages prior to timeout: {e}")


def _kafka_close_producer(prod: KafkaProducer):
    """Close the Kafka Producer"""
    try:
        if _check_connection(prod):
            prod.close()
            logging.info("Kafka producer has been closed")
    except KafkaError as e:
        logging.error(f"Failed close Kafka producer: {e}")


# -----------------------------------------------
# Consumer
# -----------------------------------------------


def _kafka_subscribe(con: KafkaConsumer, topic: str or List[str]):
    """
    Subscribe to Kafka topic(s). This will override previous subscriptions.
    """
    try:
        if _check_connection(con):
            con.subscribe(topic)
            logging.info(f"Subscribed to {str(topic)}")
    except KafkaError as e:
        logging.error(f"Failed to subscribe to topics: {e}")


def _kafka_poll_messages(
    con: KafkaConsumer, filter: callable
) -> List[MLOPS_Message]:
    """Pull messages from kafka logs that consumer is subscribed to"""
    try:
        if _check_connection(con):
            records = con.poll()
            return _filter_and_convert_messages(records, filter)
        return []
    except KafkaError as e:
        logging.error(f"Failed to read messages with Kafka consumer: {e}")


def _kafka_close_consumer(con: KafkaConsumer):
    """Close the Kafka Producer"""
    try:
        if _check_connection(con):
            con.close()
            logging.info("Kafka consumer has been closed")
    except KafkaError as e:
        logging.error(f"Failed close Kafka consumer: {e}")


def _kafka_unsubscribe(con: KafkaConsumer):
    """Unsubscribe from all current subscriptions"""
    con.unsubscribe()
    assert len(_kafka_get_subscriptions(con)) == 0


def _kafka_get_subscriptions(con: KafkaConsumer) -> List[str]:
    """Get the current subscription"""
    return list(con.subscription())


# -----------------------------------------------
# Admin
# -----------------------------------------------
