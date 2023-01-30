"""
Test messaging structure (serialization, deserialization, and message building)
"""

import mlops.orchestration.messaging as orcmes
from typing import Dict


def test_serial_deserial():
    test_dict = {"main_msg": "This is a message", "alt_msg": "hello world"}
    serial_str = orcmes.json_serializer(test_dict)
    assert isinstance(serial_str, bytes)
    deserial_dict = orcmes.json_deserializer(serial_str)
    assert isinstance(deserial_dict, dict)


def test_building_message():
    # test Producer/Consumer Naming
    test_ingest = orcmes.set_producer_consumer_type("ingest")
    assert test_ingest == "ingestion"
    test_mlops = orcmes.set_producer_consumer_type("ETL")
    assert test_mlops == "mlops"
    test_bad_str = orcmes.set_producer_consumer_type("Coeus")

    # test building kafka messages
    test_msg = "Hello World"
    new_msg = orcmes.build_kafka_json(
        orcmes.Stage_Status.START.name, test_mlops, user_msg=test_msg, retries=0
    )
    assert new_msg["producer"] == "mlops"
    assert new_msg["output"], AssertionError
    assert new_msg["user_msg"] == test_msg
