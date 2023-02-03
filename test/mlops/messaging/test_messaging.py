"""
Test messaging structure (serialization, deserialization, and message building
"""

import mlops.messaging as mes


def test_serial_deserial():
    test_dict = {"main_msg": "This is a message", "alt_msg": "hello world"}
    serial_str = mes.json_serializer(test_dict)
    assert isinstance(serial_str, bytes)
    deserial_dict = mes.json_deserializer(serial_str)
    assert isinstance(deserial_dict, dict)
    assert deserial_dict["main_msg"] == "This is a message"


def test_building_message():
    # test Producer/Consumer Naming
    test_ingest = mes._set_caller_type("ingest")
    assert test_ingest == "ingestion"
    test_mlops = mes._set_caller_type("ETL")
    assert test_mlops == "mlops"
    test_bad_str = mes._set_caller_type("Coeus")
    assert test_bad_str == "unknown"

    # test building MLOPS_messages
    test_msg = "Hello World"
    new_msg = mes.MLOPS_Message(
        mes.Message_Type.START.name,
        topic="Pipeline",
        creator="ETL",
        user_message=test_msg,
        another_keyword="other stuff",
    )
    assert new_msg.get_topic() == "Pipeline"
    assert new_msg.get_creator_type() == "mlops"
    if new_msg.get_output() is None:
        assert True
    assert new_msg.get_user_message() == test_msg
