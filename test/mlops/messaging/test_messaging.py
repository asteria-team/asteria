"""
Test messaging structure (serialization, deserialization, and message building
"""

import mlops.messaging as mes


def test_serial_deserial():
    test_dict = {"main_msg": "This is a message", "alt_msg": "hello world"}
    serial_str = mes._json_serializer(test_dict)
    assert isinstance(serial_str, bytes)
    deserial_dict = mes._json_deserializer(serial_str)
    assert isinstance(deserial_dict, dict)
    assert deserial_dict["main_msg"] == "This is a message"


def test_message():
    # test building MLOpsMessages
    test_msg = "Hello World"
    msg_builder = mes.MessageBuilder()
    new_msg = (
        msg_builder.with_msg_type(mes.MessageType.START)
        .with_creator_type("ETL")
        .with_creator("ETL")
        .with_user_msg(test_msg)
        .with_next_task("Train")
        .build()
    )

    assert new_msg.topic is None
    assert new_msg.creator_type == "mlops"
    assert new_msg.creator == "ETL"
    assert new_msg.user_message == test_msg

    # test serialize and deserialize MLOpsMessage
    serial_msg = new_msg.json_serialization()
    deserial = mes.MessageDeserializer(serial_msg)
    deserial_msg = deserial()

    assert deserial_msg.topic is None
    assert deserial_msg.output is None
    assert deserial_msg.creator_type == "mlops"
    assert deserial_msg.creator == "ETL"
    assert deserial_msg.user_message == test_msg
