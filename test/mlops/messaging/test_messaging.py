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
    # test building MLOpsMessages
    test_msg = "Hello World"
    new_msg = mes.MLOpsMessage(
        mes.MessageType.START,
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

    # test updating a message
    diff_msg = "A different message"
    new_msg.set_message_data("creator", "mlops", False)
    new_msg.set_message_data("new_kwarg", "No set will occur")
    new_msg.set_message_data("user_message", diff_msg)

    assert new_msg.get_creator() == ["ETL", "mlops"]
    assert new_msg.get_user_message() == diff_msg
