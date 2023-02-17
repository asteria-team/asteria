"""
Test consumer in context of kafka orchestration docker-compose
"""

import mlops.orchestration as orc

orch = "kafka"
dc_endpoint = "kafka:9092"


def test_kafka_consumer():
    """Test reading messages to kafka"""
    new_con = orc.Consumer(
        "Cool-New-Topic",
        orc.Orchestration(orch, dc_endpoint),
        auto_offset_reset="earliest",
    )

    new_con.subscribe("Another-Topic", False)

    msgs = []
    while msgs == []:
        msgs = new_con.recieve(filter=lambda msg: msg.topic == "Cool-New-Topic")
    assert isinstance(msgs, list)
    assert msgs[0].output == "/dataset/data"
    assert msgs[0].next_task == "Train"

    """ Check that a filter on a topic with o messages returns nothing """
    msgs = new_con.recieve(filter=lambda msg: msg.topic == "Another-Topic")
    assert msgs == []
    new_con.close()

    """ Check consuming messages on a different subscription """
    new_con = orc.Consumer("Sick-Topic", auto_offset_reset="earliest")

    msgs = []
    while msgs == []:
        msgs = new_con.recieve()

    assert isinstance(msgs, list)
    assert msgs[0].output == "/dataset/data"
    assert msgs[0].user_message == "Hello Pipeline"


if __name__ == "__main__":
    test_kafka_consumer()
