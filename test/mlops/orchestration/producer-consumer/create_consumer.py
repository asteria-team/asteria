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
        orch,
        dc_endpoint,
        auto_offset_reset="earliest",
    )
    new_con.subscribe("Another-Topic")
    assert len(new_con.get_subscriptions()) == 2
    msgs = new_con.get_messages(
        filter=lambda msg: msg.get_topic() == "Cool-New-Topic"
    )
    assert isinstance(msgs, list)
    if msgs == []:
        # may have to poll a second time
        msgs = new_con.get_messages(
            filter=lambda msg: msg.get_topic() == "Cool-New-Topic"
        )
    if msgs != []:
        assert msgs[0].get_output() == "/dataset/data"

    # check filter
    msgs = new_con.get_messages(
        filter=lambda msg: msg.get_topic() == "Another-Topic"
    )
    assert msgs == []
    new_con.unsubscribe()
    assert new_con.get_subscriptions() == []
    new_con.close()


if __name__ == "__main__":
    test_kafka_consumer()
