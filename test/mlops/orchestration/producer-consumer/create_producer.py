"""
Test producer in context of kafka orchestration docker-compose
"""

from mlops.messaging import Message_Type, MLOPS_Message
from mlops.orchestration import Producer

orch = "kafka"
dc_endpoint = "kafka:9092"


def test_kafka_producer():
    """Test sending messages to kafka"""
    new_prod = Producer(
        orchestrator_endpoint=dc_endpoint,
        orchestrator=orch,
        topic="Cool-New-Topic",
    )
    mlops_msg = MLOPS_Message(
        Message_Type.START.name,
        creator="ingest",
        user_message="Hello World",
        output="/dataset/data",
    )
    new_prod.send(mlops_msg)
    new_prod.send(mlops_msg)
    assert new_prod.flush()

    """ Test sending a message to multiple topics """
    new_prod2 = Producer(
        orchestrator_endpoint=dc_endpoint,
        orchestrator=orch,
        topic=["Cool-New-Topic", "Sick-Topic"],
    )
    mlops_msg2 = MLOPS_Message(
        Message_Type.COMPLETE.name,
        creator="training",
        user_message="Hello Pipeline",
        output="/dataset/data",
    )
    new_prod2.send(mlops_msg2)


if __name__ == "__main__":
    test_kafka_producer()
