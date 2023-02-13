"""
Test producer in context of kafka orchestration docker-compose
"""

from mlops.messaging import MessageType, MLOpsMessage
from mlops.orchestration import Producer

orch = "kafka"
dc_endpoint = "kafka:9092"


def test_kafka_producer():
    """Test sending messages to kafka"""
    new_prod = Producer(
        topic="Cool-New-Topic",
        orchestrator=orch,
        orchestrator_endpoint=dc_endpoint,
    )
    mlops_msg = MLOpsMessage(
        MessageType.START,
        creator="ingest",
        user_message="Hello World",
        output="/dataset/data",
    )
    new_prod.send(mlops_msg)
    new_prod.send(mlops_msg)
    assert new_prod.flush()

    """ Test sending a message to multiple topics """
    new_prod2 = Producer(
        ["Cool-New-Topic", "Sick-Topic"],
        orch,
        dc_endpoint,
    )
    mlops_msg2 = MLOpsMessage(
        MessagType.COMPLETE,
        creator="training",
        user_message="Hello Pipeline",
        output="/dataset/data",
    )
    new_prod2.send(mlops_msg2)


if __name__ == "__main__":
    test_kafka_producer()