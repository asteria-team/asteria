"""
Test producer in context of kafka orchestration docker-compose
"""

import mlops.messaging as mes
import mlops.orchestration as orc

orchestrator = "kafka"
dc_endpoint = "kafka:9092"


def test_kafka_producer():
    """Test sending messages to kafka"""
    orch = orc.Orchestration(orchestrator, dc_endpoint)
    new_prod = orc.Producer(orch, "Cool-New-Topic")

    test_msg = "Hello World"
    msg_builder = mes.MessageBuilder()
    mlops_msg = (
        msg_builder.with_msg_type(mes.MessageType.COMPLETE)
        .with_creator_type("ETL")
        .with_creator("ETL")
        .with_user_msg(test_msg)
        .with_next_task("Train")
        .with_output("/dataset/data")
        .build()
    )

    new_prod.send(mlops_msg)
    new_prod.send(mlops_msg, True)
    new_prod.close()

    """ Test sending a message to multiple topics with auto discovery """
    new_prod2 = orc.Producer(topic=["Cool-New-Topic", "Sick-Topic"])
    msg_builder2 = mes.MessageBuilder()
    mlops_msg2 = (
        msg_builder2.with_msg_type(mes.MessageType.INPROGRESS)
        .with_creator("training")
        .with_user_msg("Hello Pipeline")
        .with_output("/dataset/data")
        .build()
    )
    new_prod2.send(mlops_msg2)


if __name__ == "__main__":
    test_kafka_producer()
