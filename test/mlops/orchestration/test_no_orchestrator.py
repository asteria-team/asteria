"""
Test everything in orchestration without an
orchestrator to ensure failures for user
do not occur.
"""

import mlops.orchestration as orc
import mlops.messaging as mes


def test_without_orchestration():
    # producer
    new_prod = orc.Producer(orchestrator="None", topic="New Topic")
    mlops_msg = mes.MLOPS_Message(mes.Message_Type.INPROGRESS.name)
    new_prod.send(mlops_msg)
    new_prod.send(mlops_msg, flush=False)
    assert not new_prod.flush()
    new_prod.close()

    # consumer
    new_con = orc.Consumer(topic_subscribe="New Topic")
    new_con.subscribe("Another Topic")
    assert len(new_con.get_subscriptions()) == 2
    msgs = new_con.get_messages()
    assert msgs is None
    new_con.unsubscribe()
    assert new_con.get_subscriptions() is None
    new_con.close()
