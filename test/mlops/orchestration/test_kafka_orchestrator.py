"""
Test underlying kafka orchestration in a docker-compose
format. In this case, kafka has listeners externally exposed
"""

import logging

import pytest

pytest_plugings = ["docker_compose"]


@pytest.fixture()
def start_docker_compose(function_scoped_container_getter):
    """Start up the kafka docker-compose"""
    service = function_scoped_container_getter.get("kafka").network_info[0]
    kafka_url = "%s:%s" % (service.hostname, service.host_port)
    logging.info(f"Resulting kafka url: {kafka_url}")
    return kafka_url


def test_run_compose(start_docker_compose):
    endpoint = start_docker_compose
    logging.info(f"Kafka endpoint for connection: {endpoint}")
    assert True
