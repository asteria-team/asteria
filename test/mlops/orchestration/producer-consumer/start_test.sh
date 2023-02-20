#!/bin/bash
while ! nc -z kafka 9092; do sleep 3; done
python create_producer.py
sleep 3
python create_consumer.py