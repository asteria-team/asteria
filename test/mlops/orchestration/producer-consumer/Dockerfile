FROM python:latest

#to COPY the remote file at working directory in container
COPY test/mlops/orchestration/producer-consumer/requirements.txt .
COPY test/mlops/orchestration/producer-consumer/create_producer.py .
COPY test/mlops/orchestration/producer-consumer/create_consumer.py .
COPY test/mlops/orchestration/producer-consumer/start_test.sh .
COPY src/mlops/ ./mlops/

RUN pip install -r requirements.txt

RUN apt-get update && apt-get install -y iputils-ping
RUN apt-get install -y netcat

CMD ["./start_test.sh"]
