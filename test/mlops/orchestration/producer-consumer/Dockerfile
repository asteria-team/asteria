FROM python:latest

#to COPY the remote file at working directory in container
COPY requirements.txt .
COPY create_producer.py .
COPY create_consumer.py .
COPY start_test.sh .
COPY mlops/ ./mlops/

RUN pip install -r requirements.txt

RUN apt-get update && apt-get install -y iputils-ping
RUN apt-get install -y netcat

CMD ["./start_test.sh"]
