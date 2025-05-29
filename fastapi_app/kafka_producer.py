from kafka import KafkaProducer
from fastapi import HTTPException
from produce_schema import ProduceMessage
import json
import os

# Use "broker:29092" for Docker container communication (internal)
# Or fallback to localhost:9092 for local development
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL', 'broker:29092')
KAFKA_TOPIC = 'fastapi-topic'
PRODUCER_CLIENT_ID = 'fastapi_producer'

def serializer(message):
    return json.dumps(message).encode() #default utf-8

producer = KafkaProducer(
    bootstrap_servers = KAFKA_BROKER_URL,
    value_serializer = serializer,
    client_id = PRODUCER_CLIENT_ID
)

def produce_kafka_message(messageRequest : ProduceMessage):
    try:
        producer.send(KAFKA_TOPIC, {'message': messageRequest.message})
        producer.flush()  # Ensure messages are sent before returning
    except Exception as error:
        print(error)
        raise HTTPException(status_code=500, detail="Failed to send message")