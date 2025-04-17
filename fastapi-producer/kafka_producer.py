from kafka import KafkaProducer
from fastapi import HTTPException
from produce_schema import ProduceMessage
import json

KAFKA_BROKER_URL = 'localhost:9092'
KAFKA_TOPIC = 'fastapi-topic'
PRODUCER_CLIENT_ID = 'fastapi_producer'

def serializer(message):
    return json.dumps(message).encode() #default utf-8

producer = KafkaProducer(
    api_version = (0,8,0),
    bootstrap_servers = KAFKA_BROKER_URL,
    value_serializer = serializer,
    client_id = PRODUCER_CLIENT_ID
)

def produce_kafka_message(messageRequest : ProduceMessage):
    try:
        producer.send(KAFKA_TOPIC, json.dumps({'message': messageRequest.message}))
    except Exception as error:
        print(error)
        raise HTTPException(status_code=500, detail="Failed to send message")    