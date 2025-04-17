from fastapi import FastAPI, BackgroundTasks
from kafka.admin import KafkaAdminClient, NewTopic
from kafka_producer import produce_kafka_message
from contextlib import asynccontextmanager
from produce_schema import ProduceMessage

# Constants Section
KAFKA_BROKER_URL = 'localhost:9092'
KAFKA_TOPIC = 'fastapi-topic'
KAFKA_ADMIN_CLIENT = 'fastapi-admin-client'

@asynccontextmanager
async def lifespan(app: FastAPI):

    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER_URL,
        client_id=KAFKA_ADMIN_CLIENT
    )

    if KAFKA_TOPIC not in admin_client.list_topics():
        admin_client.create_topics(
            new_topics=[
                NewTopic(
                    name=KAFKA_TOPIC,
                    num_partitions=1,
                    replication_factor=1
                )
            ],
            validate_only=False
        )
    yield #seperation point
app = FastAPI(lifespan=lifespan)   

@app.post("/produce/message/", tags=["Produce Message"])
async def produce_message(messageRequest: ProduceMessage, background_tasks: BackgroundTasks):
    background_tasks.add_task(produce_kafka_message, messageRequest)
    return {"message": "Message Received, thank you for sending message"}

