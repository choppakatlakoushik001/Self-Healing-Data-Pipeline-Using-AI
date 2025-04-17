from fastapi import FastAPI
import asyncio
from kafka import KafkaConsumer
import json

# Constants Section
KAFKA_BROKER_URL = 'localhost:9092'
KAFKA_TOPIC = 'fastapi-topic'
KAFKA_CONSUMER_ID = 'fastapi-consumer'

stop_polling_event = asyncio.Event()
app = FastAPI()

def json_deserializer(value):
    if value is None:
        return
    try:
        return json.loads(value.decode('utf-8'))
    except:
        print("Unable to decode")
        return None

def create_kafka_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER_URL],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_ID,
        value_deserializer=json_deserializer
    )
    return consumer

async def poll_consumer(consumer: KafkaConsumer):
    try:
        while not stop_polling_event.is_set():
            print("Trying to Poll again")
            records = consumer.poll(5000,250)
            if records:
                for record in records.values():
                    for message in record:
                        m = json.loads(message.value).get("message")
                        print(f"Received the message {m} from the topic of {message.topic}")
            await asyncio.sleep(5)
    except Exception as e:
        print(f"Errors available {e}")
    finally:
        print("Closing the consumer")
        consumer.close()

tasklist = []

@app.get("/trigger")
async def trigger_polling():
    if not tasklist:
        stop_polling_event.clear()  # reset flag
        consumer = create_kafka_consumer()
        task = asyncio.create_task(poll_consumer(consumer=consumer))
        tasklist.append(task)

        return {"status": "Kafka polling has started"}
    return {"status": "Kafka polling was already triggered"}

@app.get("/stop-trigger")
async def stop_trigger():
    stop_polling_event.set()
    if tasklist:
        tasklist.pop()

    return {"status": "Kafka polling stopped"}
