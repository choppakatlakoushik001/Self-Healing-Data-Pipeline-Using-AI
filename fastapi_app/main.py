from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from kafka.admin import KafkaAdminClient, NewTopic
from kafka_producer import produce_kafka_message, KAFKA_BROKER_URL
from produce_schema import ProduceMessage
from custom_swagger import add_custom_swagger_ui
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Use the same KAFKA_BROKER_URL from kafka_producer
KAFKA_TOPIC = 'fastapi-topic'
KAFKA_ADMIN_CLIENT = 'fastapi-admin-client'

# Create FastAPI app with explicit SwaggerUI configuration
app = FastAPI(
    title="Kafka Message API",
    description="API for sending messages to Kafka topics",
    version="1.0.0",
    openapi_url="/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add custom Swagger UI
add_custom_swagger_ui(app)

@app.get("/", tags=["Root"])
def read_root():
    """Root endpoint to verify API is working."""
    return {"message": "Kafka Message API is running!"}

@app.get("/health", tags=["Health"])
def health_check():
    """Health check endpoint."""
    return {
        "status": "ok",
        "kafka_broker": KAFKA_BROKER_URL,
        "topic": KAFKA_TOPIC
    }

@app.post("/produce/message/", tags=["Produce Message"])
async def produce_message(messageRequest: ProduceMessage, background_tasks: BackgroundTasks):
    """Send a message to the Kafka topic."""
    logger.info(f"Received message request: {messageRequest.message}")
    background_tasks.add_task(produce_kafka_message, messageRequest)
    return {"message": "Message Received, thank you for sending message"}

@app.get("/create-topic", tags=["Admin"])
def create_kafka_topic():
    """Create a Kafka topic if it doesn't exist."""
    try:
        logger.info(f"Attempting to create/verify topic: {KAFKA_TOPIC}")
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER_URL,
            client_id=KAFKA_ADMIN_CLIENT
        )
        existing_topics = admin_client.list_topics()
        logger.info(f"Existing topics: {existing_topics}")

        if KAFKA_TOPIC not in existing_topics:
            admin_client.create_topics(
                new_topics=[NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)],
                validate_only=False
            )
            logger.info(f"Topic '{KAFKA_TOPIC}' created successfully")
            return {"message": f"Topic '{KAFKA_TOPIC}' created."}
        else:
            logger.info(f"Topic '{KAFKA_TOPIC}' already exists")
            return {"message": f"Topic '{KAFKA_TOPIC}' already exists."}
    except Exception as e:
        logger.error(f"Error creating topic: {str(e)}")
        return {"error": f"Failed to create topic: {str(e)}"}
    finally:
        if 'admin_client' in locals():
            admin_client.close()
