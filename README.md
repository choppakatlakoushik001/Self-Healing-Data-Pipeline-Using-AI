# Kafka and Flink Integration

This project demonstrates a complete data pipeline using FastAPI, Kafka, and a Python-based consumer for message processing.

## Project Structure

- `fastapi_app/` - FastAPI application that sends messages to Kafka
- `flink/` - Consumer scripts for processing Kafka messages
- `docker-compose.yml` - Docker Compose file for running the infrastructure (Kafka, Zookeeper, Flink)

## Getting Started

1. Start the infrastructure:

```bash
docker-compose up -d
```

2. Access the FastAPI Swagger UI at: http://localhost:8000/custom-docs

3. Access the Flink Dashboard at: http://localhost:8081 (for monitoring)

## Running the Kafka Message Flow

### 1. Start the Kafka Consumer

You can run the consumer in two ways:

#### A. Run inside Docker (recommended)
```bash
cd flink
.\run_docker_consumer.ps1
```

This will start a consumer inside the Docker network that displays messages sent to the 'fastapi-topic' Kafka topic.

#### B. Run locally on your host
```bash
cd flink
python test_consumer.py
```

### 2. Send Messages via FastAPI

- Open http://localhost:8000/custom-docs in your browser
- Use the POST /produce/message/ endpoint to send test messages
- Example message: `{"message": "Hello from Kafka!"}`

### 3. View Messages in the Consumer

The messages you send will appear in the console where the consumer is running, showing details such as:
- The message content
- Kafka topic and partition
- Message offset

## Running Flink Jobs

There are two ways to process messages with Flink:

### 1. Using the Console Consumer (Simple)

The simplest approach is to use the Kafka console consumer to view messages:

```bash
docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 --topic fastapi-topic --from-beginning
```

### 2. Using the Flink Dashboard (Advanced)

For more advanced processing, you can upload a Flink JAR job through the dashboard:

1. Access the Flink Dashboard at http://localhost:8081
2. Go to "Submit New Job" and upload your JAR file
3. Configure and start the job

## Troubleshooting

If you encounter issues:

1. Make sure all Docker containers are running:

```bash
docker-compose ps
```

2. Check the logs of the Kafka broker:

```bash
docker logs broker
```

3. Try restarting the containers:

```bash
docker-compose restart
```

4. If consumers can't connect to Kafka, verify you're using the correct broker addresses:
   - Use `broker:29092` when connecting from inside Docker containers
   - Use `localhost:9092` when connecting from your host machine 