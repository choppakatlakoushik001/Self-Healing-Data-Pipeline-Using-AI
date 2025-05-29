Write-Host "Starting Docker Kafka consumer..."
Write-Host "This will display all messages on the 'fastapi-topic' topic from inside the Docker network"
Write-Host "Press Ctrl+C to stop"

# Copy the consumer script to the container
docker cp docker_consumer.py fastapi:/tmp/

# Run the consumer inside the container
docker exec -it fastapi python /tmp/docker_consumer.py 