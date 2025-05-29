Write-Host "Starting Kafka console consumer..."
Write-Host "This will display all messages on the 'fastapi-topic' topic"
Write-Host "Press Ctrl+C to stop"
docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 --topic fastapi-topic --from-beginning 