Write-Host "Running PyFlink Kafka Job..."

# Run the PyFlink job
Write-Host "Starting PyFlink Kafka job..."
docker exec -it flink-jobmanager python /opt/flink/pyflink_kafka_job.py

Write-Host "Open the Flink Dashboard at http://localhost:8081 to monitor the job" 