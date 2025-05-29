# Activate virtual environment if exists
if (Test-Path .\venv\Scripts\Activate) {
    Write-Host "Activating virtual environment..."
    .\venv\Scripts\Activate
}

# Install required package if necessary
pip install confluent-kafka

# Run the test consumer
Write-Host "Starting Kafka test consumer..."
Write-Host "This will display all messages on the 'fastapi-topic' topic"
Write-Host "Press Ctrl+C to stop"
python test_consumer.py 