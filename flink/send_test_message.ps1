Write-Host "Sending test message to Kafka..."

Invoke-RestMethod -Method Post -Uri "http://localhost:8000/produce/message/" -ContentType "application/json" -Body '{"message": "Test message for PyFlink job"}'

Write-Host "Message sent. Check the Flink job output." 