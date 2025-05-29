from confluent_kafka import Consumer
import json
import time

def main():
    """
    Main function that sets up and runs a Kafka consumer for the fastapi-topic.
    Displays received messages in a formatted way.
    """
    # Configure consumer
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'kafka-consumer-group',
        'auto.offset.reset': 'earliest'  # Start from the beginning if no offset is found
    }
    
    # Create consumer
    consumer = Consumer(conf)
    
    # Subscribe to topic
    topic = 'fastapi-topic'
    consumer.subscribe([topic])
    
    print(f"Listening for messages on topic: {topic}")
    print("Press Ctrl+C to exit")
    
    try:
        while True:
            # Poll for message
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
                
            # Process message
            try:
                # Decode and print the message
                value = msg.value().decode('utf-8')
                print(f"\n====== New Message Received ======")
                print(f"Topic: {msg.topic()}")
                print(f"Partition: {msg.partition()}")
                print(f"Offset: {msg.offset()}")
                print(f"Value: {value}")
                
                # Try to parse as JSON
                try:
                    data = json.loads(value)
                    if isinstance(data, dict):
                        print("\nMessage contents:")
                        for key, val in data.items():
                            print(f"  {key}: {val}")
                except json.JSONDecodeError:
                    print("Message is not valid JSON")
                
                print("==================================\n")
                
            except Exception as e:
                print(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        # Clean up
        consumer.close()
        print("Consumer has been closed.")

if __name__ == "__main__":
    main() 