from confluent_kafka import Consumer
import json
import time
import string

def main():
    # Configure consumer specifically for Docker environment
    conf = {
        'bootstrap.servers': 'broker:29092',  # Internal Docker network address
        'group.id': 'enhanced-flink-consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    # Create consumer
    consumer = Consumer(conf)
    
    # Subscribe to topic
    topic = 'fastapi-topic'
    consumer.subscribe([topic])
    
    print(f"Enhanced Flink consumer listening for messages on topic: {topic}")
    print("This consumer performs several transformations on the data:")
    print("1. Word counting - counts occurrences of each word in messages")
    print("2. Message statistics - calculates length statistics of messages")
    print("Press Ctrl+C to exit")
    
    # Maintain word counts
    word_counts = {}
    message_lengths = []
    message_count = 0
    
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
                # Decode the message
                value = msg.value().decode('utf-8')
                print(f"\n====== New Message Received ======")
                print(f"Topic: {msg.topic()}")
                print(f"Partition: {msg.partition()}")
                print(f"Offset: {msg.offset()}")
                print(f"Value: {value}")
                
                # Try to parse as JSON
                try:
                    data = json.loads(value)
                    if isinstance(data, dict) and 'message' in data:
                        message_text = data['message']
                        message_count += 1
                        
                        # Transformation 1: Word count analysis
                        words = message_text.lower().split()
                        for word in words:
                            # Strip punctuation
                            word = word.strip(string.punctuation)
                            if word:
                                if word in word_counts:
                                    word_counts[word] += 1
                                else:
                                    word_counts[word] = 1
                        
                        # Transformation 2: Message statistics
                        message_length = len(message_text)
                        message_lengths.append(message_length)
                        
                        # Print the transformations
                        print("\n----- Word Count Analysis -----")
                        print(f"Words in this message: {len(words)}")
                        print("Word counts (cumulative):")
                        for word, count in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                            print(f"  {word}: {count}")
                        
                        print("\n----- Message Statistics -----")
                        avg_length = sum(message_lengths) / len(message_lengths)
                        print(f"Message length: {message_length} characters")
                        print(f"Average message length: {avg_length:.1f} characters")
                        print(f"Total messages processed: {message_count}")
                        
                        if len(message_lengths) > 1:
                            min_length = min(message_lengths)
                            max_length = max(message_lengths)
                            print(f"Min message length: {min_length} characters")
                            print(f"Max message length: {max_length} characters")
                
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