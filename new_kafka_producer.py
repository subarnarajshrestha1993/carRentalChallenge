from kafka import KafkaProducer
import json
import time
from datetime import datetime

# Kafka broker address
KAFKA_BROKER = 'localhost:9092'
# Kafka topic to send messages to
KAFKA_TOPIC = 'testnov10'

def create_kafka_producer():
    """
    Creates and returns a KafkaProducer instance.
    Configures value serialization for JSON messages.
    """
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def send_message(producer, topic, message):
    """
    Sends a message to the specified Kafka topic.
    """
    try:
        future = producer.send(topic, message)
        record_metadata = future.get(timeout=10) # Block until message is sent
        print(f"Message sent successfully to topic: {record_metadata.topic}, "
              f"partition: {record_metadata.partition}, "
              f"offset: {record_metadata.offset}")
    except Exception as e:
        print(f"Error sending message: {e}")

if __name__ == "__main__":
    producer = create_kafka_producer()

    # Example messages
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    messages = [
        {"id": 75, "name": "Hello Kafka from Python!", "status":'Inactive' ,"modified_time": time_now , 'Record':
         {"id": 76, "name": "Hello Kafka from Python3.0!", "status":'Inactive' ,"modified_time": time_now} },
        {"id": 88, "name": "This is a second message.","modified_time":time_now},
        {"id": 90, "name": "Sending JSON data to Kafka.","modified_time":time_now},
        {"id": 91, "name": "Sending JSON data to Kafka.","status":'active', "modified_time": time_now}
    ]

    for msg in messages:
        send_message(producer, KAFKA_TOPIC, msg)
        time.sleep(1) # Simulate some delay between messages

    # Ensure all messages are sent before closing the producer
    producer.flush()
    producer.close()
    print("Producer closed.")