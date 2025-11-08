from kafka import KafkaConsumer
import json
from database_connect import  get_db_conn, insert_record

# Define the Kafka topic and broker address
KAFKA_BROKER = 'localhost:9092'  # Adjust if your broker is on a different host
TOPIC_NAME = 'test'  # Replace with your Kafka topic name

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    # Deserialize messages from JSON format (adjust if your data is in a different format)
    value_deserializer=lambda x: (x.decode('utf-8')),
    auto_offset_reset='earliest',  # Start reading from the earliest available message
    enable_auto_commit=True,       # Automatically commit offsets
    group_id='my_consumer_group'   # Specify a consumer group ID
)

print(f"Listening for messages on topic: {TOPIC_NAME}")
conn = None
try:
    # Continuously consume messages
    conn = get_db_conn()


    cnt = 3
    for message in consumer:
        cnt += 1
        print(f"Received message: Topic={message.topic}, Partition={message.partition}, "
              f"Offset={message.offset}, Key={message.key}, Value={message.value}")
        query = f"insert into users(id, name, status) values({cnt}, '{message.value}', 'Active')"
        insert_record(conn, query)
except KeyboardInterrupt:
    print("Consumer stopped by user.")
finally:
    # Close the consumer when done
    consumer.close()
    print("Kafka consumer closed.")
    if conn:
        conn.close()