from kafka import KafkaConsumer
import json
from database_connect import  get_db_conn, insert_record

# Define the Kafka topic and broker address
KAFKA_BROKER = 'localhost:9092'  # Adjust if your broker is on a different host
TOPIC_NAME = 'testnov10'  # Replace with your Kafka topic name

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    # Deserialize messages from JSON format (adjust if your data is in a different format)
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',  # Start reading from the earliest available message
    enable_auto_commit=True,       # Automatically commit offsets
    group_id='my_consumer_group'   # Specify a consumer group ID
)

print(f"Listening for messages on topic: {TOPIC_NAME}")

def insert_data(conn, data_dict):
    id = data_dict['id']
    modified_time = data_dict['modified_time']
    data_dict = json.dumps(data_dict).replace("'", "''")
    query = f"insert into new_users(unique_id, ip_address, json_record, modified_time) values({id}, 'localhost',  '{data_dict}', '{modified_time}')"
    print(f"query: {query}")
    insert_record(conn, query)

conn = None
try:
    # Continuously consume messages
    conn = get_db_conn()


    cnt = 3
    for message in consumer:
        try:
            # cnt += 1
            print(f"Received message: Topic={message.topic}, Partition={message.partition}, "
                  f"Offset={message.offset}, Key={message.key}, Value={message.value}")
            # if len((message.value).keys()) < 4:
            #     print(f"Not all columns are present. Invalid Record: {message.value}")
            #     continue
            # cnt = message.value['id']
            # name = message.value['name']
            # modified_time = message.value['modified_time']
            # status =  message.value['status']
            data_dict = message.value
            insert_data(conn, data_dict)


        except Exception as e:
            print(f"Exception: {e}")

except KeyboardInterrupt:
    print("Consumer stopped by user.")
finally:
    # Close the consumer when done
    consumer.close()
    print("Kafka consumer closed.")
    if conn:
        conn.close()