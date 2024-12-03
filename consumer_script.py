from confluent_kafka import Consumer, KafkaError
import psycopg2
import json
from datetime import datetime

# Configure Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',  # Use 'kafka:9092' if in Docker
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['order_topic'])

# PostgreSQL Database Configuration
try:
    conn = psycopg2.connect(
        host="localhost",  # Change to PostgreSQL host if needed
        database="my_database",
        user="my_user",
        password="my_password"
    )
    cursor = conn.cursor()
except psycopg2.OperationalError as e:
    print(f"Database connection error: {e}")
    exit(1)

print("Starting consumer...")

try:
    while True:
        # Poll for messages
        msg = consumer.poll(1.0)  # `msg` is defined here
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue  # End of partition event
            else:
                print(f"Consumer error: {msg.error()}")
                break

        # Process the message
        try:
            data = json.loads(msg.value().decode('utf-8'))  # Decode and load JSON
            print(f"Received message: {data}")

            # Insert into PostgreSQL
            cursor.execute(
                """
                INSERT INTO orders (order_id, customer_name, amount, timestamp)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING;
                """,
                (
                    data.get('id', 'default_id'),  # Ensure 'id' exists in message
                    data.get('name', 'default_name'),  # Ensure 'name' exists in message
                    data.get('quantity', 0),  # Default quantity
                    data.get('timestamp', datetime.now())  # Use current timestamp if missing
                )
            )
            conn.commit()
            consumer.commit()


            # Commit Kafka offset
            consumer.commit()

        except (json.JSONDecodeError, psycopg2.Error) as e:
            print(f"Error processing message or inserting into database: {e}")
            conn.rollback()

except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    consumer.close()
    cursor.close()
    conn.close()
