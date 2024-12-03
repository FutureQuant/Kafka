# kafka_producer.py

from confluent_kafka import Producer
import json
import time

# Kafka configuration
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_order(topic, order):
    producer.produce(topic, key=order['id'], value=json.dumps(order), callback=delivery_report)
    producer.flush()

if __name__ == "__main__":
    while True:
        order = {
            "id": "unique_id",
            "name": "customer_name",
            "quantity": 5
        }
        produce_order('order_topic', order)
        time.sleep(1)
