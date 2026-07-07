import json
import time
from datetime import datetime
import csv
import os
import uuid
from confluent_kafka import Producer
from confluent_kafka import Consumer
from dotenv import load_dotenv, find_dotenv



path = find_dotenv()  # searches upward for a .env
print("find_dotenv():", path)

import os
print("CWD:", os.getcwd())

print("Before load:", os.environ.get("BOOTSTRAP_SERVER"))
load_dotenv(override=True)  # force .env to overwrite existing env var
print("After load:", os.environ.get("BOOTSTRAP_SERVER"))

topic = "user_input"
config = {
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVER"],
    "fetch.max.bytes": 8388608,
    "session.timeout.ms": 45000,
    "client.id": "GBT-consumer",
    "group.id": "GBT-consumer-group",
    "auto.offset.reset": "earliest",
}
print("BOOTSTRAP:", config["bootstrap.servers"])

def consume(topic,config):
    consumer = Consumer(config)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error() is not None:
                print("Consumer error:", msg.error())
                continue

            key = msg.key().decode("utf-8")
            value = msg.value().decode("utf-8")
            print(f"Received message: {value} with key: {key}")
    
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()    
def main():
    consume(topic, config)

main()
