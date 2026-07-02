import random
import json
from datetime import datetime
import time
import csv
import os
from confluent_kafka import Producer
from dotenv import load_dotenv
load_dotenv()  # loads .env from current working dir

topic = "DSOC_data"  #NOTE The topic to which the messages will be sent, rename accordingly to whatever topic you want to send the DDM payloads to.
config = {
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVER"],
    "message.max.bytes": 8388608,
    "client.id": "VLBA-producer"
  }

def produce(topic, config, key, value):
  #creates a new producer instance
  producer = Producer(config)

  #producing a message to the specified topic 
  producer.produce(topic, key=key, value=value) 
  print(f"Produced message to topic {topic} with key {key}.")

  #send any outstanding or buffered messages to the Kafka broker
  producer.flush()

def main():
    produce(topic, config, key, value)
    time.sleep(10)

with open("mock_assets/DSOC-data.csv", newline="") as f:
    reader = csv.DictReader(f, delimiter=" ")  #uses header row as keys
    for row in reader:
        if row["Image"]:
            with open(f"mock_assets/{row['Image']}", 'rb') as file :
                image = file.read()
                print(f"Image {row['Image']} has {len(image)} bytes.")
        else: None
    
        #build JSON payload from the row
        payload = {
            "Object": row["Object"],
            "Object_ID": row["Object_ID"],
            "Source": row["Source"],
            "Timestamp": f"{datetime.now()}",
            "Type": row["Type"],
            "Bytes": f"{len(image)}" if row["Image"] else None,
            "Image": f"{image}" if row["Image"] else None, 
            "Image_ID": f"{random.randint(100000, 999999)}"
        }
        #print(payload) sanity check

        key = str(payload["Object_ID"]) if payload.get("Object_ID") else None
        value = json.dumps(payload).encode("utf-8")

        main()
