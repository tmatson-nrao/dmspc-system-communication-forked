import hashlib
import os
import time
from datetime import datetime, timedelta
from PIL import Image
from confluent_kafka import Consumer
import psycopg2
from dotenv import load_dotenv
import random
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import io

"""
This code will:
- consume a message from the GBT
- create an image file
- load the image data + the uuid into the DB
"""

load_dotenv()  # loads .env from current working dir

database = os.environ["POSTGRES_DB"]
user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_URL"]
port = os.environ["POSTGRES_PORT"]

config = {
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVER"],
    "fetch.max.bytes": 8388608,
    "session.timeout.ms": 45000,
    "client.id": "universal-consumer",
    "group.id": "consumer-group",
    "auto.offset.reset": "earliest",
  }


#Connecting to the team's render database (fill in password):
conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
cursor = conn.cursor()

topic = ["GBT_data"]  #consumes from the GBT's topic

''' We aren't currently calculating latency at this step
We have to decide how to implement latency in addition to having a simulated delay

def latency_calc(event_time):
  #calculates the latency of the message from the time it was sent to the time it was received
  #returns latency in milliseconds

  event_time = datetime.strptime(event_time, "%Y-%m-%d %H:%M:%S.%f")
  current_time = datetime.now()
  latency = current_time - event_time
  latency_ms = latency.total_seconds() * 1000
  return latency_ms
'''

''' We don't need to import anything from the GBT database! Leaving this here for now though

def DB_import():
    #retrieves the most recent entry from the database and returns the GBT data
    cursor.execute("""
        SELECT uuid, object_id, target, tx_waveform, rec_waveform, event_time, latency_ms
        FROM "ngRadar_Website_gbtEvent"
        ORDER BY event_time DESC
        LIMIT 1;
    """)

    uuid, object_id, target, tx_waveform, rec_waveform, event_time, latency_ms = cursor.fetchone()
    return uuid, object_id, target, tx_waveform, rec_waveform, event_time, latency_ms
'''
    
def DB_columns():
  #defines the column values specific to DSOC/images

    product_type = "DDM"
    product_id = f"DDM{random.randint(1000,9999)}"
    station = random.choice(["SC", "HN", "NL", "FD", "LA", "PT", "KP", "OV", "BR", "MK"])
    created_at = datetime.now() - timedelta(seconds=10)
    xmit_station = "GBT"
    rcvr_station = station

    return product_type, product_id, station,created_at, xmit_station, rcvr_station

def publish_DB(uuid, product_type, product_id, station, created_at, xmit_station, rcvr_station, image_file, num_bytes):
  #saves the DDM payload to the database and commits it 
  cursor.execute("""
                 INSERT INTO \"ngRadar_Website_dsocEvent\" (
                 uuid,
                 product_type, 
                 product_id, 
                 station, 
                 created_at, 
                 xmit_station, 
                 rcvr_station,
                 image_file, 
                 num_bytes)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                 """, (
                   uuid,
                   product_type, 
                   product_id, 
                   station, 
                   created_at, 
                   xmit_station, 
                   rcvr_station, 
                   image_file, 
                   num_bytes))
  conn.commit()
  #conn.close()

  return print("DDM payload saved to database successfully.")


def consume(topic, config):
  #creates a new consumer instance
  consumer = Consumer(config)

  #subscribes to the specified topic
  consumer.subscribe(topic)

  try:
    while True:
      #consumer polls the topic and prints any incoming messages
        msg = consumer.poll(1.0) #polls for messages for 1 second
      #if msg is not None and msg.error() is None:
        if msg is None:
            continue
        if msg.error() is not None:
            print("Consumer error:", msg.error())
            continue

        #decode the GBT payload that is a single string of just the uuid:
        uuid = msg.value().decode("utf-8")

        #generate a random image payload to simulate the DSOC's DDM product: 
        matplotlib.use('Agg')  # Use a non-interactive backend for matplotlib
        
        #generating random data and formatting the graph:
        x_data = np.random.uniform(-30, 30, 20)
        y_data = np.random.uniform(-300, 300, 20)

        plt.scatter(x_data, y_data)
        plt.axhline(0, color='black', linewidth=0.5)
        plt.axvline(0, color='black', linewidth=0.5)
        plt.title(f"DDM for {target}")
        plt.xlabel("Doppler Freq (Hz)")
        plt.ylabel("Range (km)")
        plt.grid(True)

        #saving the bytes to a buffer instead of a file
        byte_buffer = io.BytesIO()
        plt.savefig(byte_buffer, format='png')
        byte_buffer.seek(0)

        image_file = byte_buffer.getvalue()

        plt.close()  # Close the plot to free memory
        
        num_bytes = len(image_file)

        #create the rest of the column values specific to DSOC/images:
        product_type, product_id, station,created_at, xmit_station, rcvr_station = DB_columns()

        publish_DB(uuid, product_type, product_id, station, created_at, xmit_station, rcvr_station, image_file, num_bytes)
        
        print(f"Received message from {station}; DDM is ready (Image ID: {product_id}).")
        #unique = hashlib.sha256(str(value['Image']).encode('utf-8')).hexdigest()
        #filename = f"{value['Type']}-{value['Image_ID']}-{value['Timestamp']}-{unique:.15}.png"
        #print(f"Image saved as {filename}")


  except KeyboardInterrupt: 
    pass
  finally:
    #closes the consumer connection
    consumer.close()


def main():
  consume(topic, config)


main()