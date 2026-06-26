import hashlib
import json
import os
import csv
import time
from PIL import Image
from confluent_kafka import Consumer
import psycopg2
from dotenv import load_dotenv

load_dotenv()  # loads .env from current working dir

database = os.environ["POSTGRES_DB"]
user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_URL"]
port = os.environ["POSTGRES_PORT"]

#Connecting to the team's render database (fill in password):
conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
cursor = conn.cursor()

topic = ["GBT_data", "DSOC_data"]  #NOTE The topic which the messages will be received from, rename accordingly to whatever topic you are using
'''
stats_file = "ddm_stats.csv"


def append_stats(num_bytes, latency_ms,filename=None):
  """Append DDM payload statistics for later analysis."""
  
  fieldnames = ["filename", "num_bytes", "latency_ms"]
  file_exists = os.path.exists(stats_file)
  with open(stats_file, "a", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=fieldnames)
    if not file_exists:
      writer.writeheader()
    writer.writerow({
      "filename": filename or "",
      "num_bytes": num_bytes,
      "latency_ms": f"{latency_ms:.2f}"
    })
'''

def read_config():
  #reads the client (consumer) configuration from consumer.properties
  #and returns it as a key-value map
  config = {}
  with open("consumer.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def DB_columns(value):
  #dissects the payload to get individual values, and publishes to the correct column in the database
  #some values only exist for messages with images - these have if statements
  obs_id = value['Object_ID']#done
  target = value['Object']#done
  if value['Type'] is not None:
    product_type = value['Type']#done
  else:
    product_type = None
  if value['Image_ID'] is not None:
    product_id = value['Image_ID']#done
  else:
    product_id = None
  station = value['Source']#done
  creation_time = value['Timestamp']#done
  event_time = "2026-06-25 17:00:00+00"
  created_at = "2026-06-25 17:00:00+00"
  xmit_station = 'GBT'
  if value['Receiver'] is not None:
    rcvr_station = value['Receiver']#done
  else:
    rcvr_station = None
  if value['Image'] is not None:
    image_file = value['Image']#done
  else:
    image_file = None
  if value['Bytes'] is not None:
    num_bytes = value['Bytes']#done
  else:
    num_bytes = None

  return obs_id, target, product_type, product_id, station, creation_time, event_time, created_at, xmit_station, rcvr_station, image_file, num_bytes

def publish_DB(obs_id, target, product_type, product_id, station, creation_time, event_time, created_at, xmit_station, rcvr_station, image_file, num_bytes, latency_ms):
  #saves the DDM payload to the database, commits it, and closes the DB connection. 
  #using placeholder values for the required column fields - will update with data from the actual message metadata
  cursor.execute("""
                 INSERT INTO \"ngRadar_Website_observatoryevent\" (
                 obs_id, 
                 target, 
                 product_type, 
                 product_id, 
                 station, 
                 creation_time, 
                 event_time, 
                 created_at, 
                 xmit_station, 
                 rcvr_station, 
                 image_file, 
                 num_bytes, 
                 latency_ms) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                 """, (
                   obs_id, 
                   target, 
                   product_type, 
                   product_id, 
                   station, 
                   creation_time, 
                   event_time, 
                   created_at, 
                   xmit_station, 
                   rcvr_station, 
                   image_file, 
                   num_bytes, 
                   latency_ms))
  conn.commit()
  conn.close()

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

        key = msg.key().decode("utf-8")
        value = json.loads(msg.value().decode("utf-8"))
        obs_id, target, product_type, product_id, station, creation_time, event_time, created_at, xmit_station, rcvr_station, image_file, num_bytes = DB_columns(value)

        publish_DB(obs_id, target, product_type, product_id, station, creation_time, event_time, created_at, xmit_station, rcvr_station, image_file, num_bytes, latency_ms=None)

        if value['Source'] == "GBT":
          print(f"Received message from {value['Source']} for Object {value['Object']} (Object ID: {value['Object_ID']}). Observing with waveform {value['Transmitted_WF']}.")


        elif value['Source'] == "DSOC":
          print(f"Received message from {value['Source']} for Object {value['Object']} (Object ID: {value['Object_ID']}). Checking if quick-look product is ready...")
          if value['Image']:
            print(f"{value['Type']} image ready (Image ID: {value['Image_ID']}). Produced by {value['Receiver']}") # add more robust rcvr identification using enums later
            if value['Type'] == "DDM":
                unique = hashlib.sha256(str(value['Image']).encode('utf-8')).hexdigest()
                filename = f"{value['Type']}-{value['Image_ID']}-{value['Timestamp']}-{unique:.15}.png"
                print(f"Image saved as {filename}")
                #save image first
                #img = Image.open(filename)
                #img.show()
            elif value['Type'] == "Spec":
                unique = hashlib.sha256(str(value['Image']).encode('utf-8')).hexdigest()
                filename = f"{value['Type']}-{value['Image_ID']}-{value['Timestamp']}-{unique:.15}.png"
                print(f"Image saved as {filename}")
                #save image first
                #img = Image.open(filename)
                #img.show()
            else:
                print("Image is of an unknown type. Expecting 'Spec' or 'DDM'.")
          else:
            print(f"Quick-look product not available yet for Object {value['Object']} (Object ID: {value['Object_ID']}).")
        else:
          print("Message received from a site other than GBT or VLBA, or message is empty.")

      #NOTE: NOT ACTUALLY SAVING IMAGES YET
      #NOTE: ADD LATENCY CALCS
      
        #latency_ms = (time.time() - send_time)*1000 #calculate latency in ms

    
        #append_stats(
          #num_bytes=len(value),
          #latency_ms=latency_ms,
          #filename=filename)

        #open and display the consumed image
        #img = Image.open(filename)
        #img.show()

  except KeyboardInterrupt: 
    pass
  finally:
    #closes the consumer connection
    consumer.close()


def main():
  config = read_config()
  print(config)
  consume(topic, config)


main()

