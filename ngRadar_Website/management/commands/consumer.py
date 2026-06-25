import hashlib
import json
import os
import csv
import time
from PIL import Image
from confluent_kafka import Consumer


topic = "GBT_data"  #NOTE The topic which the messages will be received from, rename accordingly to whatever topic you are using
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


def consume(topic, config):
  #creates a new consumer instance
  consumer = Consumer(config)

  #subscribes to the specified topic
  consumer.subscribe([topic])

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

        if value['Source'] == "GBT":
          print(f"Received message from {value['Source']} for Object {value['Object']} (Object ID: {value['Object_ID']}). Observing with waveform {value['Transmitted_WF']}.")


        elif value['Source'] == "DSOC":
          print(f"Received message from {value['Source']} for Object {value['Object']} (Object ID: {value['Object_ID']}). Checking if quick-look product is ready...")
          if value['Image']:
            print(f"{value['Type']} image ready (Image ID: {value['Image_ID']}). Produced by {value['Receiver']}") # add more robust rcvr identification using enums later
            if {value['Type']} == "DDM":
                unique = hashlib.sha256(value).hexdigest()
                filename = f"{value['Type']}-{value['Image_ID']}-{value['Timestamp']}-{unique:.15}.png"
            elif {value['Type']} == "Spec":
                unique = hashlib.sha256(value).hexdigest()
                filename = f"{value['Type']}-{value['Image_ID']}-{value['Timestamp']}-{unique:.15}.png"
            else:
              print("Image is of an unknown type. Expecting 'Spec' or 'DDM'.")
          else:
            print(f"Quick-look product not available yet for Object {value['Object']} (Object ID: {value['Object_ID']}).")
        else:
          print("Message received from a site other than GBT or VLBA, or message is empty.")

      

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