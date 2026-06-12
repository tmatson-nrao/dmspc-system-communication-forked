import hashlib
import json
import time
from PIL import Image
from confluent_kafka import Consumer


topic = "DDM_Payload"   # NOTE The topic which the messages will be received from, rename accordingly to whatever topic you are using
consumer_group = "gbt-group"  # NOTE rename to whatever consumer group name you want to use


def read_config():
  # reads the client (consumer) configuration from consumer.properties
  # and returns it as a key-value map
  config = {}
  with open("consumer.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config


def consume(topic, config):
  # sets the consumer group ID and offset
  config["group.id"] = consumer_group
  config["auto.offset.reset"] = "earliest"

  # creates a new consumer instance
  consumer = Consumer(config)

  # subscribes to the specified topic
  consumer.subscribe([topic])

  try:
    while True:
      # consumer polls the topic and prints any incoming messages
      msg = consumer.poll(1.0) # polls for messages for 1 second
      if msg is not None and msg.error() is None:
        key = msg.key().decode("utf-8")
        value = msg.value()
        headers = dict(msg.headers() or []) # 
        meta = json.loads(headers.get("DDM metadata", b"{}").decode("utf-8")) # loading the DDM metadata from the message headers, which is stored as a JSON string. We decode it from bytes to a string and then load it into a dictionary so we can access the individual fields.

        digest = hashlib.sha256(value).hexdigest()
        filename = f"received-DDM-{digest:.15}.png" # creating a unique filename for the received DDM payload using a random UUID. You can change this to whatever naming convention you want.
        
        with open(filename, 'wb') as file: # writing the received DDM payload to a file in bytes format.
          file.write(value)

        send_time = float(meta.get('message_timestamp')) # extract timestamp from header

        latency_ms = (time.time() - send_time)*1000 #calculate latency in ms

        print(f"Saved DDM for obs_id '{meta.get('obs_id')}' as {filename} ({len(value)} bytes). Image created at {meta.get('created_timestamp')} by {meta.get('source')}. Latency: {latency_ms:.2f} ms") 
        
        # open and displaythe consumed image
        img = Image.open(filename)
        img.show()

  except KeyboardInterrupt: 
    pass
  finally:
    # closes the consumer connection
    consumer.close()


def main():
  config = read_config()

  consume(topic, config)


main()
