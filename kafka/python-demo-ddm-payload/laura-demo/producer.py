from confluent_kafka import Producer
import time
import json

topic = "DDM_png"  # NOTE The topic to which the messages will be sent, rename accordingly

def read_config():
  # reads the client (producer) configuration from producer.properties
  # and returns it as a key-value map
  config = {}
  with open("producer.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config


def produce(topic, config, key, value, headers):
  # creates a new producer instance
  producer = Producer(config)

  # produces a sample message
  producer.produce(
    topic=topic, 
    key=key, 
    value=value,
    headers=headers)
  
  print(f"Produced image to topic {topic}")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()


def main():
  config = read_config()

  with open('DDM1.png', 'rb') as file:
    image_bytes = file.read()

  produce(
    topic, 
    config, 
    "image2", 
    image_bytes,
    headers={
      "send_time": str(time.time())# sends a timestamp of the time the message was sent
    })


main()
