from confluent_kafka import Producer


topic = "DDM-Payload"  # NOTE The topic to which the messages will be sent, rename accordingly


with open('DDM-payload.png', 'rb') as file: # reading in the DDM payload as bytes from a file. You can replace this with however you are getting the DDM payload, just make sure it's in bytes format.
  DDM = file.read()


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


value_bytes = DDM # the value of the message is the DDM payload, which is read in as bytes from the file. We don't need to encode it as we did with the JSON string in the previous demo because it's already in bytes format.


def produce(topic, config, value):
  # creates a new producer instance
  producer = Producer(config)

  # produces a sample message
  producer.produce(topic, key=None, value=value) # We are not using a key for this message, but you could if you wanted to guarantee ordering by partition or for any other reason. Just make sure to encode it as bytes like we do with the value.
  print(f"Produced message to topic {topic}.")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()


def main():
  config = read_config()

  produce(topic, config, value_bytes)


main()
