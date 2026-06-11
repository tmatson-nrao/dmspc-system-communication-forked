from confluent_kafka import Producer


topic = "DDM-payload"  # NOTE The topic to which the messages will be sent, rename accordingly to whatever topic you want to send the DDM payloads to.


with open('DDM.png', 'rb') as file: # reading in the DDM payload as bytes from a file. You can replace this with however you are getting the DDM payload, just make sure it's in bytes format.
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
print(f"Size of DDM payload: {len(value_bytes)} bytes.") # check size before sending

def produce(topic, config, value):
  # creates a new producer instance
  producer = Producer(config)

  # producing a message to the specified topic with the DDM payload as the value. The key is set to None since we are not using it in this example, but you could set it to something if you wanted to. Just make sure to encode it as bytes like we do with the value.
  producer.produce(topic, key=None, value=value) 
  print(f"Produced payload to topic {topic}.")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()


def main():
  config = read_config()

  produce(topic, config, value_bytes)


main()
