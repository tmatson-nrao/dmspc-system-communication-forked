import json

from confluent_kafka import Consumer

def read_config():
  # reads the client configuration from client.properties
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
  config["group.id"] = "python-group-2"
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
        value = json.loads(msg.value().decode("utf-8")) # loading the message back into a dictionary so we can access the individual fields

        
        print(f"Event: {value['productType']} for target '{value['target']}' now available. Created at {value['creationTime']} by {value['productSource']}.") 

  except KeyboardInterrupt:
    pass
  finally:
    # closes the consumer connection
    consumer.close()

def main():
  config = read_config()
  topic = "2nd_topic"

  #produce(topic, config)
  consume(topic, config)


main()


