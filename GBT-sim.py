import json
import time
from datetime import datetime
import csv
import os
import uuid
from confluent_kafka import Producer
from confluent_kafka import Consumer
import psycopg2
from dotenv import load_dotenv
load_dotenv()  # loads .env from current working dir

database = os.environ["POSTGRES_DB"]
user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_URL"]
port = os.environ["POSTGRES_PORT"]

producer_topic = "GBT_data"  # NOTE The topic to which the messages will be sent, rename accordingly to whatever topic you want to send the DDM payloads to.
producer_config = {
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVER"],
    "message.max.bytes": 8388608,
    "client.id": "GBT-producer"
}

consumer_topic = "user_input"  # NOTE Might want to change name
consumer_config = {
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVER"],
    "fetch.max.bytes": 8388608,
    "session.timeout.ms": 45000,
    "client.id": "GBT-consumer",
    "group.id": "GBT-consumer-group",
    "auto.offset.reset": "earliest",
}

# connecting to the team's render database:
conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
cursor = conn.cursor()


def latency_calc(event_time):
    # calculates the latency of the message from the time it was sent to the time it was received
    # returns latency in milliseconds

    event_time = datetime.strptime(event_time, "%Y-%m-%d %H:%M:%S.%f")
    current_time = datetime.now()
    latency = current_time - event_time
    latency_ms = latency.total_seconds() * 1000
    return latency_ms


def generate_initial_payload():
    object_id = '30104'
    target = 'Moretus'
    tx_waveform = 'W48'
    rec_waveform = 'W48'
    event_time = datetime.now()
    latency_ms = latency_calc(event_time)
    return object_id, target, tx_waveform, rec_waveform, event_time, latency_ms


def generate_payload():
    object_id = '30104'
    target = 'Moretus'  # TODO maybe don't hardcode these??

    #retrieves the most recent entry from the database and returns the GBT data
    cursor.execute("""
                   SELECT selected_waveform, event_time
                   FROM "ngRadar_Website_uievent"
                   ORDER BY event_time DESC
                   LIMIT 1;
                   """)
    tx_waveform, event_time = cursor.fetchone()
    rec_waveform = tx_waveform
    latency_ms = latency_calc(event_time)

    return object_id, target, tx_waveform, rec_waveform, event_time, latency_ms


def publish_to_db(object_id, target, tx_waveform, rec_waveform, event_time, latency_ms):
    # saves the DDM payload to the database, commits it, and closes the DB connection. 
    # using placeholder values for the required column fields - will update with data from the actual message metadata
    # TODO check if this is what table name gets created once we migrate
    cursor.execute("""
                   INSERT INTO \"ngRadar_Website_gbtevent\" (
                   object_id, 
                   target, 
                   tx_waveform,
                   rec_waveform,
                   event_time, 
                   latency_ms)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   """, (
                   object_id, 
                   target, 
                   tx_waveform,
                   rec_waveform,
                   event_time, 
                   latency_ms))
    conn.commit()

    return print("DDM payload saved to database successfully.")


def produce(topic, config, key, value):
    # creates a new producer instance
    producer = Producer(config)

    # producing a message to the specified topic 
    producer.produce(topic, key=key, value=value)
    print(f"Produced message to topic {topic} with key {key}.")

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()


def consume(topic, config):
    # creates a new consumer instance
    consumer = Consumer(config)

    #subscribes to the specified topic
    consumer.subscribe(topic)

    try:
        while True:
            # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0) # polls for messages for 1 second
            # if msg is not None and msg.error() is None:
            if msg is None:
                continue
            if msg.error() is not None:
                print("Consumer error:", msg.error())
                continue

            key = msg.key().decode("utf-8")
            value = msg.value().decode("utf-8")

            # fill in the values to be published to the db
            object_id, target, tx_waveform, rec_waveform, event_time, latency_ms = generate_payload()

            # publish new transmission to the db
            publish_to_db(object_id, target, tx_waveform, rec_waveform, event_time, latency_ms)

            # produce this new message, lets DSOC know to produce image(s)
            produce(producer_topic, producer_config, "GBT transmitting", value)
           
            # print(f"Received message from {value['Source']} for object {value['Object']} (Object ID: {value['Object_ID']}).")
            # if value['Tx_WF'] != "Tx_OFF":
            #     print(f"Observing with waveform {value['Tx_WF']}.")
            # else:
            #     print(f"Transmitter is currently OFF.")

    except KeyboardInterrupt: 
        pass
    finally:
        # closes the consumer connection
        consumer.close()


def main():
    # generate a dummy data payload, publish this data to the db, produce a message with this payload, then start consuming
    object_id, target, tx_waveform, rec_waveform, event_time, latency_ms = generate_initial_payload()
    publish_to_db(object_id, target, tx_waveform, rec_waveform, event_time, latency_ms)
    key, value = "GBT transmitting", tx_waveform
    produce(producer_topic, producer_config, key, value)
    consume(consumer_topic, consumer_config)


main()