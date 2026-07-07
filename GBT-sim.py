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
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
import django
django.setup()
from ngRadar_Website.models.models import uiEvent
from ngRadar_Website.models.models import gbtEvent
from dotenv import load_dotenv
load_dotenv()

# payload that will be inserted in the gbtEvent db table
payload = {
    "object_id": None, 
    "target": None, 
    "tx_waveform": None, 
    "rec_waveform": None, 
    "event_time": None, 
    "latency_ms": None,
}

producer_topic = "GBT_data"  # NOTE The topic to which the messages will be sent, rename accordingly to whatever topic you want to send the DDM payloads to.
producer_config = {
    "bootstrap.servers": "4.tcp.ngrok.io:23446",
    "message.max.bytes": 8388608,
    "client.id": "GBT-producer"
}

consumer_topic = "user_input"  # NOTE Might want to change name
consumer_config = {
    "bootstrap.servers": "4.tcp.ngrok.io:23446",
    "fetch.max.bytes": 8388608,
    "session.timeout.ms": 45000,
    "client.id": "GBT-consumer",
    "group.id": "GBT-consumer-group",
    "auto.offset.reset": "earliest",
}


def set_payload_dict(waveform):
    payload["object_id"] = '30104'
    payload["target"] = 'Moretus'
    payload["tx_waveform"] = waveform
    payload["rec_waveform"] = waveform
    payload["event_time"] = datetime.now()
    payload["latency_ms"] = latency_calc(payload["event_time"])


def latency_calc(event_time):
    # calculates the latency of the message from the time it was sent to the time it was received
    # returns latency in milliseconds
    #event_time = event_time
    current_time = datetime.now()
    latency = current_time - event_time
    latency_ms = latency.total_seconds() * 1000
    return latency_ms


def generate_payload(ui_event_uuid):
    # TODO test if this works
    ui_event = uiEvent.objects.get(uuid=ui_event_uuid)

    set_payload_dict(ui_event.selected_waveform)


def publish_to_db():
    # TODO test if this works
    gbt_event = gbtEvent.objects.create(**payload)

    return gbt_event.uuid


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
            ui_uuid = msg.value().decode("utf-8")  # this is the uuid of the ui_event

            # fill in the values to be published to the db
            generate_payload(ui_uuid)

            # publish new transmission to the db
            gbt_uuid = publish_to_db()

            key, value = "GBT transmitting", gbt_uuid

            # produce this new message, lets DSOC know to produce image(s)
            produce(producer_topic, producer_config, key, value)

    except KeyboardInterrupt: 
        pass
    finally:
        # closes the consumer connection
        consumer.close()


def main():
    # generate a dummy data payload, publish this data to the db, produce a message with this payload, then start consuming
    # object_id, target, tx_waveform, rec_waveform, event_time, latency_ms = generate_initial_payload()
    # publish_to_db(object_id, target, tx_waveform, rec_waveform, event_time, latency_ms)
    set_payload_dict('W48')
    gbt_uuid = publish_to_db()
    key, value = "GBT transmitting", json.dumps(f"{gbt_uuid}").encode("utf-8")
    produce(producer_topic, producer_config, key, value)
    consume(consumer_topic, consumer_config)


main()