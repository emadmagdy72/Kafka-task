import cv2
import numpy as np
from PIL import Image
from confluent_kafka import Consumer
from confluent_kafka import Producer, KafkaError, KafkaException
import sys
import requests
import random
import sqlite3
import sys

MAIN_DB = "main.db"

me = "emadmagdy72"

conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
        'group.id': "new_group_2",
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)



def msg_process(msg):
    RELOAD_URL = 'http://127.0.0.1:5000/reload'
    try:
        response = requests.get(RELOAD_URL)
        if response.status_code == 200:
            print("Server reload triggered successfully")
        else:
            print(f"Failed to reload server: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending reload request: {e}")




running = True


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


basic_consume_loop(consumer, [me])
