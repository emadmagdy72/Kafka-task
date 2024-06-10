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
import uuid
from PIL import Image, ImageDraw, ImageFont
import sqlite3
import os
MAIN_DB = "main.db"

me = "emadmagdy72"

conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
        'group.id': "new_group_2",
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

producer_topic = "emadmagdy72"
producer_conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
    'client.id': producer_topic}
producer = Producer(producer_conf)




def load_yolo():
    net = cv2.dnn.readNet("yolov3.weights", "yolov3.cfg")
    with open("coco.names", "r") as f:
        classes = [line.strip() for line in f.readlines()]
    return net, classes


def detect_image_objects(image):
    net, classes = load_yolo()

    # Convert PIL image to OpenCV format
    open_cv_image = np.array(image.convert("RGB"))
    open_cv_image = open_cv_image[:, :, ::-1].copy()

    height, width, _ = open_cv_image.shape

    # Create a 4D blob from the image
    blob = cv2.dnn.blobFromImage(open_cv_image, 0.00392, (416, 416), (0, 0, 0), True, crop=False)

    # Set the input to the network
    net.setInput(blob)

    # Run forward pass to get output of the output layers
    outs = net.forward(net.getUnconnectedOutLayersNames())

    # Get the class with the highest confidence
    class_id = np.argmax(outs[0][0][5:])

    # Get the class label
    object_name = classes[class_id]

    return object_name

def get_db_connection():
    conn = sqlite3.connect(MAIN_DB)
    conn.row_factory = sqlite3.Row
    return conn


def get_image_name(id):
    con = get_db_connection()
    cur = con.cursor()

    # Retrieve the filename just inserted
    cur.execute("SELECT filename FROM image WHERE id = ?", (id,))
    row = cur.fetchone()
    con.close()

    if row:
        return row['filename']
    else:
        return None
def detect_object(id):
    input_image_path=get_image_name(id)
    folder_path = r"D:\ITI 9-Month\Kafka\Server\images"
    file_path = os.path.join(folder_path, input_image_path)
    print("file_path",file_path)
    image = Image.open(file_path)
    object_name = detect_image_objects(image)
    print("Object detected in the image:", object_name)
    return object_name


def msg_process(msg):
    object_name = detect_object(msg.value().decode())
    requests.put('http://127.0.0.1:5000/object/' + msg.value().decode(),
                 json={"object": object_name})
    print("message processed successfully", object_name)
    producer.produce(producer_topic, key="random_string", value=msg.value().decode())
    producer.flush()
    print("image id produced to second consumer", object_name)




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
