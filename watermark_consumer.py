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

me = "emadmagdy72"

conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
        'group.id': "new_group",
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

MAIN_DB = "main.db"

producer_topic = "emadmagdy72"
producer_conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
                 'client.id': producer_topic}
producer = Producer(producer_conf)


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


from PIL import Image, ImageDraw, ImageFont


def add_watermark(input_image_path, watermark_text):
    try:
        # Open an image file
        image = Image.open(input_image_path).convert("RGBA")

        # Make the image editable
        txt = Image.new('RGBA', image.size, (255, 255, 255, 0))

        # Choose a font and size
        font_size = 100  # You can adjust this value for larger or smaller text
        font = ImageFont.truetype("arial.ttf", font_size)  # Make sure you have arial.ttf or another .ttf file available

        # Initialize ImageDraw
        draw = ImageDraw.Draw(txt)

        # Get the image size
        width, height = image.size

        # Draw the text multiple times across the whole image
        for x in range(0, width, int(font_size * 1.5)):
            for y in range(0, height, int(font_size * 1.5)):
                draw.text((x, y), watermark_text, font=font, fill=(255, 255, 255, 128))

        # Combine the image with the watermark
        watermarked = Image.alpha_composite(image, txt)

        # Save the image
        watermarked = watermarked.convert("RGB")  # Remove alpha for saving in jpg format.
        watermarked.save(input_image_path)

        print(f"Watermark added to {input_image_path}")

    except Exception as e:
        print(f"Error adding watermark: {e}")


def msg_process(msg):
    folder_path = r"D:\ITI 9-Month\Kafka\Server\images"
    input_image_path = get_image_name(msg.value().decode())
    file_path = os.path.join(folder_path, input_image_path)
    add_watermark(file_path, "Water_mark")
    print("Water_mark processed successfully")
    producer.produce(producer_topic, key="random_string", value=msg.value().decode())
    producer.flush()


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
