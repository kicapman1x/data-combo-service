import json
import ssl
import pika
import os
import time
import mysql.connector
import uuid
import logging
import requests
from datetime import datetime
from confluent_kafka import Consumer
import sys

def bootstrap():
    #Environment variables
    global rmq_url, rmq_port, rmq_username, rmq_password, ca_cert, mysql_url, mysql_port, mysql_user, mysql_password, CONSUME_TOPIC_NAME, logdir, loglvl, mysql_db_s1, logger, kafka_url, cert_file, key_file, CONSUME_TOPIC_NAME_FLT, CONSUME_TOPIC_NAME_PSG, PRODUCE_QUEUE_NAME_FLT, PRODUCE_QUEUE_NAME_PSG, rmq_poll_interval, kafka_consumer_group
    rmq_url = os.environ.get("RMQ_HOST")
    rmq_port = int(os.environ.get("RMQ_PORT"))
    rmq_username = os.environ.get("RMQ_USER")
    rmq_password = os.environ.get("RMQ_PW")
    rmq_poll_interval = float(os.environ.get("RMQ_POLL_INT"))
    kafka_consumer_group = os.environ.get("KAFKA_RTS_PF_CG_ID")
    kafka_url = os.environ.get("KAFKA_HOST")
    ca_cert = os.environ.get("CA_PATH")
    cert_file = os.environ.get("CERT_PATH")
    key_file = os.environ.get("KEY_PATH")
    CONSUME_TOPIC_NAME_FLT = os.environ.get("KAFKA_FLIGHT_TOPIC", "rts_flights_topic")
    CONSUME_TOPIC_NAME_PSG = os.environ.get("KAFKA_PASSENGER_TOPIC", "rts_passengers_topic")
    PRODUCE_QUEUE_NAME_FLT = os.environ.get("RMQ_FLIGHT_QUEUE", "rts_flights_queue")
    PRODUCE_QUEUE_NAME_PSG = os.environ.get("RMQ_PASSENGER_QUEUE", "rts_passengers_queue")
    logdir = os.environ.get("log_directory", ".")
    loglvl = os.environ.get("log_level", "INFO").upper()

    #Logging setup
    log_level = getattr(logging, loglvl, logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(f'{logdir}/rts-pf.log')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

def get_kafka_consumer():
    conf = {
        'bootstrap.servers': kafka_url,
        'security.protocol': 'SSL',
        'ssl.ca.location': ca_cert,
        "ssl.certificate.location": cert_file,
        "ssl.key.location": key_file,
        'group.id': kafka_consumer_group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.poll.interval.ms': 300000
    }
    return Consumer(conf)

def get_rmq_connection():
    credentials = pika.PlainCredentials(
        rmq_username,
        rmq_password
    )
    ssl_context = ssl.create_default_context(cafile=ca_cert)
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED

    ssl_options = pika.SSLOptions(
        context=ssl_context,
        server_hostname=rmq_url
    )
    params = pika.ConnectionParameters(
        host=rmq_url,
        port=rmq_port,
        credentials=credentials,
        ssl_options=ssl_options,
        heartbeat=60,
        blocked_connection_timeout=30
    )
    return pika.BlockingConnection(params)

def handle_flight_topic(channel, msg):
    try:
        logger.info("Received message for kafka data lake flight topic")
        channel.queue_declare(queue=PRODUCE_QUEUE_NAME_FLT, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=PRODUCE_QUEUE_NAME_FLT,
            body=msg,
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        raise

def handle_passenger_topic(channel, msg):
    try:
        logger.info("Received message for kafka data lake passenger topic")
        channel.queue_declare(queue=PRODUCE_QUEUE_NAME_PSG, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=PRODUCE_QUEUE_NAME_PSG,
            body=msg,
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        raise

if __name__ == "__main__":
    bootstrap()
    logger.info("Starting SSL Kafka consumer...")
    global consumer
    connection = get_rmq_connection()
    channel = connection.channel()
    consumer = get_kafka_consumer()
    consumer.subscribe([CONSUME_TOPIC_NAME_PSG,CONSUME_TOPIC_NAME_FLT])
    logger.info(f"subscribed to kafka topics: {CONSUME_TOPIC_NAME_FLT} and {CONSUME_TOPIC_NAME_PSG}")
    while True:
      msg = consumer.poll(rmq_poll_interval)      
      if msg is None:
          logger.warning("No messages detected")
          continue
      elif msg.error():
          logger.error("Consumer error:", msg.error())
          continue
      elif msg.topic() == CONSUME_TOPIC_NAME_FLT:
          handle_flight_topic(channel, msg.value().decode("utf-8"))
          logger.info(f"Consuming from {CONSUME_TOPIC_NAME_FLT}")
          consumer.commit(msg)
      elif msg.topic() == CONSUME_TOPIC_NAME_PSG:
          logger.info(f"Consuming from {CONSUME_TOPIC_NAME_PSG}")
          handle_passenger_topic(channel, msg.value().decode("utf-8"))
          consumer.commit(msg)
      else:
          logger.warning("Internal error")

         
