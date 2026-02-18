import datetime
import json
import sys
import ssl
import uuid
import pika
import os
import logging
import threading
from confluent_kafka import Producer
import pymqi
import json
import requests
import time

def bootstrap():
    #Environment variables
    global rmq_url, rmq_port, rmq_username, rmq_password, ca_cert, secret_key, logdir, loglvl, logger, cert_file, key_file, CONSUME_QUEUE_NAME_SCHED_PF, kafka_url, kafka_producer_conn, ibmmq_host, ibmmq_port, ibmmq_user, ibmmq_password, ibmmq_queue_manager, ibmmq_channel, ibmmq_key_repo, ibmmq_producer_conn, ibmmq_queue, pf_be_host,scheduler_interval
    cert_file = os.environ.get("CERT_PATH")
    key_file = os.environ.get("KEY_PATH")
    rmq_url = os.environ.get("RMQ_HOST")
    kafka_url = os.environ.get("KAFKA_HOST")
    rmq_port = int(os.environ.get("RMQ_PORT"))
    rmq_username = os.environ.get("RMQ_USER")
    rmq_password = os.environ.get("RMQ_PW")
    ibmmq_host = os.environ.get("IBM_MQ_HOST")
    ibmmq_port = int(os.environ.get("IBM_MQ_PORT"))
    ibmmq_user = os.environ.get("IBM_MQ_USER")
    ibmmq_password = os.environ.get("IBM_MQ_PW")
    ibmmq_queue_manager = os.environ.get("IBM_MQ_QUEUE_MANAGER")
    ibmmq_channel = os.environ.get("IBM_MQ_CHANNEL")
    ibmmq_key_repo = os.environ.get("IBM_MQ_KEY_REPO")
    ca_cert = os.environ.get("CA_PATH")
    secret_key = os.environ.get("HMAC_KEY").encode("utf-8")
    CONSUME_QUEUE_NAME_SCHED_PF = os.environ.get("RMQ_SCHED_PF_QUEUE", "sched_pf_queue")
    logdir = os.environ.get("log_directory", ".")
    loglvl = os.environ.get("log_level", "INFO").upper()
    kafka_producer_conn = get_kafka_producer()
    ibmmq_producer_conn = connect_ibm_mq()
    ibmmq_queue = pymqi.Queue(ibmmq_producer_conn, "PF.TOKEN.QUEUE")
    pf_be_host = os.environ.get("PF_BE_HOST", "pf-be.han.gg")
    scheduler_interval = int(os.environ.get("SCHEDULER_INTERVAL", "600"))

    #logging
    log_level = getattr(logging, loglvl, logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(f'{logdir}/token-generator.log')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

def connect_ibm_mq():
    queue_manager = ibmmq_queue_manager
    channel = ibmmq_channel
    conn_info = f"{ibmmq_host}({ibmmq_port})"

    cd = pymqi.CD()
    cd.ChannelName = channel
    cd.ConnectionName = conn_info
    cd.ChannelType = pymqi.CMQC.MQCHT_CLNTCONN
    cd.TransportType = pymqi.CMQC.MQXPT_TCP
    sco = pymqi.SCO()
    sco.KeyRepository = ibmmq_key_repo

    qmgr = pymqi.QueueManager(None)

    qmgr.connect_with_options(
        queue_manager,
        user=ibmmq_user,
        password=ibmmq_password,
        cd=cd,
        sco=sco
    )
    return qmgr

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

def get_kafka_producer():
    conf = {
        'bootstrap.servers': kafka_url,
        'security.protocol': 'SSL',
        'ssl.ca.location': ca_cert,
        "ssl.certificate.location": cert_file,
        "ssl.key.location": key_file
    }
    return Producer(conf)

def rmq_listener():
    logger.info("**********Starting passenger-flights service**********")

    logger.info("Starting SSL RabbitMQ consumer...")
    global connection, channel
    connection = get_rmq_connection()
    channel = connection.channel()

    logger.info(f"Declaring queues {CONSUME_QUEUE_NAME_SCHED_PF}")
    channel.queue_declare(queue=CONSUME_QUEUE_NAME_SCHED_PF, durable=True)
    channel.basic_qos(prefetch_count=1)

    logger.info(f"Consuming messages from {CONSUME_QUEUE_NAME_SCHED_PF}")

    channel.basic_consume(
        queue=CONSUME_QUEUE_NAME_SCHED_PF,
        on_message_callback=process_message_passenger,
        auto_ack=False
    )
    try:
        logger.info("Waiting for messages. Ctrl+C to exit.")
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        channel.stop_consuming()
        connection.close()

def process_message_passenger(channel, method, properties, body):
    try:
        message = json.loads(body)
        logger.info(f"Received message: {message}")
        token = generate_pf_token(message)
        publish_to_kafka(token)
        publish_to_ibm_mq(token)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )

def publish_to_kafka(message):
    body = json.dumps(message).encode("utf-8")
    try:
        kafka_producer_conn.produce(
            topic="pf_data_lake",
            value=body
        )
        kafka_producer_conn.flush()
        logger.info(f"Published message to Kafka topic 'pf_data_lake': {message}")
    except Exception as e:
        logger.error(f"Failed to publish message to Kafka: {e}")

def publish_to_ibm_mq(message):
    try:
        msg_str = json.dumps(message)
        ibmmq_queue.put(msg_str.encode("utf-8"))
        logger.info(f"Published message to IBM MQ queue 'PF.TOKEN.QUEUE': {message}")
    except Exception as e:
        logger.error(f"Failed to publish message to IBM MQ: {e}")


def generate_pf_token(message):
    logger.info(f"Generating token for message: {message}")
    token = {
        "token_id": str(uuid.uuid4()),
        "event_type": "pf_token_generated",
        "generated_at": datetime.utcnow().isoformat(),
        "data": {
            "passenger_id": message.get("passenger_id"),
            "passenger_name": message.get("passenger_name"),
            "passenger_age": message.get("passenger_age"),
            "passenger_nationality": message.get("passenger_nationality"),
            "flight": {
                "flight_id": message.get("flight_id"),
                "airline": message.get("airline"),
                "flight_code": message.get("flight_code"),
                "source_city": message.get("source_city"),
                "destination_city": message.get("destination_city"),
                "departure_date": message.get("departure_date")
            }
        }
    }
    logger.info(f"Generated token: {token}")
    return token

def schedule_pf():
    while True:
        try:
            response = requests.post(f"https://{pf_be_host}/schedule-pf-query", timeout=10, verify=ca_cert, cert=(cert_file, key_file))
            print("Endpoint called:", response.status_code)
        except Exception as e:
            print("HTTP error:", e)
        time.sleep(scheduler_interval)

if __name__ == "__main__":
    bootstrap()
    t1 = threading.Thread(target=rmq_listener, daemon=True)
    t2 = threading.Thread(target=schedule_pf, daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()