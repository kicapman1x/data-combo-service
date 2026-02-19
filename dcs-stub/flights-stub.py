import os
import csv
import json
import random
import time
from datetime import datetime, timedelta
import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

def bootstrap():
    #Environment variables
    global payload_dir, ca_cert, n_sampled, logdir, loglvl, kafka_url, cert_file, key_file, PRODUCE_TOPIC_NAME, kafka_producer_conn
    payload_dir = os.getenv("PAYLOAD_DIR")
    kafka_url = os.environ.get("KAFKA_HOST")
    ca_cert = os.environ.get("CA_PATH")
    cert_file = os.environ.get("CERT_PATH")
    key_file = os.environ.get("KEY_PATH")
    PRODUCE_TOPIC_NAME = os.environ.get("KAFKA_FLIGHT_TOPIC", "rts_flights_topic")
    logdir = os.environ.get("log_directory", ".")
    loglvl = os.environ.get("log_level", "INFO").upper()
    n_sampled = int(os.environ.get("sampled_flights_no"))
    kafka_producer_conn = get_kafka_producer()

    #Logging setup
    log_level = getattr(logging, loglvl, logging.INFO)
    logging.basicConfig(
        filename=f'{logdir}/flights_stub.log',
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def get_kafka_producer():
    conf = {
        'bootstrap.servers': kafka_url,
        'security.protocol': 'SSL',
        'ssl.ca.location': ca_cert,
        "ssl.certificate.location": cert_file,
        "ssl.key.location": key_file
    }
    return Producer(conf)


def load_csv():
    with open(f"{payload_dir}/flights.csv", newline="", encoding="utf-8") as csvfile:
        return list(csv.DictReader(csvfile))


def stub_flights():
    logger.info("**********Starting flights stubs**********")
    logger.info(f"Loading payloads from {payload_dir}/flights.csv")
    rows = load_csv()
    logger.info(f"Loaded {len(rows)} rows")
  
    logger.info(f"Selecting {n_sampled} random rows")
    sampled_flights = random.sample(rows, n_sampled)
    count = 1
    for r in sampled_flights:
        dt = datetime.now()
        dt += timedelta(
            hours=random.randint(1, 2),
            minutes=random.randint(0, 59)
        )
        stubbed_departure_date = dt.strftime("%Y-%m-%d %H:%M")
        r["departure_date"] = stubbed_departure_date
        stubbed_flight_id = count
        r["flight_id"] = stubbed_flight_id
        logger.info(
            f"Normalized batch to departure_date={stubbed_departure_date}, "
        )
        count += 1
        publish_to_kafka(r)

def publish_to_kafka(message):
    body = json.dumps(message).encode("utf-8")
    try:
        kafka_producer_conn.produce(
            topic=PRODUCE_TOPIC_NAME,
            value=body
        )
        kafka_producer_conn.flush()
        logger.info(f"Published message to Kafka topic '{PRODUCE_TOPIC_NAME}': {message}")
    except Exception as e:
        logger.error(f"Failed to publish message to Kafka: {e}")

if __name__ == "__main__":
    bootstrap()
    stub_flights()

