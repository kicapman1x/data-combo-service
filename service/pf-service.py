import json
import sys
import ssl
import pika
import os
import mysql.connector
import logging
import threading
from flask import Flask, request, jsonify, abort

app = Flask(__name__)

def bootstrap():
    #Environment variables
    global rmq_url, rmq_port, rmq_username, rmq_password, ca_cert, mysql_url, mysql_port, mysql_user, mysql_password, mysql_db, CONSUME_QUEUE_NAME, PRODUCE_QUEUE_NAME, logdir, loglvl, logger, CONSUME_QUEUE_NAME_FLT, CONSUME_QUEUE_NAME_PSG, cert_file, key_file, PRODUCE_QUEUE_NAME_SCHED_PF, pf_be_host
    cert_file = os.environ.get("CERT_PATH")
    key_file = os.environ.get("KEY_PATH")
    rmq_url = os.environ.get("RMQ_HOST")
    rmq_port = int(os.environ.get("RMQ_PORT"))
    rmq_username = os.environ.get("RMQ_USER")
    rmq_password = os.environ.get("RMQ_PW")
    ca_cert = os.environ.get("CA_PATH")
    mysql_url = os.environ.get("MYSQL_HOST")
    mysql_port = int(os.environ.get("MYSQL_PORT"))
    mysql_user = os.environ.get("MYSQL_USER")
    mysql_password = os.environ.get("MYSQL_PW")
    mysql_db = os.environ.get("MYSQL_DB")
    CONSUME_QUEUE_NAME_FLT = os.environ.get("RMQ_FLIGHT_QUEUE", "rts_flights_queue")
    CONSUME_QUEUE_NAME_PSG = os.environ.get("RMQ_PASSENGER_QUEUE", "rts_passengers_queue")
    PRODUCE_QUEUE_NAME_SCHED_PF = os.environ.get("RMQ_SCHED_PF_QUEUE", "sched_pf_queue")
    logdir = os.environ.get("log_directory", ".")
    loglvl = os.environ.get("log_level", "INFO").upper()
    pf_be_host = os.environ.get("PF_BE_HOST", "pf-be.han.gg")

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

    file_handler = logging.FileHandler(f'{logdir}/pf-svc.log')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

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

def get_mysql_connection():
    return mysql.connector.connect(
        host=mysql_url,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db,

        ssl_ca=ca_cert,
        ssl_verify_cert=True,
        ssl_verify_identity=True,

        autocommit=False
    )

def process_message_flight(channel, method, properties, body):
    try:
        message = json.loads(body)
        logger.info(f"Received message: {message}")
        process_flight(message)

        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )

def process_flight(message):
    logger.debug(f"Processing flight: {message}")
    f_id = message["flight_id"]
    airline = message["airline"]
    f_code = message["flight"]
    src_city = message["source_city"]
    dst_city = message["destination_city"]
    d_date = message["departure_date"]

    conn = get_mysql_connection()
    try:
        if flight_exists(conn, f_id):
            return None
        else:
            logger.info(f"Inserting new flight: {f_id}")
            insert_flight(conn, f_id, airline, f_code, src_city, dst_city, d_date)
            conn.commit()
    except mysql.connector.errors.IntegrityError:
        conn.rollback()
        return None

def flight_exists(conn, flight_id):
    logger.debug(f"Checking if flight exists: {flight_id}")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM flights WHERE flight_id = %s LIMIT 1",
        (flight_id,)
    )
    return cursor.fetchone() is not None

def insert_flight(conn, flight_id, airline, flight_code, source_city, destination_city, departure_date):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO flights (
            flight_id,
            airline,
            flight_code,
            source_city,
            destination_city,
            departure_date
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            flight_id,
            airline,
            flight_code,
            source_city,
            destination_city,
            departure_date
        )
    )

def process_message_passenger(channel, method, properties, body):
    try:
        message = json.loads(body)
        logger.info(f"Received message: {message}")
        process_passenger(message)

        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )

def process_passenger(message):
    logger.debug(f"Processing passenger: {message}")
    p_id = message["Passenger ID"]
    sn = message["First Name"]
    ln = message["Last Name"]
    p_fn = f"{sn} {ln}"
    p_nat = message["Nationality"]
    p_age = int(message["Age"])
    f_id = message["flight_id"]

    conn = get_mysql_connection()
    try:
        if passenger_exists(conn, p_id):
            return None, None
        else:
            logger.info(f"Inserting new passenger: {p_id}")
            insert_passenger(conn, p_id, p_fn, p_nat, p_age, f_id)
            conn.commit()
    except mysql.connector.errors.IntegrityError:
        conn.rollback()
        return None
    finally:
        conn.close()

def passenger_exists(conn, passenger_id):
    logger.debug(f"Checking if passenger exists: {passenger_id}")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM passengers WHERE passenger_id = %s LIMIT 1",
        (passenger_id,)
    )
    return cursor.fetchone() is not None

def insert_passenger(conn, passenger_id, passenger_name, passenger_nationality, passenger_age, flight_id):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO passengers (
            passenger_id,
            passenger_name,
            passenger_age,
            passenger_nationality,
            flight_id,
            to_delete
        )
        VALUES (%s, %s, %s, %s, %s, FALSE)
        """,
        (
            passenger_id,
            passenger_name,
            passenger_age,
            passenger_nationality,
            flight_id
        )
    )

def handlePfQuerySchedule():
    conn = get_mysql_connection()
    try:
        data = get_all_pf_data(conn)
        logger.info(f"Retrieved {len(data)} passenger-flight records")
        produce_scheduled_data_to_rmq(data)
        delete_passengers(
            conn, 
            passenger_ids=[record["passenger_id"] for record in data]
            )
        conn.commit()
    except Exception as e:
        logger.error(f"Error retrieving passenger-flight data: {e}")
        raise

def produce_scheduled_data_to_rmq(data):
    try:
        connection = get_rmq_connection()
        channel = connection.channel()
        channel.queue_declare(queue=PRODUCE_QUEUE_NAME_SCHED_PF, durable=True)

        for record in data:
            message = json.dumps(record, default=str)
            channel.basic_publish(
                exchange="",
                routing_key=PRODUCE_QUEUE_NAME_SCHED_PF,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )
        logger.info(f"Produced {len(data)} records to RabbitMQ")
    except Exception as e:
        logger.error(f"Error producing data to RabbitMQ: {e}")

def get_all_pf_data(conn):
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT 
            p.passenger_id,
            p.passenger_name,
            p.passenger_age,
            p.passenger_nationality,
            p.flight_id,
            f.airline,
            f.flight_code,
            f.source_city,
            f.destination_city,
            f.departure_date
        FROM passengers p
        JOIN flights f
            ON p.flight_id = f.flight_id
    """
    cursor.execute(query)
    return cursor.fetchall()

def delete_passengers(conn, passenger_ids):
    cursor = conn.cursor()
    format_strings = ','.join(['%s'] * len(passenger_ids))
    query = f"DELETE FROM passengers WHERE passenger_id IN ({format_strings})"
    cursor.execute(query, tuple(passenger_ids))

def rmq_listener():
    logger.info("**********Starting passenger-flights service**********")

    logger.info("Starting SSL RabbitMQ consumer...")
    global connection, channel
    connection = get_rmq_connection()
    channel = connection.channel()

    logger.info(f"Declaring queues {CONSUME_QUEUE_NAME_FLT} and {CONSUME_QUEUE_NAME_PSG}")
    channel.queue_declare(queue=CONSUME_QUEUE_NAME_FLT, durable=True)
    channel.queue_declare(queue=CONSUME_QUEUE_NAME_PSG, durable=True)
    channel.basic_qos(prefetch_count=1)

    logger.info(f"Consuming messages from {CONSUME_QUEUE_NAME_FLT} and {CONSUME_QUEUE_NAME_PSG}")

    channel.basic_consume(
        queue=CONSUME_QUEUE_NAME_FLT,
        on_message_callback=process_message_flight,
        auto_ack=False
    )
    channel.basic_consume(
        queue=CONSUME_QUEUE_NAME_PSG,
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

@app.route('/schedule-pf-query', methods=['POST'])
def TriggerPfQuery():
    print("Received request from:", request.host)
    allowed_host = pf_be_host
    host = request.host.split(":")[0]
    print(host)
    if host != allowed_host:
        abort(403, description="Forbidden: Invalid Host header")
    
    if "X-Whosurdaddy" not in request.headers:
        abort(400, "Missing required header")

    if request.headers["X-Whosurdaddy"] != "Han":
        abort(403, "Invalid header value")

    try:
        handlePfQuerySchedule()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        print("Error processing request:", str(e))
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    bootstrap()
    t1 = threading.Thread(target=rmq_listener, daemon=True)
    t1.start()
    app.run(host="0.0.0.0", port=2443, ssl_context=(cert_file, key_file), use_reloader=False)
