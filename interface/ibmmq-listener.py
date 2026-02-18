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
    global ca_cert, logdir, loglvl, logger, cert_file, key_file, ibmmq_host, ibmmq_port, ibmmq_user, ibmmq_password, ibmmq_queue_manager, ibmmq_channel, ibmmq_key_repo, ibmmq_producer_conn, ibmmq_queue
    cert_file = os.environ.get("CERT_PATH")
    key_file = os.environ.get("KEY_PATH")
    ibmmq_host = os.environ.get("IBM_MQ_HOST")
    ibmmq_port = int(os.environ.get("IBM_MQ_PORT"))
    ibmmq_user = os.environ.get("IBM_MQ_USER")
    ibmmq_password = os.environ.get("IBM_MQ_PW")
    ibmmq_queue_manager = os.environ.get("IBM_MQ_QUEUE_MANAGER")
    ibmmq_channel = os.environ.get("IBM_MQ_CHANNEL")
    ibmmq_key_repo = os.environ.get("IBM_MQ_KEY_REPO")
    ca_cert = os.environ.get("CA_PATH")
    logdir = os.environ.get("log_directory", ".")
    loglvl = os.environ.get("log_level", "INFO").upper()
    ibmmq_producer_conn = connect_ibm_mq()
    ibmmq_queue = pymqi.Queue(ibmmq_producer_conn, "PF.TOKEN.QUEUE")


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

    file_handler = logging.FileHandler(f'{logdir}/ibmmq-listener.log')
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

def listen():
    try:
        print(f"Listening on queue 'PF.TOKEN.QUEUE'...")
        while True:
            try:
                message_bytes = ibmmq_queue.get()
                message_str = message_bytes.decode("utf-8")

                print("Received message:")
                try:
                    data = json.loads(message_str)
                    print("Parsed JSON:", data)
                except:
                    pass

            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    continue
                else:
                    raise

    except Exception as e:
        print("Listener error:", e)
        sys.exit(1)

if __name__ == "__main__":
    bootstrap()
    listen()
