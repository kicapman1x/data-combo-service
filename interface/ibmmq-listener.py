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
import json
import requests
import time
import ibmmq

def bootstrap():
    #Environment variables
    global ca_cert, logdir, loglvl, logger, cert_file, key_file, ibmmq_host, ibmmq_port, ibmmq_user, ibmmq_password, ibmmq_queue_manager, ibmmq_channel, ibmmq_key_repo, ibmmq_producer_conn, ibmmq_queue_name, ibmmq_q
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
    ibmmq_queue_name = os.environ.get("IBM_MQ_QUEUE_NAME", "PF.TOKEN.QUEUE")
    ibmmq_qm = get_ibmmq_queue_manager()
    ibmmq_q = ibmmq.Queue(ibmmq_qm, ibmmq_queue_name)
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

    file_handler = logging.FileHandler(f'{logdir}/ibmmq-listener.log')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

def get_ibmmq_queue_manager():
    conn_info = '%s(%s)' % (ibmmq_host, ibmmq_port)
    ssl_cipher_spec = 'ANY_TLS12_OR_HIGHER'
    key_repo_location = ibmmq_key_repo
    cd = ibmmq.CD()
    cd.ChannelName = ibmmq_channel
    cd.ConnectionName = conn_info
    cd.ChannelType = ibmmq.CMQXC.MQCHT_CLNTCONN
    cd.TransportType = ibmmq.CMQXC.MQXPT_TCP
    cd.SSLCipherSpec = ssl_cipher_spec
    sco = ibmmq.SCO()
    sco.KeyRepository = key_repo_location
    cno = ibmmq.CNO()
    cno.Options = ibmmq.CMQC.MQCNO_CLIENT_BINDING
    qmgr = ibmmq.QueueManager(None)
    qmgr.connect_with_options(ibmmq_queue_manager, cd, sco, cno=cno)
    return qmgr


def listen():
    try:
        print(f"Listening on queue {ibmmq_queue_name}...")
        while True:
            try:
                message_bytes = ibmmq_q.get()
                message_str = message_bytes.decode("utf-8")

                print("Received message:")
                try:
                    data = json.loads(message_str)
                    print("Parsed JSON:", data)
                except:
                    pass
            except Exception as e:
                if hasattr(e, "reason") and e.reason == 2033:
                    continue
                else:
                    raise

    except Exception as e:
        print("Listener error:", e)
        sys.exit(1)

if __name__ == "__main__":
    bootstrap()
    listen()
