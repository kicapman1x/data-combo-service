FROM python:3.11-slim

WORKDIR /apps/dcs

RUN apt-get update && apt-get install -y ca-certificates dnsutils iputils-ping curl wget tar gcc g++ make libc6-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/mqm \
    && wget https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/9.4.5.0-IBM-MQC-Redist-LinuxX64.tar.gz \
    && tar -xzf 9.4.5.0-IBM-MQC-Redist-LinuxX64.tar.gz -C /opt/mqm \
    && rm -f 9.4.5.0-IBM-MQC-Redist-LinuxX64.tar.gz

ENV MQ_FILE_PATH=/opt/mqm

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ibmmq_token-generator.py ./

CMD ["python", "ibmmq_token-generator.py"]
