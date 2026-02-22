FROM python:3.11-slim

WORKDIR /apps/dcs

RUN apt-get update && apt-get install -y ca-certificates dnsutils iputils-ping curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY flights_stub.py passenger_stub.py ./

CMD ["python", "passenger_stub.py"]
