FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev \
  && rm -rf /var/lib/apt/lists/*

COPY docker/generator/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src/transaction_generator.py /app/transaction_generator.py

CMD ["python", "-u", "/app/transaction_generator.py"]
