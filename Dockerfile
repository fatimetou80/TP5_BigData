FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ \
    librdkafka-dev \
  && rm -rf /var/lib/apt/lists/*

USER jovyan

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
