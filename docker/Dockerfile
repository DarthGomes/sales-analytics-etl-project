FROM apache/spark:3.5.5

USER root

RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get clean

COPY requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

EXPOSE 8888