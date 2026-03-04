FROM apache/spark:3.5.5

USER root

RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install --no-cache-dir delta-spark==3.0.0 jupyterlab

WORKDIR /app

EXPOSE 8888