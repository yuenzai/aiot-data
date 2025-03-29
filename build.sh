#!/bin/bash

./mvnw --settings .mvn/wrapper/settings.xml clean package

if [ ! $? -eq 0 ]; then
  exit 1
fi

if [ -z $1 ]; then
  exit 0
elif [ $1 == "spark" ]; then
  docker compose build spark-master && \
  docker compose down spark-master spark-worker spark-sql && \
  docker compose up -d spark-master spark-worker spark-sql && \
  docker compose logs -f spark-master
elif [ $1 == "apiserver" ]; then
  docker compose down apiserver && \
  docker compose up -d apiserver && \
  docker compose logs -f apiserver
fi
