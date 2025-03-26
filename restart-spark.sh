#!/bin/bash

docker compose down spark-master spark-worker spark-sql && \
docker compose up -d spark-master spark-worker spark-sql && \
docker compose logs -f spark-master
