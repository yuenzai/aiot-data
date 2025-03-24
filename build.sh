#!/bin/bash
./mvnw --settings .mvn/wrapper/settings.xml clean package

docker compose down apiserver && \
docker compose up -d apiserver && \
docker compose logs -f --tail 1 apiserver
