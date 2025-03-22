#!/bin/bash

SERVICE_NAME=aiot-data

sudo systemctl stop ${SERVICE_NAME}.service && \
sudo systemctl disable ${SERVICE_NAME}.service && \
sudo systemctl daemon-reload && \
sudo rm -f /etc/systemd/system/${SERVICE_NAME}.service

if [ ! $? -eq 0 ]; then
  echo "${SERVICE_NAME} uninstall failed"
else
  echo "${SERVICE_NAME} uninstalled"
fi
