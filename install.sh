#!/bin/bash

SERVICE_NAME=aiot-data
WORKDIR=$(dirname $(readlink -f $0))

sudo tee /etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=${SERVICE_NAME}
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=${WORKDIR}
ExecStart=docker compose up -d
ExecStop=docker compose down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload && \
sudo systemctl enable ${SERVICE_NAME}.service

if [ ! $? -eq 0 ]; then
  echo "${SERVICE_NAME} install failed"
  sudo systemctl status ${SERVICE_NAME}.service
else
  echo "${SERVICE_NAME} installed"
fi
