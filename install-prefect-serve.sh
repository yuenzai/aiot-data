#!/bin/bash

SERVICE_NAME=prefect-serve
WORKDIR=$(dirname $(readlink -f $0))

sudo tee /etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=${SERVICE_NAME}

[Service]
Environment="PREFECT_API_URL=http://localhost/prefect/api"
WorkingDirectory=${WORKDIR}
ExecStart=${WORKDIR}/prefect/env/bin/python ${WORKDIR}/prefect/example_flow.py
Restart=always

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
