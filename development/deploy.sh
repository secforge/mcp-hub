#!/bin/bash
set -e

cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

if [ ! -f "remote.env" ]; then
  echo "Error: remote.env file not found."
  echo "Please create a remote.env file (see remote.env.example)"
  exit 1
fi
. remote.env
cd ..

name=mcp-hub:${REMOTE_INSTANCE:-local}

echo "Building ${name}"
SECONDS=0
docker build -t mcp-hub:local .
echo "(docker build took ${SECONDS} seconds.)"
echo

[[ -z "${REMOTE_INSTANCE}" ]] && echo "Skipping push for local image" && exit 0

echo "Pushing image ${name}"
docker image tag mcp-hub:local "${REGISTRY}/${name}"
docker push "${REGISTRY}/${name}"

echo "Deploying to ${REMOTE_HOST}"
ssh -p 64700 "${REMOTE_USER}@${REMOTE_HOST}" /bin/bash -e <<EOF
  docker image pull "${REGISTRY}/${name}"
  cd /srv/docker/mcp-hub
  docker compose down && docker compose up -d
EOF

echo "Done!"
