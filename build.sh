#!/bin/bash
set -e
rm -rf dist
poetry install
poetry build
poetry run poetry-lock-package --build --no-root
docker build .
docker-compose build --pull