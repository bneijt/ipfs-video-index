#!/bin/bash
set -e
rm -rf dist
poetry build
poetry run poetry-lock-package --build --no-root
docker build .