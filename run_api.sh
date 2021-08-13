#!/bin/bash
set -e
export DQP_PROJECT_PATH=$HOME/tmp/ipfs_indexer
poetry run uvicorn ipfs_video_index.api:app --reload