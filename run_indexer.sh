#!/bin/bash
set -e
export DQP_PROJECT_PATH=$HOME/tmp/ipfs_indexer
export IPFS_API_ADDRESS=http://127.0.0.1:5001
poetry run python -m ipfs_video_index.ipfs_indexer $DQP_PROJECT_PATH