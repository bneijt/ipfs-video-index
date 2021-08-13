#!/bin/bash
set -e
export DQP_PROJECT_PATH=$HOME/tmp/ipfs_indexer
poetry run python -m ipfs_video_index.ipfs_indexer $DQP_PROJECT_PATH