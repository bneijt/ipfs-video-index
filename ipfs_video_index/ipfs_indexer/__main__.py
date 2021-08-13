from ipfs_video_index.ipfs_indexer.procs import pipeline
from dqp.queue import Project
import argparse
import time
from loguru import logger


def main():
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument(
        "--loop", action="store_true", help="Loop processing the project continuously"
    )
    parser.add_argument(
        "project_path",
        metavar="PROJECT_PATH",
        type=str,
        help="Location of the project storage folder",
    )
    args = parser.parse_args()
    while True:
        logger.info(f"Executing pipeline on {args.project_path}")
        with Project(args.project_path) as project:
            pipeline(project)
        if args.loop:
            time.sleep(60)
        else:
            break


main()
