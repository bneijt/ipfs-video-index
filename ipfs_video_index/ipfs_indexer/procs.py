import contextlib
import hashlib
import mimetypes
import os
import sqlite3
from tempfile import NamedTemporaryFile
from typing import Dict, Set

import magic
import requests
from dqp.queue import Project, Sink, Source
from dqp.storage import Folder
from loguru import logger
from metalink.metalink import MetalinkFile4

from ipfs_video_index import database

# Unique the index_request queue
# process the index queue
IPFS_GATEWAYS = [
    "https://ipfs.io/ipfs/",
    "https://gateway.pinata.cloud/ipfs/",
    "https://cloudflare-ipfs.com/ipfs/",
    "https://hardbin.com/ipfs/",
    "https://dweb.link/ipfs/",
]

IPFS_API_ADDRESS = os.environ.get("IPFS_API_ADDRESS", "http://127.0.0.1:5001")


def extract_information(
    cid: str,
) -> MetalinkFile4:
    # https://datatracker.ietf.org/doc/html/rfc5854
    with NamedTemporaryFile() as tempfile:
        logger.info("Indexing {cid}")
        r = requests.get(IPFS_GATEWAYS[0] + cid, stream=True)
        sha1 = hashlib.sha1()

        for chunk in r.iter_content(chunk_size=1024 * 1024):
            tempfile.write(chunk)
            sha1.update(chunk)
        tempfile.flush()
        sha1_digest = sha1.hexdigest()
        mime = magic.Magic(mime=True)
        mime_type = mime.from_file(tempfile.name)
        meta_file = MetalinkFile4("", do_ed2k=False)
        meta_file.filename = f"{sha1_digest}.{mimetypes.guess_extension(mime_type)}"
        meta_file.set_size(tempfile.tell())
        meta_file.add_checksum("sha1", sha1_digest)
        for gateway in IPFS_GATEWAYS:
            meta_file.add_url(gateway + cid)
        return meta_file


def update_view_count(db: sqlite3.Connection, queue: Source):
    starting_time = 0
    count = 0

    cids_for_round = set()  # type: Set[str]
    for filename, idx, msg in queue:
        # Register view count
        cids_for_round.add(msg["cid"])

        # Every five hour slot gets a vote
        if msg["timestamp"] - starting_time > 5:
            starting_time = msg["timestamp"]
            # Rotate to next voting round
            for cid in cids_for_round:
                db.execute(
                    "update view_count set count = count + 1 where cid = ?",
                    (cid,),
                )
            cids_for_round = set()
        count += 1

    logger.info(f"Updated view count for {count}")


def get_names(directory_cid: str) -> Dict[str, str]:
    try:
        response = requests.post(
            IPFS_API_ADDRESS + "/api/v0/ls",
            params={
                "arg": directory_cid,
                "headers": "true",
                "resolve-type": "true",
                "size": "true",
                "stream": "false",
            },
        )
        if response.status_code == 200:
            result = {}
            listing = response.json()
            for link in listing["Objects"][0]["Links"]:
                if link["Size"] > 100 and link["Name"].endswith(".webm"):
                    result[link["Hash"]] = link["Name"]
            return result
    except requests.exceptions.Timeout:
        logger.error(f"Failed to index {directory_cid} due to timeout")
    return {}


def index(db: sqlite3.Connection, queue: Source):
    count = 0
    for filename, idx, msg in queue:
        cid = msg["cid"]
        for cid, name in get_names(cid).items():
            db.execute(
                "insert or ignore into names(cid, name) values(?, ?)",
                (cid, name),
            )
            db.execute(
                "insert or ignore into view_count(cid) values(?)",
                (cid,),
            )
            count += 1
    logger.info(f"Added {count} to index")


def pipeline(project: Project):
    index_queue = project.continue_source("index")
    played_queue = project.continue_source("viewed")
    with database.open(project) as db:
        index(db, index_queue)
        update_view_count(db, played_queue)
        index_queue.unlink_to()
        played_queue.unlink_to()
