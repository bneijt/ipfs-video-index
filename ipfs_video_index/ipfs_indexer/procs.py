import sqlite3
from dqp.queue import Source, Sink, Project
from dqp.storage import Folder
import plyvel
import contextlib
import requests
from tempfile import NamedTemporaryFile
import hashlib
import magic
from metalink.metalink import Metalink4, MetalinkFile4
from loguru import logger
import mimetypes
from ipfs_video_index import database
from typing import Set

# Unique the index_request queue
# process the index queue
IPFS_GATEWAYS = [
    "https://ipfs.io/ipfs/",
    "https://gateway.pinata.cloud/ipfs/",
    "https://cloudflare-ipfs.com/ipfs/",
    "https://hardbin.com/ipfs/",
    "https://dweb.link/ipfs/",
]


def uniq(source_queue: Source, sink_queue: Sink, state: Folder) -> None:
    with contextlib.closing(
        plyvel.DB(state.create_path("db"), create_if_missing=True)
    ) as db:
        for part_file, idx, index_request in source_queue:
            cid_bytes = index_request["cid"].encode("utf-8")
            if db.get(cid_bytes) is None:
                # New CID, put on index queue and add to state database
                db.put(cid_bytes, b"")
                sink_queue.write_dict(index_request)


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


def index_to(indexq: Source, metalinks_folder: Folder):
    with contextlib.closing(
        plyvel.DB(metalinks_folder.create_path("db"), create_if_missing=True)
    ) as db:
        for part_file, idx, index_request in indexq:
            cid = index_request["cid"]
            meta_file = extract_information(cid)
            metalink_doc = Metalink4()
            metalink_doc.files.append(meta_file)
            with open(
                metalinks_folder.child(f"{cid}.metalink"), "wb"
            ) as metalink_output:
                metalink_output.write(metalink_doc.generate())

            db.put(cid.encode("utf-8"), meta_file.filename.encode("utf-8"))


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


def index(db: sqlite3.Connection, queue: Source):
    count = 0
    for filename, idx, msg in queue:
        # Register view count
        db.execute(
            "insert or ignore into view_count(cid, count) values(?, ?)",
            (msg["cid"], 0),
        )
        count += 1
    logger.info(f"Added {count} to index")


def pipeline(project: Project):
    index_queue = project.continue_source("index")
    played_queue = project.continue_source("played")

    with database.open(project) as db:
        index(db, index_queue)
        update_view_count(db, played_queue)
