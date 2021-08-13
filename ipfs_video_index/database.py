from dqp.queue import Project
import sqlite3


def open(project: Project) -> sqlite3.Connection:
    database_folder = project.state_folder("db")
    return sqlite3.connect(database_folder.child("index.db"))


def create_schema(db) -> None:
    # db.execute(
    #     "create table if not exists digest_sha1(digest text, cid text, primary key (digest, cid))"
    # )    
    db.execute(
        "create table if not exists view_count(cid text primary key , count int)"
    )
