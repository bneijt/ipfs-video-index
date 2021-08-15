import json
import mimetypes
import os
from datetime import datetime, timezone
from typing import Optional

import multihash
from dqp.queue import Project
from fastapi import FastAPI, Header, Path, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from ipfs_video_index import database

mimetypes.init()
mimetypes.add_type("application/xml+metalink", ".metalink")


class IndexRequest(BaseModel):
    cid: str

    class Config:
        schema_extra = {
            "example": {
                "cid": "QmRWBg8QLeH68Q42dd5QK4Y2JpPyepQKFizWzN6DMAedYg",
            }
        }


app = FastAPI(
    title="IPFS video index API",
    description="Indexing for IPFS videos. Source available at https://github.com/bneijt/ipfs-video-index",
)

origins = [
    "https://ipfs.video",
    "https://index.ipfs.video",
    "http://localhost",
    "http://localhost:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

project = Project(os.environ.get("DQP_PROJECT_PATH", "/project"))

index_queue = project.open_sink("index")
viewed_queue = project.open_sink("viewed")

app.mount(
    "/metalinks",
    StaticFiles(directory=project.state_folder("metalinks").path),
    name="metalinks",
)


@app.on_event("startup")
async def startup_event():
    with database.open(project) as db:
        database.create_schema(db)


@app.on_event("shutdown")
async def shutdown_event():
    project.close()


@app.get("/")
async def root(accept: Optional[str] = Header(None)):
    if accept and "text/html" in accept:
        return RedirectResponse("/docs")

    def listing():
        with database.open(project) as db:
            rows = db.execute(
                "select cid from view_count order by count desc limit 500"
            )
            yield "["
            try:
                yield json.dumps(next(rows)[0])
                for row in rows:
                    yield "," + json.dumps(row[0])
            except StopIteration:
                pass
            yield "]"

    return StreamingResponse(listing(), media_type="application/json")


@app.post("/add")
async def add(index_request: IndexRequest):
    cid = multihash.from_b58_string(index_request.cid)
    if not multihash.is_valid(cid):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": "not a valid multihash"},
        )
    index_queue.write_dict(
        {
            "cid": multihash.to_b58_string(cid),
            "timestamp": datetime.now(timezone.utc).timestamp(),
        }
    )
    return {"message": "OK"}


@app.post("/viewed")
async def post_viewed(index_request: IndexRequest):
    cid = multihash.from_b58_string(index_request.cid)
    viewed_queue.write_dict(
        {
            "cid": multihash.to_b58_string(cid),
            "timestamp": datetime.now(timezone.utc).timestamp(),
        }
    )
    return {"message": "OK"}


@app.get("/viewed/{cid}")
async def get_viewed(
    cid: str = Path(
        default="",
        example="QmXDs15TwsXWotqm6aqV5VCABoNRBRHwbXMSSmR4uKh8HG",
        title="The base base 58 CID string",
    ),
):
    with database.open(project) as db:
        rows = db.execute("select count from view_count where cid = ? limit 1", (cid,))
        for row in rows:
            return {"cid": cid, "count": row[0]}
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"message": "not found"},
    )


@app.get("/names/{cid}")
async def get_names(
    cid: str = Path(
        default="",
        example="QmXDs15TwsXWotqm6aqV5VCABoNRBRHwbXMSSmR4uKh8HG",
        title="The base base 58 CID string",
    ),
):
    with database.open(project) as db:
        rows = db.execute("select name from names where cid = ? limit 50", (cid,))
        return {"cid": cid, "names": [r[0] for r in rows]}


@app.get("/names")
async def index_names():
    with database.open(project) as db:
        rows = db.execute(
            "select names.cid, names.name from view_count inner join names on names.cid = view_count.cid order by view_count.count desc limit 500"
        )
        result = {}
        for cid, name in rows:
            if cid not in result:
                result[cid] = []
            result[cid].append(name)
        return result
