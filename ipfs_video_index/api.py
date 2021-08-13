from fastapi import FastAPI, Header, status, Path
from pydantic import BaseModel
from dqp.queue import Project
import multihash
from fastapi.staticfiles import StaticFiles
import os
import mimetypes
from typing import Optional
from fastapi.responses import RedirectResponse, StreamingResponse
import json
from ipfs_video_index import database
from datetime import datetime, timezone
from fastapi.responses import JSONResponse

mimetypes.init()
mimetypes.add_type("application/xml+metalink", ".metalink")


class IndexRequest(BaseModel):
    cid: str

    class Config:
        schema_extra = {
            "example": {
                "cid": "QmXDs15TwsXWotqm6aqV5VCABoNRBRHwbXMSSmR4uKh8HG",
            }
        }


app = FastAPI()


project = Project(os.environ.get("DQP_PROJECT_PATH", "/project"))

index_queue = project.open_sink("index")
played_queue = project.open_sink("played")

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
    played_queue.write_dict(
        {
            "cid": multihash.to_b58_string(cid),
            "timestamp": datetime.now(timezone.utc).timestamp(),
        }
    )
    return {"message": "OK"}


@app.get("/viewed/{cid}")
async def get_viewed(
    cid: str = Path(default="", example="QmXDs15TwsXWotqm6aqV5VCABoNRBRHwbXMSSmR4uKh8HG", title="The base base 58 CID string"),
):
    with database.open(project) as db:
        rows = db.execute("select count from view_count where cid = ? limit 1", (cid,))
        for row in rows:
            return {"cid": cid, "count": row[0]}
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"message": "not found"},
    )
