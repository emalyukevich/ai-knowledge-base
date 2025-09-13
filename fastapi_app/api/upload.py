from fastapi import APIRouter, UploadFile, Form, BackgroundTasks
from pathlib import Path
import shutil
import asyncio

from etl.run_etl import run_etl
from fastapi_app import config

def start_etl_sync(filepath: Path):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_etl(filepath))
    loop.close()
router = APIRouter()
@router.post("/upload_chunk")
async def upload_chunk(
        file_id: str = Form(...),
        chunk_index: int = Form(...),
        chunk: UploadFile = None
):
    file_dir = config.UPLOAD_TMP_DIR/file_id
    file_dir.mkdir(parents=True, exist_ok=True)

    chunk_path = file_dir/f"{chunk_index}.part"

    with open(chunk_path, "wb") as f:
        while content := await chunk.read(1024 * 1024):
            f.write(content)

    return {"status": "ok", "chunk_index": chunk_index}

@router.post("/merge_file")
async def merge_file(
    background_tasks: BackgroundTasks,
    file_id: str = Form(...),
    total_chunks: int = Form(...),
    filename: str = Form(...)

):
    file_dir = config.UPLOAD_TMP_DIR / file_id
    suffix = Path(filename).suffix.lower().lstrip(".")

    final_dir = config.RAW_DIR / suffix
    final_dir.mkdir(parents=True, exist_ok=True)

    final_path = final_dir / filename

    with open(final_path, "wb") as outfile:
        for i in range(total_chunks):
            chunk_path = file_dir/f"{i}.part"
            with open(chunk_path, "rb") as f:
                shutil.copyfileobj(f, outfile)

    background_tasks.add_task(start_etl_sync, final_path)

    shutil.rmtree(file_dir, ignore_errors=True)

    return {"status": "accepted", "file": filename, "message": "ETL запущен в фоне"}