from fastapi import FastAPI, UploadFile, File, HTTPException
from pathlib import Path
import logging
import aiofiles

from etl.run_etl import run_etl
from fastapi_app.api.upload import router as upload_router
from fastapi_app.config import RAW_DIR
from fastapi_app.api import embeddings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
app = FastAPI(
    title = "AI Knowledge Base API",
    description="Минимальный сервис для старта проекта",
    version="0.1.0",
)
app.include_router(upload_router, prefix="/files")
app.include_router(embeddings.router, prefix="/api", tags=["embeddings"])
@app.post("/load_documents")
async def load_documents(files: list[UploadFile] = File(...)):
    results = []

    for file in files:
        if file.size and file.size > 50 * 1024 * 1024:
            raise HTTPException(status_code=403, detail=f"Файл {file.filename} слишком большой. Используйте /files/upload-chunk")

        suffix = Path(file.filename).suffix.lower().lstrip(".")

        raw_dir = RAW_DIR / suffix
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_dir / file.filename

        async with aiofiles.open(raw_path, "wb") as buffer:
            content = await file.read()
            await buffer.write(content)

        out = await run_etl(raw_path)

        if raw_path.exists():
            raw_path.unlink()

        results.append({"file": file.filename, "format": suffix, "output": out})

    return {"status": "ok", "processed": results}


