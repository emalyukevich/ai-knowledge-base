import os
import sys
import requests
import uuid
from pathlib import Path

API_URL = "http://localhost:8000"
CHUNK_SIZE = 5 * 1024 * 1024
SMALL_LIMIT = 50 * 1024 * 1024

def resolve_path(path: str) -> Path | None:
    p = Path(path).expanduser()
    if p.exists():
        return p.resolve()

    candidates = [
        Path("data/raw/csv") / p.name,
        Path("data/raw") / p.name,
        Path("data") / p.name,
    ]
    for c in candidates:
        if c.exists():
            return c.resolve()

    for root in [Path("data"), Path("data/raw")]:
        if root.exists():
            matches = list(root.rglob(p.name))
            if matches:
                return matches[0].resolve()
    return None

def upload_file(path: str):
    resolved = resolve_path(path)
    if resolved is None:
        raise FileNotFoundError(
            f"Файл {path} не найден! Проверь путь. "
            f"Текущая рабочая директория: {os.getcwd()}"
        )

    file_id = str(uuid.uuid4())
    file_size = resolved.stat().st_size
    total_chunks = (file_size // CHUNK_SIZE) + 1
    filename = resolved.name

    if file_size <= SMALL_LIMIT:
        with open(resolved, "rb") as f:
            response = requests.post(
                f"{API_URL}/load_documents",
                files={"files": (filename, f)},
                timeout=600
            )
        print("Обычная загрузка:", response.json())
    else:
        with open(resolved, "rb") as f:
            for chunk_index in range(total_chunks):
                chunk = f.read(CHUNK_SIZE)
                response = requests.post(
                    f"{API_URL}/files/upload_chunk",
                    data={"file_id": file_id, "chunk_index": chunk_index},
                    files={"chunk": (f"{filename}.part{chunk_index}", chunk)},
                )
                print(f"Uploaded chunk {chunk_index + 1}/{total_chunks}", response.status_code)

        response = requests.post(
            f"{API_URL}/files/merge_file",
            data={"file_id": file_id, "total_chunks": total_chunks, "filename": filename},
        )
        print("Merge response:", response.json())

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Укажи имя файла: python client_upload.py <filename>")
    upload_file(sys.argv[1])