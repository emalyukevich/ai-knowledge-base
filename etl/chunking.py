import json
from pathlib import Path
from etl.preprocess import clean_text
#import aiofiles

CHUNK_SIZE = 3000
OVERLAP = 300

def chunk_text(text: str) -> list[str]:
    text = text.strip()
    chunks, start = [], 0

    while start < len(text):
        end = start + CHUNK_SIZE
        chunks.append(text[start:end])
        if end >= len(text): break
        start = end - OVERLAP
    return chunks

def records_to_chunks(records):
    buffer = ""
    for rec in records:
        rec_str = "; ".join(f"{k}: {v}" for k, v in rec.items())
        rec_str = clean_text(rec_str) + "\n"

        buffer += rec_str

        while len(buffer) >= CHUNK_SIZE:
            yield buffer[:CHUNK_SIZE]
            buffer = buffer[CHUNK_SIZE - OVERLAP:]
    if buffer.strip():
        yield buffer

def save_chunks(chunks: list[str], out_path: Path, meta: dict):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for i, ch in enumerate(chunks):
            rec = {**meta, "chunk_index": i, "text": ch}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

'''async def save_chunks_async(chunks: list[str], out_path: Path, meta: dict):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(out_path, "w", encoding="utf-8") as f:
        for i, ch in enumerate(chunks):
            rec = {**meta, "chunk_index": i, "text": ch}
            await f.write(json.dumps(rec, ensure_ascii=False) + "\n")'''