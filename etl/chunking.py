import json
from pathlib import Path

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

def save_chunks(chunks: list[str], out_path: Path, meta: dict):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for i, ch in enumerate(chunks):
            rec = {**meta, "chunk_index": i, "text": ch}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")