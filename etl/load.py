import json
import logging
from pathlib import Path
import clickhouse_connect

from embeddings.embeddings import EmbeddingsModel
from fastapi_app.config import PROCESSED_DIR

import sys

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

def get_clickhouse_connect():
    return clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="default",
        password="default_pass"
    )

def load_jsonl(path: Path):
    records = []

    with path.open('r', encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            if 'text' in rec:
                records.append({"text": rec["text"], "metadata": rec.get("metadata", {})})
    return records

def insert_embeddings(client, model, records: list[dict]):
    texts = [r['text'] for r in records]
    metadata = [r['metadata'] for r in records]

    vectors = model.encode(texts)
    rows = [(t, m, v) for t, m, v in zip(texts, metadata, vectors)]

    client.insert(
        'embeddings',
        rows,
        column_names=['text', 'metadata', 'vector']
    )

def run_load(processed_dir: Path = PROCESSED_DIR):
    client = get_clickhouse_connect()
    model = EmbeddingsModel()

    for jsonl_file in processed_dir.glob("*.jsonl"):
        logger.info(f"[LOAD] Обработка {jsonl_file}")
        records = load_jsonl(jsonl_file)

        if not records:
            logger.warning(f"[LOAD] {jsonl_file} пустой, пропускаем")
            continue

        insert_embeddings(client, model, records)
        logger.info(f"[LOAD] {jsonl_file} → загружено {len(records)} записей")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
        if not path.exists():
            logger.error(f"Файл {path} не найден")
            sys.exit(1)

        client = get_clickhouse_connect()
        model = EmbeddingsModel()

        logger.info(f"[LOAD] Обработка {path}")
        records = load_jsonl(path)

        if not records:
            logger.warning(f"[LOAD] {path} пустой, пропускаем")
        else:
            insert_embeddings(client, model, records)
            logger.info(f"[LOAD] {path} → загружено {len(records)} записей")
    else:
        run_load()