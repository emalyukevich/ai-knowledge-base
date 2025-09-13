import os
import asyncio
import logging
from pathlib import Path
import aiofiles

from etl.ingest import extract_html, extract_pdf, extract_csv_content, extract_parquet
from etl.preprocess import clean_text
from etl.chunking import records_to_chunks, chunk_text, save_chunks
from fastapi_app.config import PROCESSED_DIR

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

async def run_etl(filepath: Path):
    suffix = filepath.suffix.lower().lstrip('.')
    stem = filepath.stem

    logger.info(f"[ETL] Запуск обработки файла: {filepath} (тип: {suffix})")

    try:
        loop = asyncio.get_event_loop()

        if suffix in ["htm", "html"]:
            try:
                text = await loop.run_in_executor(None, extract_html, filepath)
                logger.info(f"[ETL] Extract html завершён, длина текста: {len(text)}")

                text = clean_text(text)
                logger.info(f"[ETL] Текст после clean_text длиной {len(text)} символов")

                chunks = chunk_text(text)
                logger.info(f"[ETL] chunk_text вернул {len(chunks)} чанков")

                save_chunks(
                    chunks,
                    PROCESSED_DIR / "html" / f"{stem}.jsonl",
                    {"id": stem, "source_file": str(filepath), "format": "html"}
                )

                logger.info(f"[ETL] HTML обработан")
                return stem

            except Exception as e:
                logger.error(f"[ETL] Ошибка при обработке html: {e}", exc_info=True)
                return None

        elif suffix == "pdf":
            try:
                text = await loop.run_in_executor(None, extract_pdf, filepath)
                logger.info(f"[ETL] Extract pdf завершён, длина текста: {len(text)}")

                text = clean_text(text)
                logger.info(f"[ETL] Текст после clean_text длиной {len(text)} символов")

                chunks = chunk_text(text)
                logger.info(f"[ETL] chunk_text вернул {len(chunks)} чанков")

                save_chunks(
                    chunks,
                    PROCESSED_DIR / "pdf" / f"{stem}.jsonl",
                    {"id": stem, "source_file": str(filepath), "format": "pdf"}
                )

                logger.info(f"[ETL] PDF обработан")
                return stem

            except Exception as e:
                logger.error(f"[ETL] Ошибка при обработке pdf: {e}", exc_info=True)
                return None

        elif suffix == "csv":
            try:
                async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                    csv_content = await f.read()

                records = await loop.run_in_executor(None, extract_csv_content, csv_content)
                logger.info(f"[ETL] Extract csv завершён, количество записей: {len(records)}")

                chunks = list(records_to_chunks(records))
                logger.info(f"[ETL] Потоковый чанкинг вернул {len(chunks)} чанков")

                save_chunks(
                    chunks,
                    PROCESSED_DIR / "csv" / f"{stem}.jsonl",
                    {"id": stem, "source_file": str(filepath), "format": "csv"}
                )

                logger.info(f"[ETL] CSV обработан")
                return stem

            except Exception as e:
                logger.error(f"[ETL] Ошибка при обработке csv: {e}", exc_info=True)
                return None

        elif suffix == "parquet":
            try:
                records = await loop.run_in_executor(None, extract_parquet, filepath)

                if not records:
                    logger.warning(f"[ETL] extract_parquet вернул пустой результат для {filepath}")
                    return None

                logger.info(f"[ETL] Extract parquet завершён, количество записей: {len(records)}")

                chunks = list(records_to_chunks(records))
                logger.info(f"[ETL] Потоковый чанкинг вернул {len(chunks)} чанков")

                save_chunks(
                    chunks,
                    PROCESSED_DIR / "parquet" / f"{stem}.jsonl",
                    {"id": stem, "source_file": str(filepath), "format": "parquet"}
                )

                logger.info(f"[ETL] Parquet обработан")
                return stem

            except Exception as e:
                logger.error(f"[ETL] Ошибка при обработке parquet: {e}", exc_info=True)
                return None

        else:
            raise ValueError(f"Unsupported file type: {suffix}")

    except Exception as e:
        logger.error(f"[ETL] Ошибка при обработке {filepath}: {e}", exc_info=True)
        return None

    finally:
        if filepath.exists():
            os.remove(filepath)
            logger.info(f"[ETL] Исходный файл удалён: {filepath}")