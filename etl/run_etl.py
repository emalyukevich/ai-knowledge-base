from pathlib import Path
from ingest import extract_html, extract_pdf, extract_csv, extract_parquet
from preprocess import clean_text
from chunking import chunk_text, save_chunks
import time
from tqdm import tqdm

RAW = Path("../data/raw")
PROCESSED = Path("../data/processed")

def process_html():
    for f in (RAW/"html").glob("*.htm*"):
        text = extract_html(f)
        text = clean_text(text)
        chunks = chunk_text(text)
        save_chunks(chunks, PROCESSED/"html"/(f.stem+".jsonl"),
                    {"id": f.stem, "source_file": str(f), "format": "html"})

def process_pdf():
    for f in (RAW/"pdf").glob("*.pdf*"):
        text = extract_pdf(f)
        text = clean_text(text)
        chunks = chunk_text(text)
        save_chunks(chunks, PROCESSED/"pdf"/(f.stem+".jsonl"),
                    {"id": f.stem, "source_file": str(f), "format":"pdf"})

def process_csv():
    for f in (RAW/"csv").glob("*.csv*"):
        records = extract_csv(f) #list[dict]
        text = "\n".join([str(rec) for rec in records])
        text = clean_text(text)
        chunks = chunk_text(text)
        save_chunks(chunks, PROCESSED/"csv"/(f.stem+".jsonl"),
                    {"id": f.stem, "source_file": str(f), "format": "csv"})

def process_parquet():
    for f in (RAW/"parquet").glob("*.parquet"):
        records = extract_parquet(f) #list[dict]
        text = "\n".join([str(rec) for rec in records])
        text = clean_text(text)
        chunks = chunk_text(text)
        save_chunks(chunks, PROCESSED/"parquet"/(f.stem + ".jsonl"),
                    {"id": f.stem, "source_file": str(f), "format": "parquet"})

if __name__ == "__main__":
    processes = [
        ("Processing HTML", process_html),
        ("Processing PDF", process_pdf),
        ("Processing CSV", process_csv),
        ("Processing Parquet", process_parquet)
    ]

    for name, process_func in tqdm(processes, desc="ETL Pipeline"):
        print(f"\n🚀 Starting {name}...")
        process_func()
        time.sleep(0.1)

    print("✅ ETL complete!")
    print("ETL complete")