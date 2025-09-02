from fastapi import FastAPI, UploadFile, File
from pathlib import Path
import shutil

from etl.run_etl import process_pdf_file, process_html_file, process_csv_file, process_parquet_file
app = FastAPI(
    title = "AI Knowledge Base API",
    description="Минимальный сервис для старта проекта",
    version="0.1.0"
)

RAW_DIR = Path("../data/raw")
PROCESSED_DIR = Path("../data/processed")
@app.post("/load_documents")
async def load_documents(files: list[UploadFile] = File(...)):
    results = []
    temp_files = []

    try:
        for file in files:
            filename = file.filename
            suffix = Path(filename).suffix.lower().lstrip(".")

            raw_dir = RAW_DIR / suffix
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_path = raw_dir / filename
            temp_files.append(raw_path)

            with raw_path.open("wb") as buffer:
                shutil.copyfileobj(file.file, buffer)

            if suffix == "pdf":
                try:
                    out = process_pdf_file(raw_path)
                except Exception as e:
                    out = {"status": "error", "reason": str(e)}
            elif suffix in ["htm", "html"]:
                try:
                    out = process_html_file(raw_path)
                except Exception as e:
                    out = {"status": "error", "reason": str(e)}
            elif suffix == "csv":
                try:
                    out = process_csv_file(raw_path)
                except Exception as e:
                    out = {"status": "error", "reason": str(e)}
            elif suffix == "parquet":
                try:
                    out = process_parquet_file(raw_path)
                except Exception as e:
                    out = {"status": "error", "reason": str(e)}
            else:
                out = {"status": "unsupported", "file": filename}

            results.append({
                "file": filename,
                "format": suffix,
                "output": out
            })

        return {"status": "ok", "processed": results}
    finally:
        clean_up_temp_files(temp_files)

def clean_up_temp_files(temp_files: list[Path]):
    for temp_file in temp_files:
        try:
            if temp_file.exists():
                temp_file.unlink()
        except Exception as e:
            print(f"Failed to cleanup {temp_file}: {str(e)}")
