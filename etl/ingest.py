from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
import fitz, pdfplumber
import trafilatura

import csv
from io import StringIO

def extract_csv(path: Path) -> list[dict]:
    df = pd.read_csv(path)
    return df.to_dict(orient="records")

def extract_csv_content(content) -> list[dict]:
    reader = csv.DictReader(StringIO(content))
    return list(reader)

def extract_html(path: Path) -> str:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    try:
        return trafilatura.extract(raw) or ""
    except Exception:
        soup = BeautifulSoup(raw, "lxml")
        for s in soup(["script", "style", "noscript"]):
            s.decompose()
        return soup.get_text(" ")

def extract_parquet(path: Path) -> list[dict]:
    df = pd.read_parquet(path)
    return df.to_dict(orient="records")

def extract_pdf(path: Path) -> str:
    try:
        doc = fitz.open(path.as_posix())
        return "\n".join([p.get_text("text") for p in doc])
    except Exception:
        with pdfplumber.open(path.as_posix()) as pdf:
            return "\n".join([p.extract_text() or "" for p in pdf.pages])