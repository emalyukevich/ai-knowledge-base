# 🧠 AI-Knowledge-Base  
**Modular ML Platform for Intelligent Document Processing and RAG-based Question Answering**

---

## 🚀 Overview  
**AI-Knowledge-Base** — это production-level ML-платформа, объединяющая обработку корпоративных данных, генерацию эмбеддингов, поиск по семантическому пространству и интеграцию с LLM через RAG-пайплайн.  

Проект демонстрирует полный цикл разработки ML-системы — от извлечения данных до деплоя и мониторинга, включая MLOps-компоненты (MLflow, Airflow, Prometheus, Grafana).  

---

## 🏗Architecture  

📦 ai-knowledge-base/
├── airflow/ # DAGs, logs, plugins
├── data/
│ ├── raw/ # Сырьевые файлы (PDF, HTML, CSV, Parquet)
│ └── processed/ # Очистка, чанки, JSONL
├── etl/ # Обработка и подготовка данных
├── embeddings/ # Генерация эмбеддингов (Sentence Transformers)
├── fastapi_app/
│ ├── main.py # FastAPI entry point
│ ├── rag_pipeline.py # Основная логика RAG
│ ├── metrics/ # Prometheus метрики и middleware
│ ├── tracking/ # MLflow логирование
│ └── db/ / nlp/ # Подключения и вспомогательные модули
├── docker-compose.yml # Оркестрация всех сервисов
└── requirements.txt


---

## ⚙️ Tech Stack  

| Category | Tools / Libraries |
|-----------|------------------|
| **Language** | Python 3.10 |
| **ML / NLP** | scikit-learn, PyTorch, Hugging Face |
| **Vector Storage** | ClickHouse |
| **Orchestration** | Airflow |
| **Experiment Tracking** | MLflow |
| **API / Serving** | FastAPI, Docker |
| **Monitoring** | Prometheus, Grafana |
| **Data Handling** | Pandas, NumPy |

---

## 🔍 Key Features  

✅ **RAG Pipeline:** full chain — text preprocessing → embeddings → vector search → LLM response.  
✅ **ETL System:** unified text extraction from PDF, HTML, CSV, and Parquet.  
✅ **ClickHouse Integration:** scalable vector storage and cosine similarity search.  
✅ **FastAPI Service:** endpoints for document loading and RAG queries (`/load_documents`, `/rag/query`).  
✅ **MLflow Tracking:** automatic logging of parameters, metrics, and latency.  
✅ **Prometheus + Grafana:** live metrics for latency, request rate, and error classification.  
✅ **Airflow DAGs:** automated weekly ETL and embedding refresh pipeline.  
✅ **Containerized Setup:** all components run via `docker-compose`.  

---

## 🧩 Current Stage — Airflow Integration  

We’re currently at **Stage 6: Airflow orchestration**.  
- Airflow is deployed in Docker with `webserver`, `scheduler`, and `postgres` backend.  
- DAG `rag_etl_pipeline` automates ETL → embedding → ClickHouse update workflow.  
- Each task calls corresponding Python modules from the main project.  
- DAGs are tested and can be triggered manually via the Airflow UI.  

---

