import time
import json
import os
import mlflow

from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request

from tracking.logger import mlflow_run

USE_MLFLOW = os.getenv('USE_MLFLOW', 'false').lower() == 'true'
USE_LOCAL = os.getenv('USE_LOCAL_MODEL', 'true').lower() in ('1', 'true', 'yes')
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
LOCAL_LLM_MODEL = os.getenv("LOCAL_LLM_MODEL", "google/flan-t5-small")
TRACKED_PATHS = ['/rag/query']

class MlflowMiddleware(BaseHTTPMiddleware):
    """Автоматически создаёт MLflow run при обращении к /rag/query"""
    async def dispatch(self, request: Request, call_next):
        if not USE_MLFLOW or request.url.path not in TRACKED_PATHS:
            response = await call_next(request)
            return response

        try:
            body = await request.body()
            payload = json.loads(body.decode('utf-8')) if body else {}
        except Exception:
            payload = {}

        params = {
            'path': request.url.path,
            'method': request.method,
            'query': payload.get('query', '')[:80],
            'top_k': payload.get('top_k', None),
        }

        start_time = time.time()
        with mlflow_run(params):
            response = await call_next(request)
            latency = time.time() - start_time
            try:
                resp_json = json.loads(response.body.decode())
                mlflow.log_metric('latency', round(latency, 3))
                mlflow.log_metric('answer_length', len(resp_json.get('answer', '')))
                mlflow.log_metric('num_sources', len(resp_json.get('sources', [])))
                mlflow.set_tag('status', resp_json.get('status', 'unknown'))
                mlflow.set_tag("model_type", "local" if USE_LOCAL else "remote")
                mlflow.set_tag("embedding_model", EMBEDDING_MODEL)
            except Exception:
                mlflow.set_tag('status', 'parse_error')
            return response