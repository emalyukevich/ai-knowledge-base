import mlflow
import time
import os

from contextlib import contextmanager
from typing import Any, Dict

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_TIME = os.getenv("MLFLOW_EXPERIMENT_NAME", "rag_experiments")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment(MLFLOW_EXPERIMENT_TIME)

@contextmanager
def mlflow_run(params: Dict[str, Any]):
    """Контекстный менеджер для автоматического логирования RAG-запроса"""
    run = mlflow.start_run()
    start_time = time.time()

    for k, v in params.items():
        if v is not None:
            mlflow.log_param(k, v)

    try:
        yield run
    except Exception as e:
        mlflow.log_param('error', str(e))
        mlflow.set_tag('status', 'error')
        raise e
    finally:
        mlflow.log_metric('latency_sec', time.time() - start_time)
        mlflow.end_run()