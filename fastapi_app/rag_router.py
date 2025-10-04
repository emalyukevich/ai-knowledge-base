import logging
import uuid
import time

from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Literal, Optional
from starlette.responses import JSONResponse

from fastapi_app.metrics.metrics import REQUESTS_TOTAL, LATENCY_SECONDS, ERRORS_TOTAL
from fastapi_app.errors.exceptions import ValidationError, LLMError, DBError
from fastapi_app.rag_pipeline import rag_pipeline

logger = logging.getLogger('rag_router')

router = APIRouter()

class QueryRequest(BaseModel):
    query: str

class QueryResponse(BaseModel):
    answer: str
    sources: List[str]
    latency: float
    status: Literal['success']

class ErrorResponse(BaseModel):
    status: Literal['error']
    message: str
    latency: Optional[float] = None

@router.post('/query', response_model=QueryResponse,
             responses={400: {'model': ErrorResponse}, 500: {'model': ErrorResponse}})
async def rag_query(request: QueryRequest):
    request_id = str(uuid.uuid4())
    start = time.perf_counter()

    try:
        query_text = request.query.strip()

        #mock throw
        if not query_text:
            raise ValidationError("Query cannot be empty")
        elif query_text.lower().startswith("db_error"):
            raise DBError("Database connection failed")
        elif query_text.lower().startswith("llm_error"):
            raise LLMError("LLM request failed")

        result = rag_pipeline(query_text)
        latency = round(time.perf_counter() - start, 3)

        logger.info("[%s] query='%s' latency=%.3f", request_id, query_text[:80], latency)
        REQUESTS_TOTAL.labels(status='success').inc()
        LATENCY_SECONDS.labels(endpoint='/query').observe(latency)

        return QueryResponse(
            answer = result['answer'],
            sources = result['sources'],
            latency = latency,
            status = 'success'
        )

    except ValidationError as e:
        latency = round(time.perf_counter() - start, 3)
        ERRORS_TOTAL.labels(type='validation').inc()
        REQUESTS_TOTAL.labels(status='error').inc()
        return JSONResponse(status_code=400, content={"status": "error", "message": str(e), "latency": latency})

    except LLMError as e:
        latency = round(time.perf_counter() - start, 3)
        ERRORS_TOTAL.labels(type='llm_error').inc()
        REQUESTS_TOTAL.labels(status='error').inc()
        return JSONResponse(status_code=500, content={"status": "error", "message": 'LLM error', "latency": latency})

    except DBError as e:
        latency = round(time.perf_counter() - start, 3)
        ERRORS_TOTAL.labels(type="db_error").inc()
        REQUESTS_TOTAL.labels(status="error").inc()
        return JSONResponse(status_code=500, content={"status": "error", "message": "DB error", "latency": latency})

    except Exception as e:
        latency = round(time.perf_counter() - start, 3)
        ERRORS_TOTAL.labels(type="internal").inc()
        REQUESTS_TOTAL.labels(status="error").inc()
        logger.exception("[%s] internal error: %s", request_id, str(e))
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error", "latency": latency})