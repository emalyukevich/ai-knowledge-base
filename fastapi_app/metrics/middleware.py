import time
from starlette.middleware.base import BaseHTTPMiddleware, Request
from metrics import REQUESTS_TOTAL, LATENCY_SECONDS

class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.perf_counter()
        try:
            response = await call_next(request)
            status = 'success' if response.status_code < 400 else 'error'
        except Exception:
            status = 'error'
            raise
        finally:
            latency = round(time.perf_counter() - start_time, 3)
            REQUESTS_TOTAL.labels(status=status).inc()
            LATENCY_SECONDS.labels(endpoint=request.url.path).observe(latency)

        return response