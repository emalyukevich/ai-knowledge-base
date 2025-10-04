from prometheus_client import Counter, Histogram

REQUESTS_TOTAL = Counter(
    'requests_total',
    'Total number of requests',
    ['status']
)

LATENCY_SECONDS = Histogram(
    'latency_seconds',
    'Request latency in seconds',
    ['endpoint']
)

ERRORS_TOTAL = Counter(
    'errors_total',
    'Total number of errors',
    ['type'] # validation | llm_error | db_error | internal
)