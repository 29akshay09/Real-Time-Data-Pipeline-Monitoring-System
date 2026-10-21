from prometheus_client import start_http_server, Counter

records_processed = Counter(
    "pipeline_records_processed",
    "Total number of records processed"
)

records_failed = Counter(
    "pipeline_records_failed",
    "Total number of records that failed validation"
)

def start_metrics_server(port=8000):
    start_http_server(port)

def record_success(count=1):
    records_processed.inc(count)

def record_failure(count=1):
    records_failed.inc(count)