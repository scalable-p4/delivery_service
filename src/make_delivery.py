import os
import sys
from dotenv import load_dotenv
from celery import Celery
from sqlalchemy import insert, select
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
from src.database import token_delivery

# OpenTelemetry imports for tracing and metrics
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.celery import CeleryInstrumentor

# Logging imports
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
import logging
service_name = "delivery_worker"

# Initialize TracerProvider for OTLP
resource = Resource(attributes={SERVICE_NAME: service_name})
trace_provider = TracerProvider(resource=resource)
otlp_trace_exporter = OTLPSpanExporter(endpoint="otel-collector:4317", insecure=True)
trace_provider.add_span_processor(BatchSpanProcessor(otlp_trace_exporter))
trace.set_tracer_provider(trace_provider)

# Initialize MeterProvider for OTLP
metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint="otel-collector:4317", insecure=True))
metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(metric_provider)

# Initialize LoggerProvider for OTLP
logger_provider = LoggerProvider(resource=resource)
set_logger_provider(logger_provider)
otlp_log_exporter = OTLPLogExporter(endpoint="otel-collector:4317", insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_log_exporter))
handler = LoggingHandler(level=logging.DEBUG, logger_provider=logger_provider)

# Attach OTLP handler to root logger
logging.getLogger().addHandler(handler)

tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


BROKER_URL = os.getenv("CELERY_BROKER_URL")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")
celery_app = Celery('update_inventory', broker=BROKER_URL,
                    backend=RESULT_BACKEND)
DATABASE_URL_DELIVERY = os.getenv("DATABASE_URL_DELIVERY")
engine = create_engine(DATABASE_URL_DELIVERY)
Session = sessionmaker(bind=engine)

CeleryInstrumentor().instrument()

delivery_counter = meter.create_counter(
    "delivery_made",
    description="Total number of delivery made",
    unit="1",
)
delivery_commit_counter = meter.create_counter(
    "delivery_committed",
    description="Total number of committed delivery",
    unit="1",
)
delivery_rollback_counter = meter.create_counter(
    "delivery_rollback",
    description="Total number of rollback delivery",
    unit="1",
)

@celery_app.task(name="make_delivery")
def make_delivery(payload: dict, fn: str):
    with tracer.start_as_current_span("make_delivery_task"):
        logger.info("Making delivery", extra={"payload": payload, "function": fn})
        delivery_counter.add(1)
        print("fn="+str(fn))
        print("payload="+str(payload))
        username: str = payload.get("username")
        quantity: int = payload.get("quantity")
        delivery: bool = payload.get("delivery")
        print("username="+str(username))
        print("quantity="+str(quantity))
        print("delivery="+str(delivery))
        print("db_url="+str(DATABASE_URL_DELIVERY))
        logger.info(f"Delivery details: username={username}, quantity={quantity}, delivery={delivery}")

        if fn == "make_delivery" and delivery == True:
            session = Session()
            try:
                query = insert(token_delivery).values(
                    username=username, quantity=quantity, delivery=delivery
                )
                session.execute(query)
                session.commit()
                print("delivery made")
                logger.info("Delivery made")
                logger.info("Sending task to creating order")
                celery_app.send_task("create_order",queue="q01",args=[payload,"success_token_transaction"])
                return "SUCCESS"
            except Exception as e:
                print("error making delivery")
                print(e)
                logger.error("Error making delivery", exc_info=True)
                session.rollback()
            finally:
                session.close()
        else:
            logger.info("Invalid Function in make delivery")
            logger.info("Transaction failed in delivery...sending rollback to inventory")
            celery_app.send_task("update_inventory",queue="q03",args=[payload,"rollback_inventory"])
            return "FAIL_DELIVERY"

    
