import logging
import queue
from logging.handlers import QueueHandler, QueueListener

from opentelemetry import metrics, trace
from opentelemetry.instrumentation.aiokafka import AIOKafkaInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry._logs import set_logger_provider


logger = logging.getLogger(__name__)


class OTELManager:
    """Centralized OpenTelemetry setup for tracing, logging, and metrics."""

    def __init__(
        self,
        service_name: str = "storage-service",
        otlp_grpc_endpoint: str = "otel-collector:4317",
        otlp_http_endpoint: str = "http://otel-collector:4318",
    ):
        self.service_name = service_name
        self.otlp_grpc_endpoint = otlp_grpc_endpoint
        self.otlp_http_endpoint = otlp_http_endpoint

        # 공통 리소스
        self.resource = Resource(attributes={"service.name": self.service_name})

        # ---- Core initialization ----
        self._init_trace()
        self._init_logging()
        self._init_metrics()
        self._init_instrumentations()

        logger.info("OTEL initialized")

    def _init_trace(self) -> None:
        """Initialize tracing and span exporter."""
        tracer_provider = TracerProvider(resource=self.resource)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(endpoint=self.otlp_grpc_endpoint, insecure=True)
            )
        )
        trace.set_tracer_provider(tracer_provider)
        set_global_textmap(TraceContextTextMapPropagator())
        self.tracer = trace.get_tracer(self.service_name)

    def _init_logging(self) -> None:
        self.logger_provider = LoggerProvider(resource=self.resource)
        set_logger_provider(self.logger_provider)
        otlp_log_exporter = OTLPLogExporter(
            endpoint=self.otlp_grpc_endpoint, insecure=True
        )
        self.logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(otlp_log_exporter)
        )

        otlp_handler = LoggingHandler(
            logger_provider=self.logger_provider, level=logging.INFO
        )

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        )

        # 큐 핸들러
        self.log_queue = queue.Queue(maxsize=10_000)
        queue_handler = QueueHandler(self.log_queue)

        self.queue_listener = QueueListener(
            self.log_queue, otlp_handler, console_handler, respect_handler_level=True
        )
        self.queue_listener.start()

        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(queue_handler)

        # 트레이스, 스팬 정보 연동
        self.logging_instrumentor = LoggingInstrumentor()
        self.logging_instrumentor.instrument(
            set_logging_format=False, log_level=logging.INFO
        )

    def _init_metrics(self) -> None:
        """Initialize OTLP metrics exporter and define application metrics."""
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=self.otlp_grpc_endpoint, insecure=True),
            export_interval_millis=10_000,
        )
        meter_provider = MeterProvider(
            resource=self.resource, metric_readers=[metric_reader]
        )
        metrics.set_meter_provider(meter_provider)

        self.meter = metrics.get_meter(self.service_name)

    def _init_instrumentations(self) -> None:
        """Attach system, Redis, and Kafka instrumentors."""
        self.system_instrumentor = SystemMetricsInstrumentor()
        self.kafka_instrumentor = AIOKafkaInstrumentor()
        self.mongodb_instrumentor = PymongoInstrumentor()

        self.system_instrumentor.instrument()
        self.kafka_instrumentor.instrument()
        self.mongodb_instrumentor.instrument()

    async def stop(self) -> None:
        """Gracefully shut down all OTEL components."""
        self.system_instrumentor.uninstrument()
        self.kafka_instrumentor.uninstrument()
        self.mongodb_instrumentor.uninstrument()

        self.logger_provider.force_flush(timeout_millis=30_000)
        trace.get_tracer_provider().force_flush(timeout_millis=30_000)
        metrics.get_meter_provider().force_flush(timeout_millis=30_000)

        self.logger_provider.shutdown()
        trace.get_tracer_provider().shutdown()
        metrics.get_meter_provider().shutdown()

        if self.queue_listener:
            self.queue_listener.stop()

        logger.info("OTEL stopped")
