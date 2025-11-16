import logging
from datetime import datetime, UTC

from fastapi import FastAPI
from contextlib import asynccontextmanager

from starlette import status
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from src.application.chat_service import ChatService
from src.application.connection_manager import ConnectionManager
from src.application.broadcaster import Broadcaster
from src.application.batch_publisher import BatchPublisher
from src.config import Settings
from src.infrastructure.kafka import KafkaProducerAdapter
from src.infrastructure.otel import OTELManager
from src.infrastructure.redis import RedisAdapter
from src.routers.websocket import router as websocket_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Chat Application...")

    services_to_stop = []
    try:
        app.state.is_draining = False
        settings = Settings()
        app.state.settings = settings

        # Infra
        # OTel
        otel_manager = OTELManager(
            service_name=settings.OTEL_SERVICE_NAME,
            otlp_grpc_endpoint=settings.OTEL_OTLP_GRPC_ENDPOINT,
            otlp_http_endpoint=settings.OTEL_OTLP_HTTP_ENDPOINT,
        )
        app.state.otel_manager = otel_manager
        services_to_stop.append(otel_manager)

        # Redis
        redis_adapter = RedisAdapter(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
        )
        await redis_adapter.start()
        app.state.redis_adapter = redis_adapter
        services_to_stop.append(redis_adapter)

        # Kafka
        kafka_producer_adapter = KafkaProducerAdapter(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            acks=settings.KAFKA_ACKS,
            enable_idempotence=settings.KAFKA_ENABLE_IDEMPOTENCE,
            max_batch_size=settings.KAFKA_MAX_BATCH_SIZE,
            linger_ms=settings.KAFKA_LINGER_MS,
        )
        await kafka_producer_adapter.start()
        app.state.kafka_producer_adapter = kafka_producer_adapter
        services_to_stop.append(kafka_producer_adapter)

        # Application
        # Connection Manager
        conn_manager = ConnectionManager(
            otel_manager=otel_manager,
            num_locks=settings.CONNECTION_MANAGER_NUM_LOCKS,
            max_total_connections=settings.CONNECTION_MANAGER_MAX_TOTAL_CONNECTIONS,
        )
        await conn_manager.start()
        app.state.conn_manager = conn_manager
        services_to_stop.append(conn_manager)

        # Batch Publisher
        batch_publisher = BatchPublisher(
            otel_manager=otel_manager,
            redis_adapter=redis_adapter,
            flush_interval=settings.BATCH_PUBLISHER_FLUSH_INTERVAL,
            max_queue_size=settings.BATCH_PUBLISHER_MAX_QUEUE_SIZE,
            max_batch_size=settings.BATCH_PUBLISHER_MAX_BATCH_SIZE,
            num_partitions=settings.BATCH_PUBLISHER_NUM_PARTITIONS,
        )
        await batch_publisher.start()
        app.state.batch_publisher = batch_publisher
        services_to_stop.append(batch_publisher)

        # Broadcaster
        broadcaster = Broadcaster(
            otel_manager=otel_manager,
            conn_manager=conn_manager,
            redis_adapter=redis_adapter,
            max_queue_size=settings.BROADCASTER_MAX_QUEUE_SIZE,
            queue_put_timeout=settings.BROADCASTER_QUEUE_PUT_TIMEOUT,
        )
        await broadcaster.start()
        app.state.broadcaster = broadcaster
        services_to_stop.append(broadcaster)

        # Chat Service
        chat_service = ChatService(
            otel_manager=otel_manager,
            conn_manager=conn_manager,
            batch_publisher=batch_publisher,
            kafka_producer_adapter=kafka_producer_adapter,
            redis_adapter=redis_adapter,
        )
        app.state.chat_service = chat_service

        logger.info("Application started successfully!")

        yield

    except Exception as e:
        logger.critical(f"Startup failed: {e}", exc_info=True)
        raise

    finally:
        logger.info("Shutting down...")
        app.state.is_draining = True

        for service in reversed(services_to_stop):
            try:
                await service.stop()
            except Exception as e:
                logger.error(f"Error stopping service: {e}", exc_info=True)

        logger.info("Shutdown complete")


app = FastAPI(lifespan=lifespan)

# 미들웨어
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware)

# 라우터
app.include_router(websocket_router)


@app.get("/health")
async def health_check_liveness():
    return {"status": "ok", "timestamp": datetime.now(UTC).isoformat()}


@app.get("/readiness")
async def health_check_readiness(request: Request):
    """
    안정적인 서버 종료 (또는 배포)를 위한 헬스체크
    TODO 연동된 서비스 체크 로직 추갸
    """
    if request.app.state.is_draining:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "shutting_down",
                "timestamp": datetime.now(UTC).isoformat(),
            },
        )

    return {"status": "ready", "timestamp": datetime.now(UTC).isoformat()}
