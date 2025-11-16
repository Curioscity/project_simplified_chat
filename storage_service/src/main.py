import logging
from contextlib import asynccontextmanager
from datetime import datetime, UTC

from fastapi import FastAPI
from starlette import status
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from src.application.message_service import MessageService
from src.application.storage_processor import MessageStorageProcessor
from src.config import Settings
from src.infrastructure.kafka import (
    KafkaMessageConsumer,
    KafkaProducerAdapter,
)
from src.infrastructure.message_repository import MessageRepository
from src.infrastructure.otel import OTELManager

from src.routers.message import router as message_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Chat Application...")

    services_to_stop = []
    try:
        app.state.is_draining = False
        settings = Settings()
        app.state.settings = settings

        # OTel
        otel_manager = OTELManager(
            service_name=settings.OTEL_SERVICE_NAME,
            otlp_grpc_endpoint=settings.OTEL_OTLP_GRPC_ENDPOINT,
            otlp_http_endpoint=settings.OTEL_OTLP_HTTP_ENDPOINT,
        )
        app.state.otel_manager = otel_manager
        services_to_stop.append(otel_manager)

        # KafkaMessageConsumer
        kafka_message_consumer = KafkaMessageConsumer(
            settings.KAFKA_MESSAGE_CONSUMER_TOPIC,
            bootstrap_servers=settings.KAFKA_MESSAGE_CONSUMER_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_MESSAGE_CONSUMER_GROUP_ID,
        )
        await kafka_message_consumer.start()
        services_to_stop.append(kafka_message_consumer)
        app.state.kafka_message_consumer = kafka_message_consumer

        # KafkaProducerAdapter
        kafka_producer_adapter = KafkaProducerAdapter(
            bootstrap_servers=settings.KAFKA_PRODUCER_ADAPTER_BOOTSTRAP_SERVERS,
            acks=settings.KAFKA_PRODUCER_ADAPTER_ACKS,
            enable_idempotence=settings.KAFKA_PRODUCER_ADAPTER_ENABLE_IDEMPOTENCE,
            max_batch_size=settings.KAFKA_PRODUCER_ADAPTER_MAX_BATCH_SIZE,
            linger_ms=settings.KAFKA_PRODUCER_ADAPTER_LINGER_MS,
        )
        await kafka_producer_adapter.start()
        app.state.kafka_producer_adapter = kafka_producer_adapter
        services_to_stop.append(kafka_producer_adapter)

        # MessageRepository
        message_repository = MessageRepository(
            mongo_client_host=settings.MESSAGE_REPOSITORY_MONGO_CLIENT_HOST,
            mongo_client_max_pool_size=settings.MESSAGE_REPOSITORY_MONGO_CLIENT_MAX_POOL_SIZE,
            mongo_client_min_pool_size=settings.MESSAGE_REPOSITORY_MONGO_CLIENT_MIN_POOL_SIZE,
            server_selection_timeout_ms=settings.MESSAGE_REPOSITORY_SERVER_SELECTION_TIMEOUT_MS,
            db_name=settings.MESSAGE_REPOSITORY_DB_NAME,
        )
        await message_repository.start()
        app.state.message_repository = message_repository

        # MessageService
        message_service = MessageService(message_repository)
        app.state.message_service = message_service

        # MessageStorageProcessor
        message_storage_processor = MessageStorageProcessor(
            kafka_message_consumer=kafka_message_consumer,
            message_repository=message_repository,
            kafka_producer_adapter=kafka_producer_adapter,
            otel_manager=otel_manager,
        )
        await message_storage_processor.start()
        services_to_stop.append(message_storage_processor)
        app.state.message_storage_processor = message_storage_processor

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


app.include_router(message_router)


@app.get("/health")
async def health_check_liveness():
    return {"status": "ok", "timestamp": datetime.now(UTC).isoformat()}


@app.get("/readiness")
async def health_check_readiness(request: Request):
    if request.app.state.is_draining:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "shutting_down",
                "timestamp": datetime.now(UTC).isoformat(),
            },
        )

    return {"status": "ready", "timestamp": datetime.now(UTC).isoformat()}
