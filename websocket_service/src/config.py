from pydantic.v1 import BaseSettings


class Settings(BaseSettings):
    # OTEL
    OTEL_SERVICE_NAME: str = "websocket-service"
    OTEL_OTLP_GRPC_ENDPOINT: str = "otel-collector:4317"
    OTEL_OTLP_HTTP_ENDPOINT: str = "http://otel-collector:4318"

    # Redis
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_MAX_CONNECTIONS: int = 40

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_ACKS: int | str = "all"
    KAFKA_ENABLE_IDEMPOTENCE: bool = True
    KAFKA_MAX_BATCH_SIZE: int = 16384 * 10
    KAFKA_LINGER_MS: int = 10

    # Batch Publisher
    BATCH_PUBLISHER_FLUSH_INTERVAL: float = 0.005
    BATCH_PUBLISHER_MAX_QUEUE_SIZE: int = 10_000
    BATCH_PUBLISHER_MAX_BATCH_SIZE: int = 50
    BATCH_PUBLISHER_NUM_PARTITIONS: int = 5

    # Receive Broadcaster
    BROADCASTER_MAX_QUEUE_SIZE: int = 1000
    BROADCASTER_QUEUE_PUT_TIMEOUT: float = 0.1
    BROADCASTER_ROOM_WORKER_MAX_BATCH_SIZE: int = 50

    # ROUTER
    MAX_MESSAGE_SIZE: int = 10 * 1024  # 10KB

    # Connection Manager
    CONNECTION_MANAGER_MAX_TOTAL_CONNECTIONS: int = 3000
    CONNECTION_MANAGER_NUM_LOCKS: int = 256

    class Config:
        env_file = "/websocket_service/.env"
