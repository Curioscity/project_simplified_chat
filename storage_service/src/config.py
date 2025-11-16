from pydantic.v1 import BaseSettings


class Settings(BaseSettings):
    # OTEL
    OTEL_SERVICE_NAME: str = "storage-service"
    OTEL_OTLP_GRPC_ENDPOINT: str = "otel-collector:4317"
    OTEL_OTLP_HTTP_ENDPOINT: str = "http://otel-collector:4318"

    # KafkaMessageConsumer
    KAFKA_MESSAGE_CONSUMER_TOPIC: str = "chat-messages"
    KAFKA_MESSAGE_CONSUMER_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_MESSAGE_CONSUMER_GROUP_ID: str = "chat-batch-v1"

    # KafkaProducerAdapter
    KAFKA_PRODUCER_ADAPTER_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_PRODUCER_ADAPTER_ACKS: int | str = "all"
    KAFKA_PRODUCER_ADAPTER_ENABLE_IDEMPOTENCE: bool = True
    KAFKA_PRODUCER_ADAPTER_MAX_BATCH_SIZE: int = 16384 * 10
    KAFKA_PRODUCER_ADAPTER_LINGER_MS: int = 10

    # MessageRepository
    MESSAGE_REPOSITORY_MONGO_CLIENT_HOST: str = "mongodb://mongodb:27017"
    MESSAGE_REPOSITORY_MONGO_CLIENT_MAX_POOL_SIZE: int = 50
    MESSAGE_REPOSITORY_MONGO_CLIENT_MIN_POOL_SIZE: int = 10
    MESSAGE_REPOSITORY_SERVER_SELECTION_TIMEOUT_MS: int = 5_000
    MESSAGE_REPOSITORY_DB_NAME: str = "chat"

    class Config:
        env_file = ".env"
