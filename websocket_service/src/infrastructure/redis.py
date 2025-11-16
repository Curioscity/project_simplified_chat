import logging

import redis.asyncio as redis
from orjson import orjson
from redis import RedisError

logger = logging.getLogger(__name__)


class RedisAdapter:
    def __init__(
        self,
        host: str = "redis",
        port: int = 6379,
        max_connections: int = 100,
        socket_timeout: int = 5,
    ):
        self.pool = redis.ConnectionPool(
            host=host,
            port=port,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            retry_on_timeout=True,
        )
        self.client = redis.Redis(connection_pool=self.pool)
        self.pubsub_client = redis.Redis(
            host=host,
            port=port,
            socket_timeout=socket_timeout,
            retry_on_timeout=True,
        )
        self.pubsub = self.pubsub_client.pubsub(ignore_subscribe_messages=True)

    async def start(self):
        await self.client.ping()
        await self.pubsub_client.ping()
        logger.info("Redis started")

    async def stop(self):
        await self.client.aclose()
        await self.client.connection_pool.disconnect()
        await self.pubsub.close()
        logger.info("Redis stopped")

    @staticmethod
    def get_channel(room_id: str) -> str:
        return f"chat:room:{room_id}"

    async def subscribe(self, room_id: str):
        try:
            await self.pubsub.subscribe(RedisAdapter.get_channel(room_id))
            return True
        except RedisError as e:
            logger.error(f"Failed to subscribe to room {room_id}: {e}")
            return False

    async def unsubscribe(self, room_id: str):
        try:
            await self.pubsub.unsubscribe(RedisAdapter.get_channel(room_id))
            return True
        except RedisError as e:
            logger.error(f"Failed to unsubscribe from room {room_id}: {e}")
            return False

    async def listen_pubsub(self):
        pubsub = self.pubsub
        await pubsub.subscribe("init_service")
        async for message in pubsub.listen():
            yield message

    @staticmethod
    def prepare_message(message: dict) -> tuple[str, bytes]:
        """publish, publish_batch에 넣기 위한 구조로 변환"""
        channel = RedisAdapter.get_channel(message["room_id"])

        message["type"] = "message"
        payload = orjson.dumps(message)
        return channel, payload

    async def publish(self, channel: str, payload: bytes):
        try:
            await self.client.publish(channel, payload)
            return True
        except RedisError as e:
            logger.error(f"Failed to publish message: {e}")
            return False

    async def publish_batch(self, batch: list[tuple[str, bytes]]):
        try:
            async with self.client.pipeline() as pipe:
                for channel, payload in batch:
                    pipe.publish(channel, payload)
                await pipe.execute()
            return True
        except RedisError as e:
            logger.error(f"Failed to publish message: {e}")
            return False
