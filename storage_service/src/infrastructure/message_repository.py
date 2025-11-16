import logging
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError

logger = logging.getLogger(__name__)


class MessageRepository:
    """채팅 메시지 저장소"""

    def __init__(
        self,
        mongo_client_host: str = "mongodb://mongodb:27017",
        mongo_client_max_pool_size: int = 50,
        mongo_client_min_pool_size: int = 10,
        server_selection_timeout_ms: int = 5_000,
        db_name: str = "chat",
    ):
        self._mongo_client = AsyncIOMotorClient(
            mongo_client_host,
            maxPoolSize=mongo_client_max_pool_size,
            minPoolSize=mongo_client_min_pool_size,
            serverSelectionTimeoutMS=server_selection_timeout_ms,
        )
        self.collection = self._mongo_client[db_name].messages

    async def start(self):
        """컬렉션 및 인덱스 초기화"""
        await self._mongo_client.admin.command("ping")
        await self.collection.create_index([("room_id", 1), ("timestamp", -1)])
        logger.info(f"MessageRepository started")

    async def stop(self):
        self._mongo_client.close()
        logger.info(f"MessageRepository stopped")

    async def save_messages(self, messages: list[dict]) -> dict:
        if not messages:
            return {"inserted": 0, "duplicates": 0}

        try:
            result = await self.collection.insert_many(messages, ordered=False)
            return {"inserted": len(result.inserted_ids), "duplicates": 0}

        except BulkWriteError as e:
            duplicates = 0
            failed_messages = []
            for err in e.details.get("writeErrors", []):
                if err.get("code") == 11000:  # TODO 하드코드 된 부분 처리 필요
                    duplicates += 1
                else:
                    failed_messages.append(messages[err["index"]])

            return {
                "inserted": len(messages) - len(e.details.get("writeErrors", [])),
                "duplicates": duplicates,
                "failed_messages": failed_messages,
            }

        except Exception as e:
            logger.error(f"Failed to save messages: {e}", exc_info=e)
            return {"inserted": 0, "duplicates": 0, "failed_messages": messages}

    async def get_room_messages(
        self,
        room_id: str,
        limit: int = 30,
        last_message_timestamp: str | None = None,
    ) -> tuple[list[dict], bool]:
        """
        방의 메시지 조회 (페이지네이션)

        Args:
            room_id: 방 ID
            limit: 조회할 메시지 수
            last_message_timestamp: 이 시간 이전의 메시지만 조회 (커서 페이지네이션)

        Returns:
            (messages, is_last): 메시지 리스트와 마지막 페이지 여부
        """
        query = {"room_id": room_id}

        if last_message_timestamp:
            try:
                dt = datetime.fromisoformat(
                    last_message_timestamp.replace("Z", "+00:00")
                )
                query["timestamp"] = {"$lt": dt.isoformat()}
            except (ValueError, AttributeError) as e:
                raise ValueError(
                    f"Invalid timestamp format: {last_message_timestamp}"
                ) from e

        cursor = self.collection.find(query).sort("timestamp", -1).limit(limit + 1)
        messages = await cursor.to_list(limit + 1)

        is_last = len(messages) <= limit
        if not is_last:
            messages = messages[:limit]

        return messages, is_last
