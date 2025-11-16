import asyncio
import redis.asyncio as redis


async def test_dynamic_subscribe():
    """listen 중에 동적 subscribe가 가능한지 테스트"""

    client = redis.Redis(host="localhost", port=6379, db=0)
    pubsub = client.pubsub()

    received_messages = []

    async def listen_loop():
        """백그라운드 listen"""
        print("Listen loop started")
        async for message in pubsub.listen():
            msg_type = message.get("type")
            print(f"📨 Received: {msg_type} - {message}")

            if msg_type == "message":
                received_messages.append(message)

    # 1. Listen 시작 (백그라운드)
    listen_task = asyncio.create_task(listen_loop())
    await asyncio.sleep(0.1)

    # 2. 채널 A 구독 (listen 실행 중!)
    print("\nSubscribing to channel_a while listening...")
    await pubsub.subscribe("channel_a")
    await asyncio.sleep(0.1)

    # 3. 채널 A로 메시지 발행
    print("\nPublishing to channel_a...")
    await client.publish("channel_a", "Hello from A")
    await asyncio.sleep(0.1)

    # 4. 채널 B 추가 구독 (여전히 listen 실행 중!)
    print("\nSubscribing to channel_b while listening...")
    await pubsub.subscribe("channel_b")
    await asyncio.sleep(0.1)

    # 5. 채널 B로 메시지 발행
    print("\nPublishing to channel_b...")
    await client.publish("channel_b", "Hello from B")
    await asyncio.sleep(0.1)

    # 6. 채널 A 구독 해제 (여전히 listen 실행 중!)
    print("\nUnsubscribing from channel_a while listening...")
    await pubsub.unsubscribe("channel_a")
    await asyncio.sleep(0.1)

    # 7. 채널 A로 다시 발행 (받으면 안됨)
    print("\nPublishing to channel_a again (should not receive)...")
    await client.publish("channel_a", "Should not receive this")
    await asyncio.sleep(0.1)

    # 8. 채널 B로 발행 (받아야 함)
    print("\nPublishing to channel_b again (should receive)...")
    await client.publish("channel_b", "Should receive this")
    await asyncio.sleep(0.1)

    # 정리
    listen_task.cancel()
    try:
        await listen_task
    except asyncio.CancelledError:
        pass

    await pubsub.aclose()
    await client.aclose()

    # 결과 확인
    print("\n" + "=" * 50)
    print(f"Received {len(received_messages)} messages:")
    for msg in received_messages:
        channel = msg["channel"].decode("utf-8")
        data = msg["data"].decode("utf-8")
        print(f"  - {channel}: {data}")

if __name__ == "__main__":
    asyncio.run(test_dynamic_subscribe())
