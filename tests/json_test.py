import time
import json
import orjson

# 테스트 데이터
data = {"message": "Hello from user_1 in room1, msg 1"}

# orjson 성능 테스트
start = time.time()
for _ in range(100_000):
    serialized = orjson.dumps(data)
    deserialized = orjson.loads(serialized)
print(f"orjson: {time.time() - start:.4f} seconds")

# json 성능 테스트
start = time.time()
for _ in range(100_000):
    serialized = json.dumps(data)
    deserialized = json.loads(serialized)
print(f"json: {time.time() - start:.4f} seconds")
