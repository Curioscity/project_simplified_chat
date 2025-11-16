FROM python:3.13-slim AS builder

# uv 설치
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# 작업 디렉토리 설정
WORKDIR /app

# 의존성 설치
COPY pyproject.toml uv.lock ./
RUN uv export --no-dev --format requirements-txt | uv pip sync --system --break-system-packages --no-cache -

# 환경변수
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# FastAPI 포트
EXPOSE 8002

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8002"]