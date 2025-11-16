import pytest
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from starlette.testclient import TestClient

from src.application.chat_service import ChatService
from src.routers.websocket import router


@pytest.fixture(scope="module")
def test_app():
    """테스트용 FastAPI 앱"""
    app = FastAPI()
    app.include_router(router)

    # Mock settings and state
    app.state.is_draining = False
    app.state.settings = MagicMock()
    app.state.settings.MAX_MESSAGE_SIZE = 1024

    # Mock ChatService
    mock_chat_service = AsyncMock(spec=ChatService)
    mock_chat_service.connect = AsyncMock()
    mock_chat_service.disconnect = AsyncMock()
    mock_chat_service.handle_message = AsyncMock()
    mock_chat_service.send_error_response = AsyncMock()
    app.state.chat_service = mock_chat_service

    return app


@pytest.fixture(scope="module")
def client(test_app):
    """TestClient 인스턴스"""
    with TestClient(test_app) as client:
        yield client
