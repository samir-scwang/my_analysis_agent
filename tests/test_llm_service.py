from __future__ import annotations

from types import SimpleNamespace

import app.services.llm_service as target_module
from app.services.llm_service import LLMService


class _FakeChunk:
    def __init__(self, content: str):
        self.choices = [SimpleNamespace(delta=SimpleNamespace(content=content))]


class _FakeCompletions:
    def create(self, *, stream: bool = False, **kwargs):
        assert stream is True
        assert kwargs["messages"][0]["role"] == "user"
        return [
            _FakeChunk("part-1"),
            _FakeChunk(""),
            _FakeChunk("part-2"),
        ]


class _FakeOpenAI:
    def __init__(self, *args, **kwargs):
        self.chat = SimpleNamespace(completions=_FakeCompletions())


def test_stream_invoke_yields_non_empty_delta_chunks(monkeypatch):
    monkeypatch.setattr(
        target_module,
        "settings",
        SimpleNamespace(
            llm_api_key="test-key",
            llm_model="test-model",
            llm_base_url="https://unit.test",
        ),
    )
    monkeypatch.setattr(target_module, "OpenAI", _FakeOpenAI)

    service = LLMService()

    chunks = list(service.stream_invoke([{"role": "user", "content": "hello"}]))

    assert chunks == ["part-1", "part-2"]
