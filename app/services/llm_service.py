from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Callable, Dict, Iterator, Type

from openai import OpenAI
from pydantic import BaseModel

from app.config import settings


class LLMService:
    def __init__(self) -> None:
        if not settings.llm_api_key:
            raise ValueError("SICHENG_DEEPSEEK_API is not set.")

        self.client = OpenAI(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
        )
        self.model = settings.llm_model

    def _normalize_messages(self, messages: list[Any]) -> list[dict[str, str]]:
        normalized: list[dict[str, str]] = []

        for msg in messages:
            role = getattr(msg, "type", None) or getattr(msg, "role", None)
            content = getattr(msg, "content", None)

            if role == "human":
                role = "user"
            elif role == "ai":
                role = "assistant"
            elif role not in {"system", "user", "assistant"}:
                role = "user"

            normalized.append(
                {
                    "role": role,
                    "content": str(content or ""),
                }
            )

        return normalized

    def text_invoke(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
        )
        return response.choices[0].message.content or ""

    def json_invoke(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> Dict[str, Any]:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": (
                        user_prompt
                        + "\n\n请只输出一个 JSON 对象，不要输出 markdown，不要输出解释。"
                    ),
                },
            ],
            temperature=temperature,
            response_format={"type": "json_object"},
        )

        text = response.choices[0].message.content or "{}"
        return json.loads(text)

    def invoke(
        self,
        messages: list[Any],
        temperature: float = 0.2,
    ) -> Any:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=self._normalize_messages(messages),
            temperature=temperature,
        )
        content = response.choices[0].message.content or ""
        return SimpleNamespace(content=content)

    def stream_invoke(
        self,
        messages: list[Any],
        temperature: float = 0.2,
    ) -> Iterator[str]:
        stream = self.client.chat.completions.create(
            model=self.model,
            messages=self._normalize_messages(messages),
            temperature=temperature,
            stream=True,
        )

        for chunk in stream:
            choices = getattr(chunk, "choices", None) or []
            if not choices:
                continue

            delta = getattr(choices[0], "delta", None)
            content = getattr(delta, "content", None) or ""
            if content:
                yield content

    def with_structured_output(
        self,
        schema: Type[BaseModel],
        temperature: float = 0.2,
    ) -> StructuredLLMWrapper:
        return StructuredLLMWrapper(
            client=self.client,
            model=self.model,
            schema=schema,
            temperature=temperature,
            normalizer=self._normalize_messages,
        )


class StructuredLLMWrapper:
    def __init__(
        self,
        *,
        client: OpenAI,
        model: str,
        schema: Type[BaseModel],
        temperature: float,
        normalizer: Callable[[list[Any]], list[dict[str, str]]],
    ) -> None:
        self.client = client
        self.model = model
        self.schema = schema
        self.temperature = temperature
        self.normalizer = normalizer

    def invoke(self, messages: list[Any]) -> BaseModel:
        schema_json = json.dumps(self.schema.model_json_schema(), ensure_ascii=False)

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你必须严格按照指定 JSON Schema 输出一个 JSON 对象。"
                        "不要输出 markdown，不要输出解释，不要输出额外文本。"
                    ),
                },
                *self.normalizer(messages),
                {
                    "role": "system",
                    "content": f"JSON Schema:\n{schema_json}",
                },
            ],
            temperature=self.temperature,
            response_format={"type": "json_object"},
        )

        text = response.choices[0].message.content or "{}"
        data = json.loads(text)
        return self.schema.model_validate(data)


if __name__ == "__main__":
    llm = LLMService()
    print(
        llm.text_invoke(
            system_prompt="你是一个私人小助手",
            user_prompt="你好",
            temperature=0.2,
        )
    )
