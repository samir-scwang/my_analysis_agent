from __future__ import annotations

from typing import Any

from app.config import settings


def build_langchain_chat_model(
    *,
    temperature: float = 0.1,
    max_retries: int = 2,
    streaming: bool = False,
    **kwargs: Any,
):
    """
    为 deepagent / LangChain 构造 chat model。

    说明：
    - 这里不复用现有 LLMService，因为 LLMService 是基于 OpenAI SDK 的同步封装，
      而 deepagent / LangChain 更适合直接使用 LangChain 的 ChatModel。
    - 使用延迟导入，避免在未安装 langchain-openai 时影响整个项目其他模块。
    """
    if not settings.llm_api_key:
        raise ValueError("SICHENG_DEEPSEEK_API is not set.")

    try:
        from langchain_openai import ChatOpenAI
    except ImportError as e:
        raise ImportError(
            "langchain-openai is required for deepagent execution. "
            "Please install it with: pip install -U langchain-openai"
        ) from e

    return ChatOpenAI(
        model=settings.llm_model,
        api_key=settings.llm_api_key,
        base_url=settings.llm_base_url,
        timeout=settings.llm_timeout_seconds,
        temperature=temperature,
        max_retries=max_retries,
        streaming=streaming,
        **kwargs,
    )