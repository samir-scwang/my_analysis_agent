from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()
a = 1
def _get_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # -------------------------
    # Base LLM settings
    # -------------------------
    llm_api_key: str | None = os.getenv("SICHENG_DEEPSEEK_API")
    llm_model: str = os.getenv("LLM_MODEL", "deepseek-chat")
    llm_base_url: str = os.getenv("LLM_BASE_URL", "https://api.deepseek.com")
    llm_timeout_seconds: float = float(os.getenv("LLM_TIMEOUT_SECONDS", "60"))
    llm_enable_refine: bool = _get_bool("LLM_ENABLE_REFINE", "true")

    # -------------------------
    # DeepAgent settings
    # -------------------------
    deepagent_backend: str = os.getenv("DEEPAGENT_BACKEND", "local_shell").strip()
    deepagent_enable_fallback: bool = _get_bool("DEEPAGENT_ENABLE_FALLBACK", "true")
    deepagent_workspace_base_dir: str | None = os.getenv("DEEPAGENT_WORKSPACE_BASE_DIR")
    deepagent_skills_dir: str | None = os.getenv("DEEPAGENT_SKILLS_DIR")
    deepagent_max_steps: int = int(os.getenv("DEEPAGENT_MAX_STEPS", "25"))
    deepagent_verbose: bool = _get_bool("DEEPAGENT_VERBOSE", "false")


settings = Settings()

if __name__ == "__main__":
    print(settings)
