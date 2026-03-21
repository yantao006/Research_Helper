from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from research_batch.config import PROVIDERS, ProviderConfig


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def configure_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def first_present_env(names: tuple[str, ...]) -> tuple[str, str]:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return name, value
    raise RuntimeError(f"Missing required environment variable. Tried: {', '.join(names)}")


def is_truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def is_falsy(value: str) -> bool:
    return value.strip().lower() in {"0", "false", "no", "n", ""}


def resolve_provider(provider_id: str) -> ProviderConfig:
    try:
        return PROVIDERS[provider_id]
    except KeyError as exc:
        raise RuntimeError(f"Unsupported provider: {provider_id}") from exc


def resolve_model(provider: ProviderConfig, cli_model: str | None) -> str:
    if cli_model and cli_model.strip():
        return cli_model.strip()
    model_env_names = (
        "LLM_MODEL",
        f"{provider.provider_id.upper()}_MODEL",
        "OPENAI_MODEL",
    )
    for env_name in model_env_names:
        value = os.getenv(env_name, "").strip()
        if value:
            return value
    return provider.default_model


def resolve_api_base(provider: ProviderConfig) -> str:
    base_env_names = (
        "LLM_BASE_URL",
        f"{provider.provider_id.upper()}_BASE_URL",
        "OPENAI_BASE_URL",
    )
    for env_name in base_env_names:
        value = os.getenv(env_name, "").strip()
        if value:
            return value.rstrip("/")
    return provider.api_base.rstrip("/")


def is_production_env() -> bool:
    for name in ("RESEARCH_ENV", "APP_ENV", "ENV", "NODE_ENV"):
        value = os.getenv(name, "").strip().lower()
        if value in {"prod", "production"}:
            return True
    return False
