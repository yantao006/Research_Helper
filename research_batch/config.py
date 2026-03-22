from __future__ import annotations

from dataclasses import dataclass
from typing import Any

DEFAULT_OUTPUT_ROOT = "output"
DEFAULT_LOG_FILE = "logs/research_helper.log"


@dataclass
class PromptRow:
    prompt_id: str
    question: str
    prompt: str


class NonRetryableAPIError(RuntimeError):
    pass


@dataclass(frozen=True)
class ProviderConfig:
    provider_id: str
    display_name: str
    api_base: str
    api_key_envs: tuple[str, ...]
    default_model: str
    api_style: str
    supports_web_search: bool
    supports_include_sources: bool


@dataclass(frozen=True)
class FeishuConfig:
    app_id: str
    app_secret: str
    app_token: str
    table_id: str
    summary_table_id: str = ""
    base_url: str = "https://open.feishu.cn"


@dataclass(frozen=True)
class SyncDoc:
    prompt_id: str
    question: str
    answer: str
    sources: list[str]
    output_path: str


@dataclass(frozen=True)
class FactPack:
    company: str
    ticker: str
    report_date: str
    provider_name: str
    model: str
    collected_at: str
    collected_with_web_search: bool
    payload: dict[str, Any]
    summary_markdown: str
    delta_summary_markdown: str = ""
    output_path: str = ""


REQUIRED_FEISHU_FIELDS = {
    "sync_key",
    "company",
    "ticker",
    "report_date",
    "prompt_id",
    "question",
    "answer_markdown",
    "sources",
    "provider",
    "model",
    "output_path",
    "synced_at",
}


REQUIRED_FEISHU_SUMMARY_FIELDS = {
    "run_key",
    "company",
    "ticker",
    "report_date",
    "industry",
    "fact_pack_summary",
    "delta_summary",
    "coverage_score",
    "confidence",
    "key_financials",
    "recent_catalysts",
    "valuation_snapshot",
    "top_risks",
    "tracking_items",
    "output_dir",
    "synced_at",
}


PROVIDERS: dict[str, ProviderConfig] = {
    "openai": ProviderConfig(
        provider_id="openai",
        display_name="OpenAI",
        api_base="https://api.openai.com/v1",
        api_key_envs=("OPENAI_API_KEY",),
        default_model="gpt-5.2",
        api_style="responses",
        supports_web_search=True,
        supports_include_sources=True,
    ),
    "siliconflow": ProviderConfig(
        provider_id="siliconflow",
        display_name="SiliconFlow",
        api_base="https://api.siliconflow.com/v1",
        api_key_envs=("SILICONFLOW_API_KEY",),
        default_model="Qwen/Qwen2.5-72B-Instruct",
        api_style="chat_completions",
        supports_web_search=False,
        supports_include_sources=False,
    ),
    "modelscope": ProviderConfig(
        provider_id="modelscope",
        display_name="ModelScope",
        api_base="https://api-inference.modelscope.cn/v1",
        api_key_envs=("MODELSCOPE_API_KEY", "MODELSCOPE_SDK_TOKEN"),
        default_model="Qwen/Qwen3-32B",
        api_style="chat_completions",
        supports_web_search=False,
        supports_include_sources=False,
    ),
    "qwen": ProviderConfig(
        provider_id="qwen",
        display_name="Alibaba Cloud Bailian / Qwen",
        api_base="https://dashscope.aliyuncs.com/compatible-mode/v1",
        api_key_envs=("DASHSCOPE_API_KEY",),
        default_model="qwen-plus",
        api_style="chat_completions",
        supports_web_search=False,
        supports_include_sources=False,
    ),
    "doubao": ProviderConfig(
        provider_id="doubao",
        display_name="Volcengine / Doubao",
        api_base="https://ark.cn-beijing.volces.com/api/v3",
        api_key_envs=("ARK_API_KEY",),
        default_model="doubao-seed-1-6-250615",
        api_style="responses",
        supports_web_search=True,
        supports_include_sources=False,
    ),
    "zhipu": ProviderConfig(
        provider_id="zhipu",
        display_name="Zhipu / GLM",
        api_base="https://open.bigmodel.cn/api/paas/v4",
        api_key_envs=("ZAI_API_KEY",),
        default_model="glm-5",
        api_style="chat_completions",
        supports_web_search=False,
        supports_include_sources=False,
    ),
}
