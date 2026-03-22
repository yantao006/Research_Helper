from __future__ import annotations

import json
import logging
import re
import urllib.error
from urllib.parse import quote

from research_batch.config import ProviderConfig
from research_batch.llm import (
    call_chat_completions,
    call_openai,
    extract_chat_completion_text,
    extract_output_text,
)


def _build_keyword_extraction_prompt(
    *,
    company: str,
    ticker: str,
    question: str,
    answer: str,
    max_keywords: int,
) -> str:
    clipped = answer.strip()
    if len(clipped) > 12000:
        clipped = clipped[:12000]

    return (
        "你是资深证券内容编辑。请从下面的调研内容中提取对搜索引擎和站内检索都高价值的关键词。\n"
        "关键词优先级：公司核心产品/品牌、关键技术、关键财务指标、行业术语、重要事件。\n"
        "要求：\n"
        f"- 输出 4 到 {max_keywords} 个关键词。\n"
        "- 避免过于泛化的词（如 公司、行业、增长、风险）。\n"
        "- 关键词尽量短，建议 2~12 个字符。\n"
        "- 可以包含英文缩写（如 IP、GMV、ROE），但不要输出整句。\n"
        '- 仅输出严格 JSON：{"keywords":["词1","词2"]}，不要额外文字。\n\n'
        f"公司：{company}\n"
        f"代码：{ticker}\n"
        f"题目：{question}\n"
        "调研内容：\n"
        f"{clipped}\n"
    )


def _extract_json_payload(text: str) -> dict[str, object] | None:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start < 0 or end <= start:
        return None
    candidate = cleaned[start : end + 1]
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _normalize_keyword(raw: str) -> str:
    keyword = raw.strip()
    keyword = keyword.strip("，。；：、,.!?()[]{}\"'`")
    keyword = re.sub(r"\s+", " ", keyword)
    if len(keyword) < 2:
        return ""
    if len(keyword) > 32:
        return ""
    if "http://" in keyword or "https://" in keyword:
        return ""
    return keyword


def extract_seo_keywords(
    *,
    provider: ProviderConfig,
    api_key: str,
    api_base: str,
    model: str,
    company: str,
    ticker: str,
    question: str,
    answer: str,
    request_timeout: int,
    keyword_limit: int,
) -> list[str]:
    max_keywords = max(4, min(keyword_limit, 20))
    if not answer.strip():
        return []

    prompt_text = _build_keyword_extraction_prompt(
        company=company,
        ticker=ticker,
        question=question,
        answer=answer,
        max_keywords=max_keywords,
    )

    try:
        response_json = (
            call_openai(
                api_key=api_key,
                api_base=api_base,
                model=model,
                prompt_text=prompt_text,
                timeout=request_timeout,
                enable_web_search=False,
                include_sources=False,
            )
            if provider.api_style == "responses"
            else call_chat_completions(
                api_key=api_key,
                api_base=api_base,
                model=model,
                prompt_text=prompt_text,
                timeout=request_timeout,
            )
        )
        raw = (
            extract_output_text(response_json)
            if provider.api_style == "responses"
            else extract_chat_completion_text(response_json)
        )
        payload = _extract_json_payload(raw)
        if not payload:
            logging.warning("SEO keyword extraction: invalid JSON response")
            return []

        values = payload.get("keywords")
        if not isinstance(values, list):
            logging.warning("SEO keyword extraction: JSON missing keywords list")
            return []

        seen: set[str] = set()
        result: list[str] = []
        company_key = company.strip().casefold()
        for value in values:
            keyword = _normalize_keyword(str(value))
            if not keyword:
                continue
            fold = keyword.casefold()
            if fold in seen:
                continue
            if company_key and fold == company_key:
                continue
            seen.add(fold)
            result.append(keyword)
            if len(result) >= keyword_limit:
                break
        return result
    except (urllib.error.HTTPError, urllib.error.URLError, ValueError, TypeError):
        logging.exception(
            "SEO keyword extraction failed company=%s ticker=%s question=%s",
            company,
            ticker,
            question,
        )
        return []


def append_seo_keyword_links(answer: str, keywords: list[str]) -> str:
    content = answer.strip()
    if not keywords:
        return content

    lines = [content] if content else []
    lines.extend(["", "## SEO 关键词", ""])
    for keyword in keywords:
        safe_label = keyword.replace("[", "\\[").replace("]", "\\]")
        lines.append(f"- [{safe_label}](/topic/{quote(keyword)})")
    return "\n".join(lines).strip()
