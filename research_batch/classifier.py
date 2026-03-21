from __future__ import annotations

import json
import logging
import re
import urllib.error
from dataclasses import dataclass
from typing import Any

from research_batch.config import ProviderConfig
from research_batch.llm import (
    call_chat_completions,
    call_openai,
    extract_chat_completion_text,
    extract_output_text,
)
from research_batch.router import RouterConfig


@dataclass(frozen=True)
class IndustryClassification:
    industry_id: str | None
    secondary_industry_ids: list[str]
    industry_weights: dict[str, float]
    confidence: float
    method: str
    reason: str
    company_type: str
    manual_review: bool


def _clamp_confidence(value: float) -> float:
    return max(0.0, min(1.0, value))


def _normalize_company_type(raw: str) -> str:
    text = raw.strip().lower()
    if "conglomerate" in text or "holding" in text:
        return "conglomerate"
    if "multi" in text or "segment" in text:
        return "multi_segment"
    if "single" in text:
        return "single_industry"
    return "uncertain"


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


def _normalize_weight(value: Any) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return 0.0
    if num > 1:
        num = num / 100.0
    return max(0.0, num)


def _normalize_weight_map(weights: dict[str, float]) -> dict[str, float]:
    cleaned: dict[str, float] = {}
    for key, value in weights.items():
        if value <= 0:
            continue
        cleaned[key] = value
    total = sum(cleaned.values())
    if total <= 0:
        return {}
    return {key: value / total for key, value in cleaned.items()}


def _extract_industry_weights(
    *,
    router: RouterConfig,
    payload: dict[str, object],
) -> dict[str, float]:
    raw_weights = payload.get("industry_weights")
    weights: dict[str, float] = {}
    if isinstance(raw_weights, dict):
        for raw_id, raw_weight in raw_weights.items():
            industry_id = str(raw_id).strip().lower()
            if industry_id not in router.categories:
                continue
            weight = _normalize_weight(raw_weight)
            if weight > 0:
                weights[industry_id] = weight

    raw_industries = payload.get("industries")
    if isinstance(raw_industries, list):
        for item in raw_industries:
            if not isinstance(item, dict):
                continue
            industry_id = str(item.get("id") or item.get("industry_id") or "").strip().lower()
            if industry_id not in router.categories:
                continue
            weight = _normalize_weight(item.get("weight"))
            if weight > 0:
                weights[industry_id] = max(weights.get(industry_id, 0.0), weight)
    return _normalize_weight_map(weights)


def _extract_industry_candidates(
    *,
    router: RouterConfig,
    payload: dict[str, object],
) -> tuple[str | None, list[str], dict[str, float]]:
    primary = str(payload.get("primary_industry_id") or payload.get("industry_id") or "").strip().lower() or None
    if primary and primary not in router.categories:
        primary = None

    secondary_ids: list[str] = []
    raw_secondary = payload.get("secondary_industry_ids")
    if isinstance(raw_secondary, list):
        for raw in raw_secondary:
            industry_id = str(raw).strip().lower()
            if (
                industry_id
                and industry_id in router.categories
                and industry_id != primary
                and industry_id not in secondary_ids
            ):
                secondary_ids.append(industry_id)

    weights = _extract_industry_weights(router=router, payload=payload)
    if primary and primary not in weights:
        if not weights:
            weights[primary] = 1.0
        else:
            weights[primary] = max(0.01, max(weights.values()) * 0.8)

    if not primary and weights:
        primary = max(weights.items(), key=lambda item: item[1])[0]
    if not weights and primary:
        if secondary_ids:
            primary_weight = 0.7
            remainder = 0.3 / len(secondary_ids)
            weights = {primary: primary_weight}
            for industry_id in secondary_ids:
                weights[industry_id] = remainder
        else:
            weights = {primary: 1.0}

    weights = _normalize_weight_map(weights)
    ranked = sorted(weights.items(), key=lambda item: item[1], reverse=True)
    if ranked:
        primary = ranked[0][0]
        secondary_ids = [industry_id for industry_id, _ in ranked[1:]]

    return primary, secondary_ids, weights


def _keyword_classification(
    *,
    router: RouterConfig,
    company: str,
    ticker: str,
) -> tuple[str | None, float, str]:
    text = f"{company} {ticker}".lower()
    best_category: str | None = None
    best_score = 0
    reason = "no keyword matched"

    for category in router.categories.values():
        positive_hits = [kw for kw in category.positive_keywords if kw and kw in text]
        exclude_hits = [kw for kw in category.exclude_keywords if kw and kw in text]
        if exclude_hits:
            continue
        score = len(positive_hits)
        if score > best_score:
            best_score = score
            best_category = category.category_id
            reason = (
                f"keyword matched: {', '.join(positive_hits[:4])}"
                if positive_hits
                else "keyword weak match"
            )

    if best_category is None:
        return None, 0.0, reason
    confidence = 0.7 if best_score >= 2 else 0.56
    return best_category, confidence, reason


def _build_classification_prompt(*, router: RouterConfig, company: str, ticker: str) -> str:
    categories = []
    for category in router.categories.values():
        categories.append(
            f"- {category.category_id}: {category.label}; description={category.description}; "
            f"keywords={', '.join(category.positive_keywords[:8])}"
        )

    category_block = "\n".join(categories)
    return (
        "你是上市公司行业分类器。请根据公司主营业务和价值链位置进行分类。\n"
        "仅允许使用以下行业ID：\n"
        f"{category_block}\n\n"
        "输出必须是严格 JSON，字段如下：\n"
        '{"primary_industry_id":"<id或null>","secondary_industry_ids":["<id>"],'
        '"industry_weights":{"<id>":0.0},"confidence":0.0,'
        '"company_type":"single_industry|multi_segment|conglomerate|uncertain","reason":"<简短原因>"}\n'
        "industry_weights 的权重和应接近 1.0；若无法判断次行业，可返回空数组。\n"
        "不要输出任何额外文本。\n\n"
        f"公司名称: {company}\n"
        f"Ticker: {ticker}\n"
    )


def classify_company_industry(
    *,
    router: RouterConfig,
    company: str,
    ticker: str,
    industry_override: str | None,
    allow_llm: bool,
    provider: ProviderConfig | None,
    api_key: str | None,
    api_base: str | None,
    model: str | None,
    request_timeout: int,
) -> IndustryClassification:
    if industry_override:
        overridden = industry_override.strip().lower()
        if overridden not in router.categories:
            raise ValueError(f"industry override not found in router categories: {overridden}")
        return IndustryClassification(
            industry_id=overridden,
            secondary_industry_ids=[],
            industry_weights={overridden: 1.0},
            confidence=1.0,
            method="override",
            reason="industry override from CLI",
            company_type="single_industry",
            manual_review=False,
        )

    if allow_llm and provider and api_key and api_base and model:
        try:
            prompt = _build_classification_prompt(router=router, company=company, ticker=ticker)
            response_json = (
                call_openai(
                    api_key=api_key,
                    api_base=api_base,
                    model=model,
                    prompt_text=prompt,
                    timeout=request_timeout,
                    enable_web_search=False,
                    include_sources=False,
                )
                if provider.api_style == "responses"
                else call_chat_completions(
                    api_key=api_key,
                    api_base=api_base,
                    model=model,
                    prompt_text=prompt,
                    timeout=request_timeout,
                )
            )
            raw = (
                extract_output_text(response_json)
                if provider.api_style == "responses"
                else extract_chat_completion_text(response_json)
            )
            payload = _extract_json_payload(raw)
            if payload:
                industry_id, secondary_ids, industry_weights = _extract_industry_candidates(
                    router=router,
                    payload=payload,
                )
                confidence = _clamp_confidence(float(payload.get("confidence") or 0.0))
                reason = str(payload.get("reason") or "").strip() or "classified by llm"
                company_type = _normalize_company_type(str(payload.get("company_type") or "uncertain"))
                if company_type == "uncertain":
                    if len(industry_weights) >= 2:
                        company_type = "multi_segment"
                    elif industry_id:
                        company_type = "single_industry"
                manual_review = confidence < router.medium_confidence_threshold
                return IndustryClassification(
                    industry_id=industry_id,
                    secondary_industry_ids=secondary_ids,
                    industry_weights=industry_weights,
                    confidence=confidence,
                    method="llm",
                    reason=reason,
                    company_type=company_type,
                    manual_review=manual_review,
                )
            logging.warning("Industry classification fallback to keyword: invalid llm json output")
        except (urllib.error.HTTPError, urllib.error.URLError, ValueError, TypeError):
            logging.exception("Industry classification fallback to keyword due to llm call failure")

    industry_id, confidence, reason = _keyword_classification(
        router=router,
        company=company,
        ticker=ticker,
    )
    manual_review = confidence < router.medium_confidence_threshold
    keyword_weights = {industry_id: 1.0} if industry_id else {}
    return IndustryClassification(
        industry_id=industry_id,
        secondary_industry_ids=[],
        industry_weights=keyword_weights,
        confidence=_clamp_confidence(confidence),
        method="keyword",
        reason=reason,
        company_type="uncertain",
        manual_review=manual_review,
    )
