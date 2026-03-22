from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
from datetime import datetime

from research_batch.config import FactPack, ProviderConfig
from research_batch.llm import (
    call_chat_completions,
    call_openai,
    classify_http_error,
    extract_chat_completion_text,
    extract_output_text,
    is_tool_not_open_error,
)


def _build_fact_collection_prompt(*, company: str, ticker: str, report_date: str) -> str:
    return (
        "你是一名负责上市公司基础事实采集的研究助理。"
        "请基于截至指定日期可获得的公开信息，整理一份结构化 company fact pack。"
        "重点覆盖：公司概况、行业与竞争、商业模式、管理层与资本配置、最新公告/财报、关键财务指标、近期新闻与催化剂、估值与市场数据、核心风险、后续跟踪指标。"
        "如果信息缺失，请明确写入 missing_fields，不要编造。"
        "如果某项属于推断，请在 note 或 summary 中标明“推断”。\n\n"
        "输出要求：\n"
        "1. 仅输出严格 JSON，不要输出 markdown，不要加解释。\n"
        "2. 顶层字段必须包含：profile, industry_and_competition, business_model, management_and_capital_allocation, filings, news_and_catalysts, valuation_and_market, risks, tracking, sources, quality。\n"
        "3. sources 为数组，每项包含 title/url/published_at/source_type。\n"
        "4. quality 至少包含 coverage_score, missing_fields, confidence。\n"
        "5. 数字和日期尽量保留原始口径，不要擅自换算。\n\n"
        f"公司：{company}\n"
        f"代码：{ticker}\n"
        f"截至日期：{report_date}\n\n"
        'JSON 结构参考：{"profile":{"company_name_zh":"","company_name_en":"","exchange":"","industry":"","business_summary":""},"industry_and_competition":{"industry_stage":"","value_chain":"","competitors":[],"moat_summary":""},"business_model":{"revenue_model":[],"cost_structure":[],"unit_economics":[],"key_kpis":[]},"management_and_capital_allocation":{"management_summary":"","capital_allocation_actions":[],"governance_flags":[]},"filings":{"latest_results":[],"key_financials":[],"guidance":[],"balance_sheet_flags":[]},"news_and_catalysts":{"recent_events":[],"upcoming_catalysts":[],"market_focus":[]},"valuation_and_market":{"market_data":[],"valuation_multiples":[],"market_expectations":[]},"risks":[],"tracking":{"follow_up_items":[],"minimum_dashboard":[]},"sources":[],"quality":{"coverage_score":0,"missing_fields":[],"confidence":"medium"}}'
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


def _normalize_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def _normalize_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _normalize_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _to_display_items(value: object, *, limit: int = 5) -> list[str]:
    items = _normalize_list(value)
    result: list[str] = []
    for item in items[:limit]:
        if isinstance(item, dict):
            pairs = []
            for key in ("title", "metric", "name", "period", "value", "timing", "impact", "note"):
                text = _normalize_text(item.get(key))
                if text:
                    pairs.append(text)
            normalized = " / ".join(dict.fromkeys(pairs))
        else:
            normalized = _normalize_text(item)
        if normalized:
            result.append(normalized)
    return result


def _build_fact_pack_summary(payload: dict[str, object]) -> str:
    profile = _normalize_dict(payload.get("profile"))
    industry = _normalize_dict(payload.get("industry_and_competition"))
    business_model = _normalize_dict(payload.get("business_model"))
    management = _normalize_dict(payload.get("management_and_capital_allocation"))
    filings = _normalize_dict(payload.get("filings"))
    news = _normalize_dict(payload.get("news_and_catalysts"))
    valuation = _normalize_dict(payload.get("valuation_and_market"))
    tracking = _normalize_dict(payload.get("tracking"))
    quality = _normalize_dict(payload.get("quality"))
    risks = _normalize_list(payload.get("risks"))

    lines = ["# Company Fact Pack", ""]
    business_summary = str(profile.get("business_summary") or "").strip()
    lines.extend(
        [
            "## Profile",
            "",
            f"- Company (ZH): {str(profile.get('company_name_zh') or '').strip() or '-'}",
            f"- Company (EN): {str(profile.get('company_name_en') or '').strip() or '-'}",
            f"- Exchange: {str(profile.get('exchange') or '').strip() or '-'}",
            f"- Industry: {str(profile.get('industry') or '').strip() or '-'}",
        ]
    )
    if business_summary:
        lines.extend(["", business_summary])

    lines.extend(["", "## Industry And Competition", ""])
    industry_stage = str(industry.get("industry_stage") or "").strip()
    value_chain = str(industry.get("value_chain") or "").strip()
    moat_summary = str(industry.get("moat_summary") or "").strip()
    if industry_stage:
        lines.append(f"- Industry Stage: {industry_stage}")
    if value_chain:
        lines.append(f"- Value Chain: {value_chain}")
    if moat_summary:
        lines.append(f"- Moat Summary: {moat_summary}")
    competitors = _normalize_list(industry.get("competitors"))[:5]
    if competitors:
        lines.append("- Competitors: " + " / ".join(str(item).strip() for item in competitors if str(item).strip()))
    if not (industry_stage or value_chain or moat_summary or competitors):
        lines.append("- -")

    lines.extend(["", "## Business Model", ""])
    revenue_model = _normalize_list(business_model.get("revenue_model"))[:5]
    cost_structure = _normalize_list(business_model.get("cost_structure"))[:4]
    unit_economics = _normalize_list(business_model.get("unit_economics"))[:4]
    key_kpis = _normalize_list(business_model.get("key_kpis"))[:6]
    if revenue_model:
        lines.append("- Revenue Model: " + " / ".join(str(item).strip() for item in revenue_model if str(item).strip()))
    if cost_structure:
        lines.append("- Cost Structure: " + " / ".join(str(item).strip() for item in cost_structure if str(item).strip()))
    if unit_economics:
        lines.append("- Unit Economics: " + " / ".join(str(item).strip() for item in unit_economics if str(item).strip()))
    if key_kpis:
        lines.append("- Key KPIs: " + " / ".join(str(item).strip() for item in key_kpis if str(item).strip()))
    if not (revenue_model or cost_structure or unit_economics or key_kpis):
        lines.append("- -")

    lines.extend(["", "## Management And Capital Allocation", ""])
    management_summary = str(management.get("management_summary") or "").strip()
    governance_flags = _normalize_list(management.get("governance_flags"))[:4]
    capital_actions = _normalize_list(management.get("capital_allocation_actions"))[:5]
    if management_summary:
        lines.append(f"- Management Summary: {management_summary}")
    if capital_actions:
        lines.append("- Capital Allocation: " + " / ".join(str(item).strip() for item in capital_actions if str(item).strip()))
    if governance_flags:
        lines.append("- Governance Flags: " + " / ".join(str(item).strip() for item in governance_flags if str(item).strip()))
    if not (management_summary or capital_actions or governance_flags):
        lines.append("- -")

    key_financials = _normalize_list(filings.get("key_financials"))[:6]
    lines.extend(["", "## Key Financials", ""])
    if key_financials:
        for item in key_financials:
            if isinstance(item, dict):
                metric = str(item.get("metric") or "").strip() or "metric"
                period = str(item.get("period") or "").strip()
                value = str(item.get("value") or "").strip()
                note = str(item.get("note") or "").strip()
                detail = " / ".join(part for part in [period, value, note] if part)
                lines.append(f"- {metric}: {detail or '-'}")
            else:
                lines.append(f"- {str(item).strip()}")
    else:
        lines.append("- -")

    lines.extend(["", "## Tracking", ""])
    follow_up_items = _normalize_list(tracking.get("follow_up_items"))[:6]
    minimum_dashboard = _normalize_list(tracking.get("minimum_dashboard"))[:8]
    if follow_up_items:
        lines.append("- Follow-up: " + " / ".join(str(item).strip() for item in follow_up_items if str(item).strip()))
    if minimum_dashboard:
        lines.append("- Minimum Dashboard: " + " / ".join(str(item).strip() for item in minimum_dashboard if str(item).strip()))
    if not (follow_up_items or minimum_dashboard):
        lines.append("- -")

    recent_events = _normalize_list(news.get("recent_events"))[:5]
    upcoming_catalysts = _normalize_list(news.get("upcoming_catalysts"))[:5]
    lines.extend(["", "## Recent Events", ""])
    if recent_events:
        for item in recent_events:
            if isinstance(item, dict):
                date = str(item.get("date") or "").strip()
                title = str(item.get("title") or "").strip()
                impact = str(item.get("impact") or "").strip()
                detail = " / ".join(part for part in [date, title, impact] if part)
                lines.append(f"- {detail or '-'}")
            else:
                lines.append(f"- {str(item).strip()}")
    else:
        lines.append("- -")

    lines.extend(["", "## Upcoming Catalysts", ""])
    if upcoming_catalysts:
        for item in upcoming_catalysts:
            if isinstance(item, dict):
                title = str(item.get("title") or "").strip()
                timing = str(item.get("timing") or "").strip()
                impact = str(item.get("impact") or "").strip()
                detail = " / ".join(part for part in [timing, title, impact] if part)
                lines.append(f"- {detail or '-'}")
            else:
                lines.append(f"- {str(item).strip()}")
    else:
        lines.append("- -")

    market_data = _normalize_list(valuation.get("market_data"))[:5]
    multiples = _normalize_list(valuation.get("valuation_multiples"))[:5]
    lines.extend(["", "## Valuation And Market", ""])
    if market_data or multiples:
        for item in [*market_data, *multiples]:
            if isinstance(item, dict):
                metric = str(item.get("metric") or item.get("name") or "").strip() or "item"
                value = str(item.get("value") or "").strip()
                note = str(item.get("note") or "").strip()
                detail = " / ".join(part for part in [value, note] if part)
                lines.append(f"- {metric}: {detail or '-'}")
            else:
                lines.append(f"- {str(item).strip()}")
    else:
        lines.append("- -")

    lines.extend(["", "## Risks", ""])
    if risks:
        for item in risks[:6]:
            if isinstance(item, dict):
                title = str(item.get("title") or item.get("risk") or "").strip()
                detail = str(item.get("detail") or item.get("impact") or "").strip()
                lines.append(f"- {title}: {detail}" if detail else f"- {title or item}")
            else:
                lines.append(f"- {str(item).strip()}")
    else:
        lines.append("- -")

    lines.extend(
        [
            "",
            "## Quality",
            "",
            f"- Coverage Score: {str(quality.get('coverage_score') or '-').strip() or '-'}",
            f"- Confidence: {str(quality.get('confidence') or '-').strip() or '-'}",
        ]
    )
    missing_fields = _normalize_list(quality.get("missing_fields"))
    if missing_fields:
        lines.append("- Missing Fields: " + ", ".join(str(item).strip() for item in missing_fields if str(item).strip()))
    else:
        lines.append("- Missing Fields: -")
    lines.append("")
    return "\n".join(lines)


def build_fact_pack_prompt_context(
    fact_pack: FactPack,
    *,
    question: str = "",
    prompt_text: str = "",
    max_chars: int = 10000,
) -> str:
    question_text = f"{question} {prompt_text}".strip()
    summary = fact_pack.summary_markdown.strip()
    payload = fact_pack.payload
    quality = payload.get("quality")
    quality_note = ""
    if isinstance(quality, dict):
        coverage = str(quality.get("coverage_score") or "").strip()
        confidence = str(quality.get("confidence") or "").strip()
        missing_fields = quality.get("missing_fields")
        missing_text = ""
        if isinstance(missing_fields, list):
            missing_text = ", ".join(str(item).strip() for item in missing_fields if str(item).strip())
        parts = []
        if coverage:
            parts.append(f"coverage_score={coverage}")
        if confidence:
            parts.append(f"confidence={confidence}")
        if missing_text:
            parts.append(f"missing_fields={missing_text}")
        if parts:
            quality_note = " / ".join(parts)

    lines = [
        "以下是本轮预先采集并归一化的事实包，请优先基于这些事实进行分析，不要重复搜索和重复罗列。",
        "如果事实包中某项缺失或置信度不足，请直接说明“事实不足”或“不确定”，不要脑补。",
        f"事实包采集时间：{fact_pack.collected_at}",
        f"事实包是否联网采集：{'yes' if fact_pack.collected_with_web_search else 'no'}",
    ]
    if quality_note:
        lines.append(f"事实包质量：{quality_note}")
    if fact_pack.delta_summary_markdown.strip():
        lines.extend(["", "## 与上次更新相比", "", fact_pack.delta_summary_markdown.strip()])
    selected_sections: list[str] = []

    def append_section(title: str, value: object) -> None:
        if not value:
            return
        section_text = json.dumps(value, ensure_ascii=False, indent=2) if isinstance(value, (dict, list)) else str(value).strip()
        section_text = section_text.strip()
        if not section_text:
            return
        selected_sections.extend(["", f"## {title}", "", section_text])

    append_section("Profile", payload.get("profile"))

    if any(keyword in question_text for keyword in ("行业", "竞争", "壁垒", "护城河")):
        append_section("Industry And Competition", payload.get("industry_and_competition"))
    if any(keyword in question_text for keyword in ("商业模式", "单位经济", "收入模型", "KPI")):
        append_section("Business Model", payload.get("business_model"))
    if any(keyword in question_text for keyword in ("财务", "会计", "现金流", "资产负债")):
        append_section("Filings", payload.get("filings"))
    if any(keyword in question_text for keyword in ("管理层", "激励", "资本配置", "治理")):
        append_section("Management And Capital Allocation", payload.get("management_and_capital_allocation"))
    if any(keyword in question_text for keyword in ("市场关注", "隐含预期", "近期", "催化剂", "新闻", "事件")):
        append_section("News And Catalysts", payload.get("news_and_catalysts"))
    if any(keyword in question_text for keyword in ("估值", "情景分析", "股价", "倍数")):
        append_section("Valuation And Market", payload.get("valuation_and_market"))
    if any(keyword in question_text for keyword in ("风险", "反方", "空头")):
        append_section("Risks", payload.get("risks"))
    if any(keyword in question_text for keyword in ("跟踪", "清单", "面板", "dashboard", "KPI")):
        append_section("Tracking", payload.get("tracking"))

    if len(selected_sections) <= 4 and summary:
        selected_sections.extend(["", summary])

    lines.extend(selected_sections)
    context = "\n".join(lines).strip()
    if len(context) <= max_chars:
        return context
    return context[:max_chars].rstrip() + "\n...(truncated)"


def build_fact_pack_delta_summary(
    current: FactPack,
    previous: FactPack | None,
) -> str:
    if previous is None:
        return (
            "# Fact Pack Delta\n\n"
            "- 首次采集或无历史事实包可对比。\n"
            f"- 当前事实包日期：{current.report_date}\n"
        )

    current_payload = current.payload
    previous_payload = previous.payload
    lines = [
        "# Fact Pack Delta",
        "",
        f"- 对比区间：{previous.report_date} -> {current.report_date}",
        f"- 上次采集时间：{previous.collected_at}",
        f"- 本次采集时间：{current.collected_at}",
        "",
    ]

    changes: list[tuple[str, list[str]]] = []

    current_profile = _normalize_dict(current_payload.get("profile"))
    previous_profile = _normalize_dict(previous_payload.get("profile"))
    profile_changes: list[str] = []
    for key, label in (
        ("industry", "行业"),
        ("business_summary", "公司概况"),
    ):
        curr = _normalize_text(current_profile.get(key))
        prev = _normalize_text(previous_profile.get(key))
        if curr and curr != prev:
            profile_changes.append(f"{label}更新：{prev or '-'} -> {curr}")
    if profile_changes:
        changes.append(("公司概况", profile_changes))

    for section_key, section_title in (
        ("filings", "财务与公告"),
        ("news_and_catalysts", "新闻与催化剂"),
        ("valuation_and_market", "估值与市场"),
        ("risks", "风险"),
        ("tracking", "跟踪指标"),
    ):
        current_section = _normalize_dict(current_payload.get(section_key)) if section_key != "risks" else {"items": current_payload.get(section_key)}
        previous_section = _normalize_dict(previous_payload.get(section_key)) if section_key != "risks" else {"items": previous_payload.get(section_key)}
        section_changes: list[str] = []
        candidate_keys = list(current_section.keys() or previous_section.keys())
        if section_key == "risks":
            candidate_keys = ["items"]
        for key in candidate_keys:
            current_items = _to_display_items(current_section.get(key))
            previous_items = _to_display_items(previous_section.get(key))
            if current_items != previous_items:
                if current_items and not previous_items:
                    section_changes.append(f"{key} 新增：{' | '.join(current_items[:3])}")
                elif previous_items and not current_items:
                    section_changes.append(f"{key} 清空或缺失")
                else:
                    section_changes.append(
                        f"{key} 变化：{' | '.join(previous_items[:2]) or '-'} -> {' | '.join(current_items[:2]) or '-'}"
                    )
        if section_changes:
            changes.append((section_title, section_changes[:4]))

    if not changes:
        lines.extend(["- 与上次事实包相比，未检测到结构化字段层面的显著变化。", ""])
        return "\n".join(lines)

    for title, section_changes in changes:
        lines.extend([f"## {title}", ""])
        for item in section_changes:
            lines.append(f"- {item}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def collect_company_facts(
    *,
    provider: ProviderConfig,
    api_key: str,
    api_base: str,
    model: str,
    company: str,
    ticker: str,
    report_date: str,
    request_timeout: int,
    max_retries: int,
    retry_delay: float,
    enable_web_search: bool,
    output_path: str,
) -> FactPack:
    prompt_text = _build_fact_collection_prompt(
        company=company,
        ticker=ticker,
        report_date=report_date,
    )
    web_search_enabled = enable_web_search and provider.supports_web_search

    for attempt in range(1, max_retries + 1):
        try:
            response_json = (
                call_openai(
                    api_key=api_key,
                    api_base=api_base,
                    model=model,
                    prompt_text=prompt_text,
                    timeout=request_timeout,
                    enable_web_search=web_search_enabled,
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
                raise ValueError("Fact pack collector returned invalid JSON")
            return FactPack(
                company=company,
                ticker=ticker,
                report_date=report_date,
                provider_name=provider.display_name,
                model=model,
                collected_at=datetime.now().astimezone().isoformat(timespec="seconds"),
                collected_with_web_search=web_search_enabled,
                payload=payload,
                summary_markdown=_build_fact_pack_summary(payload),
                output_path=output_path,
            )
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            can_fallback = (
                provider.api_style == "responses"
                and web_search_enabled
                and is_tool_not_open_error(error_body)
            )
            if can_fallback:
                web_search_enabled = False
                logging.warning(
                    "Fact pack collection: web_search unavailable for company=%s ticker=%s, retry without web_search.",
                    company,
                    ticker,
                )
                continue
            non_retryable_reason = classify_http_error(exc.code, error_body)
            logging.warning(
                "Fact pack HTTP error company=%s ticker=%s attempt=%s/%s: %s | body=%s",
                company,
                ticker,
                attempt,
                max_retries,
                exc,
                error_body,
            )
            if non_retryable_reason:
                raise RuntimeError(non_retryable_reason) from exc
        except Exception:
            logging.exception(
                "Fact pack collection failed company=%s ticker=%s attempt=%s/%s",
                company,
                ticker,
                attempt,
                max_retries,
            )
        if attempt < max_retries:
            time.sleep(retry_delay)

    raise RuntimeError(f"Fact pack collection failed after retries: {ticker}")
