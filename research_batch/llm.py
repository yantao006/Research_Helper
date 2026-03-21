from __future__ import annotations

import json
import urllib.request
from typing import Any


def call_openai(
    *,
    api_key: str,
    api_base: str,
    model: str,
    prompt_text: str,
    timeout: int,
    enable_web_search: bool,
    include_sources: bool,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": model,
        "input": prompt_text,
        "reasoning": {"effort": "medium"},
    }
    if enable_web_search:
        payload["tools"] = [{"type": "web_search"}]
        if include_sources:
            payload["include"] = ["web_search_call.action.sources"]
    request = urllib.request.Request(
        url=f"{api_base.rstrip('/')}/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def call_chat_completions(
    *,
    api_key: str,
    api_base: str,
    model: str,
    prompt_text: str,
    timeout: int,
) -> dict[str, Any]:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt_text}],
        "temperature": 0.2,
    }
    request = urllib.request.Request(
        url=f"{api_base.rstrip('/')}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def classify_http_error(status_code: int, error_body: str) -> str | None:
    normalized = error_body.lower()
    if "insufficient_quota" in normalized:
        return (
            "API key 所属项目额度不足或未开通计费。"
            "请检查 OpenAI Platform 的 billing、project 和 API key。"
        )
    if status_code == 401:
        return "API key 无效、缺失，或当前 key 没有访问该接口的权限。"
    if "toolnotopen" in normalized:
        return "当前账号未开通所请求的工具（例如 web_search）。可在平台侧开通，或关闭该工具后重试。"
    if status_code == 403 and "error code: 1010" in normalized:
        return (
            "请求被上游网关或防护服务拦截了（403 / error code 1010）。"
            "这通常不是 prompt 问题，而更像是 OPENAI_BASE_URL 中转、代理节点、出口 IP 或区域策略导致。"
        )
    if status_code in {400, 403, 404}:
        return "当前请求大概率属于配置或权限问题，继续重试通常不会恢复。"
    return None


def summarize_http_error(status_code: int, error_body: str) -> str:
    reason = classify_http_error(status_code, error_body)
    if reason:
        return f"{reason} 原始响应: {error_body}"
    return f"HTTP {status_code}: {error_body}"


def is_tool_not_open_error(error_body: str) -> bool:
    return "toolnotopen" in error_body.lower()


def extract_output_text(response_json: dict[str, Any]) -> str:
    direct_text = response_json.get("output_text")
    if isinstance(direct_text, str) and direct_text.strip():
        return direct_text.strip()
    parts: list[str] = []
    for item in response_json.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                text = (content.get("text") or "").strip()
                if text:
                    parts.append(text)
    return "\n\n".join(parts).strip()


def extract_sources(response_json: dict[str, Any]) -> list[dict[str, str]]:
    sources: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in response_json.get("output", []):
        if item.get("type") != "web_search_call":
            continue
        action = item.get("action") or {}
        for source in action.get("sources", []):
            title = str(source.get("title") or "").strip()
            url = str(source.get("url") or "").strip()
            if not url:
                continue
            key = (title, url)
            if key in seen:
                continue
            seen.add(key)
            sources.append({"title": title or url, "url": url})
    return sources


def extract_chat_completion_text(response_json: dict[str, Any]) -> str:
    choices = response_json.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = str(item.get("text") or "").strip()
                if text:
                    parts.append(text)
        return "\n\n".join(parts).strip()
    return ""

