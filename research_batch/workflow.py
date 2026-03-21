from __future__ import annotations

import logging
import time
import urllib.error
from pathlib import Path

from research_batch.config import NonRetryableAPIError, PromptRow, ProviderConfig
from research_batch.llm import (
    call_chat_completions,
    call_openai,
    classify_http_error,
    extract_chat_completion_text,
    extract_output_text,
    extract_sources,
    is_tool_not_open_error,
    summarize_http_error,
)
from research_batch.storage import render_prompt, sanitize_filename, write_output


def run_provider_test(
    *,
    provider: ProviderConfig,
    api_key: str,
    api_base: str,
    model: str,
    request_timeout: int,
    enable_web_search: bool,
) -> None:
    prompt_text = "请回复“PROVIDER_TEST_OK”，并在下一行给出你识别到的模型或服务名称。"
    logging.info(
        "Running provider test provider=%s model=%s api_base=%s web_search=%s",
        provider.provider_id,
        model,
        api_base,
        enable_web_search,
    )
    web_search_enabled = enable_web_search
    for attempt in range(1, 3):
        try:
            response_json = (
                call_openai(
                    api_key=api_key,
                    api_base=api_base,
                    model=model,
                    prompt_text=prompt_text,
                    timeout=request_timeout,
                    enable_web_search=web_search_enabled,
                    include_sources=provider.supports_include_sources,
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
            answer = (
                extract_output_text(response_json)
                if provider.api_style == "responses"
                else extract_chat_completion_text(response_json)
            )
            preview = answer.replace("\n", " ").strip()[:200] or "(empty response)"
            logging.info("Provider test succeeded. Response preview: %s", preview)
            return
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            can_fallback = (
                provider.api_style == "responses"
                and web_search_enabled
                and is_tool_not_open_error(error_body)
                and attempt == 1
            )
            if can_fallback:
                web_search_enabled = False
                logging.warning(
                    "Provider test: web_search unavailable, fallback to no-web-search and retry once."
                )
                continue
            raise RuntimeError(summarize_http_error(exc.code, error_body)) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error: {exc}") from exc


def build_markdown(
    *,
    company: str,
    ticker: str,
    question: str,
    report_date: str,
    provider_name: str,
    model: str,
    answer: str,
    sources: list[dict[str, str]],
) -> str:
    lines = [
        f"# {question}",
        "",
        f"- Company: {company}",
        f"- Ticker: {ticker}",
        f"- Date: {report_date}",
        f"- Provider: {provider_name}",
        f"- Model: {model}",
        "",
        "## Answer",
        "",
        answer or "(No text returned by model.)",
    ]
    if sources:
        lines.extend(["", "## Sources", ""])
        for source in sources:
            lines.append(f"- [{source['title']}]({source['url']})")
    lines.append("")
    return "\n".join(lines)


def process_company(
    *,
    row: dict[str, str],
    prompts: list[PromptRow],
    api_key: str,
    provider: ProviderConfig,
    api_base: str,
    model: str,
    report_date: str,
    output_root: Path,
    max_retries: int,
    retry_delay: float,
    request_timeout: int,
    enable_web_search: bool,
    force_rerun: bool,
) -> bool:
    company = row["company"].strip()
    ticker = row["Ticker"].strip()
    output_dir = output_root / f"{sanitize_filename(ticker)}_{report_date}"
    logging.info("Processing company=%s ticker=%s", company, ticker)
    web_search_enabled = enable_web_search

    for prompt_row in prompts:
        safe_question = sanitize_filename(prompt_row.question)
        output_path = output_dir / f"{prompt_row.prompt_id}_{safe_question}.md"
        if output_path.exists() and not force_rerun:
            logging.info("Skip existing output: %s", output_path)
            continue

        rendered_prompt = render_prompt(
            prompt_row.prompt,
            company=company,
            ticker=ticker,
            report_date=report_date,
        )
        success = False
        try:
            for attempt in range(1, max_retries + 1):
                try:
                    logging.info(
                        "Requesting prompt_id=%s question=%s attempt=%s/%s",
                        prompt_row.prompt_id,
                        prompt_row.question,
                        attempt,
                        max_retries,
                    )
                    response_json = (
                        call_openai(
                            api_key=api_key,
                            api_base=api_base,
                            model=model,
                            prompt_text=rendered_prompt,
                            timeout=request_timeout,
                            enable_web_search=web_search_enabled,
                            include_sources=provider.supports_include_sources,
                        )
                        if provider.api_style == "responses"
                        else call_chat_completions(
                            api_key=api_key,
                            api_base=api_base,
                            model=model,
                            prompt_text=rendered_prompt,
                            timeout=request_timeout,
                        )
                    )
                    answer = (
                        extract_output_text(response_json)
                        if provider.api_style == "responses"
                        else extract_chat_completion_text(response_json)
                    )
                    sources = (
                        extract_sources(response_json)
                        if provider.api_style == "responses"
                        else []
                    )
                    markdown = build_markdown(
                        company=company,
                        ticker=ticker,
                        question=prompt_row.question,
                        report_date=report_date,
                        provider_name=provider.display_name,
                        model=model,
                        answer=answer,
                        sources=sources,
                    )
                    existed_before_write = output_path.exists()
                    write_output(output_path, markdown, force_rerun=force_rerun)
                    if existed_before_write and force_rerun:
                        logging.info("Overwrote output (force rerun): %s", output_path)
                    else:
                        logging.info("Wrote output: %s", output_path)
                    success = True
                    break
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
                            "web_search is not enabled for this account; continue without web_search from now on."
                        )
                        continue
                    non_retryable_reason = classify_http_error(exc.code, error_body)
                    logging.warning(
                        "HTTP error for ticker=%s prompt_id=%s attempt=%s: %s | body=%s",
                        ticker,
                        prompt_row.prompt_id,
                        attempt,
                        exc,
                        error_body,
                    )
                    if non_retryable_reason:
                        raise NonRetryableAPIError(non_retryable_reason) from exc
                except urllib.error.URLError as exc:
                    logging.warning(
                        "Network error for ticker=%s prompt_id=%s attempt=%s: %s",
                        ticker,
                        prompt_row.prompt_id,
                        attempt,
                        exc,
                    )
                except Exception:
                    logging.exception(
                        "Unexpected error for ticker=%s prompt_id=%s attempt=%s",
                        ticker,
                        prompt_row.prompt_id,
                        attempt,
                    )
                if attempt < max_retries:
                    time.sleep(retry_delay)
        except NonRetryableAPIError:
            logging.exception(
                "Non-retryable API error for company=%s ticker=%s prompt_id=%s",
                company,
                ticker,
                prompt_row.prompt_id,
            )
            return False

        if not success:
            logging.error(
                "Giving up for company=%s ticker=%s prompt_id=%s",
                company,
                ticker,
                prompt_row.prompt_id,
            )
            return False
    return True

