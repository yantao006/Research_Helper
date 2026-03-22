from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
import logging
import time
import urllib.error
from pathlib import Path

from research_batch.config import FactPack, PromptRow, ProviderConfig
from research_batch.facts import (
    build_fact_pack_delta_summary,
    build_fact_pack_prompt_context,
    collect_company_facts,
)
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
from research_batch.repositories import DocRepo, FactRepo, JobRepo, RunRepo


STAGE_TWO_QUESTION_KEYWORDS = (
    "综合",
    "结论",
    "投资备忘录",
    "一句话判断",
    "总结",
)

WEB_SEARCH_QUESTION_KEYWORDS = (
    "近期",
    "动态",
    "催化剂",
    "市场关注",
    "隐含预期",
    "新闻",
    "事件",
    "监管",
)


@dataclass(frozen=True)
class PromptExecutionResult:
    prompt_id: str
    question: str
    success: bool
    answer: str = ""
    non_retryable: bool = False
    skipped: bool = False
    error_message: str = ""


def _prompt_stage(prompt_row: PromptRow) -> int:
    question = prompt_row.question.strip()
    if any(keyword in question for keyword in STAGE_TWO_QUESTION_KEYWORDS):
        return 2
    return 1


def _prompt_prefers_fact_pack(prompt_row: PromptRow) -> bool:
    _ = prompt_row
    return True


def _group_prompts_by_stage(prompts: list[PromptRow]) -> list[list[PromptRow]]:
    ordered_stages: dict[int, list[PromptRow]] = {}
    for prompt in prompts:
        ordered_stages.setdefault(_prompt_stage(prompt), []).append(prompt)
    return [ordered_stages[stage] for stage in sorted(ordered_stages)]


def _prompt_needs_web_search(prompt_row: PromptRow, web_search_mode: str) -> bool:
    if web_search_mode == "all":
        return True
    question = prompt_row.question.strip()
    prompt_text = prompt_row.prompt.strip()
    return "联网搜索" in prompt_text or any(
        keyword in question for keyword in WEB_SEARCH_QUESTION_KEYWORDS
    )


def _clip_text(value: str, *, limit: int) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "\n...(truncated)"


def _build_prior_context(results: list[PromptExecutionResult]) -> str:
    successful_results = [result for result in results if result.success and result.answer.strip()]
    if not successful_results:
        return ""

    total_limit = 14000
    per_doc_limit = 3200
    remaining = total_limit
    blocks: list[str] = [
        "以下是同一家公司在本轮调研中已经完成的前置分析，可作为后续综合判断的输入材料。",
        "请优先在这些材料基础上做整合、比较和取舍；如果它们与公开信息冲突，以可验证公开信息为准；不要机械重复原文。",
        "",
    ]
    for result in successful_results:
        clipped_answer = _clip_text(result.answer, limit=per_doc_limit)
        block = f"### {result.prompt_id}. {result.question}\n\n{clipped_answer}\n"
        if len(block) > remaining:
            clipped_answer = _clip_text(result.answer, limit=max(600, remaining - 64))
            block = f"### {result.prompt_id}. {result.question}\n\n{clipped_answer}\n"
        if len(block) > remaining:
            break
        blocks.append(block)
        remaining -= len(block)
        if remaining <= 600:
            break
    if len(blocks) <= 3:
        return ""
    return "\n".join(blocks).strip()


def _execute_prompt(
    *,
    prompt_row: PromptRow,
    company: str,
    ticker: str,
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
    doc_repo: DocRepo,
    run_repo: RunRepo,
    job_repo: JobRepo,
    job_id: str,
    run_id: str,
    web_search_mode: str,
    fact_pack_context: str = "",
    prior_context: str = "",
) -> PromptExecutionResult:
    output_path = run_repo.build_output_path(
        output_root=output_root,
        ticker=ticker,
        report_date=report_date,
        prompt=prompt_row,
    )
    if run_repo.output_exists(output_path) and not force_rerun:
        logging.info("Skip existing output: %s", output_path)
        answer, _ = doc_repo.load_existing_output_context(output_path)
        return PromptExecutionResult(
            prompt_id=prompt_row.prompt_id,
            question=prompt_row.question,
            success=True,
            answer=answer,
            skipped=True,
        )

    rendered_prompt = doc_repo.render_prompt(
        template=prompt_row.prompt,
        company=company,
        ticker=ticker,
        report_date=report_date,
    )
    if fact_pack_context.strip():
        rendered_prompt = (
            f"{rendered_prompt}\n\n"
            "## 结构化事实包\n\n"
            f"{fact_pack_context}\n\n"
            "请优先利用上面的事实包完成分析；如果事实包与前置分析有冲突，以更具体、可验证、时间更新的事实为准。"
        )
    if prior_context.strip():
        rendered_prompt = (
            f"{rendered_prompt}\n\n"
            "## 前置分析上下文\n\n"
            f"{prior_context}\n\n"
            "请在吸收以上前置分析的基础上完成本题，重点做综合判断、交叉验证和结论收敛。"
        )
    prompt_web_search_enabled = (
        enable_web_search
        and _prompt_needs_web_search(prompt_row, web_search_mode)
        and not fact_pack_context.strip()
    )

    for attempt in range(1, max_retries + 1):
        try:
            logging.info(
                "Requesting prompt_id=%s question=%s attempt=%s/%s web_search=%s",
                prompt_row.prompt_id,
                prompt_row.question,
                attempt,
                max_retries,
                prompt_web_search_enabled,
            )
            job_repo.add_event(
                job_id=job_id,
                message=(
                    f"request prompt_id={prompt_row.prompt_id}"
                    f" question={prompt_row.question}"
                    f" attempt={attempt}/{max_retries}"
                    f" web_search={str(prompt_web_search_enabled).lower()}"
                ),
            )
            response_json = (
                call_openai(
                    api_key=api_key,
                    api_base=api_base,
                    model=model,
                    prompt_text=rendered_prompt,
                    timeout=request_timeout,
                    enable_web_search=prompt_web_search_enabled,
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
                extract_sources(response_json) if provider.api_style == "responses" else []
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
            existed_before_write = run_repo.output_exists(output_path)
            run_repo.write_output(output_path, markdown, force_rerun=force_rerun)
            doc_repo.save_research_doc(
                run_id=run_id,
                company=company,
                ticker=ticker,
                report_date=report_date,
                prompt_id=prompt_row.prompt_id,
                question=prompt_row.question,
                answer_markdown=answer,
                sources=sources,
                provider_name=provider.display_name,
                model=model,
                output_path=str(output_path),
                markdown=markdown,
            )
            if existed_before_write and force_rerun:
                logging.info("Overwrote output (force rerun): %s", output_path)
            else:
                logging.info("Wrote output: %s", output_path)
            return PromptExecutionResult(
                prompt_id=prompt_row.prompt_id,
                question=prompt_row.question,
                success=True,
                answer=answer,
            )
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            can_fallback = (
                provider.api_style == "responses"
                and prompt_web_search_enabled
                and is_tool_not_open_error(error_body)
            )
            if can_fallback:
                prompt_web_search_enabled = False
                logging.warning(
                    "web_search unavailable for prompt_id=%s; retry without web_search.",
                    prompt_row.prompt_id,
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
                return PromptExecutionResult(
                    prompt_id=prompt_row.prompt_id,
                    question=prompt_row.question,
                    success=False,
                    non_retryable=True,
                    error_message=non_retryable_reason,
                )
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

    return PromptExecutionResult(
        prompt_id=prompt_row.prompt_id,
        question=prompt_row.question,
        success=False,
        error_message=f"prompt_failed:{prompt_row.prompt_id}",
    )


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
    web_search_mode: str,
    fact_pack_enabled: bool,
    strict_fact_pack: bool,
    force_rerun: bool,
    prompt_workers: int,
    fact_repo: FactRepo,
    doc_repo: DocRepo,
    run_repo: RunRepo,
    job_repo: JobRepo,
) -> bool:
    company = row["company"].strip()
    ticker = row["Ticker"].strip()
    job_id = job_repo.begin_company(company=company, ticker=ticker)
    run_id = run_repo.begin_run(
        company=company,
        ticker=ticker,
        report_date=report_date,
        provider_name=provider.display_name,
        model=model,
    )
    logging.info("Processing company=%s ticker=%s", company, ticker)
    fact_pack: FactPack | None = None
    if fact_pack_enabled:
        fact_pack_path = fact_repo.build_fact_pack_path(
            output_root=output_root,
            ticker=ticker,
            report_date=report_date,
        )
        try:
            if fact_repo.fact_pack_exists(fact_pack_path) and not force_rerun:
                fact_pack = fact_repo.load_fact_pack(fact_pack_path)
                if fact_pack:
                    logging.info(
                        "Reusing fact pack company=%s ticker=%s path=%s",
                        company,
                        ticker,
                        fact_pack_path,
                    )
                    job_repo.add_event(
                        job_id=job_id,
                        message=f"fact_pack_reused path={fact_pack_path}",
                    )
                else:
                    logging.warning(
                        "Fact pack exists but failed to load company=%s ticker=%s path=%s",
                        company,
                        ticker,
                        fact_pack_path,
                    )
            else:
                logging.info(
                    "Collecting fact pack company=%s ticker=%s path=%s",
                    company,
                    ticker,
                    fact_pack_path,
                )
                job_repo.add_event(
                    job_id=job_id,
                    message=f"fact_pack_collect start path={fact_pack_path}",
                )
                fact_pack = collect_company_facts(
                    provider=provider,
                    api_key=api_key,
                    api_base=api_base,
                    model=model,
                    company=company,
                    ticker=ticker,
                    report_date=report_date,
                    request_timeout=request_timeout,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                    enable_web_search=enable_web_search,
                    output_path=str(fact_pack_path),
                )
                fact_repo.save_fact_pack(run_id=run_id, fact_pack=fact_pack)
                job_repo.add_event(
                    job_id=job_id,
                    message=f"fact_pack_collect success path={fact_pack_path}",
                )
                logging.info(
                    "Saved fact pack company=%s ticker=%s path=%s",
                    company,
                    ticker,
                    fact_pack_path,
                )
        except Exception as exc:
            logging.exception(
                "Fact pack collection failed company=%s ticker=%s",
                company,
                ticker,
            )
            job_repo.add_event(
                job_id=job_id,
                message=f"fact_pack_collect failed error={exc}",
            )
            if strict_fact_pack:
                run_repo.finish_run(
                    run_id=run_id,
                    success=False,
                    error_message="fact_pack_failed",
                )
                job_repo.finish_company(job_id=job_id, success=False)
                return False
        if fact_pack:
            previous_fact_pack = fact_repo.find_previous_fact_pack(
                output_root=output_root,
                ticker=ticker,
                report_date=report_date,
            )
            if previous_fact_pack:
                logging.info(
                    "Loaded previous fact pack company=%s ticker=%s previous_report_date=%s",
                    company,
                    ticker,
                    previous_fact_pack.report_date,
                )
                job_repo.add_event(
                    job_id=job_id,
                    message=(
                        "fact_pack_delta previous_report_date="
                        f"{previous_fact_pack.report_date}"
                    ),
                )
            else:
                logging.info(
                    "No previous fact pack found company=%s ticker=%s report_date=%s",
                    company,
                    ticker,
                    report_date,
                )
                job_repo.add_event(
                    job_id=job_id,
                    message="fact_pack_delta previous_report_date=none",
                )
            delta_summary_markdown = build_fact_pack_delta_summary(
                fact_pack,
                previous_fact_pack,
            )
            if delta_summary_markdown.strip() != fact_pack.delta_summary_markdown.strip():
                fact_pack = replace(
                    fact_pack,
                    delta_summary_markdown=delta_summary_markdown,
                )
                fact_repo.save_fact_pack(run_id=run_id, fact_pack=fact_pack)
                logging.info(
                    "Updated fact pack delta company=%s ticker=%s report_date=%s",
                    company,
                    ticker,
                    report_date,
                )
                job_repo.add_event(
                    job_id=job_id,
                    message="fact_pack_delta updated",
                )

    stage_results: list[PromptExecutionResult] = []
    for stage_index, stage_prompts in enumerate(_group_prompts_by_stage(prompts), start=1):
        prior_context = _build_prior_context(results=stage_results)
        logging.info(
            "Processing stage company=%s ticker=%s stage=%s prompts=%s prompt_workers=%s prior_context_chars=%s",
            company,
            ticker,
            stage_index,
            len(stage_prompts),
            max(1, prompt_workers),
            len(prior_context),
        )
        results: list[PromptExecutionResult] = []
        if max(1, prompt_workers) == 1 or len(stage_prompts) <= 1:
            for prompt_row in stage_prompts:
                fact_pack_context = (
                    build_fact_pack_prompt_context(
                        fact_pack,
                        question=prompt_row.question,
                        prompt_text=prompt_row.prompt,
                    )
                    if fact_pack and _prompt_prefers_fact_pack(prompt_row)
                    else ""
                )
                results.append(
                    _execute_prompt(
                        prompt_row=prompt_row,
                        company=company,
                        ticker=ticker,
                        api_key=api_key,
                        provider=provider,
                        api_base=api_base,
                        model=model,
                        report_date=report_date,
                        output_root=output_root,
                        max_retries=max_retries,
                        retry_delay=retry_delay,
                        request_timeout=request_timeout,
                        enable_web_search=enable_web_search,
                        force_rerun=force_rerun,
                        doc_repo=doc_repo,
                        run_repo=run_repo,
                        job_repo=job_repo,
                        job_id=job_id,
                        run_id=run_id,
                        web_search_mode=web_search_mode,
                        fact_pack_context=fact_pack_context,
                        prior_context=prior_context,
                    )
                )
        else:
            max_stage_workers = min(max(1, prompt_workers), len(stage_prompts))
            with ThreadPoolExecutor(max_workers=max_stage_workers) as executor:
                future_map = {
                    executor.submit(
                        _execute_prompt,
                        prompt_row=prompt_row,
                        company=company,
                        ticker=ticker,
                        api_key=api_key,
                        provider=provider,
                        api_base=api_base,
                        model=model,
                        report_date=report_date,
                        output_root=output_root,
                        max_retries=max_retries,
                        retry_delay=retry_delay,
                        request_timeout=request_timeout,
                        enable_web_search=enable_web_search,
                        force_rerun=force_rerun,
                        doc_repo=doc_repo,
                        run_repo=run_repo,
                        job_repo=job_repo,
                        job_id=job_id,
                        run_id=run_id,
                        web_search_mode=web_search_mode,
                        fact_pack_context=(
                            build_fact_pack_prompt_context(
                                fact_pack,
                                question=prompt_row.question,
                                prompt_text=prompt_row.prompt,
                            )
                            if fact_pack and _prompt_prefers_fact_pack(prompt_row)
                            else ""
                        ),
                        prior_context=prior_context,
                    ): prompt_row.prompt_id
                    for prompt_row in stage_prompts
                }
                for future in as_completed(future_map):
                    results.append(future.result())

        failed_result = next((result for result in results if not result.success), None)
        if failed_result:
            if failed_result.non_retryable:
                logging.error(
                    "Non-retryable API error for company=%s ticker=%s prompt_id=%s reason=%s",
                    company,
                    ticker,
                    failed_result.prompt_id,
                    failed_result.error_message or "unknown",
                )
                run_repo.finish_run(
                    run_id=run_id,
                    success=False,
                    error_message="non_retryable_api_error",
                )
            else:
                logging.error(
                    "Giving up for company=%s ticker=%s prompt_id=%s",
                    company,
                    ticker,
                    failed_result.prompt_id,
                )
                run_repo.finish_run(
                    run_id=run_id,
                    success=False,
                    error_message=failed_result.error_message or f"prompt_failed:{failed_result.prompt_id}",
                )
            job_repo.finish_company(job_id=job_id, success=False)
            return False

        stage_results.extend(results)

    run_repo.finish_run(run_id=run_id, success=True)
    job_repo.finish_company(job_id=job_id, success=True)
    return True
