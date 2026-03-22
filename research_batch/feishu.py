from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from research_batch.config import (
    REQUIRED_FEISHU_FIELDS,
    REQUIRED_FEISHU_SUMMARY_FIELDS,
    FeishuConfig,
    PromptRow,
    SyncDoc,
)
from research_batch.env_utils import is_falsy, require_env
from research_batch.repositories import DocRepo, RunRepo


@dataclass
class FeishuSyncTask:
    row: dict[str, str]


def resolve_feishu_config() -> FeishuConfig | None:
    enabled_value = os.getenv("FEISHU_ENABLE_SYNC", "false")
    if is_falsy(enabled_value):
        return None
    return FeishuConfig(
        app_id=require_env("FEISHU_APP_ID"),
        app_secret=require_env("FEISHU_APP_SECRET"),
        app_token=require_env("FEISHU_APP_TOKEN"),
        table_id=require_env("FEISHU_TABLE_ID"),
        summary_table_id=os.getenv("FEISHU_SUMMARY_TABLE_ID", "").strip(),
    )


def feishu_request(
    *,
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    timeout: int = 30,
) -> dict[str, Any]:
    request = urllib.request.Request(
        url=url,
        method=method,
        data=json.dumps(body).encode("utf-8") if body is not None else None,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            **(headers or {}),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Feishu HTTP error: {exc.code} {error_body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Feishu network error: {exc}") from exc
    if payload.get("code", -1) != 0:
        raise RuntimeError(
            f"Feishu API error code={payload.get('code')} msg={payload.get('msg')} payload={payload}"
        )
    return payload


def get_feishu_tenant_access_token(config: FeishuConfig, timeout: int) -> str:
    url = f"{config.base_url}/open-apis/auth/v3/tenant_access_token/internal"
    payload = feishu_request(
        method="POST",
        url=url,
        body={"app_id": config.app_id, "app_secret": config.app_secret},
        timeout=timeout,
    )
    token = str(payload.get("tenant_access_token") or "").strip()
    if not token:
        raise RuntimeError("Feishu auth succeeded but tenant_access_token is empty")
    return token


def list_feishu_field_names(
    *,
    config: FeishuConfig,
    tenant_access_token: str,
    timeout: int,
    table_id: str | None = None,
) -> set[str]:
    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    page_token = ""
    names: set[str] = set()
    resolved_table_id = (table_id or config.table_id).strip()
    while True:
        query = {"page_size": "500"}
        if page_token:
            query["page_token"] = page_token
        url = (
            f"{config.base_url}/open-apis/bitable/v1/apps/{config.app_token}/tables/{resolved_table_id}/fields?"
            + urllib.parse.urlencode(query)
        )
        payload = feishu_request(method="GET", url=url, headers=headers, timeout=timeout)
        data = payload.get("data") or {}
        for item in data.get("items") or []:
            field_name = str(item.get("field_name") or "").strip()
            if field_name:
                names.add(field_name)
        if not data.get("has_more"):
            break
        page_token = str(data.get("page_token") or "")
        if not page_token:
            break
    return names


def ensure_feishu_required_fields(
    *,
    config: FeishuConfig,
    tenant_access_token: str,
    timeout: int,
    table_id: str | None = None,
    required_fields: set[str] | None = None,
) -> None:
    existing = list_feishu_field_names(
        config=config,
        tenant_access_token=tenant_access_token,
        timeout=timeout,
        table_id=table_id,
    )
    resolved_required = required_fields or REQUIRED_FEISHU_FIELDS
    missing = sorted(resolved_required - existing)
    if missing:
        raise RuntimeError(
            "Feishu table is missing required fields: "
            + ", ".join(missing)
            + ". Please create these columns with exact names."
        )


def list_all_feishu_records(
    *,
    config: FeishuConfig,
    tenant_access_token: str,
    timeout: int,
    table_id: str | None = None,
) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    page_token = ""
    records: list[dict[str, Any]] = []
    resolved_table_id = (table_id or config.table_id).strip()
    while True:
        query = {"page_size": "500"}
        if page_token:
            query["page_token"] = page_token
        url = (
            f"{config.base_url}/open-apis/bitable/v1/apps/{config.app_token}/tables/{resolved_table_id}/records?"
            + urllib.parse.urlencode(query)
        )
        payload = feishu_request(method="GET", url=url, headers=headers, timeout=timeout)
        data = payload.get("data") or {}
        records.extend(data.get("items") or [])
        if not data.get("has_more"):
            break
        page_token = str(data.get("page_token") or "")
        if not page_token:
            break
    return records


def _normalize_fact_list(value: Any, *, limit: int = 5) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value[:limit]:
        if isinstance(item, dict):
            parts: list[str] = []
            for key in ("title", "metric", "name", "period", "value", "timing", "impact", "detail", "note"):
                text = str(item.get(key) or "").strip()
                if text:
                    parts.append(text)
            normalized = " / ".join(dict.fromkeys(parts))
        else:
            normalized = str(item or "").strip()
        if normalized:
            result.append(normalized)
    return result


def _build_fact_pack_summary_fields(
    *,
    row: dict[str, str],
    report_date: str,
    output_dir: Path,
) -> dict[str, str] | None:
    fact_pack_path = output_dir / "fact_pack.json"
    if not fact_pack_path.exists():
        return None
    payload = json.loads(fact_pack_path.read_text(encoding="utf-8"))
    fact_payload = payload.get("payload") or {}
    if not isinstance(fact_payload, dict):
        fact_payload = {}
    quality = fact_payload.get("quality") or {}
    filings = fact_payload.get("filings") or {}
    news = fact_payload.get("news_and_catalysts") or {}
    valuation = fact_payload.get("valuation_and_market") or {}
    tracking = fact_payload.get("tracking") or {}
    risks = fact_payload.get("risks") or []
    company = row["company"].strip()
    ticker = row["Ticker"].strip()
    industry = str(row.get("industry") or "").strip()
    if not industry:
        profile = fact_payload.get("profile") or {}
        if isinstance(profile, dict):
            industry = str(profile.get("industry") or "").strip()

    coverage_score = ""
    confidence = ""
    if isinstance(quality, dict):
        coverage_score = str(quality.get("coverage_score") or "").strip()
        confidence = str(quality.get("confidence") or "").strip()

    key_financials = _normalize_fact_list(
        filings.get("key_financials") if isinstance(filings, dict) else None,
        limit=5,
    )
    recent_catalysts = _normalize_fact_list(
        (news.get("recent_events") if isinstance(news, dict) else None)
        or (news.get("upcoming_catalysts") if isinstance(news, dict) else None),
        limit=5,
    )
    valuation_snapshot = _normalize_fact_list(
        (valuation.get("market_data") if isinstance(valuation, dict) else None)
        or (valuation.get("valuation_multiples") if isinstance(valuation, dict) else None),
        limit=5,
    )
    top_risks = _normalize_fact_list(risks, limit=5)
    tracking_items = _normalize_fact_list(
        (tracking.get("follow_up_items") if isinstance(tracking, dict) else None)
        or (tracking.get("minimum_dashboard") if isinstance(tracking, dict) else None),
        limit=5,
    )

    return {
        "run_key": f"{ticker}|{report_date}",
        "company": company,
        "ticker": ticker,
        "report_date": report_date,
        "industry": industry,
        "fact_pack_summary": str(payload.get("summary_markdown") or "").strip(),
        "delta_summary": str(payload.get("delta_summary_markdown") or "").strip(),
        "coverage_score": coverage_score,
        "confidence": confidence,
        "key_financials": "\n".join(key_financials),
        "recent_catalysts": "\n".join(recent_catalysts),
        "valuation_snapshot": "\n".join(valuation_snapshot),
        "top_risks": "\n".join(top_risks),
        "tracking_items": "\n".join(tracking_items),
        "output_dir": str(output_dir),
        "synced_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def sync_company_summary_to_feishu(
    *,
    config: FeishuConfig,
    tenant_access_token: str,
    row: dict[str, str],
    report_date: str,
    output_dir: Path,
    request_timeout: int,
) -> None:
    table_id = config.summary_table_id.strip()
    if not table_id:
        return
    fields = _build_fact_pack_summary_fields(
        row=row,
        report_date=report_date,
        output_dir=output_dir,
    )
    if not fields:
        logging.info(
            "Feishu summary sync skipped: fact_pack.json not found for %s %s",
            row["company"].strip(),
            row["Ticker"].strip(),
        )
        return

    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    ensure_feishu_required_fields(
        config=config,
        tenant_access_token=tenant_access_token,
        timeout=request_timeout,
        table_id=table_id,
        required_fields=REQUIRED_FEISHU_SUMMARY_FIELDS,
    )
    existing_records = list_all_feishu_records(
        config=config,
        tenant_access_token=tenant_access_token,
        timeout=request_timeout,
        table_id=table_id,
    )
    existing_by_key: dict[str, str] = {}
    for item in existing_records:
        item_fields = item.get("fields") or {}
        run_key = str(item_fields.get("run_key") or "").strip()
        record_id = str(item.get("record_id") or "").strip()
        if run_key and record_id:
            existing_by_key[run_key] = record_id

    run_key = fields["run_key"]
    record_id = existing_by_key.get(run_key)
    if record_id:
        url = (
            f"{config.base_url}/open-apis/bitable/v1/apps/{config.app_token}/tables/"
            f"{table_id}/records/{record_id}"
        )
        feishu_request(
            method="PUT",
            url=url,
            headers=headers,
            body={"fields": fields},
            timeout=request_timeout,
        )
        logging.info("Feishu updated summary record run_key=%s", run_key)
        return

    url = (
        f"{config.base_url}/open-apis/bitable/v1/apps/{config.app_token}/tables/"
        f"{table_id}/records"
    )
    payload = feishu_request(
        method="POST",
        url=url,
        headers=headers,
        body={"fields": fields},
        timeout=request_timeout,
    )
    created_id = str(((payload.get("data") or {}).get("record") or {}).get("record_id") or "")
    if created_id:
        existing_by_key[run_key] = created_id
    logging.info("Feishu created summary record run_key=%s", run_key)


def sync_company_results_to_feishu(
    *,
    config: FeishuConfig,
    tenant_access_token: str,
    row: dict[str, str],
    prompts: list[PromptRow],
    report_date: str,
    output_root: Path,
    provider_name: str,
    model: str,
    request_timeout: int,
    doc_repo: DocRepo,
    run_repo: RunRepo,
) -> None:
    company = row["company"].strip()
    ticker = row["Ticker"].strip()
    output_dir = run_repo.build_output_dir(
        output_root=output_root,
        ticker=ticker,
        report_date=report_date,
    )
    if not output_dir.exists():
        logging.warning("Feishu sync skipped: output dir not found for %s %s", company, ticker)
        return

    docs: list[SyncDoc] = []
    for prompt in prompts:
        file_path = run_repo.build_output_path(
            output_root=output_root,
            ticker=ticker,
            report_date=report_date,
            prompt=prompt,
        )
        if not file_path.exists():
            logging.warning("Feishu sync skipped missing file: %s", file_path)
            continue
        answer, sources = doc_repo.parse_saved_markdown_for_sync(file_path)
        docs.append(
            SyncDoc(
                prompt_id=prompt.prompt_id,
                question=prompt.question,
                answer=answer,
                sources=sources,
                output_path=str(file_path),
            )
        )
    if not docs:
        logging.info("Feishu sync skipped: no docs to sync for %s %s", company, ticker)
        return

    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    ensure_feishu_required_fields(
        config=config,
        tenant_access_token=tenant_access_token,
        timeout=request_timeout,
    )
    existing_records = list_all_feishu_records(
        config=config,
        tenant_access_token=tenant_access_token,
        timeout=request_timeout,
    )
    existing_by_key: dict[str, str] = {}
    for item in existing_records:
        fields = item.get("fields") or {}
        sync_key = str(fields.get("sync_key") or "").strip()
        record_id = str(item.get("record_id") or "").strip()
        if sync_key and record_id:
            existing_by_key[sync_key] = record_id

    for doc in docs:
        sync_key = f"{ticker}|{report_date}|{doc.prompt_id}"
        fields = {
            "sync_key": sync_key,
            "company": company,
            "ticker": ticker,
            "report_date": report_date,
            "prompt_id": doc.prompt_id,
            "question": doc.question,
            "answer_markdown": doc.answer,
            "sources": "\n".join(doc.sources),
            "provider": provider_name,
            "model": model,
            "output_path": doc.output_path,
            "synced_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        record_id = existing_by_key.get(sync_key)
        if record_id:
            url = (
                f"{config.base_url}/open-apis/bitable/v1/apps/{config.app_token}/tables/"
                f"{config.table_id}/records/{record_id}"
            )
            feishu_request(
                method="PUT",
                url=url,
                headers=headers,
                body={"fields": fields},
                timeout=request_timeout,
            )
            logging.info("Feishu updated record sync_key=%s", sync_key)
        else:
            url = (
                f"{config.base_url}/open-apis/bitable/v1/apps/{config.app_token}/tables/"
                f"{config.table_id}/records"
            )
            payload = feishu_request(
                method="POST",
                url=url,
                headers=headers,
                body={"fields": fields},
                timeout=request_timeout,
            )
            created_id = str(((payload.get("data") or {}).get("record") or {}).get("record_id") or "")
            if created_id:
                existing_by_key[sync_key] = created_id
            logging.info("Feishu created record sync_key=%s", sync_key)

    sync_company_summary_to_feishu(
        config=config,
        tenant_access_token=tenant_access_token,
        row=row,
        report_date=report_date,
        output_dir=output_dir,
        request_timeout=request_timeout,
    )


class FeishuSyncDispatcher:
    def __init__(
        self,
        *,
        config: FeishuConfig,
        prompts: list[PromptRow],
        report_date: str,
        output_root: Path,
        provider_name: str,
        model: str,
        request_timeout: int,
        doc_repo: DocRepo,
        run_repo: RunRepo,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        dead_letter_path: Path | None = None,
    ) -> None:
        self.config = config
        self.prompts = prompts
        self.report_date = report_date
        self.output_root = output_root
        self.provider_name = provider_name
        self.model = model
        self.request_timeout = request_timeout
        self.doc_repo = doc_repo
        self.run_repo = run_repo
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.dead_letter_path = dead_letter_path

        self._queue: queue.Queue[FeishuSyncTask | None] = queue.Queue()
        self._worker: threading.Thread | None = None
        self._started = False
        self._closed = False
        self._tenant_access_token: str | None = None
        self._token_lock = threading.Lock()
        self._enqueued = 0
        self._succeeded = 0
        self._failed = 0

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._worker = threading.Thread(
            target=self._run_worker,
            name="feishu-sync-worker",
            daemon=True,
        )
        self._worker.start()
        logging.info("Feishu async dispatcher started")

    def enqueue(self, *, row: dict[str, str]) -> None:
        if self._closed:
            raise RuntimeError("FeishuSyncDispatcher already closed")
        if not self._started:
            self.start()
        self._queue.put(FeishuSyncTask(row=dict(row)))
        self._enqueued += 1
        logging.info(
            "Feishu sync enqueued company=%s ticker=%s queue_size=%s",
            row.get("company", "").strip(),
            row.get("Ticker", "").strip(),
            self._queue.qsize(),
        )

    def close(self, *, flush_timeout: float = 20.0) -> None:
        if self._closed:
            return
        self._closed = True
        if not self._started:
            return
        self._queue.put(None)
        worker = self._worker
        if worker:
            worker.join(timeout=max(0.0, flush_timeout))
            if worker.is_alive():
                pending = self._queue.qsize()
                logging.warning(
                    "Feishu async dispatcher close timeout. pending_tasks=%s", pending
                )
        logging.info(
            "Feishu async dispatcher stopped enqueued=%s succeeded=%s failed=%s",
            self._enqueued,
            self._succeeded,
            self._failed,
        )

    def _run_worker(self) -> None:
        while True:
            task = self._queue.get()
            try:
                if task is None:
                    return
                self._handle_task(task)
            finally:
                self._queue.task_done()

    def _handle_task(self, task: FeishuSyncTask) -> None:
        company = task.row.get("company", "").strip()
        ticker = task.row.get("Ticker", "").strip()
        for attempt in range(1, self.max_retries + 1):
            try:
                tenant_access_token = self._get_tenant_access_token(refresh=(attempt > 1))
                sync_company_results_to_feishu(
                    config=self.config,
                    tenant_access_token=tenant_access_token,
                    row=task.row,
                    prompts=self.prompts,
                    report_date=self.report_date,
                    output_root=self.output_root,
                    provider_name=self.provider_name,
                    model=self.model,
                    request_timeout=self.request_timeout,
                    doc_repo=self.doc_repo,
                    run_repo=self.run_repo,
                )
                self._succeeded += 1
                logging.info(
                    "Feishu async sync completed company=%s ticker=%s attempt=%s/%s",
                    company,
                    ticker,
                    attempt,
                    self.max_retries,
                )
                return
            except Exception as exc:
                logging.exception(
                    "Feishu async sync failed company=%s ticker=%s attempt=%s/%s",
                    company,
                    ticker,
                    attempt,
                    self.max_retries,
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    self._failed += 1
                    self._write_dead_letter(task=task, exc=exc)

    def _get_tenant_access_token(self, *, refresh: bool) -> str:
        with self._token_lock:
            if refresh or not self._tenant_access_token:
                self._tenant_access_token = get_feishu_tenant_access_token(
                    self.config,
                    timeout=self.request_timeout,
                )
            return self._tenant_access_token

    def _write_dead_letter(self, *, task: FeishuSyncTask, exc: Exception) -> None:
        if not self.dead_letter_path:
            return
        self.dead_letter_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "failed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "company": task.row.get("company", ""),
            "ticker": task.row.get("Ticker", ""),
            "report_date": self.report_date,
            "provider": self.provider_name,
            "model": self.model,
            "error": str(exc),
        }
        with self.dead_letter_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def run_feishu_sync_test(*, config: FeishuConfig, request_timeout: int) -> None:
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    test_key = f"__feishu_sync_test__{int(time.time())}"
    logging.info(
        "Running Feishu sync test app_token=%s table_id=%s test_key=%s",
        config.app_token,
        config.table_id,
        test_key,
    )
    token = get_feishu_tenant_access_token(config, timeout=request_timeout)
    headers = {"Authorization": f"Bearer {token}"}
    ensure_feishu_required_fields(
        config=config,
        tenant_access_token=token,
        timeout=request_timeout,
    )
    existing_records = list_all_feishu_records(
        config=config,
        tenant_access_token=token,
        timeout=request_timeout,
    )
    logging.info("Feishu sync test read check passed. records_count=%s", len(existing_records))

    create_url = (
        f"{config.base_url}/open-apis/bitable/v1/apps/{config.app_token}/tables/"
        f"{config.table_id}/records"
    )
    create_payload = {
        "fields": {
            "sync_key": test_key,
            "company": "SYNC_TEST_COMPANY",
            "ticker": "SYNC.TEST",
            "report_date": date.today().isoformat(),
            "prompt_id": "0",
            "question": "feishu sync self-test",
            "answer_markdown": f"created at {started_at}",
            "sources": "N/A",
            "provider": "sync-test",
            "model": "sync-test",
            "output_path": "N/A",
            "synced_at": started_at,
        }
    }
    created = feishu_request(
        method="POST",
        url=create_url,
        headers=headers,
        body=create_payload,
        timeout=request_timeout,
    )
    record_id = str(((created.get("data") or {}).get("record") or {}).get("record_id") or "").strip()
    if not record_id:
        raise RuntimeError("Feishu sync test failed: create returned empty record_id")
    logging.info("Feishu sync test create check passed. record_id=%s", record_id)

    update_url = (
        f"{config.base_url}/open-apis/bitable/v1/apps/{config.app_token}/tables/"
        f"{config.table_id}/records/{record_id}"
    )
    feishu_request(
        method="PUT",
        url=update_url,
        headers=headers,
        body={"fields": {"answer_markdown": f"updated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}},
        timeout=request_timeout,
    )
    logging.info("Feishu sync test update check passed. record_id=%s", record_id)

    delete_url = (
        f"{config.base_url}/open-apis/bitable/v1/apps/{config.app_token}/tables/"
        f"{config.table_id}/records/{record_id}"
    )
    try:
        feishu_request(
            method="DELETE",
            url=delete_url,
            headers=headers,
            timeout=request_timeout,
        )
        logging.info("Feishu sync test cleanup passed. record_id=%s", record_id)
    except Exception:
        logging.exception(
            "Feishu sync test cleanup failed. Please delete record manually. record_id=%s",
            record_id,
        )
