from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Any

from research_batch.config import REQUIRED_FEISHU_FIELDS, FeishuConfig, PromptRow, SyncDoc
from research_batch.env_utils import is_falsy, require_env
from research_batch.storage import parse_saved_markdown_for_sync, sanitize_filename


def resolve_feishu_config() -> FeishuConfig | None:
    enabled_value = os.getenv("FEISHU_ENABLE_SYNC", "false")
    if is_falsy(enabled_value):
        return None
    return FeishuConfig(
        app_id=require_env("FEISHU_APP_ID"),
        app_secret=require_env("FEISHU_APP_SECRET"),
        app_token=require_env("FEISHU_APP_TOKEN"),
        table_id=require_env("FEISHU_TABLE_ID"),
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
) -> set[str]:
    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    page_token = ""
    names: set[str] = set()
    while True:
        query = {"page_size": "500"}
        if page_token:
            query["page_token"] = page_token
        url = (
            f"{config.base_url}/open-apis/bitable/v1/apps/{config.app_token}/tables/{config.table_id}/fields?"
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
) -> None:
    existing = list_feishu_field_names(
        config=config,
        tenant_access_token=tenant_access_token,
        timeout=timeout,
    )
    missing = sorted(REQUIRED_FEISHU_FIELDS - existing)
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
) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    page_token = ""
    records: list[dict[str, Any]] = []
    while True:
        query = {"page_size": "500"}
        if page_token:
            query["page_token"] = page_token
        url = (
            f"{config.base_url}/open-apis/bitable/v1/apps/{config.app_token}/tables/{config.table_id}/records?"
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
) -> None:
    company = row["company"].strip()
    ticker = row["Ticker"].strip()
    output_dir = output_root / f"{sanitize_filename(ticker)}_{report_date}"
    if not output_dir.exists():
        logging.warning("Feishu sync skipped: output dir not found for %s %s", company, ticker)
        return

    docs: list[SyncDoc] = []
    for prompt in prompts:
        file_path = output_dir / f"{prompt.prompt_id}_{sanitize_filename(prompt.question)}.md"
        if not file_path.exists():
            logging.warning("Feishu sync skipped missing file: %s", file_path)
            continue
        answer, sources = parse_saved_markdown_for_sync(file_path)
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
