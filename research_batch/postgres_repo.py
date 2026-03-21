from __future__ import annotations

import json
import re
import time
import uuid
from pathlib import Path
from typing import Any

from research_batch.config import PromptRow
from research_batch.env_utils import is_truthy
from research_batch.storage import (
    parse_saved_markdown_for_sync,
    read_prompts,
    read_tasks,
    render_prompt,
    sanitize_filename,
    write_output,
)

try:
    import psycopg  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency at runtime.
    psycopg = None  # type: ignore


_SCHEMA_READY: set[str] = set()


def _require_psycopg() -> Any:
    if psycopg is None:
        raise RuntimeError(
            "Postgres backend requires psycopg. Install it with: pip install psycopg[binary]"
        )
    return psycopg


def _ensure_schema(dsn: str) -> None:
    if dsn in _SCHEMA_READY:
        return
    driver = _require_psycopg()
    with driver.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS rb_company_tasks (
                    ticker TEXT PRIMARY KEY,
                    company TEXT NOT NULL,
                    analyzed BOOLEAN NOT NULL DEFAULT FALSE,
                    analyzed_date TEXT NOT NULL DEFAULT '',
                    extra JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS rb_runs (
                    run_id TEXT PRIMARY KEY,
                    company TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    report_date TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    model TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT NOT NULL DEFAULT '',
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS rb_outputs (
                    output_path TEXT PRIMARY KEY,
                    ticker_slug TEXT NOT NULL,
                    report_date TEXT NOT NULL,
                    prompt_id TEXT NOT NULL,
                    question_slug TEXT NOT NULL,
                    markdown TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS rb_docs (
                    sync_key TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    company TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    report_date TEXT NOT NULL,
                    prompt_id TEXT NOT NULL,
                    question TEXT NOT NULL,
                    answer_markdown TEXT NOT NULL,
                    sources_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    provider TEXT NOT NULL,
                    model TEXT NOT NULL,
                    output_path TEXT NOT NULL,
                    markdown TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS rb_seo_keywords (
                    id BIGSERIAL PRIMARY KEY,
                    sync_key TEXT NOT NULL,
                    keyword TEXT NOT NULL,
                    keyword_norm TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(sync_key, keyword_norm)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS rb_jobs (
                    job_id TEXT PRIMARY KEY,
                    company TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS rb_job_events (
                    id BIGSERIAL PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    message TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_rb_docs_ticker_date ON rb_docs (ticker, report_date);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_rb_docs_answer_search ON rb_docs USING GIN (to_tsvector('simple', coalesce(answer_markdown, '')));"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_rb_seo_keywords_norm ON rb_seo_keywords (keyword_norm);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_rb_job_events_job_id ON rb_job_events (job_id);"
            )
        conn.commit()
    _SCHEMA_READY.add(dsn)


def _decode_extra(raw: Any) -> dict[str, str]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(k): "" if v is None else str(v) for k, v in raw.items()}
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return {str(k): "" if v is None else str(v) for k, v in parsed.items()}
    return {}


def _parse_output_path(path: Path) -> tuple[str, str, str, str]:
    parent = path.parent.name
    if "_" in parent:
        ticker_slug, report_date = parent.rsplit("_", 1)
    else:
        ticker_slug, report_date = parent, ""
    stem = path.stem
    if "_" in stem:
        prompt_id, question_slug = stem.split("_", 1)
    else:
        prompt_id, question_slug = stem, ""
    return ticker_slug, report_date, prompt_id, question_slug


def _extract_seo_keywords(answer_markdown: str, markdown: str) -> list[str]:
    content = markdown.strip() or answer_markdown.strip()
    if not content:
        return []
    lines = content.splitlines()
    start = -1
    for idx, line in enumerate(lines):
        if line.strip() == "## SEO 关键词":
            start = idx
            break
    if start < 0:
        return []

    end = len(lines)
    for idx in range(start + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            end = idx
            break

    linked_re = re.compile(r"^- \[([^\]]+)\]\([^)]+\)\s*$")
    bullet_re = re.compile(r"^- (.+?)\s*$")
    result: list[str] = []
    seen: set[str] = set()
    for raw in lines[start + 1 : end]:
        line = raw.strip()
        if not line:
            continue
        keyword = ""
        linked = linked_re.match(line)
        if linked:
            keyword = linked.group(1).strip()
        else:
            bullet = bullet_re.match(line)
            if bullet:
                keyword = bullet.group(1).strip()
        if not keyword:
            continue
        key = keyword.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(keyword)
        if len(result) >= 30:
            break
    return result


class _PostgresBase:
    def __init__(self, *, dsn: str):
        self.dsn = dsn.strip()
        if not self.dsn:
            raise RuntimeError("Postgres backend requires a non-empty DSN")
        _ensure_schema(self.dsn)

    def _connect(self) -> Any:
        driver = _require_psycopg()
        return driver.connect(self.dsn)


class PostgresCompanyRepo(_PostgresBase):
    def load_tasks(self, path: Path) -> tuple[list[dict[str, str]], list[str]]:
        rows_data: list[tuple[Any, ...]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT company, ticker, analyzed, analyzed_date, extra
                    FROM rb_company_tasks
                    ORDER BY ticker
                    """
                )
                rows_data = list(cur.fetchall())

        if not rows_data:
            csv_rows, fieldnames = read_tasks(path)
            self.save_tasks(path, fieldnames=fieldnames, rows=csv_rows)
            return csv_rows, fieldnames

        required = ["company", "Ticker", "analyzed", "analyzed_date"]
        extra_keys: set[str] = set()
        decoded: list[tuple[str, str, bool, str, dict[str, str]]] = []
        for item in rows_data:
            company, ticker, analyzed, analyzed_date, extra = item
            extra_dict = _decode_extra(extra)
            extra_keys.update(extra_dict.keys())
            decoded.append(
                (
                    str(company or ""),
                    str(ticker or ""),
                    bool(analyzed),
                    str(analyzed_date or ""),
                    extra_dict,
                )
            )
        fieldnames = required + sorted(k for k in extra_keys if k not in required)
        rows: list[dict[str, str]] = []
        for company, ticker, analyzed, analyzed_date, extra in decoded:
            row = {
                "company": company,
                "Ticker": ticker,
                "analyzed": "True" if analyzed else "False",
                "analyzed_date": analyzed_date,
            }
            for key in fieldnames:
                if key not in row:
                    row[key] = extra.get(key, "")
            rows.append(row)
        return rows, fieldnames

    def save_tasks(
        self,
        path: Path,
        *,
        fieldnames: list[str],
        rows: list[dict[str, str]],
    ) -> None:
        _ = path
        required = {"company", "Ticker", "analyzed", "analyzed_date"}
        with self._connect() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    company = (row.get("company") or "").strip()
                    ticker = (row.get("Ticker") or "").strip()
                    if not company or not ticker:
                        continue
                    analyzed = is_truthy(str(row.get("analyzed", "")))
                    analyzed_date = str(row.get("analyzed_date", "") or "")
                    extra = {
                        key: str(value or "")
                        for key, value in row.items()
                        if key not in required
                    }
                    cur.execute(
                        """
                        INSERT INTO rb_company_tasks
                            (ticker, company, analyzed, analyzed_date, extra, updated_at)
                        VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
                        ON CONFLICT (ticker) DO UPDATE SET
                            company = EXCLUDED.company,
                            analyzed = EXCLUDED.analyzed,
                            analyzed_date = EXCLUDED.analyzed_date,
                            extra = EXCLUDED.extra,
                            updated_at = NOW()
                        """,
                        (
                            ticker,
                            company,
                            analyzed,
                            analyzed_date,
                            json.dumps(extra, ensure_ascii=False),
                        ),
                    )
            conn.commit()


class PostgresRunRepo(_PostgresBase):
    def build_output_dir(self, *, output_root: Path, ticker: str, report_date: str) -> Path:
        return output_root / f"{sanitize_filename(ticker)}_{report_date}"

    def build_output_path(
        self,
        *,
        output_root: Path,
        ticker: str,
        report_date: str,
        prompt: PromptRow,
    ) -> Path:
        output_dir = self.build_output_dir(
            output_root=output_root,
            ticker=ticker,
            report_date=report_date,
        )
        safe_question = sanitize_filename(prompt.question)
        return output_dir / f"{prompt.prompt_id}_{safe_question}.md"

    def output_exists(self, path: Path) -> bool:
        if path.exists():
            return True
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM rb_outputs WHERE output_path = %s LIMIT 1",
                    (str(path),),
                )
                return cur.fetchone() is not None

    def write_output(self, path: Path, content: str, *, force_rerun: bool) -> bool:
        wrote = write_output(path, content, force_rerun=force_rerun)
        ticker_slug, report_date, prompt_id, question_slug = _parse_output_path(path)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rb_outputs
                        (
                            output_path,
                            ticker_slug,
                            report_date,
                            prompt_id,
                            question_slug,
                            markdown,
                            updated_at
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (output_path) DO UPDATE SET
                        markdown = EXCLUDED.markdown,
                        updated_at = NOW()
                    """,
                    (
                        str(path),
                        ticker_slug,
                        report_date,
                        prompt_id,
                        question_slug,
                        content,
                    ),
                )
            conn.commit()
        return wrote

    def begin_run(
        self,
        *,
        company: str,
        ticker: str,
        report_date: str,
        provider_name: str,
        model: str,
    ) -> str:
        run_id = (
            f"{sanitize_filename(ticker)}:{report_date}:{int(time.time() * 1000)}:"
            f"{uuid.uuid4().hex[:8]}"
        )
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rb_runs
                        (run_id, company, ticker, report_date, provider, model, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (run_id, company, ticker, report_date, provider_name, model, "running"),
                )
            conn.commit()
        return run_id

    def finish_run(
        self,
        *,
        run_id: str,
        success: bool,
        error_message: str = "",
    ) -> None:
        status = "success" if success else "failed"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE rb_runs
                    SET status = %s, error_message = %s, finished_at = NOW()
                    WHERE run_id = %s
                    """,
                    (status, error_message, run_id),
                )
            conn.commit()


class PostgresDocRepo(_PostgresBase):
    def load_prompts(self, path: Path) -> list[PromptRow]:
        return read_prompts(path)

    def render_prompt(
        self,
        *,
        template: str,
        company: str,
        ticker: str,
        report_date: str,
    ) -> str:
        return render_prompt(
            template,
            company=company,
            ticker=ticker,
            report_date=report_date,
        )

    def parse_saved_markdown_for_sync(self, path: Path) -> tuple[str, list[str]]:
        return parse_saved_markdown_for_sync(path)

    def save_research_doc(
        self,
        *,
        run_id: str,
        company: str,
        ticker: str,
        report_date: str,
        prompt_id: str,
        question: str,
        answer_markdown: str,
        sources: list[dict[str, str]],
        provider_name: str,
        model: str,
        output_path: str,
        markdown: str,
    ) -> None:
        sync_key = f"{ticker}|{report_date}|{prompt_id}"
        keywords = _extract_seo_keywords(answer_markdown=answer_markdown, markdown=markdown)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rb_docs
                        (
                            sync_key,
                            run_id,
                            company,
                            ticker,
                            report_date,
                            prompt_id,
                            question,
                            answer_markdown,
                            sources_json,
                            provider,
                            model,
                            output_path,
                            markdown,
                            updated_at
                        )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, NOW()
                    )
                    ON CONFLICT (sync_key) DO UPDATE SET
                        run_id = EXCLUDED.run_id,
                        company = EXCLUDED.company,
                        question = EXCLUDED.question,
                        answer_markdown = EXCLUDED.answer_markdown,
                        sources_json = EXCLUDED.sources_json,
                        provider = EXCLUDED.provider,
                        model = EXCLUDED.model,
                        output_path = EXCLUDED.output_path,
                        markdown = EXCLUDED.markdown,
                        updated_at = NOW()
                    """,
                    (
                        sync_key,
                        run_id,
                        company,
                        ticker,
                        report_date,
                        prompt_id,
                        question,
                        answer_markdown,
                        json.dumps(sources, ensure_ascii=False),
                        provider_name,
                        model,
                        output_path,
                        markdown,
                    ),
                )
                cur.execute("DELETE FROM rb_seo_keywords WHERE sync_key = %s", (sync_key,))
                for keyword in keywords:
                    cur.execute(
                        """
                        INSERT INTO rb_seo_keywords (sync_key, keyword, keyword_norm)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (sync_key, keyword_norm) DO UPDATE SET
                            keyword = EXCLUDED.keyword
                        """,
                        (sync_key, keyword, keyword.casefold()),
                    )
            conn.commit()


class PostgresJobRepo(_PostgresBase):
    def begin_company(self, *, company: str, ticker: str) -> str:
        job_id = (
            f"{sanitize_filename(ticker)}:{int(time.time() * 1000)}:{uuid.uuid4().hex[:6]}"
        )
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rb_jobs (job_id, company, ticker, status)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (job_id, company, ticker, "running"),
                )
            conn.commit()
        return job_id

    def add_event(self, *, job_id: str, message: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rb_job_events (job_id, message)
                    VALUES (%s, %s)
                    """,
                    (job_id, message),
                )
            conn.commit()

    def finish_company(self, *, job_id: str, success: bool) -> None:
        status = "success" if success else "failed"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE rb_jobs
                    SET status = %s, finished_at = NOW()
                    WHERE job_id = %s
                    """,
                    (status, job_id),
                )
            conn.commit()
