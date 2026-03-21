from __future__ import annotations

import argparse
import os
from datetime import date

from research_batch.config import DEFAULT_LOG_FILE, DEFAULT_OUTPUT_ROOT, PROVIDERS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch stock research with multiple LLM providers."
    )
    parser.add_argument("--prompts", default="prompts.csv", help="Path to prompts.csv")
    parser.add_argument("--tasks", default="tasks.csv", help="Path to tasks.csv")
    parser.add_argument(
        "--output-root", default=DEFAULT_OUTPUT_ROOT, help="Directory for markdown outputs"
    )
    parser.add_argument(
        "--provider",
        choices=sorted(PROVIDERS),
        default=os.getenv("MODEL_PROVIDER", "doubao"),
        help="Model provider preset",
    )
    parser.add_argument("--model", default=None, help="Override provider default model")
    parser.add_argument(
        "--report-date",
        default=date.today().isoformat(),
        help="Date string used to fill {date} and output folder names",
    )
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=5.0)
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=600,
        help="HTTP timeout in seconds for each model request",
    )
    parser.add_argument(
        "--log-file",
        default=DEFAULT_LOG_FILE,
        help="Path to the log file",
    )
    parser.add_argument(
        "--disable-web-search",
        action="store_true",
        help="Disable web_search even for providers that support it",
    )
    parser.add_argument(
        "--provider-test",
        action="store_true",
        help="Run a lightweight connectivity test for the selected provider and exit",
    )
    parser.add_argument(
        "--force-rerun",
        action="store_true",
        help="Regenerate outputs even if markdown files already exist",
    )
    parser.add_argument(
        "--feishu-sync-test",
        action="store_true",
        help="Run Feishu connectivity/read/write self-test and exit",
    )
    parser.add_argument(
        "--feishu-sync-only",
        action="store_true",
        help="Only sync existing local markdown results to Feishu and exit",
    )
    parser.add_argument(
        "--feishu-async-flush-timeout",
        type=float,
        default=float(os.getenv("FEISHU_ASYNC_FLUSH_TIMEOUT", "20")),
        help="Max seconds to wait for async Feishu worker flush on process exit",
    )
    parser.add_argument(
        "--feishu-sync-max-retries",
        type=int,
        default=int(os.getenv("FEISHU_SYNC_MAX_RETRIES", "3")),
        help="Async Feishu sync max retries per company task",
    )
    parser.add_argument(
        "--feishu-sync-retry-delay",
        type=float,
        default=float(os.getenv("FEISHU_SYNC_RETRY_DELAY", "2")),
        help="Async Feishu sync retry delay seconds",
    )
    parser.add_argument(
        "--feishu-dead-letter",
        default=os.getenv("FEISHU_SYNC_DEAD_LETTER", "logs/feishu_sync_dead_letter.jsonl"),
        help="Path to write failed async Feishu sync tasks as JSONL",
    )
    parser.add_argument(
        "--only-ticker",
        default=None,
        help="Only process/sync one ticker (e.g. 09992.HK or AAPL)",
    )
    parser.add_argument(
        "--repo-backend",
        choices=("auto", "local", "postgres", "dual"),
        default=os.getenv("REPO_BACKEND", "auto"),
        help="Repository backend for tasks/runs/docs/jobs persistence",
    )
    parser.add_argument(
        "--postgres-dsn",
        default=os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL"),
        help="Postgres DSN, required when --repo-backend=postgres|dual",
    )
    parser.add_argument(
        "--dual-write-strict",
        action="store_true",
        help="In dual mode, fail the run if Postgres secondary write/check fails",
    )
    return parser.parse_args()
