#!/usr/bin/env python3
from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from research_batch.cli import parse_args
from research_batch.env_utils import (
    configure_logging,
    first_present_env,
    is_truthy,
    load_dotenv,
    resolve_api_base,
    resolve_model,
    resolve_provider,
)
from research_batch.feishu import (
    get_feishu_tenant_access_token,
    resolve_feishu_config,
    run_feishu_sync_test,
    sync_company_results_to_feishu,
)
from research_batch.storage import read_prompts, read_tasks, write_tasks
from research_batch.workflow import process_company, run_provider_test


def main() -> int:
    args = parse_args()
    project_root = Path.cwd()
    load_dotenv(project_root / ".env")
    configure_logging(project_root / args.log_file)

    try:
        provider = resolve_provider(args.provider)
        feishu_config = resolve_feishu_config()
        api_base = resolve_api_base(provider)
        model = resolve_model(provider, args.model)
        enable_web_search = provider.supports_web_search and not args.disable_web_search

        logging.info(
            "Using provider=%s api_style=%s model=%s api_base=%s web_search=%s",
            provider.provider_id,
            provider.api_style,
            model,
            api_base,
            enable_web_search,
        )
        if not provider.supports_web_search and not args.disable_web_search:
            logging.info(
                "Provider %s does not use built-in web_search in this script; continuing without it.",
                provider.provider_id,
            )
        if feishu_config:
            logging.info(
                "Feishu sync enabled app_token=%s table_id=%s",
                feishu_config.app_token,
                feishu_config.table_id,
            )
        else:
            logging.info("Feishu sync disabled")

        if args.feishu_sync_test:
            if not feishu_config:
                raise RuntimeError(
                    "Feishu sync test requires FEISHU_ENABLE_SYNC=true and Feishu env vars."
                )
            run_feishu_sync_test(config=feishu_config, request_timeout=args.request_timeout)
            logging.info("Feishu sync test completed")
            return 0

        prompts_path = project_root / args.prompts
        tasks_path = project_root / args.tasks
        prompts = read_prompts(prompts_path)
        tasks, fieldnames = read_tasks(tasks_path)
        tasks_to_process = tasks
        if args.only_ticker:
            target = args.only_ticker.strip().upper()
            tasks_to_process = [
                row for row in tasks if row.get("Ticker", "").strip().upper() == target
            ]
            if not tasks_to_process:
                logging.warning("No task row found for only-ticker=%s", target)
                return 0

        if args.feishu_sync_only:
            if not feishu_config:
                raise RuntimeError(
                    "Feishu sync only requires FEISHU_ENABLE_SYNC=true and Feishu env vars."
                )
            access_token = get_feishu_tenant_access_token(
                feishu_config, timeout=args.request_timeout
            )
            synced_companies = 0
            for row in tasks_to_process:
                try:
                    sync_company_results_to_feishu(
                        config=feishu_config,
                        tenant_access_token=access_token,
                        row=row,
                        prompts=prompts,
                        report_date=args.report_date,
                        output_root=project_root / args.output_root,
                        provider_name=provider.display_name,
                        model=model,
                        request_timeout=args.request_timeout,
                    )
                    synced_companies += 1
                    logging.info(
                        "Feishu sync-only finished company=%s ticker=%s",
                        row["company"].strip(),
                        row["Ticker"].strip(),
                    )
                except Exception:
                    logging.exception(
                        "Feishu sync-only failed company=%s ticker=%s",
                        row.get("company", "").strip(),
                        row.get("Ticker", "").strip(),
                    )
            logging.info("Feishu sync-only completed. companies_processed=%s", synced_companies)
            return 0

        api_key_env, api_key = first_present_env(provider.api_key_envs)
        logging.info("Using API key env var: %s", api_key_env)

        if args.provider_test:
            run_provider_test(
                provider=provider,
                api_key=api_key,
                api_base=api_base,
                model=model,
                request_timeout=args.request_timeout,
                enable_web_search=enable_web_search,
            )
            return 0

        updated = False
        for row in tasks_to_process:
            if is_truthy(row.get("analyzed", "")):
                continue

            succeeded = process_company(
                row=row,
                prompts=prompts,
                api_key=api_key,
                provider=provider,
                api_base=api_base,
                model=model,
                report_date=args.report_date,
                output_root=project_root / args.output_root,
                max_retries=args.max_retries,
                retry_delay=args.retry_delay,
                request_timeout=args.request_timeout,
                enable_web_search=enable_web_search,
                force_rerun=args.force_rerun,
            )

            if succeeded:
                row["analyzed"] = "True"
                row["analyzed_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                write_tasks(tasks_path, fieldnames, tasks)
                updated = True
                logging.info(
                    "Marked analyzed company=%s ticker=%s",
                    row["company"].strip(),
                    row["Ticker"].strip(),
                )
                if feishu_config:
                    try:
                        access_token = get_feishu_tenant_access_token(
                            feishu_config, timeout=args.request_timeout
                        )
                        for sync_attempt in range(1, 4):
                            try:
                                sync_company_results_to_feishu(
                                    config=feishu_config,
                                    tenant_access_token=access_token,
                                    row=row,
                                    prompts=prompts,
                                    report_date=args.report_date,
                                    output_root=project_root / args.output_root,
                                    provider_name=provider.display_name,
                                    model=model,
                                    request_timeout=args.request_timeout,
                                )
                                logging.info(
                                    "Feishu sync completed company=%s ticker=%s",
                                    row["company"].strip(),
                                    row["Ticker"].strip(),
                                )
                                break
                            except Exception:
                                logging.exception(
                                    "Feishu sync attempt=%s failed company=%s ticker=%s",
                                    sync_attempt,
                                    row["company"].strip(),
                                    row["Ticker"].strip(),
                                )
                                if sync_attempt < 3:
                                    time.sleep(2)
                    except Exception:
                        logging.exception(
                            "Feishu sync skipped due to auth/config error company=%s ticker=%s",
                            row["company"].strip(),
                            row["Ticker"].strip(),
                        )

        if not updated:
            logging.info("No unanalyzed tasks were completed in this run")
        return 0
    except Exception as exc:
        logging.exception("Batch run failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
