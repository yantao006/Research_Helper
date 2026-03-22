#!/usr/bin/env python3
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os
from datetime import datetime
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from research_batch.cli import parse_args
from research_batch.env_utils import (
    configure_logging,
    first_present_env,
    is_production_env,
    is_truthy,
    load_dotenv,
    resolve_api_base,
    resolve_model,
    resolve_provider,
)
from research_batch.feishu import (
    FeishuSyncDispatcher,
    get_feishu_tenant_access_token,
    resolve_feishu_config,
    run_feishu_sync_test,
    sync_company_results_to_feishu,
)
from research_batch.repositories import (
    DualCompanyRepo,
    DualDocRepo,
    DualFactRepo,
    DualJobRepo,
    DualRunRepo,
    LocalCompanyRepo,
    LocalDocRepo,
    LocalFactRepo,
    LocalJobRepo,
    LocalRunRepo,
)
from research_batch.postgres_repo import (
    PostgresCompanyRepo,
    PostgresDocRepo,
    PostgresFactRepo,
    PostgresJobRepo,
    PostgresRunRepo,
)
from research_batch.workflow import process_company, run_provider_test


def resolve_repo_backend(
    *,
    requested: str,
    is_production: bool,
    allow_non_postgres_in_production: bool,
) -> str:
    if requested != "auto":
        effective = requested
    else:
        effective = "postgres" if is_production else "local"

    if is_production and not allow_non_postgres_in_production and effective != "postgres":
        raise RuntimeError(
            "Production environment requires postgres repository backend. "
            "Set --repo-backend postgres (or REPO_BACKEND=postgres), "
            "or explicitly allow override via ALLOW_NON_POSTGRES_IN_PRODUCTION=true."
        )
    return effective


def resolve_prompt_profile(*, requested: str, is_production: bool) -> str:
    if requested != "auto":
        return requested
    return "production" if is_production else "smoke"


def resolve_prompts_path(*, project_root: Path, args, effective_profile: str) -> Path:
    # Keep backward compatibility: explicit --prompts always wins.
    if args.prompts and args.prompts != "prompts.csv":
        return project_root / args.prompts
    if effective_profile == "smoke":
        return project_root / args.smoke_prompts
    return project_root / "prompts.csv"


def main() -> int:
    project_root = Path.cwd()
    load_dotenv(project_root / ".env")
    args = parse_args()
    configure_logging(project_root / args.log_file)
    feishu_dispatcher: FeishuSyncDispatcher | None = None

    try:
        provider = resolve_provider(args.provider)
        feishu_config = resolve_feishu_config()
        api_base = resolve_api_base(provider)
        model = resolve_model(provider, args.model)
        enable_web_search = provider.supports_web_search and not args.disable_web_search
        production_env = is_production_env()
        allow_non_postgres_in_production = is_truthy(
            os.getenv("ALLOW_NON_POSTGRES_IN_PRODUCTION", "")
        )

        logging.info(
            "Using provider=%s api_style=%s model=%s api_base=%s web_search=%s web_search_mode=%s fact_pack_enabled=%s production=%s company_workers=%s prompt_workers=%s",
            provider.provider_id,
            provider.api_style,
            model,
            api_base,
            enable_web_search,
            args.web_search_mode,
            not args.disable_fact_pack,
            production_env,
            max(1, args.company_workers),
            max(1, args.prompt_workers),
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

        tasks_path = project_root / args.tasks
        prompt_profile = resolve_prompt_profile(
            requested=args.prompt_profile,
            is_production=production_env,
        )
        if (
            production_env
            and prompt_profile == "smoke"
            and not args.allow_smoke_prompts_in_production
        ):
            raise RuntimeError(
                "Production environment does not allow smoke prompts by default. "
                "Use --prompt-profile production, or explicitly pass "
                "--allow-smoke-prompts-in-production if this is intentional."
            )
        prompts_path = resolve_prompts_path(
            project_root=project_root,
            args=args,
            effective_profile=prompt_profile,
        )
        effective_repo_backend = resolve_repo_backend(
            requested=args.repo_backend,
            is_production=production_env,
            allow_non_postgres_in_production=allow_non_postgres_in_production,
        )
        if effective_repo_backend == "local":
            company_repo = LocalCompanyRepo()
            doc_repo = LocalDocRepo()
            fact_repo = LocalFactRepo()
            run_repo = LocalRunRepo()
            job_repo = LocalJobRepo()
            logging.info("Repository backend: local")
        elif effective_repo_backend == "postgres":
            if not (args.postgres_dsn or "").strip():
                raise RuntimeError(
                    "repo-backend=postgres requires --postgres-dsn or POSTGRES_DSN/DATABASE_URL"
                )
            dsn = args.postgres_dsn.strip()
            company_repo = PostgresCompanyRepo(dsn=dsn)
            doc_repo = PostgresDocRepo(dsn=dsn)
            fact_repo = PostgresFactRepo(dsn=dsn)
            run_repo = PostgresRunRepo(dsn=dsn)
            job_repo = PostgresJobRepo(dsn=dsn)
            logging.info("Repository backend: postgres")
        else:
            if not (args.postgres_dsn or "").strip():
                raise RuntimeError(
                    "repo-backend=dual requires --postgres-dsn or POSTGRES_DSN/DATABASE_URL"
                )
            dsn = args.postgres_dsn.strip()
            company_repo = DualCompanyRepo(
                primary=LocalCompanyRepo(),
                secondary=PostgresCompanyRepo(dsn=dsn),
                strict=args.dual_write_strict,
            )
            doc_repo = DualDocRepo(
                primary=LocalDocRepo(),
                secondary=PostgresDocRepo(dsn=dsn),
                strict=args.dual_write_strict,
            )
            fact_repo = DualFactRepo(
                primary=LocalFactRepo(),
                secondary=PostgresFactRepo(dsn=dsn),
                strict=args.dual_write_strict,
            )
            run_repo = DualRunRepo(
                primary=LocalRunRepo(),
                secondary=PostgresRunRepo(dsn=dsn),
                strict=args.dual_write_strict,
            )
            job_repo = DualJobRepo(
                primary=LocalJobRepo(),
                secondary=PostgresJobRepo(dsn=dsn),
                strict=args.dual_write_strict,
            )
            logging.info("Repository backend: dual (primary=local, secondary=postgres)")
        logging.info(
            "Repository backend resolved requested=%s effective=%s",
            args.repo_backend,
            effective_repo_backend,
        )
        if effective_repo_backend == "local" and (args.postgres_dsn or "").strip():
            logging.warning(
                "POSTGRES_DSN is set but repository backend is local; this run will NOT write research data to Postgres."
            )
        logging.info(
            "Prompt profile resolved requested=%s effective=%s prompts_path=%s",
            args.prompt_profile,
            prompt_profile,
            prompts_path,
        )

        prompts = doc_repo.load_prompts(prompts_path)
        tasks, fieldnames = company_repo.load_tasks(tasks_path)
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
                        doc_repo=doc_repo,
                        run_repo=run_repo,
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

        if feishu_config:
            feishu_dispatcher = FeishuSyncDispatcher(
                config=feishu_config,
                prompts=prompts,
                report_date=args.report_date,
                output_root=project_root / args.output_root,
                provider_name=provider.display_name,
                model=model,
                request_timeout=args.request_timeout,
                doc_repo=doc_repo,
                run_repo=run_repo,
                max_retries=max(1, args.feishu_sync_max_retries),
                retry_delay=max(0.0, args.feishu_sync_retry_delay),
                dead_letter_path=project_root / args.feishu_dead_letter,
            )
            feishu_dispatcher.start()

        updated = False
        rows_to_process = [
            row
            for row in tasks_to_process
            if not is_truthy(row.get("analyzed", "")) or args.force_rerun
        ]
        company_workers = min(max(1, args.company_workers), max(1, len(rows_to_process)))

        def _handle_success(row: dict[str, str]) -> None:
            nonlocal updated
            row["analyzed"] = "True"
            row["analyzed_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            company_repo.save_tasks(
                tasks_path,
                fieldnames=fieldnames,
                rows=tasks,
            )
            updated = True
            logging.info(
                "Marked analyzed company=%s ticker=%s",
                row["company"].strip(),
                row["Ticker"].strip(),
            )
            if feishu_dispatcher:
                try:
                    feishu_dispatcher.enqueue(row=row)
                except Exception:
                    logging.exception(
                        "Feishu async enqueue failed company=%s ticker=%s",
                        row["company"].strip(),
                        row["Ticker"].strip(),
                    )

        if company_workers == 1:
            for row in rows_to_process:
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
                    web_search_mode=args.web_search_mode,
                    fact_pack_enabled=not args.disable_fact_pack,
                    strict_fact_pack=args.strict_fact_pack,
                    force_rerun=args.force_rerun,
                    prompt_workers=max(1, args.prompt_workers),
                    fact_repo=fact_repo,
                    doc_repo=doc_repo,
                    run_repo=run_repo,
                    job_repo=job_repo,
                )

                if succeeded:
                    _handle_success(row)
        else:
            logging.info(
                "Processing companies concurrently companies=%s company_workers=%s",
                len(rows_to_process),
                company_workers,
            )
            with ThreadPoolExecutor(max_workers=company_workers) as executor:
                future_map = {
                    executor.submit(
                        process_company,
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
                        web_search_mode=args.web_search_mode,
                        fact_pack_enabled=not args.disable_fact_pack,
                        strict_fact_pack=args.strict_fact_pack,
                        force_rerun=args.force_rerun,
                        prompt_workers=max(1, args.prompt_workers),
                        fact_repo=fact_repo,
                        doc_repo=doc_repo,
                        run_repo=run_repo,
                        job_repo=job_repo,
                    ): row
                    for row in rows_to_process
                }
                for future in as_completed(future_map):
                    row = future_map[future]
                    try:
                        succeeded = future.result()
                    except Exception:
                        logging.exception(
                            "Company processing crashed company=%s ticker=%s",
                            row.get("company", "").strip(),
                            row.get("Ticker", "").strip(),
                        )
                        continue

                    if succeeded:
                        _handle_success(row)

        if not updated:
            logging.info("No unanalyzed tasks were completed in this run")
        if feishu_dispatcher:
            feishu_dispatcher.close(flush_timeout=max(0.0, args.feishu_async_flush_timeout))
        return 0
    except Exception as exc:
        logging.exception("Batch run failed: %s", exc)
        return 1
    finally:
        if feishu_dispatcher:
            try:
                feishu_dispatcher.close(flush_timeout=max(0.0, args.feishu_async_flush_timeout))
            except Exception:
                logging.exception("Failed to close Feishu async dispatcher")


if __name__ == "__main__":
    raise SystemExit(main())
