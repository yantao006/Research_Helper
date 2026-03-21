from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from research_batch.config import PromptRow
from research_batch.storage import (
    parse_saved_markdown_for_sync,
    read_prompts,
    read_tasks,
    render_prompt,
    sanitize_filename,
    write_output,
    write_tasks,
)


class CompanyRepo(Protocol):
    def load_tasks(self, path: Path) -> tuple[list[dict[str, str]], list[str]]:
        ...

    def save_tasks(
        self,
        path: Path,
        *,
        fieldnames: list[str],
        rows: list[dict[str, str]],
    ) -> None:
        ...


class RunRepo(Protocol):
    def build_output_dir(self, *, output_root: Path, ticker: str, report_date: str) -> Path:
        ...

    def build_output_path(
        self,
        *,
        output_root: Path,
        ticker: str,
        report_date: str,
        prompt: PromptRow,
    ) -> Path:
        ...

    def output_exists(self, path: Path) -> bool:
        ...

    def write_output(self, path: Path, content: str, *, force_rerun: bool) -> bool:
        ...

    def begin_run(
        self,
        *,
        company: str,
        ticker: str,
        report_date: str,
        provider_name: str,
        model: str,
    ) -> str:
        ...

    def finish_run(
        self,
        *,
        run_id: str,
        success: bool,
        error_message: str = "",
    ) -> None:
        ...


class DocRepo(Protocol):
    def load_prompts(self, path: Path) -> list[PromptRow]:
        ...

    def render_prompt(
        self,
        *,
        template: str,
        company: str,
        ticker: str,
        report_date: str,
    ) -> str:
        ...

    def parse_saved_markdown_for_sync(self, path: Path) -> tuple[str, list[str]]:
        ...

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
        ...


class JobRepo(Protocol):
    def begin_company(self, *, company: str, ticker: str) -> str:
        ...

    def add_event(self, *, job_id: str, message: str) -> None:
        ...

    def finish_company(self, *, job_id: str, success: bool) -> None:
        ...


class LocalCompanyRepo:
    def load_tasks(self, path: Path) -> tuple[list[dict[str, str]], list[str]]:
        return read_tasks(path)

    def save_tasks(
        self,
        path: Path,
        *,
        fieldnames: list[str],
        rows: list[dict[str, str]],
    ) -> None:
        write_tasks(path, fieldnames, rows)


class LocalRunRepo:
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
        return path.exists()

    def write_output(self, path: Path, content: str, *, force_rerun: bool) -> bool:
        return write_output(path, content, force_rerun=force_rerun)

    def begin_run(
        self,
        *,
        company: str,
        ticker: str,
        report_date: str,
        provider_name: str,
        model: str,
    ) -> str:
        return f"{sanitize_filename(ticker)}:{report_date}:{sanitize_filename(model)}"

    def finish_run(
        self,
        *,
        run_id: str,
        success: bool,
        error_message: str = "",
    ) -> None:
        _ = (run_id, success, error_message)


class LocalDocRepo:
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
        _ = (
            run_id,
            company,
            ticker,
            report_date,
            prompt_id,
            question,
            answer_markdown,
            sources,
            provider_name,
            model,
            output_path,
            markdown,
        )


@dataclass
class LocalJobRepo:
    events: dict[str, list[str]] = field(default_factory=dict)

    def begin_company(self, *, company: str, ticker: str) -> str:
        job_id = f"{sanitize_filename(ticker)}:{sanitize_filename(company)}"
        self.events.setdefault(job_id, [])
        return job_id

    def add_event(self, *, job_id: str, message: str) -> None:
        self.events.setdefault(job_id, []).append(message)

    def finish_company(self, *, job_id: str, success: bool) -> None:
        status = "success" if success else "failed"
        self.events.setdefault(job_id, []).append(f"finished:{status}")


def _on_secondary_error(*, op: str, strict: bool, exc: Exception) -> None:
    if strict:
        raise exc
    logging.warning("Dual-write secondary repo error op=%s error=%s", op, exc)


@dataclass
class DualCompanyRepo:
    primary: CompanyRepo
    secondary: CompanyRepo
    strict: bool = False

    def load_tasks(self, path: Path) -> tuple[list[dict[str, str]], list[str]]:
        primary_rows, primary_fields = self.primary.load_tasks(path)
        try:
            secondary_rows, _ = self.secondary.load_tasks(path)
            primary_by_ticker = {
                (row.get("Ticker") or "").strip().upper(): row for row in primary_rows
            }
            secondary_by_ticker = {
                (row.get("Ticker") or "").strip().upper(): row for row in secondary_rows
            }
            only_primary = sorted(set(primary_by_ticker) - set(secondary_by_ticker))
            only_secondary = sorted(set(secondary_by_ticker) - set(primary_by_ticker))
            mismatched: list[str] = []
            for ticker in sorted(set(primary_by_ticker) & set(secondary_by_ticker)):
                p = primary_by_ticker[ticker]
                s = secondary_by_ticker[ticker]
                if (p.get("company", "").strip() != s.get("company", "").strip()) or (
                    p.get("analyzed", "").strip().lower()
                    != s.get("analyzed", "").strip().lower()
                ):
                    mismatched.append(ticker)
            if only_primary or only_secondary or mismatched:
                logging.warning(
                    "Dual consistency check(company_tasks) mismatches: only_primary=%s only_secondary=%s value_mismatched=%s",
                    len(only_primary),
                    len(only_secondary),
                    len(mismatched),
                )
        except Exception as exc:
            _on_secondary_error(op="company.load_tasks", strict=self.strict, exc=exc)
        return primary_rows, primary_fields

    def save_tasks(
        self,
        path: Path,
        *,
        fieldnames: list[str],
        rows: list[dict[str, str]],
    ) -> None:
        self.primary.save_tasks(path, fieldnames=fieldnames, rows=rows)
        try:
            self.secondary.save_tasks(path, fieldnames=fieldnames, rows=rows)
        except Exception as exc:
            _on_secondary_error(op="company.save_tasks", strict=self.strict, exc=exc)


@dataclass
class DualRunRepo:
    primary: RunRepo
    secondary: RunRepo
    strict: bool = False
    _run_id_map: dict[str, str] = field(default_factory=dict)

    def build_output_dir(self, *, output_root: Path, ticker: str, report_date: str) -> Path:
        return self.primary.build_output_dir(
            output_root=output_root,
            ticker=ticker,
            report_date=report_date,
        )

    def build_output_path(
        self,
        *,
        output_root: Path,
        ticker: str,
        report_date: str,
        prompt: PromptRow,
    ) -> Path:
        return self.primary.build_output_path(
            output_root=output_root,
            ticker=ticker,
            report_date=report_date,
            prompt=prompt,
        )

    def output_exists(self, path: Path) -> bool:
        primary_exists = self.primary.output_exists(path)
        try:
            secondary_exists = self.secondary.output_exists(path)
            if primary_exists != secondary_exists:
                logging.warning(
                    "Dual consistency check(output_exists) mismatch path=%s primary=%s secondary=%s",
                    path,
                    primary_exists,
                    secondary_exists,
                )
        except Exception as exc:
            _on_secondary_error(op="run.output_exists", strict=self.strict, exc=exc)
        return primary_exists

    def write_output(self, path: Path, content: str, *, force_rerun: bool) -> bool:
        primary_written = self.primary.write_output(path, content, force_rerun=force_rerun)
        try:
            secondary_written = self.secondary.write_output(path, content, force_rerun=force_rerun)
            if primary_written != secondary_written:
                logging.warning(
                    "Dual consistency check(write_output) mismatch path=%s primary=%s secondary=%s",
                    path,
                    primary_written,
                    secondary_written,
                )
        except Exception as exc:
            _on_secondary_error(op="run.write_output", strict=self.strict, exc=exc)
        return primary_written

    def begin_run(
        self,
        *,
        company: str,
        ticker: str,
        report_date: str,
        provider_name: str,
        model: str,
    ) -> str:
        primary_run_id = self.primary.begin_run(
            company=company,
            ticker=ticker,
            report_date=report_date,
            provider_name=provider_name,
            model=model,
        )
        try:
            secondary_run_id = self.secondary.begin_run(
                company=company,
                ticker=ticker,
                report_date=report_date,
                provider_name=provider_name,
                model=model,
            )
            self._run_id_map[primary_run_id] = secondary_run_id
        except Exception as exc:
            _on_secondary_error(op="run.begin_run", strict=self.strict, exc=exc)
        return primary_run_id

    def finish_run(
        self,
        *,
        run_id: str,
        success: bool,
        error_message: str = "",
    ) -> None:
        self.primary.finish_run(run_id=run_id, success=success, error_message=error_message)
        secondary_run_id = self._run_id_map.get(run_id, run_id)
        try:
            self.secondary.finish_run(
                run_id=secondary_run_id,
                success=success,
                error_message=error_message,
            )
        except Exception as exc:
            _on_secondary_error(op="run.finish_run", strict=self.strict, exc=exc)


@dataclass
class DualDocRepo:
    primary: DocRepo
    secondary: DocRepo
    strict: bool = False

    def load_prompts(self, path: Path) -> list[PromptRow]:
        prompts = self.primary.load_prompts(path)
        try:
            secondary_prompts = self.secondary.load_prompts(path)
            if len(prompts) != len(secondary_prompts):
                logging.warning(
                    "Dual consistency check(prompts) mismatch primary=%s secondary=%s",
                    len(prompts),
                    len(secondary_prompts),
                )
        except Exception as exc:
            _on_secondary_error(op="doc.load_prompts", strict=self.strict, exc=exc)
        return prompts

    def render_prompt(
        self,
        *,
        template: str,
        company: str,
        ticker: str,
        report_date: str,
    ) -> str:
        return self.primary.render_prompt(
            template=template,
            company=company,
            ticker=ticker,
            report_date=report_date,
        )

    def parse_saved_markdown_for_sync(self, path: Path) -> tuple[str, list[str]]:
        return self.primary.parse_saved_markdown_for_sync(path)

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
        self.primary.save_research_doc(
            run_id=run_id,
            company=company,
            ticker=ticker,
            report_date=report_date,
            prompt_id=prompt_id,
            question=question,
            answer_markdown=answer_markdown,
            sources=sources,
            provider_name=provider_name,
            model=model,
            output_path=output_path,
            markdown=markdown,
        )
        try:
            self.secondary.save_research_doc(
                run_id=run_id,
                company=company,
                ticker=ticker,
                report_date=report_date,
                prompt_id=prompt_id,
                question=question,
                answer_markdown=answer_markdown,
                sources=sources,
                provider_name=provider_name,
                model=model,
                output_path=output_path,
                markdown=markdown,
            )
        except Exception as exc:
            _on_secondary_error(op="doc.save_research_doc", strict=self.strict, exc=exc)


@dataclass
class DualJobRepo:
    primary: JobRepo
    secondary: JobRepo
    strict: bool = False
    _job_id_map: dict[str, str] = field(default_factory=dict)

    def begin_company(self, *, company: str, ticker: str) -> str:
        primary_job_id = self.primary.begin_company(company=company, ticker=ticker)
        try:
            secondary_job_id = self.secondary.begin_company(company=company, ticker=ticker)
            self._job_id_map[primary_job_id] = secondary_job_id
        except Exception as exc:
            _on_secondary_error(op="job.begin_company", strict=self.strict, exc=exc)
        return primary_job_id

    def add_event(self, *, job_id: str, message: str) -> None:
        self.primary.add_event(job_id=job_id, message=message)
        secondary_job_id = self._job_id_map.get(job_id, job_id)
        try:
            self.secondary.add_event(job_id=secondary_job_id, message=message)
        except Exception as exc:
            _on_secondary_error(op="job.add_event", strict=self.strict, exc=exc)

    def finish_company(self, *, job_id: str, success: bool) -> None:
        self.primary.finish_company(job_id=job_id, success=success)
        secondary_job_id = self._job_id_map.get(job_id, job_id)
        try:
            self.secondary.finish_company(job_id=secondary_job_id, success=success)
        except Exception as exc:
            _on_secondary_error(op="job.finish_company", strict=self.strict, exc=exc)
