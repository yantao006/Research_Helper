from __future__ import annotations

import csv
import os
import re
from pathlib import Path
from tempfile import NamedTemporaryFile

from research_batch.config import PromptRow


def read_prompts(path: Path) -> list[PromptRow]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        expected = {"id", "question", "prompt"}
        missing = expected - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path} is missing columns: {sorted(missing)}")
        prompts: list[PromptRow] = []
        for index, row in enumerate(reader, start=2):
            prompt_id = (row.get("id") or "").strip()
            question = (row.get("question") or "").strip()
            prompt = (row.get("prompt") or "").strip()
            if not prompt_id or not question or not prompt:
                raise ValueError(f"{path}:{index} contains empty required fields")
            prompts.append(PromptRow(prompt_id=prompt_id, question=question, prompt=prompt))
    if not prompts:
        raise ValueError(f"{path} does not contain any prompts")
    return prompts


def read_tasks(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        expected = {"company", "Ticker", "analyzed", "analyzed_date"}
        missing = expected - set(fieldnames)
        if missing:
            raise ValueError(f"{path} is missing columns: {sorted(missing)}")
        return list(reader), fieldnames


def write_tasks(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=path.parent) as tmp:
        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        temp_name = tmp.name
    os.replace(temp_name, path)


def sanitize_filename(value: str) -> str:
    sanitized = re.sub(r"[^\w\-]+", "_", value.strip(), flags=re.UNICODE)
    sanitized = sanitized.strip("_")
    return sanitized or "untitled"


def render_prompt(template: str, company: str, ticker: str, report_date: str) -> str:
    return template.format(company=company, ticker=ticker, date=report_date)


def write_output(path: Path, content: str, force_rerun: bool) -> bool:
    if path.exists() and not force_rerun:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def parse_saved_markdown_content(raw: str) -> tuple[str, list[str]]:
    lines = raw.splitlines()
    answer_start = -1
    sources_start = -1
    for idx, line in enumerate(lines):
        if line.strip() == "## Answer":
            answer_start = idx
        elif line.strip() == "## Sources":
            sources_start = idx
            break

    answer_lines = (
        lines[answer_start + 1 : sources_start if sources_start >= 0 else None]
        if answer_start >= 0
        else []
    )
    answer = "\n".join(answer_lines).strip()

    sources: list[str] = []
    if sources_start >= 0:
        source_regex = re.compile(r"^- \[(.+?)\]\((https?://.+)\)$")
        for line in lines[sources_start + 1 :]:
            match = source_regex.match(line.strip())
            if match:
                sources.append(f"{match.group(1)} - {match.group(2)}")
    return answer, sources


def parse_saved_markdown_for_sync(path: Path) -> tuple[str, list[str]]:
    raw = path.read_text(encoding="utf-8")
    return parse_saved_markdown_content(raw)
