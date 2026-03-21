from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from research_batch.config import PromptRow


@dataclass(frozen=True)
class RouterExecutionProfile:
    profile_id: str
    generic_ids: list[str]
    industry_prompt_count: int
    industry_prompt_slots: list[str]
    notes: str


@dataclass(frozen=True)
class RouterCategory:
    category_id: str
    label: str
    description: str
    positive_keywords: list[str]
    exclude_keywords: list[str]
    industry_prompt_ids: list[str]
    slot_map: dict[str, str]


@dataclass(frozen=True)
class RouterConfig:
    version: str
    description: str
    generic_prompts_file: str
    industry_prompts_file: str
    method: str
    fallback_action: str
    high_confidence_threshold: float
    medium_confidence_threshold: float
    low_confidence_threshold: float
    execution_profiles: dict[str, RouterExecutionProfile]
    categories: dict[str, RouterCategory]
    routing_rule_names: list[str]


def _load_yaml_payload(path: Path) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass

    # Fallback for environments without PyYAML (for example Homebrew Python with PEP 668).
    # We rely on system Ruby's built-in YAML parser to keep router mode dependency-light.
    try:
        completed = subprocess.run(
            [
                "ruby",
                "-ryaml",
                "-rjson",
                "-e",
                "print JSON.generate(YAML.load_file(ARGV[0]))",
                str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(completed.stdout)
        if isinstance(payload, dict):
            return payload
    except Exception as exc:  # pragma: no cover - depends on environment
        raise RuntimeError(
            "Routing config parsing failed. Install PyYAML (pip install pyyaml) or ensure Ruby is available."
        ) from exc

    raise ValueError(f"{path} is not a valid YAML mapping")


def _as_list_of_str(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            out.append(text)
    return out


def load_router_config(path: Path) -> RouterConfig:
    payload = _load_yaml_payload(path)

    files = payload.get("files") or {}
    detection = payload.get("industry_detection") or {}
    thresholds = detection.get("confidence_thresholds") or {}
    profiles_payload = payload.get("execution_profiles") or {}
    categories_payload = detection.get("categories") or []
    routing_rules_payload = payload.get("routing_rules") or []

    execution_profiles: dict[str, RouterExecutionProfile] = {}
    for profile_id, profile_value in profiles_payload.items():
        if not isinstance(profile_value, dict):
            continue
        normalized_profile_id = str(profile_id).strip()
        if not normalized_profile_id:
            continue
        execution_profiles[normalized_profile_id] = RouterExecutionProfile(
            profile_id=normalized_profile_id,
            generic_ids=_as_list_of_str(profile_value.get("generic_ids")),
            industry_prompt_count=int(profile_value.get("industry_prompt_count") or 0),
            industry_prompt_slots=_as_list_of_str(profile_value.get("industry_prompt_slots")),
            notes=str(profile_value.get("notes") or "").strip(),
        )

    categories: dict[str, RouterCategory] = {}
    if isinstance(categories_payload, list):
        for raw in categories_payload:
            if not isinstance(raw, dict):
                continue
            category_id = str(raw.get("id") or "").strip()
            if not category_id:
                continue
            categories[category_id] = RouterCategory(
                category_id=category_id,
                label=str(raw.get("label") or "").strip(),
                description=str(raw.get("description") or "").strip(),
                positive_keywords=[k.lower() for k in _as_list_of_str(raw.get("positive_keywords"))],
                exclude_keywords=[k.lower() for k in _as_list_of_str(raw.get("exclude_keywords"))],
                industry_prompt_ids=_as_list_of_str(raw.get("industry_prompt_ids")),
                slot_map={
                    str(k).strip(): str(v).strip()
                    for k, v in (raw.get("slot_map") or {}).items()
                    if str(k).strip() and str(v).strip()
                },
            )

    if not execution_profiles:
        raise ValueError(f"{path} execution_profiles is empty")
    if not categories:
        raise ValueError(f"{path} industry_detection.categories is empty")
    high_threshold = float(thresholds.get("high") or 0.75)
    medium_threshold = float(thresholds.get("medium") or 0.6)
    low_threshold = float(thresholds.get("low") or 0.45)
    if not (0 <= low_threshold <= medium_threshold <= high_threshold <= 1):
        raise ValueError(
            f"{path} confidence thresholds must satisfy 0 <= low <= medium <= high <= 1"
        )

    routing_rule_names: list[str] = []
    if isinstance(routing_rules_payload, list):
        for item in routing_rules_payload:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if name:
                routing_rule_names.append(name)

    return RouterConfig(
        version=str(payload.get("version") or "").strip(),
        description=str(payload.get("description") or "").strip(),
        generic_prompts_file=str(files.get("generic_prompts") or "prompts.csv").strip(),
        industry_prompts_file=str(files.get("industry_prompts") or "industry_prompts.csv").strip(),
        method=str(detection.get("method") or "llm_first_then_keyword_fallback").strip(),
        fallback_action=str(detection.get("fallback_action") or "").strip(),
        high_confidence_threshold=high_threshold,
        medium_confidence_threshold=medium_threshold,
        low_confidence_threshold=low_threshold,
        execution_profiles=execution_profiles,
        categories=categories,
        routing_rule_names=routing_rule_names,
    )


def select_industry_prompt_ids(
    *,
    category: RouterCategory,
    profile: RouterExecutionProfile,
) -> list[str]:
    if profile.industry_prompt_count <= 0:
        return []

    ordered: list[str] = []
    seen: set[str] = set()

    if "all" in profile.industry_prompt_slots:
        for prompt_id in category.industry_prompt_ids:
            if prompt_id not in seen:
                ordered.append(prompt_id)
                seen.add(prompt_id)
    else:
        for slot in profile.industry_prompt_slots:
            prompt_id = category.slot_map.get(slot)
            if prompt_id and prompt_id not in seen:
                ordered.append(prompt_id)
                seen.add(prompt_id)

        # Fill remaining positions using category-defined default order.
        if len(ordered) < profile.industry_prompt_count:
            for prompt_id in category.industry_prompt_ids:
                if prompt_id in seen:
                    continue
                ordered.append(prompt_id)
                seen.add(prompt_id)
                if len(ordered) >= profile.industry_prompt_count:
                    break

    return ordered[: profile.industry_prompt_count]


def validate_router_bindings(
    *,
    router: RouterConfig,
    generic_prompts: list[PromptRow],
    industry_prompts: list[PromptRow],
) -> None:
    generic_ids = {prompt.prompt_id for prompt in generic_prompts}
    industry_ids = {prompt.prompt_id for prompt in industry_prompts}

    errors: list[str] = []
    for profile in router.execution_profiles.values():
        missing_generic = [prompt_id for prompt_id in profile.generic_ids if prompt_id not in generic_ids]
        if missing_generic:
            errors.append(
                f"profile={profile.profile_id} missing generic prompt ids: {', '.join(missing_generic)}"
            )

    for category in router.categories.values():
        missing_category_prompts = [
            prompt_id for prompt_id in category.industry_prompt_ids if prompt_id not in industry_ids
        ]
        if missing_category_prompts:
            errors.append(
                f"category={category.category_id} missing industry prompt ids: {', '.join(missing_category_prompts)}"
            )
        for slot, prompt_id in category.slot_map.items():
            if prompt_id not in industry_ids:
                errors.append(
                    f"category={category.category_id} slot={slot} maps to unknown prompt id: {prompt_id}"
                )

    if errors:
        raise ValueError("Router/prompt binding validation failed:\n- " + "\n- ".join(errors))


def confidence_band(*, router: RouterConfig, confidence: float) -> str:
    if confidence >= router.high_confidence_threshold:
        return "high"
    if confidence >= router.medium_confidence_threshold:
        return "medium"
    if confidence >= router.low_confidence_threshold:
        return "low"
    return "very_low"
