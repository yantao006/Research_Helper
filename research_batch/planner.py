from __future__ import annotations

from dataclasses import dataclass

from research_batch.classifier import IndustryClassification
from research_batch.config import PromptRow
from research_batch.router import RouterConfig, confidence_band, select_industry_prompt_ids

MULTI_SEGMENT_CORE_GENERIC_IDS = ["1", "2", "5", "10", "12"]


@dataclass(frozen=True)
class ExecutionPlan:
    profile_id: str
    prompts: list[PromptRow]
    selected_generic_ids: list[str]
    selected_industry_prompt_ids: list[str]
    classification: IndustryClassification
    confidence_band: str
    routing_rule: str
    decision_note: str
    manual_review: bool


def _build_prompt_index(prompts: list[PromptRow]) -> dict[str, PromptRow]:
    return {prompt.prompt_id: prompt for prompt in prompts}


def _format_top_industries(weights: dict[str, float]) -> str:
    if not weights:
        return "-"
    pairs = sorted(weights.items(), key=lambda item: item[1], reverse=True)
    return ", ".join(f"{industry}:{weight:.0%}" for industry, weight in pairs[:3])


def _pick_secondary_prompt_id(
    *,
    secondary_category,
    profile,
    used_ids: set[str],
) -> str | None:
    slot_priority = ["catalyst", "dashboard", "valuation", "economics", "moat", "financial_quality"]
    candidate_slots = profile.industry_prompt_slots if "all" not in profile.industry_prompt_slots else slot_priority
    for slot in slot_priority:
        if slot not in candidate_slots:
            continue
        prompt_id = secondary_category.slot_map.get(slot)
        if prompt_id and prompt_id not in used_ids:
            return prompt_id
    for prompt_id in secondary_category.industry_prompt_ids:
        if prompt_id not in used_ids:
            return prompt_id
    return None


def _build_multi_segment_industry_ids(
    *,
    profile,
    primary_category,
    secondary_category,
) -> list[str]:
    primary_ids = select_industry_prompt_ids(category=primary_category, profile=profile)
    if not secondary_category or profile.industry_prompt_count <= 1:
        return primary_ids
    selected: list[str] = []
    used_ids: set[str] = set()
    primary_budget = max(profile.industry_prompt_count - 1, 0)
    for prompt_id in primary_ids:
        if prompt_id in used_ids:
            continue
        selected.append(prompt_id)
        used_ids.add(prompt_id)
        if len(selected) >= primary_budget:
            break

    secondary_prompt_id = _pick_secondary_prompt_id(
        secondary_category=secondary_category,
        profile=profile,
        used_ids=used_ids,
    )
    if secondary_prompt_id:
        selected.append(secondary_prompt_id)
        used_ids.add(secondary_prompt_id)

    # If primary prompts are insufficient, fill from secondary defaults.
    if len(selected) < profile.industry_prompt_count:
        for prompt_id in secondary_category.industry_prompt_ids:
            if prompt_id in used_ids:
                continue
            selected.append(prompt_id)
            used_ids.add(prompt_id)
            if len(selected) >= profile.industry_prompt_count:
                break
    return selected[: profile.industry_prompt_count]


def build_execution_plan(
    *,
    router: RouterConfig,
    profile_id: str,
    generic_prompts: list[PromptRow],
    industry_prompts: list[PromptRow],
    classification: IndustryClassification,
) -> ExecutionPlan:
    profile = router.execution_profiles.get(profile_id)
    if profile is None:
        raise ValueError(
            f"Unknown profile: {profile_id}. Available: {', '.join(sorted(router.execution_profiles))}"
        )

    generic_by_id = _build_prompt_index(generic_prompts)
    industry_by_id = _build_prompt_index(industry_prompts)

    selected_generic_ids = list(profile.generic_ids)
    selected_industry_ids: list[str] = []
    routing_rule = "low_confidence_classification"
    decision_note = (
        router.fallback_action
        or "classification confidence is below threshold; run generic prompts only"
    )

    if classification.company_type == "conglomerate":
        routing_rule = "conglomerate_or_holding"
        decision_note = "company_type=conglomerate, use generic prompts only"
    elif classification.industry_id and classification.confidence >= router.medium_confidence_threshold:
        category = router.categories.get(classification.industry_id)
        if category:
            if classification.company_type == "multi_segment":
                secondary_category = None
                if classification.secondary_industry_ids:
                    secondary_category = router.categories.get(classification.secondary_industry_ids[0])
                selected_industry_ids = _build_multi_segment_industry_ids(
                    profile=profile,
                    primary_category=category,
                    secondary_category=secondary_category,
                )
                multi_segment_generic_ids = [
                    prompt_id
                    for prompt_id in MULTI_SEGMENT_CORE_GENERIC_IDS
                    if prompt_id in generic_by_id
                ]
                if multi_segment_generic_ids:
                    selected_generic_ids = multi_segment_generic_ids
                routing_rule = "multi_segment_company"
                decision_note = (
                    "company_type=multi_segment, use core generic ids + weighted industry overlay "
                    f"(top={_format_top_industries(classification.industry_weights)})"
                )
            else:
                selected_industry_ids = select_industry_prompt_ids(category=category, profile=profile)
                routing_rule = "single_industry_company"
                decision_note = "single-industry classification passed threshold, add industry overlay"

    selected_generic_prompts: list[PromptRow] = []
    for prompt_id in selected_generic_ids:
        prompt = generic_by_id.get(prompt_id)
        if not prompt:
            raise ValueError(f"profile={profile.profile_id} references unknown generic prompt id: {prompt_id}")
        selected_generic_prompts.append(prompt)

    selected_industry_prompts: list[PromptRow] = []
    for prompt_id in selected_industry_ids:
        prompt = industry_by_id.get(prompt_id)
        if not prompt:
            raise ValueError(f"industry selection references unknown prompt id: {prompt_id}")
        selected_industry_prompts.append(prompt)

    manual_review = (
        classification.manual_review
        or (not classification.industry_id)
        or classification.confidence < router.medium_confidence_threshold
    )

    return ExecutionPlan(
        profile_id=profile.profile_id,
        prompts=[*selected_generic_prompts, *selected_industry_prompts],
        selected_generic_ids=[prompt.prompt_id for prompt in selected_generic_prompts],
        selected_industry_prompt_ids=[prompt.prompt_id for prompt in selected_industry_prompts],
        classification=classification,
        confidence_band=confidence_band(router=router, confidence=classification.confidence),
        routing_rule=routing_rule,
        decision_note=decision_note,
        manual_review=manual_review,
    )


def format_plan_preview(*, company: str, ticker: str, plan: ExecutionPlan) -> str:
    classification = plan.classification
    lines = [
        f"[Plan] {company} ({ticker})",
        f"  profile={plan.profile_id} rule={plan.routing_rule} manual_review={str(plan.manual_review).lower()}",
        "  classification="
        + (
            f"industry={classification.industry_id or 'unknown'} "
            f"confidence={classification.confidence:.2f} band={plan.confidence_band} method={classification.method} "
            f"type={classification.company_type} reason={classification.reason}"
        ),
        f"  industry_mix={_format_top_industries(classification.industry_weights)}",
        f"  secondary={','.join(classification.secondary_industry_ids) if classification.secondary_industry_ids else '-'}",
        f"  decision={plan.decision_note}",
        f"  generic_ids={','.join(plan.selected_generic_ids) if plan.selected_generic_ids else '-'}",
        f"  industry_ids={','.join(plan.selected_industry_prompt_ids) if plan.selected_industry_prompt_ids else '-'}",
        "  prompts=" + ",".join(f"{p.prompt_id}({p.source})" for p in plan.prompts),
    ]
    return "\n".join(lines)
