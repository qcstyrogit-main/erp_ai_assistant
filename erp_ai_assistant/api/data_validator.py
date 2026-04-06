"""
erp_ai_assistant.api.data_validator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Data Validation Layer — new module that sits between tool execution and LLM
response generation.

Purpose:
  Intercept tool results and flag empty responses, type mismatches, suspicious
  financial values, and error payloads BEFORE the LLM has a chance to
  hallucinate around them.

This module is called by the orchestrator / llm_gateway after every tool call,
and injects a structured `_validation` key into the result dict that the LLM
uses as an authoritative signal about data quality.

Usage:
    from .data_validator import validate_tool_result, inject_validation

    raw = execute_tool(tool_name, arguments)
    validated = inject_validation(tool_name, raw)
    # Pass `validated` to the LLM — it now contains _validation metadata.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


# ── Financial fields that must be non-negative in normal ERP operation ───────

_NON_NEGATIVE_FIELDS = frozenset({
    "grand_total",
    "net_total",
    "total",
    "base_grand_total",
    "base_net_total",
    "outstanding_amount",
    "paid_amount",
    "allocated_amount",
    "total_qty",
    "qty",
    "actual_qty",
    "projected_qty",
    "valuation_rate",
    "rate",
    "amount",
    "base_amount",
    "net_pay",
    "gross_pay",
})

# Fields that should NEVER be zero for a submitted/paid document
_NON_ZERO_SUBMITTED_FIELDS = frozenset({
    "grand_total",
    "net_total",
    "paid_amount",
})

# DocTypes where amounts are expected to be positive
_FINANCIAL_DOCTYPES = frozenset({
    "sales invoice",
    "purchase invoice",
    "payment entry",
    "journal entry",
    "salary slip",
    "payroll entry",
    "expense claim",
    "purchase order",
    "sales order",
    "quotation",
})


# ── Result dataclass ─────────────────────────────────────────────────────────

@dataclass
class ToolResultValidation:
    is_valid: bool
    quality: str = "ok"           # "ok" | "warning" | "error" | "empty"
    issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    sanitized_result: Any = None  # The (possibly cleaned) result to pass on

    def as_dict(self) -> dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "quality": self.quality,
            "issues": self.issues,
            "warnings": self.warnings,
        }


# ── Core validator ───────────────────────────────────────────────────────────

def validate_tool_result(tool_name: str, result: Any) -> ToolResultValidation:
    """
    Validate a single tool result.

    Returns a ToolResultValidation describing the quality of the result.
    The orchestrator should use .is_valid and .quality to decide whether to:
      - Pass the result straight to the LLM  (ok)
      - Inject a warning into the tool message (warning)
      - Request a retry or surface the error  (error / empty)
    """
    tool = str(tool_name or "").strip().lower()

    # ── None result ──────────────────────────────────────────────────────────
    if result is None:
        return ToolResultValidation(
            is_valid=False,
            quality="error",
            issues=[f"Tool '{tool_name}' returned None — no data retrieved."],
            sanitized_result=None,
        )

    # ── Error payload from the tool itself ───────────────────────────────────
    if isinstance(result, dict):
        ok_flag = result.get("ok")
        if ok_flag is False:
            error_msg = str(result.get("error") or result.get("message") or "Unknown error")
            return ToolResultValidation(
                is_valid=False,
                quality="error",
                issues=[f"Tool '{tool_name}' returned an error: {error_msg}"],
                sanitized_result=result,
            )

        # ── Empty list result ────────────────────────────────────────────────
        data = result.get("data") or result.get("records") or result.get("items")
        if data is not None and isinstance(data, list) and len(data) == 0:
            return ToolResultValidation(
                is_valid=True,
                quality="empty",
                warnings=[f"Tool '{tool_name}' returned 0 records for this query."],
                sanitized_result=result,
            )

        # ── Financial field sanity checks ────────────────────────────────────
        issues, warnings = _check_financial_fields(tool_name, result)
        if issues:
            return ToolResultValidation(
                is_valid=False,
                quality="warning",
                issues=issues,
                warnings=warnings,
                sanitized_result=result,
            )
        if warnings:
            return ToolResultValidation(
                is_valid=True,
                quality="warning",
                warnings=warnings,
                sanitized_result=result,
            )

    # ── Empty list result at top level ───────────────────────────────────────
    if isinstance(result, list):
        if len(result) == 0:
            return ToolResultValidation(
                is_valid=True,
                quality="empty",
                warnings=[f"Tool '{tool_name}' returned an empty list."],
                sanitized_result=result,
            )
        # Validate each record in a list result
        issues = _check_list_records(tool_name, result)
        if issues:
            return ToolResultValidation(
                is_valid=True,
                quality="warning",
                warnings=issues,
                sanitized_result=result,
            )

    return ToolResultValidation(
        is_valid=True,
        quality="ok",
        sanitized_result=result,
    )


def inject_validation(tool_name: str, result: Any) -> Any:
    """
    Run validation and inject a `_validation` key into dict results.

    If the result is not a dict, wraps it in one.
    This enriched result is what the LLM gateway passes into the tool_result
    content block, giving the LLM explicit signal about data quality.

    The LLM is instructed (via system prompt DATA_CONTRACT) to check
    _validation.quality and treat "error" results as "no data available".
    """
    validation = validate_tool_result(tool_name, result)

    if isinstance(result, dict):
        enriched = dict(result)
        enriched["_validation"] = validation.as_dict()
        return enriched

    # For non-dict results, wrap to carry validation metadata
    return {
        "data": result,
        "_validation": validation.as_dict(),
        "_tool": tool_name,
    }


# ── Financial field checks ───────────────────────────────────────────────────

def _check_financial_fields(tool_name: str, result: dict[str, Any]) -> tuple[list[str], list[str]]:
    """
    Check financial fields in a document result for suspicious values.
    Returns (issues, warnings).
    Issues = hard problems (negative amounts where not expected).
    Warnings = soft flags (suspiciously round numbers, zero on submitted doc).
    """
    issues: list[str] = []
    warnings: list[str] = []

    doctype = str(result.get("doctype") or "").strip().lower()
    docstatus = result.get("docstatus")
    is_submitted = docstatus == 1

    for field_name in _NON_NEGATIVE_FIELDS:
        value = result.get(field_name)
        if value is None:
            continue
        try:
            fval = float(value)
        except (TypeError, ValueError):
            continue

        if math.isnan(fval) or math.isinf(fval):
            issues.append(
                f"Field '{field_name}' in {tool_name} result is NaN or Inf — "
                f"data corruption suspected."
            )
            continue

        if fval < 0 and field_name not in {"allocated_amount"}:
            issues.append(
                f"Field '{field_name}' = {fval} is negative in {tool_name} result "
                f"for {doctype or 'document'}. Verify this is intentional (e.g. credit note)."
            )

        if (
            is_submitted
            and doctype in _FINANCIAL_DOCTYPES
            and field_name in _NON_ZERO_SUBMITTED_FIELDS
            and fval == 0.0
        ):
            warnings.append(
                f"Submitted {doctype} has {field_name} = 0 — unusual for a "
                f"submitted financial document. Verify with the user."
            )

        # Suspiciously round numbers on invoices
        if (
            fval > 0
            and fval == int(fval)
            and fval % 100_000 == 0
            and doctype in _FINANCIAL_DOCTYPES
        ):
            warnings.append(
                f"Field '{field_name}' = {fval} is a very round number "
                f"in {tool_name} result. Confirm this is correct before reporting."
            )

    return issues, warnings


def _check_list_records(tool_name: str, records: list[Any]) -> list[str]:
    """
    Light-touch check for list results: flag null names and duplicate names.
    """
    warnings: list[str] = []
    seen_names: set[str] = set()
    null_name_count = 0

    for record in records:
        if not isinstance(record, dict):
            continue
        name = record.get("name")
        if not name:
            null_name_count += 1
        else:
            name_str = str(name)
            if name_str in seen_names:
                warnings.append(
                    f"Duplicate record name '{name_str}' found in {tool_name} result."
                )
            seen_names.add(name_str)

    if null_name_count > 0:
        warnings.append(
            f"{null_name_count} record(s) in {tool_name} result have no 'name' field. "
            f"These may be incomplete or corrupted records."
        )

    return warnings


# ── Convenience helpers for the orchestrator ─────────────────────────────────

def is_empty_result(validation: ToolResultValidation) -> bool:
    """Return True if the tool returned zero records (not an error)."""
    return validation.quality == "empty"


def is_error_result(validation: ToolResultValidation) -> bool:
    """Return True if the tool returned a hard error."""
    return validation.quality == "error" or not validation.is_valid


def format_validation_for_llm(validation: ToolResultValidation) -> str:
    """
    Format the validation result as a short natural-language note to prepend
    to the tool result message so the LLM understands data quality.
    """
    if validation.quality == "ok":
        return ""
    if validation.quality == "empty":
        return (
            "[DATA QUALITY: EMPTY] This tool returned zero records. "
            "Do NOT invent data to fill this gap. Report the empty result to the user."
        )
    if validation.quality == "error":
        issues = "; ".join(validation.issues)
        return (
            f"[DATA QUALITY: ERROR] {issues} "
            "Do NOT proceed with this data. Report the error to the user and "
            "suggest retrying or checking permissions."
        )
    if validation.quality == "warning":
        all_notes = validation.issues + validation.warnings
        notes = "; ".join(all_notes[:3])
        return (
            f"[DATA QUALITY: WARNING] {notes} "
            "Proceed with caution and flag these anomalies in your response."
        )
    return ""
