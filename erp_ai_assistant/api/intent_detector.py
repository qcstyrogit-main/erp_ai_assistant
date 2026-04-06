"""
erp_ai_assistant.api.intent_detector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
v3 — "Claude-level" upgrade.

What's new over v2:
  - SENTIMENT DETECTION: Detects frustrated/urgent users and adjusts priority.
  - FOLLOW-UP DETECTION: Recognises "do it", "yes proceed", "show me more"
    as continuations of the prior intent rather than new queries.
  - IMPLICIT INTENT: "my invoices are a mess" → analysis intent, not general_chat.
  - LANGUAGE-MIXED DETECTION: Filipino-English (Taglish) patterns for PH users.
  - COMPLIANCE INTENT: Detects BIR/BOA/tax queries as a dedicated intent class.
  - BETTER CONFIDENCE: Confidence now reflects multi-signal corroboration.
"""
from __future__ import annotations

import re
from typing import Any


# ── Keyword tables ────────────────────────────────────────────────────────────

_GUIDE_PATTERNS = (
    r"^(how to|how do i|how can i)\b",
    r"^(guide me|show me how|teach me)\b",
    r"^(what is the process|steps to)\b",
    r"\b(walkthrough|step by step|procedure for)\b",
    r"^(paano|paano ko|paano ba)\b",  # Filipino: "how"
)

_WORKFLOW_PREFIXES = (
    "submit", "cancel", "approve", "reject", "reopen", "close",
    "confirm", "validate", "authorise", "authorize",
    "i-submit", "i-cancel", "i-approve",  # Taglish
)

_UPDATE_PREFIXES = (
    "update", "change", "modify", "set", "edit", "patch", "rename",
    "correct", "fix", "adjust", "revise", "baguhin", "palitan",  # Filipino
)

_CREATE_PREFIXES = (
    "create", "new", "add", "make", "generate", "draft", "raise",
    "open", "initiate", "place", "gumawa", "mag-create",  # Filipino
)

_EXPORT_KEYWORDS = (
    "excel", "xlsx", "spreadsheet", "csv", "pdf",
    "download file", "make sheet", "save as excel",
    "export", "report file", "download report",
    "i-export", "i-download",  # Taglish
)

_READ_KEYWORDS = (
    "how many ", "count ", "list ", "show ", "display ", "get ",
    "find ", "search ", "open ", "view ", "fetch ", "look up ",
    "lookup ", "check ", "what is the status", "what is the balance",
    "ilang ", "ipakita ", "hanapin ",  # Filipino
)

_READ_PHRASE_PATTERNS = (
    r"\b(invoice list|invoices for|orders for|quotations for|records for|documents for)\b",
    r"\b(balance of|outstanding for|due from|owed by)\b",
    r"\b(stock of|quantity of|inventory of|on hand|in warehouse)\b",
    r"\b(salary of|payroll for|leave balance|attendance of)\b",
)

_ANALYSIS_KEYWORDS = (
    "why did", "why has", "why is", "why are",
    "what caused", "root cause", "reason for",
    "compare", "comparison", "versus", " vs ", " vs.",
    "trend", "over time", "month over month", "year over year",
    "breakdown", "analyse", "analyze", "analysis",
    "dropped", "increased", "decreased", "fell", "rose", "declined",
    "discrepancy", "mismatch", "gap between",
    "how is it that", "explain why",
    # Implicit analysis signals
    "is a mess", "not matching", "doesn't add up", "something wrong",
    "seems off", "incorrect", "discrepancies", "reconcile",
    "hindi tama", "may mali",  # Filipino: "not right", "there's a mistake"
)

# NEW v3: Compliance intent keywords
_COMPLIANCE_KEYWORDS = (
    "bir", "bir form", "2550", "0619", "2307", "2306", "1601",
    "vat", "withholding tax", "ewt", "fwt",
    "boa", "books of accounts", "general ledger book",
    "relief", "sawt", "alphalist",
    "sss", "philhealth", "pag-ibig", "hdmf",
    "income tax", "corporate tax", "quarterly tax",
    "tax compliance", "tax deadline", "filing deadline",
)

_ANSWER_PATTERNS = (
    r"\?$",
    r"^(what |why |which |who |when |where |explain )",
    r"\b(tell me about|explain|describe|what does)\b",
)

_DESTRUCTIVE_KEYWORDS = (
    "delete", "remove", "wipe", "erase", "purge",
    "cancel", "reverse", "void", "undo",
    "bulk delete", "mass delete",
    "burahin", "kanselahin",  # Filipino
)

# NEW v3: Follow-up / continuation signals
_FOLLOWUP_PATTERNS = (
    r"^(do it|go ahead|proceed|yes proceed|ok proceed|sige|tuloy|oo)\s*$",
    r"^(show me more|more details|drill down|expand|breakdown)\b",
    r"^(export (this|that|it)|download (this|that))\b",
    r"^(what about|and also|also show)\b",
    r"^(now (create|update|submit|cancel))\b",
)

# NEW v3: Urgency / frustration signals
_URGENCY_PATTERNS = (
    r"\b(urgent|asap|immediately|right now|emergency|critical)\b",
    r"\b(deadline today|due today|overdue now)\b",
    r"(!{2,})",  # Multiple exclamation marks
)

# ── Module-detection keyword sets ─────────────────────────────────────────────

_MODULE_SIGNALS: dict[str, tuple[str, ...]] = {
    "Finance": (
        "invoice", "payment", "journal", "account", "ledger", "balance sheet",
        "profit", "loss", "revenue", "expense", "receivable", "payable",
        "cost center", "budget", "tax", "gst", "vat", "bir",
    ),
    "Inventory": (
        "stock", "inventory", "warehouse", "item", "bin", "qty",
        "quantity", "valuation", "stock entry", "material transfer",
        "purchase receipt", "delivery note", "reorder",
    ),
    "Sales": (
        "sales order", "sales invoice", "customer", "lead", "opportunity",
        "quotation", "delivery", "crm", "pipeline", "prospect",
    ),
    "Purchasing": (
        "purchase order", "purchase invoice", "supplier", "vendor",
        "rfq", "supplier quotation", "material request",
    ),
    "HR": (
        "employee", "salary", "payroll", "leave", "attendance",
        "appraisal", "department", "designation", "hr",
        "sss", "philhealth", "pag-ibig",
    ),
    "Manufacturing": (
        "bom", "bill of materials", "work order", "job card",
        "production", "routing", "workstation", "manufacturing",
    ),
    "Projects": (
        "project", "task", "timesheet", "milestone", "gantt",
        "activity", "project update",
    ),
    "CRM": (
        "crm", "lead", "opportunity", "campaign", "prospect", "contact",
    ),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize_prompt(prompt: str) -> str:
    """Strip filler openers and normalise whitespace."""
    text = re.sub(r"\s+", " ", str(prompt or "").strip())
    filler_patterns = (
        r"^(i mean)\s+",
        r"^(can you|could you|would you|will you)\s+",
        r"^(please|kindly)\s+",
        r"^(can you help me|help me)\s+",
        r"^(what i want is to|what i want is)\s+",
        r"^(i want is to|i want is)\s+",
        r"^(i want you to|i want to)\s+",
        r"^(pwede mo ba|pwede ba)\s+",  # Filipino
        r"^(paki|pakiusap)\s+",          # Filipino: "please"
    )
    changed = True
    while changed and text:
        changed = False
        for pattern in filler_patterns:
            updated = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()
            if updated != text:
                text = updated
                changed = True
    return text


def _detect_modules(lowered: str) -> list[str]:
    matched: list[str] = []
    for module, signals in _MODULE_SIGNALS.items():
        if any(sig in lowered for sig in signals):
            matched.append(module)
    return matched


def _is_destructive(lowered: str) -> bool:
    return any(kw in lowered for kw in _DESTRUCTIVE_KEYWORDS)


def _is_urgent(lowered: str) -> bool:
    return any(re.search(p, lowered, re.IGNORECASE) for p in _URGENCY_PATTERNS)


def _is_followup(lowered: str) -> bool:
    return any(re.search(p, lowered, re.IGNORECASE) for p in _FOLLOWUP_PATTERNS)


def _is_compliance(lowered: str) -> bool:
    return any(kw in lowered for kw in _COMPLIANCE_KEYWORDS)


def _confidence(signals: int) -> float:
    if signals >= 4:
        return 0.98
    if signals >= 3:
        return 0.95
    if signals >= 2:
        return 0.90
    return 0.80


# ── Public API ────────────────────────────────────────────────────────────────

def detect_intent_heuristic(
    prompt: str, context: dict[str, Any] | None = None
) -> dict[str, Any]:
    """
    Detect the primary intent of a user prompt. v3.

    Returns a dict with:
      intent           : str
      confidence       : float (0-1)
      should_route     : bool
      normalized_prompt: str
      modules          : list[str]
      is_destructive   : bool
      is_multi_module  : bool
      is_urgent        : bool   NEW v3
      is_followup      : bool   NEW v3
      is_compliance    : bool   NEW v3
    """
    text = normalize_prompt(prompt)
    lowered = text.lower()
    context = context or {}
    modules = _detect_modules(lowered)
    destructive = _is_destructive(lowered)
    urgent = _is_urgent(lowered)
    followup = _is_followup(lowered)
    compliance = _is_compliance(lowered)

    base = {
        "normalized_prompt": text,
        "modules": modules,
        "is_destructive": destructive,
        "is_multi_module": len(modules) > 1,
        "is_urgent": urgent,
        "is_followup": followup,
        "is_compliance": compliance,
    }

    if not text:
        return {"intent": "unknown", "confidence": 0.0, "should_route": False, **base}

    # ── Follow-up / continuation ──────────────────────────────────────────────
    if followup:
        return {"intent": "followup", "confidence": 0.90, "should_route": True, **base}

    # ── Compliance (BIR/BOA/tax) ──────────────────────────────────────────────
    if compliance:
        return {
            "intent": "compliance",
            "confidence": _confidence(2 + len(modules)),
            "should_route": True,
            **base,
        }

    # ── Guide / how-to ────────────────────────────────────────────────────────
    if any(re.search(p, lowered, re.IGNORECASE) for p in _GUIDE_PATTERNS):
        return {"intent": "guide", "confidence": 0.95, "should_route": True, **base}

    # ── Analysis / root-cause / comparison ───────────────────────────────────
    analysis_hits = sum(1 for kw in _ANALYSIS_KEYWORDS if kw in lowered)
    if analysis_hits >= 1:
        return {
            "intent": "analysis",
            "confidence": _confidence(analysis_hits + len(modules)),
            "should_route": True,
            **base,
        }

    # ── Workflow (submit / cancel / approve) ──────────────────────────────────
    if re.match(r"^(" + "|".join(_WORKFLOW_PREFIXES) + r")\b", lowered):
        return {
            "intent": "workflow",
            "confidence": _confidence(2 + (1 if destructive else 0)),
            "should_route": True,
            **base,
        }

    # ── Update ───────────────────────────────────────────────────────────────
    if re.match(r"^(" + "|".join(_UPDATE_PREFIXES) + r")\b", lowered):
        return {"intent": "update", "confidence": 0.95, "should_route": True, **base}

    # ── Create ───────────────────────────────────────────────────────────────
    if re.match(r"^(" + "|".join(_CREATE_PREFIXES) + r")\b", lowered):
        return {"intent": "create", "confidence": 0.95, "should_route": True, **base}

    # ── Export ───────────────────────────────────────────────────────────────
    if any(kw in lowered for kw in _EXPORT_KEYWORDS):
        return {"intent": "export", "confidence": 0.90, "should_route": True, **base}

    # ── Read / lookup ─────────────────────────────────────────────────────────
    read_hits = sum(1 for kw in _READ_KEYWORDS if kw in lowered)
    phrase_hits = sum(1 for p in _READ_PHRASE_PATTERNS if re.search(p, lowered))
    if read_hits + phrase_hits >= 1:
        return {
            "intent": "read",
            "confidence": _confidence(read_hits + phrase_hits),
            "should_route": True,
            **base,
        }

    # ── Answer / Q&A ─────────────────────────────────────────────────────────
    if any(re.search(p, lowered, re.IGNORECASE) for p in _ANSWER_PATTERNS):
        route = bool(modules)
        return {"intent": "answer", "confidence": 0.80, "should_route": route, **base}

    # ── ERP chat (has document context) ──────────────────────────────────────
    if context.get("doctype") or context.get("docname"):
        return {"intent": "erp_chat", "confidence": 0.65, "should_route": True, **base}

    # ── Fallback ──────────────────────────────────────────────────────────────
    return {"intent": "general_chat", "confidence": 0.55, "should_route": False, **base}
