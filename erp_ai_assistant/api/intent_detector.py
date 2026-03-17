import re
from typing import Any


def normalize_prompt(prompt: str) -> str:
    text = re.sub(r"\s+", " ", str(prompt or "").strip())
    filler_patterns = (
        r"^(i mean)\s+",
        r"^(can you|could you|would you|will you)\s+",
        r"^(please|kindly)\s+",
        r"^(can you help me|help me)\s+",
        r"^(what i want is to|what i want is)\s+",
        r"^(i want is to|i want is)\s+",
        r"^(i want you to|i want to)\s+",
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


def detect_intent_heuristic(prompt: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    text = normalize_prompt(prompt)
    lowered = text.lower()
    context = context or {}

    if not text:
        return {"intent": "unknown", "confidence": 0.0, "should_route": False, "normalized_prompt": text}

    guide_patterns = (
        r"^(how to|how do i|how can i)\b",
        r"^(guide me|show me how|teach me)\b",
        r"^(what is the process|steps to)\b",
    )
    if any(re.search(pattern, lowered, re.IGNORECASE) for pattern in guide_patterns):
        return {"intent": "guide", "confidence": 0.95, "should_route": True, "normalized_prompt": text}

    if re.match(r"^(submit|cancel|approve|reject|reopen)\b", lowered):
        return {"intent": "workflow", "confidence": 0.96, "should_route": True, "normalized_prompt": text}

    if re.match(r"^(update|change|modify|set)\b", lowered):
        return {"intent": "update", "confidence": 0.95, "should_route": True, "normalized_prompt": text}

    if re.match(r"^(create|new|add)\b", lowered):
        return {"intent": "create", "confidence": 0.95, "should_route": True, "normalized_prompt": text}

    if any(term in lowered for term in ("excel", "xlsx", "spreadsheet", "csv", "pdf", "download file", "make sheet", "save as excel")):
        return {"intent": "export", "confidence": 0.9, "should_route": True, "normalized_prompt": text}

    if any(term in lowered for term in ("how many ", "count ", "list ", "show ", "display ", "get ", "find ", "search ", "open ", "view ")):
        return {"intent": "read", "confidence": 0.85, "should_route": True, "normalized_prompt": text}
    if any(term in lowered for term in (" invoice list", " invoices for ", " orders for ", " quotations for ", " records for ", " documents for ")):
        return {"intent": "read", "confidence": 0.84, "should_route": True, "normalized_prompt": text}

    if lowered.endswith("?") or any(term in lowered for term in ("what ", "why ", "which ", "who ", "when ", "where ", "explain ")):
        return {"intent": "answer", "confidence": 0.8, "should_route": False, "normalized_prompt": text}

    if context.get("doctype") or context.get("docname"):
        return {"intent": "erp_chat", "confidence": 0.65, "should_route": True, "normalized_prompt": text}

    return {"intent": "general_chat", "confidence": 0.6, "should_route": False, "normalized_prompt": text}
