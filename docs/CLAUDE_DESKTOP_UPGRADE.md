# ERP AI Desktop Upgrade Notes

This upgrade focuses on turning the assistant into a Claude Desktop-style ERP workspace for ERPNext.

## What changed

- Reworked the `/assistant` page into a desktop-like shell with stronger brand, context path, and reusable workspace layout.
- Upgraded result rendering so responses show assistant badges such as **ERP verified**, **Visualized**, and **Draft-safe**.
- Kept the existing copilot cards and data previews, but surfaced them in a clearer, more executive-friendly layout.
- Improved composer shell and message cards to feel more like a serious productivity app than a plain chat widget.

## Why this helps

- Easier to deploy across the whole ERPNext system because the UI now looks like a workspace, not only a chatbot.
- Safer user trust model because grounded responses and draft-safe actions are visibly distinguished.
- Better fit for dashboards, record analysis, and operational workflows where response rendering matters as much as model output.

## Still recommended next

1. Expose backend verification metadata directly to the UI instead of inferring from tool events.
2. Add streaming tokens or SSE/WebSocket transport for more fluid live responses.
3. Add a right-side inspector panel for sources, ERP references, and action plans.
4. Bring the same message chrome into the Desk bubble so both surfaces feel identical.
5. Add reusable response templates for summary, audit, draft, and analytics modes.
