"""Compatibility shim for legacy imports and queued background job paths."""

# Keep public exports available to existing callers.
from .infrastructure import *
from .orchestrator import *
from .llm_gateway import *

# Explicitly import private helpers used by:
# - background jobs resolving erp_ai_assistant.api.ai._run_enqueued_prompt
# - assistant.py via `from . import ai as ai_api`
# Star imports do not pull underscored names into this module.
from .infrastructure import (
    DEFAULT_OPENAI_RESPONSES_PATH,
    _cfg,
    _summarize_title,
)
from .llm_gateway import (
    _build_prompt_image_attachments,
    _extract_error_detail,
    _format_image_only_user_content,
    _llm_request_timeout_seconds,
    _normalize_beta_param,
    _openai_output_text,
    _parse_backend_json,
    _parse_prompt_images,
    _provider_name,
    _provider_compatibility_profile,
    _resolve_model,
    _resolve_model_for_request,
    requests,
)
from .orchestrator import (
    _build_message_attachments,
    _conversation_history_for_llm,
    _execute_prompt,
    _finalize_reply_text,
    _generate_response,
    _run_enqueued_prompt,
    _set_conversation_title_from_prompt,
)

run_turn = _execute_prompt
run_agent_loop = _generate_response
format_reply = _finalize_reply_text
get_history = _conversation_history_for_llm
set_title = _set_conversation_title_from_prompt
build_attachments = _build_message_attachments

