import ast
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
API_DIR = os.path.dirname(SCRIPT_DIR)
AI_FILE = os.path.join(API_DIR, "ai.py")
OUT_FILE = os.path.join(API_DIR, "ai_shim.py")

with open(AI_FILE, "r", encoding="utf-8") as f:
    source_code = f.read()

source_lines = source_code.splitlines(True)
tree = ast.parse(source_code)

# Combine both targets
extracted_targets = {
    "_execute_prompt", "_run_enqueued_prompt", "enqueue_prompt", "send_prompt",
    "_generate_response", "_finalize_reply_text", "_sanitize_assistant_reply",
    "_conversation_history_for_llm", "_set_conversation_title_from_prompt",
    "_build_message_attachments", "_progress_update", "_tool_round_limit_response",
    "_verification_prompt", "_is_bulk_operation_request", "_tool_result_feedback_payload",
    "_is_context_file_empty", "_generate_new_thread_title", "_conversation_messages",
    "create_message_artifacts", "_llm_user_prompt", "_context_resolution_prompt",
    "_system_prompt", "continue_pending_action", "create_copilot_document",
    "_render_provider_tool_output", "_generate_provider_response", "ping", "chat",
    "_provider_chat", "_provider_chat_with_resilience", "_provider_name",
    "_resolve_model", "_resolve_model_for_request", "_openai_chat", "_anthropic_chat",
    "_openai_compatible_chat", "_parse_backend_json", "_parse_sse_response",
    "_parse_sse_stream", "_parse_sse_events", "_extract_error_detail",
    "_openai_output_text", "_build_openai_input", "_build_openai_compatible_messages",
    "_build_openai_input_content", "_build_openai_compatible_content",
    "_openai_tool_specs", "_openai_compatible_tool_specs", "_extract_textual_tool_call",
    "_build_messages_with_images", "_build_image_blocks", "_build_user_multimodal_content",
    "_extract_base64_image", "_parse_message_attachments", "_history_image_attachments",
    "_describe_message_attachments", "_merge_history_content_and_attachment_notes",
    "_format_image_only_user_content", "_parse_prompt_images", "_build_prompt_image_attachments",
    "_response_rejects_images", "_is_degraded_function_error", "_is_tool_choice_schema_error",
    "_is_tool_choice_function_none_error", "_is_sampling_parameter_error",
    "_openai_supports_sampling_controls", "_openai_compatible_supports_sampling_controls",
    "get_available_models", "_available_llm_models", "_normalize_beta_param",
    "_parse_openai_tool_arguments", "_parse_json_object_text", "_llm_request_timeout_seconds",
    "_provider_compatibility_profile"
}

to_remove = []
for node in tree.body:
    if isinstance(node, ast.FunctionDef) and node.name in extracted_targets:
        if node.decorator_list:
            start = node.decorator_list[0].lineno - 1
        else:
            start = node.lineno - 1
        end = node.end_lineno
        to_remove.append((start, end))

to_remove.sort(key=lambda x: x[0])

kept_lines = []
current_line = 0
for start, end in to_remove:
    kept_lines.extend(source_lines[current_line:start])
    current_line = end
kept_lines.extend(source_lines[current_line:])

new_imports = """
# Shim imports for extracted modules
from .orchestrator import (
    _execute_prompt, _run_enqueued_prompt, enqueue_prompt, send_prompt,
    _generate_response, _finalize_reply_text, _sanitize_assistant_reply,
    _conversation_history_for_llm, _set_conversation_title_from_prompt,
    _build_message_attachments, _progress_update, _tool_round_limit_response,
    _verification_prompt, _is_bulk_operation_request, _tool_result_feedback_payload,
    _is_context_file_empty, _generate_new_thread_title, _conversation_messages,
    create_message_artifacts, _llm_user_prompt, _context_resolution_prompt,
    _system_prompt, continue_pending_action, create_copilot_document,
    _render_provider_tool_output, _generate_provider_response, ping
)
from .llm_gateway import (
    _provider_chat, _provider_chat_with_resilience, _provider_name,
    _resolve_model, _resolve_model_for_request, _openai_chat, _anthropic_chat,
    _openai_compatible_chat, _parse_backend_json, _parse_sse_response,
    _parse_sse_stream, _parse_sse_events, _extract_error_detail,
    _openai_output_text, _build_openai_input, _build_openai_compatible_messages,
    _build_openai_input_content, _build_openai_compatible_content,
    _openai_tool_specs, _openai_compatible_tool_specs, _extract_textual_tool_call,
    _build_messages_with_images, _build_image_blocks, _build_user_multimodal_content,
    _extract_base64_image, _parse_message_attachments, _history_image_attachments,
    _describe_message_attachments, _merge_history_content_and_attachment_notes,
    _format_image_only_user_content, _parse_prompt_images, _build_prompt_image_attachments,
    _response_rejects_images, _is_degraded_function_error, _is_tool_choice_schema_error,
    _is_tool_choice_function_none_error, _is_sampling_parameter_error,
    _openai_supports_sampling_controls, _openai_compatible_supports_sampling_controls,
    get_available_models, _available_llm_models, _normalize_beta_param,
    _parse_openai_tool_arguments, _parse_json_object_text, _llm_request_timeout_seconds,
    _provider_compatibility_profile, chat
)

run_turn = _execute_prompt
run_agent_loop = _generate_response
format_reply = _finalize_reply_text
get_history = _conversation_history_for_llm
set_title = _set_conversation_title_from_prompt
build_attachments = _build_message_attachments
"""

with open(OUT_FILE, "w", encoding="utf-8") as f:
    f.write(new_imports)
    f.writelines(kept_lines)

print(f"Created {OUT_FILE} with extracted functions removed. {len(to_remove)} functions deleted.")
