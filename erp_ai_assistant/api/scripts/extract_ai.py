import ast
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
API_DIR = os.path.dirname(SCRIPT_DIR)
AI_FILE = os.path.join(API_DIR, "ai.py")
ORCHESTRATOR_FILE = os.path.join(API_DIR, "orchestrator.py")
LLM_GATEWAY_FILE = os.path.join(API_DIR, "llm_gateway.py")

with open(AI_FILE, "r", encoding="utf-8") as f:
    source_code = f.read()

tree = ast.parse(source_code)

orchestrator_funcs = {
    "_execute_prompt": "run_turn",
    "_run_enqueued_prompt": "_run_enqueued_prompt",
    "enqueue_prompt": "enqueue_prompt",
    "send_prompt": "send_prompt",
    "_generate_response": "run_agent_loop",
    "_finalize_reply_text": "format_reply",
    "_sanitize_assistant_reply": "_sanitize_assistant_reply",
    "_conversation_history_for_llm": "get_history",
    "_set_conversation_title_from_prompt": "set_title",
    "_build_message_attachments": "build_attachments",
    "_progress_update": "_progress_update",
    "_tool_round_limit_response": "_tool_round_limit_response",
    "_verification_prompt": "_verification_prompt",
    "_is_bulk_operation_request": "_is_bulk_operation_request",
    "_tool_result_feedback_payload": "_tool_result_feedback_payload",
    "_is_context_file_empty": "_is_context_file_empty",
    "_generate_new_thread_title": "_generate_new_thread_title",
    "_conversation_messages": "_conversation_messages",
    "create_message_artifacts": "create_message_artifacts",
    "_llm_user_prompt": "_llm_user_prompt",
    "_context_resolution_prompt": "_context_resolution_prompt",
    "_system_prompt": "_system_prompt",
    "continue_pending_action": "continue_pending_action",
    "create_copilot_document": "create_copilot_document",
    "_render_provider_tool_output": "_render_provider_tool_output",
    "_generate_provider_response": "_generate_provider_response",
    "ping": "ping",
    "chat": "chat",
}

llm_gateway_funcs = {
    "_provider_chat": "chat",
    "_provider_chat_with_resilience": "_provider_chat_with_resilience",
    "_provider_name": "provider_name",
    "_resolve_model": "_resolve_model",
    "_resolve_model_for_request": "_resolve_model_for_request",
    "_openai_chat": "_openai_chat",
    "_anthropic_chat": "_anthropic_chat",
    "_openai_compatible_chat": "_openai_compatible_chat",
    "_parse_backend_json": "_parse_backend_json",
    "_parse_sse_response": "_parse_sse_response",
    "_parse_sse_stream": "_parse_sse_stream",
    "_parse_sse_events": "_parse_sse_events",
    "_extract_error_detail": "_extract_error_detail",
    "_openai_output_text": "_openai_output_text",
    "_build_openai_input": "_build_openai_input",
    "_build_openai_compatible_messages": "_build_openai_compatible_messages",
    "_build_openai_input_content": "_build_openai_input_content",
    "_build_openai_compatible_content": "_build_openai_compatible_content",
    "_openai_tool_specs": "_openai_tool_specs",
    "_openai_compatible_tool_specs": "_openai_compatible_tool_specs",
    "_extract_textual_tool_call": "_extract_textual_tool_call",
    "_build_messages_with_images": "_build_messages_with_images",
    "_build_image_blocks": "_build_image_blocks",
    "_build_user_multimodal_content": "_build_user_multimodal_content",
    "_extract_base64_image": "_extract_base64_image",
    "_parse_message_attachments": "_parse_message_attachments",
    "_history_image_attachments": "_history_image_attachments",
    "_describe_message_attachments": "_describe_message_attachments",
    "_merge_history_content_and_attachment_notes": "_merge_history_content_and_attachment_notes",
    "_format_image_only_user_content": "_format_image_only_user_content",
    "_parse_prompt_images": "_parse_prompt_images",
    "_build_prompt_image_attachments": "_build_prompt_image_attachments",
    "_response_rejects_images": "_response_rejects_images",
    "_is_degraded_function_error": "_is_degraded_function_error",
    "_is_tool_choice_schema_error": "_is_tool_choice_schema_error",
    "_is_tool_choice_function_none_error": "_is_tool_choice_function_none_error",
    "_is_sampling_parameter_error": "_is_sampling_parameter_error",
    "_openai_supports_sampling_controls": "_openai_supports_sampling_controls",
    "_openai_compatible_supports_sampling_controls": "_openai_compatible_supports_sampling_controls",
    "get_available_models": "get_available_models",
    "_available_llm_models": "_available_llm_models",
    "_normalize_beta_param": "_normalize_beta_param",
    "_parse_openai_tool_arguments": "_parse_openai_tool_arguments",
    "_parse_json_object_text": "_parse_json_object_text",
    "_llm_request_timeout_seconds": "_llm_request_timeout_seconds",
    "_provider_compatibility_profile": "_provider_compatibility_profile",
}

def extract_funcs(targets):
    extracted_lines = []
    source_lines = source_code.splitlines(True)
    kept_body_nodes = []
    
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in targets:
            start = node.lineno - 1
            if node.decorator_list:
                start = node.decorator_list[0].lineno - 1
            end = node.end_lineno
            extracted_lines.extend(source_lines[start:end])
            extracted_lines.append("\n\n")
        else:
            kept_body_nodes.append(node)
            
    return "".join(extracted_lines)

orch_code = extract_funcs(orchestrator_funcs)
gate_code = extract_funcs(llm_gateway_funcs)

if not os.path.exists(ORCHESTRATOR_FILE):
    with open(ORCHESTRATOR_FILE, "w", encoding="utf-8") as f:
        f.write("# Agent Orchestrator\nimport frappe\nfrom frappe import _\nimport json\nimport copy\nfrom typing import Any, Optional\n\n")
        f.write(orch_code)

if not os.path.exists(LLM_GATEWAY_FILE):
    with open(LLM_GATEWAY_FILE, "w", encoding="utf-8") as f:
        f.write("# LLM Gateway\nimport frappe\nimport json\nimport html\nimport re\nimport requests\nfrom typing import Any, Optional\n\n")
        f.write(gate_code)

print(f"Extracted {len(orch_code.splitlines())} lines to orchestrator.py")
print(f"Extracted {len(gate_code.splitlines())} lines to llm_gateway.py")
