import os

API_DIR = r"d:\frappe_docker\development\frappe-bench\apps\erp_ai_assistant\erp_ai_assistant\api"
AI_FILE = os.path.join(API_DIR, "ai.py")
INFRA_FILE = os.path.join(API_DIR, "infrastructure.py")
ORCH_FILE = os.path.join(API_DIR, "orchestrator.py")
GATE_FILE = os.path.join(API_DIR, "llm_gateway.py")

# 1. Read current ai.py
with open(AI_FILE, "r", encoding="utf-8") as f:
    ai_lines = f.readlines()

# 2. Extract the imports part (which was placed at the top)
# We know the first ~40 lines are the shim imports.
# We will find the first line that is `import json`
infra_start_idx = 0
for i, line in enumerate(ai_lines):
    if line.startswith("import json"):
        infra_start_idx = i
        break

infra_lines = ai_lines[infra_start_idx:]
shim_lines = ai_lines[:infra_start_idx]

# 3. Create infrastructure.py
with open(INFRA_FILE, "w", encoding="utf-8") as f:
    f.writelines(infra_lines)

# 4. Modify orchestrator.py to import from infrastructure and standard libs
with open(ORCH_FILE, "r", encoding="utf-8") as f:
    orch_content = f.read()

# Add missing imports to orchestrator
orch_imports = """# Agent Orchestrator
import frappe
from frappe import _
import json
import copy
import re
import html
import time
from typing import Any, Optional
from .infrastructure import *

"""
orch_content = orch_content.replace("# Agent Orchestrator\nimport frappe\nfrom frappe import _\nimport json\nimport copy\nfrom typing import Any, Optional\n\n", orch_imports)
with open(ORCH_FILE, "w", encoding="utf-8") as f:
    f.write(orch_content)

# 5. Modify llm_gateway.py
with open(GATE_FILE, "r", encoding="utf-8") as f:
    gate_content = f.read()

gate_imports = """# LLM Gateway
import frappe
from frappe import _
import json
import html
import re
import requests
import time
import base64
from typing import Any, Optional
from .infrastructure import *

"""
gate_content = gate_content.replace("# LLM Gateway\nimport frappe\nimport json\nimport html\nimport re\nimport requests\nfrom typing import Any, Optional\n\n", gate_imports)
with open(GATE_FILE, "w", encoding="utf-8") as f:
    f.write(gate_content)

# 6. Create proper ai.py
new_ai_content = """# Main AI Shim
from .infrastructure import *
from .orchestrator import *
from .llm_gateway import *

run_turn = _execute_prompt
run_agent_loop = _generate_response
format_reply = _finalize_reply_text
get_history = _conversation_history_for_llm
set_title = _set_conversation_title_from_prompt
build_attachments = _build_message_attachments

"""
with open(AI_FILE, "w", encoding="utf-8") as f:
    f.write(new_ai_content)

print("Architecture repaired successfully.")
