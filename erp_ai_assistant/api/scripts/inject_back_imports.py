import ast
import os

API_DIR = r"d:\frappe_docker\development\frappe-bench\apps\erp_ai_assistant\erp_ai_assistant\api"
INFRA_FILE = os.path.join(API_DIR, "infrastructure.py")
ORCH_FILE = os.path.join(API_DIR, "orchestrator.py")
GATE_FILE = os.path.join(API_DIR, "llm_gateway.py")

def get_funcs(path):
    with open(path, 'r', encoding='utf-8') as f:
        tree = ast.parse(f.read())
    return {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}

def get_used(path):
    with open(path, 'r', encoding='utf-8') as f:
        tree = ast.parse(f.read())
    return {node.id for node in ast.walk(tree) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)}

orch_f = get_funcs(ORCH_FILE)
gate_f = get_funcs(GATE_FILE)
infra_u = get_used(INFRA_FILE)

orch_needed = orch_f.intersection(infra_u)
gate_needed = gate_f.intersection(infra_u)

orch_imports = f"from .orchestrator import {', '.join(orch_needed)}"
gate_imports = f"from .llm_gateway import {', '.join(gate_needed)}"

with open(INFRA_FILE, 'a', encoding='utf-8') as f:
    f.write("\n\n# --- Back-imports to resolve dependencies ---\n")
    if orch_needed:
        f.write(orch_imports + "\n")
    if gate_needed:
        f.write(gate_imports + "\n")

print("Injected back-imports at bottom of infrastructure.py")
