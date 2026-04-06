import ast
import os

API_DIR = r"d:\frappe_docker\development\frappe-bench\apps\erp_ai_assistant\erp_ai_assistant\api"

def get_funcs(path):
    with open(path, 'r', encoding='utf-8') as f:
        return {n.name for n in ast.parse(f.read()).body if isinstance(n, ast.FunctionDef)}

def get_used(path):
    with open(path, 'r', encoding='utf-8') as f:
        return {n.id for n in ast.walk(ast.parse(f.read())) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}

orch_path = os.path.join(API_DIR, "orchestrator.py")
gate_path = os.path.join(API_DIR, "llm_gateway.py")

orch_f = get_funcs(orch_path)
gate_f = get_funcs(gate_path)

orch_u = get_used(orch_path)
gate_u = get_used(gate_path)

orch_needs_from_gate = orch_u.intersection(gate_f)
gate_needs_from_orch = gate_u.intersection(orch_f)

def append_imports(path, mod_name, needs):
    if not needs: return
    imports = f"from .{mod_name} import {', '.join(needs)}"
    with open(path, 'a', encoding='utf-8') as f:
        f.write(f"\n\n# --- Cross-imports to resolve dependencies ---\n{imports}\n")
    print(f"Injected into {path}:\n  {imports}")

append_imports(orch_path, "llm_gateway", orch_needs_from_gate)
append_imports(gate_path, "orchestrator", gate_needs_from_orch)
print("Done.")
