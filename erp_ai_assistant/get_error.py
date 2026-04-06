import frappe

def get_last_error():
    logs = frappe.get_all("Error Log", filters={"error": ("like", "%download_message_attachment%")}, fields=["method", "error"], limit=1, order_by="creation desc")
    if logs:
        print("--- TARGET ERROR ---")
        print(logs[0].error)
    else:
        print("No specific error found with download_message_attachment.")

    logs2 = frappe.get_all("Error Log", filters={"error": ("like", "%export%")}, fields=["method", "error"], limit=1, order_by="creation desc")
    if logs2:
        print("--- ERROR w/ export in body ---")
        print(logs2[0].error)
