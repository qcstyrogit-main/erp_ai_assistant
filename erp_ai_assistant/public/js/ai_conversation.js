frappe.ui.form.on("AI Conversation", {
  refresh(frm) {
    frm.add_custom_button("Open Workspace", () => {
      frappe.set_route("assistant-workspace");
    });
  },
});
