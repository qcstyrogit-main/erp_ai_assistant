frappe.ui.form.on("AI Provider Settings", {
  refresh(frm) {
    frm.add_custom_button("Test MCP Connection", async () => {
      const dialog = new frappe.ui.Dialog({
        title: "MCP Connection Test",
        fields: [
          {
            fieldname: "result",
            fieldtype: "HTML",
          },
        ],
        primary_action_label: "Close",
        primary_action() {
          dialog.hide();
        },
      });

      dialog.show();
      dialog.get_field("result").$wrapper.html("<p>Testing FAC MCP connection...</p>");

      try {
        const response = await frappe.call({
          method: "erp_ai_assistant.api.assistant.test_fac_mcp_connection",
        });
        const result = response.message || {};
        const indicator = result.ok ? "green" : "red";
        const tools = Array.isArray(result.tool_names) && result.tool_names.length
          ? `<div style="margin-top: 12px;"><strong>Tools</strong><br>${frappe.utils.escape_html(result.tool_names.join(", "))}</div>`
          : "";

        dialog.get_field("result").$wrapper.html(`
          <div>
            <p><strong>Status:</strong> <span class="indicator ${indicator}">${frappe.utils.escape_html(result.ok ? "Connected" : "Failed")}</span></p>
            <p><strong>Message:</strong> ${frappe.utils.escape_html(result.message || "")}</p>
            <p><strong>Endpoint:</strong> ${frappe.utils.escape_html(result.endpoint || "")}</p>
            <p><strong>Timeout:</strong> ${frappe.utils.escape_html(String(result.timeout ?? ""))}</p>
            <p><strong>Authorization:</strong> ${frappe.utils.escape_html(result.has_authorization ? "Configured" : "Not configured")}</p>
            <p><strong>Tool Count:</strong> ${frappe.utils.escape_html(String(result.tool_count ?? 0))}</p>
            ${tools}
          </div>
        `);
      } catch (error) {
        const message = error?.message || "MCP connection test failed.";
        dialog.get_field("result").$wrapper.html(`
          <div>
            <p><strong>Status:</strong> <span class="indicator red">Failed</span></p>
            <p><strong>Message:</strong> ${frappe.utils.escape_html(message)}</p>
          </div>
        `);
      }
    });
  },
});
