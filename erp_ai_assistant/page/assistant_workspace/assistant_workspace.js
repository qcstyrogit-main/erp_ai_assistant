frappe.pages["assistant-workspace"].on_page_load = function (wrapper) {
  const page = frappe.ui.make_app_page({
    parent: wrapper,
    title: "ERP Assistant",
    single_column: true,
  });

  $(page.body).html(`
    <div class="erp-ai-assistant-page">
      <div class="erp-ai-assistant-empty">
        <h3>ERP Assistant</h3>
        <p>Desk page scaffold created. Connect this page to the APIs in <code>erp_ai_assistant.api</code>.</p>
      </div>
    </div>
  `);
};
