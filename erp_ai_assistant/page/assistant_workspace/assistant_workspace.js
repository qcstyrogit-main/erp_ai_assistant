frappe.pages["assistant-workspace"].on_page_load = function (wrapper) {
  const WEB_ASSISTANT_BUILD = "2026-04-06-grounded-ux-v10";
  const webAssistantAsset = `/assets/erp_ai_assistant/js/web_assistant.js?v=${encodeURIComponent(WEB_ASSISTANT_BUILD)}`;
  const webAssistantCssAsset = `/assets/erp_ai_assistant/css/web_assistant.css?v=${encodeURIComponent(WEB_ASSISTANT_BUILD)}`;
  const page = frappe.ui.make_app_page({
    parent: wrapper,
    title: "ERP Copilot",
    single_column: true,
  });

  const markup = `
    <div class="erp-web-assistant erp-web-assistant--desk" id="erp-web-assistant-desk">
      <aside class="erp-web-assistant__sidebar">
        <div class="erp-web-assistant__sidebar-head">
          <div>
            <p class="erp-web-assistant__eyebrow">ERP Copilot</p>
            <h3>Conversations</h3>
          </div>
          <button class="erp-web-assistant__btn erp-web-assistant__btn--primary" data-action="new-chat" type="button">New</button>
        </div>
        <div class="erp-web-assistant__sidebar-meta">
          <span class="erp-web-assistant__badge" data-role="conversation-count">0 chats</span>
          <span class="erp-web-assistant__badge erp-web-assistant__badge--muted" data-role="module-badge">General</span>
        </div>
        <input class="erp-web-assistant__search" data-role="search" type="search" placeholder="Search conversations" />
        <div class="erp-web-assistant__history" data-role="history"></div>
      </aside>

      <section class="erp-web-assistant__main">
        <header class="erp-web-assistant__header">
          <div class="erp-web-assistant__header-main">
            <div>
              <p class="erp-web-assistant__eyebrow">Current context</p>
              <p class="erp-web-assistant__context" data-role="context">General workspace</p>
            </div>
            <div class="erp-web-assistant__header-actions">
              <button class="erp-web-assistant__btn" data-action="focus-prompt" type="button">Ask</button>
              <button class="erp-web-assistant__btn" data-action="refresh-context" type="button">Refresh</button>
            </div>
          </div>
          <div class="erp-web-assistant__context-panel">
            <div class="erp-web-assistant__context-grid" data-role="context-meta"></div>
            <div class="erp-web-assistant__quick-actions" data-role="quick-actions"></div>
          </div>
        </header>

        <div class="erp-web-assistant__messages" data-role="messages"></div>

        <footer class="erp-web-assistant__composer">
          <div class="erp-web-assistant__suggestions" data-role="suggestions"></div>
          <textarea data-role="prompt" rows="4" placeholder="Ask about this record, create a safe draft, explain a status, or summarize work..."></textarea>
          <div class="erp-web-assistant__image-preview" data-role="image-preview"></div>
          <input type="file" data-role="image-input" accept="image/*" multiple hidden />
          <div class="erp-web-assistant__composer-actions">
            <div class="erp-web-assistant__composer-left">
              <select class="erp-web-assistant__model-select" data-role="model-select" aria-label="Select AI model">
                <option value="">Default model</option>
              </select>
              <small data-role="status">Ask, do, or explain — draft-first is safest.</small>
            </div>
            <div class="erp-web-assistant__composer-right">
              <button class="erp-web-assistant__btn" data-action="attach-image" type="button">Image</button>
              <button class="erp-web-assistant__btn erp-web-assistant__btn--primary" data-action="send" type="button">Send</button>
            </div>
          </div>
        </footer>
      </section>
    </div>
  `;

  $(page.body).html(markup);

  function ensureStyle(href) {
    if (document.querySelector(`link[href="${href}"]`)) return;
    const link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = href;
    document.head.appendChild(link);
  }

  function ensureScript(src, callback) {
    if (window.ERPWebAssistant && window.__ERP_AI_WEB_ASSISTANT_BUILD__ === WEB_ASSISTANT_BUILD) {
      callback();
      return;
    }
    const existing = document.querySelector(`script[src="${src}"]`);
    if (existing) {
      existing.addEventListener("load", callback, { once: true });
      return;
    }
    const script = document.createElement("script");
    script.src = src;
    script.async = true;
    script.onload = callback;
    document.body.appendChild(script);
  }

  ensureStyle(webAssistantCssAsset);
  ensureScript(webAssistantAsset, function () {
    const root = page.body.querySelector("#erp-web-assistant-desk");
    if (!root || !window.ERPWebAssistant || root.dataset.booted === "1") return;
    root.dataset.booted = "1";
    new window.ERPWebAssistant(root, { mode: "desk" }).boot();
  });
};
