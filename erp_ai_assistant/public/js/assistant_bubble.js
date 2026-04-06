(function () {
  window.__ERP_AI_ASSISTANT_BUILD__ = "2026-04-06-blink-fix-v9";

  function shouldShowToolActivity() {
    try {
      return window.localStorage?.getItem("erp_ai_assistant_debug_tools") === "1";
    } catch (error) {
      return false;
    }
  }

  function logAssistantDebug(payload) {
    const debug = payload && typeof payload === "object" ? payload.debug : null;
    const doctypes = Array.isArray(debug?.discovery_doctypes) ? debug.discovery_doctypes : [];
    if (!doctypes.length || !window.console) return;
    console.info("[ERP AI Assistant] _get_discovery_doctypes()", doctypes);
  }

  function isCompactViewport() {
    return window.matchMedia && window.matchMedia("(max-width: 900px)").matches;
  }

  /**
   * @class ERPAssistantBubble
   * @description Main class for the AI assistant UI component
   * Manages conversations, messages, and user interactions
   */
  class ERPAssistantBubble {
    constructor() {
      /** @type {boolean} */
      this.drawerOpen = false;

      /** @type {Array<Object>} */
      this.conversations = [];

      /** @type {Object|null} */
      this.activeConversation = null;

      /** @type {boolean} */
      this.isDraftConversation = false;

      /** @type {Object} */
      this.elements = {};

      /** @type {boolean} */
      this.isGenerating = false;

      /** @type {JQuery.jqXHR|null} */
      this.pendingPromptRequest = null;

      /** @type {boolean} */
      this.abortRequested = false;

      /** @type {string} */
      this.modelStorageKey = "erp_ai_assistant_selected_model";

      /** @type {number|null} */
      this.progressPollTimer = null;

      /** @type {boolean} */
      this.progressPollPending = false;

      /** @type {Array<Object>} */
      this.pendingImages = [];

        /** @type {Object<string, Array<Object>>} */
        this.pendingConversationMessages = {};

        /** @type {Object<string, Array<Object>>} */
        this.conversationMessageCache = {};

        /** @type {Object<string, Array<Object>>} */
        this.optimisticAssistantMessages = {};

        /** @type {Object<string, string>} */
        this.retryPromptGuards = {};

        /** @type {Object<string, boolean>} */
        this.awaitingQueueAck = {};

        /** @type {Object<string, number>} */
        this.completionReloadTimers = {};

        /** @type {Object<string, boolean>} */
        this.completionFetchLocks = {};

        /** @type {boolean} */
        this.globalEventsBound = false;

        /** @type {string} */
        this.lastStableContextText = "General chat";

    }

    boot() {
      if (!window.frappe || frappe.session?.user === "Guest") return;
      if (document.getElementById("erp-ai-assistant-bubble")) return;
      this.render();
      this.bindGlobalEvents();
      this.refreshHistory();
      this.updateContextHint();
    }

    render() {
      // ── Edge tab (replaces round floating bubble) ─────────────────────────────────
      // The tab is a vertical pill affixed to the right edge.
      // It peeks +4px when collapsed to signal its presence without blocking content.
      // On hover it reveals the full label; on click it opens the drawer.
      const bubble = document.createElement("button");
      bubble.id = "erp-ai-assistant-bubble";
      bubble.className = "erp-ai-assistant-bubble";
      bubble.setAttribute("aria-label", "Open AI Assistant");
      bubble.setAttribute("title", "AI Assistant — click to open (Ctrl+Enter)");
      bubble.innerHTML = `
        <span class="erp-ai-assistant-bubble__icon" aria-hidden="true">AI</span>
        <span class="erp-ai-assistant-bubble__label">AI Assistant</span>
      `;

      const drawer = document.createElement("div");
      drawer.id = "erp-ai-assistant-drawer";
      drawer.className = "erp-ai-assistant-drawer";
      drawer.setAttribute("role", "dialog");
      drawer.setAttribute("aria-label", "AI Assistant Dialog");
      drawer.innerHTML = `
        <div class="erp-ai-assistant-drawer__sidebar">
          <div class="erp-ai-assistant-drawer__rail">
            <button class="erp-ai-assistant-rail__logo" data-action="toggle-sidebar" title="Toggle sidebar" type="button">AI</button>
            <button class="erp-ai-assistant-rail__btn" data-action="new-chat" title="New chat" type="button">✎</button>
            <button class="erp-ai-assistant-rail__btn" data-action="focus-search" title="Search chats" type="button">⌕</button>
          </div>
          <div class="erp-ai-assistant-drawer__sidebar-panel">
            <div class="erp-ai-assistant-drawer__sidebar-head">
              <div>
                <div class="erp-ai-assistant-drawer__eyebrow">AI Assistant</div>
                <h3>Chats</h3>
              </div>
            </div>
            <div class="erp-ai-assistant-sidebar__shortcuts">
              <button class="erp-ai-assistant-sidebar__shortcut" data-action="new-chat" type="button">
                <span class="erp-ai-assistant-sidebar__shortcut-icon">✎</span>
                <span>New chat</span>
              </button>
              <button class="erp-ai-assistant-sidebar__shortcut" data-action="focus-search" type="button">
                <span class="erp-ai-assistant-sidebar__shortcut-icon">⌕</span>
                <span>Search chats</span>
              </button>
            </div>
            <input class="erp-ai-assistant-search" placeholder="Search conversations..." aria-label="Search conversations" />
            <div class="erp-ai-assistant-history"></div>
          </div>
        </div>
        <div class="erp-ai-assistant-drawer__main">
          <div class="erp-ai-assistant-drawer__header">
            <div class="erp-ai-assistant-drawer__header-main">
              <button class="erp-ai-assistant-btn erp-ai-assistant-btn--ghost" data-action="toggle-sidebar" title="Toggle sidebar" type="button">☰</button>
              <div>
                <div class="erp-ai-assistant-drawer__eyebrow">Current Context</div>
                <div class="erp-ai-assistant-context" title="The current page context being used for responses"></div>
              </div>
            </div>
            <div class="erp-ai-assistant-drawer__header-actions">
              <button class="erp-ai-assistant-btn erp-ai-assistant-btn--ghost" data-action="connectors" title="Connector Status" type="button">Connectors</button>
              <button class="erp-ai-assistant-btn erp-ai-assistant-btn--ghost" data-action="close" title="Close assistant (Esc)" type="button">Close</button>
            </div>
          </div>
          <div class="erp-ai-assistant-messages"></div>
          <div class="erp-ai-assistant-composer">
            <div class="erp-ai-assistant-composer__shell">
              <textarea class="erp-ai-assistant-composer__textarea" rows="1" placeholder="Ask anything. If you are on a document, I can also use that ERP context..." aria-label="Message input"></textarea>
              <div class="erp-ai-assistant-image-preview"></div>
              <input type="file" class="erp-ai-assistant-image-input" accept="image/*" multiple hidden />
              <div class="erp-ai-assistant-composer__actions">
                <div class="erp-ai-assistant-composer__left">
                  <select class="erp-ai-assistant-model-select" aria-label="Select AI model">
                    <option value="">Default model</option>
                  </select>
                </div>
                <div class="erp-ai-assistant-composer__buttons">
                  <button class="erp-ai-assistant-btn" data-action="attach-image" title="Attach image or paste screenshot" type="button">📎</button>
                  <button class="erp-ai-assistant-btn erp-ai-assistant-btn--stop" data-action="stop" title="Stop response" aria-label="Stop response" hidden>⏹</button>
                  <button class="erp-ai-assistant-btn erp-ai-assistant-btn--primary" data-action="send" title="Send message">Send ↑</button>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div id="erp-ai-connector-panel" class="erp-ai-connector-panel" hidden>
          <div class="erp-ai-connector-panel__header">
            <h3>Connector Status & Health</h3>
            <button class="erp-ai-assistant-btn" data-action="close-connectors" title="Close Settings">Close</button>
          </div>
          <div class="erp-ai-connector-panel__content" aria-live="polite">
            <div class="erp-ai-connector-panel__loading">Loading connector status...</div>
          </div>
        </div>
      `;

      document.body.appendChild(bubble);
      document.body.appendChild(drawer);

      this.elements = {
        bubble,
        drawer,
        history: drawer.querySelector(".erp-ai-assistant-history"),
        search: drawer.querySelector(".erp-ai-assistant-search"),
        context: drawer.querySelector(".erp-ai-assistant-context"),
        messages: drawer.querySelector(".erp-ai-assistant-messages"),
        textarea: drawer.querySelector("textarea"),
        imagePreview: drawer.querySelector(".erp-ai-assistant-image-preview"),
        imageInput: drawer.querySelector(".erp-ai-assistant-image-input"),
        attachImageButton: drawer.querySelector('[data-action="attach-image"]'),
        modelSelect: drawer.querySelector(".erp-ai-assistant-model-select"),
        sendButton: drawer.querySelector('[data-action="send"]'),
        stopButton: drawer.querySelector('[data-action="stop"]'),
        connectorPanel: drawer.querySelector('#erp-ai-connector-panel'),
        connectorContent: drawer.querySelector('.erp-ai-connector-panel__content'),
        conversationMenu: null,
      };

      const autoGrow = (el) => {
        if (!el) return;
        el.style.height = "auto";
        el.style.height = Math.min(el.scrollHeight, 240) + "px";
      };

      bubble.addEventListener("click", () => this.toggleDrawer());
      drawer.querySelector('[data-action="close"]')?.addEventListener("click", () => this.toggleDrawer(false));
      drawer.querySelectorAll('[data-action="toggle-sidebar"]').forEach((element) => {
        element.addEventListener("click", () => this.toggleSidebar());
      });
      drawer.querySelector('[data-action="close-sidebar"]')?.addEventListener("click", () => {
        if (isCompactViewport()) {
          this.closeMobileSidebar();
        } else {
          this.toggleSidebar(true);
        }
      });
      drawer.querySelector('[data-action="connectors"]')?.addEventListener("click", () => this.toggleConnectorPanel(true));
      drawer.querySelector('[data-action="close-connectors"]')?.addEventListener("click", () => this.toggleConnectorPanel(false));
      drawer.querySelector('[data-action="send"]')?.addEventListener("click", () => this.sendPrompt());
      drawer.querySelector('[data-action="stop"]')?.addEventListener("click", () => this.stopPrompt());
      drawer.querySelectorAll('[data-action="new-chat"]').forEach((element) => {
        element.addEventListener("click", () => this.startDraftConversation());
      });
      drawer.querySelector('[data-action="new-chat-visible"]')?.addEventListener("click", () => this.startDraftConversation());
      drawer.querySelectorAll('[data-action="focus-search"]').forEach((element) => {
        element.addEventListener("click", () => {
        if (isCompactViewport()) {
          this.elements.drawer?.classList.add("mobile-sidebar-open");
        } else {
          this.elements.drawer?.classList.remove("sidebar-collapsed");
        }
        this.elements.search?.focus();
        });
      });
      drawer.addEventListener("click", (event) => {
        if (!drawer.classList.contains("mobile-sidebar-open")) return;
        if (event.target.closest(".erp-ai-assistant-drawer__sidebar")) return;
        if (event.target.closest('[data-action="toggle-sidebar"]')) return;
        this.closeMobileSidebar();
      });
      this.elements.attachImageButton?.addEventListener("click", () => this.elements.imageInput?.click());
      this.elements.imageInput?.addEventListener("change", async (event) => {
        await this._ingestImageFiles(event?.target?.files);
        if (this.elements.imageInput) {
          this.elements.imageInput.value = "";
        }
      });
      this.elements.modelSelect?.addEventListener("change", () => this._persistSelectedModel());
      this.elements.search?.addEventListener("input", () => this.renderHistory());
      this.elements.textarea?.addEventListener("input", () => autoGrow(this.elements.textarea));
      this.elements.textarea?.addEventListener("keydown", (event) => {
        if (event.key === "Enter" && !event.shiftKey) {
          event.preventDefault();
          this.sendPrompt();
        }
      });
      this.elements.textarea?.addEventListener("paste", async (event) => {
        const items = Array.from(event.clipboardData?.items || []).filter((item) => String(item.type || "").startsWith("image/"));
        if (!items.length) return;
        event.preventDefault();
        const files = items.map((item) => item.getAsFile()).filter(Boolean);
        await this._ingestImageFiles(files);
      });

      // Keyboard shortcuts
      document.addEventListener("keydown", (event) => {
        if (event.target === this.elements.textarea) {
          if (event.key === "Enter" && !event.shiftKey && (event.ctrlKey || event.metaKey)) {
            event.preventDefault();
            this.sendPrompt();
          }
          return;
        }
        if (event.ctrlKey || event.metaKey) {
          switch (event.key) {
            case "Enter":
              event.preventDefault();
              this.elements.textarea?.focus();
              break;
            case "p":
              if (event.shiftKey) {
                event.preventDefault();
                if (this.activeConversation) this.togglePin(this.activeConversation.name);
              }
              break;
            case "n":
              if (event.shiftKey) {
                event.preventDefault();
                this.startDraftConversation();
              }
              break;
            case "f":
              if (event.shiftKey) {
                event.preventDefault();
                this.elements.search?.focus();
              }
              break;
          }
        }
        if (event.key === "Escape" && this.drawerOpen) {
          event.preventDefault();
          this.closeConversationMenu();
          this.toggleDrawer(false);
        }
      });

      window.addEventListener("resize", () => {
        if (!isCompactViewport()) {
          this.elements.drawer?.classList.remove("mobile-sidebar-open");
        }
        this.closeConversationMenu();
      });

      this._setGeneratingState(false);
      this.loadModelOptions();
    }

    bindGlobalEvents() {
      if (this.globalEventsBound) return;
      this.globalEventsBound = true;
      window.addEventListener("hashchange", () => this.updateContextHint());
      $(document).on("page-change", () => this.updateContextHint());
      document.addEventListener("visibilitychange", () => {
        if (!document.hidden) {
          this.updateContextHint();
        }
      });
    }

    toggleDrawer(forceState) {
      this.drawerOpen = typeof forceState === "boolean" ? forceState : !this.drawerOpen;
      this.elements.drawer?.classList.toggle("is-open", this.drawerOpen);
      // Edge tab: hide completely when drawer is open, restore when closed
      if (this.elements.bubble) {
        if (this.drawerOpen) {
          this.elements.bubble.setAttribute("aria-hidden", "true");
          this.elements.bubble.style.pointerEvents = "none";
          this.elements.bubble.style.opacity = "0";
        } else {
          this.elements.bubble.removeAttribute("aria-hidden");
          this.elements.bubble.style.pointerEvents = "";
          this.elements.bubble.style.opacity = "";
          this.toggleConnectorPanel(false); // Make sure it closes when drawer closes
        }
      }
      if (this.drawerOpen) {
        this.updateContextHint();
        this.refreshHistory();
        this.elements.textarea?.focus();
      } else {
        this.closeConversationMenu();
      }
    }

    toggleSidebar(forceState) {
      const drawer = this.elements.drawer;
      if (!drawer) return;
      if (isCompactViewport()) {
        const open = typeof forceState === "boolean" ? forceState : !drawer.classList.contains("mobile-sidebar-open");
        drawer.classList.toggle("mobile-sidebar-open", open);
        return;
      }
      const collapsed = typeof forceState === "boolean" ? forceState : !drawer.classList.contains("sidebar-collapsed");
      drawer.classList.toggle("sidebar-collapsed", collapsed);
      this.closeConversationMenu();
    }

    closeMobileSidebar() {
      if (isCompactViewport()) {
        this.elements.drawer?.classList.remove("mobile-sidebar-open");
      }
    }

    toggleConnectorPanel(forceState) {
      if (!this.elements.connectorPanel) return;
      const isOpen = typeof forceState === 'boolean' 
        ? forceState 
        : this.elements.connectorPanel.hasAttribute('hidden');
        
      if (isOpen) {
        this.elements.connectorPanel.removeAttribute('hidden');
        this.refreshConnectors();
      } else {
        this.elements.connectorPanel.setAttribute('hidden', '');
      }
    }

    refreshConnectors() {
      if (!this.elements.connectorContent) return;
      
      this.elements.connectorContent.innerHTML = '<div class="erp-ai-connector-panel__loading">Testing connections...</div>';
      
      const calls = [
        frappe.call({ method: "erp_ai_assistant.api.assistant.test_ai_provider_connection" }),
        frappe.call({ method: "erp_ai_assistant.api.assistant.test_fac_mcp_connection" }),
        frappe.call({ method: "erp_ai_assistant.api.assistant.ping_assistant" })
      ];

      Promise.allSettled(calls).then((results) => {
        const [llmRes, facRes, pingRes] = results;
        
        let html = '<div class="erp-ai-connector-list">';
        
        // LLM Gateway Status
        const llm = llmRes.status === 'fulfilled' ? llmRes.value.message || {} : { ok: false, error: 'Request failed' };
        html += this._renderConnectorCard(
          "LLM Gateway",
          llm.ok ? 'connected' : 'error',
          llm.provider || 'Unknown',
          llm.model || 'Unknown',
          llm.ok ? 'Active Connection' : (llm.message || llm.error)
        );

        // FAC / Tool Host Status
        const fac = facRes.status === 'fulfilled' ? facRes.value.message || {} : { ok: false, error: 'Request failed' };
        html += this._renderConnectorCard(
          "Tool Host (FAC MCP)",
          fac.ok ? 'connected' : 'error',
          fac.provider || 'Local / MCP',
          fac.ok ? 'Tools active' : 'Offline',
          fac.ok ? fac.message : (fac.message || fac.error)
        );

        // Core App Status
        const ping = pingRes.status === 'fulfilled' ? pingRes.value.message || {} : { ok: false, error: 'Request failed' };
        html += this._renderConnectorCard(
          "ERP Core App",
          ping.ok ? 'connected' : 'error',
          'Internal',
          'v1',
          ping.ok ? 'Ready' : (ping.message || ping.error)
        );

        html += '</div>';
        this.elements.connectorContent.innerHTML = html;
      });
    }

    _renderConnectorCard(title, status, provider, detail1, detail2) {
      const color = status === 'connected' ? '#10b981' : '#ef4444';
      const icon = status === 'connected' ? '✓' : '⚠️';
      return `
        <div class="erp-ai-connector-card" style="border-left: 4px solid ${color}">
          <div class="erp-ai-connector-card__title">
            <strong>${title}</strong>
            <span style="color: ${color}">${icon}</span>
          </div>
          <div class="erp-ai-connector-card__body">
            <div><small>Provider:</small> ${provider}</div>
            <div><small>Info:</small> ${detail1}</div>
            <div style="margin-top:4px; font-size:11px; opacity:0.8">${detail2}</div>
          </div>
        </div>
      `;
    }

    getCurrentContext() {
      let rawRoute = [];

      try {
        rawRoute = frappe.get_route ? frappe.get_route() : [];
      } catch (e) {
        rawRoute = [];
      }

      let route = [];

      if (Array.isArray(rawRoute)) {
        route = rawRoute
          .filter((segment) => segment !== null && segment !== undefined)
          .map((segment) => String(segment));
      } else if (typeof rawRoute === "string") {
        route = rawRoute.split("/").filter(Boolean);
      } else if (rawRoute && typeof rawRoute.route === "string") {
        route = rawRoute.route.split("/").filter(Boolean);
      }

      if (!route.length && typeof window.location?.hash === "string" && window.location.hash) {
        route = window.location.hash.replace(/^#/, "").split("/").filter(Boolean);
      }

      const routeText = route.join("/");
      const [rawView, doctype, docname, extra] = route;
      const view = String(rawView || "").trim();
      const normalizedView = view.toLowerCase();
      const isFormRoute = normalizedView === "form";
      const form = window.cur_frm || (typeof cur_frm !== "undefined" ? cur_frm : null);
      const formMatchesRoute = Boolean(
        form
        && form.doc
        && isFormRoute
        && String(form.doctype || "") === String(doctype || "")
        && String(form.docname || "") === String(docname || "")
      );

      if (formMatchesRoute) {
        return {
          doctype: form.doctype || null,
          docname: form.docname || null,
          route: routeText,
          label: `${form.doctype || "Document"} / ${form.docname || ""}`.trim(),
        };
      }

      if (normalizedView === "list") {
        const listView = String(extra || "").trim();
        return {
          doctype: doctype || null,
          docname: null,
          route: routeText,
          label: doctype ? `${doctype} List${listView ? ` / ${listView}` : ""}` : "List View",
        };
      }

      if (normalizedView === "tree") {
        return {
          doctype: doctype || null,
          docname: null,
          route: routeText,
          label: doctype ? `${doctype} Tree` : "Tree View",
        };
      }

      if (normalizedView === "kanban") {
        return {
          doctype: doctype || null,
          docname: null,
          route: routeText,
          label: doctype ? `${doctype} Kanban` : "Kanban View",
        };
      }

      if (normalizedView === "query-report" || normalizedView === "report") {
        return {
          doctype: null,
          docname: null,
          route: routeText,
          label: doctype ? `Report / ${doctype}` : "Report",
        };
      }

      if (normalizedView === "workspace") {
        return {
          doctype: null,
          docname: null,
          route: routeText,
          label: doctype ? `Workspace / ${doctype}` : "Workspace",
        };
      }

      if (normalizedView === "dashboard") {
        return {
          doctype: null,
          docname: null,
          route: routeText,
          label: doctype ? `Dashboard / ${doctype}` : "Dashboard",
        };
      }

      if (normalizedView === "print") {
        return {
          doctype: doctype || null,
          docname: docname || null,
          route: routeText,
          label: doctype && docname ? `${doctype} / ${docname} (Print)` : "Print View",
        };
      }

      return {
        doctype: isFormRoute ? doctype || null : null,
        docname: isFormRoute ? docname || null : null,
        route: routeText,
        label: routeText || "General chat",
      };
    }

    updateContextHint() {
      let context = { doctype: null, docname: null, route: "", label: "General chat" };
      try {
        const resolved = this.getCurrentContext();
        if (resolved && typeof resolved === "object") {
          context = resolved;
        }
      } catch (error) {
        // Keep UI alive even if route/context cannot be resolved on a custom Desk page.
        console.warn("AI Assistant: context resolution failed", error);
      }

      const text = context.doctype && context.docname
        ? `${context.doctype} / ${context.docname}`
        : (context.label || "General chat");
      const routeText = String(context.route || "").trim();
      const isFallbackContext = !context.doctype && !context.docname && !routeText && text === "General chat";
      const displayText = this.isGenerating && isFallbackContext && this.lastStableContextText
        ? this.lastStableContextText
        : text;

      if (!isFallbackContext) {
        this.lastStableContextText = text;
      }

      if (this.elements.context) {
        this.elements.context.textContent = displayText;
      }
    }

    refreshHistory() {
      frappe.call({
        method: "erp_ai_assistant.api.chat.list_conversations",
        callback: (response) => {
          this.conversations = response.message || [];
          this.renderHistory();
          if (!this.activeConversation && !this.isDraftConversation && this.conversations.length) {
            this.loadConversation(this.conversations[0].name);
          }
        },
        error: () => {
          this.conversations = [];
          this.renderHistory();
        },
      });
    }

    renderHistory() {
      if (!this.elements.history) return;

      const query = (this.elements.search?.value || "").toLowerCase();
      this.elements.history.innerHTML = "";

      const rows = this.conversations.filter(
        (row) => !query || (row.title || "").toLowerCase().includes(query)
      );

      if (!rows.length) {
        this.elements.history.innerHTML = `<div class="erp-ai-assistant-history__empty">No conversations</div>`;
        return;
      }

      const dateGroup = (dateStr) => {
        if (!dateStr) return "Older";
        const d = new Date(dateStr), now = new Date();
        const diffDays = Math.floor((now - d) / 86400000);
        if (diffDays === 0) return "Today";
        if (diffDays === 1) return "Yesterday";
        if (diffDays <= 7) return "Previous 7 days";
        return "Older";
      };

      let lastGroup = "";
      rows.forEach((row) => {
        const group = dateGroup(row.modified);
        if (group !== lastGroup) {
          lastGroup = group;
          const label = document.createElement("div");
          label.className = "erp-ai-assistant-history__group-label";
          label.textContent = group;
          label.style.cssText = "font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: rgba(255,255,255,0.35); padding: 12px 8px 4px;";
          this.elements.history.appendChild(label);
        }

        const item = document.createElement("div");
        item.className = "erp-ai-assistant-history__item";
        item.style.cssText = "display: flex; align-items: center; justify-content: space-between; padding: 10px 12px; border-radius: 10px; cursor: pointer; transition: background 180ms ease; color: rgba(255,255,255,0.8); margin-bottom: 2px;";

        if (this.activeConversation && this.activeConversation.name === row.name) {
          item.classList.add("is-active");
          item.style.background = "var(--ai-secondary-light)";
          item.style.color = "#fff";
          item.style.borderLeft = "3px solid var(--ai-primary)";
        }

        item.innerHTML = `
          <div class="erp-ai-assistant-history__content" style="flex: 1; min-width: 0; overflow: hidden;">
            <div class="erp-ai-assistant-history__title" style="font-weight: 500; font-size: 13px; text-overflow: ellipsis; white-space: nowrap; overflow: hidden;">${frappe.utils.escape_html(row.title || "New chat")}</div>
          </div>
          <div class="erp-ai-assistant-history__actions" style="display: flex; gap: 4px; opacity: 0; transition: opacity 180ms ease;">
            <button class="erp-ai-assistant-history__icon" type="button" data-action="rename" title="Rename" style="width: 24px; height: 24px; border: 0; border-radius: 6px; background: rgba(255,255,255,0.08); color: rgba(255,255,255,0.5); font-size: 12px; cursor: pointer;">✎</button>
            <button class="erp-ai-assistant-history__icon" type="button" data-action="delete" title="Delete" style="width: 24px; height: 24px; border: 0; border-radius: 6px; background: rgba(255,255,255,0.08); color: rgba(255,255,255,0.5); font-size: 12px; cursor: pointer;">×</button>
          </div>
        `;

        item.addEventListener("mouseenter", () => {
          if (!item.classList.contains("is-active")) item.style.background = "var(--ai-secondary-light)";
          const actions = item.querySelector(".erp-ai-assistant-history__actions");
          if (actions) actions.style.opacity = "1";
        });
        item.addEventListener("mouseleave", () => {
          if (!item.classList.contains("is-active")) item.style.background = "transparent";
          const actions = item.querySelector(".erp-ai-assistant-history__actions");
          if (actions) actions.style.opacity = "0";
        });

        item.querySelector('[data-action="rename"]')?.addEventListener("click", (e) => {
          e.stopPropagation();
          const newTitle = window.prompt("Rename conversation:", row.title || "");
          if (!newTitle || newTitle === row.title) return;
          frappe.call({ method: "erp_ai_assistant.api.chat.rename_conversation", args: { name: row.name, title: newTitle }, callback: () => this.refreshHistory() });
        });
        item.querySelector('[data-action="delete"]')?.addEventListener("click", (e) => {
          e.stopPropagation();
          this.deleteConversation(row.name);
        });
        item.addEventListener("click", () => this.loadConversation(row.name));
        this.elements.history.appendChild(item);
      });
    }

    startDraftConversation() {
      this.activeConversation = null;
      this.isDraftConversation = true;
      this.closeMobileSidebar();
      this._clearPendingImages();
      this.renderMessages([]);
      this.renderHistory();
      if (this.elements.textarea) {
        this.elements.textarea.value = "";
        this.elements.textarea.focus();
      }
    }

    createConversation(selectAfterCreate) {
      frappe.call({
        method: "erp_ai_assistant.api.chat.create_conversation",
        callback: (response) => {
          const conversation = response.message;
          this.isDraftConversation = false;
          this.refreshHistory();
          if (selectAfterCreate && conversation) {
            this.loadConversation(conversation.name);
          }
        },
        error: (error) => {
          frappe.show_alert({ message: error.message || "Assistant setup is incomplete", indicator: "red" });
        },
      });
    }

    loadConversation(name, options) {
      const settings = options || {};
      frappe.call({
        method: "erp_ai_assistant.api.chat.get_conversation",
        args: { name },
        callback: (response) => {
          const payload = response.message || {};
          this.activeConversation = payload.conversation || null;
          this.isDraftConversation = false;
          if (!settings.silent) {
            this.closeMobileSidebar();
            this._clearPendingImages();
          }
          const serverMessages = this._dedupeRetryUserEcho(name, Array.isArray(payload.messages) ? payload.messages : []);
          this.conversationMessageCache[name] = serverMessages;
          const optimisticMessages = Array.isArray(this.optimisticAssistantMessages[name])
            ? this.optimisticAssistantMessages[name]
            : [];
          let hasMatchingServerReply = false;
          if (optimisticMessages.length) {
            const lastOptimistic = optimisticMessages[optimisticMessages.length - 1];
            hasMatchingServerReply = serverMessages.some((row) =>
              row
              && row.role === "assistant"
              && String(row.content || "").trim() === String(lastOptimistic.content || "").trim()
            );
            if (hasMatchingServerReply) {
              delete this.optimisticAssistantMessages[name];
            }
          }
          const shouldRender = !settings.silent && (!settings.waitForOptimisticConfirmation || hasMatchingServerReply || !optimisticMessages.length);
          if (shouldRender) {
            this.renderMessages(this._getConversationMessages(name));
            this.renderHistory();
          }
          if (typeof settings.onLoaded === "function") {
            settings.onLoaded(payload, serverMessages);
          }
        },
        error: (error) => {
          this.activeConversation = null;
          if (!settings.silent) {
            this.renderMessages([]);
          }
          frappe.show_alert({ message: error.message || "Unable to load conversation", indicator: "red" });
        },
      });
    }

    _dedupeRetryUserEcho(conversationName, rows) {
      const items = Array.isArray(rows) ? rows.slice() : [];
      const guardedPrompt = String(this.retryPromptGuards[conversationName] || "").trim();
      if (!guardedPrompt || items.length < 2) return items;

      const deduped = [];
      let removedDuplicate = false;
      for (const row of items) {
        const prev = deduped.length ? deduped[deduped.length - 1] : null;
        const isDuplicateRetryUser =
          prev
          && prev.role === "user"
          && row?.role === "user"
          && String(prev.content || "").trim() === guardedPrompt
          && String(row.content || "").trim() === guardedPrompt;
        if (isDuplicateRetryUser) {
          removedDuplicate = true;
          continue;
        }
        deduped.push(row);
      }

      if (removedDuplicate || deduped.some((row) => row?.role === "assistant")) {
        delete this.retryPromptGuards[conversationName];
      }
      return deduped;
    }

    renderMessages(messages) {
      if (!this.elements.messages) return;

      this.elements.messages.innerHTML = "";

      if (!messages.length) {
        const emptyState = document.createElement("div");
        emptyState.className = "erp-ai-assistant-messages__empty";
        emptyState.innerHTML = `
          <div class="erp-ai-assistant-messages__empty-icon"></div>
          <h2>How can I help you today?</h2>
          <div class="erp-ai-assistant-messages__suggestions">
            <button class="erp-ai-assistant-suggestion-card" type="button" data-prompt="What are my pending tasks for today?">
              <span class="erp-ai-assistant-suggestion-icon">📋</span>
              <span class="erp-ai-assistant-suggestion-text">What are my pending tasks for today?</span>
            </button>
            <button class="erp-ai-assistant-suggestion-card" type="button" data-prompt="Show me recent sales orders">
              <span class="erp-ai-assistant-suggestion-icon">📈</span>
              <span class="erp-ai-assistant-suggestion-text">Show me recent sales orders</span>
            </button>
            <button class="erp-ai-assistant-suggestion-card" type="button" data-prompt="Help me draft an email to a supplier">
              <span class="erp-ai-assistant-suggestion-icon">✉️</span>
              <span class="erp-ai-assistant-suggestion-text">Help me draft an email...</span>
            </button>
            <button class="erp-ai-assistant-suggestion-card" type="button" data-prompt="Analyze inventory levels">
              <span class="erp-ai-assistant-suggestion-icon">📦</span>
              <span class="erp-ai-assistant-suggestion-text">Analyze inventory levels</span>
            </button>
          </div>
        `;

        const suggestionButtons = emptyState.querySelectorAll(".erp-ai-assistant-suggestion-card");
        suggestionButtons.forEach(btn => {
          btn.addEventListener("click", () => {
            const prompt = btn.getAttribute("data-prompt");
            if (this.elements.textarea) {
              this.elements.textarea.value = prompt;
              this.elements.textarea.style.height = "auto";
              this.sendPrompt();
            }
          });
        });

        this.elements.messages.appendChild(emptyState);
        return;
      }

      messages.forEach((message, index) => {
        const bubble = document.createElement("div");
        bubble.className = `erp-ai-assistant-message ${message.role === "user" ? "is-user" : "is-assistant"}`;
        bubble.setAttribute("data-msg-idx", index);

        const isLastAssistantMessage = message.role === "assistant" && (index === messages.length - 1 || (index === messages.length - 2 && messages[messages.length - 1].role === "user"));
        const roleLabel = message.role === "user" ? "You" : "Assistant";
        const content = message.content || "";
        const toolEvents = message.role !== "user" ? this._parseToolEvents(message.tool_events) : [];
        const attachmentPackage = this._parseAttachmentPackage(message.attachments_json);

        bubble.innerHTML = `
          <div class="erp-ai-assistant-message__meta">
            <span class="erp-ai-assistant-message__role erp-ai-assistant-message__role--${message.role}" ${message.role === "user" ? 'style="color: var(--ai-primary)"' : ''}>${roleLabel}</span>
          </div>
          <div class="erp-ai-assistant-message__body"></div>
        `;

        const bodyElement = bubble.querySelector(".erp-ai-assistant-message__body");

        if (toolEvents.length) {
          const activity = this._renderToolEventRow(toolEvents);
          if (activity) {
            bubble.insertBefore(activity, bodyElement);
          }
        }

        bodyElement.innerHTML = this._formatMessageContent(content);

        if (attachmentPackage.copilot) {
          const copilotBlock = this._renderCopilotBlock(attachmentPackage.copilot);
          if (copilotBlock) bubble.appendChild(copilotBlock);
        }
        if (attachmentPackage.attachments.length) {
          bubble.appendChild(this._renderAttachmentRow(attachmentPackage.attachments));
        }

        if (message.role !== "user" && content) {
          const actionsWrap = document.createElement("div");
          actionsWrap.className = "erp-ai-assistant-message__actions";

          const copyBtn = document.createElement("button");
          copyBtn.className = "erp-ai-assistant-message__action-btn";
          copyBtn.innerHTML = "⧉ Copy";
          copyBtn.type = "button";
          copyBtn.addEventListener("click", async (event) => {
            event.preventDefault();
            event.stopPropagation();
            const copied = await this._copyText(content);
            if (copied) {
              copyBtn.innerHTML = "✓ Copied";
              copyBtn.classList.add("is-copied");
              setTimeout(() => {
                copyBtn.innerHTML = "⧉ Copy";
                copyBtn.classList.remove("is-copied");
              }, 2000);
            } else {
              copyBtn.innerHTML = "! Failed";
              setTimeout(() => {
                copyBtn.innerHTML = "⧉ Copy";
              }, 2000);
            }
          });
          actionsWrap.appendChild(copyBtn);

          if (isLastAssistantMessage) {
              const retryBtn = document.createElement("button");
              retryBtn.className = "erp-ai-assistant-message__action-btn";
              retryBtn.innerHTML = "⟳ Retry";
              retryBtn.type = "button";
              retryBtn.addEventListener("click", async (event) => {
                  event.preventDefault();
                  event.stopPropagation();
                  this.retryLastResponse();
              });
              actionsWrap.appendChild(retryBtn);
          }

          bubble.appendChild(actionsWrap);
        }

        this.elements.messages.appendChild(bubble);
      });

      this.elements.messages.scrollTop = this.elements.messages.scrollHeight;
    }

    _formatMessageContent(content) {
      return this._renderRichText(content || "");
    }

    _buildMessageChrome(message, context) {
      const role = String(message?.role || "assistant");
      if (role === "user") return { badges: [], summary: "" };
      const toolEvents = Array.isArray(context?.toolEvents) ? context.toolEvents : [];
      const attachmentPackage = context?.attachmentPackage || { attachments: [], exports: {}, copilot: null };
      const content = String(context?.content || "");
      const hasCopilot = !!attachmentPackage?.copilot;
      const hasAttachments = Array.isArray(attachmentPackage?.attachments) && attachmentPackage.attachments.length > 0;
      const hasDataset = attachmentPackage?.exports && Object.keys(attachmentPackage.exports).length > 0;
      const hasTable = /\|.+\|/.test(content) || /```(?:json|sql|python|javascript|html|css)/i.test(content);
      const textEvents = toolEvents.filter((item) => typeof item === "string" && String(item || "").trim());
      const structuredEvents = toolEvents.filter((item) => item && typeof item === "object" && !Array.isArray(item));
      const badges = [];
      if (textEvents.length || structuredEvents.length) badges.push({ label: "ERP verified", tone: "verified" });
      if (hasCopilot || hasAttachments || hasDataset || hasTable) badges.push({ label: "Visualized", tone: "visual" });
      const riskyAction = textEvents.some((item) => /submit_document|run_workflow|delete_document/i.test(String(item || "")));
      if (!riskyAction) badges.push({ label: "Draft-safe", tone: "safe" });

      let summary = "";
      if (hasCopilot && attachmentPackage.copilot.summary) {
        summary = String(attachmentPackage.copilot.summary.badge || attachmentPackage.copilot.summary.title || "").trim();
      }
      if (!summary && textEvents.length) {
        const parsed = textEvents.map((item) => this._parseToolEventString(item)).filter(Boolean);
        summary = parsed.slice(0, 2).map((item) => item.label).join(" • ");
      }
      if (!summary && hasAttachments) summary = `${attachmentPackage.attachments.length} attachment${attachmentPackage.attachments.length === 1 ? "" : "s"} ready`;
      if (!summary && hasDataset) summary = "Structured ERP output ready";
      return { badges, summary };
    }

    // ─── Claude Desktop-level rich text rendering ─────────────────────────────
    // Token sentinel uses characters that survive _escapeHtml unchanged.
    // \x02 (STX) and \x03 (ETX) are never in user text and never touched by escaping.

    _tok(store, html) {
      const id = store.length;
      store.push(html);
      return `\x02${id}\x03`;
    }

    _restoreAll(text, store) {
      // Keep replacing until no more sentinels remain (handles nested tokens)
      let out = text;
      let prev = null;
      while (out !== prev) {
        prev = out;
        out = out.replace(/\x02(\d+)\x03/g, (_, i) => store[+i] || "");
      }
      return out;
    }

    _renderRichText(content) {
      if (!content) return "";
      const store = [];

      // 1. Fenced code blocks  ```lang\ncode\n```
      let s = String(content).replace(/```([^\n`]*)\n?([\s\S]*?)```/g, (_, lang, code) =>
        this._tok(store, this._renderCodeBlock(code || "", (lang || "").trim()))
      );

      // 2. Inline code `...`
      s = s.replace(/`([^`\n]+)`/g, (_, code) =>
        this._tok(store, `<code class="erp-ai-code-inline">${this._escapeHtml(code)}</code>`)
      );

      // 3. Paragraphs
      const html = s.split(/\n{2,}/).map((chunk) => this._renderBlock(chunk, store)).filter(Boolean).join("");

      return this._restoreAll(html, store);
    }

    _renderCodeBlock(code, lang) {
      const highlighted = this._syntaxHighlight(code.replace(/\n$/, ""), lang);
      const langLabel = lang ? `<span class="erp-ai-code-lang">${this._escapeHtml(lang)}</span>` : "";
      const copyId = `copy-${Math.random().toString(36).slice(2, 9)}`;
      return `
        <div class="erp-ai-code-block">
          <div class="erp-ai-code-block__header">
            ${langLabel}
            <button class="erp-ai-code-block__copy" data-copy-target="${copyId}" type="button" title="Copy code">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
              Copy
            </button>
          </div>
          <pre class="erp-ai-code-block__pre"><code id="${copyId}" class="erp-ai-code-block__code language-${this._escapeHtml(lang || "text")}">${highlighted}</code></pre>
        </div>`;
    }

    _syntaxHighlight(code, lang) {
      const escaped = this._escapeHtml(code);
      const l = (lang || "").toLowerCase();

      if (l === "python" || l === "py") return this._hlPython(escaped);
      if (l === "javascript" || l === "js" || l === "ts" || l === "typescript") return this._hlJS(escaped);
      if (l === "json") return this._hlJSON(escaped);
      if (l === "sql") return this._hlSQL(escaped);
      if (l === "bash" || l === "sh" || l === "shell") return this._hlBash(escaped);
      if (l === "html" || l === "xml") return this._hlHTML(escaped);
      if (l === "css" || l === "scss") return this._hlCSS(escaped);
      return escaped;
    }

    _hlPython(code) {
      const kw = /\b(def|class|import|from|return|if|elif|else|for|while|try|except|finally|with|as|pass|break|continue|and|or|not|in|is|lambda|yield|raise|del|global|nonlocal|True|False|None|async|await)\b/g;
      const builtins = /\b(print|len|range|str|int|float|list|dict|set|tuple|bool|type|isinstance|hasattr|getattr|setattr|enumerate|zip|map|filter|sorted|reversed|sum|min|max|abs|round|open|super|property|staticmethod|classmethod)\b/g;
      const strings = /(&#39;&#39;&#39;[\s\S]*?&#39;&#39;&#39;|&quot;&quot;&quot;[\s\S]*?&quot;&quot;&quot;|&#39;[^&#39;\n]*&#39;|&quot;[^&quot;\n]*&quot;)/g;
      const comments = /(#[^\n]*)/g;
      const nums = /\b(\d+\.?\d*)\b/g;
      const decorators = /(@\w+)/g;
      return code
        .replace(strings, '<span class="hl-string">$1</span>')
        .replace(comments, '<span class="hl-comment">$1</span>')
        .replace(kw, '<span class="hl-keyword">$1</span>')
        .replace(builtins, '<span class="hl-builtin">$1</span>')
        .replace(nums, '<span class="hl-number">$1</span>')
        .replace(decorators, '<span class="hl-decorator">$1</span>');
    }

    _hlJS(code) {
      const kw = /\b(const|let|var|function|return|if|else|for|while|do|switch|case|break|continue|new|this|typeof|instanceof|class|extends|super|import|export|default|from|async|await|try|catch|finally|throw|of|in|null|undefined|true|false|void|delete)\b/g;
      const strings = /(&quot;[^&quot;\n]*&quot;|&#39;[^&#39;\n]*&#39;|`[^`]*`)/g;
      const comments = /(\/\/[^\n]*|\/\*[\s\S]*?\*\/)/g;
      const nums = /\b(\d+\.?\d*)\b/g;
      const methods = /\.([a-zA-Z_$][a-zA-Z0-9_$]*)\s*\(/g;
      return code
        .replace(strings, '<span class="hl-string">$1</span>')
        .replace(comments, '<span class="hl-comment">$1</span>')
        .replace(kw, '<span class="hl-keyword">$1</span>')
        .replace(nums, '<span class="hl-number">$1</span>')
        .replace(methods, '.<span class="hl-method">$1</span>(');
    }

    _hlJSON(code) {
      const keys = /(&quot;[^&quot;]*&quot;)\s*:/g;
      const strings = /:\s*(&quot;[^&quot;]*&quot;)/g;
      const nums = /:\s*(-?\d+\.?\d*)/g;
      const bools = /:\s*(true|false|null)/g;
      return code
        .replace(keys, '<span class="hl-key">$1</span>:')
        .replace(strings, ': <span class="hl-string">$1</span>')
        .replace(nums, ': <span class="hl-number">$1</span>')
        .replace(bools, ': <span class="hl-keyword">$1</span>');
    }

    _hlSQL(code) {
      const kw = /\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|ON|GROUP BY|ORDER BY|HAVING|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|TABLE|DROP|ALTER|INDEX|DISTINCT|AS|AND|OR|NOT|IN|LIKE|IS|NULL|LIMIT|OFFSET|COUNT|SUM|AVG|MIN|MAX|UNION|ALL)\b/gi;
      const strings = /(&quot;[^&quot;]*&quot;|&#39;[^&#39;]*&#39;)/g;
      const nums = /\b(\d+)\b/g;
      return code
        .replace(strings, '<span class="hl-string">$1</span>')
        .replace(kw, '<span class="hl-keyword">$1</span>')
        .replace(nums, '<span class="hl-number">$1</span>');
    }

    _hlBash(code) {
      const commands = /\b(echo|ls|cd|grep|awk|sed|cat|rm|cp|mv|mkdir|chmod|curl|wget|pip|python|python3|node|npm|yarn|git|docker|bench|frappe)\b/g;
      const flags = /\s(-{1,2}[a-zA-Z0-9-]+)/g;
      const strings = /(&quot;[^&quot;]*&quot;|&#39;[^&#39;]*&#39;)/g;
      const comments = /(#[^\n]*)/g;
      const vars = /(\$\{?[A-Z_][A-Z0-9_]*\}?)/g;
      return code
        .replace(strings, '<span class="hl-string">$1</span>')
        .replace(comments, '<span class="hl-comment">$1</span>')
        .replace(commands, '<span class="hl-keyword">$1</span>')
        .replace(flags, ' <span class="hl-flag">$1</span>')
        .replace(vars, '<span class="hl-variable">$1</span>');
    }

    _hlHTML(code) {
      const tags = /(&lt;\/?[a-zA-Z][a-zA-Z0-9-]*(?:\s[^&gt;]*)?\/?&gt;)/g;
      const attrs = /(\s[a-zA-Z:_][a-zA-Z0-9:_-]*)=/g;
      const strings = /=(&quot;[^&quot;]*&quot;)/g;
      const comments = /(&lt;!--[\s\S]*?--&gt;)/g;
      return code
        .replace(comments, '<span class="hl-comment">$1</span>')
        .replace(tags, '<span class="hl-tag">$1</span>')
        .replace(attrs, '<span class="hl-attr">$1</span>=')
        .replace(strings, '=<span class="hl-string">$1</span>');
    }

    _hlCSS(code) {
      const selectors = /([.#]?[a-zA-Z][a-zA-Z0-9_-]*)\s*\{/g;
      const props = /([a-zA-Z-]+)\s*:/g;
      const values = /:\s*([^;{}\n]+)/g;
      const comments = /(\/\*[\s\S]*?\*\/)/g;
      return code
        .replace(comments, '<span class="hl-comment">$1</span>')
        .replace(selectors, '<span class="hl-selector">$1</span> {')
        .replace(props, '<span class="hl-prop">$1</span>:')
        .replace(values, ': <span class="hl-value">$1</span>');
    }

    _renderBlock(chunk, store) {
      const trimmed = String(chunk || "").trim();
      if (!trimmed) return "";

      // Already a sentinel token — pass through
      if (/^\x02\d+\x03$/.test(trimmed)) return trimmed;

      // Headings
      if (/^#{1,6}\s/.test(trimmed)) {
        const level = Math.min(6, Math.max(2, (trimmed.match(/^#+/) || ["##"])[0].length));
        const text = trimmed.replace(/^#{1,6}\s*/, "");
        return `<h${level} class="erp-ai-heading erp-ai-heading--${level}">${this._renderInline(text, store)}</h${level}>`;
      }

      const lines = trimmed.split("\n").map((l) => l.trimEnd());

      // Table
      if (this._looksLikeMarkdownTable(lines)) return this._renderTable(lines, store);

      // Horizontal rule
      if (/^(-{3,}|\*{3,}|_{3,})$/.test(trimmed)) return `<hr class="erp-ai-hr">`;

      const isBullet = (l) => /^\s*([-*+])\s+/.test(l);
      const isNum    = (l) => /^\s*\d+[.)]\s+/.test(l);

      if (lines.every((l) => isBullet(l) || /^\s{2,}/.test(l))) return this._renderList(lines, "ul", store);
      if (lines.every((l) => isNum(l)    || /^\s{2,}/.test(l))) return this._renderList(lines, "ol", store);

      // Blockquote
      if (lines.every((l) => /^\s*>/.test(l))) {
        const body = lines.map((l) => l.replace(/^\s*>\s?/, "")).join("\n");
        return `<blockquote class="erp-ai-quote">${this._renderInline(body, store)}</blockquote>`;
      }

      return `<p class="erp-ai-paragraph">${this._renderInline(lines.join("\n"), store)}</p>`;
    }

    _renderList(lines, tag, store) {
      const items = [];
      let current = null;
      for (const line of lines) {
        if (/^\s*([-*+]|\d+[.)])\s+/.test(line)) {
          if (current !== null) items.push(current);
          current = line.replace(/^\s*([-*+]|\d+[.)]) /, "");
        } else if (current !== null) {
          current += " " + line.trim();
        }
      }
      if (current !== null) items.push(current);
      const lis = items.map((item) => `<li class="erp-ai-list__item">${this._renderInline(item, store)}</li>`).join("");
      return `<${tag} class="erp-ai-list erp-ai-list--${tag}">${lis}</${tag}>`;
    }

    _looksLikeMarkdownTable(lines) {
      if (!Array.isArray(lines) || lines.length < 2) return false;
      const hasPipes = lines[0].includes("|") && lines[1].includes("|");
      const separator = /^\s*\|?[\s:-]+(?:\|[\s:-]+)+\|?\s*$/;
      return hasPipes && separator.test(lines[1]);
    }

    _renderTable(lines, store) {
      const rows = lines
        .map((l) => l.trim())
        .filter(Boolean)
        .map((l) => l.replace(/^\|/, "").replace(/\|$/, "").split("|").map((c) => c.trim()));
      if (rows.length < 2) return `<p class="erp-ai-paragraph">${this._renderInline(lines.join("\n"), store)}</p>`;

      const headers = rows[0];
      const bodyRows = rows.slice(2);
      const aligns = (rows[1] || []).map((c) => {
        if (/^:.*:$/.test(c)) return "center";
        if (/^:/.test(c)) return "left";
        if (/:$/.test(c)) return "right";
        return "";
      });
      const th = headers.map((cell, i) => `<th class="erp-ai-table__th"${aligns[i] ? ` style="text-align:${aligns[i]}"` : ""}>${this._renderInline(cell, store)}</th>`).join("");
      const trs = bodyRows.map((row) => {
        const tds = headers.map((_, i) => `<td class="erp-ai-table__td"${aligns[i] ? ` style="text-align:${aligns[i]}"` : ""}>${this._renderInline(row[i] || "", store)}</td>`).join("");
        return `<tr class="erp-ai-table__tr">${tds}</tr>`;
      }).join("");
      return `<div class="erp-ai-table-wrap"><table class="erp-ai-table"><thead><tr>${th}</tr></thead><tbody>${trs}</tbody></table></div>`;
    }

    _renderInline(text, store) {
      // store is the shared sentinel array from _renderRichText
      // We use the same store so sentinels survive _escapeHtml (they use \x02/\x03, not HTML chars)
      let value = String(text || "");

      // Inline images ![alt](url)
      value = value.replace(/!\[([^\]]*)\]\(((?:https?:\/\/|\/)[^\s)]+|data:image\/[^\s)]+)\)/g,
        (_, alt, url) => this._tok(store, this._renderInlineImage(url, alt)));

      // Links [label](url)
      value = value.replace(/\[([^\]]+)\]\(((?:https?:\/\/|\/)[^\s)]+)\)/g,
        (_, label, url) => this._tok(store, this._renderAnchor(url, label)));

      // Bold **text** or __text__
      value = value.replace(/\*\*(.+?)\*\*|__(.+?)__/g,
        (_, a, b) => this._tok(store, `<strong class="erp-ai-bold">${this._escapeHtml(a || b)}</strong>`));

      // Italic *text* or _text_ (avoid matching bold markers)
      value = value.replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g,
        (_, a) => this._tok(store, `<em class="erp-ai-italic">${this._escapeHtml(a)}</em>`));

      // Strikethrough ~~text~~
      value = value.replace(/~~(.+?)~~/g,
        (_, inner) => this._tok(store, `<s>${this._escapeHtml(inner)}</s>`));

      // Escape remaining HTML (sentinels \x02N\x03 pass through untouched)
      value = this._escapeHtml(value);
      value = value.replace(/\n/g, "<br>");

      // Auto-link bare URLs (after escaping so we match &amp; etc.)
      value = value.replace(/(https?:\/\/[^\s<"]+)/g, (url) => {
        const clean = this._decodeHtml(url);
        if (this._isImageUrl(clean)) return this._tok(store, this._renderInlineImage(clean, this._filenameFromUrl(clean)));
        return this._tok(store, this._renderAnchor(clean, clean));
      });

      return value; // sentinels resolved by _restoreAll at the top level
    }

    _renderAnchor(url, label) {
      const safeUrl = this._escapeHtml(url);
      const safeLabel = this._escapeHtml(label || url);
      const isInternal = String(url || "").startsWith("/");
      return `<a class="erp-ai-link" href="${safeUrl}"${isInternal ? "" : ' target="_blank" rel="noopener noreferrer"'}>${safeLabel}</a>`;
    }

    _renderInlineImage(url, alt) {
      const safeUrl = this._escapeHtml(url);
      const safeAlt = this._escapeHtml(alt || "Image");
      return `<figure class="erp-ai-figure"><a href="${safeUrl}" target="_blank" rel="noopener noreferrer"><img class="erp-ai-figure__img" src="${safeUrl}" alt="${safeAlt}" loading="lazy"></a>${alt ? `<figcaption class="erp-ai-figure__caption">${safeAlt}</figcaption>` : ""}</figure>`;
    }

    // Legacy _restoreTokens kept for any callers that still use it
    _restoreTokens(text, tokens) {
      return tokens.reduce(
        (out, html, i) => out.replaceAll(`__ERPAI_INL_${i}__`, html).replaceAll(`__ERPAI_BLOCK_${i}__`, html),
        text
      );
    }

    _escapeHtml(value) {
      return String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    _decodeHtml(value) {
      const ta = document.createElement("textarea");
      ta.innerHTML = String(value || "");
      return ta.value;
    }

    _isImageUrl(url) {
      const v = String(url || "").toLowerCase();
      return v.startsWith("data:image/") || /\.(png|jpe?g|gif|webp|svg)(\?|#|$)/.test(v);
    }

    _filenameFromUrl(url) {
      try {
        const p = new URL(url, window.location.origin);
        return p.pathname.split("/").filter(Boolean).pop() || "Image";
      } catch {
        return "Image";
      }
    }

    // ─── Copy button wiring (called after messages render) ─────────────────────
    _wireCopyButtons(container) {
      container.querySelectorAll(".erp-ai-code-block__copy").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const targetId = btn.dataset.copyTarget;
          const codeEl = targetId ? document.getElementById(targetId) : null;
          const text = codeEl ? (codeEl.innerText || codeEl.textContent || "") : "";
          const ok = await this._copyText(text);
          if (ok) {
            btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"></polyline></svg> Copied!`;
            btn.classList.add("is-copied");
            setTimeout(() => {
              btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg> Copy`;
              btn.classList.remove("is-copied");
            }, 1800);
          }
        });
      });
    }

    _parseAttachmentPackage(raw) {
      if (!raw) return { attachments: [], exports: {}, copilot: null };
      try {
        const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
        if (Array.isArray(parsed)) {
          return { attachments: parsed.filter((item) => item && item.file_url), exports: {}, copilot: null };
        }
        if (parsed && typeof parsed === "object") {
          return {
            attachments: Array.isArray(parsed.attachments) ? parsed.attachments.filter((item) => item && item.file_url) : [],
            exports: parsed.exports && typeof parsed.exports === "object" ? parsed.exports : {},
            copilot: parsed.copilot && typeof parsed.copilot === "object" ? parsed.copilot : null,
          };
        }
        return { attachments: [], exports: {}, copilot: null };
      } catch (error) {
        return { attachments: [], exports: {}, copilot: null };
      }
    }

    _parseToolEvents(raw) {
      if (!raw) return [];
      try {
        const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
        return Array.isArray(parsed) ? parsed.filter(Boolean) : [];
      } catch (error) {
        return [];
      }
    }

    _parseToolEventString(value) {
      const text = String(value || "").trim();
      if (!text) return null;
      const match = text.match(/^([a-zA-Z0-9_]+)\s*(.*)$/);
      if (!match) {
        return { label: text, countable: false };
      }

      const toolName = match[1];
      const argsText = String(match[2] || "").trim();
      const labelMap = {
        list_documents: "Fetching records",
        get_doctype_info: "Reading DocType structure",
        create_document: "Creating document",
        update_document: "Updating document",
        run_python_code: "Running bulk operation",
        generate_report: "Generating report",
        submit_document: "Submitting document",
        run_workflow: "Running workflow action",
        search_link: "Searching linked records",
      };

      let suffix = "";
      const doctypeMatch = argsText.match(/['"]doctype['"]\s*:\s*['"]([^'"]+)['"]/i);
      const reportMatch = argsText.match(/['"]report_name['"]\s*:\s*['"]([^'"]+)['"]/i);
      if (doctypeMatch?.[1]) {
        suffix = doctypeMatch[1];
      } else if (reportMatch?.[1]) {
        suffix = reportMatch[1];
      }

      const baseLabel = labelMap[toolName] || toolName.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
      return {
        label: suffix ? `${baseLabel} \u2014 ${suffix}` : baseLabel,
        countable: Object.prototype.hasOwnProperty.call(labelMap, toolName),
      };
    }

    _renderToolEventRow(events) {
      const textEvents = events
        .filter((item) => typeof item === "string" && String(item || "").trim())
        .map((item) => this._parseToolEventString(item))
        .filter(Boolean);
      const structuredEvents = events.filter((item) => item && typeof item === "object" && !Array.isArray(item));
      if (!textEvents.length && !structuredEvents.length) {
        return null;
      }
      const wrap = document.createElement("details");
      wrap.className = "erp-ai-assistant-message__activity";
      wrap.open = false;
      const commandCount = textEvents.filter((item) => item.countable).length || textEvents.length;
      wrap.innerHTML = `
        <summary class="erp-ai-assistant-message__activity-head">
          <span class="erp-ai-assistant-message__activity-status">\u2713</span>
          <span class="erp-ai-assistant-message__activity-summary">Ran ${commandCount} command${commandCount === 1 ? "" : "s"}</span>
        </summary>
        <div class="erp-ai-assistant-message__activity-steps">
          ${textEvents.map((item) => `
            <div class="erp-ai-assistant-message__activity-step">
              <span class="erp-ai-assistant-message__activity-step-icon">\u2713</span>
              <span class="erp-ai-assistant-message__activity-step-label">${this._escapeHtml(item.label)}</span>
            </div>
          `).join("")}
        </div>
        <div class="erp-ai-assistant-message__tool-blocks"></div>
      `;
      const blocks = wrap.querySelector(".erp-ai-assistant-message__tool-blocks");
      structuredEvents.forEach((item) => {
        const card = this._renderStructuredToolEvent(item);
        if (card) blocks.appendChild(card);
      });
      if (!blocks.childElementCount) {
        blocks.remove();
      }
      return wrap;
    }

    _renderStructuredToolEvent(item) {
      const type = String(item?.type || "").trim();
      if (!type) return null;
      const wrap = document.createElement("div");
      wrap.className = "erp-ai-assistant-message__tool-card";
      if (type === "missing_fields") {
        const rows = Array.isArray(item.items) ? item.items : [];
        wrap.innerHTML = `
          <div class="erp-ai-assistant-message__tool-title">Missing fields</div>
          <div class="erp-ai-assistant-message__tool-chips">
            ${rows.slice(0, 8).map((row) => `<span class="erp-ai-assistant-message__tool-chip">${this._escapeHtml(row.label || row.fieldname || "")}</span>`).join("")}
          </div>
        `;
        return wrap;
      }
      if (type === "missing_child_rows") {
        const rows = Array.isArray(item.items) ? item.items : [];
        wrap.innerHTML = `
          <div class="erp-ai-assistant-message__tool-title">Incomplete child rows</div>
          <div class="erp-ai-assistant-message__tool-options">
            ${rows.slice(0, 6).map((row) => `<div class="erp-ai-assistant-message__tool-option"><strong>${this._escapeHtml((row.table_label || "Rows") + " row " + (row.row_index || ""))}</strong><small>${this._escapeHtml((row.fields || []).map((field) => field.label || field.fieldname || "").join(", "))}</small></div>`).join("")}
          </div>
        `;
        return wrap;
      }
      if (type === "candidates") {
        const rows = Array.isArray(item.items) ? item.items : [];
        wrap.innerHTML = `
          <div class="erp-ai-assistant-message__tool-title">Choose one</div>
          <div class="erp-ai-assistant-message__tool-options">
            ${rows.slice(0, 6).map((row) => `<div class="erp-ai-assistant-message__tool-option"><strong>${this._escapeHtml(row.name || "")}</strong><small>${this._escapeHtml(row.label || "")}</small></div>`).join("")}
          </div>
        `;
        return wrap;
      }
      if (type === "document_ref") {
        const url = String(item.url || "").trim();
        const label = `${item.doctype || "Document"} ${item.name || ""}`.trim();
        wrap.innerHTML = `
          <div class="erp-ai-assistant-message__tool-title">Document</div>
          <div class="erp-ai-assistant-message__tool-link">${url ? this._renderAnchor(url, label) : this._escapeHtml(label)}</div>
        `;
        return wrap;
      }
      if (type === "planner") {
        const source = String(item.source || "").trim();
        const reason = String(item.reason || "").trim();
        wrap.innerHTML = `
          <div class="erp-ai-assistant-message__tool-title">Planner</div>
          <div class="erp-ai-assistant-message__tool-option">
            <strong>${this._escapeHtml(String(item.intent || "unknown"))}</strong>
            <small>confidence ${this._escapeHtml(String(item.confidence ?? ""))}${source ? ` • ${this._escapeHtml(source)}` : ""}${reason ? ` • ${this._escapeHtml(reason)}` : ""}</small>
          </div>
        `;
        return wrap;
      }
      return null;
    }

    _renderCopilotBlock(copilot) {
      if (!copilot || typeof copilot !== "object") return null;
      const summary = copilot.summary && typeof copilot.summary === "object" ? copilot.summary : null;
      const actions = Array.isArray(copilot.actions) ? copilot.actions.filter((item) => item && item.label && item.prompt) : [];
      const issues = Array.isArray(copilot.issues) ? copilot.issues.filter(Boolean) : [];
      const insights = Array.isArray(copilot.insights) ? copilot.insights.filter(Boolean) : [];
      const suggestions = Array.isArray(copilot.suggestions) ? copilot.suggestions.filter(Boolean) : [];
      if (!summary && !actions.length && !issues.length && !insights.length && !suggestions.length) return null;

      const section = document.createElement("section");
      section.className = "erp-ai-assistant-copilot";

      if (summary) {
        const summaryRows = Array.isArray(summary.rows) ? summary.rows.filter((r) => r && r.label) : [];
        const card = document.createElement("div");
        card.className = "erp-ai-assistant-copilot__card";
        card.innerHTML = `
          <div class="erp-ai-assistant-copilot__head">
            <strong>${this._escapeHtml(summary.title || "ERP Copilot")}</strong>
            ${summary.badge ? `<span class="erp-ai-assistant-copilot__badge">${this._escapeHtml(summary.badge)}</span>` : ""}
          </div>
          <div class="erp-ai-assistant-copilot__grid">
            ${summaryRows.map((row) => `<div class="erp-ai-assistant-copilot__kv"><span>${this._escapeHtml(row.label)}</span><strong>${this._escapeHtml(row.value || "—")}</strong></div>`).join("")}
          </div>`;
        section.appendChild(card);
      }

      if (issues.length) {
        const card = document.createElement("div");
        card.className = "erp-ai-assistant-copilot__card";
        card.innerHTML = `<div class="erp-ai-assistant-copilot__title">Key issues</div><ul class="erp-ai-assistant-copilot__list">${issues.map((item) => `<li>${this._escapeHtml(item)}</li>`).join("")}</ul>`;
        section.appendChild(card);
      }

      if (actions.length) {
        const card = document.createElement("div");
        card.className = "erp-ai-assistant-copilot__card";
        card.innerHTML = `<div class="erp-ai-assistant-copilot__title">Recommended actions</div><div class="erp-ai-assistant-copilot__actions"></div>`;
        const actionsWrap = card.querySelector(".erp-ai-assistant-copilot__actions");
        actions.forEach((item) => {
          const btn = document.createElement("button");
          btn.type = "button";
          btn.className = `erp-ai-assistant-btn${item.style === "primary" ? " erp-ai-assistant-btn--primary" : ""}`;
          btn.textContent = item.label;
          btn.addEventListener("click", () => {
            if (this.elements.textarea) {
              this.elements.textarea.value = item.prompt;
              this.sendPrompt();
            }
          });
          actionsWrap.appendChild(btn);
        });
        section.appendChild(card);
      }

      if (insights.length) {
        const card = document.createElement("div");
        card.className = "erp-ai-assistant-copilot__card";
        card.innerHTML = `<div class="erp-ai-assistant-copilot__title">Insights</div><div class="erp-ai-assistant-copilot__notes">${insights.map((item) => `<p>${this._escapeHtml(item)}</p>`).join("")}</div>`;
        section.appendChild(card);
      }

      if (suggestions.length) {
        const card = document.createElement("div");
        card.className = "erp-ai-assistant-copilot__card";
        card.innerHTML = `<div class="erp-ai-assistant-copilot__title">Try next</div><div class="erp-ai-assistant-copilot__suggestions"></div>`;
        const suggestionsWrap = card.querySelector(".erp-ai-assistant-copilot__suggestions");
        suggestions.forEach((item) => {
          const btn = document.createElement("button");
          btn.type = "button";
          btn.className = "erp-ai-assistant-btn";
          btn.textContent = item;
          btn.addEventListener("click", () => {
            if (this.elements.textarea) {
              this.elements.textarea.value = item;
              this.sendPrompt();
            }
          });
          suggestionsWrap.appendChild(btn);
        });
        section.appendChild(card);
      }

      return section;
    }

    _renderAttachmentRow(attachments) {
      const wrap = document.createElement("div");
      wrap.className = "erp-ai-assistant-message__attachments";
      attachments.forEach((item) => {
        const card = document.createElement("button");
        card.className = "erp-ai-assistant-message__attachment";
        card.type = "button";
        const name = item.filename || "download";
        card.title = `Download ${name}`;
        card.innerHTML = `
          <span class="erp-ai-assistant-message__attachment-meta">
            <strong>${this._escapeHtml(name)}</strong>
          </span>
        `;
        card.addEventListener("click", () => this._downloadAttachment(item));
        wrap.appendChild(card);
      });
      return wrap;
    }

    async _downloadAttachment(item) {
      const fileUrl = String(item?.file_url || "").trim();
      const filename = String(item?.filename || "download").trim() || "download";
      if (!fileUrl) return;

      const triggerDownload = (href) => {
        const link = document.createElement("a");
        link.href = href;
        link.download = filename;
        link.rel = "noopener";
        link.style.display = "none";
        document.body.appendChild(link);
        link.click();
        link.remove();
      };

      if (fileUrl.startsWith("data:")) {
        triggerDownload(fileUrl);
        return;
      }

      try {
        const response = await fetch(fileUrl, { credentials: "same-origin" });
        if (!response.ok) throw new Error(`Download failed: ${response.status}`);
        const blob = await response.blob();
        const objectUrl = URL.createObjectURL(blob);
        triggerDownload(objectUrl);
        window.setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
      } catch (error) {
        triggerDownload(fileUrl);
      }
    }

    ensureConversation(callback) {
      if (this.activeConversation && this.activeConversation.name) {
        callback(this.activeConversation.name);
        return;
      }

      const title = (this.elements.textarea?.value || "").trim();

      frappe.call({
        method: "erp_ai_assistant.api.chat.create_conversation",
        args: { title },
        callback: (response) => {
          const conversation = response.message;
          this.activeConversation = conversation;
          this.isDraftConversation = false;
          this.renderHistory();
          this.refreshHistory();
          callback(conversation.name);
        },
        error: (error) => {
          frappe.show_alert({ message: error.message || "Assistant setup is incomplete", indicator: "red" });
        },
      });
    }

    sendPrompt(options) {
      if (this.isGenerating) return;
      const settings = options || {};

      const prompt = String(settings.prompt ?? this.elements.textarea?.value ?? "").trim();
      const images = Array.isArray(settings.images) ? settings.images.slice() : this.pendingImages.slice();
      const shouldQueueUserMessage = settings.queueUserMessage !== false;
      if (!prompt && !images.length) return;

      // Add user message to UI immediately
      this.ensureConversation((conversationName) => {
        if (shouldQueueUserMessage) {
          this._queuePendingUserMessage(conversationName, prompt, images);
          this._renderPendingConversation(conversationName);
        }
        this.awaitingQueueAck[conversationName] = true;

        // ── Mandatory context injection ──────────────────────────────────────
        // Always resolve and send page context. If the route is empty (e.g.
        // the user opened the assistant before navigating anywhere), we still
        // send the current hash/route so the backend can log the context gap.
        const context = this.getCurrentContext();
        const routeStr = context.route || window.location.hash.replace(/^#\/?/, "") || "unknown";

        const imagePayload = images.map((item) => ({
          name: item.name,
          type: item.type,
          data_url: item.dataUrl,
        }));

        if (this.elements.textarea && settings.clearComposer !== false) {
          this.elements.textarea.value = "";
        }
        if (settings.clearImages !== false) {
          this._clearPendingImages();
        }

        this.abortRequested = false;
        this._setGeneratingState(true);
        this._showTypingIndicator(["Starting..."]);

        const request = frappe.call({
          method: "erp_ai_assistant.api.assistant.handle_prompt",
          args: {
            conversation: conversationName,
            prompt,
            doctype: context.doctype || "",
            docname: context.docname || "",
            route: routeStr,
            model: this.elements.modelSelect?.value || undefined,
            images: imagePayload.length ? JSON.stringify(imagePayload) : undefined,
            retry_last_user: settings.retryLastUser ? 1 : 0,
          },
          callback: (response) => {
            const payload = response?.message || {};
            logAssistantDebug(payload);
            if (payload.queued) {
              this.awaitingQueueAck[conversationName] = false;
              this._startProgressPolling(conversationName);
              return;
            }
            if (String(payload.reply || "").trim()) {
              this._queueOptimisticAssistantMessage(conversationName, payload.reply, {
                toolEvents: payload.tool_events,
                attachments: payload.attachments,
              });
              this._renderPendingConversation(conversationName);
            }
            this._finishPromptRun(conversationName);
          },
          error: (error) => {
            if (this.abortRequested) return;
            delete this.awaitingQueueAck[conversationName];
            this._finishPromptRun(null, { keepMessages: true });
            frappe.show_alert({ message: error.message || "Assistant request failed", indicator: "red" });
          },
          always: () => {
            this.pendingPromptRequest = null;
          },
        });

        this.pendingPromptRequest = request;
      });
    }
    stopPrompt() {
      if (!this.isGenerating) {
        return;
      }

      this.abortRequested = true;
      if (this.pendingPromptRequest) {
        this.pendingPromptRequest.abort("abort");
      }
      this.pendingPromptRequest = null;
      Object.keys(this.awaitingQueueAck).forEach((key) => {
        delete this.awaitingQueueAck[key];
      });
      this._finishPromptRun(null, { keepMessages: true });
    }

    retryLastResponse() {
      if (!this.activeConversation || !this.activeConversation.name) return;
      if (this.isGenerating) return;

      const conversationName = this.activeConversation.name;
      const cachedMessages = Array.isArray(this.conversationMessageCache[conversationName]) ? this.conversationMessageCache[conversationName] : [];

      if (!cachedMessages.length) return;

      // Ensure the last message is from the assistant
      const lastMessage = cachedMessages[cachedMessages.length - 1];
      if (lastMessage.role !== "assistant") return;

      // Ensure there's a user message right before it to retry
      const previousUserMessage = cachedMessages.length > 1 ? cachedMessages[cachedMessages.length - 2] : null;
      if (!previousUserMessage || previousUserMessage.role !== "user") return;
      this.retryPromptGuards[conversationName] = String(previousUserMessage.content || "").trim();

      // Keep the UI in a busy state while the previous answer is removed.
      this._setGeneratingState(true);

      // Call the backend to delete the last assistant message and then resend the prompt
      frappe.call({
        method: "erp_ai_assistant.api.chat.delete_last_assistant_message",
        args: { conversation: conversationName },
        callback: (response) => {
          if (response.message && response.message.ok) {
            // Remove the last message from the local cache immediately to update UI
            this.conversationMessageCache[conversationName].pop();
            this.renderMessages(this._getConversationMessages(conversationName));

            // Resend the prompt using the previous user message's content
            if (this.elements.textarea) {
                this._setGeneratingState(false);
                this.sendPrompt({
                  prompt: previousUserMessage.content || "",
                  queueUserMessage: false,
                  clearComposer: true,
                  clearImages: false,
                  images: [],
                  retryLastUser: true,
                });
            }
          } else {
             this._setGeneratingState(false);
             frappe.show_alert({ message: "Unable to retry message.", indicator: "orange" });
          }
        },
        error: (error) => {
            this._setGeneratingState(false);
            frappe.show_alert({ message: error.message || "Unable to retry message.", indicator: "red" });
        }
      });
    }

    _addUserMessageToUI(prompt, images) {
      if (!this.elements.messages) return;

      const emptyState = this.elements.messages.querySelector(".erp-ai-assistant-messages__empty");
      if (emptyState) {
        emptyState.remove();
      }

      const userBubble = document.createElement("div");
      userBubble.className = "erp-ai-assistant-message is-user";

      const now = new Date();
      const timeStr = frappe.datetime ? frappe.datetime.str_to_user(now) : now.toLocaleTimeString();
      const text = String(prompt || "").trim();
      const imageCount = Array.isArray(images) ? images.length : 0;
      const imageNote = imageCount ? `\n\n[Attached ${imageCount} image${imageCount === 1 ? "" : "s"}]` : "";
      const body = text ? `${text}${imageNote}` : imageNote.trim();

      userBubble.innerHTML = `
        <div class="erp-ai-assistant-message__meta">
          <span class="erp-ai-assistant-message__role erp-ai-assistant-message__role--user" style="color: var(--ai-primary)">You</span>
        </div>
        <div class="erp-ai-assistant-message__body">${this._formatMessageContent(body)}</div>
      `;
      if (imageCount) {
        userBubble.appendChild(
          this._renderAttachmentRow(
            images.map((item, index) => ({
              label: "Image",
              filename: item.name || `image-${index + 1}`,
              file_type: String(item.type || "image").split("/").pop(),
              file_url: item.dataUrl,
            }))
          )
        );
      }

      this.elements.messages.appendChild(userBubble);
      this.elements.messages.scrollTop = this.elements.messages.scrollHeight;
    }

    _buildPendingUserMessage(prompt, images) {
      const text = String(prompt || "").trim();
      const imageCount = Array.isArray(images) ? images.length : 0;
      const imageNote = imageCount ? `\n\n[Attached ${imageCount} image${imageCount === 1 ? "" : "s"}]` : "";
      const body = text ? `${text}${imageNote}` : imageNote.trim();
      const attachments = imageCount
        ? {
          attachments: images.map((item, index) => ({
            label: "Image",
            filename: item.name || `image-${index + 1}`,
            file_type: String(item.type || "image").split("/").pop(),
            file_url: item.dataUrl,
          })),
        }
        : null;

      return {
        role: "user",
        content: body,
        creation: new Date().toISOString(),
        attachments_json: attachments ? JSON.stringify(attachments) : null,
      };
    }

    _queuePendingUserMessage(conversationName, prompt, images) {
      if (!conversationName) return;
      const pendingMessages = Array.isArray(this.pendingConversationMessages[conversationName])
        ? this.pendingConversationMessages[conversationName].slice()
        : [];
      pendingMessages.push(this._buildPendingUserMessage(prompt, images));
      this.pendingConversationMessages[conversationName] = pendingMessages;
    }

    _renderPendingConversation(conversationName) {
      if (!conversationName) return;
      const messages = this._getConversationMessages(conversationName);
      if (!messages.length) return;
      this.activeConversation = this.activeConversation && this.activeConversation.name === conversationName
        ? this.activeConversation
        : { name: conversationName };
      this.isDraftConversation = false;
      this.renderMessages(messages);
      this.renderHistory();
    }

    _getConversationMessages(conversationName) {
      const serverMessages = Array.isArray(this.conversationMessageCache[conversationName])
        ? this.conversationMessageCache[conversationName]
        : [];
      const pendingMessages = Array.isArray(this.pendingConversationMessages[conversationName])
        ? this.pendingConversationMessages[conversationName]
        : [];
      const optimisticAssistantMessages = Array.isArray(this.optimisticAssistantMessages[conversationName])
        ? this.optimisticAssistantMessages[conversationName]
        : [];

      if (!pendingMessages.length && !optimisticAssistantMessages.length) {
        return serverMessages;
      }

      return serverMessages.concat(pendingMessages, optimisticAssistantMessages);
    }

    _queueOptimisticAssistantMessage(conversationName, replyText, options) {
      const content = String(replyText || "").trim();
      if (!conversationName || !content) return;
      const settings = options || {};
      const existing = Array.isArray(this.optimisticAssistantMessages[conversationName])
        ? this.optimisticAssistantMessages[conversationName].slice()
        : [];
      const last = existing.length ? existing[existing.length - 1] : null;
      if (last && String(last.content || "").trim() === content) {
        return;
      }
      existing.push({
        role: "assistant",
        content,
        creation: new Date().toISOString(),
        tool_events: JSON.stringify(Array.isArray(settings.toolEvents) ? settings.toolEvents : []),
        attachments_json: settings.attachments ? JSON.stringify(settings.attachments) : null,
      });
      this.optimisticAssistantMessages[conversationName] = existing;
    }

    _showTypingIndicator(steps) {
      if (!this.elements.messages) return;

      this._hideTypingIndicator(); // Remove any existing indicator

      const indicator = document.createElement("div");
      indicator.className = "erp-ai-assistant-message is-assistant erp-ai-assistant-message--typing";
      indicator.id = "erp-ai-assistant-typing";
      indicator.innerHTML = `
        <div class="erp-ai-assistant-message__meta">
          <span class="erp-ai-assistant-message__role erp-ai-assistant-message__role--assistant">Assistant</span>
        </div>
        <div class="erp-ai-assistant-progress">
          <div class="erp-ai-assistant-progress__partial" style="display:none; font-size:15px; padding-bottom:12px; white-space:pre-wrap; opacity:0.8;"></div>
          <details class="erp-ai-assistant-progress__log" open>
            <summary class="erp-ai-assistant-message__activity-head">
               <span class="erp-ai-assistant-progress__spinner" aria-hidden="true">⟳</span>
               <span class="erp-ai-assistant-progress__summary erp-ai-thinking-label" style="animation: cd-pulse 1.8s ease-in-out infinite;">Working...</span>
            </summary>
            <div class="erp-ai-assistant-progress__steps" style="padding: 0 14px 12px;"></div>
          </details>
        </div>
      `;

      this.elements.messages.appendChild(indicator);
      this._updateTypingSteps(Array.isArray(steps) ? steps : ["Preparing response"], "", { done: false });
      this.elements.messages.scrollTop = this.elements.messages.scrollHeight;
    }

    _tryBeginCompletionFetch(conversationName) {
      if (!conversationName) return false;
      if (this.completionFetchLocks[conversationName]) return false;
      this.completionFetchLocks[conversationName] = true;
      return true;
    }

    _clearCompletionFetchLock(conversationName) {
      if (!conversationName) return;
      delete this.completionFetchLocks[conversationName];
    }

    _mapProgressStepLabel(step) {
      const text = String(step || "").trim();
      const lowered = text.toLowerCase();
      if (!text) return "Working...";
      if (lowered === "preparing request" || lowered === "preparing response") return "Starting...";
      if (lowered === "thinking") return "Thinking...";
      if (lowered === "verifying erp evidence") return "Verifying results...";
      if (lowered === "response ready") return "Done";
      if (lowered.startsWith("tool: get doctype info")) return "Read document structure";
      if (lowered.startsWith("tool: list documents")) return "Fetched records from ERP";
      if (lowered.startsWith("tool: create document")) return "Created document in ERP";
      if (lowered.startsWith("tool: run python code")) return "Ran bulk operation";
      if (lowered.startsWith("tool: generate report")) return "Generated report";
      if (lowered.startsWith("tool: update document")) return "Updated document";
      if (lowered.startsWith("tool failed:")) return `Failed: ${text.replace(/^tool failed:\s*/i, "")}`;
      if (lowered.startsWith("tool:")) return text.replace(/^tool:\s*/i, "");
      return text;
    }

    _summarizeProgressSteps(steps, done) {
      const items = Array.isArray(steps) ? steps.filter(Boolean) : [];
      const commandCount = items.filter((item) => /^tool:/i.test(String(item || "").trim())).length;
      if (commandCount) {
        return `${done ? "Ran" : "Running"} ${commandCount} command${commandCount === 1 ? "" : "s"}${done ? "" : "..."}`;
      }
      const stepCount = items.length || 1;
      return `${done ? "Completed" : "Running"} ${stepCount} step${stepCount === 1 ? "" : "s"}${done ? "" : "..."}`;
    }

    _updateTypingSteps(steps, partialText, options) {
      const settings = options || {};
      const done = !!settings.done;
      const indicator = document.getElementById("erp-ai-assistant-typing");
      if (!indicator) return;
      const wrap = indicator.querySelector(".erp-ai-assistant-progress__steps");
      const partial = indicator.querySelector(".erp-ai-assistant-progress__partial");
      const summary = indicator.querySelector(".erp-ai-assistant-progress__summary");
      const spinner = indicator.querySelector(".erp-ai-assistant-progress__spinner");
      if (!wrap) return;

      const normalized = Array.isArray(steps)
        ? steps.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
      const finalSteps = normalized.length ? normalized : ["Preparing response"];
      indicator.classList.toggle("is-done", done);

      if (summary) {
        summary.textContent = this._summarizeProgressSteps(finalSteps, done);
        if (done) {
          summary.style.animation = "none";
          summary.style.opacity = "1";
        }
      }

      if (spinner) {
        spinner.style.display = done ? "none" : "inline-block";
      }

      wrap.innerHTML = "";
      finalSteps.slice(-6).forEach((step, index, items) => {
        const row = document.createElement("div");
        row.className = "erp-ai-assistant-progress__step";
        const icon = document.createElement("span");
        icon.className = "erp-ai-assistant-progress__step-icon";
        icon.textContent = done || index < items.length - 1 ? "\u2713" : "\u27f3";
        icon.textContent = done || index < items.length - 1 ? "✓" : "⟳";
        icon.textContent = done || index < items.length - 1 ? "\u2713" : "\u27f3";
        const label = document.createElement("span");
        label.className = "erp-ai-assistant-progress__step-label";
        label.textContent = this._mapProgressStepLabel(step);
        row.appendChild(icon);
        row.appendChild(label);
        wrap.appendChild(row);
      });
      if (partial) {
        partial.textContent = String(partialText || "").trim();
        partial.style.display = partial.textContent ? "block" : "none";
      }
    }

    _startProgressPolling(conversationName) {
      this._stopProgressPolling();
      this._showTypingIndicator(["Preparing response"]);

      if (!conversationName) return;

      // ── Realtime listener (Socket.IO) ─────────────────────────────────────
      // The backend calls frappe.publish_realtime("erp_ai_progress", ...) on
      // every _progress_update call.  If Socket.IO is connected we handle the
      // event instantly, making the UI feel streaming.  The poll loop below
      // runs in parallel as a reliable fallback (e.g. WS disconnect, mobile).
      const realtimeHandler = (data) => {
        if (!data || String(data.conversation || "").trim() !== conversationName) return;
        const steps = Array.isArray(data.steps) ? data.steps : [];
        const stage = String(data.stage || "").trim().toLowerCase();
        if ((stage === "idle" || !stage) && this.awaitingQueueAck[conversationName]) return;
        this._updateTypingSteps(steps, data.partial_text || "", { done: !!data.done });
        if (data.done) {
          if (!this._tryBeginCompletionFetch(conversationName)) {
            return;
          }
          this._stopProgressPolling({ preserveIndicator: true });
          if (data.error) {
            this._clearCompletionFetchLock(conversationName);
            this._finishPromptRun(null, { keepMessages: true });
            frappe.show_alert({ message: data.error || "Assistant request failed", indicator: "red" });
            return;
          }
          frappe.call({
            method: "erp_ai_assistant.api.ai.get_prompt_result",
            args: { conversation: conversationName },
            callback: (resultResponse) => {
              const result = resultResponse?.message || {};
              if (result.done && String(result.reply || "").trim()) {
                this._queueOptimisticAssistantMessage(conversationName, result.reply, {
                  toolEvents: result.tool_events,
                  attachments: result.attachments,
                });
                this._renderPendingConversation(conversationName);
              }
              this._finishPromptRun(conversationName);
            },
            error: () => {
              this._clearCompletionFetchLock(conversationName);
              this._finishPromptRun(conversationName);
            },
          });
        }
      };

      // Store handler reference so _stopProgressPolling can remove it.
      this._realtimeProgressHandler = realtimeHandler;
      this._realtimeProgressConversation = conversationName;
      try {
        frappe.realtime.on("erp_ai_progress", realtimeHandler);
      } catch (e) {
        // frappe.realtime may not exist in all deployments — polling covers it.
      }

      // ── Polling fallback (800 ms) ─────────────────────────────────────────
      const pollProgress = () => {
        if (this.progressPollPending) return;
        this.progressPollPending = true;

        frappe.call({
          method: "erp_ai_assistant.api.ai.get_prompt_progress",
          args: { conversation: conversationName },
          callback: (response) => {
            const progress = response?.message || {};
            const steps = Array.isArray(progress.steps) ? progress.steps : [];
            const stage = String(progress.stage || "").trim().toLowerCase();
            if ((stage === "idle" || !stage) && this.awaitingQueueAck[conversationName]) {
              return;
            }
            this._updateTypingSteps(steps, progress.partial_text || "", { done: !!progress.done });
            if (progress.done) {
              if (!this._tryBeginCompletionFetch(conversationName)) {
                return;
              }
              this._stopProgressPolling({ preserveIndicator: true });
              if (progress.error) {
                this._clearCompletionFetchLock(conversationName);
                this._finishPromptRun(null, { keepMessages: true });
                frappe.show_alert({ message: progress.error || "Assistant request failed", indicator: "red" });
                return;
              }
              frappe.call({
                method: "erp_ai_assistant.api.ai.get_prompt_result",
                args: { conversation: conversationName },
                callback: (resultResponse) => {
                  const result = resultResponse?.message || {};
                  logAssistantDebug(result);
                  if (result.done && String(result.reply || "").trim()) {
                    this._queueOptimisticAssistantMessage(conversationName, result.reply, {
                      toolEvents: result.tool_events,
                      attachments: result.attachments,
                    });
                    this._renderPendingConversation(conversationName);
                  }
                  this._finishPromptRun(conversationName);
                },
                error: () => {
                  this._clearCompletionFetchLock(conversationName);
                  this._finishPromptRun(conversationName);
                },
              });
            }
          },
          always: () => {
            this.progressPollPending = false;
          },
        });
      };

      pollProgress();
      this.progressPollTimer = window.setInterval(() => {
        pollProgress();
      }, 800);
    }


    _stopProgressPolling(options) {
      const settings = options || {};
      if (this.progressPollTimer) {
        clearInterval(this.progressPollTimer);
        this.progressPollTimer = null;
      }
      this.progressPollPending = false;
      // Clean up the realtime listener registered in _startProgressPolling.
      if (this._realtimeProgressHandler) {
        try {
          frappe.realtime.off("erp_ai_progress", this._realtimeProgressHandler);
        } catch (e) {
          // ignore — realtime may not be available
        }
        this._realtimeProgressHandler = null;
        this._realtimeProgressConversation = null;
      }
      if (!settings.preserveIndicator) {
        this._hideTypingIndicator();
      }
    }


    _hideTypingIndicator() {
      const indicator = document.getElementById("erp-ai-assistant-typing");
      if (indicator) {
        indicator.remove();
      }
    }

    _clearCompletionReloadTimer(conversationName) {
      const timer = this.completionReloadTimers[conversationName];
      if (timer) {
        clearTimeout(timer);
        delete this.completionReloadTimers[conversationName];
      }
    }

    _loadConversationAfterCompletion(conversationName, attempt) {
      if (!conversationName) return;
      const tryCount = Number(attempt || 0);
      this._clearCompletionReloadTimer(conversationName);
      this.loadConversation(conversationName, {
        silent: true,
        waitForOptimisticConfirmation: true,
        onLoaded: (_payload, serverMessages) => {
          const rows = Array.isArray(serverMessages) ? serverMessages : [];
          const optimisticMessages = Array.isArray(this.optimisticAssistantMessages[conversationName])
            ? this.optimisticAssistantMessages[conversationName]
            : [];
          const expectedReply = optimisticMessages.length
            ? String(optimisticMessages[optimisticMessages.length - 1]?.content || "").trim()
            : "";
          const hasExpectedAssistantReply = expectedReply
            ? rows.some((row) => row && row.role === "assistant" && String(row.content || "").trim() === expectedReply)
            : !!(rows.length && rows[rows.length - 1]?.role === "assistant");
          if (hasExpectedAssistantReply || !expectedReply) {
            this.renderMessages(this._getConversationMessages(conversationName));
            this.renderHistory();
          }
          if (!hasExpectedAssistantReply && tryCount < 8) {
            this.completionReloadTimers[conversationName] = window.setTimeout(() => {
              this._loadConversationAfterCompletion(conversationName, tryCount + 1);
            }, 1200);
            return;
          }
          this._clearCompletionReloadTimer(conversationName);
        },
      });
    }

    _finishPromptRun(conversationName, options) {
      const settings = options || {};
      this._stopProgressPolling();
      this._setGeneratingState(false);
      this.updateContextHint();
      this.abortRequested = false;
      if (conversationName) {
        this._clearCompletionFetchLock(conversationName);
        delete this.awaitingQueueAck[conversationName];
        delete this.pendingConversationMessages[conversationName];
        this.refreshHistory();
        this._loadConversationAfterCompletion(conversationName, 0);
        return;
      }
      Object.keys(this.completionFetchLocks).forEach((key) => {
        delete this.completionFetchLocks[key];
      });
      if (!settings.keepMessages) {
        this.refreshHistory();
      }
    }

    loadModelOptions() {
      frappe.call({
        method: "erp_ai_assistant.api.ai.get_available_models",
        callback: (response) => {
          this._setModelOptions(response?.message || {});
        },
        error: () => {
          this._setModelOptions({});
        },
      });
    }

    _setModelOptions(payload) {
      const select = this.elements.modelSelect;
      if (!select) return;

      const models = Array.isArray(payload.models) ? payload.models.filter(Boolean) : [];
      const defaultModel = payload.default_model || models[0] || "";
      const persisted = localStorage.getItem(this.modelStorageKey);
      const selected = persisted && models.includes(persisted) ? persisted : defaultModel;

      select.innerHTML = "";
      models.forEach((model) => {
        const option = document.createElement("option");
        option.value = model;
        option.textContent = model;
        if (model === selected) {
          option.selected = true;
        }
        select.appendChild(option);
      });

      if (!models.length) {
        const option = document.createElement("option");
        option.value = "";
        option.textContent = "Default model";
        option.selected = true;
        select.appendChild(option);
      }
      this._persistSelectedModel();
    }

    _persistSelectedModel() {
      const selected = this.elements.modelSelect?.value;
      if (!selected) return;
      localStorage.setItem(this.modelStorageKey, selected);
    }

    async _ingestImageFiles(fileList) {
      const files = Array.from(fileList || []);
      for (const file of files) {
        if (!file || !String(file.type || "").startsWith("image/")) continue;
        if (this.pendingImages.length >= 4) {
          frappe.show_alert({ message: "Maximum 4 images per prompt", indicator: "orange" });
          break;
        }
        if (file.size > 4 * 1024 * 1024) {
          frappe.show_alert({ message: `${file.name || "Image"} exceeds 4MB`, indicator: "orange" });
          continue;
        }
        try {
          const dataUrl = await this._readFileAsDataUrl(file);
          this.pendingImages.push({
            name: file.name || "image",
            type: file.type || "image/png",
            size: file.size || 0,
            dataUrl,
          });
        } catch (error) {
          frappe.show_alert({ message: "Unable to read image", indicator: "red" });
        }
      }
      this._renderImagePreview();
    }

    _readFileAsDataUrl(file) {
      return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(String(reader.result || ""));
        reader.onerror = () => reject(new Error("read_failed"));
        reader.readAsDataURL(file);
      });
    }

    _renderImagePreview() {
      const wrap = this.elements.imagePreview;
      if (!wrap) return;
      wrap.innerHTML = "";
      if (!this.pendingImages.length) return;

      this.pendingImages.forEach((item, index) => {
        const chip = document.createElement("button");
        chip.type = "button";
        chip.className = "erp-ai-assistant-image-chip";
        chip.title = item.name;
        chip.innerHTML = `
          <img src="${item.dataUrl}" alt="${item.name}" />
          <span>${item.name}</span>
          <strong>x</strong>
        `;
        chip.addEventListener("click", () => {
          this.pendingImages.splice(index, 1);
          this._renderImagePreview();
        });
        wrap.appendChild(chip);
      });
    }

    _clearPendingImages() {
      this.pendingImages = [];
      this._renderImagePreview();
    }

    _setGeneratingState(isGenerating) {
      this.isGenerating = !!isGenerating;

      if (this.elements.sendButton) {
        this.elements.sendButton.hidden = this.isGenerating;
        this.elements.sendButton.disabled = this.isGenerating;
      }

      if (this.elements.stopButton) {
        this.elements.stopButton.hidden = !this.isGenerating;
        this.elements.stopButton.disabled = !this.isGenerating;
      }
    }

    async _copyText(text) {
      const value = String(text || "");
      if (!value) return false;

      try {
        if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
          await navigator.clipboard.writeText(value);
          return true;
        }
      } catch (error) {
        // Fall through to legacy copy fallback.
      }

      try {
        const textarea = document.createElement("textarea");
        textarea.value = value;
        textarea.setAttribute("readonly", "");
        textarea.style.position = "fixed";
        textarea.style.top = "-9999px";
        textarea.style.left = "-9999px";
        document.body.appendChild(textarea);
        textarea.focus();
        textarea.select();
        const ok = document.execCommand("copy");
        document.body.removeChild(textarea);
        return !!ok;
      } catch (error) {
        return false;
      }
    }

    togglePin(name) {
      frappe.call({
        method: "erp_ai_assistant.api.chat.toggle_pin",
        args: { name },
        callback: () => this.refreshHistory(),
        error: (error) => {
          frappe.show_alert({ message: error.message || "Unable to update pin", indicator: "red" });
        },
      });
    }

    closeConversationMenu() {
      const menu = this.elements.conversationMenu;
      if (!menu) return;
      if (typeof menu._dismiss === "function") {
        document.removeEventListener("mousedown", menu._dismiss, true);
      }
      menu.remove();
      this.elements.conversationMenu = null;
    }

    toggleConversationMenu(row, anchor) {
      if (!row || !anchor || !this.elements.drawer) return;
      const existing = this.elements.conversationMenu;
      if (existing?.dataset?.conversation === row.name) {
        this.closeConversationMenu();
        return;
      }

      this.closeConversationMenu();
      const menu = document.createElement("div");
      menu.className = "erp-ai-assistant-history-menu";
      menu.dataset.conversation = row.name;
      menu.innerHTML = `
        <button type="button" data-menu-action="rename">Rename</button>
        <button type="button" data-menu-action="pin">${row.is_pinned ? "Unpin chat" : "Pin chat"}</button>
        <button type="button" data-menu-action="archive" disabled>Archive</button>
        <button type="button" data-menu-action="delete" class="is-danger">Delete</button>
      `;
      this.elements.drawer.appendChild(menu);

      const drawerRect = this.elements.drawer.getBoundingClientRect();
      const anchorRect = anchor.getBoundingClientRect();
      const menuWidth = 220;
      const left = Math.max(12, Math.min(anchorRect.right - drawerRect.left - menuWidth + 12, drawerRect.width - menuWidth - 12));
      const top = Math.max(12, Math.min(anchorRect.bottom - drawerRect.top + 8, drawerRect.height - 180));
      menu.style.left = `${left}px`;
      menu.style.top = `${top}px`;

      menu.querySelector('[data-menu-action="rename"]')?.addEventListener("click", () => {
        this.closeConversationMenu();
        const newTitle = window.prompt("Rename conversation:", row.title || "");
        if (!newTitle || newTitle === row.title) return;
        frappe.call({
          method: "erp_ai_assistant.api.chat.rename_conversation",
          args: { name: row.name, title: newTitle },
          callback: () => this.refreshHistory(),
        });
      });

      menu.querySelector('[data-menu-action="pin"]')?.addEventListener("click", () => {
        this.closeConversationMenu();
        this.togglePin(row.name);
      });

      menu.querySelector('[data-menu-action="delete"]')?.addEventListener("click", () => {
        this.closeConversationMenu();
        this.deleteConversation(row.name);
      });

      const dismiss = (event) => {
        if (menu.contains(event.target) || anchor.contains(event.target)) return;
        this.closeConversationMenu();
      };
      menu._dismiss = dismiss;
      window.setTimeout(() => document.addEventListener("mousedown", dismiss, true), 0);
      this.elements.conversationMenu = menu;
    }

    renderHistory() {
      if (!this.elements.history) return;
      this.closeConversationMenu();

      const query = (this.elements.search?.value || "").toLowerCase();
      this.elements.history.innerHTML = "";

      const rows = this.conversations.filter(
        (row) => !query || (row.title || "").toLowerCase().includes(query)
      );

      if (!rows.length) {
        this.elements.history.innerHTML = `<div class="erp-ai-assistant-history__empty">No conversations</div>`;
        return;
      }

      const dateGroup = (dateStr) => {
        if (!dateStr) return "Older";
        const d = new Date(dateStr), now = new Date();
        const diffDays = Math.floor((now - d) / 86400000);
        if (diffDays === 0) return "Today";
        if (diffDays === 1) return "Yesterday";
        if (diffDays <= 7) return "Previous 7 days";
        return "Older";
      };

      let lastGroup = "";
      rows.forEach((row) => {
        const group = dateGroup(row.modified);
        if (group !== lastGroup) {
          lastGroup = group;
          const label = document.createElement("div");
          label.className = "erp-ai-assistant-history__group-label";
          label.textContent = group;
          this.elements.history.appendChild(label);
        }

        const item = document.createElement("div");
        item.className = "erp-ai-assistant-history__item";
        if (this.activeConversation && this.activeConversation.name === row.name) {
          item.classList.add("is-active");
        }

        item.innerHTML = `
          <button class="erp-ai-assistant-history__content" type="button">
            <div class="erp-ai-assistant-history__title">${frappe.utils.escape_html(row.title || "New chat")}</div>
          </button>
          <div class="erp-ai-assistant-history__actions">
            <button class="erp-ai-assistant-history__icon" type="button" data-action="menu" title="Conversation options">⋯</button>
          </div>
        `;

        item.querySelector(".erp-ai-assistant-history__content")?.addEventListener("click", () => this.loadConversation(row.name));
        item.querySelector('[data-action="menu"]')?.addEventListener("click", (event) => {
          event.stopPropagation();
          this.toggleConversationMenu(row, event.currentTarget);
        });
        this.elements.history.appendChild(item);
      });
    }

    deleteConversation(name) {
      frappe.confirm("Delete this conversation?", () => {
        frappe.call({
          method: "erp_ai_assistant.api.chat.delete_conversation",
          args: { name },
          callback: () => {
            if (this.activeConversation && this.activeConversation.name === name) {
              this.activeConversation = null;
              this.isDraftConversation = false;
              this.renderMessages([]);
            }
            this.refreshHistory();
          },
          error: (error) => {
            frappe.show_alert({ message: error.message || "Unable to delete conversation", indicator: "red" });
          },
        });
      });
    }
  }

  frappe.provide("erp_ai_assistant");
  erp_ai_assistant.drawer = erp_ai_assistant.drawer || new ERPAssistantBubble();

  const bootAssistantDrawer = () => {
    erp_ai_assistant.drawer.boot();
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootAssistantDrawer, { once: true });
  } else {
    bootAssistantDrawer();
  }

  if (typeof frappe.after_ajax === "function") {
    frappe.after_ajax(() => {
      bootAssistantDrawer();
    });
  }
})();
