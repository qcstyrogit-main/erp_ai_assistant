(function () {
  window.__ERP_AI_ASSISTANT_BUILD__ = "2026-03-09-context-guard-v2";

  /**
   * @class ERPAssistantBubble
   * @description Main class for ERP AI Assistant UI component
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

      /** @type {Object|null} */
      this.latestExportBundle = null;
    }

    boot() {
      if (frappe.session.user === "Guest") return;
      if (document.getElementById("erp-ai-assistant-bubble")) return;
      this.render();
      this.bindGlobalEvents();
      this.refreshHistory();
      this.updateContextHint();
    }

    render() {
      const bubble = document.createElement("button");
      bubble.id = "erp-ai-assistant-bubble";
      bubble.className = "erp-ai-assistant-bubble";
      bubble.innerHTML = `
        <span class="erp-ai-assistant-bubble__icon">AI</span>
        <span class="erp-ai-assistant-bubble__label">ERP Assistant</span>
      `;
      bubble.setAttribute("aria-label", "Open ERP AI Assistant");
      bubble.setAttribute("title", "ERP Assistant\nClick to open\nCtrl+Enter: Quick open");

      const drawer = document.createElement("div");
      drawer.id = "erp-ai-assistant-drawer";
      drawer.className = "erp-ai-assistant-drawer";
      drawer.setAttribute("role", "dialog");
      drawer.setAttribute("aria-label", "ERP AI Assistant Dialog");
      drawer.innerHTML = `
        <div class="erp-ai-assistant-drawer__sidebar">
          <div class="erp-ai-assistant-drawer__sidebar-head">
            <div>
              <div class="erp-ai-assistant-drawer__eyebrow">ERP AI Assistant</div>
              <h3>Conversations</h3>
            </div>
            <div class="erp-ai-assistant-drawer__sidebar-actions">
              <button class="erp-ai-assistant-btn" data-action="export-history" title="Export all conversations (JSON)">Export</button>
              <button class="erp-ai-assistant-btn" data-action="import-history" title="Import conversations (JSON)">Import</button>
              <input type="file" class="erp-ai-assistant-import-input" accept=".json" aria-label="Import file" style="display: none;" />
              <button class="erp-ai-assistant-btn erp-ai-assistant-btn--primary" data-action="new-chat" title="Start new conversation (Ctrl+Shift+N)">New Chat</button>
            </div>
          </div>
          <input class="erp-ai-assistant-search" placeholder="Search conversations..." aria-label="Search conversations" />
          <div class="erp-ai-assistant-history"></div>
        </div>
        <div class="erp-ai-assistant-drawer__main">
          <div class="erp-ai-assistant-drawer__header">
            <div>
              <div class="erp-ai-assistant-drawer__eyebrow">Current Context</div>
              <div class="erp-ai-assistant-context" title="The current page context being used for responses"></div>
            </div>
            <div class="erp-ai-assistant-export-panel">
              <button class="erp-ai-assistant-btn" data-action="export-excel" title="Export last response to Excel" disabled>Excel</button>
              <button class="erp-ai-assistant-btn" data-action="export-pdf" title="Export last response to PDF" disabled>PDF</button>
              <button class="erp-ai-assistant-btn" data-action="export-word" title="Export last response to Word" disabled>Word</button>
            </div>
            <button class="erp-ai-assistant-btn" data-action="close" title="Close assistant (Esc)">Close</button>
          </div>
          <div class="erp-ai-assistant-messages"></div>
          <div class="erp-ai-assistant-composer">
            <textarea rows="3" placeholder="Ask about this record, generate a report, or update data..." aria-label="Message input"></textarea>
            <div class="erp-ai-assistant-composer__actions">
              <span class="erp-ai-assistant-composer__hint"><kbd>Enter</kbd> to send <kbd>Shift+Enter</kbd> for newline</span>
              <button class="erp-ai-assistant-btn erp-ai-assistant-btn--primary" data-action="send" title="Send message">Send</button>
            </div>
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
        exportExcel: drawer.querySelector('[data-action="export-excel"]'),
        exportPdf: drawer.querySelector('[data-action="export-pdf"]'),
        exportWord: drawer.querySelector('[data-action="export-word"]'),
        exportHistory: drawer.querySelector('[data-action="export-history"]'),
        importHistory: drawer.querySelector('[data-action="import-history"]'),
        importInput: drawer.querySelector(".erp-ai-assistant-import-input"),
      };

      bubble.addEventListener("click", () => this.toggleDrawer());
      drawer.querySelector('[data-action="close"]')?.addEventListener("click", () => this.toggleDrawer(false));
      drawer.querySelector('[data-action="send"]')?.addEventListener("click", () => this.sendPrompt());
      drawer.querySelector('[data-action="new-chat"]')?.addEventListener("click", () => this.startDraftConversation());
      this.elements.exportExcel?.addEventListener("click", () => this.exportData("excel"));
      this.elements.exportPdf?.addEventListener("click", () => this.exportData("pdf"));
      this.elements.exportWord?.addEventListener("click", () => this.exportData("word"));
      this.elements.exportHistory?.addEventListener("click", () => this.exportHistory());
      this.elements.importHistory?.addEventListener("click", () => this.elements.importInput?.click());
      this.elements.importInput?.addEventListener("change", (e) => this.importHistory(e));
      this.elements.search?.addEventListener("input", () => this.renderHistory());
      this.elements.textarea?.addEventListener("keydown", (event) => {
        if (event.key === "Enter" && !event.shiftKey) {
          event.preventDefault();
          this.sendPrompt();
        }
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
            case "e":
              if (event.shiftKey && this.latestExportBundle) {
                event.preventDefault();
                this.exportData("excel");
              }
              break;
          }
        }
        if (event.key === "Escape" && this.drawerOpen) {
          event.preventDefault();
          this.toggleDrawer(false);
        }
      });
    }

    bindGlobalEvents() {
      window.addEventListener("hashchange", () => this.updateContextHint());
      $(document).on("page-change", () => this.updateContextHint());
    }

    toggleDrawer(forceState) {
      this.drawerOpen = typeof forceState === "boolean" ? forceState : !this.drawerOpen;
      this.elements.drawer?.classList.toggle("is-open", this.drawerOpen);
      this.elements.bubble?.classList.toggle("is-hidden", this.drawerOpen);
      if (this.elements.bubble) {
        this.elements.bubble.style.display = this.drawerOpen ? "none" : "inline-flex";
      }

      if (this.drawerOpen) {
        this.updateContextHint();
        this.refreshHistory();
        this.elements.textarea?.focus();
      }
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

      const form = window.cur_frm || (typeof cur_frm !== "undefined" ? cur_frm : null);

      if (form && form.doc) {
        return {
          doctype: form.doctype || null,
          docname: form.docname || null,
          route: route.join("/"),
        };
      }

      const [view, doctype, docname] = route;
      const isForm = view === "Form";

      return {
        doctype: isForm ? doctype || null : null,
        docname: isForm ? docname || null : null,
        route: route.join("/"),
      };
    }

    updateContextHint() {
      let context = { doctype: null, docname: null, route: "" };
      try {
        const resolved = this.getCurrentContext();
        if (resolved && typeof resolved === "object") {
          context = resolved;
        }
      } catch (error) {
        // Keep UI alive even if route/context cannot be resolved on a custom Desk page.
        console.warn("ERP Assistant: context resolution failed", error);
      }

      const text = context.doctype && context.docname
        ? `${context.doctype} / ${context.docname}`
        : "General ERP workspace";

      if (this.elements.context) {
        this.elements.context.textContent = text;
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

      rows.forEach((row) => {
        const item = document.createElement("div");
        item.className = "erp-ai-assistant-history__item";

        if (this.activeConversation && this.activeConversation.name === row.name) {
          item.classList.add("is-active");
        }

        item.innerHTML = `
          <button class="erp-ai-assistant-history__content" type="button">
            <span class="erp-ai-assistant-history__title">${frappe.utils.escape_html(row.title || "New chat")}</span>
            <span class="erp-ai-assistant-history__meta">${row.modified || ""}</span>
          </button>
          <div class="erp-ai-assistant-history__actions">
            <button class="erp-ai-assistant-history__icon" type="button" data-action="pin" title="${row.is_pinned ? "Unpin" : "Pin"}">${row.is_pinned ? "P" : "+"}</button>
            <button class="erp-ai-assistant-history__icon" type="button" data-action="delete" title="Delete">x</button>
          </div>
        `;

        item.querySelector('[data-action="pin"]')?.addEventListener("click", () => this.togglePin(row.name));
        item.querySelector('[data-action="delete"]')?.addEventListener("click", () => this.deleteConversation(row.name));
        item.querySelector(".erp-ai-assistant-history__content")?.addEventListener("click", () => this.loadConversation(row.name));
        this.elements.history.appendChild(item);
      });
    }

    startDraftConversation() {
      this.activeConversation = null;
      this.isDraftConversation = true;
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

    loadConversation(name) {
      frappe.call({
        method: "erp_ai_assistant.api.chat.get_conversation",
        args: { name },
        callback: (response) => {
          const payload = response.message || {};
          this.activeConversation = payload.conversation || null;
          this.isDraftConversation = false;
          this.renderMessages(payload.messages || []);
          this.renderHistory();
        },
        error: (error) => {
          this.activeConversation = null;
          this.renderMessages([]);
          frappe.show_alert({ message: error.message || "Unable to load conversation", indicator: "red" });
        },
      });
    }

    renderMessages(messages) {
      if (!this.elements.messages) return;

      this.elements.messages.innerHTML = "";

      if (!messages.length) {
        const emptyText = this.isDraftConversation
          ? '<div class="erp-ai-assistant-messages__empty"><div class="erp-ai-assistant-messages__empty-icon">💬</div><p>Start typing to create a new conversation</p></div>'
          : '<div class="erp-ai-assistant-messages__empty"><div class="erp-ai-assistant-messages__empty-icon">🤖</div><p>Select a conversation or start a new one to begin chatting</p></div>';

        this.elements.messages.innerHTML = emptyText;
        return;
      }

      messages.forEach((message) => {
        const bubble = document.createElement("div");
        bubble.className = `erp-ai-assistant-message ${message.role === "user" ? "is-user" : "is-assistant"}`;

        const roleLabel = message.role === "user" ? "You" : "Assistant";
        const emoji = message.role === "user" ? "👤" : "🤖";
        const timestamp = frappe.datetime.str_to_user ? frappe.datetime.str_to_user(message.creation) : message.creation;

        bubble.innerHTML = `
          <div class="erp-ai-assistant-message__meta">${emoji} ${roleLabel} <span class="erp-ai-assistant-message__time">• ${timestamp}</span></div>
          <div class="erp-ai-assistant-message__body"></div>
        `;

        const bodyElement = bubble.querySelector(".erp-ai-assistant-message__body");
        const content = message.content || "";

        // Preserve line breaks and basic formatting
        bodyElement.innerHTML = this._formatMessageContent(content);

        // Add copy button for assistant messages
        if (message.role !== "user" && content) {
          const copyBtn = document.createElement("button");
          copyBtn.className = "erp-ai-assistant-message__copy";
          copyBtn.innerHTML = "📋";
          copyBtn.title = "Copy message";
          copyBtn.type = "button";
          copyBtn.addEventListener("click", async (event) => {
            event.preventDefault();
            event.stopPropagation();
            const copied = await this._copyText(content);
            if (copied) {
              copyBtn.innerHTML = "✓";
              copyBtn.title = "Copied!";
              setTimeout(() => {
                copyBtn.innerHTML = "📋";
                copyBtn.title = "Copy message";
              }, 1200);
              return;
            }
            copyBtn.innerHTML = "!";
            copyBtn.title = "Copy failed";
            setTimeout(() => {
              copyBtn.innerHTML = "📋";
              copyBtn.title = "Copy message";
            }, 1200);
          });
          bubble.appendChild(copyBtn);
        }

        this.elements.messages.appendChild(bubble);
      });

      this.elements.messages.scrollTop = this.elements.messages.scrollHeight;
    }

    _formatMessageContent(content) {
      if (!content) return "";

      // Escape HTML
      const escaped = content.replace(/&/g, "&amp;")
                           .replace(/</g, "&lt;")
                           .replace(/>/g, "&gt;");

      // Convert markdown-like code blocks
      return escaped
        .replace(/```([\s\S]*?)```/g, '<pre class="erp-ai-assistant-message__code-block"><code>$1</code></pre>')
        .replace(/`([^`]+)`/g, '<code class="erp-ai-assistant-message__inline-code">$1</code>')
        .replace(/\n\n/g, '</p><p>')
        .replace(/\n/g, '<br>');
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
          this.refreshHistory();
          callback(conversation.name);
        },
        error: (error) => {
          frappe.show_alert({ message: error.message || "Assistant setup is incomplete", indicator: "red" });
        },
      });
    }

    sendPrompt() {
      const prompt = (this.elements.textarea?.value || "").trim();
      if (!prompt) return;

      // Add user message to UI immediately
      this._addUserMessageToUI(prompt);

      this.ensureConversation((conversationName) => {
        const context = this.getCurrentContext();

        if (this.elements.textarea) {
          this.elements.textarea.value = "";
        }

        // Show typing indicator
        this._showTypingIndicator();

        frappe.call({
          method: "erp_ai_assistant.api.ai.send_prompt",
          freeze: true,
          freeze_message: "Assistant is thinking...",
          args: {
            conversation: conversationName,
            prompt,
            doctype: context.doctype,
            docname: context.docname,
            route: context.route,
          },
          callback: (response) => {
            this._hideTypingIndicator();
            this.refreshHistory();
            this.loadConversation(conversationName);
            this._setExportBundle(response.message?.payload || null);
          },
          error: (error) => {
            this._hideTypingIndicator();
            frappe.show_alert({ message: error.message || "Assistant request failed", indicator: "red" });
          },
        });
      });
    }

    _addUserMessageToUI(prompt) {
      if (!this.elements.messages) return;

      const emptyState = this.elements.messages.querySelector(".erp-ai-assistant-messages__empty");
      if (emptyState) {
        emptyState.remove();
      }

      const userBubble = document.createElement("div");
      userBubble.className = "erp-ai-assistant-message is-user";

      const now = new Date();
      const timeStr = frappe.datetime ? frappe.datetime.str_to_user(now) : now.toLocaleTimeString();

      userBubble.innerHTML = `
        <div class="erp-ai-assistant-message__meta">👤 You <span class="erp-ai-assistant-message__time">• ${timeStr}</span></div>
        <div class="erp-ai-assistant-message__body">${this._formatMessageContent(prompt)}</div>
      `;

      this.elements.messages.appendChild(userBubble);
      this.elements.messages.scrollTop = this.elements.messages.scrollHeight;
    }

    _showTypingIndicator() {
      if (!this.elements.messages) return;

      this._hideTypingIndicator(); // Remove any existing indicator

      const indicator = document.createElement("div");
      indicator.className = "erp-ai-assistant-message is-assistant erp-ai-assistant-message--typing";
      indicator.id = "erp-ai-assistant-typing";
      indicator.innerHTML = `
        <div class="erp-ai-assistant-message__meta">🤖 Assistant <span class="erp-ai-assistant-message__time">• typing</span></div>
        <div class="erp-ai-assistant-message__typing-indicator">
          <span></span>
          <span></span>
          <span></span>
        </div>
      `;

      this.elements.messages.appendChild(indicator);
      this.elements.messages.scrollTop = this.elements.messages.scrollHeight;
    }

    _hideTypingIndicator() {
      const indicator = document.getElementById("erp-ai-assistant-typing");
      if (indicator) {
        indicator.remove();
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

    _setExportBundle(payload) {
      this.latestExportBundle = payload;
      const enabled = !!payload;
      if (this.elements.exportExcel) this.elements.exportExcel.disabled = !enabled;
      if (this.elements.exportPdf) this.elements.exportPdf.disabled = !enabled;
      if (this.elements.exportWord) this.elements.exportWord.disabled = !enabled;
    }

    exportData(format) {
      if (!this.latestExportBundle) {
        frappe.show_alert({ message: "No data to export", indicator: "yellow" });
        return;
      }

      const payload = {
        title: this.activeConversation?.title || "Export",
        rows: this.latestExportBundle.rows || [this.latestExportBundle],
      };

      frappe.call({
        method:
          format === "excel"
            ? "erp_ai_assistant.api.export.export_to_excel"
            : format === "pdf"
            ? "erp_ai_assistant.api.export.export_to_pdf"
            : "erp_ai_assistant.api.export.export_to_word",
        args: { payload: JSON.stringify(payload), filename: this.activeConversation?.title },
        callback: () => {
          // Frappe handles download automatically
        },
        error: (error) => {
          frappe.show_alert({ message: error.message || "Export failed", indicator: "red" });
        },
      });
    }

    exportHistory() {
      frappe.call({
        method: "erp_ai_assistant.api.import_export.export_history",
        callback: () => {
          frappe.show_alert({ message: "Export started", indicator: "green" });
        },
        error: (error) => {
          frappe.show_alert({ message: error.message || "Export failed", indicator: "red" });
        },
      });
    }

    importHistory(event) {
      const file = event?.target?.files?.[0];
      if (!file) return;

      const reader = new FileReader();
      reader.onload = (e) => {
        frappe.call({
          method: "erp_ai_assistant.api.import_export.import_history",
          args: { file_data: e.target.result },
          callback: (response) => {
            frappe.show_alert({
              message: `Imported ${response.message.count} conversations`,
              indicator: "green",
            });
            this.refreshHistory();
            event.target.value = "";
          },
          error: (error) => {
            frappe.show_alert({ message: error.message || "Import failed", indicator: "red" });
          },
        });
      };

      reader.readAsText(file);
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
  erp_ai_assistant.drawer = new ERPAssistantBubble();

  frappe.after_ajax(() => {
    erp_ai_assistant.drawer.boot();
  });
})();
