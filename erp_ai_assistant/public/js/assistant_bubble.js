(function () {
  window.__ERP_AI_ASSISTANT_BUILD__ = "2026-03-09-context-guard-v2";

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
        <span class="erp-ai-assistant-bubble__label">AI Assistant</span>
      `;
      bubble.setAttribute("aria-label", "Open AI Assistant");
      bubble.setAttribute("title", "AI Assistant\nClick to open\nCtrl+Enter: Quick open");

      const drawer = document.createElement("div");
      drawer.id = "erp-ai-assistant-drawer";
      drawer.className = "erp-ai-assistant-drawer";
      drawer.setAttribute("role", "dialog");
      drawer.setAttribute("aria-label", "AI Assistant Dialog");
      drawer.innerHTML = `
        <div class="erp-ai-assistant-drawer__sidebar">
          <div class="erp-ai-assistant-drawer__sidebar-head">
            <div>
              <div class="erp-ai-assistant-drawer__eyebrow">AI Assistant</div>
              <h3>Conversations</h3>
            </div>
            <div class="erp-ai-assistant-drawer__sidebar-actions">
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
            <button class="erp-ai-assistant-btn" data-action="close" title="Close assistant (Esc)">Close</button>
          </div>
          <div class="erp-ai-assistant-messages"></div>
          <div class="erp-ai-assistant-composer">
            <textarea rows="3" placeholder="Ask anything. If you are on a document, I can also use that ERP context..." aria-label="Message input"></textarea>
            <div class="erp-ai-assistant-image-preview"></div>
            <input type="file" class="erp-ai-assistant-image-input" accept="image/*" multiple hidden />
            <div class="erp-ai-assistant-composer__actions">
              <div class="erp-ai-assistant-composer__left">
                <select class="erp-ai-assistant-model-select" aria-label="Select AI model">
                  <option value="">Default model</option>
                </select>
                <span class="erp-ai-assistant-composer__hint"><kbd>Enter</kbd> to send <kbd>Shift+Enter</kbd> for newline</span>
              </div>
              <div class="erp-ai-assistant-composer__buttons">
                <button class="erp-ai-assistant-btn" data-action="attach-image" title="Attach image or paste screenshot" type="button">Image</button>
                <button class="erp-ai-assistant-btn erp-ai-assistant-btn--stop" data-action="stop" title="Stop response" aria-label="Stop response" hidden>
                  <span class="erp-ai-assistant-stop-icon" aria-hidden="true"></span>
                </button>
                <button class="erp-ai-assistant-btn erp-ai-assistant-btn--primary" data-action="send" title="Send message">Send</button>
              </div>
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
        imagePreview: drawer.querySelector(".erp-ai-assistant-image-preview"),
        imageInput: drawer.querySelector(".erp-ai-assistant-image-input"),
        attachImageButton: drawer.querySelector('[data-action="attach-image"]'),
        modelSelect: drawer.querySelector(".erp-ai-assistant-model-select"),
        sendButton: drawer.querySelector('[data-action="send"]'),
        stopButton: drawer.querySelector('[data-action="stop"]'),
      };

      bubble.addEventListener("click", () => this.toggleDrawer());
      drawer.querySelector('[data-action="close"]')?.addEventListener("click", () => this.toggleDrawer(false));
      drawer.querySelector('[data-action="send"]')?.addEventListener("click", () => this.sendPrompt());
      drawer.querySelector('[data-action="stop"]')?.addEventListener("click", () => this.stopPrompt());
      drawer.querySelector('[data-action="new-chat"]')?.addEventListener("click", () => this.startDraftConversation());
      this.elements.attachImageButton?.addEventListener("click", () => this.elements.imageInput?.click());
      this.elements.imageInput?.addEventListener("change", async (event) => {
        await this._ingestImageFiles(event?.target?.files);
        if (this.elements.imageInput) {
          this.elements.imageInput.value = "";
        }
      });
      this.elements.modelSelect?.addEventListener("change", () => this._persistSelectedModel());
      this.elements.search?.addEventListener("input", () => this.renderHistory());
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
          this.toggleDrawer(false);
        }
      });

      this._setGeneratingState(false);
      this.loadModelOptions();
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

      const routeText = route.join("/");
      const [view, doctype, docname] = route;
      const isFormRoute = view === "Form";
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
        };
      }

      return {
        doctype: isFormRoute ? doctype || null : null,
        docname: isFormRoute ? docname || null : null,
        route: routeText,
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
        console.warn("AI Assistant: context resolution failed", error);
      }

      const text = context.doctype && context.docname
        ? `${context.doctype} / ${context.docname}`
        : "General chat";

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

    loadConversation(name) {
      frappe.call({
        method: "erp_ai_assistant.api.chat.get_conversation",
        args: { name },
        callback: (response) => {
          const payload = response.message || {};
          this.activeConversation = payload.conversation || null;
          this.isDraftConversation = false;
          this._clearPendingImages();
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

        bodyElement.innerHTML = this._formatMessageContent(content);

        const toolEvents = this._parseToolEvents(message.tool_events);
        if (toolEvents.length) {
          bubble.appendChild(this._renderToolEventRow(toolEvents));
        }

        const attachmentPackage = this._parseAttachmentPackage(message.attachments_json);
        if (attachmentPackage.attachments.length) {
          bubble.appendChild(this._renderAttachmentRow(attachmentPackage.attachments));
        }

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
      return this._renderRichText(content || "");
    }

    _renderRichText(content) {
      if (!content) return "";

      const tokens = [];
      const withCodeTokens = String(content).replace(/```([\s\S]*?)```/g, (_, block) => {
        const token = `__ERP_AI_BLOCK_${tokens.length}__`;
        tokens.push(`<pre class="erp-ai-assistant-message__code-block"><code>${this._escapeHtml(block || "")}</code></pre>`);
        return token;
      });

      const rendered = withCodeTokens
        .split(/\n{2,}/)
        .map((chunk) => this._renderBlock(chunk))
        .filter(Boolean)
        .join("");

      return this._restoreTokens(rendered, tokens);
    }

    _renderBlock(chunk) {
      const trimmed = String(chunk || "").trim();
      if (!trimmed) return "";

      if (/^#{1,3}\s/.test(trimmed)) {
        const level = Math.min(6, Math.max(3, (trimmed.match(/^#+/) || ["###"])[0].length + 2));
        return `<h${level} class="erp-ai-assistant-message__heading">${this._renderInline(trimmed.replace(/^#{1,3}\s*/, ""))}</h${level}>`;
      }

      const lines = trimmed.split("\n").map((line) => line.trimEnd());
      if (this._looksLikeMarkdownTable(lines)) {
        return this._renderTable(lines);
      }

      if (lines.every((line) => /^\s*([-*])\s+/.test(line))) {
        const items = lines
          .map((line) => line.replace(/^\s*[-*]\s+/, ""))
          .map((line) => `<li>${this._renderInline(line)}</li>`)
          .join("");
        return `<ul class="erp-ai-assistant-message__list">${items}</ul>`;
      }

      if (lines.every((line) => /^\s*\d+\.\s+/.test(line))) {
        const items = lines
          .map((line) => line.replace(/^\s*\d+\.\s+/, ""))
          .map((line) => `<li>${this._renderInline(line)}</li>`)
          .join("");
        return `<ol class="erp-ai-assistant-message__list">${items}</ol>`;
      }

      if (trimmed.startsWith(">")) {
        const body = lines.map((line) => line.replace(/^\s*>\s?/, "")).join("\n");
        return `<blockquote class="erp-ai-assistant-message__quote">${this._renderInline(body)}</blockquote>`;
      }

      return `<p class="erp-ai-assistant-message__paragraph">${this._renderInline(lines.join("\n"))}</p>`;
    }

    _looksLikeMarkdownTable(lines) {
      if (!Array.isArray(lines) || lines.length < 2) return false;
      const hasPipes = lines[0].includes("|") && lines[1].includes("|");
      const separator = /^\s*\|?[\s:-]+(?:\|[\s:-]+)+\|?\s*$/;
      return hasPipes && separator.test(lines[1]);
    }

    _renderTable(lines) {
      const rows = lines
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => line.replace(/^\|/, "").replace(/\|$/, "").split("|").map((cell) => cell.trim()));

      if (rows.length < 2) {
        return `<p class="erp-ai-assistant-message__paragraph">${this._renderInline(lines.join("\n"))}</p>`;
      }

      const headers = rows[0];
      const bodyRows = rows.slice(2);
      const thead = `<thead><tr>${headers.map((cell) => `<th>${this._renderInline(cell)}</th>`).join("")}</tr></thead>`;
      const tbody = `<tbody>${bodyRows.map((row) => `<tr>${headers.map((_, index) => `<td>${this._renderInline(row[index] || "")}</td>`).join("")}</tr>`).join("")}</tbody>`;
      return `<div class="erp-ai-assistant-message__table-wrap"><table class="erp-ai-assistant-message__table">${thead}${tbody}</table></div>`;
    }

    _renderInline(text) {
      const tokens = [];
      let value = String(text || "");

      value = value.replace(/!\[([^\]]*)\]\((https?:\/\/[^\s)]+|data:image\/[^\s)]+)\)/g, (_, alt, url) => {
        const token = `__ERP_AI_INLINE_${tokens.length}__`;
        tokens.push(this._renderInlineImage(url, alt));
        return token;
      });

      value = value.replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, (_, label, url) => {
        const token = `__ERP_AI_INLINE_${tokens.length}__`;
        tokens.push(this._renderAnchor(url, label));
        return token;
      });

      value = value.replace(/`([^`]+)`/g, (_, code) => {
        const token = `__ERP_AI_INLINE_${tokens.length}__`;
        tokens.push(`<code class="erp-ai-assistant-message__inline-code">${this._escapeHtml(code)}</code>`);
        return token;
      });

      value = this._escapeHtml(value);
      value = value.replace(/\n/g, "<br>");
      value = value.replace(
        /(https?:\/\/[^\s<]+)/g,
        (url) => {
          const cleanUrl = this._decodeHtml(url);
          if (this._isImageUrl(cleanUrl)) {
            const token = `__ERP_AI_INLINE_${tokens.length}__`;
            tokens.push(this._renderInlineImage(cleanUrl, this._filenameFromUrl(cleanUrl)));
            return token;
          }
          const token = `__ERP_AI_INLINE_${tokens.length}__`;
          tokens.push(this._renderAnchor(cleanUrl, cleanUrl));
          return token;
        }
      );

      return this._restoreTokens(value, tokens);
    }

    _renderAnchor(url, label) {
      const safeUrl = this._escapeHtml(url);
      const safeLabel = this._escapeHtml(label || url);
      return `<a class="erp-ai-assistant-message__link" href="${safeUrl}" target="_blank" rel="noopener noreferrer">${safeLabel}</a>`;
    }

    _renderInlineImage(url, alt) {
      const safeUrl = this._escapeHtml(url);
      const safeAlt = this._escapeHtml(alt || "Image");
      return `
        <figure class="erp-ai-assistant-message__media">
          <a href="${safeUrl}" target="_blank" rel="noopener noreferrer">
            <img class="erp-ai-assistant-message__image" src="${safeUrl}" alt="${safeAlt}" loading="lazy" />
          </a>
          <figcaption class="erp-ai-assistant-message__caption">${safeAlt}</figcaption>
        </figure>
      `;
    }

    _restoreTokens(text, tokens) {
      return tokens.reduce(
        (output, tokenHtml, index) => output.replaceAll(`__ERP_AI_INLINE_${index}__`, tokenHtml).replaceAll(`__ERP_AI_BLOCK_${index}__`, tokenHtml),
        text
      );
    }

    _escapeHtml(value) {
      return String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }

    _decodeHtml(value) {
      const textarea = document.createElement("textarea");
      textarea.innerHTML = String(value || "");
      return textarea.value;
    }

    _isImageUrl(url) {
      const value = String(url || "").toLowerCase();
      return value.startsWith("data:image/") || /\.(png|jpe?g|gif|webp|svg)(\?|#|$)/.test(value);
    }

    _filenameFromUrl(url) {
      try {
        const parsed = new URL(url, window.location.origin);
        const name = parsed.pathname.split("/").filter(Boolean).pop();
        return name || "Image";
      } catch (error) {
        return "Image";
      }
    }

    _parseAttachmentPackage(raw) {
      if (!raw) return { attachments: [], exports: {} };
      try {
        const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
        if (Array.isArray(parsed)) {
          return { attachments: parsed.filter((item) => item && item.file_url), exports: {} };
        }
        if (parsed && Array.isArray(parsed.attachments)) {
          return {
            attachments: parsed.attachments.filter((item) => item && item.file_url),
            exports: parsed.exports && typeof parsed.exports === "object" ? parsed.exports : {},
          };
        }
        return { attachments: [], exports: {} };
      } catch (error) {
        return { attachments: [], exports: {} };
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

    _renderToolEventRow(events) {
      const textEvents = events.filter((item) => typeof item === "string" && String(item || "").trim());
      const structuredEvents = events.filter((item) => item && typeof item === "object" && !Array.isArray(item));
      const wrap = document.createElement("details");
      wrap.className = "erp-ai-assistant-message__tools";
      wrap.open = false;
      const items = textEvents.slice(-8).map((item) => `<li>${this._escapeHtml(item)}</li>`).join("");
      wrap.innerHTML = `
        <summary class="erp-ai-assistant-message__tools-summary">Tool activity</summary>
        <div class="erp-ai-assistant-message__tool-blocks"></div>
        <ul class="erp-ai-assistant-message__tools-list">${items}</ul>
      `;
      const blocks = wrap.querySelector(".erp-ai-assistant-message__tool-blocks");
      structuredEvents.forEach((item) => {
        const card = this._renderStructuredToolEvent(item);
        if (card) blocks.appendChild(card);
      });
      const list = wrap.querySelector(".erp-ai-assistant-message__tools-list");
      if (!items) {
        list.remove();
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
          this.refreshHistory();
          callback(conversation.name);
        },
        error: (error) => {
          frappe.show_alert({ message: error.message || "Assistant setup is incomplete", indicator: "red" });
        },
      });
    }

    sendPrompt() {
      if (this.isGenerating) return;

      const prompt = (this.elements.textarea?.value || "").trim();
      const images = this.pendingImages.slice();
      if (!prompt && !images.length) return;

      // Add user message to UI immediately
      this._addUserMessageToUI(prompt, images);

      this.ensureConversation((conversationName) => {
        const context = this.getCurrentContext();
        const imagePayload = images.map((item) => ({
          name: item.name,
          type: item.type,
          data_url: item.dataUrl,
        }));

        if (this.elements.textarea) {
          this.elements.textarea.value = "";
        }
        this._clearPendingImages();

        this.abortRequested = false;
        this._setGeneratingState(true);

        const request = frappe.call({
          method: "erp_ai_assistant.api.assistant.handle_prompt",
          args: {
            conversation: conversationName,
            prompt,
            doctype: context.doctype,
            docname: context.docname,
            route: context.route,
            model: this.elements.modelSelect?.value || undefined,
            images: imagePayload.length ? JSON.stringify(imagePayload) : undefined,
          },
          callback: (response) => {
            this.refreshHistory();
            this.loadConversation(conversationName);
          },
          error: (error) => {
            if (this.abortRequested) return;
            frappe.show_alert({ message: error.message || "Assistant request failed", indicator: "red" });
          },
          always: () => {
            this.pendingPromptRequest = null;
            this._stopProgressPolling();
            this._setGeneratingState(false);
            this._hideTypingIndicator();
            this.abortRequested = false;
          },
        });

        this.pendingPromptRequest = request;
        this._startProgressPolling(conversationName);
      });
    }

    stopPrompt() {
      if (!this.isGenerating || !this.pendingPromptRequest) {
        return;
      }

      this.abortRequested = true;
      this.pendingPromptRequest.abort("abort");
      this.pendingPromptRequest = null;
      this._stopProgressPolling();
      this._setGeneratingState(false);
      this._hideTypingIndicator();
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
        <div class="erp-ai-assistant-message__meta">👤 You <span class="erp-ai-assistant-message__time">• ${timeStr}</span></div>
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

    _showTypingIndicator(steps) {
      if (!this.elements.messages) return;

      this._hideTypingIndicator(); // Remove any existing indicator

      const indicator = document.createElement("div");
      indicator.className = "erp-ai-assistant-message is-assistant erp-ai-assistant-message--typing";
      indicator.id = "erp-ai-assistant-typing";
      indicator.innerHTML = `
        <div class="erp-ai-assistant-message__meta">🤖 Assistant <span class="erp-ai-assistant-message__time">• working</span></div>
        <div class="erp-ai-assistant-progress">
          <div class="erp-ai-assistant-progress__head">
            <span class="erp-ai-assistant-progress__spinner" aria-hidden="true"></span>
            <span>Responding</span>
          </div>
          <div class="erp-ai-assistant-progress__steps"></div>
          <div class="erp-ai-assistant-progress__partial" style="display:none;"></div>
        </div>
      `;

      this.elements.messages.appendChild(indicator);
      this._updateTypingSteps(Array.isArray(steps) ? steps : ["Preparing response"], "");
      this.elements.messages.scrollTop = this.elements.messages.scrollHeight;
    }

    _updateTypingSteps(steps, partialText) {
      const indicator = document.getElementById("erp-ai-assistant-typing");
      if (!indicator) return;
      const wrap = indicator.querySelector(".erp-ai-assistant-progress__steps");
      const partial = indicator.querySelector(".erp-ai-assistant-progress__partial");
      if (!wrap) return;

      const normalized = Array.isArray(steps)
        ? steps.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
      const finalSteps = normalized.length ? normalized : ["Preparing response"];

      wrap.innerHTML = "";
      finalSteps.slice(-4).forEach((step) => {
        const row = document.createElement("div");
        row.className = "erp-ai-assistant-progress__step";
        row.textContent = step;
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

      const pollProgress = () => {
        if (this.progressPollPending) return;
        this.progressPollPending = true;

        frappe.call({
          method: "erp_ai_assistant.api.ai.get_prompt_progress",
          args: { conversation: conversationName },
          callback: (response) => {
            const progress = response?.message || {};
            const steps = Array.isArray(progress.steps) ? progress.steps : [];
            this._updateTypingSteps(steps, progress.partial_text || "");
          },
          always: () => {
            this.progressPollPending = false;
          },
        });
      };

      pollProgress();
      this.progressPollTimer = window.setInterval(() => {
        pollProgress();
      }, 1200);
    }

    _stopProgressPolling() {
      if (this.progressPollTimer) {
        clearInterval(this.progressPollTimer);
        this.progressPollTimer = null;
      }
      this.progressPollPending = false;
    }

    _hideTypingIndicator() {
      const indicator = document.getElementById("erp-ai-assistant-typing");
      if (indicator) {
        indicator.remove();
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
