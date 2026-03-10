(function () {
  function safeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  class ERPWebAssistant {
    constructor(root) {
      this.root = root;
      this.state = {
        conversations: [],
        active: null,
        isDraft: false,
      };

      this.el = {
        history: root.querySelector('[data-role="history"]'),
        search: root.querySelector('[data-role="search"]'),
        messages: root.querySelector('[data-role="messages"]'),
        prompt: root.querySelector('[data-role="prompt"]'),
        context: root.querySelector('[data-role="context"]'),
        send: root.querySelector('[data-action="send"]'),
        newChat: root.querySelector('[data-action="new-chat"]'),
      };
    }

    boot() {
      if (!window.frappe || !frappe.call) {
        this.renderSystemMessage("Frappe web runtime not found.");
        return;
      }

      this.bind();
      this.updateContext();
      this.loadHistory();
      this.renderMessages([]);
    }

    bind() {
      this.el.send?.addEventListener("click", () => this.sendPrompt());
      this.el.newChat?.addEventListener("click", () => {
        this.state.active = null;
        this.state.isDraft = true;
        this.renderMessages([]);
        if (this.el.prompt) this.el.prompt.value = "";
      });

      this.el.search?.addEventListener("input", () => this.renderHistory());

      this.el.prompt?.addEventListener("keydown", (event) => {
        if (event.key === "Enter" && !event.shiftKey) {
          event.preventDefault();
          this.sendPrompt();
        }
      });
    }

    updateContext() {
      let route = "";
      if (window.location.hash) route = window.location.hash.slice(1);
      this.el.context.textContent = route || "General ERP workspace";
    }

    loadHistory() {
      frappe.call({
        method: "erp_ai_assistant.api.chat.list_conversations",
        callback: (r) => {
          this.state.conversations = r.message || [];
          this.renderHistory();
          if (!this.state.active && this.state.conversations.length) {
            this.loadConversation(this.state.conversations[0].name);
          }
        },
        error: () => {
          this.state.conversations = [];
          this.renderHistory();
        },
      });
    }

    renderHistory() {
      const query = (this.el.search?.value || "").toLowerCase();
      const rows = this.state.conversations.filter(
        (row) => !query || String(row.title || "").toLowerCase().includes(query)
      );

      this.el.history.innerHTML = "";
      if (!rows.length) {
        this.el.history.innerHTML = '<div class="erp-web-assistant__history-empty">No conversations</div>';
        return;
      }

      rows.forEach((row) => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "erp-web-assistant__history-item";
        if (this.state.active && this.state.active.name === row.name) {
          btn.classList.add("is-active");
        }
        btn.innerHTML = `
          <strong>${safeHtml(row.title || "New chat")}</strong><br>
          <small>${safeHtml(row.modified || "")}</small>
        `;
        btn.addEventListener("click", () => this.loadConversation(row.name));
        this.el.history.appendChild(btn);
      });
    }

    loadConversation(name) {
      frappe.call({
        method: "erp_ai_assistant.api.chat.get_conversation",
        args: { name: name },
        callback: (r) => {
          const payload = r.message || {};
          this.state.active = payload.conversation || null;
          this.state.isDraft = false;
          this.renderMessages(payload.messages || []);
          this.renderHistory();
        },
      });
    }

    ensureConversation(callback) {
      if (this.state.active && this.state.active.name) {
        callback(this.state.active.name);
        return;
      }

      const title = (this.el.prompt?.value || "").trim();
      frappe.call({
        method: "erp_ai_assistant.api.chat.create_conversation",
        args: { title: title },
        callback: (r) => {
          this.state.active = r.message;
          this.state.isDraft = false;
          this.loadHistory();
          callback(this.state.active.name);
        },
      });
    }

    sendPrompt() {
      const prompt = (this.el.prompt?.value || "").trim();
      if (!prompt) return;

      this.pushMessage({ role: "user", content: prompt, creation: new Date().toISOString() });
      if (this.el.prompt) this.el.prompt.value = "";

      this.ensureConversation((conversationName) => {
        frappe.call({
          method: "erp_ai_assistant.api.ai.send_prompt",
          args: {
            conversation: conversationName,
            prompt: prompt,
            route: window.location.hash ? window.location.hash.slice(1) : "",
          },
          callback: () => {
            this.loadConversation(conversationName);
            this.loadHistory();
          },
          error: (err) => {
            this.renderSystemMessage(err.message || "Request failed");
          },
        });
      });
    }

    renderMessages(messages) {
      this.el.messages.innerHTML = "";
      if (!messages.length) {
        const text = this.state.isDraft ? "Start typing to create a new conversation." : "No messages yet.";
        this.el.messages.innerHTML = `<div class="erp-web-assistant__empty">${safeHtml(text)}</div>`;
        return;
      }

      messages.forEach((message) => this.pushMessage(message));
    }

    pushMessage(message) {
      const item = document.createElement("div");
      item.className = "erp-web-assistant__msg";
      const isUser = message.role === "user";
      if (isUser) item.classList.add("is-user");

      item.innerHTML = `
        <span class="erp-web-assistant__msg-meta">${isUser ? "You" : "Assistant"}</span>
        <div>${safeHtml(message.content || "")}</div>
      `;
      this.el.messages.appendChild(item);
      this.el.messages.scrollTop = this.el.messages.scrollHeight;
    }

    renderSystemMessage(text) {
      this.el.messages.innerHTML = `<div class="erp-web-assistant__empty">${safeHtml(text)}</div>`;
    }
  }

  window.addEventListener("load", function () {
    const root = document.getElementById("erp-web-assistant");
    if (!root) return;
    new ERPWebAssistant(root).boot();
  });
})();
