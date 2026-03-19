(function () {
  function shouldShowToolActivity() {
    try {
      return window.localStorage?.getItem("erp_ai_assistant_debug_tools") === "1";
    } catch (error) {
      return false;
    }
  }

  function safeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function decodeHtml(value) {
    const textarea = document.createElement("textarea");
    textarea.innerHTML = String(value || "");
    return textarea.value;
  }

  function isImageUrl(url) {
    const value = String(url || "").toLowerCase();
    return value.startsWith("data:image/") || /\.(png|jpe?g|gif|webp|svg)(\?|#|$)/.test(value);
  }

  function filenameFromUrl(url) {
    try {
      const parsed = new URL(url, window.location.origin);
      const name = parsed.pathname.split("/").filter(Boolean).pop();
      return name || "Image";
    } catch (error) {
      return "Image";
    }
  }

  function renderAnchor(url, label) {
    return `<a class="erp-web-assistant__link" href="${safeHtml(url)}" target="_blank" rel="noopener noreferrer">${safeHtml(label || url)}</a>`;
  }

  function renderInlineImage(url, alt) {
    return `
      <figure class="erp-web-assistant__media">
        <a href="${safeHtml(url)}" target="_blank" rel="noopener noreferrer">
          <img class="erp-web-assistant__image" src="${safeHtml(url)}" alt="${safeHtml(alt || "Image")}" loading="lazy" />
        </a>
        <figcaption class="erp-web-assistant__caption">${safeHtml(alt || "Image")}</figcaption>
      </figure>
    `;
  }

  function restoreTokens(text, tokens) {
    return tokens.reduce(
      (output, tokenHtml, index) => output.replaceAll(`__ERP_WEB_INLINE_${index}__`, tokenHtml).replaceAll(`__ERP_WEB_BLOCK_${index}__`, tokenHtml),
      text
    );
  }

  function renderInline(text) {
    const tokens = [];
    let value = String(text || "");

    value = value.replace(/!\[([^\]]*)\]\(((?:https?:\/\/|\/)[^\s)]+|data:image\/[^\s)]+)\)/g, (_, alt, url) => {
      const token = `__ERP_WEB_INLINE_${tokens.length}__`;
      tokens.push(renderInlineImage(url, alt));
      return token;
    });

    value = value.replace(/\[([^\]]+)\]\(((?:https?:\/\/|\/)[^\s)]+)\)/g, (_, label, url) => {
      const token = `__ERP_WEB_INLINE_${tokens.length}__`;
      tokens.push(renderAnchor(url, label));
      return token;
    });

    value = value.replace(/`([^`]+)`/g, (_, code) => {
      const token = `__ERP_WEB_INLINE_${tokens.length}__`;
      tokens.push(`<code class="erp-web-assistant__inline-code">${safeHtml(code)}</code>`);
      return token;
    });

    value = safeHtml(value);
    value = value.replace(/\n/g, "<br>");
    value = value.replace(/(https?:\/\/[^\s<]+)/g, (url) => {
      const cleanUrl = decodeHtml(url);
      const token = `__ERP_WEB_INLINE_${tokens.length}__`;
      tokens.push(isImageUrl(cleanUrl) ? renderInlineImage(cleanUrl, filenameFromUrl(cleanUrl)) : renderAnchor(cleanUrl, cleanUrl));
      return token;
    });

    return restoreTokens(value, tokens);
  }

  function renderBlock(chunk) {
    const trimmed = String(chunk || "").trim();
    if (!trimmed) return "";

    if (/^#{1,3}\s/.test(trimmed)) {
      const level = Math.min(6, Math.max(3, (trimmed.match(/^#+/) || ["###"])[0].length + 2));
      return `<h${level} class="erp-web-assistant__heading">${renderInline(trimmed.replace(/^#{1,3}\s*/, ""))}</h${level}>`;
    }

    const lines = trimmed.split("\n").map((line) => line.trimEnd());
    if (looksLikeMarkdownTable(lines)) {
      return renderTable(lines);
    }

    if (lines.every((line) => /^\s*[-*]\s+/.test(line))) {
      return `<ul class="erp-web-assistant__list">${lines.map((line) => `<li>${renderInline(line.replace(/^\s*[-*]\s+/, ""))}</li>`).join("")}</ul>`;
    }

    if (lines.every((line) => /^\s*\d+\.\s+/.test(line))) {
      return `<ol class="erp-web-assistant__list">${lines.map((line) => `<li>${renderInline(line.replace(/^\s*\d+\.\s+/, ""))}</li>`).join("")}</ol>`;
    }

    if (trimmed.startsWith(">")) {
      const body = lines.map((line) => line.replace(/^\s*>\s?/, "")).join("\n");
      return `<blockquote class="erp-web-assistant__quote">${renderInline(body)}</blockquote>`;
    }

    return `<p class="erp-web-assistant__paragraph">${renderInline(lines.join("\n"))}</p>`;
  }

  function looksLikeMarkdownTable(lines) {
    if (!Array.isArray(lines) || lines.length < 2) return false;
    const hasPipes = lines[0].includes("|") && lines[1].includes("|");
    const separator = /^\s*\|?[\s:-]+(?:\|[\s:-]+)+\|?\s*$/;
    return hasPipes && separator.test(lines[1]);
  }

  function renderTable(lines) {
    const rows = lines
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => line.replace(/^\|/, "").replace(/\|$/, "").split("|").map((cell) => cell.trim()));

    if (rows.length < 2) {
      return `<p class="erp-web-assistant__paragraph">${renderInline(lines.join("\n"))}</p>`;
    }

    const headers = rows[0];
    const bodyRows = rows.slice(2);
    const thead = `<thead><tr>${headers.map((cell) => `<th>${renderInline(cell)}</th>`).join("")}</tr></thead>`;
    const tbody = `<tbody>${bodyRows.map((row) => `<tr>${headers.map((_, index) => `<td>${renderInline(row[index] || "")}</td>`).join("")}</tr>`).join("")}</tbody>`;
    return `<div class="erp-web-assistant__table-wrap"><table class="erp-web-assistant__table">${thead}${tbody}</table></div>`;
  }

  function renderRichText(content) {
    if (!content) return "";
    const tokens = [];
    const withCodeTokens = String(content).replace(/```([\s\S]*?)```/g, (_, block) => {
      const token = `__ERP_WEB_BLOCK_${tokens.length}__`;
      tokens.push(`<pre class="erp-web-assistant__code-block"><code>${safeHtml(block || "")}</code></pre>`);
      return token;
    });

    const rendered = withCodeTokens
      .split(/\n{2,}/)
      .map((chunk) => renderBlock(chunk))
      .filter(Boolean)
      .join("");

    return restoreTokens(rendered, tokens);
  }

  function parseAttachmentPackage(raw) {
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

  function renderAttachments(raw) {
    const pkg = parseAttachmentPackage(raw);
    const attachments = pkg.attachments;
    const exports = pkg.exports || {};
    if (!attachments.length) return "";
    const cards = attachments.map((item) => {
      const name = safeHtml(item.filename || "download");
      const label = safeHtml(item.label || item.file_type || "File");
      const url = safeHtml(item.file_url);
      const preview = exports[item.export_id] && Array.isArray(exports[item.export_id].rows)
        ? renderAttachmentPreview(exports[item.export_id].rows)
        : "";
      const actions = `
        <span class="erp-web-assistant__attachment-actions">
          <a class="erp-web-assistant__link" href="${url}" download="${name}">Download</a>
        </span>
      `;
      if (isImageUrl(item.file_url)) {
        return `
          <div class="erp-web-assistant__attachment is-image">
            <img class="erp-web-assistant__attachment-image" src="${url}" alt="${name}" loading="lazy" />
            <span class="erp-web-assistant__attachment-meta">
              <strong>${name}</strong>
              <small>${label}</small>
            </span>
            ${actions}
          </div>
        `;
      }
      return `
        <div class="erp-web-assistant__attachment">
          <span class="erp-web-assistant__attachment-meta">
            <strong>${name}</strong>
            <small>${label}</small>
          </span>
          ${actions}
          ${preview}
        </div>
      `;
    }).join("");

    return `<div class="erp-web-assistant__attachments">${cards}</div>`;
  }

  function renderAttachmentPreview(rows) {
    const previewRows = Array.isArray(rows) ? rows.filter((row) => row && typeof row === "object").slice(0, 3) : [];
    if (!previewRows.length) return "";
    const headers = Object.keys(previewRows[0]).slice(0, 4);
    if (!headers.length) return "";
    return `
      <div class="erp-web-assistant__attachment-preview">
        <div class="erp-web-assistant__table-wrap">
          <table class="erp-web-assistant__table">
            <thead><tr>${headers.map((header) => `<th>${renderInline(header)}</th>`).join("")}</tr></thead>
            <tbody>
              ${previewRows.map((row) => `<tr>${headers.map((header) => `<td>${renderInline(row[header] == null ? "" : String(row[header]))}</td>`).join("")}</tr>`).join("")}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }

  function parseToolEvents(raw) {
    if (!raw) return [];
    try {
      const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
      return Array.isArray(parsed) ? parsed.filter(Boolean) : [];
    } catch (error) {
      return [];
    }
  }

  function renderStructuredToolEvent(item) {
    const type = String(item?.type || "").trim();
    if (!type) return "";
    if (type === "missing_fields") {
      const rows = Array.isArray(item.items) ? item.items : [];
      return `
        <div class="erp-web-assistant__tool-card">
          <div class="erp-web-assistant__tool-title">Missing fields</div>
          <div class="erp-web-assistant__tool-chips">
            ${rows.slice(0, 8).map((row) => `<span class="erp-web-assistant__tool-chip">${safeHtml(row.label || row.fieldname || "")}</span>`).join("")}
          </div>
        </div>
      `;
    }
    if (type === "missing_child_rows") {
      const rows = Array.isArray(item.items) ? item.items : [];
      return `
        <div class="erp-web-assistant__tool-card">
          <div class="erp-web-assistant__tool-title">Incomplete child rows</div>
          <div class="erp-web-assistant__tool-options">
            ${rows.slice(0, 6).map((row) => `<div class="erp-web-assistant__tool-option"><strong>${safeHtml((row.table_label || "Rows") + " row " + (row.row_index || ""))}</strong><small>${safeHtml((row.fields || []).map((field) => field.label || field.fieldname || "").join(", "))}</small></div>`).join("")}
          </div>
        </div>
      `;
    }
    if (type === "candidates") {
      const rows = Array.isArray(item.items) ? item.items : [];
      return `
        <div class="erp-web-assistant__tool-card">
          <div class="erp-web-assistant__tool-title">Choose one</div>
          <div class="erp-web-assistant__tool-options">
            ${rows.slice(0, 6).map((row) => `<div class="erp-web-assistant__tool-option"><strong>${safeHtml(row.name || "")}</strong><small>${safeHtml(row.label || "")}</small></div>`).join("")}
          </div>
        </div>
      `;
    }
    if (type === "document_ref") {
      const label = `${item.doctype || "Document"} ${item.name || ""}`.trim();
      return `
        <div class="erp-web-assistant__tool-card">
          <div class="erp-web-assistant__tool-title">Document</div>
          <div class="erp-web-assistant__tool-link">${item.url ? renderAnchor(item.url, label) : safeHtml(label)}</div>
        </div>
      `;
    }
    if (type === "planner") {
      const source = String(item.source || "").trim();
      const reason = String(item.reason || "").trim();
      return `
        <div class="erp-web-assistant__tool-card">
          <div class="erp-web-assistant__tool-title">Planner</div>
          <div class="erp-web-assistant__tool-option">
            <strong>${safeHtml(String(item.intent || "unknown"))}</strong>
            <small>confidence ${safeHtml(String(item.confidence ?? ""))}${source ? ` • ${safeHtml(source)}` : ""}${reason ? ` • ${safeHtml(reason)}` : ""}</small>
          </div>
        </div>
      `;
    }
    return "";
  }

  function renderToolEvents(raw) {
    if (!shouldShowToolActivity()) return "";
    const events = parseToolEvents(raw);
    if (!events.length) return "";
    const textEvents = events.filter((item) => typeof item === "string" && String(item || "").trim());
    const structuredEvents = events.filter((item) => item && typeof item === "object" && !Array.isArray(item));
    return `
      <details class="erp-web-assistant__tools">
        <summary class="erp-web-assistant__tools-summary">Tool activity</summary>
        ${structuredEvents.length ? `<div class="erp-web-assistant__tool-blocks">${structuredEvents.map((item) => renderStructuredToolEvent(item)).join("")}</div>` : ""}
        ${textEvents.length ? `<ul class="erp-web-assistant__tools-list">
          ${textEvents.slice(-8).map((item) => `<li>${safeHtml(item)}</li>`).join("")}
        </ul>` : ""}
      </details>
    `;
  }

  class ERPWebAssistant {
    constructor(root) {
      this.root = root;
      this.state = {
        conversations: [],
        active: null,
        isDraft: false,
      };
      this.awaitingQueueAck = {};
      this.modelStorageKey = "erp_ai_assistant_selected_model";
      this.progressPollTimer = null;
      this.progressPollPending = false;
      this.pendingImages = [];

      this.el = {
        history: root.querySelector('[data-role="history"]'),
        search: root.querySelector('[data-role="search"]'),
        messages: root.querySelector('[data-role="messages"]'),
        prompt: root.querySelector('[data-role="prompt"]'),
        imagePreview: root.querySelector('[data-role="image-preview"]'),
        imageInput: root.querySelector('[data-role="image-input"]'),
        modelSelect: root.querySelector('[data-role="model-select"]'),
        context: root.querySelector('[data-role="context"]'),
        send: root.querySelector('[data-action="send"]'),
        attachImage: root.querySelector('[data-action="attach-image"]'),
        newChat: root.querySelector('[data-action="new-chat"]'),
      };
    }

    boot() {
      if (!window.frappe || !frappe.call) {
        this.renderSystemMessage("Frappe web runtime not found.");
        return;
      }

      this.bind();
      this.loadModelOptions();
      this.updateContext();
      this.loadHistory();
      this.renderMessages([]);
    }

    bind() {
      this.el.send?.addEventListener("click", () => this.sendPrompt());
      this.el.attachImage?.addEventListener("click", () => this.el.imageInput?.click());
      this.el.newChat?.addEventListener("click", () => {
        this.state.active = null;
        this.state.isDraft = true;
        this.clearPendingImages();
        this.renderMessages([]);
        if (this.el.prompt) this.el.prompt.value = "";
      });

      this.el.search?.addEventListener("input", () => this.renderHistory());
      this.el.modelSelect?.addEventListener("change", () => this.persistSelectedModel());
      this.el.imageInput?.addEventListener("change", async (event) => {
        await this.ingestImageFiles(event?.target?.files);
        if (this.el.imageInput) this.el.imageInput.value = "";
      });

      this.el.prompt?.addEventListener("keydown", (event) => {
        if (event.key === "Enter" && !event.shiftKey) {
          event.preventDefault();
          this.sendPrompt();
        }
      });
      this.el.prompt?.addEventListener("paste", async (event) => {
        const items = Array.from(event.clipboardData?.items || []).filter((item) => String(item.type || "").startsWith("image/"));
        if (!items.length) return;
        event.preventDefault();
        const files = items.map((item) => item.getAsFile()).filter(Boolean);
        await this.ingestImageFiles(files);
      });
    }

    updateContext() {
      let route = "";
      if (window.location.hash) route = window.location.hash.slice(1);
      this.el.context.textContent = route || "General workspace";
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
          this.clearPendingImages();
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
      const images = this.pendingImages.slice();
      if (!prompt && !images.length) return;

      const userContent = prompt || `[Attached ${images.length} image${images.length === 1 ? "" : "s"}]`;
      const userAttachments = {
        attachments: images.map((item, index) => ({
          label: "Image",
          filename: item.name || `image-${index + 1}`,
          file_type: String(item.type || "image").split("/").pop(),
          file_url: item.dataUrl,
        })),
      };
      this.pushMessage({
        role: "user",
        content: userContent,
        creation: new Date().toISOString(),
        attachments_json: JSON.stringify(userAttachments),
      });
      if (this.el.prompt) this.el.prompt.value = "";
      this.clearPendingImages();

      this.ensureConversation((conversationName) => {
        this.awaitingQueueAck[conversationName] = true;
        frappe.call({
          method: "erp_ai_assistant.api.assistant.handle_prompt",
          args: {
            conversation: conversationName,
            prompt: prompt,
            route: window.location.hash ? window.location.hash.slice(1) : "",
            model: this.el.modelSelect?.value || undefined,
            images: images.length
              ? JSON.stringify(
                images.map((item) => ({
                  name: item.name,
                  type: item.type,
                  data_url: item.dataUrl,
                }))
              )
              : undefined,
          },
          callback: (response) => {
            const payload = response?.message || {};
            if (payload.queued) {
              this.awaitingQueueAck[conversationName] = false;
              this.startProgressPolling(conversationName);
              return;
            }
            this.finishPromptRun(conversationName);
          },
          error: (err) => {
            delete this.awaitingQueueAck[conversationName];
            this.finishPromptRun(null, { keepMessages: true });
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
        <div class="erp-web-assistant__msg-body">${renderRichText(message.content || "")}</div>
        ${renderToolEvents(message.tool_events)}
        ${renderAttachments(message.attachments_json)}
      `;
      this.el.messages.appendChild(item);
      this.el.messages.scrollTop = this.el.messages.scrollHeight;
    }

    renderSystemMessage(text) {
      this.el.messages.innerHTML = `<div class="erp-web-assistant__empty">${safeHtml(text)}</div>`;
    }

    loadModelOptions() {
      frappe.call({
        method: "erp_ai_assistant.api.ai.get_available_models",
        callback: (response) => {
          this.setModelOptions(response?.message || {});
        },
        error: () => {
          this.setModelOptions({});
        },
      });
    }

    setModelOptions(payload) {
      const select = this.el.modelSelect;
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
        if (model === selected) option.selected = true;
        select.appendChild(option);
      });

      if (!models.length) {
        const option = document.createElement("option");
        option.value = "";
        option.textContent = "Default model";
        option.selected = true;
        select.appendChild(option);
      }

      this.persistSelectedModel();
    }

    persistSelectedModel() {
      const selected = this.el.modelSelect?.value;
      if (!selected) return;
      localStorage.setItem(this.modelStorageKey, selected);
    }

    async ingestImageFiles(fileList) {
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
          const dataUrl = await this.readFileAsDataUrl(file);
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
      this.renderImagePreview();
    }

    readFileAsDataUrl(file) {
      return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(String(reader.result || ""));
        reader.onerror = () => reject(new Error("read_failed"));
        reader.readAsDataURL(file);
      });
    }

    renderImagePreview() {
      const wrap = this.el.imagePreview;
      if (!wrap) return;
      wrap.innerHTML = "";
      if (!this.pendingImages.length) return;
      this.pendingImages.forEach((item, index) => {
        const chip = document.createElement("button");
        chip.type = "button";
        chip.className = "erp-web-assistant__image-chip";
        chip.title = item.name;
        chip.innerHTML = `
          <img src="${item.dataUrl}" alt="${item.name}" />
          <span>${safeHtml(item.name)}</span>
          <strong>x</strong>
        `;
        chip.addEventListener("click", () => {
          this.pendingImages.splice(index, 1);
          this.renderImagePreview();
        });
        wrap.appendChild(chip);
      });
    }

    clearPendingImages() {
      this.pendingImages = [];
      this.renderImagePreview();
    }

    startProgressPolling(conversationName) {
      this.stopProgressPolling();
      this.showProgress(["Preparing response"]);
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
            const stage = String(progress.stage || "").trim().toLowerCase();
            if ((stage === "idle" || !stage) && this.awaitingQueueAck[conversationName]) {
              return;
            }
            this.showProgress(steps, progress.partial_text || "");
            if (progress.done) {
              this.stopProgressPolling();
              if (progress.error) {
                this.finishPromptRun(null, { keepMessages: true });
                this.renderSystemMessage(progress.error || "Request failed");
                return;
              }
              this.finishPromptRun(conversationName);
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
      }, 1200);
    }

    stopProgressPolling() {
      if (this.progressPollTimer) {
        clearInterval(this.progressPollTimer);
        this.progressPollTimer = null;
      }
      this.progressPollPending = false;
      this.hideProgress();
    }

    showProgress(steps, partialText) {
      const existing = this.el.messages.querySelector(".erp-web-assistant__msg.is-progress");
      if (existing) {
        const stepWrap = existing.querySelector(".erp-web-assistant__progress-steps");
        const partialWrap = existing.querySelector(".erp-web-assistant__progress-partial");
        if (stepWrap) {
          const items = Array.isArray(steps) ? steps.filter(Boolean) : [];
          const finalItems = items.length ? items.slice(-4) : ["Preparing response"];
          stepWrap.innerHTML = "";
          finalItems.forEach((item) => {
            const row = document.createElement("div");
            row.className = "erp-web-assistant__progress-step";
            row.textContent = item;
            stepWrap.appendChild(row);
          });
        }
        if (partialWrap) {
          partialWrap.textContent = String(partialText || "").trim();
          partialWrap.style.display = partialWrap.textContent ? "block" : "none";
        }
        return;
      }

      const progress = document.createElement("div");
      progress.className = "erp-web-assistant__msg is-progress";
      progress.innerHTML = `
        <span class="erp-web-assistant__msg-meta">Assistant</span>
        <div class="erp-web-assistant__progress-head">
          <span class="erp-web-assistant__progress-spinner" aria-hidden="true"></span>
          <span>Responding</span>
        </div>
        <div class="erp-web-assistant__progress-steps"></div>
        <div class="erp-web-assistant__progress-partial" style="display:none;"></div>
      `;
      this.el.messages.appendChild(progress);
      this.showProgress(steps, partialText);
      this.el.messages.scrollTop = this.el.messages.scrollHeight;
    }

    hideProgress() {
      this.el.messages.querySelector(".erp-web-assistant__msg.is-progress")?.remove();
    }

    finishPromptRun(conversationName, options) {
      const settings = options || {};
      this.stopProgressPolling();
      if (conversationName) {
        delete this.awaitingQueueAck[conversationName];
        this.loadConversation(conversationName);
        this.loadHistory();
        return;
      }
      if (!settings.keepMessages) {
        this.loadHistory();
      }
    }
  }

  window.addEventListener("load", function () {
    const root = document.getElementById("erp-web-assistant");
    if (!root) return;
    new ERPWebAssistant(root).boot();
  });
})();
