(function () {
  window.__ERP_AI_WEB_ASSISTANT_BUILD__ = "2026-04-06-grounded-ux-v10";

  /* ── Utilities ─────────────────────────────────────────────────────────── */
  function safeHtml(v) {
    return String(v || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }
  function decodeHtml(v) { const t = document.createElement("textarea"); t.innerHTML = String(v || ""); return t.value; }
  function isImageUrl(u) { const v = String(u || "").toLowerCase(); return v.startsWith("data:image/") || /\.(png|jpe?g|gif|webp|svg)(\?|#|$)/.test(v); }
  function filenameFromUrl(u) { try { return new URL(u, location.origin).pathname.split("/").filter(Boolean).pop() || "Image"; } catch { return "Image"; } }
  function renderAnchor(u, l) { return `<a class="erp-web-assistant__link" href="${safeHtml(u)}" target="_blank" rel="noopener noreferrer">${safeHtml(l || u)}</a>`; }
  function renderInlineImage(u, a) {
    return `<figure class="erp-web-assistant__media"><a href="${safeHtml(u)}" target="_blank" rel="noopener noreferrer"><img class="erp-web-assistant__image" src="${safeHtml(u)}" alt="${safeHtml(a || "Image")}" loading="lazy" /></a><figcaption class="erp-web-assistant__caption">${safeHtml(a || "Image")}</figcaption></figure>`;
  }
  function upperWords(v) { return String(v || "").replace(/[_-]+/g, " ").replace(/\b\w/g, c => c.toUpperCase()).trim(); }
  function logDebug(p) { const d = p?.debug; if (Array.isArray(d?.discovery_doctypes) && d.discovery_doctypes.length && window.console) console.info("[ERP AI] doctypes", d.discovery_doctypes); }

  /* ── Markdown → HTML renderer ──────────────────────────────────────── */
  function restoreTokens(t, tokens) { return tokens.reduce((o, h, i) => o.replaceAll(`__T${i}__`, h), t); }

  function renderInline(text) {
    const tokens = [];
    let v = String(text || "");
    v = v.replace(/!\[([^\]]*)\]\(((?:https?:\/\/|\/)[^\s)]+|data:image\/[^\s)]+)\)/g, (_, a, u) => { tokens.push(renderInlineImage(u, a)); return `__T${tokens.length - 1}__`; });
    v = v.replace(/\[([^\]]+)\]\(((?:https?:\/\/|\/)[^\s)]+)\)/g, (_, l, u) => { tokens.push(renderAnchor(u, l)); return `__T${tokens.length - 1}__`; });
    v = v.replace(/\*\*(.+?)\*\*/g, (_, b) => { tokens.push(`<strong>${safeHtml(b)}</strong>`); return `__T${tokens.length - 1}__`; });
    v = v.replace(/`([^`]+)`/g, (_, c) => { tokens.push(`<code class="erp-web-assistant__inline-code">${safeHtml(c)}</code>`); return `__T${tokens.length - 1}__`; });
    v = safeHtml(v).replace(/\n/g, "<br>");
    v = v.replace(/(https?:\/\/[^\s<]+)/g, u => { const c = decodeHtml(u); tokens.push(isImageUrl(c) ? renderInlineImage(c, filenameFromUrl(c)) : renderAnchor(c, c)); return `__T${tokens.length - 1}__`; });
    return restoreTokens(v, tokens);
  }

  function looksLikeTable(lines) { return lines.length >= 2 && lines[0].includes("|") && lines[1].includes("|") && /^\s*\|?[\s:-]+(?:\|[\s:-]+)+\|?\s*$/.test(lines[1]); }

  function renderTable(lines) {
    const rows = lines.map(l => l.trim()).filter(Boolean).map(l => l.replace(/^\|/, "").replace(/\|$/, "").split("|").map(c => c.trim()));
    if (rows.length < 2) return `<p class="erp-web-assistant__paragraph">${renderInline(lines.join("\n"))}</p>`;
    const h = rows[0], b = rows.slice(2);
    return `<div class="erp-web-assistant__table-wrap"><table class="erp-web-assistant__table"><thead><tr>${h.map(c => `<th>${renderInline(c)}</th>`).join("")}</tr></thead><tbody>${b.map(r => `<tr>${h.map((_, i) => `<td>${renderInline(r[i] || "")}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
  }

  function renderBlock(chunk) {
    const t = String(chunk || "").trim();
    if (!t) return "";
    if (/^#{1,3}\s/.test(t)) { const lvl = Math.min(6, Math.max(3, (t.match(/^#+/) || ["###"])[0].length + 2)); return `<h${lvl} class="erp-web-assistant__heading">${renderInline(t.replace(/^#{1,3}\s*/, ""))}</h${lvl}>`; }
    const lines = t.split("\n").map(l => l.trimEnd());
    if (looksLikeTable(lines)) return renderTable(lines);
    if (lines.every(l => /^\s*[-*]\s+/.test(l))) return `<ul class="erp-web-assistant__list">${lines.map(l => `<li>${renderInline(l.replace(/^\s*[-*]\s+/, ""))}</li>`).join("")}</ul>`;
    if (lines.every(l => /^\s*\d+\.\s+/.test(l))) return `<ol class="erp-web-assistant__list">${lines.map(l => `<li>${renderInline(l.replace(/^\s*\d+\.\s+/, ""))}</li>`).join("")}</ol>`;
    if (t.startsWith(">")) return `<blockquote class="erp-web-assistant__quote">${renderInline(lines.map(l => l.replace(/^\s*>\s?/, "")).join("\n"))}</blockquote>`;
    return `<p class="erp-web-assistant__paragraph">${renderInline(lines.join("\n"))}</p>`;
  }

  function renderRichText(content) {
    if (!content) return "";
    const tokens = [];
    const s = String(content).replace(/```([\s\S]*?)```/g, (_, b) => { tokens.push(`<pre class="erp-web-assistant__code-block"><code>${safeHtml(b || "")}</code></pre>`); return `__T${tokens.length - 1}__`; });
    return restoreTokens(s.split(/\n{2,}/).map(renderBlock).filter(Boolean).join(""), tokens);
  }

  /* ── Message header ────────────────────────────────────────────────── */
  function renderMessageHeader(msg) {
    const isUser = msg.role === "user";
    const role = isUser ? "You" : "Assistant";
    const roleClass = isUser ? "is-user-role" : "";
    const timeLabel = msg.creation ? new Date(msg.creation).toLocaleString() : "";
    const time = timeLabel ? `<span class="erp-web-assistant__msg-time">${safeHtml(timeLabel)}</span>` : "";
    return `<div class="erp-web-assistant__message-head"><div class="erp-web-assistant__msg-meta"><span class="erp-web-assistant__msg-role ${roleClass}">${role}</span>${time}</div></div>`;
  }

  /* ── Tool events ───────────────────────────────────────────────────── */
  function parseToolEvents(raw) { try { const p = typeof raw === "string" ? JSON.parse(raw) : raw; return Array.isArray(p) ? p.filter(Boolean) : []; } catch { return []; } }

  function parseToolEventString(v) {
    const t = String(v || "").trim(); if (!t) return null;
    const m = t.match(/^([a-zA-Z0-9_]+)\s*(.*)$/);
    if (!m) return { label: t, countable: false };
    const map = { list_documents: "Fetched records", get_doctype_info: "Read structure", create_document: "Created draft", update_document: "Updated record", run_python_code: "Ran operation", generate_report: "Generated report", submit_document: "Submitted", run_workflow: "Ran workflow", search_link: "Searched records" };
    let suffix = ""; const dm = m[2].match(/['"]doctype['"]\s*:\s*['"]([^'"]+)['"]/i); if (dm?.[1]) suffix = dm[1];
    const label = map[m[1]] || m[1].replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
    return { label: suffix ? `${label} — ${suffix}` : label, countable: !!map[m[1]] };
  }

  function renderToolEvents(raw) {
    const steps = parseToolEvents(raw).filter(i => typeof i === "string" && i.trim()).map(parseToolEventString).filter(Boolean);
    if (!steps.length) return "";
    const count = steps.filter(i => i.countable).length || steps.length;
    return `<details class="erp-web-assistant__activity-log"><summary class="erp-web-assistant__activity-head"><span class="erp-web-assistant__activity-status">✓</span><span>Ran ${count} step${count === 1 ? "" : "s"}</span></summary><div class="erp-web-assistant__activity-steps">${steps.map(i => `<div class="erp-web-assistant__activity-step"><span class="erp-web-assistant__activity-step-icon">✓</span><span>${safeHtml(i.label)}</span></div>`).join("")}</div></details>`;
  }

  /* ── Attachments ───────────────────────────────────────────────────── */
  function parseAttachmentPkg(raw) {
    if (!raw) return { attachments: [], exports: {}, copilot: null };
    try {
      const p = typeof raw === "string" ? JSON.parse(raw) : raw;
      if (Array.isArray(p)) return { attachments: p.filter(i => i?.file_url), exports: {}, copilot: null };
      if (p && typeof p === "object") return { attachments: Array.isArray(p.attachments) ? p.attachments.filter(i => i?.file_url) : [], exports: p.exports && typeof p.exports === "object" ? p.exports : {}, copilot: p.copilot && typeof p.copilot === "object" ? p.copilot : null };
    } catch { /* ignore */ }
    return { attachments: [], exports: {}, copilot: null };
  }

  function renderDatasetRows(h, items) { return (Array.isArray(items) ? items : []).map(r => `<tr>${h.map(k => `<td>${renderInline(r?.[k] == null ? "" : String(r[k] ?? ""))}</td>`).join("")}</tr>`).join(""); }

  function renderAttachmentPreview(rows, exportId) {
    const all = Array.isArray(rows) ? rows.filter(r => r && typeof r === "object") : [];
    if (!all.length) return "";
    const h = Object.keys(all[0]).slice(0, 12); if (!h.length) return "";
    const preview = all.slice(0, 25);
    return `<div class="erp-web-assistant__attachment-preview"><div class="erp-web-assistant__dataset-toolbar"><strong>${all.length} row${all.length === 1 ? "" : "s"}</strong>${all.length > preview.length ? `<button type="button" class="erp-web-assistant__mini-btn" data-export-toggle="${safeHtml(exportId || "")}" data-state="collapsed">Show all</button>` : ""}</div><div class="erp-web-assistant__table-wrap"><table class="erp-web-assistant__table"><thead><tr>${h.map(k => `<th>${renderInline(k)}</th>`).join("")}</tr></thead><tbody data-export-body="${safeHtml(exportId || "")}" data-preview-rows='${safeHtml(JSON.stringify(preview))}' data-all-rows='${safeHtml(JSON.stringify(all))}'>${renderDatasetRows(h, preview)}</tbody></table></div></div>`;
  }

  function renderCopilotBlock(cop) {
    if (!cop || typeof cop !== "object") return "";
    const s = cop.summary && typeof cop.summary === "object" ? cop.summary : null;
    const actions = Array.isArray(cop.actions) ? cop.actions.filter(i => i?.label && i?.prompt) : [];
    const issues = Array.isArray(cop.issues) ? cop.issues.filter(Boolean) : [];
    const insights = Array.isArray(cop.insights) ? cop.insights.filter(Boolean) : [];
    const suggestions = Array.isArray(cop.suggestions) ? cop.suggestions.filter(Boolean) : [];
    const sRows = Array.isArray(s?.rows) ? s.rows.filter(i => i?.label) : [];
    let html = '<section class="erp-web-assistant__copilot">';
    if (s) html += `<div class="erp-web-assistant__copilot-card"><div class="erp-web-assistant__copilot-head"><strong>${safeHtml(s.title || "Copilot")}</strong></div><div class="erp-web-assistant__copilot-grid">${sRows.map(r => `<div class="erp-web-assistant__copilot-kv"><span>${safeHtml(r.label)}</span><strong>${safeHtml(r.value || "—")}</strong></div>`).join("")}</div></div>`;
    if (issues.length) html += `<div class="erp-web-assistant__copilot-card"><div class="erp-web-assistant__copilot-title">Key issues</div><ul class="erp-web-assistant__copilot-list">${issues.map(i => `<li>${renderInline(i)}</li>`).join("")}</ul></div>`;
    if (actions.length) html += `<div class="erp-web-assistant__copilot-card"><div class="erp-web-assistant__copilot-title">Actions</div><div class="erp-web-assistant__copilot-actions">${actions.map(a => `<button type="button" class="erp-web-assistant__action-btn ${a.style === "primary" ? "is-primary" : ""}" data-copilot-prompt="${safeHtml(a.prompt)}">${safeHtml(a.label)}</button>`).join("")}</div></div>`;
    if (suggestions.length) html += `<div class="erp-web-assistant__copilot-card"><div class="erp-web-assistant__copilot-title">Try next</div><div class="erp-web-assistant__copilot-suggestions">${suggestions.map(s => `<button type="button" class="erp-web-assistant__mini-btn" data-copilot-prompt="${safeHtml(s)}">${safeHtml(s)}</button>`).join("")}</div></div>`;
    return html + "</section>";
  }

  function renderAttachments(raw) {
    const pkg = parseAttachmentPkg(raw);
    const copilot = renderCopilotBlock(pkg.copilot);
    if (!pkg.attachments.length) return copilot;
    const atts = pkg.attachments.map(i => {
      const name = safeHtml(i.filename || "download"), label = safeHtml(i.label || i.file_type || "File"), url = safeHtml(i.file_url);
      const preview = pkg.exports[i.export_id]?.rows ? renderAttachmentPreview(pkg.exports[i.export_id].rows, i.export_id) : "";
      if (isImageUrl(i.file_url)) return `<div class="erp-web-assistant__attachment is-image"><img class="erp-web-assistant__attachment-image" src="${url}" alt="${name}" loading="lazy" /><span class="erp-web-assistant__attachment-meta"><strong>${name}</strong><small>${label}</small></span></div>`;
      return `<div class="erp-web-assistant__attachment"><span class="erp-web-assistant__attachment-meta"><strong>${name}</strong><small>${label}</small></span><a class="erp-web-assistant__link" href="${url}" download="${name}">Download</a>${preview}</div>`;
    }).join("");
    return `${copilot}<div class="erp-web-assistant__attachments">${atts}</div>`;
  }

  /* ── Context ───────────────────────────────────────────────────────── */
  const MODULE_KEYWORDS = [
    { name: "Sales", keywords: ["quotation", "sales order", "sales invoice", "lead", "opportunity", "customer"] },
    { name: "Buying", keywords: ["purchase", "supplier", "material request"] },
    { name: "Stock", keywords: ["item", "warehouse", "stock", "delivery note", "batch"] },
    { name: "Accounts", keywords: ["payment", "journal", "invoice", "account", "expense"] },
    { name: "HR", keywords: ["employee", "attendance", "leave", "payroll"] },
    { name: "Projects", keywords: ["project", "task", "timesheet"] },
  ];

  function inferModule(t) { const l = String(t || "").toLowerCase(); for (const m of MODULE_KEYWORDS) if (m.keywords.some(k => l.includes(k))) return m.name; return "General"; }

  function resolveDeskContext() {
    let route = [];
    try { route = window.frappe?.get_route ? frappe.get_route() : []; } catch { route = []; }
    if (Array.isArray(route)) route = route.filter(s => s != null).map(String);
    else if (typeof route === "string") route = route.split("/").filter(Boolean);
    else route = [];
    if (!route.length && location.hash) route = location.hash.replace(/^#/, "").split("/").filter(Boolean);
    const [rawView, doctype, docname] = route;
    const view = String(rawView || "").toLowerCase();
    const form = window.cur_frm || null;
    if (view === "form" && form?.doc && String(form.doctype || "") === String(doctype || "") && String(form.docname || "") === String(docname || "")) {
      return { mode: "record", view: "form", doctype: form.doctype, docname: form.docname, route: route.join("/"), label: `${form.doctype} / ${form.docname}`, module: inferModule(form.doctype) };
    }
    if (view === "list") return { mode: "list", view, doctype, docname: null, route: route.join("/"), label: `${doctype || "List"} List`, module: inferModule(doctype) };
    return { mode: "general", view, doctype: null, docname: null, route: route.join("/"), label: route.join("/") || "General workspace", module: inferModule(route.join(" ")) };
  }

  function getSuggestions(ctx) {
    return [
      { label: "Summarize this", prompt: ctx?.docname ? `Summarize ${ctx.doctype} ${ctx.docname} and highlight what needs attention.` : "Summarize what needs attention in this area." },
      { label: "Explain status", prompt: ctx?.docname ? `Explain the current status of ${ctx.doctype} ${ctx.docname}.` : "Explain the current status and next steps." },
      { label: "Safe next step", prompt: ctx?.docname ? `Create the safest draft action based on ${ctx.doctype} ${ctx.docname}. Do not submit.` : "What's the safest next action here?" },
      { label: "Follow-ups", prompt: ctx?.docname ? `List follow-up actions for ${ctx.doctype} ${ctx.docname}.` : "List follow-up actions for today." },
    ];
  }

  /* ── Progress step labels ──────────────────────────────────────────── */
  function mapStepLabel(s) {
    const t = String(s || "").trim(), l = t.toLowerCase();
    if (!t) return "Working...";
    if (l === "preparing request" || l === "preparing response") return "Preparing...";
    if (l === "thinking") return "Thinking...";
    if (l === "verifying erp evidence") return "Verifying...";
    if (l === "response ready") return "Ready";
    if (l.startsWith("tool:")) return parseToolEventString(t.replace(/^tool:\s*/i, ""))?.label || t.replace(/^tool:\s*/i, "");
    return t;
  }

  function summarizeSteps(steps, done) {
    const items = Array.isArray(steps) ? steps.filter(Boolean) : [];
    const cmds = items.filter(i => /^tool:/i.test(String(i || "").trim())).length;
    if (cmds) return `${done ? "Completed" : "Running"} ${cmds} step${cmds === 1 ? "" : "s"}${done ? "" : "..."}`;
    return `${done ? "Done" : "Working"}${done ? "" : "..."}`;
  }

  /* ── Date grouping ─────────────────────────────────────────────────── */
  function dateGroup(dateStr) {
    if (!dateStr) return "Older";
    const d = new Date(dateStr), now = new Date();
    const diffDays = Math.floor((now - d) / 86400000);
    if (diffDays === 0) return "Today";
    if (diffDays === 1) return "Yesterday";
    if (diffDays <= 7) return "Previous 7 days";
    return "Older";
  }

  /* ── Auto-grow textarea ────────────────────────────────────────────── */
  function autoGrow(el) {
    if (!el) return;
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, 200) + "px";
  }

  function isCompactViewport() {
    return window.matchMedia && window.matchMedia("(max-width: 900px)").matches;
  }

  /* ═══════════════════════════════════════════════════════════════════════
     ERPWebAssistant Class
     ═══════════════════════════════════════════════════════════════════════ */
  class ERPWebAssistant {
    constructor(root) {
      this.root = root;
      this.state = { conversations: [], active: null, isDraft: false, context: resolveDeskContext() };
      this.awaitingQueueAck = {};
      this.conversationMessageCache = {};
      this.optimisticAssistantMessages = {};
      this.completionReloadTimers = {};
      this.completionFetchLocks = {};
      this.modelStorageKey = "erp_ai_assistant_selected_model";
      this.progressPollTimer = null;
      this.progressPollPending = false;
      this.deferProgressRemoval = false;
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
        suggestions: root.querySelector('[data-role="suggestions"]'),
        status: root.querySelector('[data-role="status"]'),
        moduleBadge: root.querySelector('[data-role="module-badge"]'),
        conversationCount: root.querySelector('[data-role="conversation-count"]'),
        send: root.querySelector('[data-action="send"]'),
        attachImage: root.querySelector('[data-action="attach-image"]'),
        newChat: root.querySelector('[data-action="new-chat"]'),
        refreshContext: root.querySelector('[data-action="refresh-context"]'),
        toggleSidebar: root.querySelector('[data-action="toggle-sidebar"]'),
      };
    }

    boot() {
      if (!window.frappe || !frappe.call) { this.renderSystemMessage("Frappe runtime not found."); return; }
      this.bind();
      this.loadModelOptions();
      this.refreshContext(true);
      this.loadHistory();
      this.renderMessages([]);
    }

    bind() {
      this.el.send?.addEventListener("click", () => this.sendPrompt());
      this.el.attachImage?.addEventListener("click", () => this.el.imageInput?.click());
      this.el.newChat?.addEventListener("click", () => this.startNewChat());
      this.el.refreshContext?.addEventListener("click", () => this.refreshContext(true));
      this.el.search?.addEventListener("input", () => this.renderHistory());
      this.el.modelSelect?.addEventListener("change", () => this.persistSelectedModel());
      this.el.toggleSidebar?.addEventListener("click", () => this.toggleSidebar());
      this.root.addEventListener("click", (e) => {
        if (!this.root.classList.contains("mobile-sidebar-open")) return;
        if (e.target.closest(".erp-web-assistant__sidebar")) return;
        if (e.target.closest('[data-action="toggle-sidebar"]')) return;
        this.closeMobileSidebar();
      });
      this.el.imageInput?.addEventListener("change", async (e) => { await this.ingestImageFiles(e?.target?.files); if (this.el.imageInput) this.el.imageInput.value = ""; });
      this.el.prompt?.addEventListener("keydown", (e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); this.sendPrompt(); } });
      this.el.prompt?.addEventListener("input", () => autoGrow(this.el.prompt));
      this.el.prompt?.addEventListener("paste", async (e) => {
        const items = Array.from(e.clipboardData?.items || []).filter(i => String(i.type || "").startsWith("image/"));
        if (!items.length) return;
        e.preventDefault();
        await this.ingestImageFiles(items.map(i => i.getAsFile()).filter(Boolean));
      });
      window.addEventListener("hashchange", () => this.refreshContext());
      document.addEventListener("visibilitychange", () => { if (!document.hidden) this.refreshContext(); });
      $(document).on("page-change", () => this.refreshContext());
      window.addEventListener("resize", () => {
        if (!isCompactViewport()) this.root.classList.remove("mobile-sidebar-open");
      });
    }

    toggleSidebar(forceState) {
      if (isCompactViewport()) {
        const open = typeof forceState === "boolean" ? forceState : !this.root.classList.contains("mobile-sidebar-open");
        this.root.classList.toggle("mobile-sidebar-open", open);
        return;
      }
      const collapsed = typeof forceState === "boolean" ? forceState : !this.root.classList.contains("sidebar-collapsed");
      this.root.classList.toggle("sidebar-collapsed", collapsed);
    }

    closeMobileSidebar() {
      if (isCompactViewport()) this.root.classList.remove("mobile-sidebar-open");
    }

    startNewChat() {
      this.state.active = null;
      this.state.isDraft = true;
      this.closeMobileSidebar();
      this.clearPendingImages();
      this.renderMessages([]);
      if (this.el.prompt) { this.el.prompt.value = ""; autoGrow(this.el.prompt); }
      this.refreshContext();
      this.el.prompt?.focus();
    }

    refreshContext(forceToast) {
      this.state.context = resolveDeskContext();
      const ctx = this.state.context;
      if (this.el.context) this.el.context.textContent = ctx.label || "General workspace";
      const pathEl = this.root.querySelector('[data-role="workspace-path"]');
      if (pathEl) pathEl.textContent = `${upperWords(ctx.view || "desk")} / ${ctx.doctype || ctx.module || "General"}${ctx.docname ? ` / ${ctx.docname}` : ""}`;
      if (this.el.moduleBadge) this.el.moduleBadge.textContent = ctx.module || "General";
      const suggestions = getSuggestions(ctx);
      if (this.el.suggestions) {
        this.el.suggestions.innerHTML = suggestions.map(s => `<button type="button" class="erp-web-assistant__suggestion" data-prompt="${safeHtml(s.prompt)}"><span>${safeHtml(s.label)}</span></button>`).join("");
        this.el.suggestions.querySelectorAll("[data-prompt]").forEach(b => b.addEventListener("click", () => this.applyPrompt(b.getAttribute("data-prompt") || "", false)));
      }
    }

    applyPrompt(prompt, focusOnly) {
      if (!this.el.prompt) return;
      this.el.prompt.value = String(prompt || "").trim();
      autoGrow(this.el.prompt);
      this.el.prompt.focus();
    }

    handleCopilotPrompt(prompt) { if (prompt) this.applyPrompt(prompt, false); }

    loadHistory() {
      frappe.call({
        method: "erp_ai_assistant.api.chat.list_conversations",
        callback: (r) => {
          this.state.conversations = r.message || [];
          this.renderHistory();
          if (this.el.conversationCount) this.el.conversationCount.textContent = `${this.state.conversations.length} chats`;
          if (!this.state.active && this.state.conversations.length) this.loadConversation(this.state.conversations[0].name);
        },
        error: () => { this.state.conversations = []; this.renderHistory(); },
      });
    }

    renderHistory() {
      const query = (this.el.search?.value || "").toLowerCase();
      const rows = this.state.conversations.filter(r => !query || (r.title || "").toLowerCase().includes(query));
      this.el.history.innerHTML = "";
      if (!rows.length) { this.el.history.innerHTML = '<div class="erp-web-assistant__history-empty">No conversations yet</div>'; return; }

      let lastGroup = "";
      rows.forEach(row => {
        const group = dateGroup(row.modified);
        if (group !== lastGroup) {
          lastGroup = group;
          const label = document.createElement("div");
          label.className = "erp-web-assistant__history-group-label";
          label.textContent = group;
          this.el.history.appendChild(label);
        }
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "erp-web-assistant__history-item";
        if (this.state.active?.name === row.name) btn.classList.add("is-active");
        btn.innerHTML = `<strong>${safeHtml(row.title || "New chat")}</strong><small>${safeHtml((row.modified || "").slice(0, 10))}</small><div class="erp-web-assistant__history-item__actions"><button class="erp-web-assistant__history-action-btn" type="button" data-action="rename" title="Rename">✎</button><button class="erp-web-assistant__history-action-btn" type="button" data-action="delete" title="Delete">×</button></div>`;
        btn.addEventListener("click", (e) => {
          if (e.target.closest('[data-action="rename"]')) { this.renameConversation(row.name, row.title); return; }
          if (e.target.closest('[data-action="delete"]')) { this.deleteConversation(row.name); return; }
          this.loadConversation(row.name);
        });
        this.el.history.appendChild(btn);
      });
    }

    renameConversation(name, currentTitle) {
      const newTitle = window.prompt("Rename conversation:", currentTitle || "");
      if (!newTitle || newTitle === currentTitle) return;
      frappe.call({ method: "erp_ai_assistant.api.chat.rename_conversation", args: { name, title: newTitle }, callback: () => this.loadHistory() });
    }

    deleteConversation(name) {
      if (!window.confirm("Delete this conversation?")) return;
      frappe.call({
        method: "erp_ai_assistant.api.chat.delete_conversation",
        args: { name },
        callback: () => {
          if (this.state.active?.name === name) { this.state.active = null; this.renderMessages([]); }
          this.loadHistory();
        },
      });
    }

    loadConversation(name, options) {
      const settings = options || {};
      frappe.call({
        method: "erp_ai_assistant.api.chat.get_conversation",
        args: { name },
        callback: (r) => {
          const payload = r.message || {};
          this.state.active = payload.conversation || null;
          this.state.isDraft = false;
          if (!settings.silent) this.clearPendingImages();
          const serverMsgs = Array.isArray(payload.messages) ? payload.messages : [];
          this.conversationMessageCache[name] = serverMsgs;
          const opt = this.optimisticAssistantMessages[name] || [];
          let hasMatchingServerReply = false;
          if (opt.length) {
            const last = opt[opt.length - 1];
            hasMatchingServerReply = serverMsgs.some(r => r?.role === "assistant" && String(r.content || "").trim() === String(last.content || "").trim());
            if (hasMatchingServerReply) delete this.optimisticAssistantMessages[name];
          }
          if (!settings.silent) this.closeMobileSidebar();
          const shouldRender = !settings.silent && (!settings.waitForOptimisticConfirmation || hasMatchingServerReply || !opt.length);
          if (shouldRender) {
            this.renderMessages(this.getMergedMessages(name));
            this.renderHistory();
          }
          if (typeof settings.onLoaded === "function") settings.onLoaded(payload, serverMsgs);
        },
      });
    }

    getMergedMessages(name) {
      const server = this.conversationMessageCache[name] || [];
      const opt = this.optimisticAssistantMessages[name] || [];
      return opt.length ? server.concat(opt) : server;
    }

    queueOptimistic(name, text, opts) {
      if (!name || !String(text || "").trim()) return;
      const existing = (this.optimisticAssistantMessages[name] || []).slice();
      const last = existing[existing.length - 1];
      if (last && String(last.content || "").trim() === String(text).trim()) return;
      existing.push({ role: "assistant", content: text, creation: new Date().toISOString(), tool_events: JSON.stringify(opts?.toolEvents || []), attachments_json: opts?.attachments ? JSON.stringify(opts.attachments) : null });
      this.optimisticAssistantMessages[name] = existing;
    }

    renderFinalReply(convName, text, opts) {
      this.queueOptimistic(convName, text, opts);
      const merged = this.getMergedMessages(convName);
      const finalMsg = merged.length ? merged[merged.length - 1] : null;
      const progress = this.el.messages.querySelector(".erp-web-assistant__msg.is-progress");
      if (progress && finalMsg && finalMsg.role === "assistant") {
        this._populateMessageNode(progress, finalMsg, true);
        progress.dataset.fp = this._msgFingerprint(finalMsg, true);
        progress.classList.remove("is-progress", "is-done");
        this.el.messages.scrollTop = this.el.messages.scrollHeight;
        return;
      }
      this.deferProgressRemoval = true;
      this.renderMessages(merged);
    }

    ensureConversation(cb) {
      if (this.state.active?.name) { cb(this.state.active.name); return; }
      frappe.call({ method: "erp_ai_assistant.api.chat.create_conversation", args: { title: this.el.prompt?.value || "" }, callback: (r) => { this.state.active = r.message; this.state.isDraft = false; this.loadHistory(); cb(this.state.active.name); } });
    }

    sendPrompt() {
      const prompt = (this.el.prompt?.value || "").trim();
      const images = this.pendingImages.slice();
      if (!prompt && !images.length) return;
      if (prompt && [" delete ", " cancel ", " submit "].some(w => ` ${prompt.toLowerCase()} `.includes(w)) && !window.confirm("This may change records. Continue?")) return;

      const userContent = prompt || `[Attached ${images.length} image${images.length === 1 ? "" : "s"}]`;
      const userAtt = { attachments: images.map((i, idx) => ({ label: "Image", filename: i.name || `image-${idx + 1}`, file_type: String(i.type || "image").split("/").pop(), file_url: i.dataUrl })) };
      this.pushMessage({ role: "user", content: userContent, creation: new Date().toISOString(), attachments_json: JSON.stringify(userAtt) });
      if (this.el.prompt) { this.el.prompt.value = ""; autoGrow(this.el.prompt); }
      this.clearPendingImages();

      this.ensureConversation((convName) => {
        this.awaitingQueueAck[convName] = true;
        const ctx = this.state.context || resolveDeskContext();
        frappe.call({
          method: "erp_ai_assistant.api.assistant.handle_prompt",
          args: { conversation: convName, prompt, route: ctx.route || "", doctype: ctx.doctype || undefined, docname: ctx.docname || undefined, model: this.el.modelSelect?.value || undefined, images: images.length ? JSON.stringify(images.map(i => ({ name: i.name, type: i.type, data_url: i.dataUrl }))) : undefined },
          callback: (resp) => {
            const p = resp?.message || {};
            logDebug(p);
            if (p.queued) { this.awaitingQueueAck[convName] = false; this.startProgressPolling(convName); return; }
            if (String(p.reply || "").trim()) this.renderFinalReply(convName, p.reply, { toolEvents: p.tool_events, attachments: p.attachments });
            this.finishRun(convName);
          },
          error: (err) => { delete this.awaitingQueueAck[convName]; this.finishRun(null, { keepMessages: true }); this.renderSystemMessage(err.message || "Request failed"); },
        });
      });
    }

    /* ── Retry ─────────────────────────────────────────────────────────── */
    retryLast() {
      if (!this.state.active?.name) return;
      const name = this.state.active.name;
      const msgs = this.getMergedMessages(name);
      // Find last user message
      let lastUserPrompt = "";
      for (let i = msgs.length - 1; i >= 0; i--) {
        if (msgs[i].role === "user") { lastUserPrompt = msgs[i].content || ""; break; }
      }
      if (!lastUserPrompt) return;
      // Remove optimistic assistant messages
      delete this.optimisticAssistantMessages[name];
      // Delete last assistant message from server
      frappe.call({ method: "erp_ai_assistant.api.chat.delete_last_assistant_message", args: { conversation: name }, callback: () => {
        this.el.prompt.value = lastUserPrompt;
        autoGrow(this.el.prompt);
        this.sendPrompt();
      }, error: () => {
        this.el.prompt.value = lastUserPrompt;
        autoGrow(this.el.prompt);
      } });
    }

    /* ── Messages rendering ───────────────────────────────────────────── */
    renderMessages(messages) {
      if (!messages.length) {
        // Only rebuild empty state if it isn't already showing
        if (!this.el.messages.querySelector(".erp-web-assistant__empty")) {
          this.el.messages.innerHTML = "";
          const ctx = this.state.context;
          const cards = getSuggestions(ctx);
          this.el.messages.innerHTML = `
            <div class="erp-web-assistant__empty">
              <div class="erp-web-assistant__empty-icon">AI</div>
              <strong>ERP AI Assistant</strong>
              <span>Ask about your ERP data, create drafts, analyze reports, or get actionable insights — all grounded in live data.</span>
              <div class="erp-web-assistant__empty-grid">
                ${cards.map(c => `<button type="button" class="erp-web-assistant__empty-card" data-prompt="${safeHtml(c.prompt)}"><strong>${safeHtml(c.label)}</strong><small>${safeHtml(c.prompt)}</small></button>`).join("")}
              </div>
            </div>`;
          this.el.messages.querySelectorAll("[data-prompt]").forEach(b => b.addEventListener("click", () => { this.applyPrompt(b.getAttribute("data-prompt")); this.sendPrompt(); }));
        }
        return;
      }

      // Diff-aware update: only add/replace messages that have changed.
      // Key each rendered message by a stable fingerprint so we never wipe
      // messages that are already correctly displayed.
      const existing = Array.from(this.el.messages.querySelectorAll(".erp-web-assistant__msg:not(.is-progress)"));

      // If the existing DOM already has empty-state or count mismatch, do a clean rebuild
      const hasEmptyState = !!this.el.messages.querySelector(".erp-web-assistant__empty");
      if (hasEmptyState) {
        this.el.messages.innerHTML = "";
      }

      messages.forEach((m, i) => {
        const isLast = i === messages.length - 1;
        const fp = this._msgFingerprint(m, isLast);
        const domNode = existing[i];
        if (domNode && domNode.dataset.fp === fp) {
          // Already correct — just make sure it's in the right position
          return;
        }
        if (domNode) {
          this._populateMessageNode(domNode, m, isLast);
          domNode.dataset.fp = fp;
        } else {
          const newNode = this._buildMessageNode(m, isLast);
          newNode.dataset.fp = fp;
          const progress = this.el.messages.querySelector(".erp-web-assistant__msg.is-progress");
          if (progress) this.el.messages.insertBefore(newNode, progress);
          else this.el.messages.appendChild(newNode);
        }
      });

      // Remove any extra trailing nodes (e.g. after a retry removed a message)
      const remaining = Array.from(this.el.messages.querySelectorAll(".erp-web-assistant__msg:not(.is-progress)"));
      while (remaining.length > messages.length) {
        remaining.pop().remove();
      }

      const progress = this.el.messages.querySelector(".erp-web-assistant__msg.is-progress");
      if (progress && this.deferProgressRemoval) {
        this.deferProgressRemoval = false;
        progress.remove();
      }

      this.el.messages.scrollTop = this.el.messages.scrollHeight;
    }

    _msgFingerprint(msg, isLast) {
      // A cheap stable key: role + first 120 chars of content + isLast flag
      // isLast matters because it controls whether the Retry button is shown
      return `${msg.role}|${String(msg.content || "").slice(0, 120)}|${String(msg.tool_events || "").slice(0, 40)}|${String(msg.attachments_json || "").slice(0, 60)}|${isLast ? "1" : "0"}`;
    }

    _buildMessageNode(msg, isLast) {
      const item = document.createElement("div");
      item.className = "erp-web-assistant__msg";
      this._populateMessageNode(item, msg, isLast);
      return item;
    }

    _populateMessageNode(item, msg, isLast) {
      item.className = "erp-web-assistant__msg";
      const isUser = msg.role === "user";
      if (isUser) item.classList.add("is-user");

      let actionsHtml = "";
      if (!isUser) {
        actionsHtml = `<div class="erp-web-assistant__msg-actions"><button class="erp-web-assistant__msg-action-btn" data-action="copy-msg" type="button">⧉ Copy</button>${isLast ? '<button class="erp-web-assistant__msg-action-btn" data-action="retry" type="button">↻ Retry</button>' : ""}</div>`;
      }

      item.innerHTML = `
        ${renderMessageHeader(msg)}
        ${!isUser ? renderToolEvents(msg.tool_events) : ""}
        <div class="erp-web-assistant__msg-body">${renderRichText(msg.content || "")}</div>
        ${renderAttachments(msg.attachments_json)}
        ${actionsHtml}
      `;

      item.querySelectorAll("[data-copilot-prompt]").forEach(b => b.addEventListener("click", () => this.handleCopilotPrompt(b.getAttribute("data-copilot-prompt") || "")));
      item.querySelectorAll("[data-export-toggle]").forEach(b => {
        b.addEventListener("click", () => {
          const eid = b.getAttribute("data-export-toggle") || "";
          const body = item.querySelector(`[data-export-body="${CSS.escape(eid)}"]`);
          if (!body) return;
          const all = JSON.parse(decodeHtml(body.getAttribute("data-all-rows") || "[]"));
          const preview = JSON.parse(decodeHtml(body.getAttribute("data-preview-rows") || "[]"));
          const h = Object.keys((all[0] || preview[0] || {})).slice(0, 12);
          const expanded = b.getAttribute("data-state") === "expanded";
          body.innerHTML = renderDatasetRows(h, expanded ? preview : all);
          b.setAttribute("data-state", expanded ? "collapsed" : "expanded");
          b.textContent = expanded ? "Show all" : "Show preview";
        });
      });
      item.querySelector('[data-action="copy-msg"]')?.addEventListener("click", async (e) => {
        const btn = e.currentTarget;
        try { await navigator.clipboard.writeText(msg.content || ""); btn.textContent = "✓ Copied"; btn.classList.add("is-copied"); setTimeout(() => { btn.textContent = "⧉ Copy"; btn.classList.remove("is-copied"); }, 1500); } catch { btn.textContent = "! Failed"; setTimeout(() => { btn.textContent = "⧉ Copy"; }, 1500); }
      });
      item.querySelector('[data-action="retry"]')?.addEventListener("click", () => this.retryLast());
    }

    pushMessage(msg, isLast) {
      // Remove progress bubble so new message appends after content (not before it)
      const progress = this.el.messages.querySelector(".erp-web-assistant__msg.is-progress");
      const node = this._buildMessageNode(msg, !!isLast);
      node.dataset.fp = this._msgFingerprint(msg, !!isLast);
      if (progress) {
        this.el.messages.insertBefore(node, progress);
      } else {
        this.el.messages.appendChild(node);
      }
      this.el.messages.scrollTop = this.el.messages.scrollHeight;
    }

    retryLast() {
      const conv = this.activeConversation;
      if (!conv || !conv.name) return;
      if (this.isGenerating) return;

      const cache = this.messageCache[conv.name] || [];
      if (!cache.length) return;

      const last = cache[cache.length - 1];
      if (last.role !== "assistant") return;
      
      const prevUser = cache.length > 1 ? cache[cache.length - 2] : null;
      if (!prevUser || prevUser.role !== "user") return;

      this.setGenerating(true);

      frappe.call({
        method: "erp_ai_assistant.api.chat.delete_last_assistant_message",
        args: { conversation: conv.name },
        callback: (r) => {
          if (r.message && r.message.ok) {
            this.messageCache[conv.name].pop();
            this.renderMessages(this.getMergedMessages(conv.name));
            if (this.el.textarea) {
              this.el.textarea.value = prevUser.content || "";
              this.sendPrompt();
            }
          } else {
             this.setGenerating(false);
             frappe.show_alert({ message: "Unable to retry message.", indicator: "orange" });
          }
        },
        error: (err) => {
          this.setGenerating(false);
          frappe.show_alert({ message: err.message || "Failed to retry", indicator: "red" });
        }
      });
    }

    renderSystemMessage(text) {
      this.el.messages.innerHTML = `<div class="erp-web-assistant__empty"><div class="erp-web-assistant__empty-icon">AI</div><strong>ERP AI Assistant</strong><span>${safeHtml(text)}</span></div>`;
    }

    /* ── Model selector ───────────────────────────────────────────────── */
    loadModelOptions() {
      frappe.call({ method: "erp_ai_assistant.api.ai.get_available_models", callback: (r) => this.setModelOptions(r?.message || {}), error: () => this.setModelOptions({}) });
    }

    setModelOptions(payload) {
      const select = this.el.modelSelect; if (!select) return;
      const models = Array.isArray(payload.models) ? payload.models.filter(Boolean) : [];
      const def = payload.default_model || models[0] || "";
      const persisted = localStorage.getItem(this.modelStorageKey);
      const selected = persisted && models.includes(persisted) ? persisted : def;
      select.innerHTML = "";
      models.forEach(m => { const o = document.createElement("option"); o.value = m; o.textContent = m; if (m === selected) o.selected = true; select.appendChild(o); });
      if (!models.length) { const o = document.createElement("option"); o.value = ""; o.textContent = "Default model"; o.selected = true; select.appendChild(o); }
      this.persistSelectedModel();
    }

    persistSelectedModel() { const v = this.el.modelSelect?.value; if (v) localStorage.setItem(this.modelStorageKey, v); }

    /* ── Image handling ───────────────────────────────────────────────── */
    async ingestImageFiles(fileList) {
      for (const file of Array.from(fileList || [])) {
        if (!file || !String(file.type || "").startsWith("image/")) continue;
        if (this.pendingImages.length >= 4) { frappe.show_alert({ message: "Max 4 images", indicator: "orange" }); break; }
        if (file.size > 4 * 1024 * 1024) { frappe.show_alert({ message: `${file.name} exceeds 4MB`, indicator: "orange" }); continue; }
        try {
          const dataUrl = await new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(String(r.result || "")); r.onerror = () => rej(); r.readAsDataURL(file); });
          this.pendingImages.push({ name: file.name || "image", type: file.type || "image/png", size: file.size, dataUrl });
        } catch { frappe.show_alert({ message: "Unable to read image", indicator: "red" }); }
      }
      this.renderImagePreview();
    }

    renderImagePreview() {
      const w = this.el.imagePreview; if (!w) return;
      w.innerHTML = "";
      this.pendingImages.forEach((item, i) => {
        const chip = document.createElement("button"); chip.type = "button"; chip.className = "erp-web-assistant__image-chip"; chip.title = item.name;
        chip.innerHTML = `<img src="${item.dataUrl}" alt="${safeHtml(item.name)}" /><span>${safeHtml(item.name)}</span><strong>×</strong>`;
        chip.addEventListener("click", () => { this.pendingImages.splice(i, 1); this.renderImagePreview(); });
        w.appendChild(chip);
      });
    }

    clearPendingImages() { this.pendingImages = []; this.renderImagePreview(); }

    /* ── Progress / Streaming ─────────────────────────────────────────── */
    startProgressPolling(convName) {
      this.stopProgressPolling();
      this.showProgress(["Preparing response"], "", { stage: "working", done: false });
      if (!convName) return;

      const handler = (data) => {
        if (!data || String(data.conversation || "") !== convName) return;
        const steps = Array.isArray(data.steps) ? data.steps : [];
        const stage = String(data.stage || "").toLowerCase();
        if (!stage && this.awaitingQueueAck[convName]) return;
        this.showProgress(steps, data.partial_text || "", { stage, done: !!data.done });
        if (data.done) {
          if (!this.tryBeginCompletionFetch(convName)) return;
          this.stopProgressPolling({ keepVisible: true });
          if (data.error) { this.clearCompletionFetchLock(convName); this.finishRun(null, { keepMessages: true }); this.renderSystemMessage(data.error); return; }
          frappe.call({ method: "erp_ai_assistant.api.ai.get_prompt_result", args: { conversation: convName }, callback: (r) => {
            const res = r?.message || {};
            logDebug(res);
            if (res.done && String(res.reply || "").trim()) this.renderFinalReply(convName, res.reply, { toolEvents: res.tool_events, attachments: res.attachments });
            this.finishRun(convName);
          }, error: () => { this.clearCompletionFetchLock(convName); this.finishRun(convName); } });
        }
      };

      this._rtHandler = handler;
      try { frappe.realtime.on("erp_ai_progress", handler); } catch { /* polling covers it */ }

      const poll = () => {
        if (this.progressPollPending) return;
        this.progressPollPending = true;
        frappe.call({
          method: "erp_ai_assistant.api.ai.get_prompt_progress",
          args: { conversation: convName },
          callback: (r) => {
            const p = r?.message || {};
            const steps = Array.isArray(p.steps) ? p.steps : [];
            const stage = String(p.stage || "").toLowerCase();
            if (!stage && this.awaitingQueueAck[convName]) return;
            this.showProgress(steps, p.partial_text || "", { stage, done: !!p.done });
            if (p.done) {
              if (!this.tryBeginCompletionFetch(convName)) return;
              this.stopProgressPolling({ keepVisible: true });
              if (p.error) { this.clearCompletionFetchLock(convName); this.finishRun(null, { keepMessages: true }); this.renderSystemMessage(p.error); return; }
              frappe.call({ method: "erp_ai_assistant.api.ai.get_prompt_result", args: { conversation: convName }, callback: (r2) => {
                const res = r2?.message || {};
                if (res.done && String(res.reply || "").trim()) this.renderFinalReply(convName, res.reply, { toolEvents: res.tool_events, attachments: res.attachments });
                this.finishRun(convName);
              }, error: () => { this.clearCompletionFetchLock(convName); this.finishRun(convName); } });
            }
          },
          always: () => { this.progressPollPending = false; },
        });
      };
      poll();
      this.progressPollTimer = setInterval(poll, 800);
    }

    stopProgressPolling(opts) {
      const keepVisible = !!opts?.keepVisible;
      if (this.progressPollTimer) { clearInterval(this.progressPollTimer); this.progressPollTimer = null; }
      this.progressPollPending = false;
      if (this._rtHandler) { try { frappe.realtime.off("erp_ai_progress", this._rtHandler); } catch { /* ignore */ } this._rtHandler = null; }
      if (!keepVisible) this.hideProgress();
    }

    showProgress(steps, partialText, opts) {
      const done = !!opts?.done;
      const existing = this.el.messages.querySelector(".erp-web-assistant__msg.is-progress");
      if (existing) {
        const label = existing.querySelector(".erp-web-assistant__thinking-label");
        const stepWrap = existing.querySelector(".erp-web-assistant__thinking-steps");
        const partial = existing.querySelector(".erp-web-assistant__progress-partial");
        const spinner = existing.querySelector(".erp-web-assistant__thinking-spinner");
        if (label) label.textContent = summarizeSteps(steps, done);
        existing.classList.toggle("is-done", done);
        if (spinner) spinner.style.display = done ? "none" : "inline-block";
        if (stepWrap) {
          const items = (Array.isArray(steps) ? steps.filter(Boolean) : []).slice(-6);
          const final = items.length ? items : ["Preparing response"];
          stepWrap.innerHTML = final.map((s, i) => `<div class="erp-web-assistant__thinking-step"><span class="erp-web-assistant__thinking-step-icon">${done || i < final.length - 1 ? "✓" : "⟳"}</span><span>${safeHtml(mapStepLabel(s))}</span></div>`).join("");
        }
        if (partial) { partial.textContent = String(partialText || "").trim(); partial.style.display = partial.textContent ? "block" : "none"; }
        return;
      }
      const progress = document.createElement("div");
      progress.className = "erp-web-assistant__msg is-progress";
      progress.innerHTML = `
        <div class="erp-web-assistant__message-head"><div class="erp-web-assistant__msg-meta"><span class="erp-web-assistant__msg-role">Assistant</span></div></div>
        <div class="erp-web-assistant__progress-partial" style="display:none;"></div>
        <details class="erp-web-assistant__thinking" open>
          <summary class="erp-web-assistant__thinking-head">
            <span class="erp-web-assistant__thinking-spinner"></span>
            <span class="erp-web-assistant__thinking-label">Working...</span>
          </summary>
          <div class="erp-web-assistant__thinking-steps"></div>
        </details>`;
      this.el.messages.appendChild(progress);
      this.showProgress(steps, partialText, opts);
      this.el.messages.scrollTop = this.el.messages.scrollHeight;
    }

    hideProgress() { this.el.messages.querySelector(".erp-web-assistant__msg.is-progress")?.remove(); }

    tryBeginCompletionFetch(convName) {
      if (!convName) return false;
      if (this.completionFetchLocks[convName]) return false;
      this.completionFetchLocks[convName] = true;
      return true;
    }

    clearCompletionFetchLock(convName) {
      if (!convName) return;
      delete this.completionFetchLocks[convName];
    }

    hasPendingOptimisticReply(convName) {
      const opt = this.optimisticAssistantMessages[convName] || [];
      return !!opt.length && !!String(opt[opt.length - 1]?.content || "").trim();
    }

    finalizeRun(convName, opts) {
      this.stopProgressPolling(opts);
      if (convName) {
        this.clearCompletionFetchLock(convName);
        delete this.awaitingQueueAck[convName];
        this.loadHistory();
        return;
      }
      Object.keys(this.completionFetchLocks).forEach((key) => delete this.completionFetchLocks[key]);
      if (!opts?.keepMessages) this.loadHistory();
    }

    /* ── Completion ───────────────────────────────────────────────────── */
    loadConversationAfterCompletion(name, attempt) {
      if (!name) return;
      const tryCount = Number(attempt || 0);
      clearTimeout(this.completionReloadTimers[name]);
      this.loadConversation(name, {
        silent: true,
        waitForOptimisticConfirmation: true,
        onLoaded: (_, serverMsgs) => {
          const opt = this.optimisticAssistantMessages[name] || [];
          const expected = opt.length ? String(opt[opt.length - 1]?.content || "").trim() : "";
          const latestServerReply = serverMsgs.length && serverMsgs[serverMsgs.length - 1]?.role === "assistant";
          const exactReplyMatch = expected ? serverMsgs.some(r => r?.role === "assistant" && String(r.content || "").trim() === expected) : false;
          const hasReply = exactReplyMatch || !!latestServerReply;
          if (hasReply || !expected) {
            if (hasReply) delete this.optimisticAssistantMessages[name];
            this.deferProgressRemoval = true;
            this.renderMessages(this.getMergedMessages(name));
            this.renderHistory();
            this.finalizeRun(name);
            delete this.completionReloadTimers[name];
          }
          if (!hasReply && tryCount < 8) {
            this.completionReloadTimers[name] = setTimeout(() => this.loadConversationAfterCompletion(name, tryCount + 1), 1200);
            return;
          }
          if (!hasReply) this.finalizeRun(name);
        },
      });
    }

    finishRun(convName, opts) {
      if (convName) {
        if (this.hasPendingOptimisticReply(convName)) {
          this.finalizeRun(convName);
        } else {
          this.stopProgressPolling({ keepVisible: true });
        }
        this.loadConversationAfterCompletion(convName, 0);
        return;
      }
      this.finalizeRun(null, opts);
    }
  }

  window.ERPWebAssistant = ERPWebAssistant;
  window.addEventListener("load", function () {
    const root = document.getElementById("erp-web-assistant");
    if (!root || root.dataset.booted === "1") return;
    root.dataset.booted = "1";
    new ERPWebAssistant(root).boot();
  });
})();
