(function () {
  window.__ERP_AI_WEB_ASSISTANT_BUILD__ = "2026-03-24-response-render-fix-v2";

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
      return parsed.pathname.split("/").filter(Boolean).pop() || "Image";
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
    if (!raw) return { attachments: [], exports: {}, copilot: null };
    try {
      const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
      if (Array.isArray(parsed)) return { attachments: parsed.filter((item) => item && item.file_url), exports: {}, copilot: null };
      if (parsed && typeof parsed === "object") {
        return {
          attachments: Array.isArray(parsed.attachments) ? parsed.attachments.filter((item) => item && item.file_url) : [],
          exports: parsed.exports && typeof parsed.exports === "object" ? parsed.exports : {},
          copilot: parsed.copilot && typeof parsed.copilot === "object" ? parsed.copilot : null,
        };
      }
    } catch (error) {
      return { attachments: [], exports: {}, copilot: null };
    }
    return { attachments: [], exports: {}, copilot: null };
  }

  function renderDatasetRows(headers, items) {
    return (Array.isArray(items) ? items : []).map((row) => `<tr>${headers.map((header) => `<td>${renderInline(row && row[header] == null ? "" : String((row || {})[header] ?? ""))}</td>`).join("")}</tr>`).join("");
  }

  function renderAttachmentPreview(rows, exportId) {
    const allRows = Array.isArray(rows) ? rows.filter((row) => row && typeof row === "object") : [];
    if (!allRows.length) return "";
    const headers = Object.keys(allRows[0]).slice(0, 12);
    if (!headers.length) return "";
    const previewRows = allRows.slice(0, 25);
    const renderRows = (items) => renderDatasetRows(headers, items);
    return `
      <div class="erp-web-assistant__attachment-preview">
        <div class="erp-web-assistant__dataset-toolbar">
          <strong>${allRows.length} row${allRows.length === 1 ? "" : "s"}</strong>
          ${allRows.length > previewRows.length ? `<button type="button" class="erp-web-assistant__mini-btn" data-export-toggle="${safeHtml(exportId || "")}" data-state="collapsed">Show all rows</button>` : ""}
        </div>
        <div class="erp-web-assistant__table-wrap">
          <table class="erp-web-assistant__table">
            <thead><tr>${headers.map((header) => `<th>${renderInline(header)}</th>`).join("")}</tr></thead>
            <tbody data-export-body="${safeHtml(exportId || "")}" data-preview-rows='${safeHtml(JSON.stringify(previewRows))}' data-all-rows='${safeHtml(JSON.stringify(allRows))}'>
              ${renderRows(previewRows)}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }

  function renderExportsOnly(exports, opts) {
    const entries = Object.entries(exports || {});
    const showInline = !!(opts && opts.showInline);
    if (!entries.length || !showInline) return "";
    return `<div class="erp-web-assistant__attachments">${entries.map(([exportId, entry]) => {
      const title = safeHtml(entry.title || "Data preview");
      const preview = Array.isArray(entry.rows) ? renderAttachmentPreview(entry.rows, exportId) : "";
      return `<div class="erp-web-assistant__attachment erp-web-assistant__attachment--dataset"><span class="erp-web-assistant__attachment-meta"><strong>${title}</strong><small>Full response dataset</small></span>${preview}</div>`;
    }).join("")}</div>`;
  }

  function renderCopilotBlock(copilot) {
    if (!copilot || typeof copilot !== "object") return "";
    const summary = copilot.summary && typeof copilot.summary === "object" ? copilot.summary : null;
    const actions = Array.isArray(copilot.actions) ? copilot.actions.filter((item) => item && item.label && item.prompt) : [];
    const issues = Array.isArray(copilot.issues) ? copilot.issues.filter(Boolean) : [];
    const insights = Array.isArray(copilot.insights) ? copilot.insights.filter(Boolean) : [];
    const suggestions = Array.isArray(copilot.suggestions) ? copilot.suggestions.filter(Boolean) : [];
    const summaryRows = Array.isArray(summary?.rows) ? summary.rows.filter((item) => item && item.label) : [];
    return `
      <section class="erp-web-assistant__copilot">
        ${summary ? `<div class="erp-web-assistant__copilot-card"><div class="erp-web-assistant__copilot-head"><strong>${safeHtml(summary.title || "ERP Copilot")}</strong>${summary.badge ? `<span class="erp-web-assistant__copilot-badge">${safeHtml(summary.badge)}</span>` : ""}</div><div class="erp-web-assistant__copilot-grid">${summaryRows.map((row) => `<div class="erp-web-assistant__copilot-kv"><span>${safeHtml(row.label)}</span><strong>${safeHtml(row.value || "—")}</strong></div>`).join("")}</div></div>` : ""}
        ${issues.length ? `<div class="erp-web-assistant__copilot-card"><div class="erp-web-assistant__copilot-title">Key issues</div><ul class="erp-web-assistant__copilot-list">${issues.map((item) => `<li>${renderInline(item)}</li>`).join("")}</ul></div>` : ""}
        ${actions.length ? `<div class="erp-web-assistant__copilot-card"><div class="erp-web-assistant__copilot-title">Recommended actions</div><div class="erp-web-assistant__copilot-actions">${actions.map((item) => `<button type="button" class="erp-web-assistant__action-btn ${item.style === "primary" ? "is-primary" : ""}" data-copilot-prompt="${safeHtml(item.prompt)}">${safeHtml(item.label)}</button>`).join("")}</div></div>` : ""}
        ${insights.length ? `<div class="erp-web-assistant__copilot-card"><div class="erp-web-assistant__copilot-title">Insights</div><div class="erp-web-assistant__copilot-notes">${insights.map((item) => `<p>${renderInline(item)}</p>`).join("")}</div></div>` : ""}
        ${suggestions.length ? `<div class="erp-web-assistant__copilot-card"><div class="erp-web-assistant__copilot-title">Try next</div><div class="erp-web-assistant__copilot-suggestions">${suggestions.map((item) => `<button type="button" class="erp-web-assistant__mini-btn" data-copilot-prompt="${safeHtml(item)}">${safeHtml(item)}</button>`).join("")}</div></div>` : ""}
      </section>`;
  }

  function renderAttachments(raw) {
    const pkg = parseAttachmentPackage(raw);
    const attachments = pkg.attachments;
    const exports = pkg.exports || {};
    const copilot = renderCopilotBlock(pkg.copilot);
    const shouldShowInlineExports = !copilot && !!attachments.length;
    if (!attachments.length) return `${copilot}`;
    return `${copilot}<div class="erp-web-assistant__attachments">${attachments.map((item) => {
      const name = safeHtml(item.filename || "download");
      const label = safeHtml(item.label || item.file_type || "File");
      const url = safeHtml(item.file_url);
      const preview = exports[item.export_id] && Array.isArray(exports[item.export_id].rows)
        ? renderAttachmentPreview(exports[item.export_id].rows, item.export_id)
        : "";
      const actions = `<span class="erp-web-assistant__attachment-actions"><a class="erp-web-assistant__link" href="${url}" download="${name}">Download</a></span>`;
      if (isImageUrl(item.file_url)) {
        return `
          <div class="erp-web-assistant__attachment is-image">
            <img class="erp-web-assistant__attachment-image" src="${url}" alt="${name}" loading="lazy" />
            <span class="erp-web-assistant__attachment-meta"><strong>${name}</strong><small>${label}</small></span>
            ${actions}
          </div>`;
      }
      return `<div class="erp-web-assistant__attachment"><span class="erp-web-assistant__attachment-meta"><strong>${name}</strong><small>${label}</small></span>${actions}${preview}</div>`;
    }).join("")}${renderExportsOnly(exports, { showInline: shouldShowInlineExports })}</div>`;
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

  function parseToolEventString(value) {
    const text = String(value || "").trim();
    if (!text) return null;
    const match = text.match(/^([a-zA-Z0-9_]+)\s*(.*)$/);
    if (!match) return { label: text, countable: false };
    const toolName = match[1];
    const argsText = String(match[2] || "").trim();
    const labelMap = {
      list_documents: "Fetched ERP records",
      get_doctype_info: "Read document structure",
      create_document: "Created draft",
      update_document: "Updated record",
      run_python_code: "Ran bulk operation",
      generate_report: "Generated report",
      submit_document: "Submitted document",
      run_workflow: "Ran workflow action",
      search_link: "Searched linked records",
    };
    let suffix = "";
    const doctypeMatch = argsText.match(/['"]doctype['"]\s*:\s*['"]([^'"]+)['"]/i);
    const reportMatch = argsText.match(/['"]report_name['"]\s*:\s*['"]([^'"]+)['"]/i);
    if (doctypeMatch?.[1]) suffix = doctypeMatch[1];
    else if (reportMatch?.[1]) suffix = reportMatch[1];
    const baseLabel = labelMap[toolName] || toolName.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
    return { label: suffix ? `${baseLabel} — ${suffix}` : baseLabel, countable: Object.prototype.hasOwnProperty.call(labelMap, toolName) };
  }

  function mapStoredToolEvents(raw) {
    return parseToolEvents(raw)
      .filter((item) => typeof item === "string" && String(item || "").trim())
      .map((item) => parseToolEventString(item))
      .filter(Boolean);
  }

  function renderToolEvents(raw) {
    if (!shouldShowToolActivity()) return "";
    const steps = mapStoredToolEvents(raw);
    if (!steps.length) return "";
    const count = steps.filter((item) => item.countable).length || steps.length;
    return `
      <details class="erp-web-assistant__activity-log">
        <summary class="erp-web-assistant__activity-head">
          <span class="erp-web-assistant__activity-status">✓</span>
          <span class="erp-web-assistant__activity-summary">Ran ${count} command${count === 1 ? "" : "s"}</span>
        </summary>
        <div class="erp-web-assistant__activity-steps">
          ${steps.map((item) => `<div class="erp-web-assistant__activity-step"><span class="erp-web-assistant__activity-step-icon">✓</span><span class="erp-web-assistant__activity-step-label">${safeHtml(item.label)}</span></div>`).join("")}
        </div>
      </details>`;
  }

  function mapProgressStepLabel(step) {
    const text = String(step || "").trim();
    const lowered = text.toLowerCase();
    if (!text) return "Working...";
    if (lowered === "preparing request" || lowered === "preparing response") return "Starting...";
    if (lowered === "thinking") return "Thinking...";
    if (lowered === "verifying erp evidence") return "Verifying ERP results...";
    if (lowered === "response ready") return "Ready";
    if (lowered.startsWith("tool failed:")) return `Failed: ${text.replace(/^tool failed:\s*/i, "")}`;
    if (lowered.startsWith("tool:")) return parseToolEventString(text.replace(/^tool:\s*/i, ""))?.label || text.replace(/^tool:\s*/i, "");
    return text;
  }

  function summarizeProgressSteps(steps, done) {
    const items = Array.isArray(steps) ? steps.filter(Boolean) : [];
    const commandCount = items.filter((item) => /^tool:/i.test(String(item || "").trim())).length;
    if (commandCount) return `${done ? "Completed" : "Working on"} ${commandCount} ERP step${commandCount === 1 ? "" : "s"}${done ? "" : "..."}`;
    const stepCount = items.length || 1;
    return `${done ? "Completed" : "Running"} ${stepCount} step${stepCount === 1 ? "" : "s"}${done ? "" : "..."}`;
  }

  function upperWords(value) {
    return String(value || "").replace(/[_-]+/g, " ").replace(/\b\w/g, (char) => char.toUpperCase()).trim();
  }

  function firstValue(values) {
    return values.find((value) => value !== null && value !== undefined && String(value).trim()) || null;
  }

  const MODULE_KEYWORDS = [
    { name: "Sales", keywords: ["quotation", "sales order", "sales invoice", "lead", "opportunity", "customer"] },
    { name: "Buying", keywords: ["purchase", "supplier", "material request", "request for quotation"] },
    { name: "Stock", keywords: ["item", "warehouse", "stock", "delivery note", "batch", "serial"] },
    { name: "Accounts", keywords: ["payment", "journal", "invoice", "account", "expense", "general ledger"] },
    { name: "HR", keywords: ["employee", "attendance", "leave", "payroll", "expense claim"] },
    { name: "Projects", keywords: ["project", "task", "timesheet", "issue"] },
    { name: "Support", keywords: ["ticket", "support", "case"] },
  ];

  function inferModuleFromText(text) {
    const lowered = String(text || "").toLowerCase();
    for (const item of MODULE_KEYWORDS) {
      if (item.keywords.some((keyword) => lowered.includes(keyword))) return item.name;
    }
    return "General";
  }

  function resolveDeskContext() {
    let rawRoute = [];
    try {
      rawRoute = window.frappe?.get_route ? frappe.get_route() : [];
    } catch (error) {
      rawRoute = [];
    }

    let route = [];
    if (Array.isArray(rawRoute)) {
      route = rawRoute.filter((segment) => segment !== null && segment !== undefined).map((segment) => String(segment));
    } else if (typeof rawRoute === "string") {
      route = rawRoute.split("/").filter(Boolean);
    }
    if (!route.length && typeof window.location?.hash === "string" && window.location.hash) {
      route = window.location.hash.replace(/^#/, "").split("/").filter(Boolean);
    }

    const routeText = route.join("/");
    const [rawView, doctype, docname, extra] = route;
    const view = String(rawView || "").trim().toLowerCase();
    const form = window.cur_frm || (typeof cur_frm !== "undefined" ? cur_frm : null);
    const isFormRoute = view === "form";
    const formMatches = Boolean(form && form.doc && isFormRoute && String(form.doctype || "") === String(doctype || "") && String(form.docname || "") === String(docname || ""));

    if (formMatches) {
      return {
        mode: "record",
        view: "form",
        doctype: form.doctype || null,
        docname: form.docname || null,
        route: routeText,
        label: `${form.doctype || "Document"} / ${form.docname || ""}`.trim(),
        module: inferModuleFromText(form.doctype),
      };
    }
    if (view === "list") {
      return { mode: "list", view, doctype: doctype || null, docname: null, route: routeText, label: `${doctype || "List"} List`, module: inferModuleFromText(doctype || extra) };
    }
    if (view === "report") {
      return { mode: "report", view, doctype: doctype || null, docname: null, route: routeText, label: doctype ? `Report / ${doctype}` : "Report", module: inferModuleFromText(doctype || extra) };
    }
    if (view === "workspace") {
      return { mode: "workspace", view, doctype: null, docname: null, route: routeText, label: doctype ? `Workspace / ${doctype}` : "Workspace", module: inferModuleFromText(doctype || extra) };
    }
    return { mode: "general", view, doctype: isFormRoute ? doctype || null : null, docname: isFormRoute ? docname || null : null, route: routeText, label: routeText || "General workspace", module: inferModuleFromText(firstValue([doctype, extra, routeText])) };
  }

  function promptTemplate(text, context) {
    const doctype = context?.doctype || "document";
    const docname = context?.docname ? ` ${context.docname}` : "";
    const target = `${doctype}${docname}`.trim();
    if (text === "summarize") return context?.docname ? `Summarize ${target} and highlight what needs attention today.` : `Summarize what needs attention in this ${context?.label || "area"}.`;
    if (text === "explain") return context?.docname ? `Explain the current status of ${target} and what is blocking the next step.` : `Explain the current status and next steps for this ${context?.label || "area"}.`;
    if (text === "draft") return context?.docname ? `Create the safest draft action based on ${target}. Do not submit anything yet.` : `Create the safest draft action for this context. Do not submit anything yet.`;
    if (text === "followups") return context?.docname ? `List the top follow-up actions for ${target} for today's work.` : "List the top follow-up actions for today's work in this context.";
    return text;
  }

  function getSuggestionSet(context) {
    const base = [
      { label: "Summarize this", prompt: promptTemplate("summarize", context) },
      { label: "Explain status", prompt: promptTemplate("explain", context) },
      { label: "Safe next step", prompt: promptTemplate("draft", context) },
      { label: "Follow-ups", prompt: promptTemplate("followups", context) },
    ];

    const moduleMap = {
      Sales: [
        { label: "Overdue invoices", prompt: "Show overdue sales invoices and suggest follow-up actions." },
        { label: "Create quotation draft", prompt: "Create a draft quotation from the current sales context. Do not submit." },
      ],
      Buying: [
        { label: "Purchase draft", prompt: "Create a draft purchase document from the current context. Do not submit." },
        { label: "Supplier status", prompt: "Summarize supplier activity and pending actions for this context." },
      ],
      Stock: [
        { label: "Low stock", prompt: "Show items below reorder level relevant to this context." },
        { label: "Material request draft", prompt: "Create a draft material request for low-stock items only. Do not submit." },
      ],
      Accounts: [
        { label: "Due payments", prompt: "Show payments due soon and explain urgency." },
        { label: "Reconcile issue", prompt: "Explain what is preventing reconciliation or payment completion here." },
      ],
      HR: [
        { label: "Attendance summary", prompt: "Summarize today's attendance and highlight exceptions." },
        { label: "Leave check", prompt: "Show pending leave requests and recommend next actions." },
      ],
      Projects: [
        { label: "Project blockers", prompt: "Summarize blockers and pending work for this project context." },
        { label: "Task draft", prompt: "Create safe draft follow-up tasks based on this project context." },
      ],
      Support: [
        { label: "Open issues", prompt: "Show open support issues that need attention first." },
        { label: "Response draft", prompt: "Draft a helpful response based on the current issue context." },
      ],
      General: [
        { label: "Today's priorities", prompt: "Summarize today's ERP priorities for me." },
        { label: "Open approvals", prompt: "Show my pending approvals and what needs action." },
      ],
    };

    return base.concat(moduleMap[context?.module || "General"] || moduleMap.General).slice(0, 6);
  }

  class ERPWebAssistant {
    constructor(root, options) {
      this.root = root;
      this.options = options || {};
      this.state = { conversations: [], active: null, isDraft: false, context: resolveDeskContext() };
      this.awaitingQueueAck = {};
      this.conversationMessageCache = {};
      this.optimisticAssistantMessages = {};
      this.completionReloadTimers = {};
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
        contextMeta: root.querySelector('[data-role="context-meta"]'),
        quickActions: root.querySelector('[data-role="quick-actions"]'),
        suggestions: root.querySelector('[data-role="suggestions"]'),
        status: root.querySelector('[data-role="status"]'),
        moduleBadge: root.querySelector('[data-role="module-badge"]'),
        conversationCount: root.querySelector('[data-role="conversation-count"]'),
        send: root.querySelector('[data-action="send"]'),
        attachImage: root.querySelector('[data-action="attach-image"]'),
        newChat: root.querySelector('[data-action="new-chat"]'),
        focusPrompt: root.querySelector('[data-action="focus-prompt"]'),
        refreshContext: root.querySelector('[data-action="refresh-context"]'),
      };
    }

    boot() {
      if (!window.frappe || !frappe.call) {
        this.renderSystemMessage("Frappe runtime not found.");
        return;
      }
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
      this.el.focusPrompt?.addEventListener("click", () => this.el.prompt?.focus());
      this.el.refreshContext?.addEventListener("click", () => this.refreshContext(true));
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
      window.addEventListener("hashchange", () => this.refreshContext());
      document.addEventListener("visibilitychange", () => {
        if (!document.hidden) this.refreshContext();
      });
      $(document).on("page-change", () => this.refreshContext());
    }

    startNewChat() {
      this.state.active = null;
      this.state.isDraft = true;
      this.clearPendingImages();
      this.renderMessages([]);
      if (this.el.prompt) this.el.prompt.value = "";
      this.updateStatus("New conversation ready.");
      this.refreshContext();
      this.el.prompt?.focus();
    }

    refreshContext(forceToast) {
      this.state.context = resolveDeskContext();
      const context = this.state.context;
      if (this.el.context) this.el.context.textContent = context.label || "General workspace";
      const pathEl = this.root.querySelector('[data-role="workspace-path"]');
      if (pathEl) pathEl.textContent = `${upperWords(context.view || "desk")} / ${context.doctype || context.module || "General"}${context.docname ? ` / ${context.docname}` : ""}`;
      if (this.el.moduleBadge) this.el.moduleBadge.textContent = context.module || "General";
      if (this.el.contextMeta) {
        const items = [
          { label: "Mode", value: upperWords(context.mode || "general") },
          { label: "Module", value: context.module || "General" },
          { label: "DocType", value: context.doctype || "—" },
          { label: "Document", value: context.docname || "—" },
        ];
        this.el.contextMeta.innerHTML = items.map((item) => `<div class="erp-web-assistant__context-card"><span>${safeHtml(item.label)}</span><strong>${safeHtml(item.value)}</strong></div>`).join("");
      }
      const suggestions = getSuggestionSet(context);
      if (this.el.quickActions) {
        this.el.quickActions.innerHTML = suggestions.slice(0, 6).map((item) => `<button type="button" class="erp-web-assistant__chip" data-prompt="${safeHtml(item.prompt)}">${safeHtml(item.label)}</button>`).join("");
        Array.from(this.el.quickActions.querySelectorAll("[data-prompt]")).forEach((button) => {
          button.addEventListener("click", () => this.applyPrompt(button.getAttribute("data-prompt") || "", true));
        });
      }
      if (this.el.suggestions) {
        this.el.suggestions.innerHTML = suggestions.map((item) => `<button type="button" class="erp-web-assistant__suggestion" data-prompt="${safeHtml(item.prompt)}"><span>${safeHtml(item.label)}</span><small>${safeHtml(item.prompt)}</small></button>`).join("");
        Array.from(this.el.suggestions.querySelectorAll("[data-prompt]")).forEach((button) => {
          button.addEventListener("click", () => this.applyPrompt(button.getAttribute("data-prompt") || "", false));
        });
      }
      this.updateStatus(context.docname ? `Working with ${context.doctype} ${context.docname}.` : `Working in ${context.module || "General"} context.`);
      if (forceToast && window.frappe?.show_alert && this.options.mode === "desk") {
        frappe.show_alert({ message: `Context refreshed: ${context.label || "General workspace"}`, indicator: "blue" });
      }
    }

    applyPrompt(prompt, focusOnly) {
      if (!this.el.prompt) return;
      this.el.prompt.value = String(prompt || "").trim();
      this.el.prompt.focus();
      this.el.prompt.selectionStart = this.el.prompt.selectionEnd = this.el.prompt.value.length;
      if (!focusOnly) this.updateStatus("Prompt loaded. Review it, then send.");
    }

    handleCopilotPrompt(prompt, autoSend) {
      if (!prompt) return;
      this.applyPrompt(prompt, false);
      if (autoSend) this.sendPrompt();
    }

    updateStatus(text) {
      if (this.el.status) this.el.status.textContent = text;
    }

    loadHistory() {
      frappe.call({
        method: "erp_ai_assistant.api.chat.list_conversations",
        callback: (r) => {
          this.state.conversations = r.message || [];
          this.renderHistory();
          if (this.el.conversationCount) {
            const count = this.state.conversations.length;
            this.el.conversationCount.textContent = `${count} chat${count === 1 ? "" : "s"}`;
          }
          if (!this.state.active && this.state.conversations.length) this.loadConversation(this.state.conversations[0].name);
        },
        error: () => {
          this.state.conversations = [];
          this.renderHistory();
          if (this.el.conversationCount) this.el.conversationCount.textContent = "0 chats";
        },
      });
    }

    renderHistory() {
      const query = (this.el.search?.value || "").toLowerCase();
      const rows = this.state.conversations.filter((row) => !query || String(row.title || "").toLowerCase().includes(query));
      this.el.history.innerHTML = "";
      if (!rows.length) {
        this.el.history.innerHTML = '<div class="erp-web-assistant__history-empty">No conversations yet</div>';
        return;
      }
      rows.forEach((row) => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "erp-web-assistant__history-item";
        if (this.state.active && this.state.active.name === row.name) btn.classList.add("is-active");
        btn.innerHTML = `<strong>${safeHtml(row.title || "New chat")}</strong><small>${safeHtml(row.modified || "")}</small>`;
        btn.addEventListener("click", () => this.loadConversation(row.name));
        this.el.history.appendChild(btn);
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
          this.clearPendingImages();
          const serverMessages = Array.isArray(payload.messages) ? payload.messages : [];
          this.conversationMessageCache[name] = serverMessages;
          const optimisticMessages = Array.isArray(this.optimisticAssistantMessages[name])
            ? this.optimisticAssistantMessages[name]
            : [];
          if (optimisticMessages.length) {
            const lastOptimistic = optimisticMessages[optimisticMessages.length - 1];
            const hasMatchingServerReply = serverMessages.some((row) =>
              row
              && row.role === "assistant"
              && String(row.content || "").trim() === String(lastOptimistic.content || "").trim()
            );
            if (hasMatchingServerReply) {
              delete this.optimisticAssistantMessages[name];
            }
          }
          this.renderMessages(this.getConversationMessages(name));
          this.renderHistory();
          if (typeof settings.onLoaded === "function") {
            settings.onLoaded(payload, serverMessages);
          }
        },
      });
    }

    getConversationMessages(name) {
      const serverMessages = Array.isArray(this.conversationMessageCache[name])
        ? this.conversationMessageCache[name]
        : [];
      const optimisticAssistantMessages = Array.isArray(this.optimisticAssistantMessages[name])
        ? this.optimisticAssistantMessages[name]
        : [];
      if (!optimisticAssistantMessages.length) {
        return serverMessages;
      }
      return serverMessages.concat(optimisticAssistantMessages);
    }

    queueOptimisticAssistantMessage(conversationName, replyText) {
      const content = String(replyText || "").trim();
      if (!conversationName || !content) return;
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
        tool_events: "[]",
        attachments_json: null,
      });
      this.optimisticAssistantMessages[conversationName] = existing;
    }

    ensureConversation(callback) {
      if (this.state.active && this.state.active.name) {
        callback(this.state.active.name);
        return;
      }
      const title = (this.el.prompt?.value || "").trim();
      frappe.call({
        method: "erp_ai_assistant.api.chat.create_conversation",
        args: { title },
        callback: (r) => {
          this.state.active = r.message;
          this.state.isDraft = false;
          this.loadHistory();
          callback(this.state.active.name);
        },
      });
    }

    shouldConfirmPrompt(prompt) {
      const text = String(prompt || "").toLowerCase();
      return [" delete ", " cancel ", " submit ", " amend "].some((word) => ` ${text} `.includes(word));
    }

    sendPrompt() {
      const prompt = (this.el.prompt?.value || "").trim();
      const images = this.pendingImages.slice();
      if (!prompt && !images.length) return;
      if (prompt && this.shouldConfirmPrompt(prompt) && !window.confirm("This prompt looks like it could change records. Continue?")) {
        return;
      }

      const userContent = prompt || `[Attached ${images.length} image${images.length === 1 ? "" : "s"}]`;
      const userAttachments = { attachments: images.map((item, index) => ({ label: "Image", filename: item.name || `image-${index + 1}`, file_type: String(item.type || "image").split("/").pop(), file_url: item.dataUrl })) };
      this.pushMessage({ role: "user", content: userContent, creation: new Date().toISOString(), attachments_json: JSON.stringify(userAttachments) });
      if (this.el.prompt) this.el.prompt.value = "";
      this.clearPendingImages();
      this.updateStatus("Sending request...");

      this.ensureConversation((conversationName) => {
        this.awaitingQueueAck[conversationName] = true;
        const context = this.state.context || resolveDeskContext();
        frappe.call({
          method: "erp_ai_assistant.api.assistant.handle_prompt",
          args: {
            conversation: conversationName,
            prompt,
            route: context.route || (window.location.hash ? window.location.hash.slice(1) : ""),
            doctype: context.doctype || undefined,
            docname: context.docname || undefined,
            model: this.el.modelSelect?.value || undefined,
            images: images.length ? JSON.stringify(images.map((item) => ({ name: item.name, type: item.type, data_url: item.dataUrl }))) : undefined,
          },
          callback: (response) => {
            const payload = response?.message || {};
            logAssistantDebug(payload);
            if (payload.queued) {
              this.awaitingQueueAck[conversationName] = false;
              this.startProgressPolling(conversationName);
              return;
            }
            if (String(payload.reply || "").trim()) {
              this.queueOptimisticAssistantMessage(conversationName, payload.reply);
              this.renderMessages(this.getConversationMessages(conversationName));
            }
            this.finishPromptRun(conversationName);
          },
          error: (err) => {
            delete this.awaitingQueueAck[conversationName];
            this.finishPromptRun(null, { keepMessages: true });
            this.renderSystemMessage(err.message || "Request failed");
            this.updateStatus("Request failed.");
          },
        });
      });
    }

    renderMessages(messages) {
      this.el.messages.innerHTML = "";
      if (!messages.length) {
        const text = this.state.isDraft ? "Start with a suggested prompt or ask about the current ERP context." : "No messages yet.";
        this.el.messages.innerHTML = `<div class="erp-web-assistant__empty"><strong>ERP AI Desktop</strong><span>${safeHtml(text)}</span><small>Responses are designed to be grounded in ERP data, draft-safe for actions, and easier to reuse across your ERPNext system.</small></div>`;
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
        ${renderMessageHeader(message)}
        ${!isUser ? renderToolEvents(message.tool_events) : ""}
        <div class="erp-web-assistant__msg-body">${renderRichText(message.content || "")}</div>
        ${renderAttachments(message.attachments_json)}
      `;
      Array.from(item.querySelectorAll("[data-copilot-prompt]")).forEach((button) => {
        button.addEventListener("click", () => this.handleCopilotPrompt(button.getAttribute("data-copilot-prompt") || "", false));
      });
      Array.from(item.querySelectorAll("[data-export-toggle]" )).forEach((button) => {
        button.addEventListener("click", () => {
          const exportId = button.getAttribute("data-export-toggle") || "";
          const body = item.querySelector(`[data-export-body="${CSS.escape(exportId)}"]`);
          if (!body) return;
          const allRows = JSON.parse(decodeHtml(body.getAttribute("data-all-rows") || "[]"));
          const previewRows = JSON.parse(decodeHtml(body.getAttribute("data-preview-rows") || "[]"));
          const sample = allRows[0] || previewRows[0] || {};
          const headers = Object.keys(sample).slice(0, 12);
          const expanded = button.getAttribute("data-state") === "expanded";
          const targetRows = expanded ? previewRows : allRows;
          body.innerHTML = renderDatasetRows(headers, targetRows);
          button.setAttribute("data-state", expanded ? "collapsed" : "expanded");
          button.textContent = expanded ? "Show all rows" : "Show preview";
        });
      });
      this.el.messages.appendChild(item);
      this.el.messages.scrollTop = this.el.messages.scrollHeight;
    }

    renderSystemMessage(text) {
      this.el.messages.innerHTML = `<div class="erp-web-assistant__empty"><strong>ERP AI Desktop</strong><span>${safeHtml(text)}</span></div>`;
    }

    loadModelOptions() {
      frappe.call({
        method: "erp_ai_assistant.api.ai.get_available_models",
        callback: (response) => this.setModelOptions(response?.message || {}),
        error: () => this.setModelOptions({}),
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
      if (selected) localStorage.setItem(this.modelStorageKey, selected);
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
          this.pendingImages.push({ name: file.name || "image", type: file.type || "image/png", size: file.size || 0, dataUrl });
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
        chip.innerHTML = `<img src="${item.dataUrl}" alt="${safeHtml(item.name)}" /><span>${safeHtml(item.name)}</span><strong>×</strong>`;
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
      this.showProgress(["Preparing response"], "", { stage: "working", done: false });
      if (!conversationName) return;

      // ── Realtime listener (Socket.IO) ─────────────────────────────────────
      const realtimeHandler = (data) => {
        if (!data || String(data.conversation || "").trim() !== conversationName) return;
        const steps = Array.isArray(data.steps) ? data.steps : [];
        const stage = String(data.stage || "").trim().toLowerCase();
        if ((stage === "idle" || !stage) && this.awaitingQueueAck[conversationName]) return;
        this.showProgress(steps, data.partial_text || "", { stage, done: !!data.done });
        if (data.done) {
          this.stopProgressPolling();
          if (data.error) {
            this.finishPromptRun(null, { keepMessages: true });
            this.renderSystemMessage(data.error || "Request failed");
            this.updateStatus("Request failed.");
            return;
          }
          frappe.call({
            method: "erp_ai_assistant.api.ai.get_prompt_result",
            args: { conversation: conversationName },
            callback: (resultResponse) => {
              const result = resultResponse?.message || {};
              logAssistantDebug(result);
              if (result.done && String(result.reply || "").trim()) {
                this.queueOptimisticAssistantMessage(conversationName, result.reply);
                this.renderMessages(this.getConversationMessages(conversationName));
              }
              this.finishPromptRun(conversationName);
            },
            error: () => this.finishPromptRun(conversationName),
          });
        }
      };

      this._realtimeProgressHandler = realtimeHandler;
      this._realtimeProgressConversation = conversationName;
      try { frappe.realtime.on("erp_ai_progress", realtimeHandler); } catch (e) { /* polling covers it */ }

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
            if ((stage === "idle" || !stage) && this.awaitingQueueAck[conversationName]) return;
            this.showProgress(steps, progress.partial_text || "", { stage, done: !!progress.done });
            if (progress.done) {
              this.stopProgressPolling();
              if (progress.error) {
                this.finishPromptRun(null, { keepMessages: true });
                this.renderSystemMessage(progress.error || "Request failed");
                this.updateStatus("Request failed.");
                return;
              }
              frappe.call({
                method: "erp_ai_assistant.api.ai.get_prompt_result",
                args: { conversation: conversationName },
                callback: (resultResponse) => {
                  const result = resultResponse?.message || {};
                  logAssistantDebug(result);
                  if (result.done && String(result.reply || "").trim()) {
                    this.queueOptimisticAssistantMessage(conversationName, result.reply);
                    this.renderMessages(this.getConversationMessages(conversationName));
                  }
                  this.finishPromptRun(conversationName);
                },
                error: () => this.finishPromptRun(conversationName),
              });
            }
          },
          always: () => {
            this.progressPollPending = false;
          },
        });
      };
      pollProgress();
      this.progressPollTimer = window.setInterval(() => pollProgress(), 800);
    }

    stopProgressPolling() {
      if (this.progressPollTimer) {
        clearInterval(this.progressPollTimer);
        this.progressPollTimer = null;
      }
      this.progressPollPending = false;
      if (this._realtimeProgressHandler) {
        try { frappe.realtime.off("erp_ai_progress", this._realtimeProgressHandler); } catch (e) { /* ignore */ }
        this._realtimeProgressHandler = null;
        this._realtimeProgressConversation = null;
      }
      this.hideProgress();
    }

    showProgress(steps, partialText, options) {
      const settings = options || {};
      const done = !!settings.done;
      const existing = this.el.messages.querySelector(".erp-web-assistant__msg.is-progress");
      if (existing) {
        const summary = existing.querySelector(".erp-web-assistant__progress-summary");
        const stepWrap = existing.querySelector(".erp-web-assistant__progress-steps");
        const partialWrap = existing.querySelector(".erp-web-assistant__progress-partial");
        const spinner = existing.querySelector(".erp-web-assistant__progress-spinner");
        if (summary) summary.textContent = summarizeProgressSteps(steps, done);
        existing.classList.toggle("is-done", done);
        if (spinner) spinner.style.display = done ? "none" : "inline-block";
        if (stepWrap) {
          const items = Array.isArray(steps) ? steps.filter(Boolean) : [];
          const finalItems = items.length ? items.slice(-6) : ["Preparing response"];
          stepWrap.innerHTML = "";
          finalItems.forEach((item, index) => {
            const row = document.createElement("div");
            row.className = "erp-web-assistant__progress-step";
            row.innerHTML = `<span class="erp-web-assistant__progress-step-icon">${done || index < finalItems.length - 1 ? "✓" : "⟳"}</span><span class="erp-web-assistant__progress-step-label">${safeHtml(mapProgressStepLabel(item))}</span>`;
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
        <div class="erp-web-assistant__message-head"><span class="erp-web-assistant__msg-meta">Assistant</span><span class="erp-web-assistant__message-badges"><span class="erp-web-assistant__message-badge is-neutral">Processing</span></span></div>
        <div class="erp-web-assistant__progress-partial" style="display:none;"></div>
        <details class="erp-web-assistant__progress-log" open>
          <summary class="erp-web-assistant__progress-head">
            <span class="erp-web-assistant__progress-spinner" aria-hidden="true"></span>
            <span class="erp-web-assistant__progress-summary">Running...</span>
          </summary>
          <div class="erp-web-assistant__progress-steps"></div>
        </details>`;
      this.el.messages.appendChild(progress);
      this.showProgress(steps, partialText, settings);
      this.el.messages.scrollTop = this.el.messages.scrollHeight;
    }

    hideProgress() {
      this.el.messages.querySelector(".erp-web-assistant__msg.is-progress")?.remove();
    }

    clearCompletionReloadTimer(conversationName) {
      const timer = this.completionReloadTimers[conversationName];
      if (timer) {
        clearTimeout(timer);
        delete this.completionReloadTimers[conversationName];
      }
    }

    loadConversationAfterCompletion(conversationName, attempt) {
      if (!conversationName) return;
      const tryCount = Number(attempt || 0);
      this.clearCompletionReloadTimer(conversationName);
      this.loadConversation(conversationName, {
        onLoaded: (_payload, serverMessages) => {
          const rows = Array.isArray(serverMessages) ? serverMessages : [];
          const lastMessage = rows.length ? rows[rows.length - 1] : null;
          const hasAssistantReply = !!(lastMessage && lastMessage.role === "assistant");
          if (!hasAssistantReply && tryCount < 5) {
            this.completionReloadTimers[conversationName] = window.setTimeout(() => {
              this.loadConversationAfterCompletion(conversationName, tryCount + 1);
            }, 1200);
            return;
          }
          this.clearCompletionReloadTimer(conversationName);
        },
      });
    }

    finishPromptRun(conversationName, options) {
      const settings = options || {};
      this.stopProgressPolling();
      if (conversationName) {
        delete this.awaitingQueueAck[conversationName];
        this.loadConversationAfterCompletion(conversationName, 0);
        this.loadHistory();
        this.updateStatus("Response ready.");
        return;
      }
      if (!settings.keepMessages) this.loadHistory();
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
