const SERVICE_URLS = {
  server: "http://43.163.225.175:8010",
};
const DEFAULT_MODE = "server";

async function getServiceMode() {
  return DEFAULT_MODE;
}

function serviceModes(preferredMode, options = {}) {
  return ["server"];
}

function parseSseBlock(block) {
  const message = { event: "message", data: "", id: "" };
  const dataLines = [];
  for (const rawLine of String(block || "").split(/\r?\n/)) {
    if (!rawLine || rawLine.startsWith(":")) continue;
    const index = rawLine.indexOf(":");
    const field = index >= 0 ? rawLine.slice(0, index) : rawLine;
    let value = index >= 0 ? rawLine.slice(index + 1) : "";
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") message.event = value;
    if (field === "id") message.id = value;
    if (field === "data") dataLines.push(value);
  }
  message.data = dataLines.join("\n");
  return message.data || message.event !== "message" ? message : null;
}

function safePost(port, message) {
  try {
    port.postMessage(message);
    return true;
  } catch {
    return false;
  }
}

async function streamPluginEvents(port, lastId, signal) {
  const mode = await getServiceMode();
  const baseUrl = SERVICE_URLS[mode];
  const query = lastId ? `?last_id=${encodeURIComponent(lastId)}` : "";
  const resp = await fetch(`${baseUrl}/api/plugin/events${query}`, {
    method: "GET",
    headers: { Accept: "text/event-stream" },
    signal,
  });
  if (!resp.ok || !resp.body) {
    throw new Error(`plugin event stream HTTP ${resp.status}`);
  }

  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const parts = buffer.split(/\r?\n\r?\n/);
    buffer = parts.pop() || "";
    for (const part of parts) {
      const parsed = parseSseBlock(part);
      if (!parsed) continue;
      let payload = {};
      try {
        payload = parsed.data ? JSON.parse(parsed.data) : {};
      } catch {
        payload = { raw: parsed.data };
      }
      if (parsed.id && payload && typeof payload === "object" && !payload.id) {
        payload.id = parsed.id;
      }
      if (parsed.event === "ready") {
        if (!safePost(port, { type: "ready", data: payload })) return;
      } else if (parsed.event === "signal") {
        if (!safePost(port, { type: "signal", item: payload })) return;
      }
    }
  }
}

async function fetchServiceJson(path, options = {}) {
  const preferredMode = await getServiceMode();
  const modes = serviceModes(preferredMode, options);
  let lastError = null;

  for (const mode of modes) {
    const baseUrl = SERVICE_URLS[mode];
    const url = `${baseUrl}${path}`;
    try {
      const resp = await fetch(url, { method: "GET" });
      const text = await resp.text();
      let payload = {};
      try {
        payload = text ? JSON.parse(text) : {};
      } catch {
        payload = { detail: text };
      }
      if (resp.ok) {
        return { ok: true, mode, baseUrl, data: payload };
      }
      lastError = { status: resp.status, mode, baseUrl, error: payload.detail || `HTTP ${resp.status}` };
    } catch (err) {
      lastError = { status: 0, mode, baseUrl, error: err && err.message ? err.message : String(err) };
    }
  }

  return { ok: false, ...(lastError || { status: 0, error: "service unavailable" }) };
}

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (!message) {
    return false;
  }

  if (message.type === "GET_SERVICE_CONFIG") {
    getServiceMode()
      .then((mode) => sendResponse({ ok: true, mode, baseUrl: SERVICE_URLS[mode] }))
      .catch((err) => sendResponse({ ok: false, error: err && err.message ? err.message : String(err) }));
    return true;
  }

  if (message.type === "SET_SERVICE_MODE") {
    const mode = DEFAULT_MODE;
    chrome.storage.local
      .set({ serviceMode: mode })
      .then(() => sendResponse({ ok: true, mode, baseUrl: SERVICE_URLS[mode] }))
      .catch((err) => sendResponse({ ok: false, error: err && err.message ? err.message : String(err) }));
    return true;
  }

  if (message.type === "GET_PLUGIN_EVENT_URL") {
    getServiceMode()
      .then((mode) => sendResponse({ ok: true, mode, url: `${SERVICE_URLS[mode]}/api/plugin/events` }))
      .catch((err) => sendResponse({ ok: false, error: err && err.message ? err.message : String(err) }));
    return true;
  }

  if (message.type === "GET_BOTTOM_ABNORMAL") {
    const limit = Math.max(1, Math.min(Number(message.limit || 100), 200));
    fetchServiceJson(`/api/plugin/bottom-abnormal?limit=${encodeURIComponent(limit)}`, { serverOnly: true }).then(sendResponse);
    return true;
  }

  if (message.type === "GET_ALPHA_NEW_TOKENS") {
    const limit = Math.max(1, Math.min(Number(message.limit || 100), 200));
    fetchServiceJson(`/api/plugin/alpha-new-tokens?limit=${encodeURIComponent(limit)}`, { serverOnly: true }).then(sendResponse);
    return true;
  }

  return false;
});

chrome.runtime.onConnect.addListener((port) => {
  if (port.name !== "PLUGIN_EVENTS") return;
  let controller = null;
  let started = false;

  port.onMessage.addListener((message) => {
    if (!message || message.type !== "START" || started) return;
    started = true;
    controller = new AbortController();
    streamPluginEvents(port, String(message.lastId || ""), controller.signal)
      .then(() => {
        if (controller?.signal.aborted) return;
        safePost(port, { type: "error", error: "plugin event stream closed" });
        try {
          port.disconnect();
        } catch {}
      })
      .catch((err) => {
        if (controller?.signal.aborted) return;
        safePost(port, { type: "error", error: err && err.message ? err.message : String(err) });
        try {
          port.disconnect();
        } catch {}
      });
  });

  port.onDisconnect.addListener(() => {
    if (controller) controller.abort();
  });
});
