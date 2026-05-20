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
