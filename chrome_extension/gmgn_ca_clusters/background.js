const SERVICE_URLS = {
  local: "http://127.0.0.1:8000",
  server: "http://43.163.225.175:8012",
};
const DEFAULT_MODE = "local";

async function getServiceMode() {
  const data = await chrome.storage.local.get({ serviceMode: DEFAULT_MODE });
  return SERVICE_URLS[data.serviceMode] ? data.serviceMode : DEFAULT_MODE;
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
    const mode = SERVICE_URLS[message.mode] ? message.mode : DEFAULT_MODE;
    chrome.storage.local
      .set({ serviceMode: mode })
      .then(() => sendResponse({ ok: true, mode, baseUrl: SERVICE_URLS[mode] }))
      .catch((err) => sendResponse({ ok: false, error: err && err.message ? err.message : String(err) }));
    return true;
  }

  if (message.type === "GET_BOTTOM_ABNORMAL") {
    const limit = Math.max(1, Math.min(Number(message.limit || 300), 500));
    getServiceMode()
      .then((mode) => {
        const baseUrl = SERVICE_URLS[mode];
        const url = `${baseUrl}/api/recent?limit=${encodeURIComponent(limit)}`;
        return fetch(url, { method: "GET" }).then(async (resp) => ({ resp, mode, baseUrl }));
      })
      .then(async (resp) => {
        const text = await resp.resp.text();
        let payload = {};
        try {
          payload = text ? JSON.parse(text) : {};
        } catch {
          payload = { detail: text };
        }
        if (!resp.resp.ok) {
          sendResponse({ ok: false, status: resp.resp.status, mode: resp.mode, baseUrl: resp.baseUrl, error: payload.detail || `HTTP ${resp.resp.status}` });
          return;
        }
        sendResponse({ ok: true, mode: resp.mode, baseUrl: resp.baseUrl, data: payload });
      })
      .catch((err) => {
        sendResponse({ ok: false, status: 0, error: err && err.message ? err.message : String(err) });
      });
    return true;
  }

  if (message.type !== "ANALYZE_CA") {
    return false;
  }

  const address = String(message.address || "").trim();
  const chain = String(message.chain || "sol").trim() || "sol";
  const limit = Number(message.limit || 100);

  getServiceMode()
    .then((mode) => {
      const baseUrl = SERVICE_URLS[mode];
      const url = `${baseUrl}/api/ca-clusters?chain=${encodeURIComponent(chain)}&limit=${encodeURIComponent(limit)}&address=${encodeURIComponent(address)}`;
      return fetch(url, { method: "GET" }).then(async (resp) => ({ resp, mode, baseUrl }));
    })
    .then(async (resp) => {
      const text = await resp.resp.text();
      let payload = {};
      try {
        payload = text ? JSON.parse(text) : {};
      } catch {
        payload = { detail: text };
      }
      if (!resp.resp.ok) {
        sendResponse({ ok: false, status: resp.resp.status, mode: resp.mode, baseUrl: resp.baseUrl, error: payload.detail || `HTTP ${resp.resp.status}` });
        return;
      }
      if (payload && payload.ok === false) {
        sendResponse({ ok: false, status: resp.resp.status, mode: resp.mode, baseUrl: resp.baseUrl, error: payload.error || "Analysis returned no data." });
        return;
      }
      sendResponse({ ok: true, mode: resp.mode, baseUrl: resp.baseUrl, data: payload });
    })
    .catch((err) => {
      sendResponse({ ok: false, status: 0, error: err && err.message ? err.message : String(err) });
    });

  return true;
});
