(function () {
  const BASE58_CA = /\b[1-9A-HJ-NP-Za-km-z]{32,44}\b/g;
  const L = {
    none: "\u65e0",
    wallet: "\u94b1\u5305",
    copied: "\u5df2\u590d\u5236",
    copy: "\u590d\u5236",
    title: "CA \u6346\u7ed1\u5206\u6790",
    token: "\u4ee3\u5e01",
    mcapLp: "\u5e02\u503c/\u6c60\u5b50",
    chips: "\u7b79\u7801\u96c6\u4e2d",
    topProfit: "Top\u76c8\u5229",
    unsold: "\u672a\u5356\u51fa",
    summaryTitle: "\u5206\u7c7b\u6458\u8981\uff1a\u5360\u6bd4 / \u5e73\u5747\u76c8\u5229",
    walletDetails: "\u5bf9\u5e94\u6346\u7ed1\u94b1\u5305\u8be6\u60c5",
    bundleTimeDetails: "\u6346\u7ed1\u65f6\u95f4\u7c07\u8be6\u60c5",
    buyTimeDetails: "\u8d2d\u4e70\u65f6\u95f4\u7c07\u8be6\u60c5",
    riskTitle: "\u53ef\u89c2\u5bdf\u98ce\u9669\u56e0\u7d20",
    noSummary: "\u6682\u65e0\u5206\u7c7b\u6458\u8981",
    noBundlers: "\u672a\u547d\u4e2d\u672c\u5730\u6346\u7ed1\u6807\u7b7e\u6216 GMGN bundler \u6807\u7b7e",
    noCreateSecond: "\u6ca1\u6709\u540c\u79d2\u521b\u5efa\u7c07",
    noCreateHour: "\u6ca1\u6709\u660e\u663e\u5c0f\u65f6\u7ea7\u521b\u5efa\u7c07",
    noBuySecond: "\u6ca1\u6709\u540c\u79d2\u8d2d\u4e70\u7c07",
    noBuyFive: "\u6ca1\u6709\u660e\u663e 5 \u5206\u949f\u8d2d\u4e70\u7c07",
    noRisk: "\u6682\u65e0\u660e\u663e\u98ce\u9669\u6807\u7b7e",
    openHint: "\u6253\u5f00 GMGN \u4ee3\u5e01\u9875\uff0c\u6216\u624b\u52a8\u8f93\u5165 CA\u3002",
    refresh: "\u5237\u65b0",
    clear: "\u6e05\u7a7a",
    serviceLocal: "\u672c\u5730",
    serviceServer: "\u670d\u52a1\u5668",
    collapse: "\u6536\u8d77",
    analyze: "\u5206\u6790",
    run: "\u67e5",
    analyzing: "\u6b63\u5728\u5206\u6790",
    noValidCa: "\u6ca1\u6709\u8bc6\u522b\u5230\u6709\u6548 CA\u3002",
    apiError: "\u672c\u5730\u63a5\u53e3\u4e0d\u53ef\u7528\u6216\u5206\u6790\u5931\u8d25",
    walletCount: "\u94b1\u5305\u6570",
    holdPct: "\u6301\u4ed3\u5360\u6bd4",
    buyVolume: "\u4e70\u5165\u989d",
    profitable: "\u76c8\u5229\u94b1\u5305",
    realized: "\u5df2\u5b9e\u73b0",
    unrealized: "\u672a\u5b9e\u73b0",
    buy: "\u4e70\u5165",
    holding: "\u6301\u4ed3",
    profit: "\u76c8\u5229",
  };
  const POS_KEY = "ca_cluster_panel_position_v1";
  const STATE = {
    ca: "",
    loading: false,
    collapsed: false,
    lastUrl: location.href,
    lastDetectedCa: "",
    pendingScan: 0,
    result: null,
    error: "",
    copied: "",
    dragging: false,
    ignoredCa: "",
    serviceMode: "local",
    serviceBaseUrl: "",
  };

  function extractCa() {
    const hrefCandidates = location.href.match(BASE58_CA) || [];
    const hrefCandidate = hrefCandidates.find((item) => item.length >= 40);
    if (hrefCandidate) return hrefCandidate;

    const pathCandidates = location.pathname.match(BASE58_CA) || [];
    const tokenCandidate = pathCandidates.find((item) => item.length >= 40);
    if (tokenCandidate) return tokenCandidate;

    const bodyCandidates = (document.body?.innerText || "").match(BASE58_CA) || [];
    return bodyCandidates.find((item) => item.length >= 40) || "";
  }

  function fmtPct(value) {
    const n = Number(value || 0);
    return `${n.toFixed(n >= 10 ? 1 : 2)}%`;
  }

  function fmtSignedPct(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n)) return "0.00%";
    const sign = n > 0 ? "+" : "";
    return `${sign}${n.toFixed(Math.abs(n) >= 10 ? 1 : 2)}%`;
  }

  function fmtUsd(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n) || n === 0) return "$0";
    const sign = n < 0 ? "-" : "";
    const abs = Math.abs(n);
    if (abs >= 1_000_000) return `${sign}$${(abs / 1_000_000).toFixed(2)}M`;
    if (abs >= 1_000) return `${sign}$${(abs / 1_000).toFixed(1)}K`;
    return `${sign}$${abs.toFixed(0)}`;
  }

  function shortCa(ca) {
    return ca ? `${ca.slice(0, 6)}...${ca.slice(-4)}` : "No CA";
  }

  function walletName(wallet) {
    const label = wallet.label || {};
    const name = label.name ? `${label.name} ` : "";
    return `${name}${wallet.short || shortCa(wallet.address || "")}`;
  }

  function createPanel() {
    const host = document.createElement("div");
    host.className = "ca-cluster-host";
    const panel = document.createElement("div");
    panel.className = "ca-cluster-panel";
    host.appendChild(panel);
    document.documentElement.appendChild(host);
    return panel;
  }

  const panel = createPanel();
  restorePanelPosition();

  function row(label, value) {
    return `<div class="ca-cluster-row"><div class="ca-cluster-label">${escapeHtml(label)}</div><div class="ca-cluster-value">${value}</div></div>`;
  }

  function renderWalletList(wallets, max = 4) {
    if (!wallets || !wallets.length) return L.none;
    return wallets
      .slice(0, max)
      .map((wallet) => `${escapeHtml(walletName(wallet))} ${fmtPct(wallet.hold_pct || 0)} / ${fmtSignedPct(wallet.profit_pct)}`)
      .join("<br>");
  }

  function renderClusterList(clusters, emptyText) {
    if (!clusters || !clusters.length) return `<div class="ca-cluster-empty">${emptyText}</div>`;
    return `<div class="ca-cluster-list">${clusters
      .map(
        (cluster) => `<div class="ca-cluster-item">
          <div class="ca-cluster-item-head">
            <span>${escapeHtml(cluster.bucket_label || cluster.label || "")}</span>
            <span>${cluster.count || 0} ${L.wallet}</span>
          </div>
          <div class="ca-cluster-wallets">${renderWalletList(cluster.wallets || [])}</div>
        </div>`,
      )
      .join("")}</div>`;
  }

  function renderWalletCards(wallets) {
    if (!wallets || !wallets.length) return `<div class="ca-cluster-empty">${L.noBundlers}</div>`;
    return `<div class="ca-cluster-list">${wallets
      .slice(0, 20)
      .map((wallet) => {
        const groups = ((wallet.label || {}).groups || []).map((item) => `<span class="ca-cluster-chip">${escapeHtml(String(item))}</span>`).join("");
        const tags = [...(wallet.tags || []), ...(wallet.maker_tags || [])]
          .slice(0, 4)
          .map((item) => `<span class="ca-cluster-chip">${escapeHtml(String(item))}</span>`)
          .join("");
        return `<div class="ca-cluster-item">
          <div class="ca-cluster-item-head">
            <span>${escapeHtml(walletName(wallet))}</span>
            <span>${fmtPct(wallet.hold_pct)}</span>
          </div>
          <div class="ca-cluster-wallet-line">
            <code>${escapeHtml(wallet.address || "")}</code>
            <button class="ca-copy-button" data-copy="${escapeAttr(wallet.address || "")}">${STATE.copied === wallet.address ? L.copied : L.copy}</button>
          </div>
          <div class="ca-cluster-meta">
            ${L.buy} ${fmtUsd(wallet.buy_volume)} / ${L.holding} ${fmtUsd(wallet.usd_value)} / ${L.profit} ${fmtSignedPct(wallet.profit_pct)}
            <br>${L.realized} ${fmtUsd(wallet.realized_profit)} / ${L.unrealized} ${fmtUsd(wallet.unrealized_profit)}
          </div>
          <div>${groups}${tags}</div>
        </div>`;
      })
      .join("")}</div>`;
  }

  function renderSummaryCards(items) {
    if (!items || !items.length) return `<div class="ca-cluster-empty">${L.noSummary}</div>`;
    return `<div class="ca-summary-grid">${items
      .slice(0, 8)
      .map(
        (item) => `<details class="ca-summary-card">
          <summary>
            <span>${escapeHtml(item.name || "")}</span>
            <b>${fmtPct(item.wallet_pct)} / ${fmtSignedPct(item.avg_profit_pct)}</b>
          </summary>
          <div class="ca-summary-metrics">
            ${row(L.walletCount, `${item.count || 0}`)}
            ${row(L.holdPct, fmtPct(item.hold_pct))}
            ${row(L.buyVolume, fmtUsd(item.buy_volume))}
            ${row(L.profitable, fmtPct(item.profitable_pct))}
            ${row(L.realized, fmtUsd(item.realized_profit))}
            ${row(L.unrealized, fmtUsd(item.unrealized_profit))}
          </div>
          ${renderWalletCards(item.wallets || [])}
        </details>`,
      )
      .join("")}</div>`;
  }

  function renderDetails(title, content, open = false) {
    return `<details class="ca-detail-block"${open ? " open" : ""}>
      <summary>${escapeHtml(title)}</summary>
      <div class="ca-detail-content">${content}</div>
    </details>`;
  }

  function renderResult(result) {
    const token = result.token || {};
    const chip = result.chip_distribution || {};
    const known = result.known_bundled_wallets || [];
    const bundleTime = result.bundle_time_clusters || {};
    const purchaseTime = result.purchase_time_clusters || {};
    const risks = result.risk_factors || [];
    const summaries = result.bundle_category_summary || [];
    const behavior = ((result.holder_trader_structure || {}).behavior || {});

    return `
      ${row(L.token, `${escapeHtml(token.symbol || token.name || "?")} ${escapeHtml(shortCa(result.address))}`)}
      ${row(L.mcapLp, `${fmtUsd(token.market_cap)} / ${fmtUsd(token.liquidity)}`)}
      ${row(L.chips, `Top10 ${fmtPct(chip.top10_pct)} / Top30 ${fmtPct(chip.top30_pct)} / Top50 ${fmtPct(chip.top50_pct)}`)}
      ${row("Top30", `${fmtUsd(chip.top30_profit)} (${L.realized} ${fmtUsd(chip.top30_realized_profit)} / ${L.unrealized} ${fmtUsd(chip.top30_unrealized_profit)})`)}
      ${row("Top50", `${fmtUsd(chip.top50_profit)} (${L.realized} ${fmtUsd(chip.top50_realized_profit)} / ${L.unrealized} ${fmtUsd(chip.top50_unrealized_profit)})`)}
      ${row(L.unsold, `${behavior.zero_sell || 0} ${L.wallet} / ${fmtPct(behavior.zero_sell_pct)}`)}
      <div class="ca-cluster-section">
        <h3>${L.summaryTitle}</h3>
        ${renderSummaryCards(summaries)}
      </div>
      ${renderDetails(L.walletDetails, renderWalletCards(known))}
      ${renderDetails(
        L.bundleTimeDetails,
        `${renderClusterList(bundleTime.same_second_clusters, L.noCreateSecond)}
        <div style="height: 7px"></div>
        ${renderClusterList(bundleTime.hour_clusters, L.noCreateHour)}`,
      )}
      ${renderDetails(
        L.buyTimeDetails,
        `${renderClusterList(purchaseTime.same_second_clusters, L.noBuySecond)}
        <div style="height: 7px"></div>
        ${renderClusterList(purchaseTime.five_minute_clusters, L.noBuyFive)}`,
      )}
      <div class="ca-cluster-section">
        <h3>${L.riskTitle}</h3>
        ${
          risks.length
            ? risks.map((item) => `<span class="ca-cluster-chip">${escapeHtml(String(item))}</span>`).join("")
            : `<div class="ca-cluster-empty">${L.noRisk}</div>`
        }
      </div>
    `;
  }

  function render() {
    panel.className = `ca-cluster-panel${STATE.collapsed ? " ca-collapsed" : ""}`;
    const body = STATE.loading
      ? `<div class="ca-cluster-loading">${L.analyzing} ${escapeHtml(shortCa(STATE.ca))}</div>`
      : STATE.error
        ? `<div class="ca-cluster-error">${escapeHtml(STATE.error)}</div>`
        : STATE.result
          ? renderResult(STATE.result)
          : `<div class="ca-cluster-empty">${L.openHint}</div>`;

    panel.innerHTML = `
      <div class="ca-cluster-header">
        <div class="ca-cluster-title">${L.title}</div>
        <div class="ca-cluster-actions">
          <button class="ca-cluster-button ca-service-toggle" title="${STATE.serviceBaseUrl || ""}">${STATE.serviceMode === "server" ? L.serviceServer : L.serviceLocal}</button>
          <button class="ca-cluster-button ca-cluster-refresh" title="${L.refresh}">R</button>
          <button class="ca-cluster-button ca-cluster-clear" title="${L.clear}">C</button>
          <button class="ca-cluster-button ca-cluster-toggle" title="${L.collapse}">${STATE.collapsed ? "+" : "-"}</button>
        </div>
      </div>
      <div class="ca-cluster-body">
        <div class="ca-cluster-input-row">
          <input class="ca-cluster-input" value="${escapeAttr(STATE.ca)}" placeholder="Token CA" />
          <button class="ca-cluster-button ca-cluster-run" title="${L.analyze}">${L.run}</button>
        </div>
        ${body}
      </div>
    `;

    panel.querySelector(".ca-cluster-toggle")?.addEventListener("click", () => {
      STATE.collapsed = !STATE.collapsed;
      render();
    });
    panel.querySelector(".ca-service-toggle")?.addEventListener("click", () => toggleServiceMode());
    panel.querySelector(".ca-cluster-refresh")?.addEventListener("click", () => analyze(STATE.ca, true));
    panel.querySelector(".ca-cluster-clear")?.addEventListener("click", () => clearPanel());
    panel.querySelector(".ca-cluster-run")?.addEventListener("click", () => {
      const value = panel.querySelector(".ca-cluster-input")?.value?.trim() || "";
      STATE.ignoredCa = "";
      analyze(value, true);
    });
    attachDragHandlers();
    panel.querySelectorAll(".ca-copy-button").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        copyText(button.getAttribute("data-copy") || "");
      });
    });
  }

  async function analyze(ca, force) {
    if (!ca || ca.length < 32) {
      STATE.error = L.noValidCa;
      STATE.result = null;
      render();
      return;
    }
    if (!force && ca === STATE.ca && STATE.result) return;
    if (!force && ca === STATE.ignoredCa) return;
    STATE.ca = ca;
    STATE.loading = true;
    STATE.error = "";
    render();
    try {
      const message = { type: "ANALYZE_CA", address: ca, chain: "sol", limit: 100 };
      const response = await chrome.runtime.sendMessage(message);
      if (!response || !response.ok) {
        throw new Error((response && response.error) || "No response from extension background worker.");
      }
      STATE.serviceMode = response.mode || STATE.serviceMode;
      STATE.serviceBaseUrl = response.baseUrl || STATE.serviceBaseUrl;
      STATE.result = response.data;
    } catch (err) {
      STATE.result = null;
      STATE.error = `${L.apiError}: ${err.message || err}`;
    } finally {
      STATE.loading = false;
      render();
    }
  }

  async function copyText(text) {
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      const input = document.createElement("textarea");
      input.value = text;
      document.documentElement.appendChild(input);
      input.select();
      document.execCommand("copy");
      input.remove();
    }
    STATE.copied = text;
    render();
    setTimeout(() => {
      if (STATE.copied === text) {
        STATE.copied = "";
        render();
      }
    }, 1200);
  }

  function clearPanel() {
    STATE.ignoredCa = STATE.ca || STATE.lastDetectedCa || "";
    STATE.ca = "";
    STATE.result = null;
    STATE.error = "";
    STATE.loading = false;
    STATE.copied = "";
    render();
  }

  async function loadServiceConfig() {
    try {
      const response = await chrome.runtime.sendMessage({ type: "GET_SERVICE_CONFIG" });
      if (response && response.ok) {
        STATE.serviceMode = response.mode || STATE.serviceMode;
        STATE.serviceBaseUrl = response.baseUrl || STATE.serviceBaseUrl;
        render();
      }
    } catch {}
  }

  async function toggleServiceMode() {
    const nextMode = STATE.serviceMode === "server" ? "local" : "server";
    try {
      const response = await chrome.runtime.sendMessage({ type: "SET_SERVICE_MODE", mode: nextMode });
      if (response && response.ok) {
        STATE.serviceMode = response.mode || nextMode;
        STATE.serviceBaseUrl = response.baseUrl || "";
        STATE.result = null;
        STATE.error = "";
        render();
        if (STATE.ca) analyze(STATE.ca, true);
      }
    } catch (err) {
      STATE.error = `${L.apiError}: ${err.message || err}`;
      render();
    }
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function escapeAttr(value) {
    return escapeHtml(value).replaceAll("`", "&#96;");
  }

  function restorePanelPosition() {
    try {
      const saved = JSON.parse(localStorage.getItem(POS_KEY) || "null");
      if (saved && Number.isFinite(saved.left) && Number.isFinite(saved.top)) {
        applyPanelPosition(saved.left, saved.top);
        return;
      }
    } catch {}
    const left = Math.max(8, window.innerWidth - 354);
    applyPanelPosition(left, 92);
  }

  function applyPanelPosition(left, top) {
    const width = panel.offsetWidth || 336;
    const height = Math.min(panel.offsetHeight || 420, window.innerHeight - 16);
    const maxLeft = Math.max(8, window.innerWidth - width - 8);
    const maxTop = Math.max(8, window.innerHeight - Math.min(height, 120) - 8);
    const x = Math.min(Math.max(8, left), maxLeft);
    const y = Math.min(Math.max(8, top), maxTop);
    panel.style.left = `${x}px`;
    panel.style.top = `${y}px`;
    panel.style.right = "auto";
  }

  function savePanelPosition() {
    const rect = panel.getBoundingClientRect();
    localStorage.setItem(POS_KEY, JSON.stringify({ left: rect.left, top: rect.top }));
  }

  function attachDragHandlers() {
    const header = panel.querySelector(".ca-cluster-header");
    if (!header || header.dataset.dragReady === "1") return;
    header.dataset.dragReady = "1";
    header.addEventListener("pointerdown", (event) => {
      if (event.target.closest("button")) return;
      const rect = panel.getBoundingClientRect();
      const startX = event.clientX;
      const startY = event.clientY;
      const startLeft = rect.left;
      const startTop = rect.top;
      STATE.dragging = true;
      header.setPointerCapture(event.pointerId);

      const onMove = (moveEvent) => {
        if (!STATE.dragging) return;
        applyPanelPosition(startLeft + moveEvent.clientX - startX, startTop + moveEvent.clientY - startY);
      };
      const onUp = () => {
        STATE.dragging = false;
        savePanelPosition();
        header.removeEventListener("pointermove", onMove);
        header.removeEventListener("pointerup", onUp);
        header.removeEventListener("pointercancel", onUp);
      };

      header.addEventListener("pointermove", onMove);
      header.addEventListener("pointerup", onUp);
      header.addEventListener("pointercancel", onUp);
    });
  }

  function tick() {
    const ca = extractCa();
    const changed = location.href !== STATE.lastUrl || ca !== STATE.lastDetectedCa;
    if (location.href !== STATE.lastUrl && ca !== STATE.ignoredCa) {
      STATE.ignoredCa = "";
    }
    STATE.lastUrl = location.href;
    STATE.lastDetectedCa = ca;
    if (changed && ca && ca !== STATE.ca) analyze(ca, false);
  }

  function scheduleTick() {
    clearTimeout(STATE.pendingScan);
    STATE.pendingScan = setTimeout(tick, 350);
  }

  function wrapHistory(name) {
    const original = history[name];
    history[name] = function (...args) {
      const result = original.apply(this, args);
      scheduleTick();
      return result;
    };
  }

  render();
  loadServiceConfig();
  wrapHistory("pushState");
  wrapHistory("replaceState");
  window.addEventListener("popstate", scheduleTick);
  new MutationObserver(scheduleTick).observe(document.documentElement, {
    childList: true,
    subtree: true,
  });
  const initialCa = extractCa();
  if (initialCa) analyze(initialCa, false);
  setInterval(tick, 1500);
})();
