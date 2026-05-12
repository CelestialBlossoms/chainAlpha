(function () {
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
    apiKey: "\u5bc6\u94a5",
    apiKeySet: "\u5df2\u8bbe\u5bc6\u94a5",
    apiKeyUnset: "\u672a\u8bbe\u5bc6\u94a5",
    apiKeyPrompt: "\u8bf7\u8f93\u5165 Chain Alpha \u63a5\u53e3\u5bc6\u94a5\uff08\u7559\u7a7a\u53ef\u6e05\u9664\uff09",
    caView: "CA\u5206\u6790",
    abnormalView: "\u5f02\u52a8\u68c0\u6d4b",
    abnormalTitle: "\u5e95\u90e8\u5f02\u52a8\u4ee3\u5e01",
    abnormalHint: "\u70b9\u51fb\u4ee3\u5e01\u5207\u6362\u5230 CA \u5206\u6790",
    abnormalEmpty: "\u6682\u65e0\u5f02\u52a8\u4ee3\u5e01\u6570\u636e",
    new1mView: "1m\u65b0\u5e01",
    new1mTitle: "1m\u65b0\u5e01\u68c0\u6d4b",
    new1mHint: "\u4ec5\u8c37\u6b4c\u63d2\u4ef6\u63a5\u6536\uff0c\u4e0d\u63a8\u9001 TG \u548c\u524d\u7aef",
    new1mEmpty: "\u6682\u65e0 1m \u65b0\u5e01\u6570\u636e",
    currentMcap: "\u5f53\u524d\u5e02\u503c",
    maxMcap: "\u6700\u9ad8\u5e02\u503c",
    liquidity: "\u6d41\u52a8\u6027",
    priceChange: "\u6da8\u5e45",
    tokenAge: "\u5e01\u9f84",
    updated: "\u66f4\u65b0",
    serviceLocal: "\u672c\u5730",
    serviceServer: "\u670d\u52a1\u5668",
    collapse: "\u6536\u8d77",
    analyze: "\u5206\u6790",
    run: "\u67e5",
    analyzing: "\u6b63\u5728\u5206\u6790",
    noValidCa: "\u6ca1\u6709\u8bc6\u522b\u5230\u6709\u6548 CA\u3002",
    apiError: "\u672c\u5730\u63a5\u53e3\u4e0d\u53ef\u7528\u6216\u5206\u6790\u5931\u8d25",
    new1mApiError: "1m\u65b0\u5e01\u63a5\u53e3\u6216 Redis \u6d41\u4e0d\u53ef\u7528",
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
  const LAYOUT_KEY = "ca_cluster_panel_layout_v2";
  const DEFAULT_WIDTH = 336;
  const DEFAULT_HEIGHT = 520;
  const MIN_WIDTH = 280;
  const MIN_HEIGHT = 220;
  const STATE = {
    ca: "",
    loading: false,
    collapsed: false,
    result: null,
    error: "",
    copied: "",
    dragging: false,
    resizing: false,
    view: "ca",
    abnormalLoading: false,
    abnormalItems: [],
    abnormalError: "",
    abnormalTimer: 0,
    abnormalLastCount: 0,
    new1mLoading: false,
    new1mItems: [],
    new1mError: "",
    new1mTimer: 0,
    serviceMode: "local",
    serviceBaseUrl: "",
    hasApiKey: false,
  };

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

  function fmtTime(value) {
    if (!value) return "-";
    const raw = Number(value);
    const date = Number.isFinite(raw) ? new Date(raw < 1000000000000 ? raw * 1000 : raw) : new Date(value);
    if (!Number.isFinite(date.getTime())) return "-";
    return date.toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
  }

  function fmtAge(seconds) {
    const value = Number(seconds || 0);
    if (!Number.isFinite(value) || value <= 0) return "-";
    if (value < 3600) return `${Math.max(1, Math.round(value / 60))}m`;
    if (value < 86400) return `${(value / 3600).toFixed(1)}h`;
    return `${(value / 86400).toFixed(1)}d`;
  }

  function toNumber(value) {
    const n = Number(value || 0);
    return Number.isFinite(n) ? n : 0;
  }

  function shortCa(ca) {
    return ca ? `${ca.slice(0, 6)}...${ca.slice(-4)}` : "No CA";
  }

  function gmgnUrl(ca, chain = "sol") {
    return ca ? `https://gmgn.ai/${encodeURIComponent(chain || "sol")}/token/${encodeURIComponent(ca)}` : "https://gmgn.ai/";
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
  restorePanelLayout();

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

  function renderAbnormalHead() {
    return `<div class="ca-abnormal-head">
      <div>
        <h3>${L.abnormalTitle}</h3>
        <p>${L.abnormalHint}</p>
      </div>
      <button class="ca-cluster-button ca-abnormal-refresh" title="${L.refresh}">R</button>
    </div>`;
  }

  function renderAbnormalContent() {
    if (STATE.abnormalLoading) {
      return `<div class="ca-cluster-loading">${L.analyzing} ${L.abnormalView}</div>`;
    }
    if (STATE.abnormalError) {
      return `<div class="ca-cluster-error">${escapeHtml(STATE.abnormalError)}</div>`;
    }
    const items = STATE.abnormalItems || [];
    if (!items.length) {
      return `<div class="ca-cluster-empty">${L.abnormalEmpty}</div>`;
    }
    return `<div class="ca-abnormal-list">
        ${items
          .slice(0, 60)
          .map((item) => {
            const ca = String(item.ca || "");
            const symbol = item.symbol || item.token_type || "?";
            const narrative = item.narrative || item.abnormal_rule || item.source || "";
            return `<div class="ca-abnormal-row">
              <div class="ca-abnormal-token">
                <button class="ca-watch-ca" data-ca="${escapeAttr(ca)}" title="${L.analyze}">
                  <b>${escapeHtml(symbol)}</b>
                  <span>${escapeHtml(shortCa(ca))}</span>
                </button>
                <a class="ca-gmgn-link" href="${escapeAttr(gmgnUrl(ca, item.chain || "sol"))}" target="_blank" rel="noreferrer">GMGN</a>
              </div>
              <div class="ca-watch-note" title="${escapeAttr(narrative)}">${escapeHtml(narrative)}</div>
              <div class="ca-abnormal-metrics">
                <span><em>${L.currentMcap}</em><b>${fmtUsd(item.current_mcap)}</b></span>
                <span><em>${L.priceChange}</em><b class="${toNumber(item.price_change_pct) >= 0 ? "ca-positive" : "ca-negative"}">${fmtSignedPct(item.price_change_pct)}</b></span>
                <span><em>${L.tokenAge}</em><b>${escapeHtml(fmtAge(item.age_sec))}</b></span>
                <span><em>${L.maxMcap}</em><b>${fmtUsd(item.max_mcap || item.ath_mcap || item.peak_mcap)}</b></span>
                <span><em>${L.liquidity}</em><b>${fmtUsd(item.liquidity)}</b></span>
                <span><em>${L.updated}</em><b>${escapeHtml(fmtTime(item.ts || item.last_seen_at || item.added_at))}</b></span>
              </div>
            </div>`;
          })
          .join("")}
      </div>`;
  }

  function renderNew1mHead() {
    return `<div class="ca-abnormal-head">
      <div>
        <h3>${L.new1mTitle}</h3>
        <p>${L.new1mHint}</p>
      </div>
      <button class="ca-cluster-button ca-new1m-refresh" title="${L.refresh}">R</button>
    </div>`;
  }

  function renderNew1mContent() {
    if (STATE.new1mLoading) {
      return `<div class="ca-cluster-loading">${L.analyzing} ${L.new1mView}</div>`;
    }
    if (STATE.new1mError) {
      return `<div class="ca-cluster-error">${escapeHtml(STATE.new1mError)}</div>`;
    }
    const items = STATE.new1mItems || [];
    if (!items.length) {
      return `<div class="ca-cluster-empty">${L.new1mEmpty}</div>`;
    }
    return `<div class="ca-abnormal-list">
        ${items
          .slice(0, 60)
          .map((item) => {
            const ca = String(item.ca || "");
            const symbol = item.symbol || "?";
            const narrative = item.abnormal_rule || "plugin_new_1m";
            return `<div class="ca-abnormal-row ca-new1m-row">
              <div class="ca-abnormal-token">
                <button class="ca-watch-ca" data-ca="${escapeAttr(ca)}" title="${L.analyze}">
                  <b>${escapeHtml(symbol)}</b>
                  <span>${escapeHtml(shortCa(ca))}</span>
                </button>
                <a class="ca-gmgn-link" href="${escapeAttr(gmgnUrl(ca, item.chain || "sol"))}" target="_blank" rel="noreferrer">GMGN</a>
              </div>
              <div class="ca-watch-note" title="${escapeAttr(narrative)}">${escapeHtml(narrative)}</div>
              <div class="ca-abnormal-metrics">
                <span><em>${L.currentMcap}</em><b>${fmtUsd(item.current_mcap)}</b></span>
                <span><em>${L.priceChange}</em><b class="${toNumber(item.price_change_pct) >= 0 ? "ca-positive" : "ca-negative"}">${fmtSignedPct(item.price_change_pct)}</b></span>
                <span><em>${L.tokenAge}</em><b>${escapeHtml(fmtAge(item.age_sec))}</b></span>
                <span><em>${L.liquidity}</em><b>${fmtUsd(item.liquidity)}</b></span>
                <span><em>1m\u91cf</em><b>${fmtUsd(item.volume_usd)}</b></span>
                <span><em>${L.updated}</em><b>${escapeHtml(fmtTime(item.ts))}</b></span>
              </div>
            </div>`;
          })
          .join("")}
      </div>`;
  }

  function renderAbnormalView() {
    return `${renderAbnormalHead()}<div class="ca-abnormal-content">${renderAbnormalContent()}</div>`;
  }

  function renderNew1mView() {
    return `${renderNew1mHead()}<div class="ca-new1m-content">${renderNew1mContent()}</div>`;
  }

  function updateAbnormalContent() {
    const container = panel.querySelector(".ca-abnormal-content");
    if (STATE.view !== "abnormal" || !container) return false;
    const body = panel.querySelector(".ca-cluster-body");
    const scrollTop = body ? body.scrollTop : 0;
    container.innerHTML = renderAbnormalContent();
    attachAbnormalRowHandlers();
    if (body) body.scrollTop = scrollTop;
    return true;
  }

  function updateNew1mContent() {
    const container = panel.querySelector(".ca-new1m-content");
    if (STATE.view !== "new1m" || !container) return false;
    const body = panel.querySelector(".ca-cluster-body");
    const scrollTop = body ? body.scrollTop : 0;
    container.innerHTML = renderNew1mContent();
    attachAbnormalRowHandlers();
    if (body) body.scrollTop = scrollTop;
    return true;
  }

  function render() {
    panel.className = `ca-cluster-panel${STATE.collapsed ? " ca-collapsed" : ""}`;
    const caBody = STATE.loading
      ? `<div class="ca-cluster-loading">${L.analyzing} ${escapeHtml(shortCa(STATE.ca))}</div>`
      : STATE.error
        ? `<div class="ca-cluster-error">${escapeHtml(STATE.error)}</div>`
        : STATE.result
          ? renderResult(STATE.result)
          : `<div class="ca-cluster-empty">${L.openHint}</div>`;
    const body = STATE.view === "abnormal" ? renderAbnormalView() : STATE.view === "new1m" ? renderNew1mView() : caBody;
    const inputRow =
      STATE.view === "ca"
        ? `<div class="ca-cluster-input-row">
          <input class="ca-cluster-input" value="${escapeAttr(STATE.ca)}" placeholder="Token CA" />
          <button class="ca-cluster-button ca-cluster-run" title="${L.analyze}">${L.run}</button>
        </div>`
        : "";

    panel.innerHTML = `
      <div class="ca-cluster-header">
        <div class="ca-cluster-title">${L.title}</div>
        <div class="ca-cluster-actions">
          <button class="ca-cluster-button ca-service-toggle" title="${STATE.serviceBaseUrl || ""}">${STATE.serviceMode === "server" ? L.serviceServer : L.serviceLocal}</button>
          <button class="ca-cluster-button ca-api-key" title="${STATE.hasApiKey ? L.apiKeySet : L.apiKeyUnset}">${L.apiKey}</button>
          <button class="ca-cluster-button ca-cluster-refresh" title="${L.refresh}">R</button>
          <button class="ca-cluster-button ca-cluster-clear" title="${L.clear}">C</button>
          <button class="ca-cluster-button ca-cluster-toggle" title="${L.collapse}">${STATE.collapsed ? "+" : "-"}</button>
        </div>
      </div>
      <div class="ca-cluster-body">
        ${inputRow}
        ${body}
      </div>
      <div class="ca-bottom-tabs">
        <button class="ca-view-button${STATE.view === "ca" ? " is-active" : ""}" data-view="ca">${L.caView}</button>
        <button class="ca-view-button${STATE.view === "abnormal" ? " is-active" : ""}" data-view="abnormal">${L.abnormalView}</button>
        <button class="ca-view-button${STATE.view === "new1m" ? " is-active" : ""}" data-view="new1m">${L.new1mView}</button>
      </div>
      <div class="ca-resize-handle" title="Resize"></div>
    `;

    panel.querySelector(".ca-cluster-toggle")?.addEventListener("click", () => {
      STATE.collapsed = !STATE.collapsed;
      render();
    });
    panel.querySelector(".ca-service-toggle")?.addEventListener("click", () => toggleServiceMode());
    panel.querySelector(".ca-api-key")?.addEventListener("click", () => setApiKey());
    panel.querySelector(".ca-cluster-refresh")?.addEventListener("click", () => analyze(STATE.ca, true));
    panel.querySelector(".ca-abnormal-refresh")?.addEventListener("click", () => loadBottomWatchlist(true));
    panel.querySelector(".ca-new1m-refresh")?.addEventListener("click", () => loadPluginNew1m(true));
    panel.querySelector(".ca-cluster-clear")?.addEventListener("click", () => clearPanel());
    panel.querySelector(".ca-cluster-run")?.addEventListener("click", () => {
      const value = panel.querySelector(".ca-cluster-input")?.value?.trim() || "";
      analyze(value, true);
    });
    attachDragHandlers();
    attachResizeHandlers();
    panel.querySelectorAll(".ca-view-button").forEach((button) => {
      button.addEventListener("click", () => {
        const view = button.getAttribute("data-view") || "ca";
        STATE.view = view;
        render();
        if (view === "abnormal") {
          startAbnormalAutoRefresh();
          stopNew1mAutoRefresh();
          if (!STATE.abnormalItems.length && !STATE.abnormalLoading) {
            loadBottomWatchlist(false);
          }
        } else if (view === "new1m") {
          stopAbnormalAutoRefresh();
          startNew1mAutoRefresh();
          if (!STATE.new1mItems.length && !STATE.new1mLoading) {
            loadPluginNew1m(false);
          }
        } else {
          stopAbnormalAutoRefresh();
          stopNew1mAutoRefresh();
        }
      });
    });
    attachAbnormalRowHandlers();
    panel.querySelectorAll(".ca-copy-button").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        copyText(button.getAttribute("data-copy") || "");
      });
    });
  }

  function attachAbnormalRowHandlers() {
    panel.querySelectorAll(".ca-watch-ca").forEach((button) => {
      if (button.dataset.caReady === "1") return;
      button.dataset.caReady = "1";
      button.addEventListener("click", () => {
        const ca = button.getAttribute("data-ca") || "";
        if (!ca) return;
        STATE.view = "ca";
        STATE.ca = ca;
        STATE.result = null;
        STATE.error = "";
        render();
        window.setTimeout(() => {
          const input = panel.querySelector(".ca-cluster-input");
          input?.focus();
          input?.select();
        }, 0);
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

  function normalizeBottomAbnormal(item) {
    if (!item || item.source !== "bottom_abnormal") return null;
    const extra = item.extra || {};
    const signalType = String(extra.signal_type || "");
    if (signalType === "watch") return null;
    const ca = String(extra.address || item.ca || "").trim();
    if (!ca) return null;
    const currentMcap = toNumber(extra.current_mcap);
    const athMcap = toNumber(extra.ath_mcap);
    const maxMcap = Math.max(
      currentMcap,
      athMcap,
      toNumber(extra.max_abnormal_mcap),
      toNumber(extra.first_signal_mcap),
    );
    return {
      id: item.id || `${ca}:${item.ts || ""}`,
      ca,
      ts: toNumber(item.ts),
      symbol: extra.symbol || item.title || "UNKNOWN",
      source: item.source || "",
      status: item.status || "",
      signal_type: signalType,
      abnormal_rule: extra.abnormal_rule || "",
      narrative: extra.narrative || extra.narrative_desc || item.text || "",
      current_mcap: currentMcap,
      max_mcap: maxMcap,
      ath_mcap: athMcap,
      liquidity: toNumber(extra.pool_total_liquidity || extra.pool_liquidity),
      price_change_pct: toNumber(extra.price_change_pct),
      age_sec: toNumber(extra.age_sec),
      pool_mcap_ratio: toNumber(extra.pool_mcap_ratio),
    };
  }

  function normalizePluginNew1m(item) {
    if (!item || item.source !== "plugin_new_1m") return null;
    const extra = item.extra || {};
    const ca = String(extra.address || item.ca || "").trim();
    if (!ca) return null;
    return {
      id: item.id || `${ca}:${item.ts || ""}`,
      ca,
      ts: toNumber(item.ts),
      symbol: extra.symbol || item.title || "UNKNOWN",
      source: item.source || "",
      status: item.status || "",
      signal_type: extra.signal_type || "new_1m",
      abnormal_rule: extra.abnormal_rule || "",
      chain: extra.chain || "sol",
      current_mcap: toNumber(extra.current_mcap),
      liquidity: toNumber(extra.pool_total_liquidity || extra.pool_liquidity),
      price_change_pct: toNumber(extra.price_change_pct),
      bottom_to_current_pct: toNumber(extra.bottom_to_current_pct),
      volume_usd: toNumber(extra.volume_usd),
      age_sec: toNumber(extra.age_sec),
    };
  }

  async function loadBottomWatchlist(force) {
    if (!force && (STATE.abnormalLoading || STATE.abnormalItems.length)) return;
    const hasRows = STATE.abnormalItems.length > 0;
    STATE.abnormalLoading = true;
    STATE.abnormalError = "";
    if (!hasRows) {
      if (!updateAbnormalContent()) render();
    }
    try {
      const response = await chrome.runtime.sendMessage({ type: "GET_BOTTOM_ABNORMAL", limit: 300 });
      if (!response || !response.ok) {
        throw new Error((response && response.error) || "No response from extension background worker.");
      }
      STATE.serviceMode = response.mode || STATE.serviceMode;
      STATE.serviceBaseUrl = response.baseUrl || STATE.serviceBaseUrl;
      const seen = new Set();
      STATE.abnormalItems = (Array.isArray(response.data?.items) ? response.data.items : [])
        .map(normalizeBottomAbnormal)
        .filter((item) => {
          if (!item || seen.has(item.ca)) return false;
          seen.add(item.ca);
          return true;
        })
        .sort((a, b) => (b.ts || 0) - (a.ts || 0));
      STATE.abnormalLastCount = STATE.abnormalItems.length;
    } catch (err) {
      if (!hasRows) STATE.abnormalItems = [];
      STATE.abnormalError = `${L.apiError}: ${err.message || err}`;
    } finally {
      STATE.abnormalLoading = false;
      if (!updateAbnormalContent()) render();
    }
  }

  function startAbnormalAutoRefresh() {
    if (STATE.abnormalTimer) return;
    STATE.abnormalTimer = window.setInterval(() => {
      if (STATE.view === "abnormal" && !STATE.abnormalLoading) {
        loadBottomWatchlist(true);
      }
    }, 10000);
  }

  function stopAbnormalAutoRefresh() {
    if (!STATE.abnormalTimer) return;
    window.clearInterval(STATE.abnormalTimer);
    STATE.abnormalTimer = 0;
  }

  async function loadPluginNew1m(force) {
    if (!force && (STATE.new1mLoading || STATE.new1mItems.length)) return;
    const hasRows = STATE.new1mItems.length > 0;
    STATE.new1mLoading = true;
    STATE.new1mError = "";
    if (!hasRows) {
      if (!updateNew1mContent()) render();
    }
    try {
      const response = await chrome.runtime.sendMessage({ type: "GET_PLUGIN_NEW_1M", limit: 200 });
      if (!response || !response.ok) {
        const where = response && (response.baseUrl || response.mode || response.status)
          ? ` [${response.mode || "?"} ${response.baseUrl || ""} HTTP ${response.status || 0}]`
          : "";
        throw new Error(`${(response && response.error) || "No response from extension background worker."}${where}`);
      }
      STATE.serviceMode = response.mode || STATE.serviceMode;
      STATE.serviceBaseUrl = response.baseUrl || STATE.serviceBaseUrl;
      const seen = new Set();
      STATE.new1mItems = (Array.isArray(response.data?.items) ? response.data.items : [])
        .map(normalizePluginNew1m)
        .filter((item) => {
          if (!item || seen.has(item.ca)) return false;
          seen.add(item.ca);
          return true;
        })
        .sort((a, b) => (b.ts || 0) - (a.ts || 0));
    } catch (err) {
      if (!hasRows) STATE.new1mItems = [];
      STATE.new1mError = `${L.new1mApiError}: ${err.message || err}`;
    } finally {
      STATE.new1mLoading = false;
      if (!updateNew1mContent()) render();
    }
  }

  function startNew1mAutoRefresh() {
    if (STATE.new1mTimer) return;
    STATE.new1mTimer = window.setInterval(() => {
      if (STATE.view === "new1m" && !STATE.new1mLoading) {
        loadPluginNew1m(true);
      }
    }, 10000);
  }

  function stopNew1mAutoRefresh() {
    if (!STATE.new1mTimer) return;
    window.clearInterval(STATE.new1mTimer);
    STATE.new1mTimer = 0;
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
        STATE.hasApiKey = Boolean(response.hasApiKey);
        render();
      }
    } catch {}
  }

  async function setApiKey() {
    const apiKey = window.prompt(L.apiKeyPrompt, "");
    if (apiKey === null) return;
    try {
      const response = await chrome.runtime.sendMessage({ type: "SET_API_KEY", apiKey });
      if (response && response.ok) {
        STATE.serviceMode = response.mode || STATE.serviceMode;
        STATE.serviceBaseUrl = response.baseUrl || STATE.serviceBaseUrl;
        STATE.hasApiKey = Boolean(response.hasApiKey);
        STATE.error = "";
        STATE.abnormalError = "";
        STATE.new1mError = "";
        render();
      }
    } catch (err) {
      STATE.error = `${L.apiError}: ${err.message || err}`;
      render();
    }
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

  function restorePanelLayout() {
    try {
      const saved = JSON.parse(localStorage.getItem(LAYOUT_KEY) || localStorage.getItem(POS_KEY) || "null");
      if (saved && Number.isFinite(saved.left) && Number.isFinite(saved.top)) {
        applyPanelLayout(
          saved.left,
          saved.top,
          Number.isFinite(saved.width) ? saved.width : DEFAULT_WIDTH,
          Number.isFinite(saved.height) ? saved.height : DEFAULT_HEIGHT,
        );
        return;
      }
    } catch {}
    const left = Math.max(8, window.innerWidth - 354);
    applyPanelLayout(left, 92, DEFAULT_WIDTH, DEFAULT_HEIGHT);
  }

  function applyPanelPosition(left, top) {
    applyPanelLayout(left, top, panel.offsetWidth || DEFAULT_WIDTH, panel.offsetHeight || DEFAULT_HEIGHT);
  }

  function applyPanelLayout(left, top, width, height) {
    const safeWidth = Math.min(Math.max(MIN_WIDTH, width), Math.max(MIN_WIDTH, window.innerWidth - 16));
    const safeHeight = Math.min(Math.max(MIN_HEIGHT, height), Math.max(MIN_HEIGHT, window.innerHeight - 16));
    panel.style.width = `${safeWidth}px`;
    panel.style.height = `${safeHeight}px`;
    panel.style.maxHeight = "none";

    const maxLeft = Math.max(8, window.innerWidth - safeWidth - 8);
    const maxTop = Math.max(8, window.innerHeight - safeHeight - 8);
    const x = Math.min(Math.max(8, left), maxLeft);
    const y = Math.min(Math.max(8, top), maxTop);
    panel.style.left = `${x}px`;
    panel.style.top = `${y}px`;
    panel.style.right = "auto";
  }

  function savePanelLayout() {
    const rect = panel.getBoundingClientRect();
    localStorage.setItem(LAYOUT_KEY, JSON.stringify({ left: rect.left, top: rect.top, width: rect.width, height: rect.height }));
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
        savePanelLayout();
        header.removeEventListener("pointermove", onMove);
        header.removeEventListener("pointerup", onUp);
        header.removeEventListener("pointercancel", onUp);
      };

      header.addEventListener("pointermove", onMove);
      header.addEventListener("pointerup", onUp);
      header.addEventListener("pointercancel", onUp);
    });
  }

  function attachResizeHandlers() {
    const handle = panel.querySelector(".ca-resize-handle");
    if (!handle || handle.dataset.resizeReady === "1") return;
    handle.dataset.resizeReady = "1";
    handle.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const rect = panel.getBoundingClientRect();
      const startX = event.clientX;
      const startY = event.clientY;
      const startWidth = rect.width;
      const startHeight = rect.height;
      const startLeft = rect.left;
      const startTop = rect.top;
      STATE.resizing = true;
      handle.setPointerCapture(event.pointerId);

      const onMove = (moveEvent) => {
        if (!STATE.resizing) return;
        applyPanelLayout(
          startLeft,
          startTop,
          startWidth + moveEvent.clientX - startX,
          startHeight + moveEvent.clientY - startY,
        );
      };
      const onUp = () => {
        STATE.resizing = false;
        savePanelLayout();
        handle.removeEventListener("pointermove", onMove);
        handle.removeEventListener("pointerup", onUp);
        handle.removeEventListener("pointercancel", onUp);
      };

      handle.addEventListener("pointermove", onMove);
      handle.addEventListener("pointerup", onUp);
      handle.addEventListener("pointercancel", onUp);
    });
  }

  render();
  loadServiceConfig();
})();
