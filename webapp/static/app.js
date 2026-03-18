const state = {
  files: [],
  session: null,
  currentFile: "",
  chunkSize: 512,
  chunkCache: new Map(),
  currentChunkIndex: 0,
  currentIndex: 0,
  virtualTime: 0,
  realtimeTimerId: null,
  activeSection: "board",
  chartMode: "line",
  chartTimeframe: "1m",
  chartBarsVisible: 20,
  lastBoardOrderSig: "",
  lastBoardOrderAtMs: 0,
};

const els = {
  appHeader: document.querySelector(".app-header"),
  appMain: document.querySelector(".app-main"),
  errorBanner: document.getElementById("error-banner"),
  boardBody: document.getElementById("board-body"),
  tradesBody: document.getElementById("trades-body"),
  sectionViewport: document.getElementById("section-viewport"),
  sectionTrack: document.getElementById("section-track"),
  pendingBody: document.getElementById("pending-body"),
  logBody: document.getElementById("log-body"),
  chartCanvas: document.getElementById("chart-canvas"),
  chartLine: document.getElementById("chart-line"),
  tf1m: document.getElementById("tf-1m"),
  tf5m: document.getElementById("tf-5m"),
  zoomOut: document.getElementById("zoom-out"),
  zoomIn: document.getElementById("zoom-in"),
  showBoard: document.getElementById("show-board"),
  showChart: document.getElementById("show-chart"),
  showTrades: document.getElementById("show-trades"),
  cancelOrdersBtn: document.getElementById("cancel-orders-btn"),
  emptyState: document.getElementById("empty-state"),
  metaTime: document.getElementById("meta-time"),
  bestBid: document.getElementById("best-bid"),
  bestAsk: document.getElementById("best-ask"),
  headlineCash: document.getElementById("headline-cash"),
  speedLabel: document.getElementById("speed-label"),
  sumCash: document.getElementById("sum-cash"),
  sumPos: document.getElementById("sum-pos"),
  sumAvg: document.getElementById("sum-avg"),
  sumReal: document.getElementById("sum-real"),
  sumUnreal: document.getElementById("sum-unreal"),
  sumEquity: document.getElementById("sum-equity"),
  shareXBtn: document.getElementById("share-x-btn"),
};

const soundPaths = {
  place: "/voice/hattyu.wav",
  fill: "/voice/yakujo.wav",
  profit: "/voice/kati.wav",
  loss: "/voice/make.wav",
};

const soundPriority = {
  place: 0,
  fill: 1,
  profit: 2,
  loss: 2,
};

const soundVolumes = {
  place: 0.08,
  fill: 0.09,
  profit: 0.1,
  loss: 0.1,
};

const soundPool = new Map();
let audioUnlocked = false;

class TradingEngine {
  constructor(initialCash = 1_000_000, lotSize = 100) {
    this.initialCash = initialCash;
    this.lotSize = lotSize;
    this.reset();
  }

  reset() {
    this.cash = this.initialCash;
    this.position = 0;
    this.avgPrice = 0;
    this.realizedPnl = 0;
    this.lastPrice = 0;
    this.lastTradePrice = 0;
    this.bestBid = 0;
    this.bestAsk = 0;
    this.nextOid = 1;
    this.pending = [];
    this.logRows = [];
    this.pushRow({
      timeMicro: 0,
      kind: "RESET",
      side: "-",
      price: 0,
      qtyLot: 0,
      reason: "engine-reset",
      posAfter: 0,
      avgAfter: 0,
      realizedDelta: 0,
      realizedTotal: 0,
    });
  }

  configure(initialCash, lotSize = this.lotSize) {
    this.initialCash = initialCash;
    this.lotSize = lotSize;
    this.reset();
  }

  pushRow(row) {
    this.logRows.push(row);
    if (this.logRows.length > 2000) {
      this.logRows = this.logRows.slice(-2000);
    }
  }

  pendingPrices() {
    return new Set(this.pending.map((order) => order.limitPrice));
  }

  cancelAll() {
    const count = this.pending.length;
    this.pending = [];
    this.pushRow({
      timeMicro: 0,
      kind: "CANCEL",
      side: "-",
      price: 0,
      qtyLot: count,
      reason: "cancel-all",
      posAfter: this.position,
      avgAfter: this.avgPrice,
      realizedDelta: 0,
      realizedTotal: this.realizedPnl,
    });
  }

  markPrice() {
    return this.lastTradePrice > 0 ? this.lastTradePrice : this.lastPrice;
  }

  equity() {
    return this.cash + this.position * this.markPrice() * this.lotSize;
  }

  unrealized() {
    if (this.position === 0) {
      return 0;
    }
    const mark = this.markPrice();
    if (this.position > 0) {
      return (mark - this.avgPrice) * this.lotSize * this.position;
    }
    return (this.avgPrice - mark) * this.lotSize * Math.abs(this.position);
  }

  snapshot() {
    return {
      cash: this.cash,
      position: this.position,
      avgPrice: this.avgPrice,
      realizedPnl: this.realizedPnl,
      unrealizedPnl: this.unrealized(),
      equity: this.equity(),
    };
  }

  applyFill(side, price, reason, timeMicro) {
    const qty = side === "BUY" ? 1 : -1;
    let realizedDelta = 0;

    if (side === "BUY") {
      this.cash -= price * this.lotSize;
    } else {
      this.cash += price * this.lotSize;
    }

    const prevPos = this.position;
    const nextPos = prevPos + qty;

    if (prevPos === 0) {
      this.position = nextPos;
      this.avgPrice = price;
    } else if (prevPos > 0) {
      if (qty > 0) {
        this.avgPrice = (this.avgPrice * prevPos + price) / nextPos;
        this.position = nextPos;
      } else {
        realizedDelta = (price - this.avgPrice) * this.lotSize;
        this.realizedPnl += realizedDelta;
        this.position = nextPos;
        this.avgPrice = this.position === 0 ? 0 : price;
      }
    } else if (qty < 0) {
      this.avgPrice = (this.avgPrice * Math.abs(prevPos) + price) / Math.abs(nextPos);
      this.position = nextPos;
    } else {
      realizedDelta = (this.avgPrice - price) * this.lotSize;
      this.realizedPnl += realizedDelta;
      this.position = nextPos;
      this.avgPrice = this.position === 0 ? 0 : price;
    }

    this.pushRow({
      timeMicro,
      kind: "FILL",
      side,
      price,
      qtyLot: 1,
      reason,
      posAfter: this.position,
      avgAfter: this.avgPrice,
      realizedDelta,
      realizedTotal: this.realizedPnl,
    });

    if (realizedDelta > 0) {
      return "profit";
    }
    if (realizedDelta < 0) {
      return "loss";
    }
    return "fill";
  }

  canPlace(side, price) {
    const referencePrice = Math.max(1, price, this.markPrice(), this.bestAsk, this.bestBid);
    const pendingSameSide = this.pending.filter((order) => order.side === side).length;
    const nextDirectionalLots = side === "BUY"
      ? Math.max(0, this.position) + pendingSameSide + 1
      : Math.max(0, -this.position) + pendingSameSide + 1;
    const notional = nextDirectionalLots * referencePrice * this.lotSize;
    return notional <= Math.max(0, this.cash);
  }

  placeLimit(side, limitPrice, timeMicro) {
    const price = Number(limitPrice);
    if (!this.canPlace(side, price)) {
      this.pushRow({
        timeMicro,
        kind: "REJECT",
        side,
        price,
        qtyLot: 1,
        reason: "cash-limit",
        posAfter: this.position,
        avgAfter: this.avgPrice,
        realizedDelta: 0,
        realizedTotal: this.realizedPnl,
      });
      return null;
    }
    if (side === "BUY" && this.bestAsk > 0 && price >= this.bestAsk) {
      return this.applyFill("BUY", this.bestAsk, "marketable-limit", timeMicro);
    }
    if (side === "SELL" && this.bestBid > 0 && price <= this.bestBid) {
      return this.applyFill("SELL", this.bestBid, "marketable-limit", timeMicro);
    }
    const order = { oid: this.nextOid, side, limitPrice: price, placedTimeMicro: timeMicro };
    this.nextOid += 1;
    this.pending.push(order);
    this.pushRow({
      timeMicro,
      kind: "PLACE",
      side,
      price,
      qtyLot: 1,
      reason: `id#${order.oid}`,
      posAfter: this.position,
      avgAfter: this.avgPrice,
      realizedDelta: 0,
      realizedTotal: this.realizedPnl,
    });
    return "place";
  }

  onMarket({ timeMicro, bestBid, bestAsk, lastPrice, eventType, tradePrice }) {
    this.bestBid = bestBid > 0 ? bestBid : 0;
    this.bestAsk = bestAsk > 0 ? bestAsk : 0;
    this.lastPrice = lastPrice > 0 ? lastPrice : this.lastPrice;
    if (eventType === 2 && tradePrice > 0) {
      this.lastTradePrice = tradePrice;
    }
    if (this.pending.length === 0) {
      return null;
    }
    let sound = null;
    const remain = [];
    for (const order of this.pending) {
      if (order.side === "BUY") {
        if (this.bestAsk > 0 && this.bestAsk <= order.limitPrice) {
          sound = preferSound(sound, this.applyFill("BUY", this.bestAsk, `touch#${order.oid}`, timeMicro));
        } else {
          remain.push(order);
        }
      } else if (this.bestBid > 0 && this.bestBid >= order.limitPrice) {
        sound = preferSound(sound, this.applyFill("SELL", this.bestBid, `touch#${order.oid}`, timeMicro));
      } else {
        remain.push(order);
      }
    }
    this.pending = remain;
    return sound;
  }

  syncQuote({ bestBid, bestAsk, lastPrice, eventType, tradePrice }) {
    this.bestBid = bestBid > 0 ? bestBid : 0;
    this.bestAsk = bestAsk > 0 ? bestAsk : 0;
    this.lastPrice = lastPrice > 0 ? lastPrice : this.lastPrice;
    if (eventType === 2 && tradePrice > 0) {
      this.lastTradePrice = tradePrice;
    }
  }
}

const engine = new TradingEngine();

function initialCashForSession(session, lotSize = 100) {
  const firstClose = session?.chart?.["1m"]?.closes?.[0] ?? 0;
  const baseNotional = Math.max(1, firstClose) * lotSize;
  return Math.max(10_000_000, baseNotional * 8);
}

function formatTime(timeMicro) {
  if (!timeMicro || timeMicro <= 0) {
    return "--:--:--";
  }
  const totalSec = Math.floor(timeMicro / 1_000_000);
  const hh = String(Math.floor(totalSec / 3600)).padStart(2, "0");
  const mm = String(Math.floor((totalSec % 3600) / 60)).padStart(2, "0");
  const ss = String(totalSec % 60).padStart(2, "0");
  return `${hh}:${mm}:${ss}`;
}

function formatInt(value) {
  if (!value) {
    return "-";
  }
  return Number(value).toLocaleString("ja-JP");
}

function formatSignedYen(value) {
  return Number(value).toLocaleString("ja-JP", {
    signDisplay: "always",
    maximumFractionDigits: 0,
  });
}

function upperBound(arr, target) {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] <= target) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

function bestPrices(asks, bids) {
  const askPrices = asks.map((row) => row[0]).filter((value) => value > 0);
  const bidPrices = bids.map((row) => row[0]).filter((value) => value > 0);
  return {
    bestBid: bidPrices.length ? Math.max(...bidPrices) : 0,
    bestAsk: askPrices.length ? Math.min(...askPrices) : 0,
  };
}

function dayTimeMicro(now = new Date()) {
  return (
    ((now.getHours() * 60 + now.getMinutes()) * 60 + now.getSeconds()) * 1_000_000 +
    now.getMilliseconds() * 1000
  );
}

function showError(message) {
  if (!els.errorBanner) return;
  els.errorBanner.textContent = message;
  els.errorBanner.classList.remove("hidden");
}

function clearError() {
  if (!els.errorBanner) return;
  els.errorBanner.textContent = "";
  els.errorBanner.classList.add("hidden");
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function preprocessSession(session) {
  session.maxIndex = Math.max(0, session.rowCount - 1);
  session.chunkCount = session.chunkFirstTimes.length;
  return session;
}

async function loadFiles() {
  clearError();
  state.files = await fetchJson("/api/files");
  const hasFiles = state.files.length > 0;
  els.emptyState.classList.toggle("hidden", hasFiles);
  els.appHeader.classList.toggle("hidden", !hasFiles);
  if (hasFiles) {
    await loadSession(pickDefaultFile(state.files).name);
  }
}

function pickDefaultFile(files) {
  const livePattern = /(24h|live|realtime)/i;
  return files.find((file) => livePattern.test(file.name)) ?? files[0];
}

async function loadSession(filename) {
  clearError();
  const raw = await fetchJson(`/api/session-summary?file=${encodeURIComponent(filename)}`);
  state.session = preprocessSession(raw);
  state.currentFile = filename;
  state.chunkSize = raw.chunkSize || 512;
  state.chunkCache = new Map();
  engine.configure(initialCashForSession(state.session), 100);
  await syncToRealClock();
  await updateView();
  startRealtimeClock();
}

function chunkIndexForRow(rowIndex) {
  return Math.floor(rowIndex / state.chunkSize);
}

function chunkIndexForTime(targetTime) {
  if (!state.session) return 0;
  const idx = upperBound(state.session.chunkLastTimes, targetTime);
  return Math.max(0, Math.min(idx, state.session.chunkCount - 1));
}

async function ensureChunk(chunkIndex) {
  if (state.chunkCache.has(chunkIndex)) {
    return state.chunkCache.get(chunkIndex);
  }
  const payload = await fetchJson(
    `/api/session-chunk?file=${encodeURIComponent(state.currentFile)}&chunk=${chunkIndex}`
  );
  state.chunkCache.set(chunkIndex, payload);
  return payload;
}

async function ensureChunkForIndex(rowIndex) {
  if (!state.session) return null;
  const chunkIndex = chunkIndexForRow(rowIndex);
  const chunk = await ensureChunk(chunkIndex);
  if (rowIndex >= chunk.end - 24 && chunk.end < state.session.rowCount) {
    ensureChunk(chunkIndex + 1).catch(() => {});
  }
  return chunk;
}

function frameAt(index) {
  const chunkIndex = chunkIndexForRow(index);
  const chunk = state.chunkCache.get(chunkIndex);
  if (!chunk) return null;
  const offset = index - chunk.start;
  return {
    time: chunk.times[offset],
    event: chunk.events[offset],
    price: chunk.prices[offset],
    size: chunk.sizes[offset],
    direction: chunk.directions[offset],
    asks: chunk.asks[offset],
    bids: chunk.bids[offset],
  };
}

function indexInChunkForTime(chunk, targetTime) {
  const local = Math.max(0, upperBound(chunk.times, targetTime) - 1);
  return Math.min(chunk.end - chunk.start - 1, local);
}

async function syncToRealClock() {
  if (!state.session) return;
  state.virtualTime = dayTimeMicro();
  const chunkIndex = chunkIndexForTime(state.virtualTime);
  const chunk = await ensureChunk(chunkIndex);
  state.currentChunkIndex = chunkIndex;
  state.currentIndex = chunk.start + indexInChunkForTime(chunk, state.virtualTime);
}

async function collectTradeRows(limit = 120) {
  const rows = [];
  for (let chunkIndex = state.currentChunkIndex; chunkIndex >= 0 && rows.length < limit; chunkIndex -= 1) {
    const chunk = await ensureChunk(chunkIndex);
    const maxOffset = chunkIndex === state.currentChunkIndex ? state.currentIndex - chunk.start : chunk.end - chunk.start - 1;
    for (let offset = maxOffset; offset >= 0 && rows.length < limit; offset -= 1) {
      if (chunk.events[offset] !== 2) continue;
      const price = chunk.prices[offset];
      let side = 0;
      if (offset > 0 && chunk.prices[offset - 1] !== undefined) {
        side = price > chunk.prices[offset - 1] ? 1 : price < chunk.prices[offset - 1] ? -1 : 0;
      }
      if (side === 0) {
        side = chunk.directions[offset] < 0 ? 1 : -1;
      }
      rows.push({
        time: chunk.times[offset],
        price,
        size: chunk.sizes[offset],
        side,
      });
    }
  }
  return rows;
}

function latestTradePriceFromCache() {
  for (let chunkIndex = state.currentChunkIndex; chunkIndex >= 0; chunkIndex -= 1) {
    const chunk = state.chunkCache.get(chunkIndex);
    if (!chunk) continue;
    const maxOffset = chunkIndex === state.currentChunkIndex ? state.currentIndex - chunk.start : chunk.end - chunk.start - 1;
    for (let offset = maxOffset; offset >= 0; offset -= 1) {
      if (chunk.events[offset] === 2) {
        return chunk.prices[offset];
      }
    }
  }
  return 0;
}

function estimateTickSize(prices) {
  const unique = Array.from(new Set(prices.filter((price) => Number.isFinite(price) && price > 0))).sort((a, b) => a - b);
  if (unique.length < 2) return 1;
  let tick = Infinity;
  for (let i = 1; i < unique.length; i += 1) {
    const diff = unique[i] - unique[i - 1];
    if (diff > 0 && diff < tick) tick = diff;
  }
  return Number.isFinite(tick) ? tick : 1;
}

function buildBoardLadder(priceSet, tickSize, anchorPrice, rows = 20) {
  const prices = Array.from(priceSet).filter((price) => Number.isFinite(price) && price > 0).sort((a, b) => b - a);
  if (prices.length === 0) return [];
  const tick = Math.max(1, tickSize);
  const topPrice = prices[0];
  const bottomPrice = prices[prices.length - 1];
  const ladder = [];
  for (let price = topPrice; price >= bottomPrice; price -= tick) {
    ladder.push(price);
  }
  if (ladder.length <= rows) return ladder;
  const target = Number.isFinite(anchorPrice) && anchorPrice > 0 ? anchorPrice : ladder[Math.floor(ladder.length / 2)];
  let nearestIndex = 0;
  let nearestDelta = Infinity;
  for (let i = 0; i < ladder.length; i += 1) {
    const delta = Math.abs(ladder[i] - target);
    if (delta < nearestDelta) {
      nearestDelta = delta;
      nearestIndex = i;
    }
  }
  const start = Math.max(0, Math.min(ladder.length - rows, nearestIndex - Math.floor(rows * 0.4)));
  return ladder.slice(start, start + rows);
}

function renderBoard(frame, highlightPrice) {
  const askMap = new Map();
  const bidMap = new Map();
  const prices = new Set();
  for (const level of frame.asks) {
    const [price, qty, orderCount] = level;
    if (price > 0) {
      prices.add(price);
      askMap.set(price, { qty, orderCount });
    }
  }
  for (const level of frame.bids) {
    const [price, qty, orderCount] = level;
    if (price > 0) {
      prices.add(price);
      bidMap.set(price, { qty, orderCount });
    }
  }
  const { bestBid, bestAsk } = bestPrices(frame.asks, frame.bids);
  const lastTradePrice = latestTradePriceFromCache();
  const tickSize = estimateTickSize([...Array.from(prices), lastTradePrice, bestBid, bestAsk]);
  const sorted = buildBoardLadder(prices, tickSize, bestAsk || lastTradePrice || bestBid, 20);
  const pendingPrices = engine.pendingPrices();
  const fillCell = (value) => (value > 0 ? formatInt(value) : "");

  const rows = sorted.map((price) => {
    const ask = askMap.get(price) ?? { qty: 0, orderCount: 0 };
    const bid = bidMap.get(price) ?? { qty: 0, orderCount: 0 };
    const classes = ["price-cell"];
    if (bestAsk > 0 && price >= bestAsk) classes.push("ask-price");
    else if (bestBid > 0 && price <= bestBid) classes.push("bid-price");
    if (pendingPrices.has(price)) classes.push("pending");
    if (lastTradePrice === price) classes.push("last-trade");
    if (highlightPrice > 0 && highlightPrice === price) classes.push("highlight");

    return `
      <tr>
        <td class="ask-order">${fillCell(ask.orderCount)}</td>
        <td class="ask-qty"><button type="button" class="${ask.qty > 0 ? "" : "empty-level"}" data-side="SELL" data-price="${price}">${fillCell(ask.qty)}</button></td>
        <td class="${classes.join(" ")}">${formatInt(price)}</td>
        <td class="bid-qty"><button type="button" class="${bid.qty > 0 ? "" : "empty-level"}" data-side="BUY" data-price="${price}">${fillCell(bid.qty)}</button></td>
        <td class="bid-order">${fillCell(bid.orderCount)}</td>
      </tr>
    `;
  });

  while (rows.length < 20) {
    rows.push(`
      <tr>
        <td class="ask-order"></td>
        <td class="ask-qty"><button type="button" disabled></button></td>
        <td class="price-cell"></td>
        <td class="bid-qty"><button type="button" disabled></button></td>
        <td class="bid-order"></td>
      </tr>
    `);
  }
  els.boardBody.innerHTML = rows.join("");
}

function shareTextFromSnapshot() {
  const delta = Math.round(engine.cash - engine.initialCash);
  const firstLine = delta >= 0
    ? `デモトレで ${formatSignedYen(delta)}円得しました！`
    : `デモトレで ${formatSignedYen(delta)}円でした！`;
  return `${firstLine}\n遊んでみる`;
}

async function shareToX() {
  if (!state.session) return;
  const text = shareTextFromSnapshot();
  const shareUrl = window.location.href;
  const intent = new URL("https://twitter.com/intent/tweet");
  intent.searchParams.set("text", text);
  intent.searchParams.set("url", shareUrl);
  const popup = window.open(intent.toString(), "_blank");
  if (!popup) {
    window.location.href = intent.toString();
    return;
  }
  popup.opener = null;
}

async function renderTrades() {
  const rows = await collectTradeRows();
  els.tradesBody.innerHTML = rows.map((row) => `
    <tr>
      <td>${formatTime(row.time)}</td>
      <td>${formatInt(row.price)}</td>
      <td>${formatInt(row.size)}</td>
      <td class="${row.side < 0 ? "sell" : "buy"}">${row.side < 0 ? "売" : "買"}</td>
    </tr>
  `).join("");
}

function renderTrading() {
  const snap = engine.snapshot();
  els.sumCash.textContent = `${formatInt(Math.round(snap.cash))} JPY`;
  els.sumPos.textContent = `${snap.position} lot`;
  els.sumAvg.textContent = snap.avgPrice ? snap.avgPrice.toFixed(1) : "-";
  els.sumReal.textContent = `${formatInt(Math.round(snap.realizedPnl))} JPY`;
  els.sumUnreal.textContent = `${formatInt(Math.round(snap.unrealizedPnl))} JPY`;
  els.sumEquity.textContent = `${formatInt(Math.round(snap.equity))} JPY`;

  els.pendingBody.innerHTML = engine.pending.map((order) => `
    <tr>
      <td>${order.oid}</td>
      <td class="${order.side === "BUY" ? "buy" : "sell"}">${order.side === "BUY" ? "買" : "売"}</td>
      <td>${formatInt(order.limitPrice)}</td>
      <td>${formatTime(order.placedTimeMicro)}</td>
    </tr>
  `).join("");

  els.logBody.innerHTML = engine.logRows.slice(-200).reverse().map((row) => `
    <tr>
      <td>${formatTime(row.timeMicro)}</td>
      <td>${row.kind}</td>
      <td class="${row.side === "BUY" ? "buy" : row.side === "SELL" ? "sell" : ""}">${row.side === "BUY" ? "買" : row.side === "SELL" ? "売" : row.side}</td>
      <td>${row.price > 0 ? formatInt(row.price) : "-"}</td>
      <td>${row.qtyLot > 0 ? row.qtyLot : "-"}</td>
      <td>${row.reason}</td>
      <td>${row.posAfter}</td>
      <td>${row.avgAfter > 0 ? row.avgAfter.toFixed(1) : "-"}</td>
      <td>${row.realizedDelta ? row.realizedDelta.toLocaleString("ja-JP", { signDisplay: "always", maximumFractionDigits: 0 }) : "0"}</td>
      <td>${formatInt(Math.round(row.realizedTotal))}</td>
    </tr>
  `).join("");
}

function renderChart() {
  const series = state.session?.chart?.[state.chartTimeframe];
  const canvas = els.chartCanvas;
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#081119";
  ctx.fillRect(0, 0, width, height);
  if (!series || series.bucketTimes.length === 0) {
    ctx.fillStyle = "#edf7fb";
    ctx.font = "16px sans-serif";
    ctx.textAlign = "center";
    ctx.fillText("No chart data", width / 2, height / 2);
    return;
  }

  const end = upperBound(series.bucketTimes, state.virtualTime);
  if (end <= 0) {
    ctx.fillStyle = "#edf7fb";
    ctx.font = "16px sans-serif";
    ctx.textAlign = "center";
    ctx.fillText("Waiting for first trade", width / 2, height / 2);
    return;
  }

  const start = Math.max(0, end - state.chartBarsVisible);
  const times = series.bucketTimes.slice(start, end);
  const opens = series.opens.slice(start, end);
  const highs = series.highs.slice(start, end);
  const lows = series.lows.slice(start, end);
  const closes = series.closes.slice(start, end);
  const pad = { top: 18, right: 56, bottom: 24, left: 14 };
  const tMin = times[0];
  const tMax = Math.max(times[times.length - 1], tMin + 1);
  const pMin = Math.min(...lows);
  const pMax = Math.max(...highs);
  const tRange = Math.max(1, tMax - tMin);
  const pRange = Math.max(1, pMax - pMin);

  ctx.strokeStyle = "rgba(143, 179, 191, 0.15)";
  ctx.lineWidth = 1;
  for (let i = 0; i < 5; i += 1) {
    const y = pad.top + ((height - pad.top - pad.bottom) * i) / 4;
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(width - pad.right, y);
    ctx.stroke();
  }

  if (state.chartMode === "line") {
    ctx.strokeStyle = "#5ec8ff";
    ctx.lineWidth = 2;
    ctx.beginPath();
    for (let i = 0; i < times.length; i += 1) {
      const x = pad.left + ((times[i] - tMin) / tRange) * (width - pad.left - pad.right);
      const y = height - pad.bottom - ((closes[i] - pMin) / pRange) * (height - pad.top - pad.bottom);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  } else {
    const candleGap = Math.max(6, (width - pad.left - pad.right) / Math.max(1, times.length));
    const candleWidth = Math.max(3, candleGap * 0.55);
    for (let i = 0; i < times.length; i += 1) {
      const x = pad.left + ((times[i] - tMin) / tRange) * (width - pad.left - pad.right);
      const openY = height - pad.bottom - ((opens[i] - pMin) / pRange) * (height - pad.top - pad.bottom);
      const highY = height - pad.bottom - ((highs[i] - pMin) / pRange) * (height - pad.top - pad.bottom);
      const lowY = height - pad.bottom - ((lows[i] - pMin) / pRange) * (height - pad.top - pad.bottom);
      const closeY = height - pad.bottom - ((closes[i] - pMin) / pRange) * (height - pad.top - pad.bottom);
      const rising = closes[i] >= opens[i];
      ctx.strokeStyle = rising ? "#ff6b6b" : "#3fd08a";
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.moveTo(x, highY);
      ctx.lineTo(x, lowY);
      ctx.stroke();
      const bodyTop = Math.min(openY, closeY);
      const bodyHeight = Math.max(2, Math.abs(closeY - openY));
      ctx.fillStyle = rising ? "#ff6b6b" : "#3fd08a";
      ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);
    }
  }

  const currentX = pad.left + ((state.virtualTime - tMin) / tRange) * (width - pad.left - pad.right);
  ctx.strokeStyle = "#ff6961";
  ctx.beginPath();
  ctx.moveTo(currentX, pad.top);
  ctx.lineTo(currentX, height - pad.bottom);
  ctx.stroke();

  ctx.fillStyle = "rgba(143, 179, 191, 0.9)";
  ctx.font = "12px sans-serif";
  ctx.textAlign = "center";
  for (let i = 0; i < Math.min(4, times.length); i += 1) {
    const idx = Math.min(times.length - 1, Math.floor((times.length - 1) * (i / Math.max(1, Math.min(3, times.length - 1)))));
    const tickTime = times[idx];
    const x = pad.left + ((tickTime - tMin) / tRange) * (width - pad.left - pad.right);
    ctx.fillText(formatTime(tickTime).slice(0, 5), x, height - 6);
  }

  ctx.textAlign = "right";
  for (let i = 0; i < 5; i += 1) {
    const price = pMax - (pRange * i) / 4;
    const y = pad.top + ((height - pad.top - pad.bottom) * i) / 4 + 4;
    ctx.fillText(formatInt(Math.round(price)), width - 4, y);
  }
}

function updatePanels() {
  const sectionOrder = ["board", "chart", "trades"];
  const sectionIndex = Math.max(0, sectionOrder.indexOf(state.activeSection));
  if (els.sectionTrack && els.sectionViewport) {
    const offset = sectionIndex * els.sectionViewport.clientWidth;
    els.sectionTrack.style.transform = `translateX(-${offset}px)`;
  }
  els.showBoard.classList.toggle("active", state.activeSection === "board");
  els.showChart.classList.toggle("active", state.activeSection === "chart");
  els.showTrades.classList.toggle("active", state.activeSection === "trades");
  els.chartLine?.classList.toggle("active", state.chartMode === "line");
  els.tf1m?.classList.toggle("active", state.chartTimeframe === "1m");
  els.tf5m?.classList.toggle("active", state.chartTimeframe === "5m");
}

function renderPlaybackMeta(bestBid = null, bestAsk = null) {
  if (!state.session) return;
  els.metaTime.textContent = formatTime(state.virtualTime);
  if (bestBid !== null) els.bestBid.textContent = bestBid ? formatInt(bestBid) : "-";
  if (bestAsk !== null) els.bestAsk.textContent = bestAsk ? formatInt(bestAsk) : "-";
  const cash = Math.round(engine.cash);
  els.headlineCash.textContent = formatInt(cash);
  els.headlineCash.classList.toggle("profit", cash > engine.initialCash);
  els.headlineCash.classList.toggle("loss", cash < engine.initialCash);
  if (els.speedLabel) els.speedLabel.textContent = state.chartTimeframe.toUpperCase();
}

async function tickRealtime() {
  if (!state.session) return;
  const prevIndex = state.currentIndex;
  await syncToRealClock();
  if (prevIndex !== state.currentIndex) {
    await updateView();
  } else {
    const frame = frameAt(state.currentIndex);
    if (frame) {
      const { bestBid, bestAsk } = bestPrices(frame.asks, frame.bids);
      renderPlaybackMeta(bestBid, bestAsk);
    }
    if (state.activeSection === "chart") renderChart();
  }
}

function startRealtimeClock() {
  if (state.realtimeTimerId !== null) {
    window.clearInterval(state.realtimeTimerId);
  }
  state.realtimeTimerId = window.setInterval(() => {
    tickRealtime().catch((error) => {
      console.error(error);
      showError(`Runtime error: ${error instanceof Error ? error.message : String(error)}`);
    });
  }, 100);
}

async function handleBoardOrder(event) {
  const button = event.target.closest("button[data-side][data-price]");
  if (!button || !state.session) return;
  event.preventDefault();
  const chunk = await ensureChunkForIndex(state.currentIndex);
  if (!chunk) return;
  const frame = frameAt(state.currentIndex);
  if (!frame) return;
  const orderSig = `${state.currentIndex}:${button.dataset.side}:${button.dataset.price}`;
  const nowMs = performance.now();
  if (state.lastBoardOrderSig === orderSig && nowMs - state.lastBoardOrderAtMs < 250) {
    return;
  }
  state.lastBoardOrderSig = orderSig;
  state.lastBoardOrderAtMs = nowMs;
  const { bestBid, bestAsk } = bestPrices(frame.asks, frame.bids);
  engine.syncQuote({
    bestBid,
    bestAsk,
    lastPrice: frame.price,
    eventType: frame.event,
    tradePrice: frame.price,
  });
  const orderSound = engine.placeLimit(button.dataset.side, Number(button.dataset.price), state.virtualTime);
  beep(orderSound);
  await updateView();
}

function beep(kind) {
  const src = soundPaths[kind];
  if (!src || !audioUnlocked) return;
  let audio = soundPool.get(kind);
  if (!audio) {
    audio = new Audio(src);
    audio.preload = "auto";
    soundPool.set(kind, audio);
  }
  audio.pause();
  audio.currentTime = 0;
  audio.volume = soundVolumes[kind] ?? 0.1;
  audio.play().catch(() => {});
}

function preferSound(currentKind, nextKind) {
  if (!nextKind) return currentKind;
  if (!currentKind) return nextKind;
  return (soundPriority[nextKind] ?? -1) >= (soundPriority[currentKind] ?? -1) ? nextKind : currentKind;
}

async function unlockAudio() {
  if (audioUnlocked) return;
  for (const [kind, src] of Object.entries(soundPaths)) {
    let audio = soundPool.get(kind);
    if (!audio) {
      audio = new Audio(src);
      audio.preload = "auto";
      soundPool.set(kind, audio);
    }
    audio.pause();
    audio.currentTime = 0;
    audio.volume = soundVolumes[kind] ?? 0.1;
    audio.load();
  }
  audioUnlocked = true;
}

function bindEvents() {
  els.boardBody.addEventListener("click", async (event) => {
    await unlockAudio().catch(() => {});
    await handleBoardOrder(event);
  });
  els.cancelOrdersBtn.addEventListener("click", () => {
    engine.cancelAll();
    updateView();
  });
  els.shareXBtn?.addEventListener("click", async () => {
    try {
      await shareToX();
    } catch (error) {
      console.error(error);
      showError(`Share failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  });
  els.chartLine?.addEventListener("click", () => {
    state.chartMode = state.chartMode === "line" ? "candle" : "line";
    els.chartLine.textContent = state.chartMode === "line" ? "通常" : "ローソク";
    updatePanels();
    renderChart();
  });
  els.tf1m?.addEventListener("click", () => {
    state.chartTimeframe = "1m";
    state.chartBarsVisible = 20;
    updatePanels();
    renderChart();
  });
  els.tf5m?.addEventListener("click", () => {
    state.chartTimeframe = "5m";
    state.chartBarsVisible = 20;
    updatePanels();
    renderChart();
  });
  els.zoomIn?.addEventListener("click", () => {
    state.chartBarsVisible = Math.max(8, Math.floor(state.chartBarsVisible * 0.7));
    renderChart();
  });
  els.zoomOut?.addEventListener("click", () => {
    state.chartBarsVisible = Math.min(240, Math.ceil(state.chartBarsVisible * 1.4));
    renderChart();
  });
  els.showBoard.addEventListener("click", () => {
    state.activeSection = "board";
    updatePanels();
  });
  els.showChart.addEventListener("click", () => {
    state.activeSection = "chart";
    updatePanels();
    renderChart();
  });
  els.showTrades.addEventListener("click", () => {
    state.activeSection = "trades";
    updatePanels();
  });

  let swipeStartX = 0;
  let swipeActive = false;
  if (els.sectionViewport) {
    els.sectionViewport.addEventListener("pointerdown", (event) => {
      swipeStartX = event.clientX;
      swipeActive = true;
    });
    els.sectionViewport.addEventListener("pointerup", (event) => {
      if (!swipeActive) return;
      const dx = event.clientX - swipeStartX;
      if (Math.abs(dx) > 40) {
        const sectionOrder = ["board", "chart", "trades"];
        const current = Math.max(0, sectionOrder.indexOf(state.activeSection));
        const next = dx < 0 ? Math.min(sectionOrder.length - 1, current + 1) : Math.max(0, current - 1);
        state.activeSection = sectionOrder[next];
        updatePanels();
        if (state.activeSection === "chart") renderChart();
      }
      swipeActive = false;
    });
    els.sectionViewport.addEventListener("pointercancel", () => {
      swipeActive = false;
    });
  }

  window.addEventListener("resize", () => {
    updatePanels();
    renderChart();
  });
}

async function updateView() {
  try {
    if (!state.session) return;
    await ensureChunkForIndex(state.currentIndex);
    const frame = frameAt(state.currentIndex);
    if (!frame) return;
    const { bestBid, bestAsk } = bestPrices(frame.asks, frame.bids);
    const marketSound = engine.onMarket({
      timeMicro: state.virtualTime,
      bestBid,
      bestAsk,
      lastPrice: frame.price,
      eventType: frame.event,
      tradePrice: frame.price,
    });
    beep(marketSound);
    renderBoard(frame, frame.event === 2 ? frame.price : 0);
    await renderTrades();
    renderTrading();
    renderChart();
    renderPlaybackMeta(bestBid, bestAsk);
    clearError();
  } catch (error) {
    console.error(error);
    showError(`Runtime error: ${error instanceof Error ? error.message : String(error)}`);
    throw error;
  }
}

async function main() {
  updatePanels();
  bindEvents();
  try {
    await loadFiles();
  } catch (error) {
    console.error(error);
    showError(`Failed to load app: ${error instanceof Error ? error.message : String(error)}`);
    els.emptyState.classList.remove("hidden");
  }
}

main();
