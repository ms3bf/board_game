const state = {
  files: [],
  session: null,
  currentFile: "",
  chunkSize: 512,
  chunkCache: new Map(),
  currentIndex: 0,
  virtualTime: 0,
  isPlaying: false,
  playSpeed: 1,
  lastRealTime: 0,
  playTimerId: null,
  playTickBusy: false,
  playbackStepMicro: 500_000,
  activeSection: "board",
  audioEnabled: true,
};

const els = {
  fileSelect: document.getElementById("file-select"),
  playBtn: document.getElementById("play-btn"),
  backBtn: document.getElementById("back-btn"),
  forwardBtn: document.getElementById("forward-btn"),
  speedBtn: document.getElementById("speed-btn"),
  audioBtn: document.getElementById("audio-btn"),
  seekSlider: document.getElementById("seek-slider"),
  boardBody: document.getElementById("board-body"),
  boardPanel: document.getElementById("board-panel"),
  tradesBody: document.getElementById("trades-body"),
  pendingBody: document.getElementById("pending-body"),
  logBody: document.getElementById("log-body"),
  chartCanvas: document.getElementById("chart-canvas"),
  chartPanel: document.getElementById("chart-panel"),
  tradesPanel: document.getElementById("trades-panel"),
  tradingPanel: document.getElementById("trading-panel"),
  showBoard: document.getElementById("show-board"),
  showChart: document.getElementById("show-chart"),
  showTrades: document.getElementById("show-trades"),
  orderSide: document.getElementById("order-side"),
  orderPrice: document.getElementById("order-price"),
  placeLimitBtn: document.getElementById("place-limit-btn"),
  crossBuyBtn: document.getElementById("cross-buy-btn"),
  crossSellBtn: document.getElementById("cross-sell-btn"),
  cancelOrdersBtn: document.getElementById("cancel-orders-btn"),
  resetAccountBtn: document.getElementById("reset-account-btn"),
  emptyState: document.getElementById("empty-state"),
  metaFile: document.getElementById("meta-file"),
  metaTime: document.getElementById("meta-time"),
  metaIndex: document.getElementById("meta-index"),
  bestBid: document.getElementById("best-bid"),
  bestAsk: document.getElementById("best-ask"),
  lastTrade: document.getElementById("last-trade"),
  speedLabel: document.getElementById("speed-label"),
  sumCash: document.getElementById("sum-cash"),
  sumPos: document.getElementById("sum-pos"),
  sumAvg: document.getElementById("sum-avg"),
  sumReal: document.getElementById("sum-real"),
  sumUnreal: document.getElementById("sum-unreal"),
  sumEquity: document.getElementById("sum-equity"),
};

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
    this.logs = [];
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

  pushRow(row) {
    this.logRows.push(row);
    if (this.logRows.length > 2000) {
      this.logRows = this.logRows.slice(-2000);
    }
  }

  log(message) {
    this.logs.push(message);
    if (this.logs.length > 400) {
      this.logs = this.logs.slice(-400);
    }
  }

  pendingPrices() {
    return new Set(this.pending.map((order) => order.limitPrice));
  }

  cancelAll() {
    const count = this.pending.length;
    this.pending = [];
    this.log(`Cancel all pending (${count})`);
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
      bestBid: this.bestBid,
      bestAsk: this.bestAsk,
      lastPrice: this.lastPrice,
      lastTradePrice: this.lastTradePrice,
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
        this.avgPrice = (this.avgPrice * prevPos + price * qty) / nextPos;
        this.position = nextPos;
      } else {
        const closeQty = Math.min(prevPos, -qty);
        realizedDelta = (price - this.avgPrice) * this.lotSize * closeQty;
        this.realizedPnl += realizedDelta;
        this.position = nextPos;
        if (this.position === 0) {
          this.avgPrice = 0;
        } else if (this.position < 0) {
          this.avgPrice = price;
        }
      }
    } else if (qty < 0) {
      this.avgPrice = (this.avgPrice * Math.abs(prevPos) + price * Math.abs(qty)) / Math.abs(nextPos);
      this.position = nextPos;
    } else {
      const closeQty = Math.min(Math.abs(prevPos), qty);
      realizedDelta = (this.avgPrice - price) * this.lotSize * closeQty;
      this.realizedPnl += realizedDelta;
      this.position = nextPos;
      if (this.position === 0) {
        this.avgPrice = 0;
      } else if (this.position > 0) {
        this.avgPrice = price;
      }
    }

    this.log(`${formatTime(timeMicro)} FILL ${side} 1lot @${formatInt(price)} (${reason})`);
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

    beep("fill");
    if (realizedDelta > 0) {
      beep("profit");
    } else if (realizedDelta < 0) {
      beep("loss");
    }
  }

  placeLimit(side, limitPrice, timeMicro) {
    const price = Number(limitPrice);
    beep("place");
    if (side === "BUY" && this.bestAsk > 0 && price >= this.bestAsk) {
      this.applyFill("BUY", this.bestAsk, "marketable-limit", timeMicro);
      return;
    }
    if (side === "SELL" && this.bestBid > 0 && price <= this.bestBid) {
      this.applyFill("SELL", this.bestBid, "marketable-limit", timeMicro);
      return;
    }

    const order = {
      oid: this.nextOid,
      side,
      limitPrice: price,
      placedTimeMicro: timeMicro,
    };
    this.nextOid += 1;
    this.pending.push(order);
    this.log(`${formatTime(timeMicro)} PLACE #${order.oid} ${side} @${formatInt(price)}`);
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
  }

  onMarket({ timeMicro, bestBid, bestAsk, lastPrice, eventType, tradePrice }) {
    this.bestBid = bestBid > 0 ? bestBid : 0;
    this.bestAsk = bestAsk > 0 ? bestAsk : 0;
    this.lastPrice = lastPrice > 0 ? lastPrice : this.lastPrice;
    if (eventType === 2 && tradePrice > 0) {
      this.lastTradePrice = tradePrice;
    }
    if (this.pending.length === 0) {
      return;
    }

    const remain = [];
    for (const order of this.pending) {
      if (order.side === "BUY") {
        if (this.bestAsk > 0 && this.bestAsk <= order.limitPrice) {
          this.applyFill("BUY", this.bestAsk, `touch#${order.oid}`, timeMicro);
        } else {
          remain.push(order);
        }
      } else if (this.bestBid > 0 && this.bestBid >= order.limitPrice) {
        this.applyFill("SELL", this.bestBid, `touch#${order.oid}`, timeMicro);
      } else {
        remain.push(order);
      }
    }
    this.pending = remain;
  }
}

const engine = new TradingEngine();
const audioState = { context: null };

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

function lowerBound(arr, target) {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] < target) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
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

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function preprocessSession(session) {
  session.maxIndex = Math.max(0, session.rowCount - 1);
  session.playbackTimes = buildPlaybackTimes(session.times);
  session.tradeTimes = session.tradeIndices.map((index) => session.playbackTimes[index]);
  return session;
}

function buildPlaybackTimes(times) {
  if (!times || times.length === 0) {
    return [];
  }
  const playback = new Array(times.length);
  let start = 0;
  while (start < times.length) {
    const secondBase = Math.floor(times[start] / 1_000_000) * 1_000_000;
    let end = start + 1;
    while (end < times.length && Math.floor(times[end] / 1_000_000) * 1_000_000 === secondBase) {
      end += 1;
    }
    const count = end - start;
    for (let i = 0; i < count; i += 1) {
      playback[start + i] = secondBase + Math.floor((i * 1_000_000) / count);
    }
    start = end;
  }
  return playback;
}

async function loadFiles() {
  state.files = await fetchJson("/api/files");
  els.fileSelect.innerHTML = "";
  for (const file of state.files) {
    const option = document.createElement("option");
    option.value = file.name;
    option.textContent = `${file.name} (${(file.size / 1024 / 1024).toFixed(2)} MB)`;
    els.fileSelect.appendChild(option);
  }

  const hasFiles = state.files.length > 0;
  els.emptyState.classList.toggle("hidden", hasFiles);
  document.querySelectorAll(".panel").forEach((panel) => panel.classList.toggle("hidden", !hasFiles));
  if (hasFiles) {
    await loadSession(state.files[0].name);
  }
}

async function loadSession(filename) {
  stopPlayback();
  const raw = await fetchJson(`/api/session-summary?file=${encodeURIComponent(filename)}`);
  state.session = preprocessSession(raw);
  state.currentFile = filename;
  state.chunkSize = raw.chunkSize || 512;
  state.chunkCache = new Map();
  state.currentIndex = 0;
  state.virtualTime = state.session.playbackTimes[0] ?? 0;
  engine.reset();

  els.fileSelect.value = filename;
  els.seekSlider.max = String(state.session.maxIndex);
  els.seekSlider.value = "0";
  await ensureChunkForIndex(0);
  await updateView();
}

function chunkIndexForRow(rowIndex) {
  return Math.floor(rowIndex / state.chunkSize);
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
  if (!state.session) {
    return null;
  }
  const chunkIndex = chunkIndexForRow(rowIndex);
  const current = await ensureChunk(chunkIndex);
  if (rowIndex >= current.end - 16 && current.end < state.session.rowCount) {
    ensureChunk(chunkIndex + 1).catch(() => {});
  }
  return current;
}

function frameAt(index) {
  const chunkIndex = chunkIndexForRow(index);
  const chunk = state.chunkCache.get(chunkIndex);
  if (!chunk) {
    return null;
  }
  const offset = index - chunk.start;
  return {
    asks: chunk.asks[offset],
    bids: chunk.bids[offset],
  };
}

function tradeRows(limit = 100) {
  const session = state.session;
  if (!session) {
    return [];
  }
  const end = upperBound(session.tradeIndices, state.currentIndex);
  const start = Math.max(0, end - limit);
  return session.tradeIndices.slice(start, end).reverse().map((index) => ({
    time: session.playbackTimes[index],
    price: session.prices[index],
    size: session.sizes[index],
    side: session.directions[index],
  }));
}

function latestTradePriceUpTo(index) {
  const session = state.session;
  if (!session || session.tradeIndices.length === 0) {
    return null;
  }
  const pos = upperBound(session.tradeIndices, index) - 1;
  if (pos < 0) {
    return null;
  }
  return session.prices[session.tradeIndices[pos]];
}

function tradePricesInRange(startIndex, endIndex) {
  const session = state.session;
  const prices = new Set();
  if (!session) {
    return prices;
  }
  let leftIndex = startIndex;
  let rightIndex = endIndex;
  if (rightIndex < leftIndex) {
    [leftIndex, rightIndex] = [rightIndex, leftIndex];
  }
  const left = lowerBound(session.tradeIndices, Math.max(0, leftIndex));
  const right = upperBound(session.tradeIndices, rightIndex);
  for (let i = left; i < right; i += 1) {
    prices.add(session.prices[session.tradeIndices[i]]);
  }
  return prices;
}

function stepToIndex(targetIndex) {
  if (!state.session) {
    return;
  }
  state.currentIndex = Math.max(0, Math.min(targetIndex, state.session.maxIndex));
  state.virtualTime = state.session.playbackTimes[state.currentIndex];
}

function processUntilTime(targetTime) {
  if (!state.session) {
    return false;
  }
  const times = state.session.playbackTimes;
  let idx = state.currentIndex;
  if (targetTime >= times[idx]) {
    while (idx < state.session.maxIndex && times[idx + 1] <= targetTime) {
      idx += 1;
    }
  } else {
    while (idx > 0 && times[idx] > targetTime) {
      idx -= 1;
    }
  }
  stepToIndex(idx);
  return state.currentIndex < state.session.maxIndex;
}

function renderBoard(frame, highlightPrices) {
  const session = state.session;
  if (!session || !frame) {
    return;
  }
  const asks = frame.asks;
  const bids = frame.bids;
  const askMap = new Map();
  const bidMap = new Map();
  const prices = new Set();

  for (const level of asks) {
    const [price, qty, orderCount] = level;
    if (price > 0) {
      prices.add(price);
      askMap.set(price, { qty, orderCount });
    }
  }
  for (const level of bids) {
    const [price, qty, orderCount] = level;
    if (price > 0) {
      prices.add(price);
      bidMap.set(price, { qty, orderCount });
    }
  }

  const sorted = Array.from(prices).sort((a, b) => b - a).slice(0, 20);
  const lastTradePrice = latestTradePriceUpTo(state.currentIndex);
  const pendingPrices = engine.pendingPrices();
  const rows = [];

  const fillCell = (value) => (value > 0 ? formatInt(value) : "");

  for (const price of sorted) {
    const ask = askMap.get(price) ?? { qty: 0, orderCount: 0 };
    const bid = bidMap.get(price) ?? { qty: 0, orderCount: 0 };
    const classes = ["price-cell"];
    if (pendingPrices.has(price)) {
      classes.push("pending");
    }
    if (lastTradePrice === price) {
      classes.push("last-trade");
    }
    if (highlightPrices.has(price)) {
      classes.push("highlight");
    }

    rows.push(`
      <tr>
        <td class="ask-order">${fillCell(ask.orderCount)}</td>
        <td class="ask-qty"><button type="button" data-side="SELL" data-price="${price}" ${ask.qty > 0 ? "" : "disabled"}>${fillCell(ask.qty)}</button></td>
        <td class="${classes.join(" ")}">${formatInt(price)}</td>
        <td class="bid-qty"><button type="button" data-side="BUY" data-price="${price}" ${bid.qty > 0 ? "" : "disabled"}>${fillCell(bid.qty)}</button></td>
        <td class="bid-order">${fillCell(bid.orderCount)}</td>
      </tr>
    `);
  }

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

function renderTrades() {
  els.tradesBody.innerHTML = tradeRows().map((row) => `
    <tr>
      <td>${formatTime(row.time)}</td>
      <td>${formatInt(row.price)}</td>
      <td>${formatInt(row.size)}</td>
      <td class="${row.side < 0 ? "sell" : "buy"}">${row.side < 0 ? "SELL" : "BUY"}</td>
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
      <td class="${order.side === "BUY" ? "buy" : "sell"}">${order.side}</td>
      <td>${formatInt(order.limitPrice)}</td>
      <td>${formatTime(order.placedTimeMicro)}</td>
    </tr>
  `).join("");

  els.logBody.innerHTML = engine.logRows.slice(-200).reverse().map((row) => `
    <tr>
      <td>${formatTime(row.timeMicro)}</td>
      <td>${row.kind}</td>
      <td class="${row.side === "BUY" ? "buy" : row.side === "SELL" ? "sell" : ""}">${row.side}</td>
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
  const session = state.session;
  const canvas = els.chartCanvas;
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#081119";
  ctx.fillRect(0, 0, width, height);

  if (!session || session.tradeTimes.length === 0) {
    ctx.fillStyle = "#edf7fb";
    ctx.font = "16px sans-serif";
    ctx.textAlign = "center";
    ctx.fillText("No trade data", width / 2, height / 2);
    return;
  }

  const pad = { top: 18, right: 14, bottom: 24, left: 14 };
  const tMin = session.tradeTimes[0];
  const tMax = session.tradeTimes[session.tradeTimes.length - 1];
  const pMin = Math.min(...session.tradePrices);
  const pMax = Math.max(...session.tradePrices);
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

  ctx.strokeStyle = "#5ec8ff";
  ctx.lineWidth = 2;
  ctx.beginPath();
  const step = Math.max(1, Math.floor(session.tradeTimes.length / Math.max(1, width / 2)));
  for (let i = 0; i < session.tradeTimes.length; i += step) {
    const x = pad.left + ((session.tradeTimes[i] - tMin) / tRange) * (width - pad.left - pad.right);
    const y = height - pad.bottom - ((session.tradePrices[i] - pMin) / pRange) * (height - pad.top - pad.bottom);
    if (i === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  }
  ctx.stroke();

  if (state.virtualTime >= tMin && state.virtualTime <= tMax) {
    const x = pad.left + ((state.virtualTime - tMin) / tRange) * (width - pad.left - pad.right);
    ctx.strokeStyle = "#ff6961";
    ctx.beginPath();
    ctx.moveTo(x, pad.top);
    ctx.lineTo(x, height - pad.bottom);
    ctx.stroke();
  }
}

function updatePanels() {
  els.boardPanel.classList.toggle("hidden", state.activeSection !== "board");
  els.chartPanel.classList.toggle("hidden", state.activeSection !== "chart");
  els.tradesPanel.classList.toggle("hidden", state.activeSection !== "trades");
  els.showBoard.classList.toggle("active", state.activeSection === "board");
  els.showChart.classList.toggle("active", state.activeSection === "chart");
  els.showTrades.classList.toggle("active", state.activeSection === "trades");
}

function renderPlaybackMeta(bestBid = null, bestAsk = null) {
  const session = state.session;
  if (!session) {
    return;
  }
  const displayTime = state.isPlaying ? state.virtualTime : session.playbackTimes[state.currentIndex];
  els.metaFile.textContent = session.name;
  els.metaTime.textContent = formatTime(displayTime);
  els.metaIndex.textContent = `${state.currentIndex} / ${session.maxIndex}`;
  if (bestBid !== null) {
    els.bestBid.textContent = bestBid ? formatInt(bestBid) : "-";
  }
  if (bestAsk !== null) {
    els.bestAsk.textContent = bestAsk ? formatInt(bestAsk) : "-";
  }
  const latestTrade = latestTradePriceUpTo(state.currentIndex);
  els.lastTrade.textContent = latestTrade ? formatInt(latestTrade) : "-";
  els.speedLabel.textContent = state.playSpeed === 5 ? "x5" : "x1";
  els.seekSlider.value = String(state.currentIndex);
}

async function tickPlayback() {
  if (!state.isPlaying || !state.session || state.playTickBusy) {
    return;
  }
  state.playTickBusy = true;
  try {
    const prevIndex = state.currentIndex;
    state.virtualTime += state.playbackStepMicro * state.playSpeed;
    const hasMore = processUntilTime(state.virtualTime);
    if (prevIndex !== state.currentIndex) {
      await updateView(tradePricesInRange(prevIndex + 1, state.currentIndex));
    } else {
      renderPlaybackMeta();
      renderChart();
    }
    if (!hasMore) {
      stopPlayback();
    }
  } finally {
    state.playTickBusy = false;
  }
}

function startPlayback() {
  if (!state.session || state.isPlaying) {
    return;
  }
  state.isPlaying = true;
  state.playTickBusy = false;
  els.playBtn.textContent = "Stop";
  state.playTimerId = window.setInterval(() => {
    tickPlayback().catch((error) => {
      console.error(error);
      stopPlayback();
    });
  }, 500);
}

function stopPlayback() {
  state.isPlaying = false;
  if (state.playTimerId !== null) {
    window.clearInterval(state.playTimerId);
    state.playTimerId = null;
  }
  els.playBtn.textContent = "Play";
}

function togglePlayback() {
  if (state.isPlaying) {
    stopPlayback();
  } else {
    startPlayback();
  }
}

function stepForward() {
  stopPlayback();
  const prev = state.currentIndex;
  stepToIndex(state.currentIndex + 1);
  updateView(tradePricesInRange(prev + 1, state.currentIndex));
}

function stepBackward() {
  stopPlayback();
  const prev = state.currentIndex;
  stepToIndex(state.currentIndex - 1);
  updateView(tradePricesInRange(state.currentIndex, prev - 1));
}

async function handleBoardOrder(event) {
  const button = event.target.closest("button[data-side][data-price]");
  if (!button || !state.session) {
    return;
  }
  await ensureChunkForIndex(state.currentIndex);
  const frame = frameAt(state.currentIndex);
  if (!frame) {
    return;
  }
  const { bestBid, bestAsk } = bestPrices(frame.asks, frame.bids);
  engine.onMarket({
    timeMicro: state.session.playbackTimes[state.currentIndex],
    bestBid,
    bestAsk,
    lastPrice: state.session.prices[state.currentIndex],
    eventType: state.session.events[state.currentIndex],
    tradePrice: state.session.prices[state.currentIndex],
  });
  engine.placeLimit(button.dataset.side, Number(button.dataset.price), state.session.playbackTimes[state.currentIndex]);
  await updateView();
}

async function submitLimitOrder(side, price) {
  if (!state.session || !Number.isFinite(price) || price <= 0) {
    return;
  }
  await ensureChunkForIndex(state.currentIndex);
  const frame = frameAt(state.currentIndex);
  if (!frame) {
    return;
  }
  const { bestBid, bestAsk } = bestPrices(frame.asks, frame.bids);
  engine.onMarket({
    timeMicro: state.session.playbackTimes[state.currentIndex],
    bestBid,
    bestAsk,
    lastPrice: state.session.prices[state.currentIndex],
    eventType: state.session.events[state.currentIndex],
    tradePrice: state.session.prices[state.currentIndex],
  });
  engine.placeLimit(side, Number(price), state.session.playbackTimes[state.currentIndex]);
  await updateView();
}

async function chartSeekFromEvent(event) {
  if (!state.session || state.session.tradeTimes.length === 0) {
    return;
  }
  const rect = els.chartCanvas.getBoundingClientRect();
  const x = Math.max(0, Math.min(event.clientX - rect.left, rect.width));
  const ratio = rect.width > 0 ? x / rect.width : 0;
  const firstTime = state.session.tradeTimes[0];
  const lastTime = state.session.tradeTimes[state.session.tradeTimes.length - 1];
  const target = Math.round(firstTime + (lastTime - firstTime) * ratio);
  stopPlayback();
  processUntilTime(target);
  await updateView();
}

function beep(kind) {
  if (!state.audioEnabled) {
    return;
  }
  const AudioCtx = window.AudioContext || window.webkitAudioContext;
  if (!AudioCtx) {
    return;
  }
  if (!audioState.context) {
    audioState.context = new AudioCtx();
  }
  if (audioState.context.state === "suspended") {
    audioState.context.resume();
  }

  const config = {
    place: { frequency: 720, duration: 0.08, type: "square" },
    fill: { frequency: 540, duration: 0.12, type: "triangle" },
    profit: { frequency: 860, duration: 0.18, type: "sine" },
    loss: { frequency: 220, duration: 0.2, type: "sawtooth" },
  }[kind];
  if (!config) {
    return;
  }

  const now = audioState.context.currentTime;
  const oscillator = audioState.context.createOscillator();
  const gain = audioState.context.createGain();
  oscillator.type = config.type;
  oscillator.frequency.setValueAtTime(config.frequency, now);
  gain.gain.setValueAtTime(0.0001, now);
  gain.gain.exponentialRampToValueAtTime(0.05, now + 0.01);
  gain.gain.exponentialRampToValueAtTime(0.0001, now + config.duration);
  oscillator.connect(gain);
  gain.connect(audioState.context.destination);
  oscillator.start(now);
  oscillator.stop(now + config.duration + 0.02);
}

function bindEvents() {
  els.fileSelect.addEventListener("change", async (event) => {
    await loadSession(event.target.value);
  });
  els.playBtn.addEventListener("click", togglePlayback);
  els.backBtn.addEventListener("click", stepBackward);
  els.forwardBtn.addEventListener("click", stepForward);
  els.speedBtn.addEventListener("click", () => {
    state.playSpeed = state.playSpeed === 5 ? 1 : 5;
    els.speedBtn.textContent = state.playSpeed === 5 ? "x1" : "x5";
    updateView();
  });
  els.audioBtn.addEventListener("click", () => {
    state.audioEnabled = !state.audioEnabled;
    els.audioBtn.textContent = state.audioEnabled ? "Audio On" : "Audio Off";
  });
  els.seekSlider.addEventListener("input", (event) => {
    stopPlayback();
    stepToIndex(Number(event.target.value));
    updateView();
  });
  els.boardBody.addEventListener("click", handleBoardOrder);
  els.placeLimitBtn.addEventListener("click", async () => {
    await submitLimitOrder(els.orderSide.value, Number(els.orderPrice.value));
  });
  els.crossBuyBtn.addEventListener("click", async () => {
    const bestAsk = Number(els.bestAsk.textContent.replace(/,/g, ""));
    if (Number.isFinite(bestAsk) && bestAsk > 0) {
      await submitLimitOrder("BUY", bestAsk);
    }
  });
  els.crossSellBtn.addEventListener("click", async () => {
    const bestBid = Number(els.bestBid.textContent.replace(/,/g, ""));
    if (Number.isFinite(bestBid) && bestBid > 0) {
      await submitLimitOrder("SELL", bestBid);
    }
  });
  els.cancelOrdersBtn.addEventListener("click", () => {
    engine.cancelAll();
    updateView();
  });
  els.resetAccountBtn.addEventListener("click", () => {
    engine.reset();
    updateView();
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

  let draggingChart = false;
  els.chartCanvas.addEventListener("pointerdown", (event) => {
    draggingChart = true;
    chartSeekFromEvent(event);
  });
  els.chartCanvas.addEventListener("pointermove", (event) => {
    if (draggingChart) {
      chartSeekFromEvent(event);
    }
  });
  window.addEventListener("pointerup", () => {
    draggingChart = false;
  });

  window.addEventListener("keydown", (event) => {
    if (event.target instanceof HTMLInputElement || event.target instanceof HTMLSelectElement) {
      return;
    }
    if (event.code === "Space") {
      event.preventDefault();
      togglePlayback();
    } else if (event.key === "ArrowRight") {
      event.preventDefault();
      stepForward();
    } else if (event.key === "ArrowLeft") {
      event.preventDefault();
      stepBackward();
    }
  });

  window.addEventListener("resize", renderChart);
}

async function updateView(highlightPrices = null) {
  const session = state.session;
  if (!session) {
    return;
  }

  await ensureChunkForIndex(state.currentIndex);
  const frame = frameAt(state.currentIndex);
  if (!frame) {
    return;
  }

  const { bestBid, bestAsk } = bestPrices(frame.asks, frame.bids);
  engine.onMarket({
    timeMicro: session.playbackTimes[state.currentIndex],
    bestBid,
    bestAsk,
    lastPrice: session.prices[state.currentIndex],
    eventType: session.events[state.currentIndex],
    tradePrice: session.prices[state.currentIndex],
  });

  const prices = highlightPrices ?? new Set(
    session.events[state.currentIndex] === 2 ? [session.prices[state.currentIndex]] : []
  );

  renderBoard(frame, prices);
  renderTrades();
  renderTrading();
  renderChart();
  renderPlaybackMeta(bestBid, bestAsk);
}

async function main() {
  updatePanels();
  bindEvents();
  try {
    await loadFiles();
  } catch (error) {
    console.error(error);
    els.emptyState.classList.remove("hidden");
    els.emptyState.innerHTML = `
      <h2>Failed to load app</h2>
      <p>サーバが起動しているか、parquet の読み込みに必要な Python 依存が入っているか確認してください。</p>
    `;
    document.querySelectorAll(".panel").forEach((panel) => panel.classList.add("hidden"));
  }
}

main();
