from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtWidgets import (
    QHeaderView,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)


def fmt_time_micro(t_micro: int) -> str:
    if t_micro <= 0:
        return "--:--:--"
    return str(timedelta(seconds=t_micro / 1_000_000)).split(".")[0]


@dataclass
class PendingOrder:
    oid: int
    side: str  # BUY / SELL
    limit_price: int
    placed_time_micro: int


@dataclass
class TradeLogRow:
    time_micro: int
    kind: str
    side: str
    price: int
    qty_lot: int
    reason: str
    pos_after: int
    avg_after: float
    realized_delta: float
    realized_total: float


class TradingEngine:
    def __init__(self, initial_cash: float = 1_000_000.0, lot_size: int = 100):
        self.initial_cash = float(initial_cash)
        self.lot_size = int(lot_size)
        self.reset()

    def reset(self):
        self.cash = float(self.initial_cash)
        self.position = 0  # signed lots
        self.avg_price = 0.0
        self.realized_pnl = 0.0
        self.last_price = 0
        self.last_trade_price = 0
        self.best_bid = 0
        self.best_ask = 0
        self._next_oid = 1
        self.pending: List[PendingOrder] = []
        self.logs: List[str] = []
        self.log_rows: List[TradeLogRow] = []
        self.audio_events: List[str] = []
        self._log("Engine reset")
        self._push_row(
            TradeLogRow(
                time_micro=0,
                kind="RESET",
                side="-",
                price=0,
                qty_lot=0,
                reason="engine-reset",
                pos_after=0,
                avg_after=0.0,
                realized_delta=0.0,
                realized_total=0.0,
            )
        )

    def _log(self, msg: str):
        self.logs.append(msg)
        if len(self.logs) > 400:
            self.logs = self.logs[-400:]

    def _emit_audio(self, name: str):
        self.audio_events.append(name)
        if len(self.audio_events) > 200:
            self.audio_events = self.audio_events[-200:]

    def pop_audio_events(self) -> List[str]:
        if not self.audio_events:
            return []
        out = list(self.audio_events)
        self.audio_events = []
        return out

    def _push_row(self, row: TradeLogRow):
        self.log_rows.append(row)
        if len(self.log_rows) > 2000:
            self.log_rows = self.log_rows[-2000:]

    def pending_prices(self) -> set[int]:
        return set(int(o.limit_price) for o in self.pending)

    def cancel_all(self):
        n = len(self.pending)
        self.pending = []
        self._log(f"Cancel all pending ({n})")
        self._push_row(
            TradeLogRow(
                time_micro=0,
                kind="CANCEL",
                side="-",
                price=0,
                qty_lot=n,
                reason="cancel-all",
                pos_after=self.position,
                avg_after=self.avg_price,
                realized_delta=0.0,
                realized_total=self.realized_pnl,
            )
        )

    def _equity(self) -> float:
        mark = self.last_trade_price if self.last_trade_price > 0 else self.last_price
        return self.cash + float(self.position * mark * self.lot_size)

    def _unrealized(self) -> float:
        if self.position == 0:
            return 0.0
        mark = self.last_trade_price if self.last_trade_price > 0 else self.last_price
        if self.position > 0:
            return (mark - self.avg_price) * self.lot_size * self.position
        return (self.avg_price - mark) * self.lot_size * abs(self.position)

    def snapshot(self) -> dict:
        return {
            "cash": self.cash,
            "position": self.position,
            "avg_price": self.avg_price,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self._unrealized(),
            "equity": self._equity(),
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "last_price": self.last_price,
            "last_trade_price": self.last_trade_price,
        }

    def _apply_fill(self, side: str, price: int, reason: str, t_micro: int):
        qty = 1 if side == "BUY" else -1
        realized_delta = 0.0

        if side == "BUY":
            self.cash -= price * self.lot_size
        else:
            self.cash += price * self.lot_size

        prev_pos = self.position
        new_pos = prev_pos + qty

        if prev_pos == 0:
            self.position = new_pos
            self.avg_price = float(price)
        elif prev_pos > 0:
            if qty > 0:
                total = self.avg_price * prev_pos + price * qty
                self.position = new_pos
                self.avg_price = total / self.position
            else:
                close_qty = min(prev_pos, -qty)
                # long close: sell above avg => positive
                pnl = (price - self.avg_price) * self.lot_size * close_qty
                self.realized_pnl += pnl
                realized_delta += pnl
                self.position = new_pos
                if self.position == 0:
                    self.avg_price = 0.0
                elif self.position < 0:
                    self.avg_price = float(price)
        else:  # prev_pos < 0
            if qty < 0:
                total_abs = abs(prev_pos)
                total = self.avg_price * total_abs + price * abs(qty)
                self.position = new_pos
                self.avg_price = total / abs(self.position)
            else:
                close_qty = min(abs(prev_pos), qty)
                # short close: buy below avg => positive
                pnl = (self.avg_price - price) * self.lot_size * close_qty
                self.realized_pnl += pnl
                realized_delta += pnl
                self.position = new_pos
                if self.position == 0:
                    self.avg_price = 0.0
                elif self.position > 0:
                    self.avg_price = float(price)

        self._log(
            f"{fmt_time_micro(t_micro)} FILL {side} 1lot @{price:,} ({reason}) "
            f"pos={self.position} avg={self.avg_price:.1f}"
        )
        self._push_row(
            TradeLogRow(
                time_micro=int(t_micro),
                kind="FILL",
                side=side,
                price=int(price),
                qty_lot=1,
                reason=reason,
                pos_after=self.position,
                avg_after=self.avg_price,
                realized_delta=realized_delta,
                realized_total=self.realized_pnl,
            )
        )
        self._emit_audio("yakujo")
        if realized_delta > 0:
            self._emit_audio("kati")
        elif realized_delta < 0:
            self._emit_audio("make")

    def place_limit(self, side: str, limit_price: int, t_micro: int) -> str:
        side = side.upper()
        px = int(limit_price)
        if side not in ("BUY", "SELL"):
            return "invalid side"
        if px <= 0:
            return "invalid price"
        self._emit_audio("hattyu")
        if side == "BUY" and self.best_ask > 0 and px >= self.best_ask:
            self._apply_fill("BUY", int(self.best_ask), "marketable-limit", t_micro)
            return f"filled BUY @{self.best_ask:,}"
        if side == "SELL" and self.best_bid > 0 and px <= self.best_bid:
            self._apply_fill("SELL", int(self.best_bid), "marketable-limit", t_micro)
            return f"filled SELL @{self.best_bid:,}"

        o = PendingOrder(
            oid=self._next_oid,
            side=side,
            limit_price=px,
            placed_time_micro=int(t_micro),
        )
        self._next_oid += 1
        self.pending.append(o)
        self._log(f"{fmt_time_micro(t_micro)} PLACE #{o.oid} {o.side} @{o.limit_price:,}")
        self._push_row(
            TradeLogRow(
                time_micro=int(t_micro),
                kind="PLACE",
                side=o.side,
                price=o.limit_price,
                qty_lot=1,
                reason=f"id#{o.oid}",
                pos_after=self.position,
                avg_after=self.avg_price,
                realized_delta=0.0,
                realized_total=self.realized_pnl,
            )
        )
        return f"placed #{o.oid}"

    def on_market(
        self,
        t_micro: int,
        best_bid: int,
        best_ask: int,
        last_price: int,
        event_type: int,
        trade_dir: int,
        trade_price: int,
    ):
        self.best_bid = int(best_bid) if best_bid > 0 else 0
        self.best_ask = int(best_ask) if best_ask > 0 else 0
        self.last_price = int(last_price) if last_price > 0 else self.last_price
        if int(event_type) == 2 and int(trade_price) > 0:
            self.last_trade_price = int(trade_price)

        if not self.pending:
            return
        remain: List[PendingOrder] = []
        for o in self.pending:
            if o.side == "BUY":
                if self.best_ask > 0 and self.best_ask <= o.limit_price:
                    self._apply_fill("BUY", int(self.best_ask), f"touch#{o.oid}", t_micro)
                else:
                    remain.append(o)
            else:
                if self.best_bid > 0 and self.best_bid >= o.limit_price:
                    self._apply_fill("SELL", int(self.best_bid), f"touch#{o.oid}", t_micro)
                else:
                    remain.append(o)
        self.pending = remain


class TradingWindow(QWidget):
    cancel_all_requested = pyqtSignal()
    reset_requested = pyqtSignal()

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Trading")
        self.resize(980, 620)

        layout = QVBoxLayout(self)

        summary = QGridLayout()
        self.lbl_cash = QLabel("Cash: -")
        self.lbl_pos = QLabel("Position: -")
        self.lbl_avg = QLabel("Avg: -")
        self.lbl_real = QLabel("Realized: -")
        self.lbl_unreal = QLabel("Unrealized: -")
        self.lbl_equity = QLabel("Equity: -")
        summary.addWidget(self.lbl_cash, 0, 0)
        summary.addWidget(self.lbl_pos, 0, 1)
        summary.addWidget(self.lbl_avg, 1, 0)
        summary.addWidget(self.lbl_real, 1, 1)
        summary.addWidget(self.lbl_unreal, 2, 0)
        summary.addWidget(self.lbl_equity, 2, 1)
        layout.addLayout(summary)

        buttons = QHBoxLayout()
        btn_cancel = QPushButton("Cancel All")
        btn_cancel.clicked.connect(self.cancel_all_requested.emit)
        buttons.addWidget(btn_cancel)
        btn_reset = QPushButton("Reset Account")
        btn_reset.clicked.connect(self.reset_requested.emit)
        buttons.addWidget(btn_reset)
        buttons.addStretch()
        layout.addLayout(buttons)

        layout.addWidget(QLabel("Pending Orders"))
        self.pending_table = QTableWidget()
        self.pending_table.setColumnCount(4)
        self.pending_table.setHorizontalHeaderLabels(["ID", "Side", "Limit", "Placed"])
        layout.addWidget(self.pending_table, 1)

        layout.addWidget(QLabel("Trade Log"))
        self.log_table = QTableWidget()
        self.log_table.setColumnCount(10)
        self.log_table.setHorizontalHeaderLabels(
            ["Time", "Type", "Side", "Price", "Lot", "Reason", "Pos", "Avg", "dReal", "Realized"]
        )
        self.log_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.log_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.log_table, 2)

    def refresh(
        self,
        snap: dict,
        pending: List[PendingOrder],
        logs: List[str],
        log_rows: Optional[List[TradeLogRow]] = None,
    ):
        self.lbl_cash.setText(f"Cash: {snap['cash']:,.0f} JPY")
        self.lbl_pos.setText(f"Position: {snap['position']} lot")
        avg = snap["avg_price"]
        self.lbl_avg.setText(f"Avg: {avg:,.1f}" if avg else "Avg: -")
        self.lbl_real.setText(f"Realized: {snap['realized_pnl']:,.0f} JPY")
        self.lbl_unreal.setText(f"Unrealized: {snap['unrealized_pnl']:,.0f} JPY")
        self.lbl_equity.setText(f"Equity: {snap['equity']:,.0f} JPY")

        self.pending_table.setRowCount(0)
        for o in pending:
            r = self.pending_table.rowCount()
            self.pending_table.insertRow(r)
            self.pending_table.setItem(r, 0, QTableWidgetItem(str(o.oid)))
            self.pending_table.setItem(r, 1, QTableWidgetItem(o.side))
            self.pending_table.setItem(r, 2, QTableWidgetItem(f"{o.limit_price:,}"))
            self.pending_table.setItem(r, 3, QTableWidgetItem(fmt_time_micro(o.placed_time_micro)))

        shown_rows = log_rows[-200:] if log_rows else []
        if not shown_rows:
            shown = logs[-120:]
            self.log_table.setRowCount(0)
            for line in reversed(shown):
                r = self.log_table.rowCount()
                self.log_table.insertRow(r)
                self.log_table.setItem(r, 0, QTableWidgetItem(line))
            return

        self.log_table.setRowCount(0)
        for row in reversed(shown_rows):
            r = self.log_table.rowCount()
            self.log_table.insertRow(r)
            vals = [
                fmt_time_micro(int(row.time_micro)),
                row.kind,
                row.side,
                f"{int(row.price):,}" if int(row.price) > 0 else "-",
                str(int(row.qty_lot)) if int(row.qty_lot) > 0 else "-",
                row.reason,
                str(int(row.pos_after)),
                f"{float(row.avg_after):,.1f}" if float(row.avg_after) > 0 else "-",
                f"{float(row.realized_delta):+,.0f}" if float(row.realized_delta) != 0 else "0",
                f"{float(row.realized_total):,.0f}",
            ]
            for c, v in enumerate(vals):
                item = QTableWidgetItem(v)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.log_table.setItem(r, c, item)
