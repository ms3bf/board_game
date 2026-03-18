import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from PyQt6.QtCore import Qt, QTimer, pyqtSignal
from PyQt6.QtGui import QAction, QColor, QPainter, QPen
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSlider,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

try:
    import onnxruntime as ort
except ImportError:
    ort = None

try:
    import winsound
except Exception:
    winsound = None

try:
    if __package__:
        from .trade import TradingEngine, TradingWindow
    else:
        from trade import TradingEngine, TradingWindow
except Exception:
    TradingEngine = None
    TradingWindow = None


FILENAME_RE = re.compile(r"^(?P<name>.+)\.parquet$", re.IGNORECASE)
HORIZON_RE = re.compile(r"(?:horizon|_h)(\d+)", re.IGNORECASE)
SAMPLING_HINT_RE = re.compile(r"_(none|time|quantity)_t([^_]+)_q(\d+)", re.IGNORECASE)
SESSION_HINT_RE = re.compile(r"_(AM|PM|ALL)_", re.IGNORECASE)
ENABLE_TRADING_FEATURE = True


@dataclass
class ParsedFile:
    path: str
    name: str


class ResultPredictionProvider:
    def __init__(self):
        self.loaded = False
        self.path = ""
        self.times_micro = np.array([], dtype=np.int64)
        self.preds = np.array([], dtype=np.int64)
        self.max_prob = None

    @staticmethod
    def _to_micro_of_day(series: pd.Series) -> np.ndarray:
        s = series
        if np.issubdtype(s.dtype, np.number):
            arr = s.to_numpy(dtype=np.float64, copy=False)
            out = np.zeros_like(arr, dtype=np.int64)
            for i, x in enumerate(arr):
                if x > 1e14:  # ns epoch
                    ts = pd.to_datetime(int(x), unit="ns", utc=False)
                    out[i] = int((ts.hour * 3600 + ts.minute * 60 + ts.second) * 1_000_000 + ts.microsecond)
                elif x > 1e9:  # sec epoch
                    ts = pd.to_datetime(float(x), unit="s", utc=False)
                    out[i] = int((ts.hour * 3600 + ts.minute * 60 + ts.second) * 1_000_000 + ts.microsecond)
                elif x > 1e7:  # already micro-like
                    out[i] = int(x)
                else:  # seconds of day
                    out[i] = int(x * 1_000_000)
            return out

        ts = pd.to_datetime(s, errors="coerce")
        if ts.notna().any():
            return (
                (
                    ts.dt.hour.fillna(0).astype(np.int64) * 3600
                    + ts.dt.minute.fillna(0).astype(np.int64) * 60
                    + ts.dt.second.fillna(0).astype(np.int64)
                )
                * 1_000_000
                + ts.dt.microsecond.fillna(0).astype(np.int64)
            ).to_numpy(np.int64, copy=False)
        return np.zeros(len(s), dtype=np.int64)

    def load(self, csv_path: str):
        df = pd.read_csv(csv_path)
        required = {"timestamp", "Preds"}
        if not required.issubset(df.columns):
            raise ValueError("result.csv must include timestamp and Preds columns")

        self.times_micro = self._to_micro_of_day(df["timestamp"])
        self.preds = df["Preds"].to_numpy(np.int64, copy=False)
        self.max_prob = df["max_prob"].to_numpy(np.float32, copy=False) if "max_prob" in df.columns else None
        order = np.argsort(self.times_micro)
        self.times_micro = self.times_micro[order]
        self.preds = self.preds[order]
        if self.max_prob is not None:
            self.max_prob = self.max_prob[order]
        self.loaded = True
        self.path = csv_path

    def predict(self, current_time_micro: int):
        if not self.loaded or self.times_micro.size == 0:
            return None
        pos = np.searchsorted(self.times_micro, current_time_micro, side="right") - 1
        if pos < 0:
            return None
        prob = float(self.max_prob[pos]) if self.max_prob is not None else None
        return int(self.preds[pos]), prob, int(self.times_micro[pos])


class OnnxRealtimePredictor:
    def __init__(self):
        self.session = None
        self.input_name = None
        self.model_path = ""
        self.seq_size = 128
        self.sampling_mode = "none"
        self.sampling_time = None
        self.sampling_quantity = None
        self.session_hint = "ALL"
        self._cache = {}

    def is_ready(self):
        return self.session is not None and self.input_name is not None

    def clear_cache(self):
        self._cache = {}

    def load_model(self, onnx_path: str, seq_size: int = 128):
        if ort is None:
            raise RuntimeError("onnxruntime is not installed.")
        so = ort.SessionOptions()
        so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        providers = ["CUDAExecutionProvider", "CPUExecutionProvider"]
        self.session = ort.InferenceSession(onnx_path, sess_options=so, providers=providers)
        self.input_name = self.session.get_inputs()[0].name
        self.model_path = onnx_path
        self.seq_size = int(seq_size)
        normalized = onnx_path.replace("\\", "/")
        if "_event_" in normalized.lower():
            self.sampling_mode = "event"
            self.sampling_time = None
            self.sampling_quantity = None
        else:
            m = SAMPLING_HINT_RE.search(normalized)
            if m:
                self.sampling_mode = m.group(1).lower()
                self.sampling_time = m.group(2)
                self.sampling_quantity = int(m.group(3))
            else:
                self.sampling_mode = "none"
                self.sampling_time = None
                self.sampling_quantity = None
        s = SESSION_HINT_RE.search(normalized)
        self.session_hint = s.group(1).upper() if s else "ALL"
        self.clear_cache()

    def predict_at(self, dm: "ItayomikunDataManager", current_time_micro: int):
        if not self.is_ready():
            return None
        window_data = dm.get_model_window_by_time(int(current_time_micro), self.seq_size)
        if window_data is None:
            return None
        x, pred_time_micro, proc_idx = window_data
        key = (int(proc_idx), self.seq_size)
        if key in self._cache:
            return self._cache[key]
        y = self.session.run(None, {self.input_name: x})[0]
        logits = y[0]
        pred = int(np.argmax(logits))
        logits = logits - np.max(logits)
        ex = np.exp(logits)
        probs = ex / np.sum(ex)
        result = (pred, float(np.max(probs)), int(pred_time_micro))
        self._cache[key] = result
        return result


class ItayomikunDataManager:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.files = {}
        self.available_files = []
        self.current_file = None

        self.time_arr = None
        self.event_arr = None
        self.price_arr = None
        self.size_arr = None
        self.order_arr = None
        self.dir_arr = None
        self.ask_arr = None
        self.bid_arr = None
        self.trade_indices = np.array([], dtype=np.int64)
        self.feature66 = None
        self.model_feature66 = None
        self.model_time_arr = None
        self.model_mode = "none"
        self.model_sampling_time = None
        self.model_sampling_quantity = None
        self.model_session = "ALL"

        self.current_idx = 0
        self.max_idx = 0
        self.current_time_str = "--:--:--"
        self.current_timestamp = 0

        self._scan()

    def _scan(self):
        self.files = {}
        for path in sorted(Path(self.data_dir).glob("*.parquet")):
            name = path.name
            m = FILENAME_RE.match(name)
            if not m:
                continue
            info = ParsedFile(path=str(path), name=m.group("name"))
            self.files[info.name] = info.path
        self.available_files = sorted(self.files.keys(), reverse=True)

    def load_file(self, name: str):
        path = self.files.get(name)
        if not path:
            raise FileNotFoundError(f"parquet not found: {name}")

        cols = ["Time", "Event", "Price", "Size", "OrderCount", "Direction"]
        for i in range(1, 11):
            cols += [f"Ask{i}_P", f"Ask{i}_Q", f"Ask{i}_O", f"Bid{i}_P", f"Bid{i}_Q", f"Bid{i}_O"]
        df = pd.read_parquet(path, columns=cols)

        self.time_arr = df["Time"].to_numpy(np.int64, copy=False)
        self.event_arr = df["Event"].to_numpy(np.int8, copy=False)
        self.price_arr = df["Price"].to_numpy(np.int64, copy=False)
        self.size_arr = df["Size"].to_numpy(np.int64, copy=False)
        self.order_arr = df["OrderCount"].to_numpy(np.int64, copy=False)
        self.dir_arr = df["Direction"].to_numpy(np.int8, copy=False)

        asks = []
        bids = []
        for i in range(1, 11):
            asks.append(df[[f"Ask{i}_P", f"Ask{i}_Q", f"Ask{i}_O"]].to_numpy(np.int64, copy=False))
            bids.append(df[[f"Bid{i}_P", f"Bid{i}_Q", f"Bid{i}_O"]].to_numpy(np.int64, copy=False))
        self.ask_arr = np.stack(asks, axis=1)
        self.bid_arr = np.stack(bids, axis=1)

        self.trade_indices = np.where(self.event_arr == 2)[0]
        self._build_feature66(df)
        self.configure_model_input(
            mode=self.model_mode,
            sampling_time=self.model_sampling_time,
            sampling_quantity=self.model_sampling_quantity,
            session_filter=self.model_session,
        )
        self.current_file = name
        self.current_idx = 0
        self.max_idx = max(0, len(df) - 1)
        self._sync_time()

    def _build_feature66(self, df: pd.DataFrame):
        obs_cols = []
        for i in range(1, 11):
            obs_cols += [f"Ask{i}_P", f"Ask{i}_Q", f"Ask{i}_O", f"Bid{i}_P", f"Bid{i}_Q", f"Bid{i}_O"]
        obs = df[obs_cols].to_numpy(np.float32, copy=True)
        msgs = df[["Time", "Event", "Price", "Size", "OrderCount", "Direction"]].to_numpy(np.float32, copy=True)

        t_sec = msgs[:, 0] / 1_000_000.0
        t_diff = np.diff(t_sec, prepend=t_sec[0])
        msgs[:, 0] = t_diff

        price_vals = obs[:, 0::3].reshape(-1)
        size_vals = obs[:, 1::3].reshape(-1)
        order_vals = obs[:, 2::3].reshape(-1)
        time_vals = t_diff[1:] if len(t_diff) > 1 else t_diff

        def ms(v):
            mean = float(np.mean(v)) if v.size else 0.0
            std = float(np.std(v)) if v.size else 1.0
            if std == 0:
                std = 1.0
            return mean, std

        p_mean, p_std = ms(price_vals)
        s_mean, s_std = ms(size_vals)
        o_mean, o_std = ms(order_vals)
        t_mean, t_std = ms(time_vals)

        obs[:, 0::3] = (obs[:, 0::3] - p_mean) / p_std
        obs[:, 1::3] = (obs[:, 1::3] - s_mean) / s_std
        obs[:, 2::3] = (obs[:, 2::3] - o_mean) / o_std
        msgs[:, 0] = (msgs[:, 0] - t_mean) / t_std
        msgs[:, 2] = (msgs[:, 2] - p_mean) / p_std
        msgs[:, 3] = (msgs[:, 3] - s_mean) / s_std
        msgs[:, 4] = (msgs[:, 4] - o_mean) / o_std

        feat = np.concatenate([obs, msgs], axis=1).astype(np.float32, copy=False)
        self.feature66 = feat[1:] if feat.shape[0] > 1 else np.empty((0, 66), dtype=np.float32)

    @staticmethod
    def _interval_to_microseconds(interval: Optional[str]) -> Optional[int]:
        if not interval:
            return None
        s = interval.strip().lower()
        m = re.match(r"^(\d+)\s*(us|ms|s|m|min|h)$", s)
        if not m:
            return None
        n = int(m.group(1))
        unit = m.group(2)
        if unit == "us":
            return n
        if unit == "ms":
            return n * 1_000
        if unit == "s":
            return n * 1_000_000
        if unit in ("m", "min"):
            return n * 60 * 1_000_000
        if unit == "h":
            return n * 3600 * 1_000_000
        return None

    @staticmethod
    def _session_range_micro(session_filter: str):
        sf = (session_filter or "ALL").upper()
        if sf == "AM":
            return 9 * 3600 * 1_000_000, (11 * 3600 + 30 * 60) * 1_000_000
        if sf == "PM":
            return (12 * 3600 + 30 * 60) * 1_000_000, (15 * 3600 + 25 * 60) * 1_000_000
        return None

    def configure_model_input(
        self,
        mode: str = "none",
        sampling_time: Optional[str] = None,
        sampling_quantity: Optional[int] = None,
        session_filter: str = "ALL",
    ):
        self.model_mode = (mode or "none").lower()
        self.model_sampling_time = sampling_time
        self.model_sampling_quantity = sampling_quantity
        self.model_session = (session_filter or "ALL").upper()
        self.model_feature66 = np.empty((0, 66), dtype=np.float32)
        self.model_time_arr = np.array([], dtype=np.int64)

        if self.time_arr is None or self.time_arr.size == 0:
            return

        idx = np.arange(self.time_arr.size, dtype=np.int64)
        if self.model_mode != "event":
            session_range = self._session_range_micro(self.model_session)
            if session_range is not None:
                st, ed = session_range
                mask = (self.time_arr >= st) & (self.time_arr <= ed)
                idx = idx[mask]
            if idx.size == 0:
                return

            sess_events = self.event_arr[idx]
            first_trade_pos = np.where(sess_events == 2)[0]
            if first_trade_pos.size == 0:
                return
            start_pos = int(first_trade_pos[0]) + 1
            if start_pos >= idx.size:
                return
            idx = idx[start_pos:]
            if idx.size <= 1:
                return

        if self.model_mode == "time":
            interval_us = self._interval_to_microseconds(self.model_sampling_time)
            if interval_us is None or interval_us <= 0:
                interval_us = 1_000_000
            bins = (self.time_arr[idx] // interval_us).astype(np.int64, copy=False)
            _, first_pos = np.unique(bins, return_index=True)
            idx = idx[np.sort(first_pos)]
        elif self.model_mode == "quantity":
            q = int(self.model_sampling_quantity) if self.model_sampling_quantity else 500
            if q <= 0:
                q = 500
            sizes = self.size_arr[idx]
            csum = np.cumsum(sizes, dtype=np.int64)
            keep = (csum % q) < sizes
            idx = idx[keep]

        if idx.size <= 1:
            return

        asks = self.ask_arr[idx]
        bids = self.bid_arr[idx]
        obs = np.concatenate([asks, bids], axis=2).reshape(idx.size, 60).astype(np.float32, copy=False)
        msgs = np.stack(
            [
                self.time_arr[idx],
                self.event_arr[idx],
                self.price_arr[idx],
                self.size_arr[idx],
                self.order_arr[idx],
                self.dir_arr[idx],
            ],
            axis=1,
        ).astype(np.float32, copy=False)

        t_sec = msgs[:, 0] / 1_000_000.0
        t_diff = np.diff(t_sec, prepend=t_sec[0])
        msgs[:, 0] = t_diff

        price_vals = obs[:, 0::3].reshape(-1)
        size_vals = obs[:, 1::3].reshape(-1)
        order_vals = obs[:, 2::3].reshape(-1)
        time_vals = t_diff[1:] if len(t_diff) > 1 else t_diff

        def ms(v):
            mean = float(np.mean(v)) if v.size else 0.0
            std = float(np.std(v)) if v.size else 1.0
            if std == 0:
                std = 1.0
            return mean, std

        p_mean, p_std = ms(price_vals)
        s_mean, s_std = ms(size_vals)
        o_mean, o_std = ms(order_vals)
        t_mean, t_std = ms(time_vals)

        obs[:, 0::3] = (obs[:, 0::3] - p_mean) / p_std
        obs[:, 1::3] = (obs[:, 1::3] - s_mean) / s_std
        obs[:, 2::3] = (obs[:, 2::3] - o_mean) / o_std
        msgs[:, 0] = (msgs[:, 0] - t_mean) / t_std
        msgs[:, 2] = (msgs[:, 2] - p_mean) / p_std
        msgs[:, 3] = (msgs[:, 3] - s_mean) / s_std
        msgs[:, 4] = (msgs[:, 4] - o_mean) / o_std

        feat = np.concatenate([obs, msgs], axis=1).astype(np.float32, copy=False)
        self.model_feature66 = feat[1:] if feat.shape[0] > 1 else np.empty((0, 66), dtype=np.float32)
        self.model_time_arr = self.time_arr[idx][1:] if idx.size > 1 else np.array([], dtype=np.int64)

    def get_model_window(self, row_idx: int, seq_size: int):
        if self.feature66 is None or self.feature66.shape[0] == 0:
            return None
        proc_idx = int(row_idx) - 1
        if proc_idx < seq_size - 1:
            return None
        start = proc_idx - seq_size + 1
        x = self.feature66[start : proc_idx + 1]
        if x.shape[0] != seq_size:
            return None
        return np.expand_dims(x, axis=0)

    def get_model_window_by_time(self, current_time_micro: int, seq_size: int):
        if self.model_feature66 is None or self.model_feature66.shape[0] == 0:
            return None
        if self.model_time_arr is None or self.model_time_arr.size == 0:
            return None
        proc_idx = np.searchsorted(self.model_time_arr, int(current_time_micro), side="right") - 1
        if proc_idx < seq_size - 1:
            return None
        start = int(proc_idx) - seq_size + 1
        x = self.model_feature66[start : int(proc_idx) + 1]
        if x.shape[0] != seq_size:
            return None
        return np.expand_dims(x, axis=0), int(self.model_time_arr[int(proc_idx)]), int(proc_idx)

    def _sync_time(self):
        if self.time_arr is None or self.max_idx < 0:
            self.current_timestamp = 0
            self.current_time_str = "--:--:--"
            return
        t = int(self.time_arr[self.current_idx])
        self.current_timestamp = t
        self.current_time_str = str(timedelta(seconds=t / 1_000_000)).split(".")[0]

    def step_to_index(self, target_idx: int):
        if self.time_arr is None:
            return
        self.current_idx = int(max(0, min(target_idx, self.max_idx)))
        self._sync_time()

    def next_step(self):
        if self.current_idx < self.max_idx:
            self.step_to_index(self.current_idx + 1)
            return True
        return False

    def prev_step(self):
        if self.current_idx > 0:
            self.step_to_index(self.current_idx - 1)
            return True
        return False

    def process_until_time(self, target_time_micro: int):
        if self.time_arr is None:
            return False
        idx = np.searchsorted(self.time_arr, target_time_micro, side="right") - 1
        if idx < 0:
            idx = 0
        self.step_to_index(int(idx))
        return self.current_idx < self.max_idx

    def board_levels(self):
        if self.ask_arr is None:
            return np.zeros((10, 3), dtype=np.int64), np.zeros((10, 3), dtype=np.int64)
        return self.ask_arr[self.current_idx], self.bid_arr[self.current_idx]

    def trade_rows(self, limit=100):
        if self.trade_indices.size == 0:
            return []
        end = np.searchsorted(self.trade_indices, self.current_idx, side="right")
        if end <= 0:
            return []
        sel = self.trade_indices[max(0, end - limit):end][::-1]
        rows = []
        for i in sel:
            rows.append(
                (
                    int(self.time_arr[i]),
                    int(self.price_arr[i]),
                    int(self.size_arr[i]),
                    int(self.dir_arr[i]),
                )
            )
        return rows

    def trade_prices_in_range(self, start_idx: int, end_idx: int) -> set:
        if self.trade_indices.size == 0:
            return set()
        if end_idx < start_idx:
            start_idx, end_idx = end_idx, start_idx
        if end_idx < 0:
            return set()
        left = np.searchsorted(self.trade_indices, max(0, start_idx), side="left")
        right = np.searchsorted(self.trade_indices, end_idx, side="right")
        if right <= left:
            return set()
        idx = self.trade_indices[left:right]
        return set(int(x) for x in np.unique(self.price_arr[idx]))

    def latest_trade_price_up_to(self, idx: int) -> Optional[int]:
        if self.trade_indices.size == 0:
            return None
        pos = np.searchsorted(self.trade_indices, idx, side="right") - 1
        if pos < 0:
            return None
        trade_idx = int(self.trade_indices[pos])
        return int(self.price_arr[trade_idx])


class ChartWidget(QWidget):
    seek_requested = pyqtSignal(int)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Chart")
        self.resize(640, 300)
        self.times = np.array([])
        self.prices = np.array([])
        self.current_time = 0

    def set_data(self, times: np.ndarray, prices: np.ndarray):
        self.times = times
        self.prices = prices
        self.update()

    def set_current_time(self, t: int):
        self.current_time = t
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.fillRect(self.rect(), QColor("#1f1f1f"))
        if self.times.size == 0:
            painter.setPen(QColor("white"))
            painter.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, "No trade data")
            return

        t_min = float(self.times[0])
        t_max = float(self.times[-1])
        p_min = float(np.min(self.prices))
        p_max = float(np.max(self.prices))
        t_range = max(1.0, t_max - t_min)
        p_range = max(1.0, p_max - p_min)
        w, h = self.width(), self.height()

        painter.setPen(QPen(QColor("#5ec8ff"), 1))
        step = max(1, int(len(self.times) / max(1, w)))
        points = []
        for i in range(0, len(self.times), step):
            x = int((float(self.times[i]) - t_min) / t_range * (w - 1))
            y = int(h - 1 - ((float(self.prices[i]) - p_min) / p_range * (h - 20)) - 10)
            points.append((x, y))
        for i in range(1, len(points)):
            painter.drawLine(points[i - 1][0], points[i - 1][1], points[i][0], points[i][1])

        if t_min <= self.current_time <= t_max:
            x = int((float(self.current_time) - t_min) / t_range * (w - 1))
            painter.setPen(QPen(QColor("#ff5050"), 1))
            painter.drawLine(x, 0, x, h)

    def mousePressEvent(self, event):
        self._seek(event.pos().x())

    def mouseMoveEvent(self, event):
        if event.buttons() & Qt.MouseButton.LeftButton:
            self._seek(event.pos().x())

    def _seek(self, x: int):
        if self.times.size == 0:
            return
        x = max(0, min(x, self.width()))
        ratio = x / max(1, self.width())
        t = int(float(self.times[0]) + (float(self.times[-1]) - float(self.times[0])) * ratio)
        self.seek_requested.emit(t)


class TradeWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Trades")
        self.resize(360, 600)

        layout = QVBoxLayout(self)
        self.lock_top = QCheckBox("Lock Top")
        self.lock_top.setChecked(True)
        layout.addWidget(self.lock_top)

        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["Time", "Price", "Size", "Side"])
        layout.addWidget(self.table)

    def set_rows(self, rows):
        self.table.setRowCount(0)
        for r in rows:
            row = self.table.rowCount()
            self.table.insertRow(row)
            t, p, q, side = r
            t_str = str(timedelta(seconds=t / 1_000_000)).split(".")[0]
            side_str = "SELL" if side < 0 else "BUY"
            color = QColor("#ff6060") if side < 0 else QColor("#60ff60")
            values = [t_str, f"{p:,}", f"{q:,}", side_str]
            for c, v in enumerate(values):
                item = QTableWidgetItem(v)
                item.setForeground(color)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(row, c, item)
        if self.lock_top.isChecked():
            self.table.scrollToTop()


class BoardWidget(QTableWidget):
    order_requested = pyqtSignal(str, int)  # side, price

    def __init__(self):
        super().__init__(20, 5)
        self.setHorizontalHeaderLabels(["AskO", "AskQ", "Price", "BidQ", "BidO"])
        self.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.horizontalHeader().setFixedHeight(20)
        self.verticalHeader().setVisible(False)
        self.setAlternatingRowColors(False)
        self.setShowGrid(True)
        self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.setStyleSheet(
            """
            QTableWidget {
                background: #000000;
                color: #d5e4ff;
                gridline-color: #17202a;
                font-family: Consolas, 'Courier New', monospace;
                font-size: 22px;
                font-weight: 700;
            }
            QHeaderView::section {
                background: #0f1318;
                color: #9db2c8;
                border: 1px solid #17202a;
                padding: 1px;
                font-size: 11px;
                font-weight: 700;
            }
            """
        )
        self._row_to_price = {}
        self.cellDoubleClicked.connect(self._on_cell_double_clicked)
        self._fit_rows_to_view()

    def _fit_rows_to_view(self):
        rows = max(1, self.rowCount())
        vh = max(1, self.viewport().height())
        row_h = max(11, vh // rows)
        for r in range(rows):
            self.setRowHeight(r, row_h)
        font = self.font()
        font.setPointSize(max(9, min(24, row_h - 2)))
        self.setFont(font)
        self.horizontalHeader().setFixedHeight(max(16, min(24, row_h + 2)))

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._fit_rows_to_view()

    @staticmethod
    def _fmt(v: int) -> str:
        return "" if int(v) == 0 else f"{int(v):,}"

    def _set_cell(self, row: int, col: int, text: str, fg: str, bg: str):
        item = QTableWidgetItem(text)
        item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        item.setForeground(QColor(fg))
        item.setBackground(QColor(bg))
        self.setItem(row, col, item)

    def _on_cell_double_clicked(self, row: int, col: int):
        if col not in (1, 3):
            return
        p = self._row_to_price.get(int(row))
        if p is None or p <= 0:
            return
        side = "SELL" if col == 1 else "BUY"
        self.order_requested.emit(side, int(p))

    def set_levels(
        self,
        asks: np.ndarray,
        bids: np.ndarray,
        highlight_prices: Optional[set] = None,
        last_trade_price: Optional[int] = None,
        pending_order_prices: Optional[set] = None,
    ):
        prices = set()
        ask_map = {}
        bid_map = {}
        self._row_to_price = {}

        for lv in asks:
            p, q, o = int(lv[0]), int(lv[1]), int(lv[2])
            if p <= 0:
                continue
            prices.add(p)
            ask_map[p] = (q, o)
        for lv in bids:
            p, q, o = int(lv[0]), int(lv[1]), int(lv[2])
            if p <= 0:
                continue
            prices.add(p)
            bid_map[p] = (q, o)

        if not prices:
            for r in range(self.rowCount()):
                for c in range(self.columnCount()):
                    self._set_cell(r, c, "", "#9db2c8", "#000000")
            return

        sorted_prices = sorted(prices, reverse=True)
        if len(sorted_prices) > self.rowCount():
            sorted_prices = sorted_prices[: self.rowCount()]

        for row, p in enumerate(sorted_prices):
            self._row_to_price[row] = int(p)
            aq, ao = ask_map.get(p, (0, 0))
            bq, bo = bid_map.get(p, (0, 0))

            self._set_cell(row, 0, self._fmt(ao), "#8fa3bb", "#000000")
            self._set_cell(row, 1, self._fmt(aq), "#00ff5f", "#000000")

            price_bg = "#262a30"
            price_fg = "#e7f2ff"
            if pending_order_prices and int(p) in pending_order_prices:
                price_bg = "#1a3d9e"
                price_fg = "#d7e6ff"
            if last_trade_price is not None and int(p) == int(last_trade_price):
                price_bg = "#6f6700"
                price_fg = "#fff7b2"
            if highlight_prices and int(p) in highlight_prices:
                price_bg = "#fff100"
                price_fg = "#000000"
            self._set_cell(row, 2, f"{p:,}", price_fg, price_bg)

            self._set_cell(row, 3, self._fmt(bq), "#ff3d3d", "#000000")
            self._set_cell(row, 4, self._fmt(bo), "#8fa3bb", "#000000")

        for row in range(len(sorted_prices), self.rowCount()):
            self._set_cell(row, 0, "", "#8fa3bb", "#000000")
            self._set_cell(row, 1, "", "#00ff5f", "#000000")
            self._set_cell(row, 2, "", "#e7f2ff", "#262a30")
            self._set_cell(row, 3, "", "#ff3d3d", "#000000")
            self._set_cell(row, 4, "", "#8fa3bb", "#000000")


class MainWindow(QMainWindow):
    def __init__(self, data_dir: str):
        super().__init__()
        self.dm = ItayomikunDataManager(data_dir)

        self.is_playing = False
        self.play_speed = 1.0
        self.last_real_time = 0.0
        self.virtual_time = 0

        self.chart = ChartWidget()
        self.trades = TradeWidget()
        self.trading_engine = (
            TradingEngine() if ENABLE_TRADING_FEATURE and TradingEngine is not None else None
        )
        self.trading = TradingWindow() if ENABLE_TRADING_FEATURE and TradingWindow is not None else None
        self.chart.seek_requested.connect(self.on_chart_seek)
        self.board_pending_prices = set()
        self.voice_dir = os.path.join(os.path.dirname(__file__), "voice")
        self.voice_files = {
            "hattyu": os.path.join(self.voice_dir, "hattyu.wav"),
            "yakujo": os.path.join(self.voice_dir, "yakujo.wav"),
            "kati": os.path.join(self.voice_dir, "kati.wav"),
            "make": os.path.join(self.voice_dir, "make.wav"),
        }

        self._init_ui()
        if self.trading is not None:
            self.trading.cancel_all_requested.connect(self.on_cancel_all_orders)
            self.trading.reset_requested.connect(self.on_reset_account)
        self.timer = QTimer(self)
        self.timer.setInterval(33)
        self.timer.timeout.connect(self.play_loop)
        self.refresh_dates()

    def _init_ui(self):
        self.setWindowTitle("synthetic parquet player")
        self.resize(980, 760)
        self.setStyleSheet("background:#101010;color:#f0f0f0;")

        tb = QToolBar("toolbar")
        self.addToolBar(tb)
        act_chart = QAction("Show Chart", self)
        act_chart.setCheckable(True)
        act_chart.toggled.connect(lambda v: self.chart.setVisible(v))
        tb.addAction(act_chart)
        act_trade = QAction("Show Trades", self)
        act_trade.setCheckable(True)
        act_trade.toggled.connect(lambda v: self.trades.setVisible(v))
        tb.addAction(act_trade)
        if self.trading is not None:
            act_trading = QAction("Show Trading", self)
            act_trading.setCheckable(True)
            act_trading.toggled.connect(lambda v: self.trading.setVisible(v))
            tb.addAction(act_trading)

        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)

        top = QHBoxLayout()
        top.addWidget(QLabel("Replay File"))
        self.combo_file = QComboBox()
        self.combo_file.currentTextChanged.connect(self.on_file_change)
        top.addWidget(self.combo_file)
        self.lbl_time = QLabel("--:--:--")
        top.addWidget(self.lbl_time)
        layout.addLayout(top)

        pred_line = QHBoxLayout()
        self.lbl_pred_mode = QLabel("Source: game/*.parquet only")
        self.lbl_pred_mode.setStyleSheet("color:#9db2c8;font-weight:700;")
        pred_line.addWidget(self.lbl_pred_mode)
        pred_line.addStretch()
        layout.addLayout(pred_line)

        seek = QHBoxLayout()
        self.lbl_idx = QLabel("Index: 0")
        seek.addWidget(self.lbl_idx)
        self.slider = QSlider(Qt.Orientation.Horizontal)
        self.slider.valueChanged.connect(self.on_slider_change)
        self.slider.sliderReleased.connect(self.on_slider_release)
        seek.addWidget(self.slider)
        layout.addLayout(seek)

        self.board = BoardWidget()
        self.board.order_requested.connect(self.on_board_order_request)
        layout.addWidget(self.board, 1)

        footer = QHBoxLayout()
        footer.setSpacing(4)
        btn_back = QPushButton("Back")
        btn_back.setMaximumHeight(28)
        btn_back.clicked.connect(self.step_back)
        footer.addWidget(btn_back)
        self.btn_play = QPushButton("Play")
        self.btn_play.setMaximumHeight(28)
        self.btn_play.clicked.connect(self.toggle_play)
        footer.addWidget(self.btn_play)
        btn_fwd = QPushButton("Forward")
        btn_fwd.setMaximumHeight(28)
        btn_fwd.clicked.connect(self.step_fwd)
        footer.addWidget(btn_fwd)
        btn_speed = QPushButton("x5")
        btn_speed.setMaximumHeight(28)
        btn_speed.setCheckable(True)
        btn_speed.toggled.connect(lambda on: setattr(self, "play_speed", 5.0 if on else 1.0))
        footer.addWidget(btn_speed)
        layout.addLayout(footer)

    def refresh_dates(self):
        self.combo_file.blockSignals(True)
        self.combo_file.clear()
        self.combo_file.addItems(self.dm.available_files)
        self.combo_file.blockSignals(False)
        if self.combo_file.count() > 0:
            self.combo_file.setCurrentIndex(0)
            self.on_file_change(self.combo_file.currentText())

    def on_file_change(self, name: str):
        if not name:
            return
        self.dm.load_file(name)
        if self.trading_engine is not None:
            self.trading_engine.reset()
            self._refresh_trading_window()
        self.slider.setRange(0, self.dm.max_idx)
        self.slider.setValue(0)
        self.virtual_time = self.dm.current_timestamp
        self.update_views()

        trades = self.dm.trade_indices
        self.chart.set_data(self.dm.time_arr[trades], self.dm.price_arr[trades])

    @staticmethod
    def _best_prices_from_levels(asks: np.ndarray, bids: np.ndarray):
        ask_prices = [int(x) for x in asks[:, 0] if int(x) > 0]
        bid_prices = [int(x) for x in bids[:, 0] if int(x) > 0]
        best_ask = min(ask_prices) if ask_prices else 0
        best_bid = max(bid_prices) if bid_prices else 0
        return best_bid, best_ask

    def _refresh_trading_window(self):
        if self.trading_engine is None or self.trading is None:
            return
        self.board_pending_prices = self.trading_engine.pending_prices()
        for ev in self.trading_engine.pop_audio_events():
            self._play_voice(ev)
        self.trading.refresh(
            self.trading_engine.snapshot(),
            list(self.trading_engine.pending),
            list(self.trading_engine.logs),
            list(self.trading_engine.log_rows),
        )

    def _play_voice(self, name: str):
        path = self.voice_files.get(name)
        if not path or not os.path.exists(path):
            return
        if winsound is None:
            return
        try:
            winsound.PlaySound(path, winsound.SND_FILENAME | winsound.SND_ASYNC | winsound.SND_NODEFAULT)
        except Exception:
            pass

    def on_cancel_all_orders(self):
        if self.trading_engine is None:
            return
        self.trading_engine.cancel_all()
        self._refresh_trading_window()
        self.update_views()

    def on_reset_account(self):
        if self.trading_engine is None:
            return
        self.trading_engine.reset()
        self._refresh_trading_window()
        self.update_views()

    def on_board_order_request(self, side: str, price: int):
        if self.trading_engine is None:
            return
        asks, bids = self.dm.board_levels()
        best_bid, best_ask = self._best_prices_from_levels(asks, bids)
        self.trading_engine.on_market(
            t_micro=int(self.dm.current_timestamp),
            best_bid=best_bid,
            best_ask=best_ask,
            last_price=int(self.dm.price_arr[self.dm.current_idx]),
            event_type=int(self.dm.event_arr[self.dm.current_idx]),
            trade_dir=int(self.dm.dir_arr[self.dm.current_idx]),
            trade_price=int(self.dm.price_arr[self.dm.current_idx]),
        )
        self.trading_engine.place_limit(
            side=side,
            limit_price=int(price),
            t_micro=int(self.dm.current_timestamp),
        )
        self._refresh_trading_window()
        self.update_views()

    def on_slider_change(self, value: int):
        self.lbl_idx.setText(f"Index: {value}")

    def on_slider_release(self):
        self.dm.step_to_index(self.slider.value())
        self.virtual_time = self.dm.current_timestamp
        self.update_views()

    def toggle_play(self):
        self.is_playing = not self.is_playing
        if self.is_playing:
            self.btn_play.setText("Stop")
            self.last_real_time = time.time()
            self.timer.start()
        else:
            self.btn_play.setText("Play")
            self.timer.stop()

    def play_loop(self):
        if not self.is_playing:
            return
        prev_idx = self.dm.current_idx
        now = time.time()
        dt = now - self.last_real_time
        self.last_real_time = now
        self.virtual_time += int(dt * 1_000_000 * self.play_speed)
        has_more = self.dm.process_until_time(self.virtual_time)
        highlight_prices = self.dm.trade_prices_in_range(prev_idx + 1, self.dm.current_idx)
        self.update_views(highlight_prices=highlight_prices)
        if self.slider.value() != self.dm.current_idx:
            self.slider.blockSignals(True)
            self.slider.setValue(self.dm.current_idx)
            self.slider.blockSignals(False)
        if not has_more:
            self.toggle_play()

    def step_fwd(self):
        self.toggle_stop_only()
        prev_idx = self.dm.current_idx
        self.dm.next_step()
        self.virtual_time = self.dm.current_timestamp
        self.slider.setValue(self.dm.current_idx)
        self.update_views(highlight_prices=self.dm.trade_prices_in_range(prev_idx + 1, self.dm.current_idx))

    def step_back(self):
        self.toggle_stop_only()
        prev_idx = self.dm.current_idx
        self.dm.prev_step()
        self.virtual_time = self.dm.current_timestamp
        self.slider.setValue(self.dm.current_idx)
        self.update_views(highlight_prices=self.dm.trade_prices_in_range(self.dm.current_idx, prev_idx - 1))

    def toggle_stop_only(self):
        if self.is_playing:
            self.is_playing = False
            self.timer.stop()
            self.btn_play.setText("Play")

    def update_views(self, highlight_prices: Optional[set] = None):
        asks, bids = self.dm.board_levels()
        if self.trading_engine is not None:
            best_bid, best_ask = self._best_prices_from_levels(asks, bids)
            self.trading_engine.on_market(
                t_micro=int(self.dm.current_timestamp),
                best_bid=best_bid,
                best_ask=best_ask,
                last_price=int(self.dm.price_arr[self.dm.current_idx]),
                event_type=int(self.dm.event_arr[self.dm.current_idx]),
                trade_dir=int(self.dm.dir_arr[self.dm.current_idx]),
                trade_price=int(self.dm.price_arr[self.dm.current_idx]),
            )
            self._refresh_trading_window()
        if highlight_prices is None:
            highlight_prices = set()
            if int(self.dm.event_arr[self.dm.current_idx]) == 2:
                highlight_prices.add(int(self.dm.price_arr[self.dm.current_idx]))
        last_trade_price = self.dm.latest_trade_price_up_to(self.dm.current_idx)
        self.board.set_levels(
            asks,
            bids,
            highlight_prices=highlight_prices,
            last_trade_price=last_trade_price,
            pending_order_prices=self.board_pending_prices,
        )
        self.lbl_time.setText(self.dm.current_time_str)
        self.lbl_idx.setText(f"Index: {self.dm.current_idx}/{self.dm.max_idx}")
        self.trades.set_rows(self.dm.trade_rows(limit=100))
        self.chart.set_current_time(self.dm.current_timestamp)

    def on_chart_seek(self, t: int):
        self.toggle_stop_only()
        self.virtual_time = t
        self.dm.process_until_time(t)
        self.slider.setValue(self.dm.current_idx)
        self.update_views()

    def keyPressEvent(self, event):
        if event.key() == Qt.Key.Key_Space:
            self.toggle_play()
        elif event.key() == Qt.Key.Key_Right:
            self.step_fwd()
        elif event.key() == Qt.Key.Key_Left:
            self.step_back()


def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    data_dir = base_dir
    app = QApplication(sys.argv)
    w = MainWindow(data_dir=data_dir)
    w.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
