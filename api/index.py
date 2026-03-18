from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
from flask import Flask, Response, abort, request, send_file


APP_ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = APP_ROOT / "webapp" / "static"
DATA_DIR = Path.cwd()
CHUNK_SIZE = 512


def build_columns() -> list[str]:
    cols = ["Time", "Event", "Price", "Size", "OrderCount", "Direction"]
    for i in range(1, 11):
        cols += [f"Ask{i}_P", f"Ask{i}_Q", f"Ask{i}_O", f"Bid{i}_P", f"Bid{i}_Q", f"Bid{i}_O"]
    return cols


@dataclass(frozen=True)
class FileEntry:
    name: str
    path: Path
    size: int
    modified_ms: int


class SessionStore:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.columns = build_columns()

    def files(self) -> list[FileEntry]:
        items: list[FileEntry] = []
        if not self.data_dir.exists():
            return items
        for path in sorted(self.data_dir.glob("*.parquet"), reverse=True):
            stat = path.stat()
            items.append(
                FileEntry(
                    name=path.name,
                    path=path,
                    size=int(stat.st_size),
                    modified_ms=int(stat.st_mtime * 1000),
                )
            )
        return items

    @lru_cache(maxsize=8)
    def load_raw(self, filename: str) -> dict[str, Any]:
        path = (self.data_dir / filename).resolve()
        if path.parent != self.data_dir.resolve() or not path.exists() or path.suffix.lower() != ".parquet":
            raise FileNotFoundError(filename)

        df = pd.read_parquet(path, columns=self.columns)
        asks = []
        bids = []
        for i in range(1, 11):
            asks.append(df[[f"Ask{i}_P", f"Ask{i}_Q", f"Ask{i}_O"]].astype("int64").to_numpy())
            bids.append(df[[f"Bid{i}_P", f"Bid{i}_Q", f"Bid{i}_O"]].astype("int64").to_numpy())

        return {
            "name": filename,
            "row_count": len(df.index),
            "times": df["Time"].astype("int64").to_numpy(),
            "events": df["Event"].astype("int64").to_numpy(),
            "prices": df["Price"].astype("int64").to_numpy(),
            "sizes": df["Size"].astype("int64").to_numpy(),
            "directions": df["Direction"].astype("int64").to_numpy(),
            "asks": asks,
            "bids": bids,
        }

    def session_summary(self, filename: str) -> dict[str, Any]:
        raw = self.load_raw(filename)
        events = raw["events"]
        trade_indices = [i for i, event in enumerate(events.tolist()) if int(event) == 2]
        trade_times = raw["times"][trade_indices].tolist() if trade_indices else []
        trade_prices = raw["prices"][trade_indices].tolist() if trade_indices else []
        return {
            "name": raw["name"],
            "rowCount": raw["row_count"],
            "chunkSize": CHUNK_SIZE,
            "times": raw["times"].tolist(),
            "events": events.tolist(),
            "prices": raw["prices"].tolist(),
            "sizes": raw["sizes"].tolist(),
            "directions": raw["directions"].tolist(),
            "tradeIndices": trade_indices,
            "tradeTimes": trade_times,
            "tradePrices": trade_prices,
        }

    def session_chunk(self, filename: str, chunk_index: int) -> dict[str, Any]:
        raw = self.load_raw(filename)
        start = max(0, int(chunk_index) * CHUNK_SIZE)
        end = min(raw["row_count"], start + CHUNK_SIZE)
        ask_rows = []
        bid_rows = []
        for row in range(start, end):
            ask_levels = []
            bid_levels = []
            for level in range(10):
                ask_levels.append(raw["asks"][level][row].tolist())
                bid_levels.append(raw["bids"][level][row].tolist())
            ask_rows.append(ask_levels)
            bid_rows.append(bid_levels)
        return {
            "chunkIndex": int(chunk_index),
            "start": start,
            "end": end,
            "asks": ask_rows,
            "bids": bid_rows,
        }


app = Flask(__name__)
store = SessionStore(DATA_DIR)


def json_response(payload: Any) -> Response:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    accept_encoding = request.headers.get("Accept-Encoding", "")
    use_gzip = "gzip" in accept_encoding.lower() and len(raw) > 1024
    body = gzip.compress(raw) if use_gzip else raw
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store",
    }
    if use_gzip:
        headers["Content-Encoding"] = "gzip"
    return Response(body, headers=headers)


@app.get("/api/files")
def api_files() -> Response:
    payload = [
        {
            "name": item.name,
            "size": item.size,
            "modifiedMs": item.modified_ms,
        }
        for item in store.files()
    ]
    return json_response(payload)


@app.get("/api/session-summary")
def api_session_summary() -> Response:
    filename = request.args.get("file", "")
    if not filename:
        abort(400)
    try:
        return json_response(store.session_summary(filename))
    except FileNotFoundError:
        abort(404)


@app.get("/api/session-chunk")
def api_session_chunk() -> Response:
    filename = request.args.get("file", "")
    chunk = int(request.args.get("chunk", "0"))
    if not filename:
        abort(400)
    try:
        return json_response(store.session_chunk(filename, chunk))
    except FileNotFoundError:
        abort(404)


@app.get("/api/health")
def api_health() -> Response:
    return json_response({"ok": True})


@app.get("/")
def index() -> Response:
    return send_file(STATIC_DIR / "index.html")


@app.get("/app.js")
def app_js() -> Response:
    return send_file(STATIC_DIR / "app.js", mimetype="application/javascript")


@app.get("/styles.css")
def styles_css() -> Response:
    return send_file(STATIC_DIR / "styles.css", mimetype="text/css")


@app.get("/<path:_path>")
def fallback(_path: str) -> Response:
    return send_file(STATIC_DIR / "index.html")
