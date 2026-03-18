from __future__ import annotations

import argparse
import gzip
import json
from dataclasses import dataclass
from functools import lru_cache
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd


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


class BoardGameHandler(SimpleHTTPRequestHandler):
    server_version = "BoardGameWeb/1.0"

    def __init__(self, *args, directory: str, session_store: SessionStore, **kwargs):
        self.session_store = session_store
        super().__init__(*args, directory=directory, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/files":
            payload = [
                {
                    "name": item.name,
                    "size": item.size,
                    "modifiedMs": item.modified_ms,
                }
                for item in self.session_store.files()
            ]
            self._send_json(payload)
            return

        if parsed.path == "/api/session-summary":
            params = parse_qs(parsed.query)
            filename = (params.get("file") or [""])[0]
            if not filename:
                self.send_error(HTTPStatus.BAD_REQUEST, "file query is required")
                return
            try:
                self._send_json(self.session_store.session_summary(filename))
            except FileNotFoundError:
                self.send_error(HTTPStatus.NOT_FOUND, "parquet not found")
            return

        if parsed.path == "/api/session-chunk":
            params = parse_qs(parsed.query)
            filename = (params.get("file") or [""])[0]
            chunk_index = int((params.get("chunk") or ["0"])[0])
            if not filename:
                self.send_error(HTTPStatus.BAD_REQUEST, "file query is required")
                return
            try:
                self._send_json(self.session_store.session_chunk(filename, chunk_index))
            except FileNotFoundError:
                self.send_error(HTTPStatus.NOT_FOUND, "parquet not found")
            return

        if parsed.path == "/api/health":
            self._send_json({"ok": True})
            return

        return super().do_GET()

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def _send_json(self, payload: Any):
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        accept_encoding = self.headers.get("Accept-Encoding", "")
        use_gzip = "gzip" in accept_encoding.lower() and len(raw) > 1024
        body = gzip.compress(raw) if use_gzip else raw

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        if use_gzip:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser(description="Board Game web app server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--data-dir",
        default=str(Path(__file__).resolve().parent.parent),
        help="Directory containing replay parquet files",
    )
    args = parser.parse_args()

    app_dir = Path(__file__).resolve().parent
    static_dir = app_dir / "static"
    data_dir = Path(args.data_dir).resolve()
    session_store = SessionStore(data_dir=data_dir)

    def handler(*handler_args, **handler_kwargs):
        return BoardGameHandler(
            *handler_args,
            directory=str(static_dir),
            session_store=session_store,
            **handler_kwargs,
        )

    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"Serving http://{args.host}:{args.port} with parquet from {data_dir}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
