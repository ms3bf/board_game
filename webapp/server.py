from __future__ import annotations

import argparse
import gzip
import json
import mimetypes
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
        for path in sorted(self.data_dir.glob("*.parquet"), key=lambda item: item.stat().st_mtime, reverse=True):
            if path.name.endswith(".chart.parquet"):
                continue
            if not path.with_suffix(".chart.parquet").exists():
                continue
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

    def resolve_source(self, filename: str) -> Path:
        path = (self.data_dir / filename).resolve()
        if path.parent != self.data_dir.resolve() or not path.exists() or path.suffix.lower() != ".parquet":
            raise FileNotFoundError(filename)
        if path.name.endswith(".chart.parquet"):
            raise FileNotFoundError(filename)
        return path

    def resolve_chart(self, filename: str) -> Path:
        source = self.resolve_source(filename)
        chart_path = source.with_suffix(".chart.parquet")
        if not chart_path.exists():
            raise FileNotFoundError(chart_path.name)
        return chart_path

    @lru_cache(maxsize=8)
    def load_frame(self, filename: str) -> pd.DataFrame:
        path = self.resolve_source(filename)
        return pd.read_parquet(path, columns=self.columns)

    @lru_cache(maxsize=8)
    def load_chart_frame(self, filename: str) -> pd.DataFrame:
        path = self.resolve_chart(filename)
        return pd.read_parquet(path)

    @lru_cache(maxsize=16)
    def chunk_meta(self, filename: str) -> dict[str, Any]:
        frame = self.load_frame(filename)
        times = frame["Time"].astype("int64").to_numpy()
        chunk_first_times: list[int] = []
        chunk_last_times: list[int] = []
        for start in range(0, len(frame.index), CHUNK_SIZE):
            end = min(len(frame.index), start + CHUNK_SIZE)
            chunk_first_times.append(int(times[start]))
            chunk_last_times.append(int(times[end - 1]))
        return {
            "name": filename,
            "rowCount": len(frame.index),
            "chunkSize": CHUNK_SIZE,
            "firstTime": int(times[0]) if len(times) else 0,
            "lastTime": int(times[-1]) if len(times) else 0,
            "chunkFirstTimes": chunk_first_times,
            "chunkLastTimes": chunk_last_times,
        }

    def session_summary(self, filename: str) -> dict[str, Any]:
        summary = dict(self.chunk_meta(filename))
        chart = self.chart_data(filename)
        summary["chart"] = chart
        return summary

    def chart_data(self, filename: str) -> dict[str, Any]:
        frame = self.load_chart_frame(filename)
        payload: dict[str, Any] = {}
        for timeframe, group in frame.groupby("Timeframe", sort=False):
            ordered = group.sort_values("BucketTime")
            payload[str(timeframe)] = {
                "bucketTimes": ordered["BucketTime"].astype("int64").tolist(),
                "opens": ordered["Open"].astype("int64").tolist(),
                "highs": ordered["High"].astype("int64").tolist(),
                "lows": ordered["Low"].astype("int64").tolist(),
                "closes": ordered["Close"].astype("int64").tolist(),
                "volumes": ordered["Volume"].astype("int64").tolist(),
                "trades": ordered["Trades"].astype("int64").tolist(),
            }
        return payload

    def session_chunk(self, filename: str, chunk_index: int) -> dict[str, Any]:
        frame = self.load_frame(filename)
        start = max(0, int(chunk_index) * CHUNK_SIZE)
        end = min(len(frame.index), start + CHUNK_SIZE)
        chunk = frame.iloc[start:end]
        asks: list[list[list[int]]] = []
        bids: list[list[list[int]]] = []
        for _, row in chunk.iterrows():
            ask_levels = []
            bid_levels = []
            for level in range(1, 11):
                ask_levels.append([int(row[f"Ask{level}_P"]), int(row[f"Ask{level}_Q"]), int(row[f"Ask{level}_O"])])
                bid_levels.append([int(row[f"Bid{level}_P"]), int(row[f"Bid{level}_Q"]), int(row[f"Bid{level}_O"])])
            asks.append(ask_levels)
            bids.append(bid_levels)
        return {
            "chunkIndex": int(chunk_index),
            "start": start,
            "end": end,
            "times": chunk["Time"].astype("int64").tolist(),
            "events": chunk["Event"].astype("int64").tolist(),
            "prices": chunk["Price"].astype("int64").tolist(),
            "sizes": chunk["Size"].astype("int64").tolist(),
            "directions": chunk["Direction"].astype("int64").tolist(),
            "asks": asks,
            "bids": bids,
        }


class BoardGameHandler(SimpleHTTPRequestHandler):
    server_version = "BoardGameWeb/1.0"

    def __init__(self, *args, directory: str, session_store: SessionStore, **kwargs):
        self.session_store = session_store
        super().__init__(*args, directory=directory, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/voice/"):
            rel_path = parsed.path.removeprefix("/voice/")
            voice_roots = [
                Path(__file__).resolve().parent / "voice",
                self.session_store.data_dir / "voice",
            ]
            voice_path = None
            for root in voice_roots:
                candidate = (root / rel_path).resolve()
                if candidate.parent == root.resolve() and candidate.exists():
                    voice_path = candidate
                    break
            if voice_path is None:
                self.send_error(HTTPStatus.NOT_FOUND, "voice not found")
                return
            content_type = mimetypes.guess_type(str(voice_path))[0] or "application/octet-stream"
            body = voice_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

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

        if parsed.path == "/api/chart-data":
            params = parse_qs(parsed.query)
            filename = (params.get("file") or [""])[0]
            if not filename:
                self.send_error(HTTPStatus.BAD_REQUEST, "file query is required")
                return
            try:
                self._send_json(self.session_store.chart_data(filename))
            except FileNotFoundError:
                self.send_error(HTTPStatus.NOT_FOUND, "chart parquet not found")
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
