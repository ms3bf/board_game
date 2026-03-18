from __future__ import annotations

import gzip
import json
import mimetypes
import os
from functools import lru_cache
from pathlib import Path
from urllib.request import Request, urlopen

from flask import Flask, Response, abort, request, send_file

from webapp.server import SessionStore


APP_ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = APP_ROOT / "webapp" / "static"
VOICE_DIRS = [APP_ROOT / "webapp" / "voice", APP_ROOT / "voice"]
LOCAL_DATA_DIR = APP_ROOT
CACHE_DIR = Path(os.environ.get("BOARD_GAME_CACHE_DIR", "/tmp/board-game-data"))
BOARD_FILE_NAME = "demo_trade.parquet"
CHART_FILE_NAME = "demo_trade.chart.parquet"
LOGO_PATH = APP_ROOT / "logo-white.png"

app = Flask(__name__)


def json_response(payload):
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


def _blob_headers() -> dict[str, str]:
    token = os.environ.get("BLOB_READ_WRITE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BLOB_READ_WRITE_TOKEN is not set")
    return {"Authorization": f"Bearer {token}"}


def _download_blob(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = Request(url, headers=_blob_headers())
    with urlopen(req, timeout=60) as response, dest.open("wb") as fh:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            fh.write(chunk)


def _prepare_blob_cache() -> Path:
    board_url = os.environ.get("BOARD_PARQUET_URL", "").strip()
    chart_url = os.environ.get("CHART_PARQUET_URL", "").strip()
    if not board_url or not chart_url:
        return LOCAL_DATA_DIR

    board_path = CACHE_DIR / BOARD_FILE_NAME
    chart_path = CACHE_DIR / CHART_FILE_NAME
    if not board_path.exists() or board_path.stat().st_size == 0:
        _download_blob(board_url, board_path)
    if not chart_path.exists() or chart_path.stat().st_size == 0:
        _download_blob(chart_url, chart_path)
    return CACHE_DIR


@lru_cache(maxsize=1)
def get_store() -> SessionStore:
    data_dir = _prepare_blob_cache()
    return SessionStore(data_dir)


@app.get("/api/files")
def api_files():
    store = get_store()
    return json_response([
        {"name": item.name, "size": item.size, "modifiedMs": item.modified_ms}
        for item in store.files()
    ])


@app.get("/api/session-summary")
def api_session_summary():
    filename = request.args.get("file", "")
    if not filename:
        abort(400)
    try:
        return json_response(get_store().session_summary(filename))
    except FileNotFoundError:
        abort(404)


@app.get("/api/session-chunk")
def api_session_chunk():
    filename = request.args.get("file", "")
    chunk = int(request.args.get("chunk", "0"))
    if not filename:
        abort(400)
    try:
        return json_response(get_store().session_chunk(filename, chunk))
    except FileNotFoundError:
        abort(404)


@app.get("/api/chart-data")
def api_chart_data():
    filename = request.args.get("file", "")
    if not filename:
        abort(400)
    try:
        return json_response(get_store().chart_data(filename))
    except FileNotFoundError:
        abort(404)


@app.get("/api/health")
def api_health():
    return json_response({"ok": True})


@app.get("/")
def index():
    return send_file(STATIC_DIR / "index.html")


@app.get("/app.js")
def app_js():
    return send_file(STATIC_DIR / "app.js", mimetype="application/javascript")


@app.get("/styles.css")
def styles_css():
    return send_file(STATIC_DIR / "styles.css", mimetype="text/css")


@app.get("/logo-white.png")
def logo_png():
    return send_file(LOGO_PATH, mimetype="image/png")


@app.get("/voice/<path:filename>")
def voice_file(filename: str):
    for root in VOICE_DIRS:
        path = (root / filename).resolve()
        if path.parent == root.resolve() and path.exists():
            guessed = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
            return send_file(path, mimetype=guessed)
    abort(404)


@app.get("/<path:_path>")
def fallback(_path: str):
    return send_file(STATIC_DIR / "index.html")
