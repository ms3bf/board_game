from __future__ import annotations

from pathlib import Path

from flask import Flask

from webapp.server import SessionStore

# Vercel imports this module-level app.
# Reuse the same Flask app structure as the local web server endpoints.
from flask import Response, abort, request, send_file
import gzip
import json
import mimetypes


APP_ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = APP_ROOT / "webapp" / "static"
DATA_DIR = APP_ROOT
VOICE_DIRS = [APP_ROOT / "webapp" / "voice", APP_ROOT / "voice"]

app = Flask(__name__)
store = SessionStore(DATA_DIR)


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


@app.get("/api/files")
def api_files():
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
        return json_response(store.session_summary(filename))
    except FileNotFoundError:
        abort(404)


@app.get("/api/session-chunk")
def api_session_chunk():
    filename = request.args.get("file", "")
    chunk = int(request.args.get("chunk", "0"))
    if not filename:
        abort(400)
    try:
        return json_response(store.session_chunk(filename, chunk))
    except FileNotFoundError:
        abort(404)


@app.get("/api/chart-data")
def api_chart_data():
    filename = request.args.get("file", "")
    if not filename:
        abort(400)
    try:
        return json_response(store.chart_data(filename))
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
