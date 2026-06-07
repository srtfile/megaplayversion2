"""
MegaPlayer Server
=================
Flask backend that:
  • Serves the player UI            (GET /)
  • Proxies HLS manifests+segments  (GET /proxy)
  • Persists per-host rules         (GET/POST/DELETE /api/profiles)
  • Extracts stream URLs live       (POST /api/extract)  ← NEW
  • Exposes last results.json       (GET /results)

Run locally:
    pip install -r requirements.txt
    python app.py

Deploy to Render:
    Uses render.yaml — gunicorn starts with gthread worker for SSE support.
"""

import json
import os
import re
import sys
import threading
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import urllib3
from flask import Flask, Response, jsonify, request, send_file, stream_with_context

urllib3.disable_warnings()

# ── App setup ─────────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
PROFILES_PATH = os.path.join(BASE_DIR, "profile.json")
PORT          = int(os.environ.get("PORT", 6789))

app = Flask(__name__, static_folder=None)

# ── Upstream session ──────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.verify = False

DEFAULT_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36",
    "Referer":         "https://megaplay.buzz/",
    "Origin":          "https://megaplay.buzz",
    "Accept":          "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Site":  "cross-site",
    "Sec-Fetch-Mode":  "cors",
    "Sec-Fetch-Dest":  "empty",
}


# ── CORS helper ───────────────────────────────────────────────────────────────
CORS_HEADERS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Accept",
}

@app.after_request
def add_cors(resp):
    for k, v in CORS_HEADERS.items():
        resp.headers[k] = v
    return resp


# ── Helper: rewrite m3u8 URLs through /proxy ──────────────────────────────────
def rewrite_m3u8(content: str, original_url: str, referer: str, origin: str) -> str:
    base = original_url.rsplit("/", 1)[0] + "/"
    lines = []
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            def replace_uri(m):
                uri = m.group(1)
                abs_uri = urllib.parse.urljoin(base, uri)
                proxied = (
                    f"/proxy?url={urllib.parse.quote(abs_uri, safe='')}"
                    f"&ref={urllib.parse.quote(referer, safe='')}"
                    f"&origin={urllib.parse.quote(origin, safe='')}"
                )
                return f'URI="{proxied}"'
            line = re.sub(r'URI="([^"]+)"', replace_uri, line)
            lines.append(line)
        elif stripped:
            abs_url = urllib.parse.urljoin(base, stripped)
            proxied = (
                f"/proxy?url={urllib.parse.quote(abs_url, safe='')}"
                f"&ref={urllib.parse.quote(referer, safe='')}"
                f"&origin={urllib.parse.quote(origin, safe='')}"
            )
            lines.append(proxied)
        else:
            lines.append(line)
    return "\n".join(lines)


# ── /proxy ────────────────────────────────────────────────────────────────────
@app.route("/proxy")
def proxy():
    url     = request.args.get("url", "").strip()
    referer = request.args.get("ref",    DEFAULT_HEADERS["Referer"])
    origin  = request.args.get("origin", DEFAULT_HEADERS["Origin"])

    if not url:
        return Response("Missing url param", 400)

    headers = {**DEFAULT_HEADERS, "Referer": referer, "Origin": origin}

    try:
        upstream = SESSION.get(url, headers=headers, timeout=20, stream=True)
    except Exception as exc:
        return Response(f"Proxy error: {exc}", 502)

    content_type = upstream.headers.get("Content-Type", "application/octet-stream")
    is_m3u8 = (
        "mpegurl" in content_type.lower()
        or url.split("?")[0].lower().endswith(".m3u8")
    )

    if is_m3u8:
        body = upstream.content.decode("utf-8", errors="ignore")
        rewritten = rewrite_m3u8(body, url, referer, origin)
        return Response(
            rewritten,
            status=upstream.status_code,
            content_type="application/vnd.apple.mpegurl",
        )

    def generate():
        for chunk in upstream.iter_content(chunk_size=65536):
            if chunk:
                yield chunk

    resp = Response(generate(), status=upstream.status_code, content_type=content_type)
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


# ── /api/extract — Stream URL extractor (SSE) ────────────────────────────────
#
#   POST /api/extract
#   Body JSON: { "tmdb_id": 12345 }
#   Accept: text/event-stream   →  Server-Sent Events (live progress)
#   Accept: application/json    →  wait for all results, return array
#
#   SSE events:
#     data: {"event":"start","tmdb_id":N,"title":"...","total":N,"embed_urls":[...]}
#     data: {"event":"result","status":"ok|error","host_label":"...","stream_url":"...","stream_type":"m3u8|mp4","headers":{...},"embed_url":"...","error":"..."}
#     data: {"event":"done","ok":N,"total":N}
#     data: {"event":"error","message":"..."}
#     data: [DONE]

def _fetch_pipeline_quiet(url: str) -> list:
    """Fetch pipeline JSON without printing to stdout (safe inside SSE generator)."""
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def _run_extraction(tmdb_id: int):
    """
    Generator that yields result dicts one by one.
    All stdout prints from the extractor are suppressed so they don't corrupt SSE.
    """
    # Lazy-import the extractor module
    try:
        sys.path.insert(0, BASE_DIR)
        import allstreaming_combined_version_2_without_gui as astream
    except ImportError as e:
        yield {"event": "error", "message": f"Extractor module not found: {e}"}
        return

    # Fetch pipeline (quiet version — no print statements)
    try:
        pipeline = _fetch_pipeline_quiet(astream.PIPELINE_JSON_URL)
    except Exception as e:
        yield {"event": "error", "message": f"Pipeline fetch failed: {e}"}
        return

    # Find matching entries
    entries = astream.find_entries_by_tmdb(pipeline, [tmdb_id])
    if not entries:
        yield {"event": "error", "message": f"TMDB ID {tmdb_id} not found in pipeline"}
        return

    entry      = entries[0]
    embed_items = astream.get_embed_urls(entry)
    total      = len(embed_items)

    # Send start event — includes ALL embed URLs immediately for instant iframe buttons
    yield {
        "event":    "start",
        "tmdb_id":  tmdb_id,
        "title":    entry.get("title", "Unknown"),
        "imdb_id":  entry.get("imdb_id"),
        "total":    total,
        "embed_urls": [
            {"host_label": item["host_label"], "embed_url": item["embed_url"]}
            for item in embed_items
        ],
    }

    if total == 0:
        yield {"event": "done", "ok": 0, "total": 0}
        return

    # Extract in parallel; yield each result as it finishes
    ok_count = 0
    with ThreadPoolExecutor(max_workers=6) as pool:
        future_to_idx = {
            pool.submit(astream.process_embed, item): i
            for i, item in enumerate(embed_items)
        }
        for future in as_completed(future_to_idx):
            res = future.result()
            if res.get("status") == "ok":
                ok_count += 1
            yield {"event": "result", **res}

    yield {"event": "done", "ok": ok_count, "total": total}


@app.route("/api/extract", methods=["POST", "OPTIONS"])
def api_extract():
    if request.method == "OPTIONS":
        return Response("", 204)

    body = request.get_json(force=True, silent=True) or {}
    tmdb_id_raw = body.get("tmdb_id")
    if not tmdb_id_raw:
        return jsonify({"error": "Missing tmdb_id"}), 400
    try:
        tmdb_id = int(tmdb_id_raw)
    except (ValueError, TypeError):
        return jsonify({"error": "tmdb_id must be an integer"}), 400

    accept  = request.headers.get("Accept", "")
    use_sse = "text/event-stream" in accept

    if use_sse:
        def sse_stream():
            for item in _run_extraction(tmdb_id):
                yield f"data: {json.dumps(item)}\n\n"
            yield "data: [DONE]\n\n"

        resp = Response(
            stream_with_context(sse_stream()),
            content_type="text/event-stream; charset=utf-8",
        )
        resp.headers["Cache-Control"]    = "no-cache"
        resp.headers["X-Accel-Buffering"] = "no"   # disable nginx buffering on Render
        return resp

    # Plain JSON: collect everything synchronously
    results = list(_run_extraction(tmdb_id))
    return jsonify(results)


# ── / — serve player HTML ─────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_file(os.path.join(BASE_DIR, "index.html"))


# ── /results — serve last results.json if present ────────────────────────────
@app.route("/results")
def results():
    path = os.path.join(BASE_DIR, "results.json")
    if os.path.exists(path):
        return send_file(path, mimetype="application/json")
    return jsonify({}), 404


# ── /health — Render health check ────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


# ── Profile helpers ───────────────────────────────────────────────────────────
def _read_profiles() -> dict:
    if os.path.exists(PROFILES_PATH):
        try:
            with open(PROFILES_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _write_profiles(data: dict) -> None:
    with open(PROFILES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ── /api/profiles ─────────────────────────────────────────────────────────────
@app.route("/api/profiles", methods=["GET"])
def profiles_get():
    return jsonify(_read_profiles())


@app.route("/api/profiles", methods=["POST"])
def profiles_post():
    body = request.get_json(force=True, silent=True) or {}
    host = body.get("host", "").strip()
    if not host:
        return jsonify({"error": "Missing host"}), 400
    data = _read_profiles()
    data[host] = {k: v for k, v in body.items() if k != "host"}
    data[host].setdefault(
        "saved", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    _write_profiles(data)
    return jsonify({"ok": True, "host": host})


@app.route("/api/profiles/<path:host>", methods=["DELETE"])
def profiles_delete(host):
    data = _read_profiles()
    removed = host in data
    data.pop(host, None)
    _write_profiles(data)
    return jsonify({"ok": True, "removed": removed})


# ── Entry point (local dev) ───────────────────────────────────────────────────
if __name__ == "__main__":
    import time, webbrowser

    def _open():
        time.sleep(1.2)
        webbrowser.open(f"http://localhost:{PORT}")

    threading.Thread(target=_open, daemon=True).start()
    print(f"\n  ╔══════════════════════════════════════╗")
    print(f"  ║   MegaPlayer  →  http://localhost:{PORT}  ║")
    print(f"  ╚══════════════════════════════════════╝\n")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
