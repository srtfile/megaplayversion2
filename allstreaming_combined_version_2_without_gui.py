#!/usr/bin/env python3
"""
Stream URL Extractor — CLI
Fetches embed URLs from pipeline_summary.json by TMDB ID,
then extracts direct stream links from each host.

Just run the script and enter TMDB ID(s) when prompted.
"""

import re, sys, json, ast, codecs, random, string, time
from urllib.parse import urlparse
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
PIPELINE_JSON_URL = "https://raw.githubusercontent.com/srtfile/movie-data/main/pipeline_summary.json"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36")

MAX_WORKERS = 6   # parallel extraction threads per movie


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────
def _session(headers: dict = None) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    if headers:
        s.headers.update(headers)
    return s


# ─────────────────────────────────────────────────────────────────────────────
# Shared utilities
# ─────────────────────────────────────────────────────────────────────────────
def _to_base(n: int, base: int) -> str:
    if n == 0:
        return "0"
    chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    out = []
    while n:
        out.append(chars[n % base])
        n //= base
    return "".join(reversed(out))


def unpack_packer(packed: str) -> str:
    """Dean Edwards p,a,c,k,e,d decoder."""
    m = re.search(
        r"}\s*\(\s*'((?:[^'\\]|\\.)*)'\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*'((?:[^'\\]|\\.)*)'\s*\.split\(",
        packed, re.DOTALL)
    if not m:
        m = re.search(
            r"eval\(function\(p,a,c,k,e,d\)\{[^}]+\}\('(.*?)',(\d+),(\d+),'(.*?)'\.split\('\|'\)\)\)",
            packed, re.DOTALL)
    if not m:
        return packed
    payload = m.group(1).replace("\\'", "'")
    base = int(m.group(2))
    keys = m.group(4).split("|")
    lookup = {_to_base(i, base): w for i, w in enumerate(keys) if w}
    return re.sub(r"\b\w+\b", lambda mo: lookup.get(mo.group(0), mo.group(0)), payload)


def find_m3u8(text: str) -> list:
    return list(dict.fromkeys(re.findall(
        r'https?://[^\s"\'\]\[<>]+\.m3u8[^\s"\'\]\[<>]*', text)))


def find_mp4(text: str) -> list:
    return list(dict.fromkeys(re.findall(
        r'https?://[^\s"\'\]\[<>]+\.mp4[^\s"\'\]\[<>]*', text)))


# ─────────────────────────────────────────────────────────────────────────────
# Extractor functions
# ─────────────────────────────────────────────────────────────────────────────

# ── MixDrop ──────────────────────────────────────────────────────────────────
def _mixdrop_unpack(p, a, c, k):
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    def base_encode(n):
        rem = n % a
        digit = chr(rem + 29) if rem > 35 else digits[rem]
        return digit if n < a else base_encode(n // a) + digit
    d = {}
    for i in range(c - 1, -1, -1):
        key = base_encode(i)
        d[key] = k[i] if i < len(k) and k[i] else key
    return re.compile(r'\b\w+\b').sub(lambda mo: d.get(mo.group(0), mo.group(0)), p)


def _mixdrop_extract_args(html: str) -> str:
    start = html.find("eval(function(p,a,c,k,e,d)")
    if start == -1:
        raise RuntimeError("MixDrop: eval(function... not found in page")
    i = start + len("eval(function(p,a,c,k,e,d)")
    depth = 0
    while i < len(html):
        if html[i] == '{':
            depth += 1
        elif html[i] == '}':
            depth -= 1
            if depth == 0:
                i += 1
                break
        i += 1
    while i < len(html) and html[i] != '(':
        i += 1
    if i >= len(html):
        raise RuntimeError("MixDrop: argument list opening paren not found")
    i += 1
    arg_start = i
    depth = 1
    while i < len(html) and depth > 0:
        if html[i] == '(':
            depth += 1
        elif html[i] == ')':
            depth -= 1
        i += 1
    return html[arg_start:i - 1]


def extract_mixdrop(url: str) -> dict:
    url = url.replace('/f/', '/e/')
    host = urlparse(url).scheme + "://" + urlparse(url).netloc
    r = _session({"Referer": host + "/"}).get(url, timeout=20)
    r.raise_for_status()
    raw_args = _mixdrop_extract_args(r.text)
    raw_args = raw_args.replace(".split('|')", "")
    try:
        data = ast.literal_eval(f"({raw_args})")
    except Exception as e:
        raise RuntimeError(f"MixDrop: failed to parse packed args — {e}")
    p, a, c, k = str(data[0]), int(data[1]), int(data[2]), data[3]
    if isinstance(k, str):
        k = k.split('|')
    decoded = _mixdrop_unpack(p, a, c, k)
    vm = re.search(r'MDCore\.wurl\s*=\s*["\']([^"\']+)["\']', decoded)
    if not vm:
        raise RuntimeError("MixDrop: MDCore.wurl not found in decoded JS")
    video_url = vm.group(1)
    if not video_url.startswith("http"):
        video_url = "https:" + video_url
    return {"url": video_url, "type": "mp4", "headers": {"Referer": host + "/"}}


# ── Vidmoly ──────────────────────────────────────────────────────────────────
def extract_vidmoly(url: str) -> dict:
    r = _session({"Referer": "https://vidmoly.biz"}).get(url, timeout=20)
    r.raise_for_status()
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', r.text, re.DOTALL)
    joined = "\n".join(filter(None, scripts))
    m = re.search(r'file\s*:\s*[\'"]([^\'"]+?\.m3u8[^\'"]*)[\'"]', joined)
    if not m:
        raise RuntimeError("Vidmoly: m3u8 not found")
    return {"url": m.group(1), "type": "m3u8", "headers": {"Referer": "https://vidmoly.biz"}}


# ── Voe.sx ───────────────────────────────────────────────────────────────────
def extract_voe(url: str) -> dict:
    from bs4 import BeautifulSoup
    host = urlparse(url).scheme + "://" + urlparse(url).netloc + "/"
    r = _session({"Referer": host}).get(url, timeout=20)
    r.raise_for_status()
    html = r.text
    if 'Redirecting...' in html:
        new_url = re.search(r"href\s*=\s*'(.*?)';", html).group(1)
        r = _session({"Referer": host}).get(new_url, timeout=20)
        r.raise_for_status()
        html = r.text
    soup = BeautifulSoup(html, 'html.parser')
    script_tag = soup.find('script', attrs={'type': 'application/json'})
    if not script_tag:
        raise RuntimeError("Voe: JSON script tag not found")
    encoded = re.search(r'\["(.*?)"\]', script_tag.string).group(1)
    data = codecs.decode(encoded, 'rot_13')
    for p in ["@$", "^^", "~@", "%?", "*~", "!!", "#&"]:
        data = re.sub(re.escape(p), "_", data)
    data = data.replace("_", "")
    data = b64decode(data).decode()
    data = ''.join(chr(ord(c) - 3) for c in data)
    data = data[::-1]
    data = b64decode(data).decode()
    parsed = json.loads(data)
    video_url = parsed.get('source') or parsed.get('hls') or parsed.get('url')
    if not video_url:
        raise RuntimeError("Voe: source URL not found in decoded JSON")
    vtype = "m3u8" if ".m3u8" in video_url else "mp4"
    return {"url": video_url, "type": vtype, "headers": {"Referer": host}}


# ── StreamWish ────────────────────────────────────────────────────────────────
def extract_streamwish(url: str) -> dict:
    m = re.search(r'/e/([A-Za-z0-9]+)', url)
    if not m:
        raise ValueError("StreamWish: cannot parse file code")
    file_code = m.group(1)
    origin = urlparse(url).netloc
    target = f"https://playnixes.com/e/{file_code}"
    r = _session({"Referer": f"https://{origin}/"}).get(target, timeout=20)
    r.raise_for_status()
    packed = re.search(
        r"(eval\(function\(p,a,c,k,e,d\)\{.*?\.split\('\|'\)[^)]*\)\))",
        r.text, re.DOTALL)
    if not packed:
        urls = find_m3u8(r.text)
        if urls:
            return {"url": urls[0], "type": "m3u8", "extra": urls}
        raise ValueError("StreamWish: packed JS not found")
    decoded = unpack_packer(packed.group(1))
    streams = dict(re.findall(r'"(hls[234])"\s*:\s*"([^"]+)"', decoded))
    extra = find_m3u8(decoded)
    best = streams.get("hls4") or streams.get("hls3") or streams.get("hls2") or (extra[0] if extra else None)
    if not best:
        raise RuntimeError("StreamWish: no stream URL found")
    return {"url": best, "type": "m3u8", "streams": streams, "extra": extra}


# ── StreamTa ──────────────────────────────────────────────────────────────────
_ST_TERM = re.compile(r"\s*(['\"])((?:\\.|(?!\1).)*)\1\s*")
_ST_PSTR = re.compile(r"\s*\(\s*(['\"])((?:\\.|(?!\1).)*)\1\s*\)\s*")
_ST_SUBS = re.compile(r"\.substring\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\)")
_ST_PLUS = re.compile(r"\s*\+\s*")

def _st_read_term(s, i):
    for pat in (_ST_TERM, _ST_PSTR):
        mo = pat.match(s, i)
        if mo:
            lit = mo.group(2)
            j = mo.end()
            while True:
                sm = _ST_SUBS.match(s, j)
                if not sm: break
                a = int(sm.group(1))
                b = int(sm.group(2)) if sm.group(2) else None
                lit = lit[a:b] if b is not None else lit[a:]
                j = sm.end()
            return lit, j
    return None

def extract_streamta(url: str) -> dict:
    r = _session().get(url, timeout=20)
    r.raise_for_status()
    candidates = []
    for mo in re.finditer(
        r"document\.getElementById\(\s*['\"]([^'\"]+)['\"]\s*\)\.innerHTML\s*=\s*([^;]+);",
        r.text):
        stmt = mo.group(2).strip()
        parts, i, n = [], 0, len(stmt)
        ok = True
        while i < n:
            t = _st_read_term(stmt, i)
            if t is None: ok = False; break
            parts.append(t[0]); i = t[1]
            if i >= n: break
            pm = _ST_PLUS.match(stmt, i)
            if not pm: ok = False; break
            i = pm.end()
        if not ok: continue
        res = "".join(parts)
        if "/get_video?id=" not in res or "token=" not in res: continue
        if res.startswith("//"): res = "https:" + res
        elif res.startswith("/"): res = "https://streamta.site" + res
        candidates.append(res)
    if not candidates:
        raise RuntimeError("StreamTa: no /get_video URL deobfuscated")
    s2 = _session()
    for signed in candidates:
        r2 = s2.get(signed, headers={"Referer": url}, allow_redirects=False, timeout=20)
        if r2.status_code in (301,302,303,307,308) and "Location" in r2.headers:
            return {"url": r2.headers["Location"], "type": "mp4"}
        if r2.status_code == 200:
            return {"url": signed, "type": "mp4"}
    raise RuntimeError("StreamTa: none of the candidates worked")


# ── StreamRuby ────────────────────────────────────────────────────────────────
def extract_streamruby(url: str) -> dict:
    try:
        from curl_cffi import requests as cf
        r = cf.get(url, headers={"User-Agent": UA, "Referer": "https://streamruby.com/"},
                   impersonate="chrome120", timeout=30)
        html = r.text
    except Exception:
        r = _session({"Referer": "https://streamruby.com/"}).get(url, timeout=30)
        html = r.text
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    packed = next((s for s in scripts if 'eval(function(p,a,c,k' in s), None)
    if not packed:
        raise RuntimeError("StreamRuby: no packed JS")
    decoded = unpack_packer(packed)
    urls = find_m3u8(decoded)
    if not urls:
        raise RuntimeError("StreamRuby: no m3u8 found")
    best = next((u for u in urls if "master.m3u8" in u), urls[0])
    return {"url": best, "type": "m3u8", "extra": urls}


# ── Vids.st ───────────────────────────────────────────────────────────────────
def extract_vids_st(url: str) -> dict:
    ID_RE = re.compile(r"/e/(\d+)")
    URL_RE = re.compile(r'const\s+url\s*=\s*"([^"]+\.m3u8[^"]*)"')
    CDN = "https://cdn.vids.st/video{id}/master.m3u8"
    m = ID_RE.search(url)
    if m:
        stream = CDN.format(id=m.group(1))
        try:
            h = {"User-Agent": UA, "Referer": "https://vids.st/", "Accept": "*/*"}
            r2 = requests.get(stream, headers=h, timeout=15, stream=True)
            if r2.status_code == 200 and b"#EXTM3U" in r2.raw.read(64):
                r2.close()
                return {"url": stream, "type": "m3u8", "method": "cdn-direct"}
        except Exception:
            pass
    try:
        from curl_cffi import requests as cf
        r = cf.get(url, impersonate="chrome", timeout=20, headers={"Referer": "https://vids.st/"})
    except Exception:
        r = _session({"Referer": "https://vids.st/"}).get(url, timeout=20)
    html = r.text.replace("\\/", "/")
    mv = URL_RE.search(html)
    if not mv:
        raise RuntimeError("Vids.st: m3u8 not found in page")
    return {"url": mv.group(1), "type": "m3u8", "method": "page-scrape"}


# ── SaveFiles ─────────────────────────────────────────────────────────────────
def extract_savefiles(url: str) -> dict:
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False})
    except ImportError:
        raise RuntimeError("SaveFiles requires cloudscraper: pip install cloudscraper")
    m = re.search(r'/e/([a-z0-9]+)', url)
    if not m:
        raise ValueError("SaveFiles: cannot extract file_code")
    file_code = m.group(1)
    H = {"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
         "accept-language": "en-US,en;q=0.9", "origin": "https://savefiles.com",
         "referer": "https://savefiles.com/", "user-agent": UA, "dnt": "1"}
    scraper.get(url, headers=H)
    resp = scraper.post("https://savefiles.com/dl",
                        data=f"op=embed&file_code={file_code}&auto=1&referer=",
                        headers={**H, "content-type": "application/x-www-form-urlencoded",
                                  "referer": url}, allow_redirects=True)
    resp.raise_for_status()
    mv = re.search(r'sources:\s*\[\{file:"([^"]+\.m3u8[^"]+)"', resp.text) or \
         re.search(r'(https://[^\s"\']+\.m3u8[^\s"\']*)', resp.text)
    if not mv:
        raise RuntimeError("SaveFiles: no m3u8 found")
    return {"url": mv.group(1), "type": "m3u8"}


# ── BigShare ──────────────────────────────────────────────────────────────────
def extract_bigshare(url: str) -> dict:
    try:
        r = _session().get(url, timeout=30)
        html = r.text
        matches = list(dict.fromkeys(re.findall(
            r"""['"](https?://[^'"]+\.(?:mp4|m3u8|mkv|webm)[^'"]*)['"']""", html, re.I)))
        if matches:
            return {"url": matches[0], "type": "m3u8" if ".m3u8" in matches[0] else "mp4",
                    "extra": matches}
    except Exception:
        pass
    try:
        import cloudscraper
        s = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})
        r = s.get(url, timeout=45)
        html = r.text
        matches = list(dict.fromkeys(re.findall(
            r"""['"](https?://[^'"]+\.(?:mp4|m3u8|mkv|webm)[^'"]*)['"']""", html, re.I)))
        if matches:
            return {"url": matches[0], "type": "m3u8" if ".m3u8" in matches[0] else "mp4",
                    "extra": matches}
    except Exception:
        pass
    raise RuntimeError("BigShare: no stream URL found")


# ── DoodStream family ─────────────────────────────────────────────────────────
DOOD_MIRRORS = [
    "dood.watch","dood.re","dood.so","dood.la","dood.pm","dood.ws","dood.wf",
    "dood.to","dood.cx","dood.sh","dood.li","doods.pro","ds2play.com",
    "ds2video.com","d000d.com","d0000d.com","d-s.io","vidply.com","playmogo.com",
]

def _dood_session(use_cs=False):
    if use_cs:
        try:
            import cloudscraper
            s = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False})
            s.headers.update({"User-Agent": UA})
            return s
        except ImportError:
            pass
    return _session()

def _dood_try_mirror(session, mirror, vid):
    url = f"https://{mirror}/e/{vid}"
    try:
        r = session.get(url, timeout=20, allow_redirects=True)
    except Exception:
        return None
    if r.status_code != 200 or "/pass_md5/" not in r.text:
        return None
    return r.url, r.text

def extract_dood(url: str) -> dict:
    m = re.search(r'/[ed]/([A-Za-z0-9]+)', url.strip())
    vid = m.group(1) if m else url.strip()
    session = None; player_url = None; html = None
    for engine in ("requests", "cloudscraper"):
        sess = _dood_session(engine == "cloudscraper")
        for mirror in DOOD_MIRRORS:
            hit = _dood_try_mirror(sess, mirror, vid)
            if hit:
                session, player_url, html = sess, hit[0], hit[1]; break
        if html: break
    if not html:
        raise RuntimeError(f"DoodStream: no working mirror for id={vid!r}")
    parsed = urlparse(player_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    pm = re.search(r"\$\.get\(['\"](/pass_md5/[^'\"]+)['\"]", html)
    if not pm:
        raise RuntimeError("DoodStream: pass_md5 endpoint not in HTML")
    path = pm.group(1)
    token = path.rstrip("/").rsplit("/", 1)[-1]
    r2 = session.get(base + path, headers={"Referer": player_url,
                                             "X-Requested-With": "XMLHttpRequest"}, timeout=20)
    r2.raise_for_status()
    body = r2.text.strip()
    if body == "RELOAD" or not body.startswith("http"):
        raise RuntimeError(f"DoodStream: pass_md5 returned: {body!r}")
    rnd = "".join(random.choices(string.ascii_letters + string.digits, k=10))
    direct = body + f"{rnd}?token={token}&expiry={int(time.time()*1000)}"
    return {"url": direct, "type": "mp4", "headers": {"Referer": player_url, "User-Agent": UA}}


# ── Luluvdoo ──────────────────────────────────────────────────────────────────
def extract_luluvdoo(url: str) -> dict:
    r = _session({"Referer": "https://luluvdoo.com/", "Origin": "https://luluvdoo.com"}).get(url, timeout=20)
    r.raise_for_status()
    packed = re.search(r"(eval\(function\(p,a,c,k,e,d\)\{.*?\.split\('\|'\)[^)]*\)\))", r.text, re.DOTALL)
    if not packed:
        raise RuntimeError("Luluvdoo: packed JS not found")
    decoded = unpack_packer(packed.group(1))
    urls = find_m3u8(decoded)
    if not urls:
        raise RuntimeError("Luluvdoo: m3u8 not found")
    return {"url": urls[0], "type": "m3u8"}


# ── FileNoons / FileLions / VidNest / Bysejikuar / Vidara family ──────────────
_FN_PACKER = re.compile(
    r"\}\s*\(\s*'((?:[^'\\]|\\.)*)'\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*'((?:[^'\\]|\\.)*)'\.split\('\|'\)",
    re.S)
_FN_LINKS = re.compile(r'(?:var\s+)?(?:links|sources)\s*=\s*(\{[^{}]*"hls[234]"\s*:[^{}]*\})', re.S)
_FN_KV = re.compile(r'"(hls[234])"\s*:\s*"([^"]+)"')

def _fn_decode_base(word, base):
    n = 0
    for ch in word:
        if ch.isdigit(): d = int(ch)
        elif ch.islower(): d = ord(ch) - ord('a') + 10
        elif ch.isupper(): d = ord(ch) - ord('A') + 36
        else: return None
        if d >= base: return None
        n = n * base + d
    return n

def _fn_unpack(payload):
    m = _FN_PACKER.search(payload)
    if not m: return payload
    p, a, c, k = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4).split('|')
    p = p.encode().decode('unicode_escape')
    def repl(mo):
        word = mo.group(0)
        idx = _fn_decode_base(word, a)
        if idx is not None and 0 <= idx < len(k) and k[idx]:
            return k[idx]
        return word
    return re.sub(r"\b\w+\b", repl, p)

def _fn_resolve_txt(u, referer, sess):
    r = sess.get(u, headers={**{"User-Agent": UA}, "Referer": referer},
                 allow_redirects=True, timeout=20)
    final = r.url; body = (r.text or "").strip()
    if final.endswith(".m3u8") or "m3u8" in final: return final
    if body.startswith("http") and ".m3u8" in body.split()[0]: return body.split()[0]
    if body.startswith("#EXTM3U"): return final
    return None

def extract_filenoons(url: str) -> dict:
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    sess = _session()
    r = sess.get(url, timeout=20)
    r.raise_for_status()
    unpacked = _fn_unpack(r.text)
    block = _FN_LINKS.search(unpacked)
    if not block:
        urls = find_m3u8(unpacked)
        if urls:
            return {"url": urls[0], "type": "m3u8", "extra": urls}
        raise RuntimeError("FileNoons: no links block found")
    links = dict(_FN_KV.findall(block.group(1)))
    m3u8 = links.get("hls2")
    if not m3u8:
        for k in ("hls4", "hls3"):
            if k in links:
                resolved = _fn_resolve_txt(links[k], origin + "/", sess)
                if resolved: m3u8 = resolved; break
    if not m3u8:
        raise RuntimeError("FileNoons: no m3u8 resolved")
    return {"url": m3u8, "type": "m3u8", "streams": links}


# ── Vidoza ────────────────────────────────────────────────────────────────────
def extract_vidoza(url: str) -> dict:
    r = _session().get(url, timeout=20)
    r.raise_for_status()
    html = r.text
    if "sourcesCode:" not in html:
        raise RuntimeError("Vidoza: sourcesCode not found")
    m = re.search(r'src:\s*"([^"]+)"', html)
    if not m:
        raise RuntimeError("Vidoza: src URL not found")
    return {"url": m.group(1), "type": "mp4"}


# ── Upzur ─────────────────────────────────────────────────────────────────────
def extract_upzur(url: str) -> dict:
    sess = _session({"DNT": "1"})
    sess.cookies.update({"lang": "english", "aff": "4881"})
    r = sess.get(url, timeout=15)
    r.raise_for_status()
    html = r.text
    fid = re.search(r'embed-([a-z0-9]+)\.html', url)
    if not fid:
        raise ValueError("Upzur: cannot parse file ID")
    results = []
    arr = re.search(r'var\s+\w+\s*=\s*(\[(?:"[^"]*",?\s*)+\])', html)
    if arr:
        chars = re.findall(r'"(\\x[0-9a-fA-F]{2}|[^"\\])"', arr.group(1))
        decoded = "".join(bytes.fromhex(c[2:]).decode() if c.startswith("\\x") else c
                          for c in reversed(chars))
        mp4 = re.search(r'src="(https://[^"]+\.mp4)"', decoded)
        if mp4:
            results.append(mp4.group(1))
    direct = re.findall(r'https://peanut\.upzur\.com/d/[^"\'>\s]+\.mp4', html)
    results.extend(u for u in direct if u not in results)
    if results:
        return {"url": results[0], "type": "mp4", "extra": results}
    raise RuntimeError("Upzur: no direct media links found")


# ── Vinovo ────────────────────────────────────────────────────────────────────
def extract_vinovo(url: str) -> dict:
    r = _session({"Referer": "https://vinovo.to/"}).get(url, timeout=20)
    r.raise_for_status()
    urls = find_m3u8(r.text)
    if not urls:
        packed = re.search(r"(eval\(function\(p,a,c,k,e,d\)\{.*?\.split\('\|'\)[^)]*\)\))", r.text, re.DOTALL)
        if packed:
            decoded = unpack_packer(packed.group(1))
            urls = find_m3u8(decoded)
    if not urls:
        raise RuntimeError("Vinovo: no m3u8 found")
    return {"url": urls[0], "type": "m3u8", "extra": urls}


# ── Generic fallback ──────────────────────────────────────────────────────────
def extract_generic(url: str) -> dict:
    try:
        from curl_cffi import requests as cf
        r = cf.get(url, impersonate="chrome", timeout=25,
                   headers={"Referer": urlparse(url).scheme + "://" + urlparse(url).netloc + "/"})
        html = r.text
    except Exception:
        r = _session().get(url, timeout=20)
        html = r.text
    html = html.replace("\\/", "/")
    packed = re.search(r"(eval\(function\(p,a,c,k,e,d\)\{.*?\.split\('\|'\)[^)]*\)\))", html, re.DOTALL)
    text = html
    if packed:
        text = html + "\n" + unpack_packer(packed.group(1))
    m3us = find_m3u8(text)
    mp4s = find_mp4(text)
    combined = m3us + [u for u in mp4s if u not in m3us]
    if combined:
        return {"url": combined[0], "type": "m3u8" if combined[0] in m3us else "mp4",
                "extra": combined}
    raise RuntimeError("Generic: no stream URL found")


# ─────────────────────────────────────────────────────────────────────────────
# Host map & dispatch
# ─────────────────────────────────────────────────────────────────────────────
HOST_MAP = {
    "mixdrop":    ["mixdrop"],
    "vidmoly":    ["vidmoly"],
    "voe":        ["voe.sx", "kellywhatcould", "jilliandescribecompany"],
    "streamwish": ["streamwish", "playnixes"],
    "streamta":   ["streamta.site"],
    "streamruby": ["streamruby.com"],
    "vids_st":    ["vids.st"],
    "savefiles":  ["savefiles.com"],
    "bigshare":   ["bigshare.io"],
    "dood":       ["dood.", "doods.", "ds2play", "ds2video", "d000d", "d-s.io",
                   "vidply", "playmogo"],
    "luluvdoo":   ["luluvdoo.com"],
    "filenoons":  ["filenoons", "earnvideo", "filelions", "vdhide", "callistanise",
                   "vidnest", "bysejikuar", "vidara"],
    "vidoza":     ["vidoza", "videzz"],
    "upzur":      ["upzur.com"],
    "vinovo":     ["vinovo.to"],
    "streamplay": ["streamplay.to"],
    "streamtape": ["streamtape"],
}

EXTRACTOR_MAP = {
    "mixdrop":    extract_mixdrop,
    "vidmoly":    extract_vidmoly,
    "voe":        extract_voe,
    "streamwish": extract_streamwish,
    "streamta":   extract_streamta,
    "streamruby": extract_streamruby,
    "vids_st":    extract_vids_st,
    "savefiles":  extract_savefiles,
    "bigshare":   extract_bigshare,
    "dood":       extract_dood,
    "luluvdoo":   extract_luluvdoo,
    "filenoons":  extract_filenoons,
    "vidoza":     extract_vidoza,
    "upzur":      extract_upzur,
    "vinovo":     extract_vinovo,
    "streamplay": extract_filenoons,
    "generic":    extract_generic,
}


def detect_host(url: str) -> str:
    host = urlparse(url).netloc.lower().lstrip("www.")
    for family, patterns in HOST_MAP.items():
        for p in patterns:
            if p in host:
                return family
    return "generic"


def extract_stream(url: str) -> dict:
    host = detect_host(url)
    fn = EXTRACTOR_MAP.get(host, extract_generic)
    result = fn(url)
    result["host"] = host
    result["input_url"] = url
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline JSON helpers
# ─────────────────────────────────────────────────────────────────────────────
def fetch_pipeline(url: str) -> list:
    print(f"[*] Fetching pipeline JSON from {url} ...")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    print(f"[*] Loaded {len(data)} entries from pipeline JSON.")
    return data


def get_embed_urls(entry: dict) -> list:
    """Extract all host/url pairs from a pipeline entry."""
    urls = []
    i = 1
    while True:
        url_key = f"url-{i}"
        host_key = f"host-{i}"
        if url_key not in entry:
            break
        urls.append({
            "host_label": entry.get(host_key, "unknown"),
            "embed_url": entry[url_key],
        })
        i += 1
    return urls


def find_entries_by_tmdb(pipeline: list, tmdb_ids: list) -> list:
    tmdb_set = set(int(t) for t in tmdb_ids)
    found = []
    for entry in pipeline:
        if entry.get("tmdb_id") in tmdb_set:
            found.append(entry)
    return found


# ─────────────────────────────────────────────────────────────────────────────
# Extraction worker
# ─────────────────────────────────────────────────────────────────────────────
def process_embed(item: dict) -> dict:
    """Extract a single embed URL; returns result dict."""
    embed_url = item["embed_url"]
    host_label = item["host_label"]
    try:
        result = extract_stream(embed_url)
        return {
            "status": "ok",
            "host_label": host_label,
            "embed_url": embed_url,
            "stream_url": result.get("url"),
            "stream_type": result.get("type"),
            "headers": result.get("headers"),
        }
    except Exception as e:
        return {
            "status": "error",
            "host_label": host_label,
            "embed_url": embed_url,
            "error": str(e),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Output formatting
# ─────────────────────────────────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"

def print_separator(char="─", width=72):
    print(DIM + char * width + RESET)

def print_movie_header(entry: dict):
    print()
    print_separator("═")
    print(f"{BOLD}{CYAN}  {entry['title']}{RESET}  "
          f"{DIM}TMDB:{entry['tmdb_id']}  IMDB:{entry.get('imdb_id','N/A')}{RESET}")
    print_separator("═")

def print_result(idx: int, res: dict):
    if res["status"] == "ok":
        stype = f"[{res['stream_type'].upper()}]" if res.get("stream_type") else ""
        print(f"  {GREEN}✔{RESET}  {BOLD}#{idx}{RESET}  "
              f"{YELLOW}{res['host_label']}{RESET}  {DIM}{stype}{RESET}")
        print(f"       {res['stream_url']}")
        if res.get("headers"):
            for k, v in res["headers"].items():
                print(f"       {DIM}Header {k}: {v}{RESET}")
    else:
        print(f"  {RED}✘{RESET}  {BOLD}#{idx}{RESET}  "
              f"{YELLOW}{res['host_label']}{RESET}")
        print(f"       {DIM}Embed:  {res['embed_url']}{RESET}")
        print(f"       {RED}Error:  {res['error']}{RESET}")

def save_results(all_results: list, output_file: str):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    print(f"\n{GREEN}[*] Results saved to: {output_file}{RESET}")

def save_urls_txt(all_results: list, output_file: str):
    """Save only successful stream URLs, one per line."""
    lines = []
    for movie in all_results:
        lines.append(f"# {movie['title']} (TMDB:{movie['tmdb_id']})")
        for r in movie["results"]:
            if r["status"] == "ok" and r.get("stream_url"):
                lines.append(r["stream_url"])
        lines.append("")
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"{GREEN}[*] Plain URLs saved to: {output_file}{RESET}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print(f"{BOLD}{CYAN}")
    print("╔══════════════════════════════════════════════════╗")
    print("║          STREAM URL EXTRACTOR — CLI              ║")
    print("╚══════════════════════════════════════════════════╝")
    print(RESET)

    # ── Get TMDB IDs from user ────────────────────────────────────────────────
    while True:
        raw = input(f"{BOLD}Enter TMDB ID(s) separated by space: {RESET}").strip()
        if not raw:
            print(f"{YELLOW}[!] Please enter at least one TMDB ID.{RESET}")
            continue
        try:
            tmdb_ids = [int(x) for x in raw.split()]
            break
        except ValueError:
            print(f"{RED}[!] Invalid input. Enter numeric IDs only, e.g: 218 45450 1007757{RESET}")

    # ── Optional save outputs ─────────────────────────────────────────────────
    print()
    save_json = input(f"{DIM}Save full JSON results? Enter filename or leave blank to skip: {RESET}").strip()
    save_txt  = input(f"{DIM}Save plain URLs to text file? Enter filename or leave blank to skip: {RESET}").strip()
    print()

    # ── Fetch pipeline JSON ───────────────────────────────────────────────────
    try:
        pipeline = fetch_pipeline(PIPELINE_JSON_URL)
    except Exception as e:
        print(f"{RED}[!] Failed to fetch pipeline JSON: {e}{RESET}")
        sys.exit(1)

    # ── Match TMDB IDs ────────────────────────────────────────────────────────
    entries = find_entries_by_tmdb(pipeline, tmdb_ids)
    if not entries:
        print(f"{RED}[!] No entries found for TMDB IDs: {tmdb_ids}{RESET}")
        sys.exit(1)

    found_ids = {e["tmdb_id"] for e in entries}
    missing = [t for t in tmdb_ids if t not in found_ids]
    if missing:
        print(f"{YELLOW}[!] TMDB IDs not found in pipeline: {missing}{RESET}")

    print(f"{GREEN}[*] Found {len(entries)} movie(s) matching your TMDB IDs.{RESET}")

    all_results = []

    for entry in entries:
        print_movie_header(entry)
        embed_items = get_embed_urls(entry)
        total = len(embed_items)
        print(f"  {DIM}Found {total} embed URL(s). Extracting with {MAX_WORKERS} workers...{RESET}\n")

        results = [None] * total

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            future_to_idx = {
                pool.submit(process_embed, item): i
                for i, item in enumerate(embed_items)
            }
            completed = 0
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                res = future.result()
                results[idx] = res
                completed += 1
                status_icon = GREEN + "✔" + RESET if res["status"] == "ok" else RED + "✘" + RESET
                print(f"  {status_icon} [{completed}/{total}] {res['host_label']}", flush=True)

        print()
        ok_count = 0
        for i, res in enumerate(results, 1):
            print_result(i, res)
            if res["status"] == "ok":
                ok_count += 1

        print_separator()
        print(f"  {GREEN}{ok_count}{RESET}/{total} extracted successfully for "
              f"{BOLD}{entry['title']}{RESET}")

        all_results.append({
            "tmdb_id": entry["tmdb_id"],
            "imdb_id": entry.get("imdb_id"),
            "title": entry["title"],
            "extracted_at": entry.get("extracted_at"),
            "results": results,
        })

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print_separator("═")
    total_ok = sum(
        sum(1 for r in m["results"] if r["status"] == "ok")
        for m in all_results
    )
    total_all = sum(len(m["results"]) for m in all_results)
    print(f"\n{BOLD}  SUMMARY:{RESET}  {GREEN}{total_ok}{RESET}/{total_all} total streams extracted "
          f"across {len(all_results)} movie(s).")

    # ── Save outputs ──────────────────────────────────────────────────────────
    if save_json:
        save_results(all_results, save_json)
    if save_txt:
        save_urls_txt(all_results, save_txt)

    print()


if __name__ == "__main__":
    main()