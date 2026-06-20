import asyncio, json, ssl, re, os, sys, time, threading
py313 = "/data/data/com.termux/files/usr/lib/python3.13/site-packages"
if py313 not in sys.path: sys.path.insert(0, py313)
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify
from websockets.exceptions import ConnectionClosed
EMAIL = "doneyummy45@gmail.com"
PASSWORD = "Yummydone45@"
import httpx, certifi

app = Flask(__name__)
app.json.sort_keys = False
TZ = timezone(timedelta(hours=5))
DOMAIN = "https://qxbroker.com"
WS_URL = "wss://ws2.qxbroker.com/socket.io/?EIO=3&transport=websocket"
TF_NAMES = {60: "1m", 300: "5m", 900: "15m", 1800: "30m", 3600: "1h", 7200: "2h", 14400: "4h"}
_pairs = []
_ws = None
_ssid = None
_cookies = None
_bg_task = None

CIPHERS = "TLS_AES_128_GCM_SHA256:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_256_GCM_SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES256-SHA:ECDHE-ECDSA-AES128-SHA:ECDHE-RSA-AES128-SHA:ECDHE-RSA-AES256-SHA:DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA:AES128-SHA:AES256-SHA:DES-CBC3-SHA"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Connection": "keep-alive",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Referer": DOMAIN,
    "Upgrade-Insecure-Requests": "1",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Linux"',
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Dnt": "1",
}

WS_HEADERS = {
    "User-Agent": HEADERS["User-Agent"],
    "Origin": DOMAIN,
    "Referer": f"{DOMAIN}/pt/trade",
    "Accept-Language": "en-US,en;q=0.5",
}

def make_ssl():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(certifi.where())
    ctx.set_ciphers(CIPHERS)
    ctx.set_ecdh_curve("prime256v1")
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.maximum_version = ssl.TLSVersion.TLSv1_3
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

def ts_str(ts):
    return datetime.fromtimestamp(ts, tz=TZ).strftime("%Y-%m-%d %H:%M:%S")

def login():
    global _ssid, _cookies
    ssl_ctx = make_ssl()
    for attempt in range(1, 21):
        print(f"Login {attempt}/20...")
        try:
            with httpx.Client(verify=ssl_ctx, timeout=30, follow_redirects=True) as c:
                c.headers.update(HEADERS)
                r1 = c.get(f"{DOMAIN}/pt")
                if r1.status_code != 200:
                    print(f"  GET page: {r1.status_code}")
                    time.sleep(3); continue
                r2 = c.get(f"{DOMAIN}/pt/sign-in/modal/")
                if r2.status_code != 200:
                    print(f"  GET modal: {r2.status_code}")
                    time.sleep(3); continue
                m = re.search(r'name="_token"\s+value="([^"]+)"', r2.text)
                tok = m.group(1) if m else None
                if not tok: print(f"  No CSRF token"); time.sleep(3); continue
                time.sleep(2)
                r4 = c.post(f"{DOMAIN}/pt/sign-in/", data={"_token": tok, "email": EMAIL, "password": PASSWORD, "remember": 1})
                for sc in re.findall(r'<script[^>]*>(.*?)</script>', r4.text, re.DOTALL):
                    if "window.settings" in sc:
                        start = sc.find("{"); end = sc.rfind("}") + 1
                        d = json.loads(sc[start:end])
                        if d.get("token"):
                            _cookies = "; ".join([f"{k}={v}" for k, v in c.cookies.items()])
                            _ssid = d["token"]
                            print(f"  OK"); return
                print(f"  Failed")
        except Exception as e:
            print(f"  Error: {e}")
        time.sleep(3)
    print("Login failed after 20 attempts")

async def connect_ws():
    global _ws, _bg_task
    import websockets
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    hdrs = dict(WS_HEADERS)
    if _cookies:
        hdrs["Cookie"] = _cookies
    _ws = await websockets.connect(WS_URL, additional_headers=hdrs, ssl=ssl_ctx,
                                    ping_interval=10, ping_timeout=20, max_size=2**23)
    for _ in range(2):
        await asyncio.wait_for(_ws.recv(), timeout=3)
    await _ws.send(f'42["authorization",{json.dumps({"session":_ssid,"isDemo":1,"tournamentId":0})}]')
    _bg_task = asyncio.create_task(bg_keepalive(_ws))

async def bg_keepalive(ws):
    while True:
        await asyncio.sleep(5)
        try:
            await ws.send("2")
            _ = await asyncio.wait_for(ws.recv(), timeout=3)
        except: break

async def init():
    global _pairs
    print("Logging in...")
    login()
    if not _ssid: print("Login failed!"); return
    print("Connecting WS...")
    await connect_ws()
    await _ws.send('42["instruments/update",{"asset":"EURUSD_otc","period":60}]')
    await _ws.send('42["indicator/list"]')
    pending = ""; instruments = None; T0 = time.time()
    while time.time() - T0 < 10:
        try:
            raw = await asyncio.wait_for(_ws.recv(), timeout=2)
        except: continue
        if isinstance(raw, str) and raw == "2":
            try: await _ws.send("3")
            except: pass; continue
        msg = raw if isinstance(raw, str) else str(raw)
        if ("451-" in msg or "51-" in msg) and "_placeholder" in msg: pending = msg; continue
        if isinstance(raw, bytes) and len(raw) > 10 and pending:
            try:
                bdata = raw[1:] if raw[0:1] == b'\x04' else raw
                dj = json.loads(bdata.decode(errors="replace"))
                if isinstance(dj, list) and len(dj) > 0 and isinstance(dj[0], list) and len(dj[0]) >= 10:
                    instruments = dj
            except: pass; pending = ""
        if instruments: break
    if instruments:
        for item in instruments:
            if isinstance(item, list) and len(item) >= 2:
                _pairs.append((item[0], str(item[1]), str(item[2]) if len(item) > 2 else str(item[1]),
                               item[5] if len(item) > 5 and isinstance(item[5], (int, float)) else 0,
                               item[19] if len(item) > 19 and isinstance(item[19], (int, float)) else 0))
        print(f"Pairs: {len(_pairs)}")
    else: print("No instruments!")

async def fetch_candles(pair_name, tf_sec=60):
    global _ws, _pairs
    sid = None; display = ""; payout = 0; max_pay = 0
    for p in _pairs:
        if p[1] == pair_name: sid = p[0]; display = p[2]; payout = p[3]; max_pay = p[4]; break
    if sid is None: return None
    for attempt in range(1, 21):
        try: await _ws.send("2"); break
        except ConnectionClosed:
            if attempt >= 20: return None
            print(f"Reconnect {attempt}/20..."); await connect_ws()
    await _ws.send(f'42["instruments/update",{json.dumps({"asset":pair_name,"period":tf_sec})}]')
    await _ws.send('42["indicator/list"]')
    await _ws.send(f'42["chart_notification/get",{json.dumps({"asset":pair_name,"version":"1.0.0"})}]')
    now = int(time.time())
    await _ws.send(f'42["history/load/line",{json.dumps({"id":sid,"index":now,"time":now-3600,"offset":5000})}]')
    ticks = {}; candles = []; pending = ""; got = False; T0 = time.time()
    while time.time() - T0 < 12:
        try: raw = await asyncio.wait_for(_ws.recv(), timeout=0.5)
        except asyncio.TimeoutError:
            if got: break; continue
        except: break
        if isinstance(raw, str):
            if raw == "2":
                try: await _ws.send("3")
                except: pass; continue
            if raw.strip() == "41": break
        msg = raw if isinstance(raw, str) else str(raw)
        if ("451-" in msg or "51-" in msg) and "_placeholder" in msg:
            pending = msg
            ev = msg.split('["')[1].split('"')[0] if '["' in msg else ""
            if "history" in ev: got = True; continue
        if isinstance(raw, bytes) and len(raw) > 10 and pending:
            try:
                bdata = raw[1:] if raw[0:1] == b'\x04' else raw
                dj = json.loads(bdata.decode(errors="replace"))
                if isinstance(dj, list) and len(dj) > 0 and isinstance(dj[0], list) and len(dj[0]) >= 10:
                    fresh = []
                    for item in dj:
                        if isinstance(item, list) and len(item) >= 2:
                            nm = str(item[1])
                            fresh.append((item[0], nm, str(item[2]) if len(item) > 2 else nm,
                                          item[5] if len(item) > 5 and isinstance(item[5], (int, float)) else 0,
                                          item[19] if len(item) > 19 and isinstance(item[19], (int, float)) else 0))
                    if fresh: _pairs = fresh
                    for p in _pairs:
                        if p[1] == pair_name: payout = p[3]; max_pay = p[4]; break
                    pending = ""; continue
                if isinstance(dj, dict):
                    for k in ["history", "data"]:
                        dl = dj.get(k, [])
                        if isinstance(dl, list) and len(dl) > 0:
                            for t in dl:
                                if isinstance(t, (list, tuple)) and len(t) >= 3:
                                    ticks[int(t[0])] = (float(t[1]), int(t[2]))
                    dl = dj.get("candles", [])
                    if isinstance(dl, list) and len(dl) > 0:
                        for t in dl:
                            if isinstance(t, (list, tuple)) and len(t) >= 5:
                                o, c = float(t[1]), float(t[2])
                                candles.append({"t": int(t[0]), "o": o, "c": c, "h": float(t[3]), "l": float(t[4]),
                                                 "v": float(t[5]) if len(t) > 5 else 0,
                                                 "d": 1 if c > o else 2 if c < o else 3})
            except: pass; pending = ""
    candles.sort(key=lambda x: x["t"])
    if candles:
        last = candles[-1]["t"]; cutoff = last + tf_sec
        buckets = {}
        for ts, (price, _) in sorted(ticks.items()):
            if ts < cutoff: continue
            bk = (ts // tf_sec) * tf_sec
            if bk not in buckets: buckets[bk] = {"o": price, "h": price, "l": price, "c": price}
            else:
                b = buckets[bk]; b["h"] = max(b["h"], price); b["l"] = min(b["l"], price); b["c"] = price
        for bk, v in sorted(buckets.items()):
            candles.append({"t": bk, "o": v["o"], "c": v["c"], "h": v["h"], "l": v["l"], "v": 0,
                             "d": 1 if v["c"] > v["o"] else 2 if v["c"] < v["o"] else 3, "running": True})
        candles.sort(key=lambda x: x["t"])
    return {"pair": pair_name, "display": display, "tf": tf_sec, "candles": candles, "ticks": len(ticks), "payout": payout, "max_payout": max_pay}

@app.route('/')
def home():
    out = {"owner": "@BINARYSUPPORT", "owner_name": "GHULAM MUJTABA", "status": "ok", "total": len(_pairs), "pairs": []}
    for p in _pairs:
        out["pairs"].append({"id": p[0], "name": p[1], "display": p[2], "payout": p[3], "max_payout": p[4]})
    return jsonify(out)

@app.route('/<pair>')
def get_data(pair): return get_data_tf(pair, 60)

@app.route('/<pair>/<int:tf>')
def get_data_tf(pair, tf):
    future = asyncio.run_coroutine_threadsafe(fetch_candles(pair, tf), loop)
    try: result = future.result(timeout=20)
    except Exception as e: return jsonify({"error": str(e)}), 500
    if result is None: return jsonify({"error": "Not found"}), 404
    tf_name = TF_NAMES.get(tf, f"{tf}s")
    candles_fmt = []
    for c in result["candles"]:
        candles_fmt.append({"time": ts_str(c["t"]), "timestamp": c["t"],
                             "open": c["o"], "high": c["h"], "low": c["l"], "close": c["c"],
                             "volume": c["v"],
                             "direction": "up" if c["d"] == 1 else "down" if c["d"] == 2 else "equal",
                             "running": c.get("running", False)})
    return jsonify({"owner": "@BINARYSUPPORT", "owner_name": "GHULAM MUJTABA",
                     "pair": result["pair"], "display": result["display"],
                     "payout": result["payout"], "max_payout": result["max_payout"],
                     "timeframe": tf_name, "total_candles": len(result["candles"]),
                     "ticks_count": result["ticks"], "candles": candles_fmt})

loop = asyncio.new_event_loop()
init_done = threading.Event()

def start_loop():
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init())
    init_done.set(); loop.run_forever()

t = threading.Thread(target=start_loop, daemon=True); t.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    if not init_done.wait(timeout=120): print("Init timed out")
    elif _pairs:
        print(f"\nServer ready! http://localhost:{port}")
        app.run(host="0.0.0.0", port=port, debug=False)
    else: print("Init failed")
