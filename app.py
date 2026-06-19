import asyncio
import json
import threading
import time
import ssl
import re
import os
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify
from websockets.exceptions import ConnectionClosed
import cloudscraper

# ------------------------------------------------------------
# Import credentials from login.py
# ------------------------------------------------------------
from login import EMAIL, PASSWORD

# ------------------------------------------------------------
# Flask app and constants
# ------------------------------------------------------------
app = Flask(__name__)
app.json.sort_keys = False
TZ = timezone(timedelta(hours=5))

WS_URL = "wss://ws2.quotex.io/socket.io/?EIO=3&transport=websocket"
BASE_DOMAIN = "https://quotex.io"
WS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin": BASE_DOMAIN,
    "Referer": f"{BASE_DOMAIN}/pt/trade",
}
TF_NAMES = {60: "1m", 300: "5m", 900: "15m", 1800: "30m", 3600: "1h"}

_pairs = []
_ws = None
_ssid = None
_cookies = None
_bg_task = None

# ------------------------------------------------------------
# Helper
# ------------------------------------------------------------
def ts_str(ts):
    return datetime.fromtimestamp(ts, tz=TZ).strftime("%Y-%m-%d %H:%M:%S")

# ------------------------------------------------------------
# Smart Login with aggressive cloudscraper settings
# ------------------------------------------------------------
def login():
    print("🔐 Attempting login with enhanced fingerprint...")

    # 1. Create a cloudscraper with the most robust options
    try:
        # Try Node.js interpreter (if available on Render)
        s = cloudscraper.create_scraper(
            interpreter='nodejs',
            delay=15,
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True,
                'mobile': False
            }
        )
        print("  Using Node.js interpreter")
    except Exception:
        # Fallback to js2py (pure Python, slower but works)
        s = cloudscraper.create_scraper(
            interpreter='js2py',
            delay=15,
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        print("  Using js2py interpreter (fallback)")

    # 2. Set extremely realistic headers
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "Sec-Ch-Ua": '"Not/A)Brand";v="99", "Google Chrome";v="126", "Chromium";v="126"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "DNT": "1",
    })

    # 3. Retry with backoff
    for attempt in range(1, 8):  # 7 attempts
        wait = 5 * attempt  # 5,10,15,... seconds
        print(f"  Attempt {attempt}/7 (wait {wait}s before next if fail)...")

        try:
            # ---- STRATEGY 1: API Login (fast) ----
            print("  🚀 Trying API login...")
            api_resp = s.post(
                f"{BASE_DOMAIN}/api/v3/auth/sign-in",
                json={"email": EMAIL, "password": PASSWORD},
                timeout=30,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/plain, */*",
                    "X-Requested-With": "XMLHttpRequest",
                }
            )
            if api_resp.status_code == 200:
                data = api_resp.json()
                token = data.get("token")
                if token:
                    cookies = "; ".join([f"{k}={v}" for k, v in s.cookies.items()])
                    print("  ✅ API Login SUCCESS")
                    return token, cookies
                else:
                    print("  API responded but no token")
            else:
                print(f"  API status {api_resp.status_code}")

            # ---- STRATEGY 2: HTML Form Login ----
            print("  🌐 Trying HTML form login...")
            # Visit trade page to get CSRF token
            r1 = s.get(f"{BASE_DOMAIN}/pt/trade", timeout=30)
            if r1.status_code != 200:
                print(f"  ❌ Page blocked (status {r1.status_code})")
                time.sleep(wait)
                continue

            # Extract CSRF token from HTML
            m1 = re.search(r'name="_token"\s+value="([^"]+)"', r1.text)
            tok = m1.group(1) if m1 else None
            if not tok:
                # Try to find it in JSON inside a script
                script_re = re.search(r'window\.settings\s*=\s*({.*?});', r1.text, re.DOTALL)
                if script_re:
                    try:
                        data = json.loads(script_re.group(1))
                        if data.get('token'):
                            cookies = "; ".join([f"{k}={v}" for k, v in s.cookies.items()])
                            print("  ✅ Token found in window.settings (no form login needed)")
                            return data['token'], cookies
                    except:
                        pass
                print("  ❌ No CSRF token found")
                time.sleep(wait)
                continue

            time.sleep(2)
            r4 = s.post(
                f"{BASE_DOMAIN}/pt/sign-in/",
                data={"_token": tok, "email": EMAIL, "password": PASSWORD, "remember": 1},
                timeout=30
            )

            # Search for token in response
            for sc in re.findall(r'<script[^>]*>(.*?)</script>', r4.text, re.DOTALL):
                if "window.settings" in sc:
                    start = sc.find("{")
                    end = sc.rfind("}") + 1
                    d = json.loads(sc[start:end])
                    if d.get("token"):
                        cookies = "; ".join([f"{k}={v}" for k, v in s.cookies.items()])
                        print("  ✅ HTML Login SUCCESS")
                        return d["token"], cookies

            print("  ❌ Login failed (no token in response)")
            time.sleep(wait)

        except Exception as e:
            print(f"  ❌ Exception: {e}")
            time.sleep(wait)

    print("❌ All login attempts failed.")
    return None, None

# ------------------------------------------------------------
# WebSocket connection (unchanged)
# ------------------------------------------------------------
async def connect_ws(ssid, cookies=""):
    import websockets
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    hdrs = dict(WS_HEADERS)
    if cookies:
        hdrs["Cookie"] = cookies
    ws = await websockets.connect(WS_URL, additional_headers=hdrs, ssl=ssl_ctx,
                                  ping_interval=10, ping_timeout=20, max_size=2**23)
    for _ in range(2):
        await asyncio.wait_for(ws.recv(), timeout=3)
    await ws.send(f'42["authorization",{json.dumps({"session":ssid,"isDemo":1,"tournamentId":0})}]')
    return ws

async def bg_keepalive(ws):
    while True:
        await asyncio.sleep(5)
        try:
            await ws.send("2")
            _ = await asyncio.wait_for(ws.recv(), timeout=3)
        except Exception:
            break

# ------------------------------------------------------------
# Initialization (unchanged)
# ------------------------------------------------------------
async def init():
    global _ws, _ssid, _cookies, _pairs, _bg_task
    try:
        print("⏳ Logging in...")
        _ssid, _cookies = login()
        if not _ssid:
            print("❌ Login failed! WebSocket will not be connected.")
            return

        print("🌐 Connecting WebSocket...")
        _ws = await connect_ws(_ssid, _cookies)
        _bg_task = asyncio.create_task(bg_keepalive(_ws))

        print("📡 Requesting instruments...")
        await _ws.send('42["instruments/update",{"asset":"EURUSD_otc","period":60}]')
        await _ws.send('42["indicator/list"]')

        pending = ""
        instruments = None
        T0 = time.time()
        while time.time() - T0 < 15:
            try:
                raw = await asyncio.wait_for(_ws.recv(), timeout=2)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"  WS recv error: {e}")
                break

            if isinstance(raw, str):
                if raw == "2":
                    try:
                        await _ws.send("3")
                    except:
                        pass
                    continue

            msg = raw if isinstance(raw, str) else str(raw)
            if ("451-" in msg or "51-" in msg) and "_placeholder" in msg:
                pending = msg
                continue

            if isinstance(raw, bytes) and len(raw) > 10 and pending:
                try:
                    bdata = raw[1:] if raw[0:1] == b'\x04' else raw
                    dj = json.loads(bdata.decode(errors="replace"))
                    if isinstance(dj, list) and len(dj) > 0 and isinstance(dj[0], list) and len(dj[0]) >= 10:
                        instruments = dj
                except Exception as e:
                    print(f"  Parse error: {e}")
                pending = ""
                if instruments:
                    break

        if instruments:
            for item in instruments:
                if isinstance(item, list) and len(item) >= 2:
                    sid = item[0]
                    name = str(item[1])
                    display = str(item[2]) if len(item) > 2 else name
                    payout = item[5] if len(item) > 5 and isinstance(item[5], (int, float)) else 0
                    max_payout = item[19] if len(item) > 19 and isinstance(item[19], (int, float)) else 0
                    _pairs.append((sid, name, display, payout, max_payout))
            print(f"✅ Instruments loaded: {len(_pairs)} pairs")
        else:
            print("⚠️ No instruments received – check WS communication.")
    except Exception as e:
        print(f"🔥 init() crashed: {e}")
        import traceback
        traceback.print_exc()

# ------------------------------------------------------------
# fetch_candles (unchanged, keep from previous version)
# ------------------------------------------------------------
async def fetch_candles(pair_name, tf_sec=60):
    global _ws, _pairs
    if _ws is None:
        return {"error": "WebSocket not connected"}

    sid = None
    display = ""
    payout = 0
    max_payout = 0
    for p in _pairs:
        if p[1] == pair_name:
            sid = p[0]
            display = p[2]
            payout = p[3]
            max_payout = p[4]
            break
    if sid is None:
        return None

    for attempt in range(1, 21):
        try:
            await _ws.send("2")
            break
        except ConnectionClosed:
            if attempt >= 20:
                return None
            print(f"🔄 Reconnecting WS {attempt}/20...")
            _bg_task.cancel()
            try:
                _ws = await connect_ws(_ssid, _cookies)
                _bg_task = asyncio.create_task(bg_keepalive(_ws))
            except Exception as e:
                print(f"  Reconnect error: {e}")
                return None

    await _ws.send(f'42["instruments/update",{json.dumps({"asset":pair_name,"period":tf_sec})}]')
    await _ws.send('42["indicator/list"]')
    await _ws.send(f'42["chart_notification/get",{json.dumps({"asset":pair_name,"version":"1.0.0"})}]')
    now = int(time.time())
    await _ws.send(f'42["history/load/line",{json.dumps({"id":sid,"index":now,"time":now-3600,"offset":5000})}]')

    ticks = {}
    candles = []
    pending = ""
    got = False
    T0 = time.time()
    while time.time() - T0 < 15:
        try:
            raw = await asyncio.wait_for(_ws.recv(), timeout=0.5)
        except asyncio.TimeoutError:
            if got:
                break
            continue
        except Exception:
            break

        if isinstance(raw, str):
            if raw == "2":
                try:
                    await _ws.send("3")
                except:
                    pass
                continue
            if raw.strip() == "41":
                break

        msg = raw if isinstance(raw, str) else str(raw)
        if ("451-" in msg or "51-" in msg) and "_placeholder" in msg:
            pending = msg
            ev = msg.split('["')[1].split('"')[0] if '["' in msg else ""
            if "history" in ev:
                got = True
            continue

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
                    if fresh:
                        _pairs = fresh
                        for p in _pairs:
                            if p[1] == pair_name:
                                payout = p[3]
                                max_payout = p[4]
                                break
                    pending = ""
                    continue

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
                                o = float(t[1])
                                c = float(t[2])
                                candles.append({
                                    "t": int(t[0]),
                                    "o": o,
                                    "c": c,
                                    "h": float(t[3]),
                                    "l": float(t[4]),
                                    "v": float(t[5]) if len(t) > 5 else 0,
                                    "d": 1 if c > o else 2 if c < o else 3
                                })
            except Exception:
                pass
            pending = ""

    candles.sort(key=lambda x: x["t"])
    if candles:
        last = candles[-1]["t"]
        cutoff = last + tf_sec
        buckets = {}
        for ts, (price, _) in sorted(ticks.items()):
            if ts < cutoff:
                continue
            bk = (ts // tf_sec) * tf_sec
            if bk not in buckets:
                buckets[bk] = {"o": price, "h": price, "l": price, "c": price}
            else:
                b = buckets[bk]
                b["h"] = max(b["h"], price)
                b["l"] = min(b["l"], price)
                b["c"] = price
        for bk, v in sorted(buckets.items()):
            candles.append({
                "t": bk,
                "o": v["o"],
                "c": v["c"],
                "h": v["h"],
                "l": v["l"],
                "v": 0,
                "d": 1 if v["c"] > v["o"] else 2 if v["c"] < v["o"] else 3,
                "running": True
            })
        candles.sort(key=lambda x: x["t"])

    return {
        "pair": pair_name,
        "display": display,
        "tf": tf_sec,
        "candles": candles,
        "ticks": len(ticks),
        "payout": payout,
        "max_payout": max_payout
    }

# ------------------------------------------------------------
# Flask Routes (unchanged)
# ------------------------------------------------------------
@app.route('/')
def home():
    out = {
        "owner": "@BINARYSUPPORT",
        "owner_name": "GHULAM MUJTABA",
        "status": "ok",
        "total": len(_pairs),
        "pairs": []
    }
    for p in _pairs:
        out["pairs"].append({
            "id": p[0],
            "name": p[1],
            "display": p[2],
            "payout": p[3],
            "max_payout": p[4]
        })
    return jsonify(out)

@app.route('/<pair>')
def get_data(pair):
    return get_data_tf(pair, 60)

@app.route('/<pair>/<int:tf>')
def get_data_tf(pair, tf):
    if _ws is None:
        return jsonify({"error": "WebSocket not connected"}), 503

    future = asyncio.run_coroutine_threadsafe(fetch_candles(pair, tf), loop)
    try:
        result = future.result(timeout=25)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if result is None:
        return jsonify({"error": "Pair not found"}), 404
    if isinstance(result, dict) and "error" in result:
        return jsonify({"error": result["error"]}), 503

    tf_name = TF_NAMES.get(tf, f"{tf}s")
    candles_fmt = []
    for c in result["candles"]:
        candles_fmt.append({
            "time": ts_str(c["t"]),
            "timestamp": c["t"],
            "open": c["o"],
            "high": c["h"],
            "low": c["l"],
            "close": c["c"],
            "volume": c["v"],
            "direction": "up" if c["d"] == 1 else "down" if c["d"] == 2 else "equal",
            "running": c.get("running", False),
        })

    return jsonify({
        "owner": "@BINARYSUPPORT",
        "owner_name": "GHULAM MUJTABA",
        "pair": result["pair"],
        "display": result["display"],
        "payout": result["payout"],
        "max_payout": result["max_payout"],
        "timeframe": tf_name,
        "total_candles": len(result["candles"]),
        "ticks_count": result["ticks"],
        "candles": candles_fmt,
    })

# ------------------------------------------------------------
# Start background loop and Flask
# ------------------------------------------------------------
loop = asyncio.new_event_loop()
init_done = threading.Event()

def start_loop():
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init())
    init_done.set()
    loop.run_forever()

t = threading.Thread(target=start_loop, daemon=True)
t.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"⏳ Waiting up to 120s for initialization...")
    init_done.wait(timeout=120)

    if _pairs:
        print(f"✅ Init completed – {len(_pairs)} pairs loaded. Starting server on port {port}.")
    else:
        print("⚠️  Init incomplete – server will start, but endpoints may return errors.")

    app.run(host="0.0.0.0", port=port, debug=False)
