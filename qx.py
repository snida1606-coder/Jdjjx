import os
from flask import Flask, jsonify
import socket, ssl, time
from urllib.request import Request, urlopen

app = Flask(__name__)

DOMAINS = [
    "market-qx.trade",
    "ws2.market-qx.trade",
    "ws.market-qx.trade",
    "qxbroker.com",
    "quotex.io",
]

def check(domain):
    results = {}
    # DNS resolve
    try:
        t0 = time.time()
        ip = socket.getaddrinfo(domain, 443)[0][4][0]
        results["dns"] = f"{ip} ({time.time()-t0:.1f}s)"
    except Exception as e:
        results["dns"] = f"FAIL: {e}"
        return results
    # HTTPS connect
    for host in [domain, f"www.{domain}"]:
        try:
            t0 = time.time()
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            sock = socket.create_connection((ip, 443), timeout=5)
            ssock = ctx.wrap_socket(sock, server_hostname=host)
            ssock.close()
            results[f"https_{host}"] = f"OK ({time.time()-t0:.1f}s)"
        except Exception as e:
            results[f"https_{host}"] = f"FAIL: {str(e)[:50]}"
    # HTTP GET
    try:
        t0 = time.time()
        req = Request(f"https://{domain}", headers={"User-Agent": "Mozilla/5.0"}, method="HEAD")
        r = urlopen(req, timeout=5)
        results["http_status"] = f"{r.status} ({time.time()-t0:.1f}s)"
    except Exception as e:
        results["http_status"] = f"FAIL: {str(e)[:50]}"
    return results

@app.route('/')
def index():
    output = {}
    for d in DOMAINS:
        output[d] = check(d)
    return jsonify(output)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
