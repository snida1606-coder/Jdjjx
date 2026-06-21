import json, os, httpx
from flask import Flask, jsonify
try:
    from github_config import GITHUB_USER, GITHUB_REPO
except:
    GITHUB_USER = GITHUB_REPO = ""

app = Flask(__name__)
app.json.sort_keys = False
RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/main"

@app.route('/')
def home():
    return jsonify({"status": "ok", "data_source": "GitHub",
                     "repo": f"{GITHUB_USER}/{GITHUB_REPO}" if GITHUB_USER else "not configured",
                     "usage": "/PAIR_NAME (e.g. /EURUSD_otc)"})

@app.route('/<pair>')
def get_data(pair):
    pair = pair.replace(".json", "")
    url = f"{RAW_URL}/{pair}.json"
    try:
        r = httpx.get(url, timeout=15)
        if r.status_code == 404:
            return jsonify({"error": f"File not found: {pair}.json on GitHub"}), 404
        if r.status_code != 200:
            return jsonify({"error": f"GitHub returned {r.status_code}", "url": url}), 502
        data = r.json()
        return jsonify(data)
    except httpx.ConnectError:
        return jsonify({"error": "Cannot reach GitHub"}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    print(f"Server ready! http://localhost:5000", flush=True)
    print(f"Data source: {RAW_URL}", flush=True)
    app.run(host="0.0.0.0", port=5000, debug=False)
