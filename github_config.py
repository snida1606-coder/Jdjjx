import os

GITHUB_USER = os.environ.get("GITHUB_USER", "ghulammujtabaquotex-cloud")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "Quotex-server")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_RAW = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/main"

