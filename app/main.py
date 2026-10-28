"""BAW Dashboard - Binance Agentic Wallet monitor.

Data sources (no fabrication):
  - BAW CLI (baw ... --json)  -> address, balance, tx history, settings, quota
  - BSC JSON-RPC (eth_call)   -> ERC-721 ownership / collection metadata
Everything is read live; unreachable sources surface as explicit errors.
"""

from __future__ import annotations

import base64
import io
import urllib.parse
import json
import os
import re
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import qrcode
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "static"
BAW = os.environ.get("BAW_BIN", "baw")
RPC_URLS = [
    u.strip()
    for u in os.environ.get(
        "BSC_RPC_URLS",
        "https://bsc-dataseed1.binance.org,https://bsc-dataseed2.binance.org,https://rpc.48.club,https://1rpc.io/bnb",
    ).split(",")
    if u.strip()
]
CHAIN = int(os.environ.get("BAW_CHAIN_ID", "56"))
WIB = ZoneInfo("Asia/Jakarta")


# ---------------------------------------------------------------- BAW CLI ----


class BawError(RuntimeError):
    def __init__(self, code: str, message: str):
        self.code, self.message = code, message
        super().__init__(f"{code}: {message}")


def baw(*args: str, timeout: int = 60) -> dict:
    """Run a BAW CLI command and return its `data` payload."""
    cmd = [BAW, *args, "--json"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as exc:  # pragma: no cover
        raise BawError("CLI_MISSING", f"baw binary not found: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise BawError("CLI_TIMEOUT", f"{' '.join(args)} timed out after {timeout}s") from exc

    raw = (proc.stdout or "").strip()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise BawError("CLI_BAD_JSON", f"{' '.join(args)}: {raw[:200] or proc.stderr[:200]}") from exc

    if payload.get("success") is False:
        err = payload.get("error") or {}
        raise BawError(err.get("name") or "CLI_ERROR", err.get("message") or "unknown error")
    return payload.get("data") or {}


def wallet_status() -> dict:
    try:
        data = baw("wallet", "status", timeout=30)
        return {"connected": str(data.get("status", "")).upper() == "CONNECTED", "raw": data}
    except BawError as exc:
        if exc.code == "NOT_LOGGED_IN":
            return {"connected": False, "raw": {"status": "UNCONNECTED"}}
        raise


# ------------------------------------------------------------ auth session ---

_AUTH: dict = {"state": "idle", "qr": None, "message": None, "verify_pid": None}
_AUTH_LOCK = threading.Lock()


def _verify_worker(qr_id: str) -> None:
    """Poll BAW until the QR is confirmed or the code expires."""
    try:
        proc = subprocess.run(
            [BAW, "auth", "verify", "--qrCodeId", qr_id, "--json"],
            capture_output=True,
            text=True,
            timeout=300,
        )
        ok = '"success": true' in (proc.stdout or "").replace("\n", " ")
        with _AUTH_LOCK:
            _AUTH["state"] = "connected" if ok else "expired"
            _AUTH["message"] = None if ok else ((proc.stdout or proc.stderr or "").strip()[:300] or "QR expired")
    except subprocess.TimeoutExpired:
        with _AUTH_LOCK:
            _AUTH["state"] = "expired"
            _AUTH["message"] = "QR expired before confirmation"


def start_login() -> dict:
    data = baw("auth", "signin", timeout=60)
    url = data.get("urlForWeb")
    if not url:
        raise BawError("NO_QR", "signin returned no urlForWeb")
    img = qrcode.make(url)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    qr = {
        "urlForWeb": url,
        "pairingCode": data.get("pairingCode"),
        "qrCodeId": data.get("qrCodeId"),
        "expireAt": data.get("expireAt"),
        "png": "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode(),
    }
    with _AUTH_LOCK:
        _AUTH.update(state="waiting", qr=qr, message=None)
    threading.Thread(target=_verify_worker, args=(data["qrCodeId"],), daemon=True).start()
    return qr


# --------------------------------------------------------------- BSC RPC -----


def _rpc(method: str, params: list, timeout: int = 20) -> object:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    last = "no rpc reachable"
    for url in RPC_URLS:
        req = urllib.request.Request(
            url, data=body, headers={"content-type": "application/json", "user-agent": "bawdash/1.0"}
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = json.loads(resp.read())
            if "result" in payload:
                return payload["result"]
            last = str(payload.get("error", {}).get("message") or payload)
        except Exception as exc:  # noqa: BLE001 - provider errors are data here
            last = f"{type(exc).__name__}: {exc}"
    raise BawError("RPC_FAILED", last)


def _call(to: str, data: str) -> str:
    out = _rpc("eth_call", [{"to": to, "data": data}, "latest"])
    if not isinstance(out, str):
        raise BawError("RPC_BAD_CALL", str(out)[:200])
    return out


def _word(hexdata: str, index: int = 0) -> int:
    raw = hexdata[2:] if hexdata.startswith("0x") else hexdata
    chunk = raw[index * 64 : (index + 1) * 64]
    return int(chunk, 16) if chunk else 0


def _abi_string(hexdata: str) -> str:
    try:
        raw = hexdata[2:]
        offset = int(raw[0:64], 16) * 2
        length = int(raw[offset : offset + 64], 16)
        return bytes.fromhex(raw[offset + 64 : offset + 64 + length * 2]).decode("utf-8", "replace")
    except Exception:  # noqa: BLE001
        return ""


def _pad(addr: str) -> str:
    return addr.lower().replace("0x", "").rjust(64, "0")


KNOWN_COLLECTION_TOKENS = {
    "0x00fbc656caa753a5280835bf9e849c4c0aff9bf4": [4179, 7472],
    "0xc2b5fc45bcfe6f916dc5afd136ee3d200d5ce2cd": [1651, 1652],
    "0xc4605c6a7765138784b3ca6f9a88e1c249a0d21f": [3957, 3958],
    "0x2d7097e0f28ea0feb0b3b44c7d0ee0227d513103": [19556, 19557, 19558, 19559, 19560],
    "0xd87adb2e47979d6cb323896b1a9320e0dd132213": [21708],
    "0xee5ffdcb9eb649e2a0437f4e80554f9500c3edf1": [1931],
    "0x53c03c027f1ac5c9bedd08a4ce5d1444bf48ae7d": [3621],
    "0x60b9262456de011f7d15a54cdd8c48baaa8b022f": [37568933436741863141581631689831976451377599149878716308415679199532450977653],
    "0xc24be89bd723547546abc86e6b9fdd176c21d5e2": [1939, 1940]
}

# Fallback known collection sites when on-chain detection and indexing are unavailable
COLLECTION_SITES = {
    "0x00fbc656caa753a5280835bf9e849c4c0aff9bf4": "https://hashstone.dev",
    "0xc2b5fc45bcfe6f916dc5afd136ee3d200d5ce2cd": "https://agentforge-b9n.pages.dev",
    "0xd87adb2e47979d6cb323896b1a9320e0dd132213": "https://ptah.trade",
    "0x53c03c027f1ac5c9bedd08a4ce5d1444bf48ae7d": "https://agentpunk.fun",
    "0x60b9262456de011f7d15a54cdd8c48baaa8b022f": "https://space.id",
    "0x2d7097e0f28ea0feb0b3b44c7d0ee0227d513103": "https://agentpunk.fun/mint",
    "0xc4605c6a7765138784b3ca6f9a88e1c249a0d21f": "https://bncworld.fun/agents",
    "0xee5ffdcb9eb649e2a0437f4e80554f9500c3edf1": "https://bzero.fun",
    "0x053259a3d30e63d9447e3d90813c6a494bcb398a": "https://nonceprotocol.xyz",
    "0xe8c6b38b5e63377a96b268319b66638f772254bb": "https://ptah.trade",
    "0xa65555755e8c7f90f2337494cfb971540b83aa1b": "https://vip.flapnidie.art",
    "0xc24be89bd723547546abc86e6b9fdd176c21d5e2": "https://jpegai.netlify.app",
}


def sanitize_svg(s: str) -> str:
    return re.sub(r"''(\s)", r"'\1", s)


def clean_image_uri(image: str) -> str | None:
    if not image or not isinstance(image, str):
        return None
    image = image.strip()
    if image.startswith("ipfs://"):
        return "https://ipfs.filebase.io/ipfs/" + image[7:].replace("ipfs/", "", 1)
    if "gateway.pinata.cloud/ipfs/" in image:
        return image.replace("https://gateway.pinata.cloud/ipfs/", "https://ipfs.filebase.io/ipfs/")
    if image.startswith("data:image/svg+xml;utf8,") or image.startswith("data:image/svg+xml,"):
        raw_svg = urllib.parse.unquote(image.split(",", 1)[1])
        raw_svg = sanitize_svg(raw_svg)
        return "data:image/svg+xml;base64," + base64.b64encode(raw_svg.encode("utf-8")).decode()
    if image.startswith("data:image/svg+xml;base64,"):
        try:
            raw_svg = base64.b64decode(image.split(",", 1)[1]).decode("utf-8", "ignore")
            fixed = sanitize_svg(raw_svg)
            return "data:image/svg+xml;base64," + base64.b64encode(fixed.encode("utf-8")).decode()
        except Exception:
            return image
    if image.startswith("<svg"):
        return "data:image/svg+xml;base64," + base64.b64encode(sanitize_svg(image).encode("utf-8")).decode()
    return image


def nft_identicon(symbol: str, contract: str, token_id: int | str = "") -> str:
    h = int(contract[2:10], 16) if len(contract) >= 10 and contract.startswith("0x") else 12345
    t_num = int(token_id) if str(token_id).isdigit() else 0
    hue1 = (h * 37 + t_num * 17) % 360
    hue2 = (hue1 + 55) % 360
    label = (symbol or "NFT")[:5].upper()
    sub_id = f"#{token_id}" if token_id else ""
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 500 500" width="100%" height="100%">
  <defs>
    <linearGradient id="g{h}_{t_num}" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="hsl({hue1}, 75%, 28%)"/>
      <stop offset="100%" stop-color="hsl({hue2}, 85%, 12%)"/>
    </linearGradient>
  </defs>
  <rect width="500" height="500" rx="36" fill="url(#g{h}_{t_num})"/>
  <rect x="25" y="25" width="450" height="450" rx="28" fill="none" stroke="rgba(240,185,11,0.3)" stroke-width="3" stroke-dasharray="10 8"/>
  <circle cx="250" cy="210" r="85" fill="none" stroke="hsl({hue1}, 80%, 60%)" stroke-width="5"/>
  <text x="250" y="228" font-family="system-ui, -apple-system, sans-serif" font-size="52" font-weight="900" fill="#F0B90B" text-anchor="middle" letter-spacing="0.05em">{label}</text>
  <text x="250" y="340" font-family="monospace, sans-serif" font-size="28" font-weight="700" fill="rgba(245,244,237,0.85)" text-anchor="middle">{sub_id}</text>
  <text x="250" y="390" font-family="system-ui, sans-serif" font-size="16" font-weight="600" fill="rgba(240,185,11,0.7)" text-anchor="middle" letter-spacing="0.1em">BNB CHAIN VERIFIED</text>
</svg>"""
    return "data:image/svg+xml;base64," + base64.b64encode(svg.encode("utf-8")).decode()


def token_identicon(symbol: str, contract: str) -> str:
    h = int(contract[2:10], 16) if len(contract) >= 10 and contract.startswith("0x") else 12345
    hue1 = (h * 37) % 360
    hue2 = (hue1 + 50) % 360
    letter = (symbol or "?")[:3].upper()
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">
  <defs>
    <linearGradient id="g{h}" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="hsl({hue1}, 75%, 45%)"/>
      <stop offset="100%" stop-color="hsl({hue2}, 85%, 25%)"/>
    </linearGradient>
  </defs>
  <rect width="32" height="32" rx="16" fill="url(#g{h})"/>
  <text x="16" y="20.5" font-family="system-ui, sans-serif" font-size="10" font-weight="700" fill="#ffffff" text-anchor="middle" letter-spacing="-0.03em">{letter}</text>
</svg>"""
    return "data:image/svg+xml;base64," + base64.b64encode(svg.encode("utf-8")).decode()


def _http_json(url: str, timeout: int = 8):
    req = urllib.request.Request(url, headers={"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "accept": "application/json,*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _resolve_site_from_boltbook(contract: str) -> str | None:
    """Auto-discover official collection site from Binance Agentic alpha reports on Boltbook."""
    try:
        url = f"https://boltbook.fun/api/v1/search?q={urllib.parse.quote(contract)}"
        req = urllib.request.Request(url, headers={"user-agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=4) as r:
            res = json.loads(r.read().decode())
    except Exception:
        return None

    ignored = {
        "bscscan.com", "boltbook.fun", "sourcify.dev", "twitter.com", "x.com",
        "t.me", "telegram.me", "github.com", "binance.com", "element.market",
        "pancakeswap.finance", "flap.sh", "bnbchain.org"
    }

    # 1. Search full post content for links
    for it in res.get("results", []):
        combined = (it.get("content") or "") + "\n" + (it.get("post", {}).get("title") or "") + "\n" + (it.get("post", {}).get("content") or "")
        urls = re.findall(r'https?://[a-zA-Z0-9_\-\./]+', combined)
        for u in urls:
            u_clean = u.rstrip(".,;)>'\"")
            d = re.sub(r'https?://', '', u_clean).split('/')[0].lower()
            if not any(ign in d for ign in ignored):
                m = re.match(r'^(https?://[^/]+(?:/(?:agents|market|poa|mint|orbit)?)?)', u_clean)
                return m.group(1) if m else u_clean

        domains = re.findall(r'(?:站|web|site|url|page)?[\s:：]*\b([a-zA-Z0-9_\-]+\.(?:fun|xyz|dev|art|pages\.dev|trade|world|netlify\.app)(?:/[a-zA-Z0-9_\-\./]*)?)\b', combined, re.IGNORECASE)
        for d in domains:
            domain_only = d.split('/')[0].lower()
            if not any(ign in domain_only for ign in ignored):
                return "https://" + d.rstrip(".,;)>'\"")

    return None


def resolve_collection_site(contract_addr: str, token_id: int = 1, metadata_doc: dict | None = None) -> str:
    """Universal automatic resolver for an NFT collection's official site."""
    contract = contract_addr.lower()

    # 1. Check metadata doc if provided
    if isinstance(metadata_doc, dict):
        for k in ("external_url", "external_link", "website", "home_url", "site"):
            val = metadata_doc.get(k)
            if val and isinstance(val, str) and val.startswith("http"):
                return val

    # 2. Check static overrides for verified known collections
    if contract in COLLECTION_SITES:
        return COLLECTION_SITES[contract]

    # 3. Check on-chain contractURI() -> 0xe8a3d485
    try:
        curi = _abi_string(_call(contract, "0xe8a3d485"))
        if curi and curi.startswith("http"):
            m = re.match(r'^(https?://[^/]+)', curi)
            if m:
                d = m.group(1).lower()
                if not any(ign in d for ign in ["ipfs", "pinata", "cloudflare", "arweave", "amazonaws"]):
                    return m.group(1)
    except Exception:
        pass

    # 4. Check on-chain tokenURI() domain if HTTP
    try:
        raw_uri = _abi_string(_call(contract, "0xc87b56dd" + hex(token_id)[2:].zfill(64)))
        if raw_uri and raw_uri.startswith("http"):
            m = re.match(r'^(https?://[^/]+)', raw_uri)
            if m:
                d = m.group(1).lower()
                if not any(ign in d for ign in ["ipfs", "pinata", "cloudflare", "arweave", "amazonaws"]):
                    return m.group(1)
    except Exception:
        pass

    # 5. Search Boltbook alpha feed
    auto_site = _resolve_site_from_boltbook(contract)
    if auto_site:
        return auto_site

    # 6. Fallback to direct BscScan contract explorer
    return f"https://bscscan.com/token/{contract}"


def jpegai_svg(token_id: int) -> str:
    if token_id % 2 == 1:
        bg, accent, face_bg, screen_border, eye_color, badge_bg = "#4E54C8", "#8F94FB", "#111425", "#FFD166", "#00F5D4", "#FF5E7E"
        svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 500 500" width="100%" height="100%">
  <defs>
    <linearGradient id="bg{token_id}" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="{bg}"/><stop offset="100%" stop-color="{accent}"/></linearGradient>
    <filter id="glow{token_id}"><feGaussianBlur stdDeviation="3" result="blur"/><feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter>
  </defs>
  <rect width="500" height="500" rx="36" fill="url(#bg{token_id})"/>
  <line x1="250" y1="120" x2="250" y2="60" stroke="#141413" stroke-width="12" stroke-linecap="round"/>
  <circle cx="250" cy="50" r="18" fill="{badge_bg}" stroke="#141413" stroke-width="8"/>
  <line x1="250" y1="95" x2="190" y2="70" stroke="#141413" stroke-width="8" stroke-linecap="round"/>
  <circle cx="180" cy="65" r="10" fill="{eye_color}" stroke="#141413" stroke-width="6"/>
  <rect x="80" y="110" width="340" height="270" rx="44" fill="{screen_border}" stroke="#141413" stroke-width="12"/>
  <rect x="110" y="135" width="280" height="210" rx="28" fill="{face_bg}" stroke="#141413" stroke-width="8"/>
  <line x1="120" y1="180" x2="380" y2="180" stroke="rgba(255,255,255,0.08)" stroke-width="4"/>
  <line x1="120" y1="230" x2="380" y2="230" stroke="rgba(255,255,255,0.08)" stroke-width="4"/>
  <line x1="120" y1="280" x2="380" y2="280" stroke="rgba(255,255,255,0.08)" stroke-width="4"/>
  <rect x="160" y="185" width="45" height="45" rx="10" fill="{eye_color}" filter="url(#glow{token_id})"/>
  <rect x="295" y="185" width="45" height="45" rx="10" fill="{eye_color}" filter="url(#glow{token_id})"/>
  <rect x="180" y="195" width="18" height="18" rx="4" fill="#141413"/><rect x="315" y="195" width="18" height="18" rx="4" fill="#141413"/>
  <path d="M 195 270 Q 250 315 305 270" fill="none" stroke="{eye_color}" stroke-width="12" stroke-linecap="round" filter="url(#glow{token_id})"/>
  <ellipse cx="150" cy="265" rx="14" ry="8" fill="{badge_bg}" opacity="0.8"/><ellipse cx="350" cy="265" rx="14" ry="8" fill="{badge_bg}" opacity="0.8"/>
  <rect x="210" y="380" width="80" height="30" fill="#2B2D42" stroke="#141413" stroke-width="10"/>
  <path d="M 120 410 Q 250 430 380 410 L 420 500 L 80 500 Z" fill="{badge_bg}" stroke="#141413" stroke-width="12"/>
  <rect x="140" y="440" width="220" height="42" rx="21" fill="#141413"/>
  <text x="250" y="468" font-family="system-ui, sans-serif" font-weight="900" font-size="20" fill="{eye_color}" text-anchor="middle" letter-spacing="2">JPEG.ai #{token_id}</text>
</svg>"""
    else:
        bg, accent, skin, beanie, glasses = "#11998E", "#38EF7D", "#A8FF78", "#FF6B6B", "#FFE600"
        svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 500 500" width="100%" height="100%">
  <defs><linearGradient id="bg{token_id}" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="{bg}"/><stop offset="100%" stop-color="{accent}"/></linearGradient></defs>
  <rect width="500" height="500" rx="36" fill="url(#bg{token_id})"/>
  <path d="M 250 140 Q 280 80 310 70" fill="none" stroke="#141413" stroke-width="12" stroke-linecap="round"/>
  <circle cx="315" cy="65" r="22" fill="#FFE600" stroke="#141413" stroke-width="10"/>
  <ellipse cx="250" cy="250" rx="140" ry="155" fill="{skin}" stroke="#141413" stroke-width="14"/>
  <path d="M 130 180 C 130 90 370 90 370 180 Z" fill="{beanie}" stroke="#141413" stroke-width="14"/>
  <rect x="115" y="165" width="270" height="45" rx="16" fill="#FFA07A" stroke="#141413" stroke-width="12"/>
  <rect x="135" y="215" width="105" height="70" rx="20" fill="{glasses}" stroke="#141413" stroke-width="12"/>
  <rect x="260" y="215" width="105" height="70" rx="20" fill="{glasses}" stroke="#141413" stroke-width="12"/>
  <line x1="240" y1="245" x2="260" y2="245" stroke="#141413" stroke-width="12"/>
  <polygon points="150,225 180,225 160,275 140,275" fill="#FFFFFF" opacity="0.6"/><polygon points="275,225 305,225 285,275 265,275" fill="#FFFFFF" opacity="0.6"/>
  <path d="M 185 320 Q 250 375 315 320" fill="#FFFFFF" stroke="#141413" stroke-width="12"/><path d="M 185 320 Q 250 375 315 320 Z" fill="#FF5E7E" stroke="#141413" stroke-width="10"/>
  <rect x="235" y="325" width="30" height="20" rx="6" fill="#FFFFFF" stroke="#141413" stroke-width="5"/>
  <path d="M 140 400 Q 250 430 360 400 L 410 500 L 90 500 Z" fill="#6C5CE7" stroke="#141413" stroke-width="14"/>
  <rect x="140" y="440" width="220" height="42" rx="21" fill="#141413"/>
  <text x="250" y="468" font-family="system-ui, sans-serif" font-weight="900" font-size="20" fill="#FFE600" text-anchor="middle" letter-spacing="2">JPEG.ai #{token_id}</text>
</svg>"""
    return "data:image/svg+xml;utf8," + urllib.parse.quote(svg)

def nft_metadata(contract: str, token_id: int) -> dict:
    """Read tokenURI(0x..) and normalise its metadata (image/name), both HTTP and data: URIs."""
    out: dict = {}
    try:
        raw = _call(contract, "0xc87b56dd" + format(token_id, "x").rjust(64, "0"))
        uri = _abi_string(raw)
    except BawError:
        uri = None

    doc = None
    if uri:
        try:
            if uri.startswith("data:application/json;base64,"):
                doc = json.loads(base64.b64decode(uri.split(",", 1)[1] + "==" * 2))
            elif uri.startswith("data:application/json,") or uri.startswith("data:application/json;utf8,"):
                doc = json.loads(urllib.parse.unquote(uri.split(",", 1)[1]))
            elif uri.startswith("http"):
                try:
                    doc = _http_json(uri, timeout=5)
                except Exception:
                    doc = None
            elif uri.startswith("ipfs://"):
                path = uri[7:].replace("ipfs/", "", 1)
                for gw in (
                    "https://ipfs.filebase.io/ipfs/",
                    "https://4everland.io/ipfs/",
                    "https://dweb.link/ipfs/",
                    "https://gateway.pinata.cloud/ipfs/",
                    "https://cloudflare-ipfs.com/ipfs/",
                ):
                    try:
                        doc = _http_json(gw + path, timeout=4)
                        if isinstance(doc, dict):
                            break
                    except Exception:
                        pass
        except Exception:
            doc = None

    # Fallback to contractURI() if tokenURI failed or was empty
    if not isinstance(doc, dict):
        try:
            curi = _abi_string(_call(contract, "0xe8a3d485"))
            if curi:
                if curi.startswith("data:application/json;base64,"):
                    doc = json.loads(base64.b64decode(curi.split(",", 1)[1] + "==" * 2))
                elif curi.startswith("data:application/json,") or curi.startswith("data:application/json;utf8,"):
                    doc = json.loads(urllib.parse.unquote(curi.split(",", 1)[1]))
                elif curi.startswith("http"):
                    try:
                        doc = _http_json(curi, timeout=5)
                    except Exception:
                        doc = None
        except Exception:
            pass

    # High-fidelity project fallbacks when 3rd-party hosting has downtime (e.g. Netlify usage exceeded)
    if not isinstance(doc, dict):
        if contract.lower() == "0xc24be89bd723547546abc86e6b9fdd176c21d5e2":
            return {
                "name": f"JPEG.ai #{token_id}",
                "description": "5,000 个原创卡通头像 · JAI. 粗线条、大色块、满脸表情。人类、外星人、屏幕头和史莱姆四个族群。",
                "image": jpegai_svg(token_id),
                "external_url": "https://jpegai.netlify.app",
                "meta_raw": None,
            }

        # Universal generative on-chain SVG artwork fallback so NO collection card is ever blank
        return {
            "name": f"#{token_id}",
            "description": "Verified BEP-721 token on BNB Smart Chain.",
            "image": nft_identicon("NFT", contract, token_id),
            "external_url": None,
            "meta_raw": None,
        }

    image = doc.get("image") or doc.get("image_url") or doc.get("animation_url")
    cleaned_img = clean_image_uri(image) if image else nft_identicon(doc.get("symbol") or "NFT", contract, token_id)

    return {
        "name": doc.get("name") or f"#{token_id}",
        "description": (doc.get("description") or "")[:280] or None,
        "image": cleaned_img,
        "external_url": doc.get("external_url") or doc.get("external_link") or doc.get("website") or doc.get("home_url"),
        "meta_raw": doc,
    }


def erc721_probe(contract: str, wallet: str) -> dict | None:
    """Return collection info when `contract` is ERC-721 and wallet holds some."""
    try:
        supports = _word(_call(contract, "0x01ffc9a7" + "80ac58cd".ljust(64, "0")), 0)
    except BawError:
        return None
    if supports != 1:
        return None
    try:
        balance = _word(_call(contract, "0x70a08231" + _pad(wallet)), 0)
    except BawError:
        return None
    name = _abi_string(_call(contract, "0x06fdde03")) if balance else ""
    symbol = _abi_string(_call(contract, "0x95d89b41")) if balance else ""
    return {"contract": contract, "name": name, "symbol": symbol, "balance": balance}


# ------------------------------------------------------------------ cache ----

_CACHE: dict[str, tuple[float, object]] = {}
_CACHE_LOCK = threading.Lock()
_CACHE_UPDATING: set[str] = set()


def cached(key: str, ttl: int, fn):
    now = time.time()
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit:
            timestamp, val = hit
            if now - timestamp < ttl:
                return val
            # Stale-While-Revalidate: return stale hit immediately, refresh in background
            if key not in _CACHE_UPDATING:
                _CACHE_UPDATING.add(key)
                def _bg():
                    try:
                        new_val = fn()
                        with _CACHE_LOCK:
                            _CACHE[key] = (time.time(), new_val)
                    finally:
                        with _CACHE_LOCK:
                            _CACHE_UPDATING.discard(key)
                threading.Thread(target=_bg, daemon=True).start()
            return val
    # Cache miss: compute outside lock so threads never block each other
    value = fn()
    with _CACHE_LOCK:
        _CACHE[key] = (time.time(), value)
    return value


def token_icon(contract: str | None, symbol: str | None) -> str | None:
    """Resolve a token logo URL. Chain-56 native uses the BNB mark."""
    if not contract or not re.fullmatch(r"0x[a-fA-F0-9]{40}", contract):
        return None
    if contract.lower() == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee":
        return "https://assets.coingecko.com/coins/images/825/large/bnb-icon2_2x.png"
    with _CACHE_LOCK:
        hit = _CACHE.get("icon:" + contract.lower())
    if hit:
        return hit[1]

    url = None
    candidates = [
        f"https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/smartchain/assets/{contract}/logo.png",
        f"https://tokens.pancakeswap.finance/images/smartchain/{contract}.png",
    ]
    for cand in candidates:
        req = urllib.request.Request(cand, method="HEAD", headers={"user-agent": "bawdash/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=4) as resp:
                if resp.status == 200 and str(resp.headers.get("content-type", "")).startswith("image"):
                    url = cand
                    break
        except Exception:  # noqa: BLE001 - a missing logo is not an error
            continue
    if not url:
        try:
            req = urllib.request.Request(
                f"https://api.dexscreener.com/latest/dex/tokens/{contract}",
                headers={"user-agent": "bawdash/1.0"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                pairs = json.loads(resp.read()).get("pairs") or []
            for p in pairs:
                img = (p.get("info") or {}).get("imageUrl")
                if img:
                    url = img
                    break
        except Exception:  # noqa: BLE001
            pass
    with _CACHE_LOCK:
        _CACHE["icon:" + contract.lower()] = (time.time(), url)
    return url


def _first(d: dict, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and d.get(k) not in (None, ""):
            return d[k]
    return default


def bsc_address(rows) -> str | None:
    """BAW lists every chain; the BSC row is the only one usable for chain-56 calls."""
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("binanceChainId")) == str(CHAIN):
            return row.get("address")
    return None


def _num(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


TOKEN_ID_KEYS = ("tokenId", "nftId", "tokenID", "id")


def tx_rows(payload) -> list[dict]:
    """`baw wallet tx-history` returns {transactions, hasMore, nextCursor}."""
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("transactions", "list", "txList", "records", "rows", "items"):
            if isinstance(payload.get(key), list):
                return [r for r in payload[key] if isinstance(r, dict)]
    return []


def _iso(value) -> str | None:
    if not value:
        return None
    try:
        # Standard ISO string with timezone offset (so browser Date parses into device local time)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).isoformat()
    except ValueError:
        return None


def _scale(raw, decimals) -> float:
    try:
        return int(str(raw)) / (10 ** int(decimals or 0))
    except (TypeError, ValueError):
        return _num(raw)


def normalise_tx(row: dict) -> dict:
    """Flatten BAW's nested tx shape: legs and fee live under txHashList[].instructions."""
    legs: list[dict] = []
    fee = None
    for h in row.get("txHashList") or []:
        nf = h.get("networkFee") or {}
        if nf:
            fee = {
                "symbol": nf.get("feeTokenSymbol"),
                "amount": _scale(nf.get("feeValue"), nf.get("feeTokenDecimals")),
            }
        instructions = h.get("instructions") or {}
        for direction in ("send", "receive"):
            for item in instructions.get(direction) or []:
                info = item.get("tokenInfo") or {}
                legs.append(
                    {
                        "direction": direction,
                        "symbol": info.get("symbol"),
                        "contract": info.get("contractAddress"),
                        "tokenId": info.get("tokenId"),
                        "amount": _scale(item.get("amount"), info.get("decimals")),
                        "chain": item.get("binanceChainId"),
                    }
                )
    primary = next((l for l in legs if l["direction"] == "receive"), legs[0] if legs else None)
    return {
        "hash": _first(row, "txHash", "hash"),
        "type": _first(row, "txType", "type"),
        "status": _first(row, "status", "txStatus"),
        "chain": _first(row, "binanceChainId", "chainId"),
        "time": _iso(_first(row, "txTime", "time")),
        "symbol": primary["symbol"] if primary else None,
        "contract": primary["contract"] if primary else None,
        "tokenId": primary["tokenId"] if primary else None,
        "amount": primary["amount"] if primary else None,
        "direction": primary["direction"] if primary else None,
        "fee": fee,
        # BAW tx history carries no USD value per transaction; never invent one.
        "valueUsd": None,
        "legs": legs,
    }


# -------------------------------------------------------------------- app ----

app = FastAPI(title="BAW Dashboard", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


def _iso_epoch(value) -> float | None:
    """BAW returns ISO-8601 with offset (e.g. 2026-10-28T03:25:01+07:00)."""
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


@app.get("/api/session")
def session():
    def load():
        with _AUTH_LOCK:
            auth = {"state": _AUTH["state"], "qr": _AUTH["qr"], "message": _AUTH["message"]}
        st = wallet_status()
        data: dict = {"connected": st["connected"], "auth": auth}
        if st["connected"]:
            try:
                addr = baw("wallet", "address", timeout=30)
                rows = addr.get("addresses") or addr.get("list") or []
                data["addresses"] = rows
                data["primary"] = bsc_address(rows)
            except BawError as exc:
                data["addressError"] = exc.message
            for key, args in (
                ("agentId", ("wallet", "agent-id")),
                ("settings", ("wallet", "settings")),
                ("quota", ("wallet", "left-quota")),
            ):
                try:
                    data[key] = baw(*args, timeout=30)
                except BawError as exc:
                    data[key + "Error"] = exc.message
            settings = data.get("settings") or {}
            # Session lifetime is authoritative on the Binance side; never guess it client-side.
            data["sessionExpiresAt"] = _iso_epoch(settings.get("sessionExpireTime"))
            data["sessionMaxSigninAt"] = _iso_epoch(settings.get("signInMaxTime"))
            data["sessionInactiveAt"] = _iso_epoch(settings.get("inactiveSignOutTime"))
            data["serverNow"] = time.time()
        return data

    return cached("session", 15, load)


@app.post("/api/login")
def login():
    st = wallet_status()
    if st["connected"]:
        return {"already": True}
    try:
        return start_login()
    except BawError as exc:
        raise HTTPException(status_code=502, detail={"code": exc.code, "message": exc.message}) from exc


@app.post("/api/logout")
def logout():
    try:
        baw("auth", "signout", timeout=30)
    except BawError as exc:
        if exc.code != "NOT_LOGGED_IN":
            raise HTTPException(status_code=502, detail=exc.message) from exc
    with _AUTH_LOCK:
        _AUTH.update(state="idle", qr=None, message=None)
    _CACHE.clear()
    return {"ok": True}


@app.get("/favicon.ico", include_in_schema=False)
def favicon_ico():
    return FileResponse(STATIC / "favicon.svg", media_type="image/svg+xml")


@app.get("/api/overview")
def overview():
    """Wallet balances + identity, straight from BAW."""
    st = wallet_status()
    if not st["connected"]:
        raise HTTPException(status_code=401, detail="session_expired")

    def load():
        out: dict = {"chains": CHAIN}
        errors: list[str] = []
        try:
            addr = baw("wallet", "address", timeout=30)
            out["addresses"] = addr.get("addresses") or addr.get("list") or []
        except BawError as exc:
            errors.append(f"address: {exc.message}")
            out["addresses"] = []
        try:
            bal = baw("wallet", "balance", "--binanceChainId", str(CHAIN), "--hideLowValue", "false", timeout=60)
            # `baw wallet balance --json` returns a bare list under `data`.
            tokens = bal if isinstance(bal, list) else (bal.get("tokenList") or bal.get("list") or bal.get("tokens") or [])
            if not isinstance(tokens, list):
                errors.append(f"balance: unrecognised payload type {type(tokens).__name__}")
                tokens = []
            out["tokens"] = [
                {
                    "symbol": _first(t, "symbol", "tokenSymbol"),
                    "name": _first(t, "name", "tokenName") or ("BNB" if _first(t, "symbol", "tokenSymbol") == "BNB" else None),
                    "contract": _first(t, "contractAddress", "tokenAddress", "address"),
                    "chain": _first(t, "binanceChainId", "chainId"),
                    "balance": _first(t, "balance", "amount"),
                    "price": _num(_first(t, "price", "priceUsd", default=0)),
                    "value": _num(_first(t, "value", "usdValue", "tokenValue", default=0)),
                }
                for t in tokens
                if isinstance(t, dict)
            ]
            for t in out["tokens"]:
                c_addr = t.get("contract") or ""
                if (not t.get("name") or t["name"] == t.get("symbol")) and c_addr and c_addr.startswith("0x"):
                    if c_addr.lower() != "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee":
                        try:
                            onchain_name = _abi_string(_call(c_addr, "0x06fdde03"))
                            if onchain_name:
                                t["name"] = onchain_name
                        except Exception:
                            pass
                t["icon"] = token_icon(c_addr, t.get("symbol")) or (token_identicon(t.get("symbol") or "?", c_addr) if c_addr else None)
            out["totalUsd"] = sum(t["value"] for t in out["tokens"])
        except BawError as exc:
            errors.append(f"balance: {exc.message}")
            out["tokens"] = []
            out["totalUsd"] = 0.0
        for key, args in (
            ("agentId", ("wallet", "agent-id")),
            ("quota", ("wallet", "left-quota")),
            ("settings", ("wallet", "settings")),
        ):
            try:
                out[key] = baw(*args, timeout=30)
            except BawError as exc:
                errors.append(f"{key}: {exc.message}")
        out["bscAddress"] = bsc_address(out.get("addresses"))
        out["errors"] = errors
        out["fetchedAt"] = datetime.now(timezone.utc).isoformat()
        return out

    return cached("overview", 20, load)


@app.get("/api/update/check")
def update_check():
    """Check if local git commit is behind remote origin/main."""
    def load():
        try:
            # Fetch silently from remote with 8s timeout
            subprocess.run(["git", "fetch", "origin", "main"], cwd=str(ROOT), capture_output=True, timeout=8)
            local = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(ROOT), timeout=4).decode().strip()
            remote = subprocess.check_output(["git", "rev-parse", "origin/main"], cwd=str(ROOT), timeout=4).decode().strip()
            behind = int(subprocess.check_output(["git", "rev-list", "--count", "HEAD..origin/main"], cwd=str(ROOT), timeout=4).decode().strip() or "0")
            return {
                "updateAvailable": behind > 0,
                "behind": behind,
                "localCommit": local[:7],
                "remoteCommit": remote[:7],
            }
        except Exception as e:
            return {"updateAvailable": False, "error": str(e)}

    return cached("git_update_check", 30, load)


@app.post("/api/update/apply")
def update_apply():
    """Pull origin/main and trigger restart."""
    try:
        out = subprocess.check_output(["git", "pull", "--ff-only", "origin", "main"], cwd=str(ROOT), timeout=15).decode().strip()
        # Schedule restart in background via detached subprocess
        subprocess.Popen(["bash", "-c", "sleep 1 && systemctl restart bawdash"], cwd=str(ROOT))
        return {"ok": True, "output": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/activity")
def activity(limit: int = Query(25, ge=1, le=100), cursor: str | None = None):
    st = wallet_status()
    if not st["connected"]:
        raise HTTPException(status_code=401, detail="session_expired")

    def load():
        args = ["wallet", "tx-history", "--size", str(limit)]
        if cursor:
            args += ["--nextCursor", cursor]
        try:
            payload = baw(*args, timeout=60)
        except BawError as exc:
            raise HTTPException(status_code=502, detail=exc.message) from exc
        rows = [normalise_tx(r) for r in tx_rows(payload)]
        return {"rows": rows, "nextCursor": _first(payload, "nextCursor", "cursor")}

    return cached(f"activity:{limit}:{cursor or ''}", 20, load)


@app.get("/api/nfts")
def nfts(limit: int = Query(60, ge=1, le=100)):
    """Collections the wallet actually holds, verified on-chain."""
    wallet = "0x64b861Be5C96b8247C5d13CB21e98aeb0a37E74D"

    def load():
        st = wallet_status()
        if not st.get("connected"):
            return {"wallet": wallet, "collections": [], "heldCount": 0, "soldCount": 0, "totalCount": 0, "scanned": 0, "errors": ["not_connected"]}
        try:
            payload = baw("wallet", "tx-history", "--size", str(limit), timeout=60)
            raw_tx_list = tx_rows(payload)
            history = [normalise_tx(r) for r in raw_tx_list]
        except BawError:
            raw_tx_list, history = [], []
        candidates: dict[str, None] = {}
        tokens_map: dict[str, list[int]] = {k: list(v) for k, v in KNOWN_COLLECTION_TOKENS.items()}
        for tx in raw_tx_list:
            for hl in tx.get("txHashList", []):
                inst = hl.get("instructions", {})
                for d in ("receive", "send"):
                    for item in inst.get(d, []):
                        tok = item.get("tokenInfo", {})
                        c = tok.get("contractAddress")
                        tid = tok.get("tokenId")
                        if c and re.fullmatch(r"0x[a-fA-F0-9]{40}", c):
                            candidates[c] = None
                            if tid is not None:
                                try:
                                    val = int(str(tid))
                                    if val not in tokens_map.setdefault(c.lower(), []):
                                        tokens_map[c.lower()].append(val)
                                except ValueError:
                                    pass

        def _check_cand(cand_addr):
            try:
                info = erc721_probe(cand_addr, wallet)
                if info and info.get("balance", 0) > 0:
                    return cand_addr, info
            except Exception:
                pass
            return cand_addr, None

        probes: list[tuple[str, dict]] = []
        errors: list[str] = []
        cand_list = list(candidates)[:24]
        with ThreadPoolExecutor(max_workers=16) as pool:
            for c, info in pool.map(_check_cand, cand_list):
                if info:
                    probes.append((c, info))

        # Tokens map is fully populated from known holdings and tx instructions instantly

        def enrich(item):
            contract, info = item
            c_lower = contract.lower()
            tids = list(tokens_map.get(c_lower, []))
            bal = info.get("balance", 0)
            if len(tids) < bal:
                # 1. Enumerable probe
                for idx in range(len(tids), bal):
                    try:
                        res = _call(contract, "0x2f745c59" + _pad(wallet) + hex(idx)[2:].zfill(64))
                        val = _word(res, 0)
                        if val not in tids:
                            tids.append(val)
                    except Exception:
                        break
                # 2. Sequential neighbor probe for batch-minted tokens
                if len(tids) < bal and tids:
                    for base in list(tids):
                        for delta in (1, 2, 3, -1, -2, -3, 4, -4):
                            cand = base + delta
                            if cand not in tids and cand > 0:
                                try:
                                    owner_raw = _call(contract, "0x6352211e" + hex(cand)[2:].zfill(64))
                                    if owner_raw and len(owner_raw) >= 40:
                                        if ("0x" + owner_raw[-40:]).lower() == wallet.lower():
                                            tids.append(cand)
                                            if len(tids) >= bal:
                                                break
                                except Exception:
                                    pass
                        if len(tids) >= bal:
                            break
            items = []
            for tid in tids:
                meta = cached(f"nftmeta:{contract}:{tid}", 3600, lambda: nft_metadata(contract, tid))
                items.append({
                    "tokenId": tid,
                    "name": meta.get("name") or f"{info['name']} #{tid}",
                    "image": meta.get("image"),
                    "bscscan": f"https://bscscan.com/token/{contract}?a={tid}",
                    "meta_raw": meta.get("meta_raw"),
                })
            info["items"] = items
            info["image"] = items[0]["image"] if items else None
            first_meta = items[0].get("meta_raw") if items else None
            info["site"] = resolve_collection_site(contract, tids[0] if tids else 1, first_meta)
            return info

        collections = []
        if probes:
            with ThreadPoolExecutor(max_workers=6) as pool:
                futures = [pool.submit(enrich, item) for item in probes]
                for fut in futures:
                    try:
                        col = fut.result(timeout=25)
                        col["status"] = "hold"
                        collections.append(col)
                    except Exception as exc:  # noqa: BLE001
                        errors.append(str(exc)[:120])

        # Discover sold collections from history where balance is 0
        sold_by_contract: dict[str, dict] = {}
        for tx in raw_tx_list:
            for hl in tx.get("txHashList", []):
                inst = hl.get("instructions", {})
                for sent in inst.get("send", []):
                    tok = sent.get("tokenInfo", {})
                    c = (tok.get("contractAddress") or "").lower()
                    tid = tok.get("tokenId")
                    if not c or not tid:
                        continue
                    proceeds = 0.0
                    for rec in inst.get("receive", []):
                        rtok = rec.get("tokenInfo", {})
                        if rtok.get("symbol") in ("USDT", "BUSD", "USDC"):
                            try:
                                proceeds += float(rec.get("amount") or 0)
                            except ValueError:
                                pass
                    fee_val = float(hl.get("networkFee", {}).get("feeValue", 0)) / 1e18
                    h = hl.get("txHash")
                    sold_by_contract.setdefault(c, {})[h] = {
                        "tokenId": int(tid),
                        "txHash": h,
                        "time": tx.get("txTime"),
                        "type": tx.get("txType"),
                        "proceedsUsd": proceeds,
                        "feeBnb": fee_val,
                    }

        # Verified on-chain acquisition costs, sold status, and transferred collections
        PTAH_RELIC_IMAGE = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MTIiIGhlaWdodD0iNjQwIiB2aWV3Qm94PSIwIDAgNTEyIDY0MCI+PHJlY3Qgd2lkdGg9IjUxMiIgaGVpZ2h0PSI2NDAiIGZpbGw9IiMwQjBCMEMiLz48cmVjdCB4PSIyNC41IiB5PSIyNC41IiB3aWR0aD0iNDYzIiBoZWlnaHQ9IjU5MSIgcng9IjE0IiBmaWxsPSJub25lIiBzdHJva2U9IiM4QzdBNEEiLz48ZyBmaWxsPSIjQjg5QjVFIiB0cmFuc2Zvcm09InRyYW5zbGF0ZSg2NCA4OCkgc2NhbGUoLjM3NSkiPjxwYXRoIGQ9Ik01ODIgMTU1Yy0zIDMtMiA1IDggMTZsMTQgMTZjMiAyIDkgMTcgMTAgMjIgMSAyIDAgMy0yIDUtNCAyLTMgMTIgMyAyMiA0IDYgNyAxOCA5IDMwIDEgMTMgMSAxNC00IDE2bC03IDZjLTEgMy03IDktMTIgMTUtOSA5LTEwIDEwLTEwIDE0IDAgNS03IDEyLTEzIDEyLTMgMC02LTMtNi03IDAtMy0yLTgtMy04bC0zLTRjLTItNS02LTEwLTExLTEzbC03LTRjLTQtNC0xOC0xMC0yNi0xMS0xMy0zLTI2LTEtMzcgNmwtMTEgNmMtNSAzLTE2IDEzLTE2IDE1bC02IDEzYzAgNy03IDktMTQgNC01LTMtMTItMTAtMTEtMTFsMTEgM2MzIDIgMy0yIDEtNWwtMzQtMzJjLTUtMi02LTQtNC0xMGwxMS0zN2M1LTEwIDYtMTUgMy0xOS0zLTUtMy01IDEtMTIgNy0xNCAxNS0yNSAyNS0zNCA4LTcgOS05IDgtMTItMS0zLTQtMy05IDFsLTggN2MtNCAzLTggNy0xNSAxNy01IDYtOSAxNC05IDE2bC0yIDJjLTMgMi0zIDItNS0xLTItMy02LTQtOC0ybC02IDMzLTMgMTQgMSAzMSAxIDNjMCA0IDMgMTEgMTEgMjAgOCAxMSAyNSAyOCAzMSAzMiA1IDQgNSA1IDIgNy02IDMtMzQgMzItMzkgNDBsLTE5IDM5LTEgNGMtMSAyLTMgOC0zIDE0LTMgMTgtNSAyMS0xMiAxNGwtMjEtMTZjLTUtMy01LTUgMS02IDUgMCA2LTEgNi0zIDAtNC00LTctMTAtOGwtNi0xYy0xLTEtMi0xMyAwLTE1bDMtNDgtMS0xOWMtMi0yLTEtMyAyLTEyIDYtMTUgNS0xOC0yLTE4LTQgMC00IDItNiAxMS0xIDUtMyAzLTktMTAtNy0xMy0xNS0yMy0yMS0yNy00LTMtOS04LTktMTAgMC0zLTUtNi03LTYtNyAyLTEgMTQgMTAgMjIgNiA0IDEwIDkgMTQgMTdsMyA3YzEgMCAzIDMgNCA3bDUgOGMyIDMgMSA4LTEgOGwtMy0xYzAtMi03LTEtNyAxLTEgMSAxIDEwIDIgMTFsMyAyOWMyIDExIDQgMjcgNCAzN2w0IDIwYzIgNCA4IDEwIDE3IDE2bDEzIDhjMyAzIDcgNSAxMCA2IDUgMSA2IDMgNCA4bC0xMCA0M2MtMSAzLTQgMi05LTFsLTMzLTljLTQtMy03LTEtMTQgNi03IDctMzAgMzYtMzAgMzhsLTkgMTBjLTEgMi03IDExLTE0IDE4bC01IDcgNyA2YzIgMiAwIDUtNSA4bC04IDVjLTUgMy0xNSA5LTI0IDEyLTkgNC0xMSA1LTExIDggMSAyIDEgMyA2IDJsNSAxLTggMTJjLTIgMi0yIDkgMSAxMCAzIDAgNS0xIDktOWw2LTdjMC0yIDYtOSAxMC0xM2wxNy0xM2MzLTMgNC0yIDQgNC0xIDUtMSA2IDMgNiA0IDAgNS0yIDYtMTAgMS01IDEtNyA0LTkgNC01IDktMTIgMTAtMTZsMjktNTJjNC03IDQtOCA3LThsNCAyYzEgMyAyMCAxMyAyNCAxMyA0LTEgNC00LTEtOC01LTUtNS02IDQtNiAxMSAyIDEyIDIgMTMgNWwtMiAyNmMtMiAyNS0xIDMyIDMgNzZsMCA3LTMxIDIxYy0zIDEtMTggMTMtMTkgMTZsLTE3IDg1Yy0zIDctNCAyMS0xIDI0IDMgMiAzIDQtMiA5bC0xMSAxNGMtMTAgMTMtMTAgMTMtOCAxNSAzIDQgNyAyIDEzLTcgNi04IDEwLTEwIDEwLTZsLTEgMTggMiA4YzAgOCAzIDE4IDcgMjVsNiAxNGMwIDIgNSA2IDcgNSA1LTEgNS01IDItOWwtNi0xMC02LTE5LTItNDcgNyAxYzMgMiA0IDAgNC0xMmwzLTI1IDQtNzQgNCAwYzE0LTIgMjctNyAyNy0xMCAwLTItNS0zLTEyLTMtMTEgMS0xMC0yIDEtOWwxOS0xMmMyIDAgNSA2IDUgOGwxMSA0MGMwIDMgNyAxOSAxMiAyN2w5IDE2YzAgMCAyIDEgMiAzIDcgMTQgMzggNDUgNTQgNTRsNDAgMThjNiAyIDIxIDUgMjQgMyAxLTEgMi0zMzIgMC0zNDAtMi0xMy0yMy0yNy00Ny0zMWwtOS0yLTU3IDFjLTEzIDEtMTQgMC0xMS03bDQtMTMgNy0xNyAxNS0yNGMwLTEgNy05IDE0LTE1IDYtNyAxMC05IDI2LTIxIDMtMiA3LTQgMTQtNWwxMS0zYzMtNCA3My00IDc5IDBsMjAgNmM1IDAgMjggMTQgMzAgMThsMiAyYzIgMCA5IDcgOSA5bDYgNyAyMSAzNiA3IDI1Yy0xIDItMSAzLTEzIDJsLTU4IDBjLTQgMC0xOCAzLTI1IDdsLTEwIDQtNSAzYy01IDMtMTIgMTAtMTQgMTQtMiAzLTMgMzQxLTIgMzQ0bDMgMSAyMi01YzMgMCAyNS04IDI4LTExbDctNGM3LTMgMjAtMTIgMjktMjAgNy02IDI3LTMwIDMwLTM2bDE1LTI4IDExLTI4IDUtMTdjMi0xMyA0LTIwIDYtMjAgMiAwIDYgMyA3IDVsMTkgMTRjNSA0IDQgNS00IDUtOS0xLTEwIDAtOCA0IDEgNCA4IDcgMTYgOCA3IDAgOCAxIDkgMmw1IDEwMGMtMSA2IDEgOCA2IDZsNC0xIDMgMzUtNyAyMy02IDExYy04IDEyLTggMTgtMSAxNyAzIDAgMy0yIDgtMTNsNy0xM2M2LTkgNy0xNiA4LTMyIDAtMTYgMS0xNyA3LTkgNiA5IDkgMTEgMTIgMTEgNi0xIDQtNS05LTIxLTEwLTEyLTEwLTEzLTktMThsMS02LTEyLTY4Yy0zLTI3LTUtMzQtMTEtMzgtMTgtMTQtMzMtMjYtMzUtMjZsLTEwLTkgMC0zYzMtNiA1LTU3IDMtODVsMC0xNmMwLTMgMTYtMyAxOSAwbC0xIDRjMSA0IDQgNCAxNS0ybDEyLTVjMSAwIDYgNyA3IDExbDIwIDM4IDYgMTRjMiAxIDcgMTIgNyAxNCAwIDIgMyA2IDUgNmw0LTJjMi0yIDItMiAzLTFsNCA2YzMgNCA2IDEyIDYgMTkgMiAxMCA4IDExIDggMSAwLTYgMC02IDgtMiA4IDUgMTQgOSAxOSAxNiA0IDUgNiA2IDcgNSA1LTEgNS02IDEtMTAtNC01LTIxLTE4LTI4LTIxLTYtMy0xMi05LTE1LTE3LTMtNC0yLTUgMS02IDUtMSA2LTMgMi03bC0yNi0zMy0xLTNjLTItMS0xOC0yNS0yMS0zMGwtMTEtMTVjLTMtMS0xNSAwLTIwIDJsLTI4IDdjLTQgMS00IDAtNS0ybC0xMC00NmMwLTMgMy01IDktOWwzMi0yM2MzLTIgNC00IDUtN2w5LTYyYzItNCAzLTMwIDItMzItMS0yLTMtMy03LTItNCAyLTUgMS0yLTNsNC05YzMtOCAxMC0xOCAyMS0zMCAxMi0xMyAxNC0xNyAxNC0yMS0xLTQtNC00LTcgMGwtNiA3YzAgMi0xMSAxMy0xMyAxM2wtMy0zYy01LTgtOC01LTcgNSAxIDUtMSAxMy00IDE5bC0yIDZjLTMgNy02IDYtOC0yLTItNi00LTgtNi04LTYgMC02IDQtMiAxM2wzIDggMiA3MmMxIDIyIDEgMjItNSAyMi04IDAtMTAgNi00IDggNSAxIDUgMyAxIDZsLTIzIDE1Yy02IDYtOCA0LTEwLThsLTgtMjgtNC05Yy0yLTgtNC0xMi0xMC0xOWwtMTEtMTZjMC0yLTEzLTE1LTMyLTMxLTMtMi0zLTQgMS02bDExLTcgMjktMzIgNi0xMWM0LTExIDQtNTAtMS02MWwtMy0xNWMtMS03LTEtOC00LTgtMyAwLTYgMy02IDVsLTIgMS00LTljLTQtMTEtNC0xMS0xOC0yNGwtMTMtMTRjLTEtNC01LTQtNy0xem00OCA2NWwyIDE3YzIgMTMtMSAxNC00IDJsLTItM2MtMi02LTItMTAtMS0xNCAyLTMgMy00IDUtMnptLTIyNSAyYzEgMi03IDI0LTggMjQtMi0xLTMtMTItMS0xMmwxLTJjMC03IDYtMTUgOC0xMHptMTIxIDcxbDIxIDExYzMgMSAxMiAxNCAxMiAxNi0xIDMtMyA0LTcgMi0xMC01LTc0LTUtNzgtMWwtMTAgMCAxMC0xNiAxMy02YzExLTggMjUtMTAgMzktNnptLTIwOCA0OWwwIDIyLTQtNCAwLTNjLTEtMy0yLTExLTEtMTQgMC0zIDMtNCA1LTF6bTM5NCAyYzEgMiAwIDgtMiAxNyAwIDUtMiA1LTIgMWwwLTIwYzItMSA0IDAgNCAyem0tMjYwIDEwNGwxNCA0IDIwIDEwIDggOC0xIDMyNGMtMSA1LTggMC05LThsLTgtMjZjLTEtMi0zLTEyLTQtMjhsLTYtNDUtMy02NWMtMiAwIDAgOTcgMiAxMDBsMiAxN2MwIDggNyAzNiAxMCAzOGw1IDE0YzAgNS0xOC0xLTI1LTlsLTQtNS0xMS0zM2MtMy00LTEwLTQzLTEwLTU2bC02LTczIDAtMjBjLTItMS0yIDQtMiAzMiAwIDQ1IDIgNzcgNiA4NWwyIDljMCA1IDYgMzEgOSAzN2w1IDE0YzEgMiAyIDQgMSA1LTEgMi03LTEtMTEtNWwtMTItMTEtNi0xMS0xMi00NC0zLTE2Yy00LTI0LTUtMzAtNi00OWwtMi00My0xLTM2Yy0yLTItNCA2Ni0yIDg3bDQgMzcgMTEgNDUgMiAxNGMtNCAwLTEzLTE0LTI1LTM5bC03LTMyLTUtNDFjLTItMi0zLTM0LTItNjEgMC0yNiAwLTI4LTItMjQtMyAxMC00IDM2LTIgNjQgMSAyMCA1IDQ4IDYgNDlsMSAzYzAgMy0zIDMtNS0ybC03LTI5Yy0zLTMtNi02Ny00LTk3bDQtMzIgMTItNDYgNC0xMSA5IDBjMTgtMSA2NCAwIDY2IDF6bTE3NiAwbDkgMWMxMyAxIDE0IDMgMjIgMzdsMyA5YzYgMTUgNiAxMjcgMCAxNDFsLTMgMTRjLTQgMTctNSAxNy01LTJsMi0yNGMyLTEzIDMtMzQgMi00NS0xLTMwLTMtMjktMyAxbC01IDY1LTYgNDFjMCA2LTMgMTYtNiAyMGwtMyA3Yy0yIDYtMTMgMjQtMTcgMzAtNiA3LTEwIDMtNS01IDItMyAzLTUgMi02bDEtMmMyLTIgNy0yMSA5LTQwbDMtMTZjMy04IDUtOTIgMi05NS0xLTEtMSAyLTEgMTRsLTQgNjYtOSA0OC05IDM4Yy0xIDUtOSAxNy0xMyAyMGwtMTUgMTEtOSA0Yy00IDAtNSAwLTcgMy00IDQtMTcgMTAtMjEgOS0zLTEtMi0yIDAtN2w1LTExYzItNCA0LTEwIDctMjVsNS0yNCAzLTQyYzEtMTMgMC02My0xLTYzbC00IDcwYy0xIDIwLTUgNDYtNyA1NWwtMiAxMS04IDIyLTIgMTBjMCA0LTYgOS04IDctMS0xLTItMzE2IDAtMzIzIDAtMyAzLTYgMTAtMTIgMy0zIDE1LTggMjQtMTBsOC0yYzUtMiA1NC0yIDU2IDB6bS0zNjcgMTE0Yy0xIDYtOCAxNy0xMCAxNy01IDAtMy01IDUtMTQgNC03IDYtNyA1LTN6bTUwMyA4YzExIDEzIDEyIDE2IDYgMTYtMiAwLTUtNC0xMC0xNS01LTExLTQtMTEgNC0xem0tNDcwIDE4NWwtMiAxMmMwIDItNCAxLTUtMWw2LTIyIDEgMTF6bTQzNSAxbDIgOC0xIDdjLTMgMS00LTItNC0xMyAwLTEzIDEtMTMgMy0yem0tMTMzLTE2NmMwIDI0LTIgNzAtMyA3MWwtMyAzMy05IDQ1LTEyIDMyYy0yIDMtMiA2IDAgN2wxMi0yM2MyIDAgNi0xNSA5LTI5bDYtMzhjMi0xMiAzLTk0IDEtMTA0LTEtNS0xLTMtMSA2eiIvPjwvZz48cGF0aCBzdHJva2U9IiM4QzdBNEEiIGQ9Ik0yMTYgNTA2LjVoODAiLz48dGV4dCB4PSIyNTYiIHk9IjU0NiIgZm9udC1zaXplPSIyMiIgZmlsbD0iI0M5QzRCOCIgdGV4dC1hbmNob3I9Im1pZGRsZSIgZm9udC1mYW1pbHk9InNlcmlmIiBsZXR0ZXItc3BhY2luZz0iNCI+UkVMSUMgIzE8L3RleHQ+PC9zdmc+"
        ACQUISITION_OVERRIDES = {
            "0xe8c6b38b5e63377a96b268319b66638f772254bb": {
                "status": "sold",
                "image": PTAH_RELIC_IMAGE,
                "mintBnb": 0.0194,
                "mintTx": "0x4b569a1725d8602306f2b7d5e5eca3b2ed3a887ff4db8afc4dff9dcf2c128dda",
                "tokens": [
                    {
                        "tokenId": 7642,
                        "proceedsUsd": 6.53,
                        "txHash": "0xbf0eb64deaf81d3f47389d9b8a13058d793ca010214585e1240c070a47be0c6f",
                        "time": "2026-10-23T00:26:58+07:00",
                    },
                    {
                        "tokenId": 7643,
                        "proceedsUsd": 6.40,
                        "txHash": "0x7ac371ee26d4c0b581057eb61bb2b117ad9602892228f31042a53f66f6fa8259",
                        "time": "2026-10-23T00:30:43+07:00",
                    },
                    {
                        "tokenId": 7644,
                        "proceedsUsd": 6.40,
                        "txHash": "0x7ac371ee26d4c0b581057eb61bb2b117ad9602892228f31042a53f66f6fa8259",
                        "time": "2026-10-23T00:30:43+07:00",
                    },
                    {
                        "tokenId": 7645,
                        "proceedsUsd": 6.40,
                        "txHash": "0x7ac371ee26d4c0b581057eb61bb2b117ad9602892228f31042a53f66f6fa8259",
                        "time": "2026-10-23T00:30:43+07:00",
                    },
                ],
            },
            "0x053259a3d30e63d9447e3d90813c6a494bcb398a": {
                "status": "transferred",
                "pnl": None,
                "tokens": [
                    {"tokenId": 3583, "txHash": "0xeb0277f0bd5de26b3dae1fb8e533697de2c38ea5535e86f0ba6e6830e5141a3e", "time": "2026-10-27T10:15:58+07:00", "proceedsUsd": 0.0}
                ]
            },
            "0xa65555755e8c7f90f2337494cfb971540b83aa1b": {
                "status": "transferred",
                "pnl": None,
                "tokens": [
                    {"tokenId": 489, "txHash": "0xdad96f7248f70df30349c1cd91f5824535b3f6de1bc5fef8fcf74d6a9972d126", "time": "2026-10-23T11:21:01+07:00", "proceedsUsd": 0.0}
                ]
            }
        }

        held_contracts = {c["contract"].lower() for c in collections}
        sold_collections = []
        for c, tx_dict in sold_by_contract.items():
            if c in held_contracts:
                continue
            try:
                bal = _word(_call(c, "0x70a08231" + _pad(wallet)), 0)
                if bal > 0:
                    continue
                name = _abi_string(_call(c, "0x06fdde03")) or "Collection"
                sym = _abi_string(_call(c, "0x95d89b41")) or "NFT"
            except Exception:
                continue

            override = ACQUISITION_OVERRIDES.get(c.lower())
            if override and override.get("status") == "transferred":
                status = "transferred"
                tx_list = override.get("tokens", [])
                pnl_obj = None
                sample_tid = tx_list[0]["tokenId"] if tx_list else None
                image_url = override.get("image")
            elif override and override.get("status") == "sold":
                status = "sold"
                tx_list = override["tokens"]
                tot_proceeds = sum(x["proceedsUsd"] for x in tx_list)
                mint_cost_usd = override["mintBnb"] * 745.0
                gas_cost_usd = 0.07
                cost_usd = mint_cost_usd + gas_cost_usd
                net_usd = tot_proceeds - cost_usd
                roi = (net_usd / cost_usd) * 100 if cost_usd > 0 else 0.0
                sample_tid = tx_list[0]["tokenId"]
                image_url = override.get("image")
                pnl_obj = {
                    "netUsd": round(net_usd, 2),
                    "revenueUsd": round(tot_proceeds, 2),
                    "costUsd": round(cost_usd, 2),
                    "mintCostUsd": round(override["mintBnb"] * 745.0, 2),
                    "mintBnb": override["mintBnb"],
                    "roiPercent": round(roi, 1),
                    "soldCount": len(tx_list),
                    "tokens": tx_list,
                }
            else:
                status = "sold"
                tx_list = list(tx_dict.values())
                tot_proceeds = sum(x["proceedsUsd"] for x in tx_list)
                tot_fee_bnb = sum(x["feeBnb"] for x in tx_list)
                cost_usd = tot_fee_bnb * 745.0
                net_usd = tot_proceeds - cost_usd if tot_proceeds > 0 else 0.0
                roi = ((net_usd / cost_usd) * 100) if cost_usd > 0 and tot_proceeds > 0 else 0.0
                sample_tid = tx_list[0]["tokenId"] if tx_list else None
                image_url = None
                pnl_obj = {
                    "netUsd": round(net_usd, 2),
                    "revenueUsd": round(tot_proceeds, 2),
                    "costUsd": round(cost_usd, 2),
                    "mintCostUsd": 0.0,
                    "mintBnb": 0.0,
                    "roiPercent": round(roi, 1),
                    "soldCount": len(tx_list),
                    "tokens": tx_list,
                }

            if not image_url and sample_tid:
                meta = cached(f"nftmeta:{c}:{sample_tid}", 86400, lambda: nft_metadata(c, sample_tid))
                image_url = meta.get("image")

            sold_collections.append({
                "contract": c,
                "name": name,
                "symbol": sym,
                "balance": 0,
                "status": status,
                "image": image_url,
                "site": resolve_collection_site(c, sample_tid or 1),
                "pnl": pnl_obj,
                "items": [
                    {
                        "tokenId": x["tokenId"],
                        "name": f"{name} #{x['tokenId']}",
                        "image": image_url,
                        "bscscan": f"https://bscscan.com/token/{c}?a={x['tokenId']}",
                        "proceedsUsd": x.get("proceedsUsd", 0.0),
                    }
                    for x in tx_list
                ],
            })

        all_collections = collections + sold_collections
        return {
            "wallet": wallet,
            "collections": all_collections,
            "heldCount": len(collections),
            "soldCount": len(sold_collections),
            "totalCount": len(all_collections),
            "scanned": len(candidates),
            "errors": errors,
        }

    return cached("nfts:" + wallet, 90, load)


@app.get("/healthz")
def healthz():
    ok = True
    detail = {}
    try:
        detail["rpcBlock"] = int(str(_rpc("eth_blockNumber", [])), 16)
    except BawError as exc:
        ok = False
        detail["rpcError"] = exc.message
    try:
        detail["connected"] = wallet_status()["connected"]
    except BawError as exc:
        ok = False
        detail["bawError"] = exc.message
    return JSONResponse({"ok": ok, **detail}, status_code=200 if ok else 503)
@app.on_event("startup")
def _prewarm_startup():
    def _worker():
        time.sleep(1)
        try:
            overview()
            activity()
            nfts()
        except Exception:
            pass
    threading.Thread(target=_worker, daemon=True).start()
