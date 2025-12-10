import asyncio
import hashlib
import html
import io
import json
import math
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo

from telegram import (
    Update,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.error import Forbidden, BadRequest, TelegramError
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# Optional dependency for Excel catalog (bot will still run without it)
try:
    from openpyxl import Workbook  # type: ignore
    from openpyxl.styles import Font  # type: ignore
except Exception:
    Workbook = None  # type: ignore
    Font = None  # type: ignore

BOT_VERSION = "2025-12-10_fix3"

# ---------- ENV ----------
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CLAIM_CODE = os.environ.get("CLAIM_CODE", "").strip()

# If your service filesystem is read-only, saving state.json can crash the bot.
# We'll try state.json first, and automatically fall back to /tmp if needed.
STATE_FILE = Path(os.environ.get("STATE_FILE", "state.json"))
STATE_FALLBACK = Path("/tmp/dispatch_bot_state.json")

TRIGGERS = {t.strip().lower() for t in os.environ.get("TRIGGERS", "eta,1717").split(",") if t.strip()}

NOMINATIM_USER_AGENT = os.environ.get("NOMINATIM_USER_AGENT", "dispatch-eta-bot/1.0").strip()
NOMINATIM_MIN_INTERVAL = float(os.environ.get("NOMINATIM_MIN_INTERVAL", "1.1"))

ETA_ALL_MAX = int(os.environ.get("ETA_ALL_MAX", "6"))
DELETEALL_DEFAULT = int(os.environ.get("DELETEALL_DEFAULT", "300"))

# Progress alerts posted to the chat when buttons are pressed (silent + optionally auto-delete)
ALERT_TTL_SECONDS = int(os.environ.get("ALERT_TTL_SECONDS", "25"))  # set 0 to disable auto-delete

DEBUG = os.environ.get("DEBUG", "0").strip().lower() in ("1", "true", "yes", "on")


def log(msg: str) -> None:
    if DEBUG:
        print(f"[bot {BOT_VERSION}] {msg}", flush=True)


# ---------- GLOBALS ----------
TF = TimezoneFinder()
NOM_URL = "https://nominatim.openstreetmap.org/search"
OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"

_state_lock = asyncio.Lock()
_geo_lock = asyncio.Lock()
_geo_last = 0.0


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def safe_tz(name: str):
    try:
        return ZoneInfo(name)
    except Exception:
        return timezone.utc


def h(x: Any) -> str:
    return html.escape("" if x is None else str(x), quote=False)


def local_stamp(tz_name: str) -> str:
    tz = safe_tz(tz_name or "UTC")
    return now_utc().astimezone(tz).strftime("%Y-%m-%d %H:%M")


# ---------- STATE (with migration from older scripts) ----------
def _migrate_state(st: dict) -> Tuple[dict, bool]:
    changed = False

    # Owner migration (owner <-> owner_id)
    if st.get("owner_id") is None and st.get("owner") is not None:
        st["owner_id"] = st.get("owner")
        changed = True
    if st.get("owner") is None and st.get("owner_id") is not None:
        st["owner"] = st.get("owner_id")
        changed = True

    # Allowed chats migration (allowed <-> allowed_chats)
    if (not st.get("allowed_chats")) and st.get("allowed"):
        st["allowed_chats"] = st.get("allowed")
        changed = True
    if (not st.get("allowed")) and st.get("allowed_chats"):
        st["allowed"] = st.get("allowed_chats")
        changed = True

    # Location migration (last <-> last_location)
    if st.get("last_location") is None and st.get("last") is not None:
        ll = st.get("last") or {}
        st["last_location"] = {
            "lat": ll.get("lat"),
            "lon": ll.get("lon"),
            "tz": ll.get("tz"),
            "updated_at": ll.get("at") or ll.get("updated_at") or ll.get("timestamp"),
        }
        changed = True
    if st.get("last") is None and st.get("last_location") is not None:
        ll = st.get("last_location") or {}
        st["last"] = {
            "lat": ll.get("lat"),
            "lon": ll.get("lon"),
            "tz": ll.get("tz"),
            "at": ll.get("updated_at") or ll.get("at"),
        }
        changed = True

    # Geocode cache migration (gc <-> geocode_cache)
    if (not st.get("geocode_cache")) and st.get("gc"):
        st["geocode_cache"] = st.get("gc")
        changed = True
    if (not st.get("gc")) and st.get("geocode_cache"):
        st["gc"] = st.get("geocode_cache")
        changed = True

    # History migration (hist <-> history)
    if (not st.get("history")) and st.get("hist"):
        st["history"] = st.get("hist")
        changed = True
    if (not st.get("hist")) and st.get("history"):
        st["hist"] = st.get("history")
        changed = True

    # Focus index migration
    if st.get("focus_i") is None and st.get("del_index") is not None:
        st["focus_i"] = st.get("del_index")
        changed = True

    # Defaults
    st.setdefault("owner_id", None)
    st.setdefault("allowed_chats", [])
    st.setdefault("last_location", None)
    st.setdefault("job", None)
    st.setdefault("focus_i", 0)
    st.setdefault("geocode_cache", {})
    st.setdefault("history", [])

    # Keep aliases too
    st["owner"] = st.get("owner_id")
    st["allowed"] = st.get("allowed_chats")
    st["gc"] = st.get("geocode_cache")
    st["hist"] = st.get("history")

    return st, changed


def load_state() -> dict:
    global STATE_FILE

    if (not STATE_FILE.exists()) and STATE_FALLBACK.exists():
        STATE_FILE = STATE_FALLBACK

    if STATE_FILE.exists():
        try:
            st = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            st = {}
    else:
        st = {}

    st, changed = _migrate_state(st)
    if changed:
        try:
            save_state(st)
        except Exception:
            pass
    return st


def save_state(st: dict) -> None:
    global STATE_FILE

    def _write(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)

    try:
        _write(STATE_FILE)
    except Exception as e:
        log(f"save_state failed at {STATE_FILE}: {e}. Falling back to {STATE_FALLBACK}")
        STATE_FILE = STATE_FALLBACK
        _write(STATE_FILE)


def is_owner(update: Update, st: dict) -> bool:
    u = update.effective_user
    return bool(u and st.get("owner_id") and u.id == st["owner_id"])


def chat_allowed(update: Update, st: dict) -> bool:
    chat = update.effective_chat
    if not chat:
        return False
    if chat.type == "private":
        return is_owner(update, st)
    return chat.id in set(st.get("allowed_chats") or [])


# ---------- ROUTING / GEOCODING ----------
def hav_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def fallback_seconds(dist_m: float) -> float:
    km = dist_m / 1000.0
    sp = 55 if km < 80 else (85 if km < 320 else 105)
    return (km / sp) * 3600.0


def fmt_dur(seconds: float) -> str:
    seconds = max(0, int(seconds))
    m = seconds // 60
    h_ = m // 60
    m = m % 60
    return f"{h_}h {m}m" if h_ else f"{m}m"


def fmt_mi(meters: float) -> str:
    mi = meters / 1609.344
    return f"{mi:.1f} mi" if mi < 10 else f"{mi:.0f} mi"


def addr_variants(addr: str) -> List[str]:
    a = " ".join((addr or "").split())
    if not a:
        return []
    out = [a]
    parts = [p.strip() for p in a.split(",") if p.strip()]
    if len(parts) >= 2:
        out.append(", ".join(parts[1:]))
    out.append(re.sub(r"\b(?:suite|ste|unit|#)\s*[\w\-]+\b", "", a, flags=re.I).strip())
    if len(parts) >= 2:
        out.append(", ".join(parts[-2:]))
    seen, res = set(), []
    for x in out:
        x = " ".join(x.split())
        if x and x not in seen:
            seen.add(x)
            res.append(x)
    return res


async def geocode_cached(st: dict, addr: str) -> Optional[Tuple[float, float, str]]:
    cache = st.get("geocode_cache") or {}
    if addr in cache and isinstance(cache[addr], dict):
        try:
            v = cache[addr]
            return float(v["lat"]), float(v["lon"]), (v.get("tz") or "UTC")
        except Exception:
            pass

    if not NOMINATIM_USER_AGENT:
        return None

    headers = {"User-Agent": NOMINATIM_USER_AGENT}
    async with httpx.AsyncClient(timeout=15, headers=headers) as c:
        for q in addr_variants(addr):
            async with _geo_lock:
                global _geo_last
                wait = (_geo_last + NOMINATIM_MIN_INTERVAL) - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                r = await c.get(NOM_URL, params={"q": q, "format": "jsonv2", "limit": 1})
                _geo_last = time.monotonic()

            if r.status_code >= 400:
                continue
            js = r.json() or []
            if not js:
                continue

            lat, lon = float(js[0]["lat"]), float(js[0]["lon"])
            tz = TF.timezone_at(lat=lat, lng=lon) or "UTC"
            cache[addr] = {"lat": lat, "lon": lon, "tz": tz}
            st["geocode_cache"] = cache

            async with _state_lock:
                st2 = load_state()
                st2.setdefault("geocode_cache", {})
                st2["geocode_cache"][addr] = cache[addr]
                save_state(st2)

            return lat, lon, tz

    return None


async def route(origin: Tuple[float, float], dest: Tuple[float, float]) -> Optional[Tuple[float, float]]:
    url = OSRM_URL.format(lon1=origin[1], lat1=origin[0], lon2=dest[1], lat2=dest[0])
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(url, params={"overview": "false"})
            if r.status_code >= 400:
                return None
            js = r.json() or {}
            routes = js.get("routes") or []
            if not routes:
                return None
            return float(routes[0]["distance"]), float(routes[0]["duration"])
    except Exception:
        return None


async def eta_to(st: dict, origin: Tuple[float, float], label: str, addr: str) -> dict:
    g = await geocode_cached(st, addr)
    if not g:
        return {"ok": False, "err": f"Couldn't locate {label}."}
    dest = (g[0], g[1])
    r = await route(origin, dest)
    if r:
        return {"ok": True, "m": r[0], "s": r[1], "method": "osrm", "tz": g[2]}
    dist = hav_m(origin[0], origin[1], dest[0], dest[1])
    return {"ok": True, "m": dist, "s": fallback_seconds(dist), "method": "approx", "tz": g[2]}


# ---------- LOAD PARSING ----------
RATE_RE = re.compile(r"\b(?:RATE|PAY)\b\s*:\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)", re.I)
MILES_RE = re.compile(r"\b(?:LOADED|MILES)\b\s*:\s*([0-9][0-9,]*)", re.I)

PU_TIME_RE = re.compile(r"^\s*PU time:\s*(.+)$", re.I)
DEL_TIME_RE = re.compile(r"^\s*DEL time:\s*(.+)$", re.I)
PU_ADDR_RE = re.compile(r"^\s*PU Address\s*:\s*(.*)$", re.I)
DEL_ADDR_RE = re.compile(r"^\s*DEL Address(?:\s*\d+)?\s*:\s*(.*)$", re.I)

LOAD_NUM_RE = re.compile(r"^\s*Load Number\s*:\s*(.+)$", re.I)
LOAD_DATE_RE = re.compile(r"^\s*Load Date\s*:\s*(.+)$", re.I)
PICKUP_RE = re.compile(r"^\s*Pickup\s*:\s*(.+)$", re.I)
DELIVERY_RE = re.compile(r"^\s*Delivery\s*:\s*(.+)$", re.I)
TIMEISH = re.compile(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2}|\d{1,2}:\d{2})\b")


def extract_rate_miles(text: str) -> Tuple[Optional[float], Optional[int]]:
    rate = None
    miles = None
    m = RATE_RE.search(text)
    if m:
        try:
            rate = float(m.group(1).replace(",", ""))
        except Exception:
            pass
    m = MILES_RE.search(text)
    if m:
        try:
            miles = int(m.group(1).replace(",", ""))
        except Exception:
            pass
    return rate, miles


def take_block(lines: List[str], i: int, first: str) -> Tuple[List[str], int]:
    out = []
    if first.strip():
        out.append(first.strip())
    j = i + 1
    while j < len(lines):
        s = lines[j].strip()
        if not s:
            break
        low = s.lower()
        if low.startswith(("pu time:", "del time:", "pu address", "del address", "pickup:", "delivery:")):
            break
        if set(s) <= {"-"} or set(s) <= {"="}:
            break
        out.append(s)
        j += 1
    return out, j


def init_job(job: dict) -> dict:
    job.setdefault("meta", {})
    pu = job.setdefault("pu", {})
    pu.setdefault("status", {"arr": None, "load": None, "dep": None, "comp": None})
    pu.setdefault("docs", {"pti": False, "bol": False})

    dels = job.setdefault("del", [])
    for d in dels:
        d.setdefault("status", {"arr": None, "del": None, "dep": None, "comp": None, "skip": False})
        d.setdefault("docs", {"pod": False})
    return job


def normalize_job(job: Optional[dict]) -> Optional[dict]:
    if not job or not isinstance(job, dict):
        return None

    if "pu" in job and "del" in job:
        return init_job(job)

    # Older schema support (just in case)
    if "pickup" in job and "deliveries" in job:
        pu = job.get("pickup") or {}
        dels = job.get("deliveries") or []
        new_job = {
            "id": job.get("job_id") or job.get("id") or hashlib.sha1(now_iso().encode()).hexdigest()[:10],
            "meta": job.get("meta") or {},
            "pu": {
                "addr": pu.get("address") or pu.get("addr") or "",
                "lines": pu.get("lines") or [pu.get("address") or pu.get("addr") or ""],
                "time": pu.get("time"),
            },
            "del": [
                {
                    "addr": d.get("address") or d.get("addr") or "",
                    "lines": d.get("lines") or [d.get("address") or d.get("addr") or ""],
                    "time": d.get("time"),
                }
                for d in dels
            ],
        }
        return init_job(new_job)

    return init_job(job)


def parse_detailed(text: str) -> Optional[dict]:
    low = text.lower()
    if "pu address" not in low or "del address" not in low:
        return None

    lines = [ln.rstrip() for ln in text.splitlines()]
    pu_time = None
    cur_del_time = None
    pu_addr = None
    pu_lines = None
    dels = []
    load_num = None
    load_date = None

    for i, ln in enumerate(lines):
        m = LOAD_NUM_RE.match(ln)
        if m:
            load_num = m.group(1).strip()

        m = LOAD_DATE_RE.match(ln)
        if m:
            load_date = m.group(1).strip()

        m = PU_TIME_RE.match(ln)
        if m:
            pu_time = m.group(1).strip()

        m = DEL_TIME_RE.match(ln)
        if m:
            cur_del_time = m.group(1).strip()

        m = PU_ADDR_RE.match(ln)
        if m and not pu_addr:
            blk, _ = take_block(lines, i, m.group(1))
            if blk:
                pu_lines = blk
                pu_addr = ", ".join(blk)

        m = DEL_ADDR_RE.match(ln)
        if m:
            blk, _ = take_block(lines, i, m.group(1))
            if blk:
                dels.append({"addr": ", ".join(blk), "lines": blk, "time": cur_del_time})

    if not pu_addr or not dels:
        return None

    rate, miles = extract_rate_miles(text)
    meta: Dict[str, Any] = {"rate": rate, "miles": miles}
    if load_num:
        meta["load_number"] = load_num
    if load_date:
        meta["load_date"] = load_date

    jid = hashlib.sha1((pu_addr + "|" + "|".join(d["addr"] for d in dels)).encode()).hexdigest()[:10]
    job = {"id": jid, "meta": meta, "pu": {"addr": pu_addr, "lines": pu_lines or [pu_addr], "time": pu_time}, "del": dels}
    return init_job(job)


def parse_summary(text: str) -> Optional[dict]:
    low = text.lower()
    if "pickup:" not in low or "delivery:" not in low:
        return None

    meta: Dict[str, Any] = {}
    pu_addr = None
    pu_time = None
    load_date = None
    dels: List[dict] = []
    pending: Optional[dict] = None

    for ln in [x.strip() for x in text.splitlines() if x.strip()]:
        m = LOAD_NUM_RE.match(ln)
        if m:
            meta["load_number"] = m.group(1).strip()
            continue

        m = LOAD_DATE_RE.match(ln)
        if m:
            load_date = m.group(1).strip()
            continue

        m = PICKUP_RE.match(ln)
        if m:
            v = m.group(1).strip()
            if TIMEISH.search(v):
                pu_time = v
            else:
                pu_addr = v
            continue

        m = DELIVERY_RE.match(ln)
        if m:
            v = m.group(1).strip()
            if TIMEISH.search(v):
                if pending and not pending.get("time"):
                    pending["time"] = v
                    pending = None
            else:
                pending = {"addr": v, "lines": [v], "time": None}
                dels.append(pending)
            continue

    if not pu_addr or not dels:
        return None

    rate, miles = extract_rate_miles(text)
    if rate is not None:
        meta["rate"] = rate
    if miles is not None:
        meta["miles"] = miles
    if load_date:
        meta["load_date"] = load_date

    jid = hashlib.sha1((str(meta.get("load_number", "")) + "|" + pu_addr + "|" + "|".join(d["addr"] for d in dels)).encode()).hexdigest()[:10]
    job = {"id": jid, "meta": meta, "pu": {"addr": pu_addr, "lines": [pu_addr], "time": pu_time}, "del": dels}
    return init_job(job)


def parse_job(text: str) -> Optional[dict]:
    return parse_detailed(text) or parse_summary(text)


# ---------- WORKFLOW HELPERS ----------
def pu_complete(job: dict) -> bool:
    return bool((job.get("pu") or {}).get("status", {}).get("comp"))


def next_incomplete(job: dict, start: int = 0) -> Optional[int]:
    for i, d in enumerate(job.get("del") or []):
        if i < start:
            continue
        if not (d.get("status") or {}).get("comp"):
            return i
    return None


def focus(job: dict, st: dict) -> Tuple[str, int]:
    """Returns ('PU', 0) until pickup is marked complete, then ('DEL', idx)."""
    if not pu_complete(job):
        return "PU", 0
    dels = job.get("del") or []
    if not dels:
        return "DEL", 0
    i = int(st.get("focus_i") or 0)
    i = max(0, min(i, len(dels) - 1))
    if (dels[i].get("status") or {}).get("comp"):
        ni = next_incomplete(job, i + 1)
        if ni is not None:
            i = ni
    return "DEL", i


def short_place(lines: List[str], addr: str) -> str:
    for x in reversed(lines or []):
        x = (x or "").strip()
        if x and len(x) <= 70:
            return x
    return (addr or "").strip()


def job_title(job: dict) -> str:
    ln = (job.get("meta") or {}).get("load_number") or ""
    return f"Load {ln}" if ln else "Load"


def load_id_text(job: dict) -> str:
    m = job.get("meta") or {}
    if m.get("load_number"):
        return f"Load {m.get('load_number')}"
    return f"Job {job.get('id','')}"


def toggle_ts(obj: dict, key: str) -> bool:
    """Toggle timestamp on/off. Returns True if toggled ON, False if toggled OFF."""
    if obj.get(key):
        obj[key] = None
        return False
    obj[key] = now_iso()
    return True


async def send_progress_alert(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str) -> None:
    """Send a minimal progress message; try to auto-delete after ALERT_TTL_SECONDS."""
    try:
        m = await ctx.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_notification=True)
    except TelegramError:
        return

    if AL
