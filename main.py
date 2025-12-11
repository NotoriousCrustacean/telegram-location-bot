"""
Telegram Dispatch Bot - Enhanced Version
A bot for tracking load dispatches with ETA calculations and Excel reporting.
"""

import asyncio
import hashlib
import html
import io
import json
import math
import os
import re
import time
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
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

BOT_VERSION = "2025-12-11_pdf_docs_v1"


# ============================================================================
# ENVIRONMENT CONFIGURATION
# ============================================================================

def _strip_quotes(s: str) -> str:
    """Remove surrounding quotes from environment variables."""
    s = (s or "").strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        return s[1:-1].strip()
    return s


def env_str(name: str, default: str = "") -> str:
    """Get string from environment."""
    v = os.environ.get(name)
    if v is None:
        return default
    return _strip_quotes(v)


def env_int(name: str, default: int) -> int:
    """Get integer from environment."""
    v = env_str(name, "")
    if not v:
        return default
    try:
        return int(v)
    except (ValueError, TypeError):
        return default


def env_float(name: str, default: float) -> float:
    """Get float from environment."""
    v = env_str(name, "")
    if not v:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def env_bool(name: str, default: bool = False) -> bool:
    """Get boolean from environment."""
    v = env_str(name, "")
    if not v:
        return default
    return v.lower() in ("1", "true", "yes", "y", "on")


# Environment Variables
TOKEN = env_str("TELEGRAM_TOKEN", "")
CLAIM_CODE = env_str("CLAIM_CODE", "")

STATE_FILE = Path(env_str("STATE_FILE", "state.json"))
STATE_FALLBACK = Path("/tmp/dispatch_bot_state.json")

TRIGGERS = {t.strip().lower() for t in env_str("TRIGGERS", "eta,1717").split(",") if t.strip()}

NOMINATIM_USER_AGENT = env_str("NOMINATIM_USER_AGENT", "dispatch-eta-bot/1.0")
NOMINATIM_MIN_INTERVAL = env_float("NOMINATIM_MIN_INTERVAL", 1.1)

ETA_ALL_MAX = env_int("ETA_ALL_MAX", 6)
DELETEALL_DEFAULT = env_int("DELETEALL_DEFAULT", 300)
ALERT_TTL_SECONDS = env_int("ALERT_TTL_SECONDS", 25)

DEBUG = env_bool("DEBUG", False)


def log(msg: str) -> None:
    """Log debug messages."""
    if DEBUG:
        print(f"[bot {BOT_VERSION}] {msg}", flush=True)


# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

TF = TimezoneFinder()
NOM_URL = "https://nominatim.openstreetmap.org/search"
OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"

_state_lock = asyncio.Lock()
_geo_lock = asyncio.Lock()
_geo_last = 0.0


# ============================================================================
# TIME & FORMATTING UTILITIES
# ============================================================================

def now_utc() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def now_iso() -> str:
    """Get current UTC datetime as ISO string."""
    return now_utc().isoformat()


def safe_tz(name: str) -> timezone:
    """Get timezone safely, fallback to UTC."""
    try:
        return ZoneInfo(name)
    except Exception:
        return timezone.utc


def h(x: Any) -> str:
    """HTML escape for Telegram."""
    return html.escape("" if x is None else str(x), quote=False)


def local_stamp(tz_name: str) -> str:
    """Get local timestamp string."""
    tz = safe_tz(tz_name or "UTC")
    return now_utc().astimezone(tz).strftime("%Y-%m-%d %H:%M")


def week_key(dt: datetime) -> str:
    """Get ISO week key (e.g., '2025-W50')."""
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def money(x: Optional[float]) -> str:
    """Format money value."""
    if x is None:
        return "-"
    try:
        return f"${float(x):,.0f}"
    except (ValueError, TypeError):
        return str(x)


# ============================================================================
# STATE MANAGEMENT
# ============================================================================

def _migrate_state(st: dict) -> Tuple[dict, bool]:
    """Migrate state to current schema."""
    changed = False

    # Owner aliases
    if st.get("owner_id") is None and st.get("owner") is not None:
        st["owner_id"] = st.get("owner")
        changed = True
    if st.get("owner") is None and st.get("owner_id") is not None:
        st["owner"] = st.get("owner_id")
        changed = True

    # Allowed chats aliases
    if (not st.get("allowed_chats")) and st.get("allowed"):
        st["allowed_chats"] = st.get("allowed")
        changed = True
    if (not st.get("allowed")) and st.get("allowed_chats"):
        st["allowed"] = st.get("allowed_chats")
        changed = True

    # Last location aliases
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

    # Geocode cache aliases
    if (not st.get("geocode_cache")) and st.get("gc"):
        st["geocode_cache"] = st.get("gc")
        changed = True
    if (not st.get("gc")) and st.get("geocode_cache"):
        st["gc"] = st.get("geocode_cache")
        changed = True

    # History aliases
    if (not st.get("history")) and st.get("hist"):
        st["history"] = st.get("hist")
        changed = True
    if (not st.get("hist")) and st.get("history"):
        st["hist"] = st.get("history")
        changed = True

    if st.get("focus_i") is None and st.get("del_index") is not None:
        st["focus_i"] = st.get("del_index")
        changed = True

    # Set defaults
    st.setdefault("owner_id", None)
    st.setdefault("allowed_chats", [])
    st.setdefault("last_location", None)
    st.setdefault("job", None)
    st.setdefault("focus_i", 0)
    st.setdefault("geocode_cache", {})
    st.setdefault("history", [])
    st.setdefault("last_finished", None)
    st.setdefault("panel_messages", {})
    st.setdefault("documents", {})  # {load_number_or_job_id: [{file_id, type, filename, timestamp, chat_id}]}
    st.setdefault("documents", {})  # {load_id: [{type, file_id, file_name, added_at}, ...]}
    st.setdefault("pending_doc", None)  # {file_id, file_name, message_id, chat_id}

    # Mirror legacy keys
    st["owner"] = st.get("owner_id")
    st["allowed"] = st.get("allowed_chats")
    st["gc"] = st.get("geocode_cache")
    st["hist"] = st.get("history")

    return st, changed


def load_state() -> dict:
    """Load state from disk."""
    global STATE_FILE

    if (not STATE_FILE.exists()) and STATE_FALLBACK.exists():
        STATE_FILE = STATE_FALLBACK

    if STATE_FILE.exists():
        try:
            st = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            log(f"Failed to load state: {e}")
            st = {}
    else:
        st = {}

    st, changed = _migrate_state(st)
    if changed:
        try:
            save_state(st)
        except Exception as e:
            log(f"Failed to save migrated state: {e}")
    return st


def save_state(st: dict) -> None:
    """Save state to disk atomically."""
    global STATE_FILE

    def _write(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    try:
        _write(STATE_FILE)
    except Exception as e:
        log(f"save_state failed at {STATE_FILE}: {e}. Falling back to {STATE_FALLBACK}")
        STATE_FILE = STATE_FALLBACK
        _write(STATE_FILE)


def is_owner(update: Update, st: dict) -> bool:
    """Check if user is the bot owner."""
    u = update.effective_user
    return bool(u and st.get("owner_id") and u.id == st["owner_id"])


def chat_allowed(update: Update, st: dict) -> bool:
    """Check if chat is allowed to use the bot."""
    chat = update.effective_chat
    if not chat:
        return False
    if chat.type == "private":
        return is_owner(update, st)
    return chat.id in set(st.get("allowed_chats") or [])


# ============================================================================
# GEOCODING & ROUTING
# ============================================================================

def hav_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate haversine distance in meters."""
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def fallback_seconds(dist_m: float) -> float:
    """Estimate travel time based on distance."""
    km = dist_m / 1000.0
    # Adjust speed based on distance: local = 55km/h, regional = 85km/h, long = 105km/h
    speed = 55 if km < 80 else (85 if km < 320 else 105)
    return (km / speed) * 3600.0


def fmt_dur(seconds: float) -> str:
    """Format duration as 'Xh Ym' or 'Ym'."""
    seconds = max(0, int(seconds))
    m = seconds // 60
    h = m // 60
    m = m % 60
    return f"{h}h {m}m" if h else f"{m}m"


def fmt_mi(meters: float) -> str:
    """Format distance in miles."""
    mi = meters / 1609.344
    return f"{mi:.1f} mi" if mi < 10 else f"{mi:.0f} mi"


def addr_variants(addr: str) -> List[str]:
    """Generate address variants for geocoding with improved parsing."""
    # Normalize whitespace and add spaces after commas
    a = " ".join((addr or "").split())
    a = re.sub(r',(?=\S)', ', ', a)  # Add space after comma if missing
    
    if not a:
        return []
    
    out = []
    parts = [p.strip() for p in a.split(",") if p.strip()]
    
    # Find street address (contains numbers and street keywords)
    street = None
    for part in parts:
        if re.search(r'\d+.*(?:road|rd|street|st|avenue|ave|lane|ln|drive|dr|blvd|boulevard|way|court|ct|circle|cir)', part, re.I):
            # Clean building/gate/suite info from street
            street = re.sub(r'\b(?:bldg|bld|building|gate|suite|ste|unit|#)\s*[\w\-]+\b', '', part, flags=re.I).strip()
            break
    
    # Variant 1: Street + last 2 parts (city, state+zip)
    if street and len(parts) >= 2:
        out.append(", ".join([street] + parts[-2:]))
    
    # Variant 2: Street + city + state (no zip)
    if street and len(parts) >= 3:
        # Try to find state abbreviation
        for i, part in enumerate(parts):
            if re.search(r'\b[A-Z]{2}\b', part):
                out.append(", ".join([street, parts[i-1] if i > 0 else parts[-2], part.split()[0]]))
                break
    
    # Variant 3: City + State + ZIP
    if len(parts) >= 2:
        out.append(", ".join(parts[-2:]))
    
    # Variant 4: Just ZIP code + USA
    for part in parts:
        zip_match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', part)
        if zip_match:
            out.append(zip_match.group(1) + ", USA")
    
    # Variant 5: City + State only (no ZIP)
    if len(parts) >= 2:
        for i, part in enumerate(parts):
            if re.search(r'\b[A-Z]{2}\b', part):
                if i > 0:
                    out.append(f"{parts[i-1]}, {part.split()[0]}")
    
    # Variant 6: Original with spaces fixed
    out.insert(0, a)
    
    # Deduplicate while preserving order
    seen, res = set(), []
    for x in out:
        x = " ".join(x.split())
        if x and x not in seen:
            seen.add(x)
            res.append(x)
    return res


async def geocode_cached(st: dict, addr: str) -> Optional[Tuple[float, float, str]]:
    """Geocode address with caching and detailed logging."""
    cache = st.get("geocode_cache") or {}
    
    # Check cache
    if addr in cache and isinstance(cache[addr], dict):
        try:
            v = cache[addr]
            log(f"✓ Cache hit for: {addr[:50]}")
            return float(v["lat"]), float(v["lon"]), (v.get("tz") or "UTC")
        except (ValueError, TypeError, KeyError):
            pass

    if not NOMINATIM_USER_AGENT:
        log("NOMINATIM_USER_AGENT not set")
        return None

    headers = {"User-Agent": NOMINATIM_USER_AGENT}
    
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        variants = addr_variants(addr)
        log(f"Trying {len(variants)} address variants for: {addr[:60]}...")
        
        for idx, query in enumerate(variants):
            log(f"  Variant {idx+1}/{len(variants)}: {query[:80]}")
            
            # Rate limiting
            async with _geo_lock:
                global _geo_last
                wait = (_geo_last + NOMINATIM_MIN_INTERVAL) - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                
                try:
                    r = await client.get(NOM_URL, params={"q": query, "format": "jsonv2", "limit": 1})
                    _geo_last = time.monotonic()
                except Exception as e:
                    log(f"    ✗ Request error: {e}")
                    continue

            if r.status_code >= 400:
                log(f"    ✗ HTTP {r.status_code}")
                continue
            
            try:
                js = r.json() or []
            except Exception as e:
                log(f"    ✗ JSON parse error: {e}")
                continue
                
            if not js:
                log(f"    ✗ No results")
                continue

            try:
                lat, lon = float(js[0]["lat"]), float(js[0]["lon"])
                tz = TF.timezone_at(lat=lat, lng=lon) or "UTC"
                
                log(f"    ✓ SUCCESS: {lat:.4f}, {lon:.4f} ({tz})")
                
                # Update cache
                cache[addr] = {"lat": lat, "lon": lon, "tz": tz}
                st["geocode_cache"] = cache

                # Persist to disk
                async with _state_lock:
                    st2 = load_state()
                    st2.setdefault("geocode_cache", {})
                    st2["geocode_cache"][addr] = cache[addr]
                    save_state(st2)

                return lat, lon, tz
            except (ValueError, TypeError, KeyError, IndexError) as e:
                log(f"    ✗ Parse error: {e}")
                continue

    log(f"  ✗ All {len(variants)} variants failed for: {addr[:60]}")
    return None


async def route(origin: Tuple[float, float], dest: Tuple[float, float]) -> Optional[Tuple[float, float]]:
    """Get route distance and duration from OSRM."""
    url = OSRM_URL.format(lon1=origin[1], lat1=origin[0], lon2=dest[1], lat2=dest[0])
    
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, params={"overview": "false"})
            
            if r.status_code >= 400:
                return None
            
            js = r.json() or {}
            routes = js.get("routes") or []
            
            if not routes:
                return None
            
            return float(routes[0]["distance"]), float(routes[0]["duration"])
    except Exception as e:
        log(f"Routing error: {e}")
        return None


async def eta_to(st: dict, origin: Tuple[float, float], label: str, addr: str) -> dict:
    """Calculate ETA to destination."""
    g = await geocode_cached(st, addr)
    if not g:
        return {"ok": False, "err": f"Couldn't locate {label}."}
    
    dest = (g[0], g[1])
    r = await route(origin, dest)
    
    if r:
        return {"ok": True, "m": r[0], "s": r[1], "method": "osrm", "tz": g[2]}
    
    # Fallback to haversine
    dist = hav_m(origin[0], origin[1], dest[0], dest[1])
    return {"ok": True, "m": dist, "s": fallback_seconds(dist), "method": "approx", "tz": g[2]}


async def estimate_miles(st: dict, job: dict) -> Optional[float]:
    """Estimate total miles for a job."""
    addrs = [job["pu"]["addr"]] + [d["addr"] for d in (job.get("del") or [])]
    coords: List[Tuple[float, float]] = []
    
    for a in addrs:
        g = await geocode_cached(st, a)
        if not g:
            return None
        coords.append((g[0], g[1]))
    
    if len(coords) < 2:
        return 0.0
    
    total_m = 0.0
    for a, b in zip(coords, coords[1:]):
        r = await route(a, b)
        total_m += r[0] if r else hav_m(a[0], a[1], b[0], b[1])
    
    return total_m / 1609.344


# ============================================================================
# LOAD PARSING
# ============================================================================

RATE_RE = re.compile(r"\b(?:RATE|PAY)\b\s*:\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)", re.I)
MILES_RE = re.compile(r"\b(?:LOADED|MILES)\b\s*:\s*([0-9][0-9,]*)", re.I)

PU_TIME_RE = re.compile(r"^\s*PU time:\s*(.+)$", re.I)
DEL_TIME_RE = re.compile(r"^\s*DEL time:\s*(.+)$", re.I)
PU_ADDR_RE = re.compile(r"^\s*PU Address\s*:\s*(.*)$", re.I)
DEL_ADDR_RE = re.compile(r"^\s*DEL Address(?:\s*\d+)?\s*:\s*(.*)$", re.I)

LOAD_NUM_RE = re.compile(r"^\s*Load\s*(?:Number|#)\s*:?\s*(.+)$", re.I)
LOAD_DATE_RE = re.compile(r"^\s*Load Date\s*:\s*(.+)$", re.I)
PICKUP_RE = re.compile(r"^\s*Pickup\s*:\s*(.+)$", re.I)
DELIVERY_RE = re.compile(r"^\s*Delivery\s*:\s*(.+)$", re.I)
TIMEISH = re.compile(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2}|\d{1,2}:\d{2})\b")

# Document types for PDF storage
DOC_TYPES = {
    "RC": "Rate Confirmation",
    "BOL": "Bill of Lading",
    "POD": "Proof of Delivery",
    "LUMPER": "Lumper Receipt",
    "SCALE": "Scale Ticket",
    "OTHER": "Other Document",
}


def extract_rate_miles(text: str) -> Tuple[Optional[float], Optional[int]]:
    """Extract rate and miles from text."""
    rate = None
    miles = None
    
    m = RATE_RE.search(text)
    if m:
        try:
            rate = float(m.group(1).replace(",", ""))
        except (ValueError, TypeError):
            pass
    
    m = MILES_RE.search(text)
    if m:
        try:
            miles = int(m.group(1).replace(",", ""))
        except (ValueError, TypeError):
            pass
    
    return rate, miles


def take_block(lines: List[str], i: int, first: str) -> Tuple[List[str], int]:
    """Extract a block of address lines."""
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
    """Initialize job structure with defaults."""
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
    """Normalize and validate job structure."""
    if not job or not isinstance(job, dict):
        return None
    if "pu" in job and "del" in job:
        return init_job(job)
    return None


def parse_detailed(text: str) -> Optional[dict]:
    """Parse detailed load format."""
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
    job = {
        "id": jid,
        "meta": meta,
        "pu": {"addr": pu_addr, "lines": pu_lines or [pu_addr], "time": pu_time},
        "del": dels,
    }
    return init_job(job)


def parse_summary(text: str) -> Optional[dict]:
    """Parse summary load format."""
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
    job = {
        "id": jid,
        "meta": meta,
        "pu": {"addr": pu_addr, "lines": [pu_addr], "time": pu_time},
        "del": dels,
    }
    return init_job(job)


def parse_job(text: str) -> Optional[dict]:
    """Parse job from text (tries both formats)."""
    return parse_detailed(text) or parse_summary(text)


# ============================================================================
# WORKFLOW HELPERS
# ============================================================================

def pu_complete(job: dict) -> bool:
    """Check if pickup is complete."""
    return bool((job.get("pu") or {}).get("status", {}).get("comp"))


def next_incomplete(job: dict, start: int = 0) -> Optional[int]:
    """Find next incomplete delivery stop."""
    for i, d in enumerate(job.get("del") or []):
        if i < start:
            continue
        if not (d.get("status") or {}).get("comp"):
            return i
    return None


def focus(job: dict, st: dict) -> Tuple[str, int]:
    """Determine current focus (PU or DEL index)."""
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


def load_id_text(job: dict) -> str:
    """Get load identifier text."""
    m = job.get("meta") or {}
    if m.get("load_number"):
        return f"Load {m.get('load_number')}"
    return f"Job {job.get('id', '')}"


def toggle_ts(obj: dict, key: str) -> bool:
    """Toggle timestamp (set if None, clear if set)."""
    if obj.get(key):
        obj[key] = None
        return False
    obj[key] = now_iso()
    return True


async def send_progress_alert(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str) -> None:
    """Send short alert that auto-deletes."""
    try:
        m = await ctx.bot.send_message(
            chat_id=chat_id, 
            text=text, 
            parse_mode="HTML", 
            disable_notification=True
        )
    except TelegramError as e:
        log(f"Failed to send alert: {e}")
        return

    if ALERT_TTL_SECONDS <= 0:
        return

    async def _delete_later() -> None:
        await asyncio.sleep(ALERT_TTL_SECONDS)
        try:
            await ctx.bot.delete_message(chat_id=chat_id, message_id=m.message_id)
        except TelegramError:
            pass

    asyncio.create_task(_delete_later())


def short_place(lines: List[str], addr: str) -> str:
    """Get shortened place name."""
    for x in reversed(lines or []):
        x = (x or "").strip()
        if x and len(x) <= 70:
            return x
    return (addr or "").strip()


# ============================================================================
# UI BUTTON BUILDERS
# ============================================================================

def b(label: str, data: str) -> InlineKeyboardButton:
    """Create inline button."""
    return InlineKeyboardButton(label, callback_data=data)


def chk(on: bool, label: str) -> str:
    """Add checkmark to label if on."""
    return ("✅ " + label) if on else label


def build_finished_keyboard() -> InlineKeyboardMarkup:
    """Build keyboard for finished loads."""
    return InlineKeyboardMarkup([[b("📊 Catalog", "SHOW:CAT")]])


def build_keyboard(job: dict, st: dict) -> InlineKeyboardMarkup:
    """Build interactive keyboard for current job stage."""
    stage, i = focus(job, st)
    pu = job["pu"]
    ps = pu["status"]
    pd = pu["docs"]

    rows: List[List[InlineKeyboardButton]] = []

    if stage == "PU":
        rows.append([
            b(chk(bool(ps["arr"]), "Arrived PU"), "PU:A"),
            b(chk(bool(ps["load"]), "Loaded"), "PU:L"),
            b(chk(bool(ps["dep"]), "Departed"), "PU:D"),
        ])
        rows.append([
            b(chk(bool(pd.get("pti")), "PTI"), "DOC:PTI"),
            b(chk(bool(pd.get("bol")), "BOL"), "DOC:BOL"),
            b(chk(bool(ps["comp"]), "PU Complete"), "PU:C"),
        ])
    else:
        dels = job.get("del") or []
        d = dels[i] if dels else {"addr": "", "lines": []}
        ds = d.get("status") or {}
        dd = d.get("docs") or {}
        lbl = f"DEL {i+1}/{len(dels)}" if dels else "DEL"

        rows.append([
            b(chk(bool(ds.get("arr")), f"Arrived {lbl}"), "DEL:A"),
            b(chk(bool(ds.get("del")), "Delivered"), "DEL:DL"),
            b(chk(bool(ds.get("dep")), "Departed"), "DEL:D"),
        ])
        rows.append([
            b(chk(bool(dd.get("pod")), "POD"), "DOC:POD"),
            b(chk(bool(ds.get("comp")), "Stop Complete"), "DEL:C"),
            b("Skip Stop", "DEL:S"),
        ])

    rows.append([b("ETA", "ETA:A"), b("ETA all", "ETA:ALL")])
    rows.append([b("📎 Docs", "SHOW:DOCS"), b("📊 Catalog", "SHOW:CAT"), b("Finish Load", "JOB:FIN")])
    
    return InlineKeyboardMarkup(rows)


# ============================================================================
# FINISH LOAD & WEEKLY TOTALS
# ============================================================================

def week_totals(hist: List[dict], wk: str) -> Tuple[int, float, float]:
    """Calculate weekly totals."""
    count = 0
    sum_rate = 0.0
    sum_miles = 0.0
    
    for r in hist:
        if (r.get("week") or "") != wk:
            continue
        count += 1
        
        rate = r.get("rate")
        if isinstance(rate, (int, float)):
            sum_rate += float(rate)
        
        pm = r.get("posted_miles")
        em = r.get("est_miles")
        use = pm if isinstance(pm, (int, float)) else (em if isinstance(em, (int, float)) else None)
        if use is not None:
            sum_miles += float(use)
    
    return count, sum_rate, sum_miles


async def finish_active_load(
    update: Update, 
    ctx: ContextTypes.DEFAULT_TYPE, 
    *, 
    source: str
) -> Optional[Tuple[dict, dict]]:
    """Finish the active load and record to history."""
    async with _state_lock:
        st = load_state()

    if not is_owner(update, st):
        if source == "callback" and update.callback_query:
            await update.callback_query.answer("Owner only. DM /claim <code>.", show_alert=True)
        else:
            await update.effective_message.reply_text("Owner only. DM me: /claim <code>")
        return None

    job = normalize_job(st.get("job"))
    if not job:
        if source == "callback" and update.callback_query:
            await update.callback_query.answer("No active load.", show_alert=True)
        else:
            await update.effective_message.reply_text("No active load.")
        return None

    loc = st.get("last_location") or {}
    tz_name = loc.get("tz") or "UTC"
    dt_local = now_utc().astimezone(safe_tz(tz_name))
    wk = week_key(dt_local)

    meta = job.get("meta") or {}
    est = await estimate_miles(st, job)

    pu = job["pu"]
    dels = job.get("del") or []
    del_times = " | ".join(((d.get("time") or "").strip() or "-") for d in dels)

    # Get document count for this load
    load_key = get_load_key(job)
    all_docs = st.get("documents") or {}
    load_docs = all_docs.get(load_key, []) if load_key else []

    rec = {
        "week": wk,
        "completed": dt_local.strftime("%Y-%m-%d %H:%M"),
        "completed_utc": now_iso(),
        "tz": tz_name,
        "load_number": meta.get("load_number") or "",
        "job_id": job.get("id"),
        "load_date": meta.get("load_date"),
        "pu_time": pu.get("time"),
        "pickup": (pu.get("addr") or ""),
        "deliveries": " | ".join((d.get("addr") or "") for d in dels),
        "del_times": del_times,
        "stops": len(dels),
        "rate": meta.get("rate"),
        "posted_miles": meta.get("miles"),
        "est_miles": est,
        "documents": len(load_docs),
    }

    chat_id = update.effective_chat.id if update.effective_chat else None

    async with _state_lock:
        st2 = load_state()
        hist = list(st2.get("history") or [])
        hist.append(rec)
        st2["history"] = hist[-1000:]
        st2["last_finished"] = rec
        st2["job"] = None
        st2["focus_i"] = 0
        
        if chat_id is not None:
            pm = st2.get("panel_messages") or {}
            pm.pop(str(chat_id), None)
            st2["panel_messages"] = pm
        
        save_state(st2)

    count, sum_rate, sum_miles = week_totals(st2.get("history") or [], wk)
    rec["_wk_count"] = count
    rec["_wk_rate"] = sum_rate
    rec["_wk_miles"] = sum_miles

    return rec, st2


# ============================================================================
# EXCEL CATALOG GENERATION
# ============================================================================

def try_parse_date(s: Any) -> Optional[date]:
    """Parse date from various formats."""
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y", "%m-%d-%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except (ValueError, TypeError):
            pass
    return None


def try_parse_dt(s: Any) -> Optional[datetime]:
    """Parse datetime from various formats."""
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M", "%m/%d/%Y %H:%M", "%m/%d/%y %H:%M", "%m-%d-%Y %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except (ValueError, TypeError):
            pass
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def autosize_columns(ws, min_w: int = 10, max_w: int = 60) -> None:
    """Auto-size worksheet columns."""
    widths: Dict[int, int] = {}
    for row in ws.iter_rows(values_only=True):
        for i, v in enumerate(row, start=1):
            if v is None:
                continue
            txt = str(v)
            widths[i] = max(widths.get(i, 0), len(txt))
    
    for i, w in widths.items():
        ws.column_dimensions[get_column_letter(i)].width = max(min_w, min(max_w, w + 2))


def write_week_sheet(wb: Workbook, wk: str, records: List[dict]) -> None:
    """Write a weekly sheet to workbook."""
    name = wk[:31]
    ws = wb.create_sheet(title=name)

    def _sort_key(r: dict):
        d = try_parse_dt(r.get("completed_utc")) or try_parse_dt(r.get("completed"))
        return d or datetime(1970, 1, 1)

    records = sorted(records, key=_sort_key)

    ws.append([f"Weekly Loads — {wk}"])
    ws["A1"].font = Font(bold=True, size=14)

    headers = [
        "Completed (Local)",
        "TZ",
        "Load #",
        "Job ID",
        "Load Date",
        "PU Time",
        "Pickup",
        "Delivery Times",
        "Deliveries",
        "Stops",
        "Rate",
        "Posted Miles",
        "Est Miles",
        "Rate/EstMi",
        "Docs",
    ]
    ws.append(headers)
    for c in ws[2]:
        c.font = Font(bold=True)

    sum_rate = 0.0
    sum_miles = 0.0

    for r in records:
        completed_dt = try_parse_dt(r.get("completed")) or try_parse_dt(r.get("completed_utc"))
        load_date = try_parse_date(r.get("load_date"))
        pu_time = try_parse_dt(r.get("pu_time"))

        rate = r.get("rate")
        posted = r.get("posted_miles")
        est = r.get("est_miles")

        rpm = None
        if isinstance(rate, (int, float)) and isinstance(est, (int, float)) and float(est) > 0:
            rpm = float(rate) / float(est)

        ws.append([
            completed_dt if completed_dt else (r.get("completed") or ""),
            r.get("tz") or "",
            r.get("load_number") or "",
            r.get("job_id") or "",
            load_date if load_date else (r.get("load_date") or ""),
            pu_time if pu_time else (r.get("pu_time") or ""),
            r.get("pickup") or "",
            r.get("del_times") or "",
            r.get("deliveries") or "",
            r.get("stops") or "",
            float(rate) if isinstance(rate, (int, float)) else None,
            float(posted) if isinstance(posted, (int, float)) else None,
            float(est) if isinstance(est, (int, float)) else None,
            float(rpm) if isinstance(rpm, (int, float)) else None,
            r.get("documents") or 0,
        ])

        if isinstance(rate, (int, float)):
            sum_rate += float(rate)
        use = posted if isinstance(posted, (int, float)) else (est if isinstance(est, (int, float)) else None)
        if use is not None:
            sum_miles += float(use)

    ws.append([])
    ws.append([
        "TOTAL",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        sum_rate,
        "",
        sum_miles,
        (sum_rate / sum_miles) if sum_miles else None,
        "",
    ])
    for c in ws[ws.max_row]:
        c.font = Font(bold=True)

    for row in ws.iter_rows(min_row=3, max_row=ws.max_row):
        if isinstance(row[0].value, datetime):
            row[0].number_format = "yyyy-mm-dd hh:mm"
        if isinstance(row[4].value, date):
            row[4].number_format = "yyyy-mm-dd"
        if isinstance(row[5].value, datetime):
            row[5].number_format = "yyyy-mm-dd hh:mm"
        if row[10].value is not None:
            row[10].number_format = '"$"#,##0'
        if row[11].value is not None:
            row[11].number_format = "0"
        if row[12].value is not None:
            row[12].number_format = "0"
        if row[13].value is not None:
            row[13].number_format = '"$"#,##0.00'

    ws.freeze_panes = "A3"
    autosize_columns(ws)


def make_xlsx_weekly(records: List[dict], wk: str) -> Tuple[bytes, str]:
    """Generate Excel file for week(s)."""
    wb = Workbook()
    
    try:
        wb.remove(wb.active)
    except Exception:
        pass

    if wk == "ALL":
        by: Dict[str, List[dict]] = {}
        for r in records:
            k = (r.get("week") or "UNKNOWN")
            by.setdefault(k, []).append(r)
        
        for wk2 in sorted(by.keys()):
            write_week_sheet(wb, wk2, by[wk2])
        
        filename = "load_catalog_ALL.xlsx"
    else:
        write_week_sheet(wb, wk, records)
        filename = f"load_catalog_{wk}.xlsx"

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue(), filename


def parse_catalog_arg(args: List[str], tz_name: str) -> str:
    """Parse catalog command argument."""
    wk = week_key(now_utc().astimezone(safe_tz(tz_name)))
    
    if not args:
        return wk
    
    a = args[0].strip().lower()
    
    if a == "all":
        return "ALL"
    
    if a in ("last", "prev", "previous"):
        return week_key(now_utc().astimezone(safe_tz(tz_name)) - timedelta(days=7))
    
    if re.fullmatch(r"\d{4}-w\d{2}", a, re.I):
        return a.upper().replace("w", "W")
    
    return wk


async def send_catalog(
    update: Update, 
    ctx: ContextTypes.DEFAULT_TYPE, 
    *, 
    from_callback: bool = False
):
    """Send Excel catalog to user."""
    async with _state_lock:
        st = load_state()

    if not is_owner(update, st):
        if from_callback and update.callback_query:
            await update.callback_query.answer("Owner only.", show_alert=True)
        else:
            await update.effective_message.reply_text("Owner only.")
        return

    if not chat_allowed(update, st):
        if from_callback and update.callback_query:
            await update.callback_query.answer("Run /allowhere in this group.", show_alert=True)
        else:
            await update.effective_message.reply_text("This chat isn't allowed. Owner: run /allowhere here.")
        return

    hist = list(st.get("history") or [])
    if not hist:
        if from_callback and update.callback_query:
            await update.callback_query.answer("No finished loads yet.", show_alert=True)
        else:
            await update.effective_message.reply_text("No finished loads yet. Finish a load first.")
        return

    tz_name = ((st.get("last_location") or {}).get("tz")) or "UTC"
    wk = parse_catalog_arg(getattr(ctx, "args", []) or [], tz_name)

    records = hist if wk == "ALL" else [r for r in hist if r.get("week") == wk]
    if not records:
        if from_callback and update.callback_query:
            await update.callback_query.answer("No records for that week.", show_alert=True)
        else:
            await update.effective_message.reply_text("No records for that week.")
        return

    xlsx, filename = make_xlsx_weekly(records, wk)
    bio = io.BytesIO(xlsx)
    bio.name = filename
    
    await ctx.bot.send_document(
        chat_id=update.effective_chat.id,
        document=bio,
        filename=filename,
        caption=f"📊 Catalog ({wk})",
    )


# ============================================================================
# TELEGRAM COMMAND HANDLERS
# ============================================================================

async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    await update.effective_message.reply_text(
        f"🚚 <b>Dispatch Bot</b> ({BOT_VERSION})\n"
        f"Triggers: {', '.join(sorted(TRIGGERS))}\n\n"
        "<b>DM Setup:</b>\n"
        "1) /claim &lt;code&gt;\n"
        "2) /update (send location)\n\n"
        "<b>Group Setup:</b>\n"
        "3) /allowhere (in the group)\n\n"
        "<b>Usage:</b>\n"
        "• eta / 1717 or /panel\n"
        "• /finish (owner)\n"
        "• /catalog (owner)\n"
        "• /docs (view saved PDFs)\n\n"
        "<b>Documents:</b>\n"
        "📎 Upload PDFs while a load is active to save BOLs, PODs, etc.\n\n"
        "<b>Tools:</b>\n"
        "• /skip • /reset • /deleteall • /leave\n\n"
        "<b>Debug:</b>\n"
        "• /status • /ping",
        parse_mode="HTML"
    )


async def ping_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /ping command."""
    await update.effective_message.reply_text(f"pong ✅ ({BOT_VERSION})")


async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /status command."""
    async with _state_lock:
        st = load_state()

    uid = update.effective_user.id if update.effective_user else None
    allowed_here = chat_allowed(update, st)
    loc = st.get("last_location")
    job = normalize_job(st.get("job"))
    all_docs = st.get("documents") or {}
    total_docs = sum(len(docs) for docs in all_docs.values())

    lines = [
        f"<b>Status</b> ({h(BOT_VERSION)})",
        f"<b>Your user ID:</b> {h(uid)}",
        f"<b>Owner ID:</b> {h(st.get('owner_id'))}",
        f"<b>This chat allowed:</b> {h(allowed_here)}",
        f"<b>Allowed chats:</b> {h(len(st.get('allowed_chats') or []))}",
        f"<b>State file:</b> {h(str(STATE_FILE))}",
        f"<b>Location saved:</b> {'✅' if loc else '❌'}",
        f"<b>Active load:</b> {'✅' if job else '❌'}",
        f"<b>History rows:</b> {h(len(st.get('history') or []))}",
        f"<b>Saved documents:</b> {h(total_docs)} across {h(len(all_docs))} loads",
    ]
    
    await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML")


async def claim_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /claim command."""
    if update.effective_chat.type != "private":
        await update.effective_message.reply_text("DM me: /claim &lt;code&gt;", parse_mode="HTML")
        return
    
    if not CLAIM_CODE:
        await update.effective_message.reply_text("CLAIM_CODE is missing in Railway Variables.")
        return

    code = " ".join(ctx.args or []).strip()
    if code != CLAIM_CODE:
        await update.effective_message.reply_text("❌ Wrong claim code.")
        return

    async with _state_lock:
        st = load_state()
        st["owner_id"] = update.effective_user.id
        save_state(st)

    await update.effective_message.reply_text("✅ Owner set. Now send /update to save your location.")


async def allowhere_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /allowhere command."""
    async with _state_lock:
        st = load_state()
        
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only. DM /claim &lt;code&gt; first.", parse_mode="HTML")
            return

        chat = update.effective_chat
        if chat.type == "private":
            await update.effective_message.reply_text("Run /allowhere inside the group you want to allow.")
            return

        allowed = set(st.get("allowed_chats") or [])
        allowed.add(chat.id)
        st["allowed_chats"] = sorted(list(allowed))
        save_state(st)

    await update.effective_message.reply_text("✅ Group allowed.")


async def update_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /update command."""
    async with _state_lock:
        st = load_state()
        
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only. DM /claim &lt;code&gt; first.", parse_mode="HTML")
            return

    if update.effective_chat.type != "private":
        await update.effective_message.reply_text("Please DM me /update (best).")
        return

    kb = [[KeyboardButton("📍 Send my current location", request_location=True)]]
    await update.effective_message.reply_text(
        "Tap to send your location.\n"
        "Tip: Attach → Location → Share Live Location for ongoing updates.",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True),
    )


async def on_location(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle location updates."""
    msg = update.effective_message
    if not msg or not msg.location:
        return

    async with _state_lock:
        st = load_state()
        
        if not is_owner(update, st):
            return
        
        lat, lon = msg.location.latitude, msg.location.longitude
        tz = TF.timezone_at(lat=lat, lng=lon) or "UTC"
        
        st["last_location"] = {
            "lat": lat, 
            "lon": lon, 
            "tz": tz, 
            "updated_at": now_iso()
        }
        save_state(st)

    if update.effective_chat.type == "private":
        await msg.reply_text("✅ Location saved.", reply_markup=ReplyKeyboardRemove())


async def panel_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /panel command."""
    async with _state_lock:
        st = load_state()

    if not chat_allowed(update, st):
        if update.effective_chat.type != "private":
            await update.effective_message.reply_text(
                "This chat isn't allowed yet. Owner: run /allowhere here."
            )
        return

    job = normalize_job(st.get("job"))
    if not job:
        await update.effective_message.reply_text("No active load detected yet.")
        return

    m = await update.effective_message.reply_text(
        f"<b>{h(load_id_text(job))}</b>\nTap buttons to update status.",
        parse_mode="HTML",
        reply_markup=build_keyboard(job, st),
    )

    async with _state_lock:
        st2 = load_state()
        pm = st2.get("panel_messages") or {}
        pm[str(update.effective_chat.id)] = int(m.message_id)
        st2["panel_messages"] = pm
        save_state(st2)


async def finish_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /finish command."""
    out = await finish_active_load(update, ctx, source="command")
    if not out:
        return
    
    rec, st2 = out

    rate_txt = money(rec.get("rate") if isinstance(rec.get("rate"), (int, float)) else None)
    id_txt = rec.get("load_number") or rec.get("job_id") or ""

    await send_progress_alert(
        ctx, 
        update.effective_chat.id, 
        f"✅ <b>Load finished</b> {h(id_txt)} · {h(rate_txt)}"
    )

    wk_count = int(rec.get("_wk_count") or 0)
    wk_rate = float(rec.get("_wk_rate") or 0.0)
    wk_miles = float(rec.get("_wk_miles") or 0.0)

    report = "\n".join([
        f"✅ <b>Load finished</b>",
        f"{h(id_txt)} · {h(rate_txt)}",
        f"Week {h(rec.get('week'))}: {h(wk_count)} loads · {h(money(wk_rate))} · {h(int(round(wk_miles)))} mi",
        "📊 Tap Catalog for Excel.",
    ])

    chat_id = update.effective_chat.id
    pm = (st2.get("panel_messages") or {})
    msg_id = pm.get(str(chat_id))
    
    if msg_id:
        try:
            await ctx.bot.edit_message_text(
                chat_id=chat_id, 
                message_id=int(msg_id), 
                text=report, 
                parse_mode="HTML", 
                reply_markup=build_finished_keyboard()
            )
            return
        except TelegramError:
            pass

    await ctx.bot.send_message(
        chat_id=chat_id, 
        text=report, 
        parse_mode="HTML", 
        reply_markup=build_finished_keyboard()
    )


async def catalog_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /catalog command."""
    await send_catalog(update, ctx, from_callback=False)


async def skip_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /skip command."""
    async with _state_lock:
        st = load_state()

    if not is_owner(update, st):
        await update.effective_message.reply_text("Owner only.")
        return

    job = normalize_job(st.get("job"))
    if not job:
        await update.effective_message.reply_text("No active load.")
        return

    stage, i = focus(job, st)
    if stage != "DEL":
        await update.effective_message.reply_text("Can't skip yet — finish PU first.")
        return

    dels = job.get("del") or []
    if not dels:
        await update.effective_message.reply_text("No delivery stops.")
        return

    dd = dels[i]
    ds = dd.get("status") or {}
    ds["skip"] = True
    if not ds.get("comp"):
        ds["comp"] = now_iso()
    dd["status"] = ds
    dels[i] = dd
    job["del"] = dels

    ni = next_incomplete(job, i + 1)
    if ni is not None:
        st["focus_i"] = ni

    async with _state_lock:
        st2 = load_state()
        st2["job"] = job
        st2["focus_i"] = st.get("focus_i", 0)
        save_state(st2)

    await send_progress_alert(
        ctx, 
        update.effective_chat.id, 
        f"⏭️ <b>Skipped</b> stop {h(i+1)}/{h(len(dels))}"
    )
    await update.effective_message.reply_text("Skipped. Use /panel to refresh buttons.")


# ============================================================================
# ETA CALCULATION
# ============================================================================

async def send_eta(update: Update, ctx: ContextTypes.DEFAULT_TYPE, which: str):
    """Send ETA information."""
    async with _state_lock:
        st = load_state()

    if not chat_allowed(update, st):
        return

    loc = st.get("last_location")
    if not loc:
        await update.effective_message.reply_text(
            "No saved location yet. Owner: DM /update."
        )
        return

    origin = (float(loc["lat"]), float(loc["lon"]))
    tz_now = loc.get("tz") or "UTC"
    tz = safe_tz(tz_now)

    await ctx.bot.send_location(
        chat_id=update.effective_chat.id, 
        latitude=origin[0], 
        longitude=origin[1]
    )

    job = normalize_job(st.get("job"))
    if not job:
        await update.effective_message.reply_text(
            f"<b>⏱ ETA</b>\n"
            f"Now: {h(datetime.now(tz).strftime('%Y-%m-%d %H:%M'))} ({h(tz_now)})\n\n"
            f"<i>No active load yet.</i>",
            parse_mode="HTML",
        )
        return

    which = (which or "AUTO").upper()

    if which == "ALL":
        lines: List[str] = [f"<b>{h(load_id_text(job))}</b>"]
        stops: List[Tuple[str, str, List[str], Optional[str]]] = [
            ("PU", job["pu"]["addr"], job["pu"].get("lines") or [], job["pu"].get("time"))
        ]
        
        for j, d in enumerate((job.get("del") or [])[:ETA_ALL_MAX]):
            stops.append((f"D{j+1}", d["addr"], d.get("lines") or [], d.get("time")))

        for lab, addr, lines2, appt in stops:
            r = await eta_to(st, origin, lab, addr)
            place = short_place(lines2, addr)
            
            if r.get("ok"):
                arr = (now_utc().astimezone(tz) + timedelta(seconds=float(r["s"]))).strftime("%H:%M")
                tag = " (approx)" if r.get("method") == "approx" else ""
                appt_txt = f" · Appt: {appt}" if appt else ""
                lines.append(
                    f"<b>{h(lab)}:</b> <b>{h(fmt_dur(r['s']))}</b>{h(tag)} · "
                    f"{h(fmt_mi(r['m']))} · ~{h(arr)}{h(appt_txt)} — {h(place)}"
                )
            else:
                lines.append(f"<b>{h(lab)}:</b> ⚠️ {h(r.get('err'))} — {h(place)}")

        await update.effective_message.reply_text(
            "\n".join(lines), 
            parse_mode="HTML", 
            reply_markup=build_keyboard(job, st)
        )
        return

    # Single stop ETA
    stage, i = focus(job, st)
    
    if stage == "PU":
        addr = job["pu"]["addr"]
        lines2 = job["pu"].get("lines") or []
        appt = job["pu"].get("time")
        stop_label = "PU"
    else:
        dels = job.get("del") or []
        d = dels[i] if dels else {"addr": "", "lines": [], "time": None}
        addr = d["addr"]
        lines2 = d.get("lines") or []
        appt = d.get("time")
        stop_label = f"DEL {i+1}/{len(dels)}" if dels else "DEL"

    r = await eta_to(st, origin, stop_label, addr)
    place = short_place(lines2, addr)

    if r.get("ok"):
        arr = (now_utc().astimezone(tz) + timedelta(seconds=float(r["s"]))).strftime("%H:%M")
        tag = " (approx)" if r.get("method") == "approx" else ""
        out = [
            f"<b>⏱ ETA: {h(fmt_dur(r['s']))}</b>{h(tag)} — {h(stop_label)}",
            f"{h(load_id_text(job))} · {h(place)}",
            f"Arrive ~ {h(arr)} ({h(tz_now)}) · {h(fmt_mi(r['m']))}",
        ]
        if appt:
            out.append(f"Appt: {h(appt)}")
        
        await update.effective_message.reply_text(
            "\n".join(out), 
            parse_mode="HTML", 
            reply_markup=build_keyboard(job, st)
        )
    else:
        await update.effective_message.reply_text(
            f"<b>{h(load_id_text(job))}</b>\n"
            f"<b>⏱ ETA:</b> ⚠️ {h(r.get('err'))}\n"
            f"<b>Target:</b> {h(place)}",
            parse_mode="HTML",
            reply_markup=build_keyboard(job, st),
        )


# ============================================================================
# ADMIN TOOLS
# ============================================================================

async def deleteall_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete recent messages in group (admin only)."""
    async with _state_lock:
        st = load_state()
        
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only.")
            return

    chat = update.effective_chat
    if not chat or chat.type == "private":
        await update.effective_message.reply_text(
            "Bots can't clear a DM history. Delete the chat from your side."
        )
        return

    # Check if bot is admin
    try:
        bot_member = await ctx.bot.get_chat_member(chat.id, ctx.bot.id)
        if bot_member.status not in ("administrator", "creator"):
            await update.effective_message.reply_text(
                "❌ <b>Bot is not admin!</b>\n\n"
                "<b>To enable /deleteall:</b>\n"
                "1. Tap this group name at top\n"
                "2. Tap 'Edit' → 'Administrators'\n"
                "3. Tap 'Add Administrator'\n"
                "4. Select this bot\n"
                "5. Enable 'Delete Messages' permission\n"
                "6. Tap 'Done'\n\n"
                "Then try /deleteall again.",
                parse_mode="HTML"
            )
            return
    except Exception as e:
        log(f"Error checking admin status: {e}")
        await update.effective_message.reply_text(
            "⚠️ Couldn't verify permissions. Bot may need to be added as admin."
        )
        return

    n = DELETEALL_DEFAULT
    if ctx.args:
        try:
            n = max(1, min(2000, int(ctx.args[0])))
        except ValueError:
            pass

    notice = await update.effective_message.reply_text(
        f"🧹 Starting cleanup... (deleting up to {n} messages)"
    )
    
    start_id = notice.message_id
    deleted_count = 0
    failed_count = 0

    # Delete messages going backwards from current message
    for mid in range(start_id, max(1, start_id - n) - 1, -1):
        try:
            await ctx.bot.delete_message(chat_id=chat.id, message_id=mid)
            deleted_count += 1
            # Small delay to avoid rate limits
            if deleted_count % 20 == 0:
                await asyncio.sleep(0.5)
            else:
                await asyncio.sleep(0.05)
        except Forbidden:
            # Bot doesn't have permission
            failed_count += 1
            if failed_count > 10:
                # Too many permission errors, stop trying
                await ctx.bot.send_message(
                    chat_id=chat.id,
                    text=f"⚠️ Stopped after {deleted_count} messages. Bot may have lost admin permissions."
                )
                return
        except BadRequest:
            # Message doesn't exist or can't be deleted
            failed_count += 1
            if failed_count > 50:
                # Too many missing messages, probably reached the end
                break
        except Exception as e:
            log(f"Error deleting message {mid}: {e}")
            failed_count += 1

    # Send completion message
    try:
        completion_msg = await ctx.bot.send_message(
            chat_id=chat.id,
            text=f"✅ Cleanup complete! Deleted {deleted_count} messages."
        )
        # Auto-delete completion message after 10 seconds
        await asyncio.sleep(10)
        try:
            await ctx.bot.delete_message(chat_id=chat.id, message_id=completion_msg.message_id)
        except:
            pass
    except Exception as e:
        log(f"Error sending completion message: {e}")


async def reset_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Reset bot to default state (owner only)."""
    async with _state_lock:
        st = load_state()
        
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only.")
            return

    # Show confirmation buttons
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Reset Everything", callback_data="RESET:CONFIRM"),
            InlineKeyboardButton("❌ Cancel", callback_data="RESET:CANCEL")
        ]
    ])

    await update.effective_message.reply_text(
        "⚠️ <b>Reset Bot Data?</b>\n\n"
        "This will permanently delete:\n"
        "• Active load\n"
        "• Load history (all weeks)\n"
        "• Geocoding cache\n"
        "• All progress tracking\n\n"
        "<b>Keeps:</b>\n"
        "• Your ownership\n"
        "• Allowed groups\n"
        "• Your location\n\n"
        "Are you sure?",
        parse_mode="HTML",
        reply_markup=keyboard
    )


async def handle_reset_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, action: str):
    """Handle reset confirmation callbacks."""
    q = update.callback_query
    
    async with _state_lock:
        st = load_state()
        
        if not is_owner(update, st):
            await q.answer("Owner only.", show_alert=True)
            return

    if action == "CANCEL":
        await q.answer("Reset cancelled.", show_alert=False)
        try:
            await q.edit_message_text("❌ Reset cancelled. No data was deleted.")
        except TelegramError:
            pass
        return

    if action == "CONFIRM":
        await q.answer("Resetting bot...", show_alert=False)
        
        async with _state_lock:
            st2 = load_state()
            
            # Keep only essential data
            owner_id = st2.get("owner_id")
            allowed_chats = st2.get("allowed_chats", [])
            last_location = st2.get("last_location")
            
            # Create fresh state
            new_state = {
                "owner_id": owner_id,
                "owner": owner_id,
                "allowed_chats": allowed_chats,
                "allowed": allowed_chats,
                "last_location": last_location,
                "last": last_location,
                "job": None,
                "focus_i": 0,
                "geocode_cache": {},
                "gc": {},
                "history": [],
                "hist": [],
                "last_finished": None,
                "panel_messages": {}
            }
            
            save_state(new_state)
        
        try:
            await q.edit_message_text(
                "✅ <b>Bot Reset Complete!</b>\n\n"
                "Deleted:\n"
                "• All load history\n"
                "• Active load\n"
                "• Geocoding cache\n"
                "• Progress tracking\n\n"
                "Kept:\n"
                "• Your ownership\n"
                "• Allowed groups\n"
                "• Your location\n\n"
                "Bot is ready for new loads!",
                parse_mode="HTML"
            )
        except TelegramError:
            pass


async def leave_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Leave a group chat."""
    async with _state_lock:
        st = load_state()
        
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only.")
            return

        chat = update.effective_chat
        if not chat or chat.type == "private":
            await update.effective_message.reply_text(
                "Run /leave inside the group you want the bot to leave."
            )
            return

        allowed = set(st.get("allowed_chats") or [])
        allowed.discard(chat.id)
        st["allowed_chats"] = sorted(list(allowed))
        save_state(st)

    await update.effective_message.reply_text("👋 Leaving this chat…")
    
    try:
        await ctx.bot.leave_chat(chat.id)
    except Exception as e:
        log(f"Failed to leave chat: {e}")


# ============================================================================
# DOCUMENT (PDF) HANDLING
# ============================================================================

def get_load_key(job: Optional[dict]) -> Optional[str]:
    """Get a unique key for the load (load_number preferred, else job_id)."""
    if not job:
        return None
    meta = job.get("meta") or {}
    return meta.get("load_number") or job.get("id")


def build_doc_type_keyboard(file_unique_id: str) -> InlineKeyboardMarkup:
    """Build keyboard for document type selection."""
    return InlineKeyboardMarkup([
        [
            b("📄 BOL", f"DOC_SAVE:BOL:{file_unique_id}"),
            b("📋 POD", f"DOC_SAVE:POD:{file_unique_id}"),
        ],
        [
            b("💰 Rate Con", f"DOC_SAVE:RATE:{file_unique_id}"),
            b("📁 Other", f"DOC_SAVE:OTHER:{file_unique_id}"),
        ],
        [
            b("❌ Don't Save", f"DOC_SAVE:CANCEL:{file_unique_id}"),
        ]
    ])


async def on_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle document uploads (PDFs)."""
    msg = update.effective_message
    if not msg or not msg.document:
        return

    doc = msg.document
    
    # Only process PDFs
    if doc.mime_type != "application/pdf":
        return

    async with _state_lock:
        st = load_state()

    # Check if chat is allowed
    if not chat_allowed(update, st):
        return

    # Check if there's an active load
    job = normalize_job(st.get("job"))
    if not job:
        # No active load - ignore silently or notify
        return

    load_key = get_load_key(job)
    if not load_key:
        return

    # Store file info temporarily in context for the callback
    pending_docs = ctx.bot_data.setdefault("pending_docs", {})
    pending_docs[doc.file_unique_id] = {
        "file_id": doc.file_id,
        "file_unique_id": doc.file_unique_id,
        "filename": doc.file_name or "document.pdf",
        "load_key": load_key,
        "chat_id": update.effective_chat.id,
        "timestamp": now_iso(),
    }

    load_label = load_id_text(job)
    await msg.reply_text(
        f"📎 <b>PDF Detected</b>\n"
        f"<code>{h(doc.file_name or 'document.pdf')}</code>\n\n"
        f"Save this document for <b>{h(load_label)}</b>?\n"
        f"Select document type:",
        parse_mode="HTML",
        reply_markup=build_doc_type_keyboard(doc.file_unique_id),
    )


async def handle_doc_save_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, doc_type: str, file_unique_id: str):
    """Handle document save callbacks."""
    q = update.callback_query

    # Get pending doc info
    pending_docs = ctx.bot_data.get("pending_docs", {})
    doc_info = pending_docs.get(file_unique_id)

    if not doc_info:
        await q.answer("Document expired. Please re-upload.", show_alert=True)
        try:
            await q.edit_message_text("❌ Document expired. Please upload again.")
        except TelegramError:
            pass
        return

    if doc_type == "CANCEL":
        await q.answer("Document not saved.", show_alert=False)
        try:
            await q.edit_message_text("❌ Document not saved.")
        except TelegramError:
            pass
        # Clean up
        pending_docs.pop(file_unique_id, None)
        return

    # Map type to friendly name
    type_names = {
        "BOL": "Bill of Lading",
        "POD": "Proof of Delivery", 
        "RATE": "Rate Confirmation",
        "OTHER": "Other Document",
    }
    type_name = type_names.get(doc_type, doc_type)

    # Save to state
    async with _state_lock:
        st = load_state()
        docs = st.get("documents") or {}
        load_key = doc_info["load_key"]
        
        if load_key not in docs:
            docs[load_key] = []
        
        docs[load_key].append({
            "file_id": doc_info["file_id"],
            "type": doc_type,
            "type_name": type_name,
            "filename": doc_info["filename"],
            "timestamp": doc_info["timestamp"],
            "chat_id": doc_info["chat_id"],
        })
        
        st["documents"] = docs
        save_state(st)

    await q.answer(f"✅ Saved as {type_name}", show_alert=False)
    try:
        await q.edit_message_text(
            f"✅ <b>Document Saved</b>\n"
            f"<code>{h(doc_info['filename'])}</code>\n"
            f"Type: <b>{h(type_name)}</b>\n"
            f"Load: <b>{h(load_key)}</b>\n\n"
            f"Use /docs to view saved documents.",
            parse_mode="HTML"
        )
    except TelegramError:
        pass

    # Clean up
    pending_docs.pop(file_unique_id, None)


async def docs_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /docs command - list and retrieve documents."""
    async with _state_lock:
        st = load_state()

    if not is_owner(update, st):
        await update.effective_message.reply_text("Owner only.")
        return

    if not chat_allowed(update, st):
        await update.effective_message.reply_text("This chat isn't allowed.")
        return

    all_docs = st.get("documents") or {}
    
    # Check for argument (specific load number)
    args = ctx.args or []
    
    if args:
        # Looking for specific load
        search = " ".join(args).strip()
        
        # Try exact match first
        if search in all_docs:
            load_key = search
        else:
            # Try partial match
            matches = [k for k in all_docs.keys() if search.lower() in k.lower()]
            if len(matches) == 1:
                load_key = matches[0]
            elif len(matches) > 1:
                await update.effective_message.reply_text(
                    f"Multiple loads match '{h(search)}':\n" + 
                    "\n".join(f"• {h(m)}" for m in matches[:10]) +
                    "\n\nBe more specific.",
                    parse_mode="HTML"
                )
                return
            else:
                await update.effective_message.reply_text(f"No documents found for '{h(search)}'.", parse_mode="HTML")
                return
        
        # Send documents for this load
        docs = all_docs.get(load_key, [])
        if not docs:
            await update.effective_message.reply_text(f"No documents for {h(load_key)}.", parse_mode="HTML")
            return

        await update.effective_message.reply_text(
            f"📁 <b>Documents for {h(load_key)}</b>\n"
            f"Sending {len(docs)} document(s)...",
            parse_mode="HTML"
        )

        for doc in docs:
            try:
                await ctx.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=doc["file_id"],
                    caption=f"📄 {doc.get('type_name', doc['type'])} — {load_key}",
                )
            except TelegramError as e:
                log(f"Failed to send document: {e}")
                await update.effective_message.reply_text(
                    f"⚠️ Could not retrieve: {h(doc['filename'])}",
                    parse_mode="HTML"
                )
        return

    # No argument - show current load docs or list of loads with docs
    job = normalize_job(st.get("job"))
    
    if job:
        load_key = get_load_key(job)
        docs = all_docs.get(load_key, []) if load_key else []
        
        if docs:
            lines = [f"📁 <b>Documents for {h(load_key)}</b> (current load)\n"]
            for i, doc in enumerate(docs, 1):
                lines.append(f"{i}. <b>{h(doc.get('type_name', doc['type']))}</b> — {h(doc['filename'])}")
            
            lines.append(f"\nUse /docs {load_key} to download all.")
            await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML")
            return
    
    # Show list of loads with documents
    if not all_docs:
        await update.effective_message.reply_text(
            "📁 <b>No documents saved yet.</b>\n\n"
            "Upload a PDF while a load is active to save it.",
            parse_mode="HTML"
        )
        return

    lines = ["📁 <b>Saved Documents</b>\n"]
    for load_key, docs in sorted(all_docs.items(), reverse=True)[:20]:
        doc_types = ", ".join(set(d.get("type", "?") for d in docs))
        lines.append(f"• <b>{h(load_key)}</b>: {len(docs)} doc(s) ({h(doc_types)})")
    
    if len(all_docs) > 20:
        lines.append(f"\n... and {len(all_docs) - 20} more loads")
    
    lines.append("\nUse /docs &lt;load#&gt; to download documents.")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML")


# ============================================================================
# CALLBACK QUERY HANDLER
# ============================================================================

async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks."""
    q = update.callback_query
    if not q or not q.data:
        return

    data = q.data

    async with _state_lock:
        st = load_state()

    if not chat_allowed(update, st):
        await q.answer("Not allowed here.", show_alert=False)
        return

    # Reset confirmation callbacks
    if data.startswith("RESET:"):
        action = data.split(":", 1)[1]
        await handle_reset_callback(update, ctx, action)
        return

    # Document save callbacks
    if data.startswith("DOC_SAVE:"):
        parts = data.split(":", 2)
        if len(parts) == 3:
            doc_type = parts[1]
            file_unique_id = parts[2]
            await handle_doc_save_callback(update, ctx, doc_type, file_unique_id)
        return

    # ETA requests
    if data.startswith("ETA:"):
        await q.answer("Computing ETA…", show_alert=False)
        await send_eta(update, ctx, data.split(":", 1)[1])
        return

    if data == "SHOW:CAT":
        await send_catalog(update, ctx, from_callback=True)
        return

    if data == "JOB:FIN":
        if not is_owner(update, st):
            await q.answer("Owner only. DM /claim <code>.", show_alert=True)
            return

        await q.answer("Finishing…", show_alert=False)
        out = await finish_active_load(update, ctx, source="callback")
        if not out:
            return
        
        rec, _ = out

        rate_txt = money(rec.get("rate") if isinstance(rec.get("rate"), (int, float)) else None)
        id_txt = rec.get("load_number") or rec.get("job_id") or ""
        
        await send_progress_alert(
            ctx, 
            update.effective_chat.id, 
            f"✅ <b>Load finished</b> {h(id_txt)} · {h(rate_txt)}"
        )

        wk_count = int(rec.get("_wk_count") or 0)
        wk_rate = float(rec.get("_wk_rate") or 0.0)
        wk_miles = float(rec.get("_wk_miles") or 0.0)

        report = "\n".join([
            f"✅ <b>Load finished</b>",
            f"{h(id_txt)} · {h(rate_txt)}",
            f"Week {h(rec.get('week'))}: {h(wk_count)} loads · {h(money(wk_rate))} · {h(int(round(wk_miles)))} mi",
            "📊 Tap Catalog for Excel.",
        ])

        try:
            await q.edit_message_text(
                text=report, 
                parse_mode="HTML", 
                reply_markup=build_finished_keyboard()
            )
        except TelegramError:
            try:
                await q.edit_message_reply_markup(reply_markup=build_finished_keyboard())
            except TelegramError:
                pass
        return

    async with _state_lock:
        st2 = load_state()
        job = normalize_job(st2.get("job"))
        
        if not job:
            await q.answer("No active load.", show_alert=True)
            try:
                await q.edit_message_reply_markup(reply_markup=build_finished_keyboard())
            except TelegramError:
                pass
            return

        stage, i = focus(job, st2)
        tz_name = ((st2.get("last_location") or {}).get("tz")) or "UTC"
        ts = local_stamp(tz_name)
        load_label = load_id_text(job)

        progress_broadcast: Optional[str] = None

        if data.startswith("PU:"):
            ps = job["pu"]["status"]
            
            if data == "PU:A":
                on = toggle_ts(ps, "arr")
                if on:
                    progress_broadcast = f"📍 <b>PU Arrived</b> — {h(ts)} — {h(load_label)}"
            elif data == "PU:L":
                on = toggle_ts(ps, "load")
                if on:
                    progress_broadcast = f"📦 <b>Loaded</b> — {h(ts)} — {h(load_label)}"
            elif data == "PU:D":
                on = toggle_ts(ps, "dep")
                if on:
                    progress_broadcast = f"🚚 <b>Departed PU</b> — {h(ts)} — {h(load_label)}"
            elif data == "PU:C":
                on = toggle_ts(ps, "comp")
                if on:
                    progress_broadcast = f"✅ <b>PU COMPLETE</b> — {h(ts)} — {h(load_label)}"
                    ni = next_incomplete(job, 0)
                    if ni is not None:
                        st2["focus_i"] = ni

        elif data.startswith("DEL:"):
            if stage != "DEL":
                await q.answer("Complete PU first.", show_alert=False)
                return

            dels = job.get("del") or []
            if not dels:
                await q.answer("No deliveries.", show_alert=False)
                return

            dd = dels[i]
            ds = dd.get("status") or {}
            lbl = f"DEL {i+1}/{len(dels)}"

            if data == "DEL:A":
                on = toggle_ts(ds, "arr")
                if on:
                    progress_broadcast = f"📍 <b>Arrived {h(lbl)}</b> — {h(ts)} — {h(load_label)}"
            elif data == "DEL:DL":
                on = toggle_ts(ds, "del")
                if on:
                    progress_broadcast = f"📦 <b>Delivered {h(lbl)}</b> — {h(ts)} — {h(load_label)}"
            elif data == "DEL:D":
                on = toggle_ts(ds, "dep")
                if on:
                    progress_broadcast = f"🚚 <b>Departed {h(lbl)}</b> — {h(ts)} — {h(load_label)}"
            elif data == "DEL:C":
                on = toggle_ts(ds, "comp")
                if on:
                    progress_broadcast = f"✅ <b>STOP COMPLETE {h(lbl)}</b> — {h(ts)} — {h(load_label)}"
                    ni = next_incomplete(job, i + 1)
                    if ni is not None:
                        st2["focus_i"] = ni
            elif data == "DEL:S":
                ds["skip"] = True
                if not ds.get("comp"):
                    ds["comp"] = now_iso()
                progress_broadcast = f"⏭️ <b>SKIPPED {h(lbl)}</b> — {h(ts)} — {h(load_label)}"
                ni = next_incomplete(job, i + 1)
                if ni is not None:
                    st2["focus_i"] = ni

            dd["status"] = ds
            dels[i] = dd
            job["del"] = dels

        elif data.startswith("DOC:"):
            if data == "DOC:PTI":
                job["pu"]["docs"]["pti"] = not bool(job["pu"]["docs"].get("pti"))
            elif data == "DOC:BOL":
                job["pu"]["docs"]["bol"] = not bool(job["pu"]["docs"].get("bol"))
            elif data == "DOC:POD":
                if stage != "DEL":
                    await q.answer("Complete PU first.", show_alert=False)
                    return
                
                dels = job.get("del") or []
                if not dels:
                    await q.answer("No deliveries.", show_alert=False)
                    return
                
                dels[i].setdefault("docs", {})
                dels[i]["docs"]["pod"] = not bool(dels[i]["docs"].get("pod"))
                job["del"] = dels

        st2["job"] = job
        save_state(st2)

    await q.answer("Updated.", show_alert=False)

    if progress_broadcast:
        await send_progress_alert(ctx, update.effective_chat.id, progress_broadcast)

    try:
        await q.edit_message_reply_markup(reply_markup=build_keyboard(job, st2))
    except TelegramError:
        pass


# ============================================================================
# TEXT MESSAGE HANDLER
# ============================================================================

async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle text messages (load detection and triggers)."""
    msg = update.effective_message
    if not msg or not msg.text:
        return

    async with _state_lock:
        st = load_state()

    chat = update.effective_chat

    if chat and chat.type in ("group", "supergroup"):
        if chat.id not in set(st.get("allowed_chats") or []):
            return
        
        job = parse_job(msg.text)
        if job:
            async with _state_lock:
                st2 = load_state()
                st2["job"] = job
                st2["focus_i"] = 0
                save_state(st2)
            
            await msg.reply_text("📦 New load detected. Type eta / 1717 or /panel.")
            return

    if not chat_allowed(update, st):
        return

    parts = msg.text.strip().split()
    if not parts:
        return

    first = re.sub(r"^[^\w]+|[^\w]+$", "", parts[0].lower())
    if first in TRIGGERS:
        rest = " ".join(parts[1:]).lower()
        which = "ALL" if "all" in rest else "AUTO"
        await send_eta(update, ctx, which)


# ============================================================================
# STARTUP HOOK
# ============================================================================

async def _post_init(app):
    """Post-initialization hook."""
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        log(f"delete_webhook failed: {e}")
    
    try:
        me = await app.bot.get_me()
        log(f"Connected as @{me.username} (id {me.id})")
    except Exception as e:
        log(f"get_me failed: {e}")
    
    log("Ready.")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main() -> None:
    """Main entry point."""
    if not TOKEN:
        raise RuntimeError("Missing TELEGRAM_TOKEN environment variable")

    builder = ApplicationBuilder().token(TOKEN)
    
    try:
        builder = builder.post_init(_post_init)
    except Exception as e:
        log(f"Failed to set post_init: {e}")

    app = builder.build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("ping", ping_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("claim", claim_cmd))
    app.add_handler(CommandHandler("allowhere", allowhere_cmd))
    app.add_handler(CommandHandler("update", update_cmd))
    app.add_handler(CommandHandler("panel", panel_cmd))
    app.add_handler(CommandHandler("finish", finish_cmd))
    app.add_handler(CommandHandler("catalog", catalog_cmd))
    app.add_handler(CommandHandler("skip", skip_cmd))
    app.add_handler(CommandHandler("reset", reset_cmd))
    app.add_handler(CommandHandler("deleteall", deleteall_cmd))
    app.add_handler(CommandHandler("leave", leave_cmd))
    app.add_handler(CommandHandler("docs", docs_cmd))

    app.add_handler(CallbackQueryHandler(on_callback))
    
    app.add_handler(MessageHandler(filters.LOCATION, on_location))
    try:
        app.add_handler(
            MessageHandler(
                filters.UpdateType.EDITED_MESSAGE & filters.LOCATION, 
                on_location
            )
        )
    except Exception as e:
        log(f"Failed to add edited location handler: {e}")

    # Document (PDF) handler
    app.add_handler(MessageHandler(filters.Document.PDF, on_document))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    log("Starting polling…")
    app.run_polling(
        drop_pending_updates=True, 
        allowed_updates=Update.ALL_TYPES, 
        close_loop=False
    )


if __name__ == "__main__":
    main()