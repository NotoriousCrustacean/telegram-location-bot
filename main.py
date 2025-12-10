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
from functools import wraps

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

BOT_VERSION = "2025-12-10_secure"

# ---------- ENV ----------
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CLAIM_CODE = os.environ.get("CLAIM_CODE", "").strip()

# If your service filesystem is read-only, saving state.json can crash the bot.
STATE_FILE = Path(os.environ.get("STATE_FILE", "state.json"))
STATE_FALLBACK = Path("/tmp/dispatch_bot_state.json")

TRIGGERS = {t.strip().lower() for t in os.environ.get("TRIGGERS", "eta,1717").split(",") if t.strip()}

NOMINATIM_USER_AGENT = os.environ.get("NOMINATIM_USER_AGENT", "dispatch-eta-bot/1.0").strip()
NOMINATIM_MIN_INTERVAL = float(os.environ.get("NOMINATIM_MIN_INTERVAL", "1.1"))

ETA_ALL_MAX = int(os.environ.get("ETA_ALL_MAX", "6"))
DELETEALL_DEFAULT = int(os.environ.get("DELETEALL_DEFAULT", "300"))
ALERT_TTL_SECONDS = int(os.environ.get("ALERT_TTL_SECONDS", "25"))
GEOCODE_CACHE_DAYS = int(os.environ.get("GEOCODE_CACHE_DAYS", "30"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
DEBUG = os.environ.get("DEBUG", "0").strip().lower() in ("1", "true", "yes", "on")

# Security limits
MAX_INPUT_LENGTH = 10000
MAX_ADDRESS_LENGTH = 500
COMMAND_RATE_LIMIT = 2.0  # seconds between commands per user

# ---------- GLOBALS ----------
TF = TimezoneFinder()
NOM_URL = "https://nominatim.openstreetmap.org/search"
OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"

_state_lock = asyncio.Lock()
_geo_lock = asyncio.Lock()
_geo_last = 0.0
_user_last_command: Dict[int, float] = {}


def log(msg: str, level: str = "INFO") -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}", flush=True)


def log_security(msg: str, user_id: Optional[int] = None) -> None:
    user_str = f" [user={user_id}]" if user_id else ""
    log(f"SECURITY{user_str}: {msg}", "WARN")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def safe_tz(name: str):
    """Get timezone, fallback to UTC if invalid."""
    try:
        return ZoneInfo(name)
    except Exception as e:
        if DEBUG:
            log(f"Invalid timezone '{name}': {e}", "WARN")
        return timezone.utc


def h(x: Any) -> str:
    """HTML escape for safe output."""
    return html.escape("" if x is None else str(x), quote=False)


def validate_lat_lon(lat: float, lon: float) -> bool:
    """Validate coordinate bounds."""
    return -90 <= lat <= 90 and -180 <= lon <= 180


def validate_rate(rate: Optional[float]) -> Optional[float]:
    """Validate and sanitize rate."""
    if rate is None:
        return None
    if rate < 0 or rate > 1000000:
        log(f"Invalid rate: {rate}", "WARN")
        return None
    return rate


def validate_miles(miles: Optional[int]) -> Optional[int]:
    """Validate and sanitize miles."""
    if miles is None:
        return None
    if miles < 0 or miles > 100000:
        log(f"Invalid miles: {miles}", "WARN")
        return None
    return miles


def truncate_input(text: str, max_len: int = MAX_INPUT_LENGTH) -> str:
    """Truncate input to prevent abuse."""
    if len(text) > max_len:
        log(f"Input truncated from {len(text)} to {max_len} chars", "WARN")
        return text[:max_len]
    return text


def local_stamp(tz_name: str) -> str:
    tz = safe_tz(tz_name or "UTC")
    return now_utc().astimezone(tz).strftime("%Y-%m-%d %H:%M")


# ---------- STATE MANAGEMENT ----------
def _migrate_state(st: dict) -> Tuple[dict, bool]:
    """Migrate old state format to new, returns (state, changed)."""
    changed = False

    # Owner migration
    if st.get("owner_id") is None and st.get("owner") is not None:
        st["owner_id"] = st.get("owner")
        changed = True

    # Allowed chats migration
    if not st.get("allowed_chats") and st.get("allowed"):
        st["allowed_chats"] = st.get("allowed")
        changed = True

    # Location migration
    if st.get("last_location") is None and st.get("last") is not None:
        ll = st.get("last") or {}
        st["last_location"] = {
            "lat": ll.get("lat"),
            "lon": ll.get("lon"),
            "tz": ll.get("tz"),
            "updated_at": ll.get("at") or ll.get("updated_at"),
        }
        changed = True

    # Geocode cache migration
    if not st.get("geocode_cache") and st.get("gc"):
        st["geocode_cache"] = st.get("gc")
        changed = True

    # History migration
    if not st.get("history") and st.get("hist"):
        st["history"] = st.get("hist")
        changed = True

    # Focus index migration
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

    return st, changed


def load_state() -> dict:
    """Load state from file with fallback."""
    global STATE_FILE

    if not STATE_FILE.exists() and STATE_FALLBACK.exists():
        STATE_FILE = STATE_FALLBACK

    if STATE_FILE.exists():
        try:
            st = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            log(f"Failed to load state: {e}", "ERROR")
            st = {}
    else:
        st = {}

    st, changed = _migrate_state(st)
    if changed:
        try:
            save_state(st)
        except Exception as e:
            log(f"Failed to save migrated state: {e}", "ERROR")
    
    return st


def save_state(st: dict) -> None:
    """Atomically save state to file."""
    global STATE_FILE

    def _write(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    try:
        _write(STATE_FILE)
        if DEBUG:
            log(f"State saved to {STATE_FILE}")
    except Exception as e:
        log(f"save_state failed at {STATE_FILE}: {e}. Falling back to {STATE_FALLBACK}", "ERROR")
        STATE_FILE = STATE_FALLBACK
        _write(STATE_FILE)


def atomic_state_update(func):
    """Decorator for atomic state updates."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        async with _state_lock:
            st = load_state()
            result = await func(st, *args, **kwargs)
            save_state(st)
            return result
    return wrapper


# ---------- AUTHORIZATION ----------
def is_owner(update: Update, st: dict) -> bool:
    """Check if user is the bot owner."""
    u = update.effective_user
    return bool(u and st.get("owner_id") and u.id == st["owner_id"])


def chat_allowed(update: Update, st: dict) -> bool:
    """Check if chat is authorized."""
    chat = update.effective_chat
    if not chat:
        return False
    if chat.type == "private":
        return is_owner(update, st)
    return chat.id in set(st.get("allowed_chats") or [])


def check_rate_limit(user_id: int) -> bool:
    """Check if user is rate limited."""
    now = time.monotonic()
    last = _user_last_command.get(user_id, 0)
    if now - last < COMMAND_RATE_LIMIT:
        return False
    _user_last_command[user_id] = now
    return True


def require_auth(func):
    """Decorator to require authorization."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        async with _state_lock:
            st = load_state()
        
        if not chat_allowed(update, st):
            user = update.effective_user
            chat = update.effective_chat
            log_security(
                f"Unauthorized access attempt from user {user.id if user else 'unknown'} "
                f"in chat {chat.id if chat else 'unknown'}",
                user.id if user else None
            )
            await update.message.reply_text("⛔ Unauthorized. This bot is private.")
            return
        
        # Rate limiting
        user = update.effective_user
        if user and not check_rate_limit(user.id):
            await update.message.reply_text("⏱️ Please wait before sending another command.")
            return
        
        return await func(update, context)
    return wrapper


# ---------- GEOCODING & ROUTING ----------
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
    sp = 55 if km < 80 else (85 if km < 320 else 105)
    return (km / sp) * 3600.0


def fmt_dur(seconds: float) -> str:
    """Format duration as hours and minutes."""
    seconds = max(0, int(seconds))
    m = seconds // 60
    h_ = m // 60
    m = m % 60
    return f"{h_}h {m}m" if h_ else f"{m}m"


def fmt_mi(meters: float) -> str:
    """Format distance in miles."""
    mi = meters / 1609.344
    return f"{mi:.1f} mi" if mi < 10 else f"{mi:.0f} mi"


def addr_variants(addr: str) -> List[str]:
    """Generate address variants for geocoding fallback."""
    a = " ".join((addr or "").split())
    if not a:
        return []
    
    out = [a]
    parts = [p.strip() for p in a.split(",") if p.strip()]
    
    if len(parts) >= 2:
        out.append(", ".join(parts[1:]))
    
    # Remove suite/unit numbers
    out.append(re.sub(r"\b(?:suite|ste|unit|#)\s*[\w\-]+\b", "", a, flags=re.I).strip())
    
    if len(parts) >= 2:
        out.append(", ".join(parts[-2:]))
    
    # Deduplicate
    seen, res = set(), []
    for x in out:
        x = " ".join(x.split())
        if x and x not in seen:
            seen.add(x)
            res.append(x)
    
    return res


def is_cache_expired(cached_at: Optional[str], days: int = GEOCODE_CACHE_DAYS) -> bool:
    """Check if cache entry is expired."""
    if not cached_at:
        return True
    try:
        cached = datetime.fromisoformat(cached_at)
        age = now_utc() - cached
        return age > timedelta(days=days)
    except Exception:
        return True


async def geocode_cached(st: dict, addr: str) -> Optional[Tuple[float, float, str]]:
    """Geocode address with caching and retry logic."""
    addr = truncate_input(addr, MAX_ADDRESS_LENGTH)
    
    cache = st.get("geocode_cache") or {}
    
    # Check cache
    if addr in cache and isinstance(cache[addr], dict):
        entry = cache[addr]
        if not is_cache_expired(entry.get("cached_at")):
            try:
                lat, lon = float(entry["lat"]), float(entry["lon"])
                if validate_lat_lon(lat, lon):
                    return lat, lon, entry.get("tz") or "UTC"
            except Exception as e:
                log(f"Invalid cache entry: {e}", "WARN")

    if not NOMINATIM_USER_AGENT:
        return None

    headers = {"User-Agent": NOMINATIM_USER_AGENT}
    
    # Retry logic with exponential backoff
    for attempt in range(MAX_RETRIES):
        try:
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
                    
                    # Validate coordinates
                    if not validate_lat_lon(lat, lon):
                        log(f"Invalid coordinates from geocoder: {lat}, {lon}", "WARN")
                        continue
                    
                    tz = TF.timezone_at(lat=lat, lng=lon) or "UTC"
                    
                    # Update cache
                    cache[addr] = {
                        "lat": lat,
                        "lon": lon,
                        "tz": tz,
                        "cached_at": now_iso()
                    }
                    st["geocode_cache"] = cache

                    # Persist cache
                    async with _state_lock:
                        st2 = load_state()
                        st2.setdefault("geocode_cache", {})
                        st2["geocode_cache"][addr] = cache[addr]
                        save_state(st2)

                    return lat, lon, tz
            
            # If we got here, no results found
            return None
            
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt
                log(f"Geocoding attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s", "WARN")
                await asyncio.sleep(wait_time)
            else:
                log(f"Geocoding failed after {MAX_RETRIES} attempts: {e}", "ERROR")
                return None

    return None


async def route(origin: Tuple[float, float], dest: Tuple[float, float]) -> Optional[Tuple[float, float]]:
    """Get route distance and duration with retry logic."""
    url = OSRM_URL.format(lon1=origin[1], lat1=origin[0], lon2=dest[1], lat2=dest[0])
    
    for attempt in range(MAX_RETRIES):
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(url, params={"overview": "false"})
                
                if r.status_code >= 400:
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return None
                
                js = r.json() or {}
                routes = js.get("routes") or []
                if not routes:
                    return None
                
                distance = float(routes[0]["distance"])
                duration = float(routes[0]["duration"])
                
                # Sanity check
                if distance < 0 or duration < 0:
                    log(f"Invalid route data: distance={distance}, duration={duration}", "WARN")
                    return None
                
                return distance, duration
                
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                log(f"Routing attempt {attempt + 1} failed: {e}. Retrying...", "WARN")
                await asyncio.sleep(2 ** attempt)
            else:
                log(f"Routing failed after {MAX_RETRIES} attempts: {e}", "ERROR")
                return None
    
    return None


async def eta_to(st: dict, origin: Tuple[float, float], label: str, addr: str) -> dict:
    """Calculate ETA to destination."""
    g = await geocode_cached(st, addr)
    if not g:
        return {"ok": False, "err": f"Couldn't locate {label}."}
    
    dest = (g[0], g[1])
    r = await route(origin, dest)
    
    if r:
        return {
            "ok": True,
            "m": r[0],
            "s": r[1],
            "method": "osrm",
            "tz": g[2]
        }
    
    # Fallback to haversine
    dist = hav_m(origin[0], origin[1], dest[0], dest[1])
    return {
        "ok": True,
        "m": dist,
        "s": fallback_seconds(dist),
        "method": "approx",
        "tz": g[2]
    }


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
    """Extract rate and miles from load text."""
    rate = None
    miles = None
    
    m = RATE_RE.search(text)
    if m:
        try:
            rate = float(m.group(1).replace(",", ""))
            rate = validate_rate(rate)
        except Exception as e:
            log(f"Failed to parse rate: {e}", "WARN")
    
    m = MILES_RE.search(text)
    if m:
        try:
            miles = int(m.group(1).replace(",", ""))
            miles = validate_miles(miles)
        except Exception as e:
            log(f"Failed to parse miles: {e}", "WARN")
    
    return rate, miles


def take_block(lines: List[str], i: int, first: str) -> Tuple[List[str], int]:
    """Extract multi-line address block."""
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
    """Initialize job with default structure."""
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
    """Normalize job from various formats."""
    if not job or not isinstance(job, dict):
        return None

    if "pu" in job and "del" in job:
        return init_job(job)

    # Legacy format support
    if "pickup" in job and "deliveries" in job:
        pu = job.get("pickup") or {}
        dels = job.get("deliveries") or []
     
