"""
Telegram Trucker Dispatch Assistant Bot
Version: 2025-12-11_v4 (Multi-Geocoder Edition)

Features:
- Multiple geocoding services (Nominatim + Photon fallback)
- Enhanced address parsing and normalization
- Detailed debug logging
- Fixed deleteall command
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
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

import httpx
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
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
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

BOT_VERSION = "2025-12-11_v4"

# ============================================================================
# CONFIGURATION
# ============================================================================
def _strip_quotes(s: str) -> str:
    s = (s or "").strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        return s[1:-1].strip()
    return s

def env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return _strip_quotes(v) if v else default

def env_int(name: str, default: int) -> int:
    try: return int(env_str(name, ""))
    except: return default

def env_float(name: str, default: float) -> float:
    try: return float(env_str(name, ""))
    except: return default

def env_bool(name: str, default: bool = False) -> bool:
    return env_str(name, "").lower() in ("1", "true", "yes", "on")

def env_list_int(name: str, default: List[int]) -> List[int]:
    v = env_str(name, "")
    if not v: return default
    try: return [int(x.strip()) for x in v.split(",") if x.strip()]
    except: return default

TOKEN = env_str("TELEGRAM_TOKEN")
CLAIM_CODE = env_str("CLAIM_CODE")
STATE_FILE = Path(env_str("STATE_FILE", "state.json"))
TRIGGERS = {t.strip().lower() for t in env_str("TRIGGERS", "eta,1717").split(",") if t.strip()}

NOMINATIM_USER_AGENT = env_str("NOMINATIM_USER_AGENT", "dispatch-bot/1.0")
NOMINATIM_MIN_INTERVAL = env_float("NOMINATIM_MIN_INTERVAL", 1.2)

ETA_ALL_MAX = env_int("ETA_ALL_MAX", 6)
ALERT_TTL_SECONDS = env_int("ALERT_TTL_SECONDS", 25)
DELETEALL_DEFAULT = env_int("DELETEALL_DEFAULT", 300)

GEOFENCE_MILES = env_float("GEOFENCE_MILES", 5.0)
REMINDER_DOC_AFTER_MIN = env_int("REMINDER_DOC_AFTER_MIN", 15)
REMINDER_THRESHOLDS_MIN = env_list_int("REMINDER_THRESHOLDS_MIN", [60, 30, 10])
SCHEDULE_GRACE_MIN = env_int("SCHEDULE_GRACE_MIN", 30)

DEBUG = env_bool("DEBUG", True)

def log(msg: str):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)

def log_debug(msg: str):
    if DEBUG:
        log(f"DEBUG: {msg}")

def log_error(msg: str, exc: Exception = None):
    log(f"ERROR: {msg}")
    if exc:
        log(f"  {type(exc).__name__}: {exc}")
        if DEBUG:
            traceback.print_exc()

# ============================================================================
# GLOBALS
# ============================================================================
TF = TimezoneFinder()

# Multiple geocoding endpoints for fallback
GEOCODE_SERVICES = [
    {
        "name": "Nominatim",
        "url": "https://nominatim.openstreetmap.org/search",
        "params": lambda q: {"q": q, "format": "jsonv2", "limit": 1, "countrycodes": "us"},
        "parse": lambda data: (float(data[0]["lat"]), float(data[0]["lon"])) if data else None,
    },
    {
        "name": "Photon",
        "url": "https://photon.komoot.io/api/",
        "params": lambda q: {"q": q, "limit": 1, "lang": "en"},
        "parse": lambda data: (
            data["features"][0]["geometry"]["coordinates"][1],
            data["features"][0]["geometry"]["coordinates"][0]
        ) if data.get("features") else None,
    },
]

OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"

_state_lock = asyncio.Lock()
_geo_lock = asyncio.Lock()
_geo_last = 0.0
_tasks: Set[asyncio.Task] = set()

METERS_PER_MILE = 1609.344

# ============================================================================
# HELPERS
# ============================================================================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_iso() -> str:
    return now_utc().isoformat()

def safe_tz(name: str):
    try: return ZoneInfo(name) if name else timezone.utc
    except: return timezone.utc

def h(x: Any) -> str:
    return html.escape(str(x) if x is not None else "", quote=False)

def money(x) -> str:
    try: return f"${float(x):,.0f}" if x is not None else "-"
    except: return str(x) if x else "-"

def fmt_dur(secs: float) -> str:
    secs = max(0, int(secs))
    hours, mins = divmod(secs // 60, 60)
    return f"{hours}h {mins}m" if hours else f"{mins}m"

def fmt_mi(meters: float) -> str:
    mi = meters / METERS_PER_MILE
    return f"{mi:.1f} mi" if mi < 10 else f"{mi:.0f} mi"

def week_key(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

def local_stamp(tz_name: str) -> str:
    return now_utc().astimezone(safe_tz(tz_name)).strftime("%Y-%m-%d %H:%M")

# ============================================================================
# STATE MANAGEMENT
# ============================================================================
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            st = json.loads(STATE_FILE.read_text())
            if isinstance(st, dict):
                return st
        except Exception as e:
            log_error(f"Failed to load state", e)
    return {}

def save_state(st: dict):
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, indent=2))
        tmp.replace(STATE_FILE)
    except Exception as e:
        log_error("Failed to save state", e)

def is_owner(update: Update, st: dict) -> bool:
    u = update.effective_user
    return bool(u and st.get("owner_id") and u.id == st["owner_id"])

def chat_allowed(update: Update, st: dict) -> bool:
    chat = update.effective_chat
    if not chat: return False
    if chat.type == "private": return is_owner(update, st)
    return chat.id in (st.get("allowed_chats") or [])

def get_broadcast_chats(st: dict) -> List[int]:
    chats = list(st.get("allowed_chats") or [])
    if st.get("owner_id") and st["owner_id"] not in chats:
        chats.append(st["owner_id"])
    return chats

# ============================================================================
# ADDRESS NORMALIZATION
# ============================================================================
def normalize_address(addr: str) -> str:
    """Normalize address for better geocoding."""
    if not addr:
        return ""
    
    # Uppercase for consistency
    result = addr.upper().strip()
    
    # Remove company names / labels before the actual address
    # Pattern: "COMPANY NAME, 123 Street..." or "COMPANY NAME - 123 Street..."
    # Keep everything from the first number onward
    num_match = re.search(r'(\d+\s+.+)', result)
    if num_match:
        result = num_match.group(1)
    
    # Normalize street types
    replacements = [
        (r'\bSTREET\b', 'ST'),
        (r'\bAVENUE\b', 'AVE'),
        (r'\bBOULEVARD\b', 'BLVD'),
        (r'\bDRIVE\b', 'DR'),
        (r'\bROAD\b', 'RD'),
        (r'\bLANE\b', 'LN'),
        (r'\bCOURT\b', 'CT'),
        (r'\bPLACE\b', 'PL'),
        (r'\bCIRCLE\b', 'CIR'),
        (r'\bHIGHWAY\b', 'HWY'),
        (r'\bPARKWAY\b', 'PKWY'),
        (r'\bCENTER\b', 'CTR'),
        (r'\bNORTH\b', 'N'),
        (r'\bSOUTH\b', 'S'),
        (r'\bEAST\b', 'E'),
        (r'\bWEST\b', 'W'),
    ]
    
    for pattern, repl in replacements:
        result = re.sub(pattern, repl, result)
    
    # Clean up multiple spaces/commas
    result = re.sub(r'\s+', ' ', result)
    result = re.sub(r',\s*,', ',', result)
    result = result.strip(' ,')
    
    return result

def extract_address_components(addr: str) -> dict:
    """Extract street, city, state, zip from address."""
    components = {
        "street": None,
        "city": None,
        "state": None,
        "zip": None,
        "original": addr,
    }
    
    if not addr:
        return components
    
    # Find ZIP code
    zip_match = re.search(r'\b(\d{5})(?:-\d{4})?\b', addr)
    if zip_match:
        components["zip"] = zip_match.group(1)
    
    # Find state (2-letter code)
    state_match = re.search(r'\b([A-Z]{2})\b(?:\s+\d{5})?(?:\s*,?\s*(?:USA|US)?)?$', addr.upper())
    if state_match:
        components["state"] = state_match.group(1)
    
    # Find street number + name
    street_match = re.search(r'(\d+\s+[^,]+?)(?:,|\s+[A-Z]{2}\b)', addr, re.I)
    if street_match:
        components["street"] = street_match.group(1).strip()
    
    # Find city (usually before state)
    if components["state"]:
        city_match = re.search(r'([A-Za-z\s]+),?\s*' + components["state"], addr, re.I)
        if city_match:
            city = city_match.group(1).strip()
            # Make sure it's not the street
            if city and components["street"] and city not in components["street"]:
                components["city"] = city
            elif city and not components["street"]:
                components["city"] = city
    
    return components

def generate_address_variants(addr: str) -> List[str]:
    """Generate multiple address variants for geocoding attempts."""
    if not addr:
        return []
    
    variants = []
    
    # Normalize the address first
    normalized = normalize_address(addr)
    components = extract_address_components(normalized)
    
    log_debug(f"Address components: {components}")
    
    # Build variants from most specific to least
    
    # 1. Full normalized address
    if normalized:
        variants.append(normalized)
        variants.append(f"{normalized}, USA")
    
    # 2. Street + City + State + ZIP
    if all([components["street"], components["city"], components["state"]]):
        full = f"{components['street']}, {components['city']}, {components['state']}"
        variants.append(full)
        variants.append(f"{full}, USA")
        if components["zip"]:
            variants.append(f"{full} {components['zip']}")
    
    # 3. Street + City + State (without zip)
    if components["street"] and components["state"]:
        variants.append(f"{components['street']}, {components['state']}")
        variants.append(f"{components['street']}, {components['state']}, USA")
    
    # 4. City + State
    if components["city"] and components["state"]:
        variants.append(f"{components['city']}, {components['state']}")
        variants.append(f"{components['city']}, {components['state']}, USA")
    
    # 5. ZIP code alone (very reliable)
    if components["zip"]:
        variants.append(components["zip"])
    
    # 6. Try original with USA
    if addr and "USA" not in addr.upper():
        variants.append(f"{addr}, USA")
    
    # Remove duplicates while preserving order
    seen = set()
    unique = []
    for v in variants:
        v_clean = " ".join(v.split()).strip()
        v_key = v_clean.lower()
        if v_key and v_key not in seen and len(v_clean) >= 3:
            seen.add(v_key)
            unique.append(v_clean)
    
    log_debug(f"Generated {len(unique)} address variants")
    for i, v in enumerate(unique[:5]):
        log_debug(f"  Variant {i+1}: {v}")
    
    return unique

# ============================================================================
# MULTI-SERVICE GEOCODING
# ============================================================================
def haversine_miles(lat1, lon1, lat2, lon2) -> float:
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def haversine_meters(lat1, lon1, lat2, lon2) -> float:
    return haversine_miles(lat1, lon1, lat2, lon2) * METERS_PER_MILE

async def geocode_with_service(client: httpx.AsyncClient, service: dict, query: str) -> Optional[Tuple[float, float]]:
    """Try geocoding with a specific service."""
    try:
        r = await client.get(service["url"], params=service["params"](query))
        log_debug(f"  {service['name']}: HTTP {r.status_code}")
        
        if r.status_code == 200:
            data = r.json()
            result = service["parse"](data)
            if result:
                log_debug(f"  {service['name']}: Found {result[0]:.4f}, {result[1]:.4f}")
                return result
            else:
                log_debug(f"  {service['name']}: No results in response")
        elif r.status_code == 429:
            log(f"  {service['name']}: Rate limited (429)")
        else:
            log_debug(f"  {service['name']}: HTTP {r.status_code}")
            
    except httpx.TimeoutException:
        log(f"  {service['name']}: Timeout")
    except Exception as e:
        log_error(f"  {service['name']}: Error", e)
    
    return None

async def geocode(addr: str, cache: dict) -> Optional[Tuple[float, float, str]]:
    """Geocode with multiple services and fallbacks."""
    if not addr:
        log_debug("Geocode: Empty address")
        return None
    
    # Check cache first
    cache_key = addr.lower().strip()
    if cache_key in cache:
        c = cache[cache_key]
        log_debug(f"Geocode: Cache hit")
        return c["lat"], c["lon"], c.get("tz", "UTC")
    
    variants = generate_address_variants(addr)
    if not variants:
        log("Geocode: No variants generated")
        return None
    
    headers = {
        "User-Agent": NOMINATIM_USER_AGENT or "DispatchBot/1.0",
        "Accept": "application/json",
    }
    
    async with httpx.AsyncClient(timeout=20, headers=headers, follow_redirects=True) as client:
        for variant in variants[:6]:  # Try up to 6 variants
            log_debug(f"Geocode trying: {variant}")
            
            # Rate limiting
            async with _geo_lock:
                global _geo_last
                wait = _geo_last + NOMINATIM_MIN_INTERVAL - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                _geo_last = time.monotonic()
            
            # Try each geocoding service
            for service in GEOCODE_SERVICES:
                result = await geocode_with_service(client, service, variant)
                
                if result:
                    lat, lon = result
                    tz = TF.timezone_at(lat=lat, lng=lon) or "UTC"
                    
                    # Cache the result
                    cache[cache_key] = {"lat": lat, "lon": lon, "tz": tz}
                    
                    log(f"Geocode SUCCESS via {service['name']}: {lat:.4f}, {lon:.4f}")
                    return lat, lon, tz
                
                # Small delay between services
                await asyncio.sleep(0.3)
    
    log(f"Geocode FAILED: All variants and services exhausted for '{addr[:40]}'")
    return None

async def get_route(origin: Tuple[float, float], dest: Tuple[float, float]) -> Optional[Tuple[float, float]]:
    """Get driving route from OSRM."""
    url = OSRM_URL.format(lat1=origin[0], lon1=origin[1], lat2=dest[0], lon2=dest[1])
    
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, params={"overview": "false"})
            if r.status_code == 200:
                data = r.json()
                if data.get("code") == "Ok" and data.get("routes"):
                    rt = data["routes"][0]
                    log_debug(f"Route: {rt['distance']/1609:.1f} mi, {rt['duration']/60:.0f} min")
                    return rt["distance"], rt["duration"]
    except Exception as e:
        log_error("Route error", e)
    
    return None

async def calc_eta(st: dict, origin: Tuple[float, float], addr: str) -> dict:
    """Calculate ETA to address."""
    if not addr:
        return {"ok": False, "err": "No address"}
    
    cache = st.setdefault("geocode_cache", {})
    geo = await geocode(addr, cache)
    
    if not geo:
        return {"ok": False, "err": f"Could not locate address"}
    
    dest = (geo[0], geo[1])
    route = await get_route(origin, dest)
    
    if route:
        return {
            "ok": True,
            "meters": route[0],
            "seconds": route[1],
            "tz": geo[2],
            "method": "route",
        }
    
    # Fallback to haversine
    dist_m = haversine_meters(origin[0], origin[1], dest[0], dest[1])
    dist_mi = dist_m / METERS_PER_MILE
    
    speed_mph = 35 if dist_mi < 50 else (50 if dist_mi < 150 else 60)
    est_secs = (dist_mi / speed_mph) * 3600 * 1.2
    
    return {
        "ok": True,
        "meters": dist_m,
        "seconds": est_secs,
        "tz": geo[2],
        "method": "estimate",
    }

# ============================================================================
# LOAD PARSING
# ============================================================================
LOAD_NUM_PATTERN = re.compile(r"Load\s*#\s*(\S+)", re.I)
RATE_PATTERN = re.compile(r"Rate\s*:\s*\$?([\d,]+(?:\.\d{2})?)", re.I)
MILES_PATTERN = re.compile(r"Total\s*mi\s*:\s*([\d,]+)", re.I)
PU_TIME_PATTERN = re.compile(r"PU\s*time\s*:\s*(.+?)(?:\n|$)", re.I)
DEL_TIME_PATTERN = re.compile(r"DEL\s*time\s*:\s*(.+?)(?:\n|$)", re.I)
PU_ADDR_PATTERN = re.compile(r"PU\s*Address\s*:\s*(.+?)(?=\n\s*\n|\nDEL|\n-{3,}|$)", re.I | re.S)
DEL_ADDR_PATTERN = re.compile(r"DEL\s*Address\s*:\s*(.+?)(?=\n\s*\n|\n-{3,}|\nTotal|$)", re.I | re.S)

def clean_address(addr: str) -> str:
    """Clean address from load sheet."""
    if not addr:
        return ""
    
    lines = []
    for ln in addr.strip().split("\n"):
        ln = ln.strip()
        if not ln or len(ln) < 2:
            continue
        
        ln_lower = ln.lower()
        if any(skip in ln_lower for skip in ["---", "===", "total mi", "rate :", "trailer", "failure"]):
            break
        
        lines.append(ln)
    
    if not lines:
        return ""
    
    # Join lines, preserving structure
    # First line might be company name, rest is address
    result = ", ".join(lines)
    
    # Clean up
    result = re.sub(r'\s+', ' ', result)
    result = re.sub(r',\s*,', ',', result)
    
    return result.strip()

def parse_load(text: str) -> Optional[dict]:
    """Parse load from dispatcher message."""
    
    if "pu address" not in text.lower() or "del address" not in text.lower():
        return None
    
    log("Parsing load...")
    
    load_num = None
    m = LOAD_NUM_PATTERN.search(text)
    if m:
        load_num = m.group(1).strip()
        log(f"  Load #: {load_num}")
    
    rate = None
    m = RATE_PATTERN.search(text)
    if m:
        try:
            rate = float(m.group(1).replace(",", ""))
        except: pass
    
    miles = None
    m = MILES_PATTERN.search(text)
    if m:
        try:
            miles = int(m.group(1).replace(",", ""))
        except: pass
    
    pu_time = None
    m = PU_TIME_PATTERN.search(text)
    if m:
        pu_time = m.group(1).strip()
    
    del_time = None
    m = DEL_TIME_PATTERN.search(text)
    if m:
        del_time = m.group(1).strip()
    
    pu_addr = None
    m = PU_ADDR_PATTERN.search(text)
    if m:
        pu_addr = clean_address(m.group(1))
        log(f"  PU: {pu_addr}")
    
    del_addr = None
    m = DEL_ADDR_PATTERN.search(text)
    if m:
        del_addr = clean_address(m.group(1))
        log(f"  DEL: {del_addr}")
    
    if not pu_addr or not del_addr:
        log("  FAILED: Missing address")
        return None
    
    job_id = hashlib.sha1(f"{load_num}|{pu_addr}|{del_addr}".encode()).hexdigest()[:10]
    
    job = {
        "id": job_id,
        "created_at": now_iso(),
        "meta": {"load_number": load_num, "rate": rate, "miles": miles},
        "pu": {
            "addr": pu_addr,
            "time": pu_time,
            "status": {"arr": None, "load": None, "dep": None, "comp": None},
            "docs": {"pti": False, "bol": False},
        },
        "del": [{
            "addr": del_addr,
            "time": del_time,
            "status": {"arr": None, "del": None, "dep": None, "comp": None, "skip": False},
            "docs": {"pod": False},
        }],
    }
    
    log(f"  SUCCESS: Job {job_id}")
    return job

def get_job(st: dict) -> Optional[dict]:
    job = st.get("job")
    if not job or not isinstance(job, dict):
        return None
    if "pu" not in job or "del" not in job:
        return None
    return job

# ============================================================================
# WORKFLOW HELPERS
# ============================================================================
def is_pu_complete(job: dict) -> bool:
    return bool(job.get("pu", {}).get("status", {}).get("comp"))

def get_focus(job: dict, st: dict) -> Tuple[str, int]:
    if not is_pu_complete(job):
        return "PU", 0
    
    dels = job.get("del") or []
    idx = st.get("focus_i", 0)
    idx = max(0, min(idx, len(dels) - 1)) if dels else 0
    
    if dels and idx < len(dels):
        if dels[idx].get("status", {}).get("comp"):
            for i in range(idx + 1, len(dels)):
                if not dels[i].get("status", {}).get("comp"):
                    idx = i
                    break
    
    return "DEL", idx

def load_label(job: dict) -> str:
    meta = job.get("meta") or {}
    if meta.get("load_number"):
        return f"Load #{meta['load_number']}"
    return f"Job {job.get('id', '?')[:8]}"

def short_addr(addr: str, max_len: int = 50) -> str:
    if not addr:
        return "(no address)"
    if len(addr) <= max_len:
        return addr
    return addr[:max_len-3] + "..."

def toggle_timestamp(obj: dict, key: str) -> bool:
    if obj.get(key):
        obj[key] = None
        return False
    obj[key] = now_iso()
    return True

# ============================================================================
# KEYBOARDS
# ============================================================================
def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text, callback_data=data)

def check(on: bool, label: str) -> str:
    return f"✅ {label}" if on else label

def build_panel_keyboard(job: dict, st: dict) -> InlineKeyboardMarkup:
    stage, idx = get_focus(job, st)
    rows = []
    
    if stage == "PU":
        pu = job.get("pu", {})
        ps = pu.get("status", {})
        pd = pu.get("docs", {})
        
        rows.append([
            btn(check(ps.get("arr"), "Arrived"), "PU:ARR"),
            btn(check(ps.get("load"), "Loaded"), "PU:LOAD"),
            btn(check(ps.get("dep"), "Departed"), "PU:DEP"),
        ])
        rows.append([
            btn(check(pd.get("pti"), "PTI"), "DOC:PTI"),
            btn(check(pd.get("bol"), "BOL"), "DOC:BOL"),
            btn(check(ps.get("comp"), "✓ PU Done"), "PU:COMP"),
        ])
    else:
        dels = job.get("del", [])
        if idx < len(dels):
            d = dels[idx]
            ds = d.get("status", {})
            dd = d.get("docs", {})
            lbl = f"{idx+1}/{len(dels)}"
            
            rows.append([
                btn(check(ds.get("arr"), f"Arr {lbl}"), "DEL:ARR"),
                btn(check(ds.get("del"), "Delivered"), "DEL:DEL"),
                btn(check(ds.get("dep"), "Departed"), "DEL:DEP"),
            ])
            rows.append([
                btn(check(dd.get("pod"), "POD"), "DOC:POD"),
                btn(check(ds.get("comp"), "✓ Done"), "DEL:COMP"),
                btn("⏭ Skip", "DEL:SKIP"),
            ])
            
            if len(dels) > 1:
                nav = []
                if idx > 0:
                    nav.append(btn("◀️ Prev", f"NAV:{idx-1}"))
                if idx < len(dels) - 1:
                    nav.append(btn("Next ▶️", f"NAV:{idx+1}"))
                if nav:
                    rows.append(nav)
    
    rows.append([btn("📍 ETA", "ETA:ONE"), btn("📍 All ETAs", "ETA:ALL")])
    rows.append([btn("📊 Catalog", "CATALOG"), btn("✅ Finish Load", "FINISH")])
    
    return InlineKeyboardMarkup(rows)

def build_done_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[btn("📊 Catalog", "CATALOG")]])

# ============================================================================
# ALERTS
# ============================================================================
async def send_alert(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str):
    try:
        msg = await ctx.bot.send_message(chat_id, text, parse_mode="HTML", disable_notification=True)
        if ALERT_TTL_SECONDS > 0:
            async def delete_later():
                await asyncio.sleep(ALERT_TTL_SECONDS)
                try:
                    await ctx.bot.delete_message(chat_id, msg.message_id)
                except:
                    pass
            task = asyncio.create_task(delete_later())
            _tasks.add(task)
            task.add_done_callback(_tasks.discard)
    except Exception as e:
        log_error("Alert failed", e)

# ============================================================================
# EXCEL EXPORT
# ============================================================================
def make_catalog_xlsx(records: List[dict], week: str) -> Tuple[bytes, str]:
    wb = Workbook()
    ws = wb.active
    ws.title = week[:31] if week != "ALL" else "All Loads"
    
    headers = ["Completed", "Load #", "Pickup", "Delivery", "Rate", "Miles", "$/Mile"]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = PatternFill("solid", fgColor="4472C4")
    
    total_rate, total_miles = 0, 0
    for r in sorted(records, key=lambda x: x.get("completed_utc", "")):
        rate = r.get("rate")
        miles = r.get("posted_miles") or r.get("est_miles")
        rpm = (rate / miles) if rate and miles else None
        
        ws.append([
            r.get("completed", ""),
            r.get("load_number", ""),
            r.get("pickup", "")[:50],
            r.get("deliveries", "")[:50],
            rate,
            miles,
            round(rpm, 2) if rpm else None,
        ])
        
        if rate: total_rate += rate
        if miles: total_miles += miles
    
    ws.append([])
    ws.append(["TOTAL", "", "", "", total_rate, total_miles,
               round(total_rate/total_miles, 2) if total_miles else None])
    for cell in ws[ws.max_row]:
        cell.font = Font(bold=True)
    
    for col, width in [("A", 18), ("B", 15), ("C", 40), ("D", 40), ("E", 12), ("F", 10), ("G", 10)]:
        ws.column_dimensions[col].width = width
    
    buf = io.BytesIO()
    wb.save(buf)
    filename = f"loads_{week}.xlsx" if week != "ALL" else "loads_all.xlsx"
    return buf.getvalue(), filename

# ============================================================================
# COMMAND HANDLERS
# ============================================================================
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = f"""🚚 <b>Trucker Dispatch Bot</b> v{BOT_VERSION}

<b>Setup:</b>
1. DM: /claim &lt;code&gt;
2. DM: /update (share location)
3. Group: /allowhere

<b>Usage:</b>
• Forward load sheet → auto-detects
• Type <code>eta</code> or <code>1717</code>
• /panel for controls

<b>Debug:</b>
/testgeo &lt;address&gt; - Test geocoding
/clearcache - Reset address cache"""
    
    await update.message.reply_text(text, parse_mode="HTML")

async def cmd_ping(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🏓 pong • {BOT_VERSION}")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    job = get_job(st)
    loc = st.get("last_location")
    gc = st.get("geocode_cache", {})
    
    loc_info = "❌ Not set"
    if loc:
        age_min = 0
        if loc.get("updated_at"):
            try:
                age_min = int((now_utc() - datetime.fromisoformat(loc["updated_at"])).total_seconds() / 60)
            except: pass
        loc_info = f"✅ {loc['lat']:.4f}, {loc['lon']:.4f} ({age_min}m ago)"
    
    text = f"""<b>Status</b>
• Version: {BOT_VERSION}
• Owner: {st.get('owner_id', 'Not set')}
• Location: {loc_info}
• Active load: {load_label(job) if job else '❌ None'}
• History: {len(st.get('history', []))} loads
• Cache: {len(gc)} addresses
• Debug: {'ON' if DEBUG else 'OFF'}"""
    
    await update.message.reply_text(text, parse_mode="HTML")

async def cmd_testgeo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Test geocoding for an address."""
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        await update.message.reply_text("⚠️ Owner only")
        return
    
    if not ctx.args:
        await update.message.reply_text("Usage: /testgeo <address>")
        return
    
    addr = " ".join(ctx.args)
    await update.message.reply_text(f"🔍 Testing: {addr}\n\nPlease wait...")
    
    cache = {}  # Fresh cache for test
    geo = await geocode(addr, cache)
    
    if geo:
        lat, lon, tz = geo
        await update.message.reply_text(
            f"✅ <b>Geocode SUCCESS</b>\n\n"
            f"📍 {lat:.6f}, {lon:.6f}\n"
            f"🕐 Timezone: {tz}\n\n"
            f"<a href=\"https://www.google.com/maps?q={lat},{lon}\">Open in Google Maps</a>",
            parse_mode="HTML"
        )
        # Send location pin
        try:
            await ctx.bot.send_location(update.effective_chat.id, lat, lon)
        except: pass
    else:
        # Show what variants were tried
        variants = generate_address_variants(addr)
        variant_list = "\n".join(f"• {v}" for v in variants[:6])
        await update.message.reply_text(
            f"❌ <b>Geocode FAILED</b>\n\n"
            f"Tried variants:\n{variant_list}\n\n"
            f"All services returned no results.",
            parse_mode="HTML"
        )

async def cmd_claim(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.message.reply_text("⚠️ DM me: /claim <code>")
        return
    
    if not CLAIM_CODE:
        await update.message.reply_text("⚠️ CLAIM_CODE not set")
        return
    
    code = " ".join(ctx.args) if ctx.args else ""
    if code != CLAIM_CODE:
        await update.message.reply_text("❌ Wrong code")
        return
    
    async with _state_lock:
        st = load_state()
        st["owner_id"] = update.effective_user.id
        save_state(st)
    
    log(f"Owner: {update.effective_user.id}")
    await update.message.reply_text("✅ You're the owner! Use /update to share location.")

async def cmd_allowhere(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        await update.message.reply_text("⚠️ Owner only")
        return
    
    if update.effective_chat.type == "private":
        await update.message.reply_text("⚠️ Run in group")
        return
    
    async with _state_lock:
        st = load_state()
        allowed = set(st.get("allowed_chats", []))
        allowed.add(update.effective_chat.id)
        st["allowed_chats"] = list(allowed)
        save_state(st)
    
    log(f"Group allowed: {update.effective_chat.id}")
    await update.message.reply_text("✅ Group allowed!")

async def cmd_update(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        await update.message.reply_text("⚠️ Owner only")
        return
    
    if update.effective_chat.type != "private":
        await update.message.reply_text("📍 DM me /update")
        return
    
    kb = [[KeyboardButton("📍 Share Location", request_location=True)]]
    await update.message.reply_text(
        "Tap to share location.\n💡 Use 'Live Location' for geofence!",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)
    )

async def cmd_panel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not chat_allowed(update, st):
        await update.message.reply_text("⚠️ Not allowed")
        return
    
    job = get_job(st)
    if not job:
        await update.message.reply_text("📭 No active load")
        return
    
    meta = job.get("meta", {})
    stage, idx = get_focus(job, st)
    
    lines = [f"<b>{h(load_label(job))}</b>"]
    if meta.get("rate") or meta.get("miles"):
        parts = []
        if meta.get("rate"): parts.append(f"💰 {money(meta['rate'])}")
        if meta.get("miles"): parts.append(f"{meta['miles']} mi")
        lines.append(" • ".join(parts))
    
    if stage == "PU":
        pu = job.get("pu", {})
        lines.append(f"\n<b>📍 Pickup</b>")
        lines.append(h(short_addr(pu.get("addr", ""), 60)))
    else:
        dels = job.get("del", [])
        if idx < len(dels):
            d = dels[idx]
            lines.append(f"\n<b>📍 Delivery {idx+1}/{len(dels)}</b>")
            lines.append(h(short_addr(d.get("addr", ""), 60)))
    
    msg = await update.message.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=build_panel_keyboard(job, st)
    )
    
    async with _state_lock:
        st = load_state()
        st.setdefault("panel_msgs", {})[str(update.effective_chat.id)] = msg.message_id
        save_state(st)

async def cmd_finish(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        await update.message.reply_text("⚠️ Owner only")
        return
    
    job = get_job(st)
    if not job:
        await update.message.reply_text("📭 No active load")
        return
    
    await do_finish_load(update, ctx, st, job)

async def cmd_catalog(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        await update.message.reply_text("⚠️ Owner only")
        return
    
    history = st.get("history", [])
    if not history:
        await update.message.reply_text("📭 No loads yet")
        return
    
    tz_name = (st.get("last_location") or {}).get("tz", "UTC")
    current_week = week_key(now_utc().astimezone(safe_tz(tz_name)))
    
    arg = ctx.args[0].lower() if ctx.args else ""
    if arg == "all":
        week = "ALL"
        records = history
    else:
        week = current_week
        records = [r for r in history if r.get("week") == week]
    
    if not records:
        await update.message.reply_text(f"No loads for {week}")
        return
    
    xlsx_data, filename = make_catalog_xlsx(records, week)
    total_rate = sum(r.get("rate", 0) or 0 for r in records)
    
    await update.message.reply_document(
        document=io.BytesIO(xlsx_data),
        filename=filename,
        caption=f"📊 {len(records)} loads • {money(total_rate)}",
        parse_mode="HTML"
    )

async def cmd_skip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        return
    
    job = get_job(st)
    if not job:
        return
    
    stage, idx = get_focus(job, st)
    if stage != "DEL":
        await update.message.reply_text("⚠️ Complete pickup first")
        return
    
    async with _state_lock:
        st = load_state()
        job = get_job(st)
        if job:
            dels = job.get("del", [])
            if idx < len(dels):
                dels[idx]["status"]["skip"] = True
                dels[idx]["status"]["comp"] = now_iso()
                st["job"] = job
                save_state(st)
    
    await update.message.reply_text(f"⏭ Skipped stop {idx+1}")

async def cmd_deleteall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete messages - doesn't delete itself."""
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        await update.message.reply_text("⚠️ Owner only")
        return
    
    chat = update.effective_chat
    if not chat or chat.type == "private":
        await update.message.reply_text("❌ Use in group")
        return
    
    n = DELETEALL_DEFAULT
    if ctx.args:
        try:
            n = int(ctx.args[0])
        except: pass
    n = max(1, min(n, 2000))
    
    cmd_msg_id = update.message.message_id
    status_msg = await update.message.reply_text(f"🧹 Deleting up to {n} messages...")
    status_msg_id = status_msg.message_id
    
    deleted = 0
    failed = 0
    
    for mid in range(cmd_msg_id - 1, max(0, cmd_msg_id - n - 1), -1):
        try:
            await ctx.bot.delete_message(chat.id, mid)
            deleted += 1
            await asyncio.sleep(0.05)
        except BadRequest:
            failed += 1
            if failed > 30:
                break
        except Forbidden:
            await status_msg.edit_text("❌ No permission")
            return
        except:
            failed += 1
            if failed > 30:
                break
    
    try:
        await ctx.bot.delete_message(chat.id, cmd_msg_id)
    except: pass
    
    try:
        await status_msg.edit_text(f"✅ Deleted {deleted} messages")
        await asyncio.sleep(3)
        await ctx.bot.delete_message(chat.id, status_msg_id)
    except: pass

async def cmd_leave(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        return
    
    chat = update.effective_chat
    if not chat or chat.type == "private":
        return
    
    async with _state_lock:
        st = load_state()
        allowed = set(st.get("allowed_chats", []))
        allowed.discard(chat.id)
        st["allowed_chats"] = list(allowed)
        save_state(st)
    
    await update.message.reply_text("👋")
    try:
        await ctx.bot.leave_chat(chat.id)
    except: pass

async def cmd_clearcache(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Clear geocode cache."""
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        await update.message.reply_text("⚠️ Owner only")
        return
    
    async with _state_lock:
        st = load_state()
        old = len(st.get("geocode_cache", {}))
        st["geocode_cache"] = {}
        save_state(st)
    
    await update.message.reply_text(f"✅ Cleared {old} cached addresses")

# ============================================================================
# LOCATION HANDLER
# ============================================================================
async def on_location(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.location:
        return
    
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        return
    
    loc = update.message.location
    tz = TF.timezone_at(lat=loc.latitude, lng=loc.longitude) or "UTC"
    
    async with _state_lock:
        st = load_state()
        st["last_location"] = {
            "lat": loc.latitude,
            "lon": loc.longitude,
            "tz": tz,
            "updated_at": now_iso()
        }
        save_state(st)
    
    log(f"Location: {loc.latitude:.4f}, {loc.longitude:.4f}")
    
    if update.effective_chat.type == "private":
        await update.message.reply_text(
            f"✅ Location saved!\n📍 {loc.latitude:.4f}, {loc.longitude:.4f}\n🕐 {tz}",
            reply_markup=ReplyKeyboardRemove()
        )

# ============================================================================
# TEXT HANDLER
# ============================================================================
async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    
    text = update.message.text
    chat = update.effective_chat
    
    async with _state_lock:
        st = load_state()
    
    if chat and chat.type in ("group", "supergroup"):
        if chat.id not in (st.get("allowed_chats") or []):
            return
        
        job = parse_load(text)
        if job:
            async with _state_lock:
                st = load_state()
                st["job"] = job
                st["focus_i"] = 0
                st["reminders_sent"] = {}
                st["geofence_state"] = {}
                save_state(st)
            
            meta = job.get("meta", {})
            lines = [f"📦 <b>New Load!</b>", f"<b>{h(load_label(job))}</b>"]
            if meta.get("rate") or meta.get("miles"):
                parts = []
                if meta.get("rate"): parts.append(f"💰 {money(meta['rate'])}")
                if meta.get("miles"): parts.append(f"{meta['miles']} mi")
                lines.append(" • ".join(parts))
            
            lines.append(f"\n📍 PU: {h(short_addr(job['pu'].get('addr', ''), 40))}")
            lines.append(f"📍 DEL: {h(short_addr(job['del'][0].get('addr', ''), 40))}")
            lines.append(f"\nType <code>eta</code> or /panel")
            
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
            return
    
    if not chat_allowed(update, st):
        return
    
    first_word = text.strip().split()[0].lower() if text.strip() else ""
    first_word = re.sub(r"[^\w]", "", first_word)
    
    if first_word in TRIGGERS:
        is_all = "all" in text.lower()
        await send_eta_response(update, ctx, st, all_stops=is_all)

# ============================================================================
# CALLBACK HANDLER
# ============================================================================
async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    
    data = query.data
    log_debug(f"Callback: {data}")
    
    await query.answer()
    
    async with _state_lock:
        st = load_state()
    
    if not chat_allowed(update, st):
        return
    
    try:
        if data.startswith("ETA:"):
            await handle_eta_callback(update, ctx, st, data)
        elif data == "CATALOG":
            await handle_catalog_callback(update, ctx, st)
        elif data == "FINISH":
            await handle_finish_callback(update, ctx, st)
        elif data.startswith("NAV:"):
            await handle_nav_callback(update, ctx, st, data)
        else:
            await handle_status_callback(update, ctx, st, data)
    except Exception as e:
        log_error(f"Callback error: {data}", e)

async def handle_eta_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, st: dict, data: str):
    await send_eta_response(update, ctx, st, all_stops=(data == "ETA:ALL"))

async def handle_catalog_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, st: dict):
    if not is_owner(update, st):
        return
    
    history = st.get("history", [])
    if not history:
        await update.effective_message.reply_text("📭 No loads")
        return
    
    tz_name = (st.get("last_location") or {}).get("tz", "UTC")
    week = week_key(now_utc().astimezone(safe_tz(tz_name)))
    records = [r for r in history if r.get("week") == week] or history
    
    xlsx_data, filename = make_catalog_xlsx(records, week if records else "ALL")
    total_rate = sum(r.get("rate", 0) or 0 for r in records)
    
    await ctx.bot.send_document(
        chat_id=update.effective_chat.id,
        document=io.BytesIO(xlsx_data),
        filename=filename,
        caption=f"📊 {len(records)} loads • {money(total_rate)}"
    )

async def handle_finish_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, st: dict):
    if not is_owner(update, st):
        return
    
    job = get_job(st)
    if job:
        await do_finish_load(update, ctx, st, job)

async def handle_nav_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, st: dict, data: str):
    try:
        new_idx = int(data.split(":")[1])
    except:
        return
    
    async with _state_lock:
        st = load_state()
        st["focus_i"] = new_idx
        save_state(st)
        job = get_job(st)
    
    if job:
        try:
            await update.callback_query.edit_message_reply_markup(
                reply_markup=build_panel_keyboard(job, st)
            )
        except: pass

async def handle_status_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, st: dict, data: str):
    async with _state_lock:
        st = load_state()
        job = get_job(st)
        if not job:
            return
        
        stage, idx = get_focus(job, st)
        tz_name = (st.get("last_location") or {}).get("tz", "UTC")
        ts = local_stamp(tz_name)
        alert_msg = None
        
        if data.startswith("PU:"):
            ps = job["pu"].setdefault("status", {})
            if data == "PU:ARR":
                if toggle_timestamp(ps, "arr"):
                    alert_msg = f"📍 <b>Arrived PU</b> • {h(ts)}"
            elif data == "PU:LOAD":
                if toggle_timestamp(ps, "load"):
                    alert_msg = f"📦 <b>Loaded</b> • {h(ts)}"
            elif data == "PU:DEP":
                if toggle_timestamp(ps, "dep"):
                    alert_msg = f"🚚 <b>Departed PU</b> • {h(ts)}"
            elif data == "PU:COMP":
                if toggle_timestamp(ps, "comp"):
                    alert_msg = f"✅ <b>PU Complete</b> • {h(ts)}"
        
        elif data.startswith("DEL:"):
            if stage != "DEL":
                save_state(st)
                return
            
            dels = job.get("del", [])
            if idx >= len(dels):
                save_state(st)
                return
            
            ds = dels[idx].setdefault("status", {})
            lbl = f"{idx+1}/{len(dels)}"
            
            if data == "DEL:ARR":
                if toggle_timestamp(ds, "arr"):
                    alert_msg = f"📍 <b>Arrived {lbl}</b> • {h(ts)}"
            elif data == "DEL:DEL":
                if toggle_timestamp(ds, "del"):
                    alert_msg = f"📦 <b>Delivered {lbl}</b> • {h(ts)}"
            elif data == "DEL:DEP":
                if toggle_timestamp(ds, "dep"):
                    alert_msg = f"🚚 <b>Departed {lbl}</b> • {h(ts)}"
            elif data == "DEL:COMP":
                if toggle_timestamp(ds, "comp"):
                    alert_msg = f"✅ <b>Complete {lbl}</b> • {h(ts)}"
                    for i in range(idx + 1, len(dels)):
                        if not dels[i].get("status", {}).get("comp"):
                            st["focus_i"] = i
                            break
            elif data == "DEL:SKIP":
                ds["skip"] = True
                ds["comp"] = ds.get("comp") or now_iso()
                alert_msg = f"⏭ <b>Skipped {lbl}</b>"
                for i in range(idx + 1, len(dels)):
                    if not dels[i].get("status", {}).get("comp"):
                        st["focus_i"] = i
                        break
        
        elif data.startswith("DOC:"):
            if data == "DOC:PTI":
                job["pu"].setdefault("docs", {})["pti"] = not job["pu"].get("docs", {}).get("pti", False)
            elif data == "DOC:BOL":
                job["pu"].setdefault("docs", {})["bol"] = not job["pu"].get("docs", {}).get("bol", False)
            elif data == "DOC:POD" and stage == "DEL":
                dels = job.get("del", [])
                if idx < len(dels):
                    dels[idx].setdefault("docs", {})["pod"] = not dels[idx].get("docs", {}).get("pod", False)
        
        st["job"] = job
        save_state(st)
    
    if alert_msg:
        await send_alert(ctx, update.effective_chat.id, alert_msg)
    
    try:
        await update.callback_query.edit_message_reply_markup(
            reply_markup=build_panel_keyboard(job, st)
        )
    except: pass

# ============================================================================
# ETA RESPONSE
# ============================================================================
async def send_eta_response(update: Update, ctx: ContextTypes.DEFAULT_TYPE, st: dict, all_stops: bool = False):
    loc = st.get("last_location")
    if not loc:
        await update.effective_message.reply_text("📍 No location. Owner: /update in DM")
        return
    
    loc_age_min = 999
    if loc.get("updated_at"):
        try:
            loc_age_min = int((now_utc() - datetime.fromisoformat(loc["updated_at"])).total_seconds() / 60)
        except: pass
    
    origin = (loc["lat"], loc["lon"])
    tz_name = loc.get("tz", "UTC")
    tz = safe_tz(tz_name)
    
    try:
        await ctx.bot.send_location(update.effective_chat.id, origin[0], origin[1])
    except: pass
    
    job = get_job(st)
    if not job:
        await update.effective_message.reply_text(
            f"⏱ {now_utc().astimezone(tz).strftime('%H:%M')} ({tz_name})\n\n<i>No active load</i>",
            parse_mode="HTML"
        )
        return
    
    age_warn = f"\n⚠️ <i>Location {loc_age_min}m old</i>" if loc_age_min > 10 else ""
    
    if all_stops:
        await send_all_etas(update, ctx, st, job, origin, tz, tz_name, age_warn)
    else:
        await send_single_eta(update, ctx, st, job, origin, tz, tz_name, age_warn)

async def send_all_etas(update, ctx, st, job, origin, tz, tz_name, age_warn=""):
    lines = [f"<b>{h(load_label(job))}</b>{age_warn}", ""]
    
    pu = job.get("pu", {})
    if pu.get("status", {}).get("comp"):
        lines.append(f"✅ <b>PU:</b> Done")
    else:
        eta = await calc_eta(st, origin, pu.get("addr", ""))
        if eta["ok"]:
            arr = (now_utc() + timedelta(seconds=eta["seconds"])).astimezone(tz).strftime("%H:%M")
            m = "≈" if eta["method"] == "estimate" else ""
            lines.append(f"<b>PU:</b> {fmt_dur(eta['seconds'])}{m} • {fmt_mi(eta['meters'])} • ~{arr}")
        else:
            lines.append(f"<b>PU:</b> ⚠️ {eta['err']}")
    
    for i, d in enumerate(job.get("del", [])[:ETA_ALL_MAX]):
        if d.get("status", {}).get("comp"):
            lines.append(f"✅ <b>D{i+1}:</b> Done")
        else:
            eta = await calc_eta(st, origin, d.get("addr", ""))
            if eta["ok"]:
                arr = (now_utc() + timedelta(seconds=eta["seconds"])).astimezone(tz).strftime("%H:%M")
                m = "≈" if eta["method"] == "estimate" else ""
                lines.append(f"<b>D{i+1}:</b> {fmt_dur(eta['seconds'])}{m} • {fmt_mi(eta['meters'])} • ~{arr}")
            else:
                lines.append(f"<b>D{i+1}:</b> ⚠️ {eta['err']}")
    
    async with _state_lock:
        st2 = load_state()
        st2["geocode_cache"] = st.get("geocode_cache", {})
        save_state(st2)
    
    await update.effective_message.reply_text(
        "\n".join(lines), parse_mode="HTML",
        reply_markup=build_panel_keyboard(job, st)
    )

async def send_single_eta(update, ctx, st, job, origin, tz, tz_name, age_warn=""):
    stage, idx = get_focus(job, st)
    
    if stage == "PU":
        addr = job.get("pu", {}).get("addr", "")
        appt = job.get("pu", {}).get("time")
        label = "Pickup"
    else:
        dels = job.get("del", [])
        d = dels[idx] if idx < len(dels) else {}
        addr = d.get("addr", "")
        appt = d.get("time")
        label = f"Delivery {idx+1}/{len(dels)}"
    
    eta = await calc_eta(st, origin, addr)
    
    async with _state_lock:
        st2 = load_state()
        st2["geocode_cache"] = st.get("geocode_cache", {})
        save_state(st2)
    
    if eta["ok"]:
        arr_time = now_utc() + timedelta(seconds=eta["seconds"])
        arr_str = arr_time.astimezone(tz).strftime("%H:%M")
        m = " ≈" if eta["method"] == "estimate" else ""
        
        lines = [
            f"⏱ <b>ETA: {fmt_dur(eta['seconds'])}</b>{m}{age_warn}",
            "",
            f"<b>{label}</b> • {h(load_label(job))}",
            f"📍 {h(short_addr(addr, 50))}",
            f"🚚 {fmt_mi(eta['meters'])} • ~{arr_str}",
        ]
        if appt:
            lines.append(f"⏰ Appt: {h(appt)}")
        
        await update.effective_message.reply_text(
            "\n".join(lines), parse_mode="HTML",
            reply_markup=build_panel_keyboard(job, st)
        )
    else:
        await update.effective_message.reply_text(
            f"⚠️ <b>Could not calculate ETA</b>\n{eta['err']}\n\n"
            f"📍 {h(short_addr(addr, 45))}\n\n"
            f"Try: /testgeo {addr[:30]}",
            parse_mode="HTML",
            reply_markup=build_panel_keyboard(job, st)
        )

# ============================================================================
# FINISH LOAD
# ============================================================================
async def do_finish_load(update: Update, ctx: ContextTypes.DEFAULT_TYPE, st: dict, job: dict):
    tz_name = (st.get("last_location") or {}).get("tz", "UTC")
    dt = now_utc().astimezone(safe_tz(tz_name))
    wk = week_key(dt)
    
    meta = job.get("meta", {})
    pu = job.get("pu", {})
    dels = job.get("del", [])
    
    record = {
        "week": wk,
        "completed": dt.strftime("%Y-%m-%d %H:%M"),
        "completed_utc": now_iso(),
        "load_number": meta.get("load_number", ""),
        "pickup": pu.get("addr", ""),
        "deliveries": " | ".join(d.get("addr", "") for d in dels),
        "rate": meta.get("rate"),
        "posted_miles": meta.get("miles"),
    }
    
    async with _state_lock:
        st = load_state()
        history = st.setdefault("history", [])
        history.append(record)
        st["history"] = history[-1000:]
        st["job"] = None
        st["focus_i"] = 0
        st["reminders_sent"] = {}
        st["geofence_state"] = {}
        save_state(st)
    
    week_records = [r for r in st["history"] if r.get("week") == wk]
    wk_rate = sum(r.get("rate", 0) or 0 for r in week_records)
    wk_miles = sum(r.get("posted_miles", 0) or 0 for r in week_records)
    
    report = f"""✅ <b>Load Complete!</b>

<b>#{h(meta.get('load_number') or job.get('id', '')[:8])}</b> • {money(meta.get('rate'))}

📊 <b>Week {wk}:</b>
• {len(week_records)} loads
• {money(wk_rate)} gross
• {wk_miles} miles"""
    
    chat_id = update.effective_chat.id
    panel_id = st.get("panel_msgs", {}).get(str(chat_id))
    
    if panel_id:
        try:
            await ctx.bot.edit_message_text(
                chat_id=chat_id, message_id=panel_id,
                text=report, parse_mode="HTML",
                reply_markup=build_done_keyboard()
            )
            return
        except: pass
    
    await ctx.bot.send_message(chat_id, report, parse_mode="HTML", reply_markup=build_done_keyboard())

# ============================================================================
# BACKGROUND JOBS
# ============================================================================
async def reminder_job(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        async with _state_lock:
            st = load_state()
        
        job = get_job(st)
        if not job:
            return
        
        tz_name = (st.get("last_location") or {}).get("tz", "UTC")
        sent = st.get("reminders_sent", {})
        chats = get_broadcast_chats(st)
        alerts = []
        
        for threshold in REMINDER_THRESHOLDS_MIN:
            pu = job.get("pu", {})
            if pu.get("time") and not pu.get("status", {}).get("comp"):
                appt = parse_appt_time(pu["time"], tz_name)
                if appt:
                    mins = (appt - now_utc()).total_seconds() / 60
                    key = f"appt:pu:{threshold}"
                    if threshold - 5 < mins <= threshold and key not in sent:
                        alerts.append((key, f"⏰ <b>PU in ~{int(mins)}m</b>"))
        
        if alerts:
            async with _state_lock:
                st = load_state()
                sent = st.setdefault("reminders_sent", {})
                for key, msg in alerts:
                    sent[key] = True
                    for chat_id in chats:
                        try:
                            await ctx.bot.send_message(chat_id, msg, parse_mode="HTML")
                        except: pass
                save_state(st)
    except Exception as e:
        log_error("Reminder job", e)

def parse_appt_time(time_str: str, tz_name: str) -> Optional[datetime]:
    if not time_str:
        return None
    
    m = re.search(r"(\w{3})\s+(\d{1,2}),?\s+(\d{4})\s+(\d{1,2}):(\d{2})", time_str)
    if m:
        try:
            dt_str = f"{m.group(1)} {m.group(2)} {m.group(3)} {m.group(4)}:{m.group(5)}"
            dt = datetime.strptime(dt_str, "%b %d %Y %H:%M")
            return dt.replace(tzinfo=safe_tz(tz_name))
        except: pass
    return None

async def geofence_job(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        async with _state_lock:
            st = load_state()
        
        job = get_job(st)
        loc = st.get("last_location")
        
        if not job or not loc:
            return
        
        try:
            loc_time = datetime.fromisoformat(loc["updated_at"])
            if (now_utc() - loc_time).total_seconds() > 300:
                return
        except:
            return
        
        origin = (loc["lat"], loc["lon"])
        gf_state = st.get("geofence_state", {})
        chats = get_broadcast_chats(st)
        events = []
        cache = st.get("geocode_cache", {})
        
        pu = job.get("pu", {})
        if pu.get("addr") and not pu.get("status", {}).get("comp"):
            geo = await geocode(pu["addr"], cache)
            if geo:
                dist = haversine_miles(origin[0], origin[1], geo[0], geo[1])
                was_in = gf_state.get("pu", False)
                is_in = dist <= GEOFENCE_MILES
                
                if is_in and not was_in:
                    events.append(("pu", True, "Pickup", pu["addr"]))
                elif not is_in and was_in:
                    events.append(("pu", False, "Pickup", pu["addr"]))
                gf_state["pu"] = is_in
        
        for i, d in enumerate(job.get("del", [])):
            if d.get("addr") and not d.get("status", {}).get("comp"):
                geo = await geocode(d["addr"], cache)
                if geo:
                    dist = haversine_miles(origin[0], origin[1], geo[0], geo[1])
                    key = f"del{i}"
                    was_in = gf_state.get(key, False)
                    is_in = dist <= GEOFENCE_MILES
                    
                    if is_in and not was_in:
                        events.append((key, True, f"Delivery {i+1}", d["addr"]))
                    elif not is_in and was_in:
                        events.append((key, False, f"Delivery {i+1}", d["addr"]))
                    gf_state[key] = is_in
        
        if events:
            async with _state_lock:
                st = load_state()
                job = get_job(st)
                if not job:
                    return
                
                for key, entered, label, addr in events:
                    if entered:
                        msg = f"📍 <b>ARRIVED: {label}</b>"
                        if key == "pu":
                            job["pu"].setdefault("status", {})["arr"] = now_iso()
                        elif key.startswith("del"):
                            idx = int(key[3:])
                            if idx < len(job.get("del", [])):
                                job["del"][idx].setdefault("status", {})["arr"] = now_iso()
                    else:
                        msg = f"🚚 <b>DEPARTED: {label}</b>"
                        if key == "pu":
                            job["pu"].setdefault("status", {})["dep"] = now_iso()
                        elif key.startswith("del"):
                            idx = int(key[3:])
                            if idx < len(job.get("del", [])):
                                job["del"][idx].setdefault("status", {})["dep"] = now_iso()
                    
                    for chat_id in chats:
                        try:
                            await ctx.bot.send_message(chat_id, msg, parse_mode="HTML")
                        except: pass
                
                st["job"] = job
                st["geofence_state"] = gf_state
                st["geocode_cache"] = cache
                save_state(st)
    except Exception as e:
        log_error("Geofence job", e)

# ============================================================================
# MAIN
# ============================================================================
async def post_init(app: Application):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        me = await app.bot.get_me()
        log(f"Bot: @{me.username}")
    except Exception as e:
        log_error("Init", e)
    
    if app.job_queue:
        app.job_queue.run_repeating(reminder_job, interval=60, first=10)
        app.job_queue.run_repeating(geofence_job, interval=30, first=15)
    
    log(f"Ready! v{BOT_VERSION}")
    log(f"Geocoders: {', '.join(s['name'] for s in GEOCODE_SERVICES)}")

def main():
    if not TOKEN:
        print("ERROR: TELEGRAM_TOKEN not set")
        return
    
    log(f"Starting v{BOT_VERSION}...")
    
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("testgeo", cmd_testgeo))
    app.add_handler(CommandHandler("claim", cmd_claim))
    app.add_handler(CommandHandler("allowhere", cmd_allowhere))
    app.add_handler(CommandHandler("update", cmd_update))
    app.add_handler(CommandHandler("panel", cmd_panel))
    app.add_handler(CommandHandler("finish", cmd_finish))
    app.add_handler(CommandHandler("catalog", cmd_catalog))
    app.add_handler(CommandHandler("skip", cmd_skip))
    app.add_handler(CommandHandler("deleteall", cmd_deleteall))
    app.add_handler(CommandHandler("leave", cmd_leave))
    app.add_handler(CommandHandler("clearcache", cmd_clearcache))
    
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.LOCATION, on_location))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
