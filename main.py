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
from typing import Dict, List, Optional, Tuple, Any

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
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# Optional: big-text ETA card images
try:
    from PIL import Image, ImageDraw, ImageFont  # type: ignore
except Exception:  # pragma: no cover
    Image = None  # type: ignore
    ImageDraw = None  # type: ignore
    ImageFont = None  # type: ignore

# ------------------ Config ------------------

TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CLAIM_CODE = os.environ.get("CLAIM_CODE", "").strip()
STATE_FILE = Path(os.environ.get("STATE_FILE", "state.json"))

TRIGGERS = {
    t.strip().lower()
    for t in os.environ.get("TRIGGERS", "eta,1717").split(",")
    if t.strip()
}

ETA_ALL_MAX_STOPS = int(os.environ.get("ETA_ALL_MAX_STOPS", "6"))

GEOFENCE_MILES = float(os.environ.get("GEOFENCE_MILES", "3.0"))
REMINDER_THRESHOLDS_MIN = [
    int(x) for x in os.environ.get("REMINDER_THRESHOLDS_MIN", "60,30,10").split(",") if x.strip().isdigit()
]
REMINDER_DOC_AFTER_MIN = int(os.environ.get("REMINDER_DOC_AFTER_MIN", "15"))
SCHEDULE_GRACE_MIN = int(os.environ.get("SCHEDULE_GRACE_MIN", "15"))

TF = TimezoneFinder()

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"

# IMPORTANT: Set NOMINATIM_USER_AGENT to something unique in Railway Variables
# Example: "MyDispatchBot/1.0 (yourname@email.com)"
NOMINATIM_USER_AGENT = os.environ.get("NOMINATIM_USER_AGENT", "telegram-dispatch-eta-bot/1.0").strip()
NOMINATIM_MIN_INTERVAL = float(os.environ.get("NOMINATIM_MIN_INTERVAL", "1.1"))

# Delete-all settings (how many messages to attempt)
DELETEALL_DEFAULT = int(os.environ.get("DELETEALL_DEFAULT", "500"))
DELETEALL_MAX = int(os.environ.get("DELETEALL_MAX", "5000"))

_nominatim_lock = asyncio.Lock()
_nominatim_last_request = 0.0
_state_lock = asyncio.Lock()


# ------------------ Basic helpers ------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_utc_iso() -> str:
    return now_utc().isoformat()


def parse_iso(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def atomic_write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            state = {}
    else:
        state = {}

    state.setdefault("owner_id", None)
    state.setdefault("allowed_chats", [])
    state.setdefault("last_location", None)  # {"lat","lon","updated_at","tz"}
    state.setdefault("job", None)
    state.setdefault("job_stage", "PU")  # "PU" or "DEL"
    state.setdefault("del_index", 0)
    state.setdefault("job_chat_id", None)  # where to post reminders
    state.setdefault("geocode_cache", {})  # address -> {"lat","lon","tz"}

    return state


def save_state(state: dict) -> None:
    atomic_write_json(STATE_FILE, state)


def is_private(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == "private")


def is_group(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type in ("group", "supergroup"))


def chat_allowed(state: dict, chat_id: int) -> bool:
    return chat_id in set(state.get("allowed_chats") or [])


def is_owner(update: Update, state: dict) -> bool:
    return (
        state.get("owner_id") is not None
        and update.effective_user is not None
        and update.effective_user.id == state["owner_id"]
    )


def h(s: str) -> str:
    return html.escape(s or "", quote=False)


def format_delta(dt: datetime) -> str:
    delta = now_utc() - dt
    seconds = max(0, int(delta.total_seconds()))
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 48:
        return f"{hours}h {minutes % 60}m ago"
    days = hours // 24
    return f"{days}d ago"


def fmt_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    m, _ = divmod(seconds, 60)
    h_, m = divmod(m, 60)
    return f"{h_}h {m}m" if h_ else f"{m}m"


def fmt_distance_miles(meters: float) -> str:
    miles = meters / 1609.344
    return f"{miles:.1f} mi" if miles < 10 else f"{miles:.0f} mi"


def best_timezone_for_coords(lat: float, lon: float) -> str:
    return TF.timezone_at(lat=lat, lng=lon) or "UTC"


def local_time_str(tz_name: str) -> str:
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
        tz_name = "UTC"
    dt = now_utc().astimezone(tz)
    return f"{dt.strftime('%Y-%m-%d %H:%M')} ({tz_name})"


def normalize_trigger_token(tok: str) -> str:
    tok = tok.strip().lower()
    tok = re.sub(r"^[^\w]+|[^\w]+$", "", tok)
    return tok


def miles_between(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    return haversine_m(lat1, lon1, lat2, lon2) / 1609.344


# ------------------ Job normalization & extras ------------------

def normalize_job(job: Optional[dict]) -> Optional[dict]:
    """Upgrades legacy single-delivery job to multi-delivery structure."""
    if not job:
        return None
    if isinstance(job, dict) and "pickup" in job and "deliveries" in job:
        job.setdefault("meta", {})
        job.setdefault("source_format", "unknown")
        return job

    # legacy
    if isinstance(job, dict) and "pickup_address" in job and "delivery_address" in job:
        pickup_lines = job.get("pickup_lines") or [job.get("pickup_address")]
        delivery_lines = job.get("delivery_lines") or [job.get("delivery_address")]
        pickup_addr = job.get("pickup_address") or ", ".join(pickup_lines)
        del_addr = job.get("delivery_address") or ", ".join(delivery_lines)

        return {
            "job_id": job.get("job_id")
            or hashlib.sha1(f"{pickup_addr}|{del_addr}".encode("utf-8")).hexdigest()[:10],
            "set_at": job.get("set_at") or now_utc_iso(),
            "source_format": job.get("source_format") or "legacy",
            "meta": job.get("meta") or {},
            "pickup": {"time": job.get("pu_time"), "lines": pickup_lines, "address": pickup_addr},
            "deliveries": [{"time": job.get("del_time"), "lines": delivery_lines, "address": del_addr}],
        }

    return job if isinstance(job, dict) else None


def init_job_extras(job: dict) -> dict:
    job.setdefault("events", [])
    job.setdefault("reminders", {"sent": {}, "last_tick": None})
    job.setdefault("geofence", {"pu_prompted_at": None, "del_prompted_at": {}})

    pu = job.setdefault("pickup", {})
    pu.setdefault("status", {"arrived_at": None, "loaded_at": None, "departed_at": None})
    pu.setdefault(
        "docs",
        {
            "pti_video": {"done": False, "done_at": None},
            "bol": {"done": False, "done_at": None},
        },
    )

    deliveries = job.setdefault("deliveries", [])
    for d in deliveries:
        d.setdefault("status", {"arrived_at": None, "delivered_at": None, "departed_at": None})
        d.setdefault("docs", {"pod": {"done": False, "done_at": None}})

    return job


def record_event(job: dict, event_type: str, by_user_id: Optional[int] = None, detail: Optional[str] = None) -> None:
    job.setdefault("events", [])
    job["events"].append(
        {"type": event_type, "at": now_utc_iso(), "by": by_user_id, "detail": detail}
    )
    # keep it from growing forever
    if len(job["events"]) > 200:
        job["events"] = job["events"][-200:]


def get_current_delivery(job: dict, del_index: int) -> Optional[Tuple[int, dict]]:
    deliveries = job.get("deliveries") or []
    if not deliveries:
        return None
    idx = max(0, min(int(del_index), len(deliveries) - 1))
    return idx, deliveries[idx]


def get_target_stop(state: dict) -> Optional[dict]:
    job = normalize_job(state.get("job"))
    if not job:
        return None
    job = init_job_extras(job)
    stage = state.get("job_stage", "PU")
    del_index = int(state.get("del_index", 0))

    if stage == "PU":
        pu = job.get("pickup") or {}
        return {"kind": "PU", "index": None, "stop": pu, "job": job}

    cur = get_current_delivery(job, del_index)
    if not cur:
        return None
    idx, d = cur
    return {"kind": "DEL", "index": idx, "stop": d, "job": job}


# ------------------ Date/time parsing ------------------

DATE_RE1 = re.compile(r"(?P<m>\d{1,2})[/-](?P<d>\d{1,2})[/-](?P<y>\d{2,4})")
DATE_RE2 = re.compile(r"(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})")
TIME_RE = re.compile(r"(?P<h>\d{1,2}):(?P<min>\d{2})")


def _parse_date(date_str: str) -> Optional[Tuple[int, int, int]]:
    s = date_str.strip()
    m = DATE_RE1.search(s)
    if m:
        mm = int(m.group("m"))
        dd = int(m.group("d"))
        yy = int(m.group("y"))
        if yy < 100:
            yy += 2000
        return yy, mm, dd
    m = DATE_RE2.search(s)
    if m:
        yy = int(m.group("y"))
        mm = int(m.group("m"))
        dd = int(m.group("d"))
        return yy, mm, dd
    return None


def parse_time_window(raw: Optional[str], tz_name: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Parses things like:
      - '12/08/2025 12:00'
      - '12/09/2025 08:00 - 15:00'
    Returns timezone-aware datetimes in tz_name.
    """
    if not raw:
        return None, None
    s = " ".join(raw.replace("\u2013", "-").split())  # normalize whitespace and en-dash
    d = _parse_date(s)
    if not d:
        return None, None

    times = TIME_RE.findall(s)
    if not times:
        return None, None

    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    y, m, day = d
    # first time is start
    start_h, start_min = int(times[0][0]), int(times[0][1])
    start = datetime(y, m, day, start_h, start_min, tzinfo=tz)

    end = None
    if len(times) >= 2:
        end_h, end_min = int(times[1][0]), int(times[1][1])
        end = datetime(y, m, day, end_h, end_min, tzinfo=tz)
        if end < start:
            # assume end is next day
            end = end + timedelta(days=1)

    return start, end


# ------------------ Dispatch parsing (detailed & summary) ------------------

PU_TIME_RE = re.compile(r"^\s*PU time:\s*(.+?)\s*$", re.IGNORECASE)
DEL_TIME_RE = re.compile(r"^\s*DEL time:\s*(.+?)\s*$", re.IGNORECASE)
PU_ADDR_RE = re.compile(r"^\s*PU Address\s*:\s*(.*)$", re.IGNORECASE)
DEL_ADDR_RE = re.compile(r"^\s*DEL Address(?:\s*\d+)?\s*:\s*(.*)$", re.IGNORECASE)

LOAD_NUM_RE = re.compile(r"^\s*Load Number\s*:\s*(.+?)\s*$", re.IGNORECASE)
LOAD_DATE_RE = re.compile(r"^\s*Load Date\s*:\s*(.+?)\s*$", re.IGNORECASE)
WEIGHT_RE = re.compile(r"^\s*Expected Weight\s*:\s*(.+?)\s*$", re.IGNORECASE)
PICKUP_RE = re.compile(r"^\s*Pickup\s*:\s*(.+?)\s*$", re.IGNORECASE)
DELIVERY_RE = re.compile(r"^\s*Delivery\s*:\s*(.+?)\s*$", re.IGNORECASE)


def _collect_block_from(lines: List[str], start_idx: int, first_after: str) -> Tuple[List[str], int]:
    stop_prefixes = ("pu time:", "del time:", "pu address", "del address", "pickup:", "delivery:")
    block: List[str] = []
    if first_after.strip():
        block.append(first_after.strip())
    j = start_idx + 1
    while j < len(lines):
        s = lines[j].strip()
        if not s:
            break
        low = s.lower()
        if any(low.startswith(p) for p in stop_prefixes):
            break
        if set(s) <= {"-"} or set(s) <= {"="}:
            break
        block.append(s)
        j += 1
    return block, j


def parse_detailed_format(text: str) -> Optional[dict]:
    low = text.lower()
    if "pu address" not in low or "del address" not in low:
        return None

    lines = [ln.rstrip() for ln in text.splitlines()]

    pu_time: Optional[str] = None
    for ln in lines:
        m = PU_TIME_RE.match(ln)
        if m:
            pu_time = m.group(1).strip()
            break

    pickup_lines: Optional[List[str]] = None
    pickup_addr: Optional[str] = None
    for i, ln in enumerate(lines):
        m = PU_ADDR_RE.match(ln)
        if m:
            after = m.group(1).strip()
            block, _ = _collect_block_from(lines, i, after)
            if block:
                pickup_lines = block
                pickup_addr = ", ".join(block)
                break
    if not pickup_addr:
        return None

    deliveries: List[dict] = []
    current_del_time: Optional[str] = None
    i = 0
    while i < len(lines):
        ln = lines[i].strip()
        m = DEL_TIME_RE.match(ln)
        if m:
            current_del_time = m.group(1).strip()
            i += 1
            continue

        m = DEL_ADDR_RE.match(ln)
        if m:
            after = m.group(1).strip()
            block, j = _collect_block_from(lines, i, after)
            if block:
                deliveries.append({"time": current_del_time, "lines": block, "address": ", ".join(block)})
            i = j
            continue

        i += 1

    if not deliveries:
        return None

    job_key = f"{pickup_addr}|{pu_time or ''}|" + "|".join(
        f"{d.get('address','')}|{d.get('time','') or ''}" for d in deliveries
    )
    job_id = hashlib.sha1(job_key.encode("utf-8")).hexdigest()[:10]

    return init_job_extras(
        {
            "job_id": job_id,
            "set_at": now_utc_iso(),
            "source_format": "detailed",
            "meta": {},
            "pickup": {"time": pu_time, "lines": pickup_lines or [pickup_addr], "address": pickup_addr},
            "deliveries": deliveries,
        }
    )


def _is_timeish(s: str) -> bool:
    s = s.strip()
    if not s:
        return False
    if re.search(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b", s):
        return True
    if re.search(r"\b\d{4}-\d{2}-\d{2}\b", s):
        return True
    if re.search(r"\b\d{1,2}:\d{2}\b", s):
        return True
    return False


def parse_summary_format(text: str) -> Optional[dict]:
    low = text.lower()
    if "pickup:" not in low or "delivery:" not in low:
        return None

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    meta: Dict[str, str] = {}
    pickup_addr: Optional[str] = None
    pickup_time: Optional[str] = None
    deliveries: List[dict] = []
    pending: Optional[dict] = None

    for ln in lines:
        m = LOAD_NUM_RE.match(ln)
        if m:
            meta["load_number"] = m.group(1).strip()
            continue
        m = LOAD_DATE_RE.match(ln)
        if m:
            meta["load_date"] = m.group(1).strip()
            continue
        m = WEIGHT_RE.match(ln)
        if m:
            meta["expected_weight"] = m.group(1).strip()
            continue

        m = PICKUP_RE.match(ln)
        if m:
            val = m.group(1).strip()
            if _is_timeish(val):
                pickup_time = val
            else:
                pickup_addr = val
            continue

        m = DELIVERY_RE.match(ln)
        if m:
            val = m.group(1).strip()
            if _is_timeish(val):
                # time line belongs to the most recent delivery stop
                if pending is None:
                    stop = {"time": val, "lines": [], "address": ""}
                    deliveries.append(stop)
                    pending = stop
                else:
                    pending["time"] = val
                    pending = None
            else:
                stop = {"time": None, "lines": [val], "address": val}
                deliveries.append(stop)
                pending = stop
            continue

    if not pickup_addr or not deliveries:
        return None

    cleaned: List[dict] = []
    for d in deliveries:
        addr = (d.get("address") or ", ".join(d.get("lines") or [])).strip()
        if not addr:
            continue
        cleaned.append({"time": d.get("time"), "lines": d.get("lines") or [addr], "address": addr})
    if not cleaned:
        return None

    job_key = f"{meta.get('load_number','')}|{pickup_addr}|{pickup_time or ''}|" + "|".join(
        f"{d['address']}|{d.get('time') or ''}" for d in cleaned
    )
    job_id = hashlib.sha1(job_key.encode("utf-8")).hexdigest()[:10]

    return init_job_extras(
        {
            "job_id": job_id,
            "set_at": now_utc_iso(),
            "source_format": "summary",
            "meta": meta,
            "pickup": {"time": pickup_time, "lines": [pickup_addr], "address": pickup_addr},
            "deliveries": cleaned,
        }
    )


def parse_job_from_text(text: str) -> Optional[dict]:
    job = parse_detailed_format(text)
    if job:
        return job
    return parse_summary_format(text)


# ------------------ ETA utilities ------------------

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def fallback_seconds_for_distance_m(meters: float) -> float:
    km = meters / 1000.0
    if km < 80:
        speed_kph = 55
    elif km < 320:
        speed_kph = 85
    else:
        speed_kph = 105
    return (km / speed_kph) * 3600.0


def _strip_suite_unit(s: str) -> str:
    return re.sub(r"\b(?:suite|ste|unit|#)\s*[\w\-]+\b", "", s, flags=re.IGNORECASE).strip()


def address_variants(address: str) -> List[str]:
    base = " ".join(address.strip().split())
    if not base:
        return []

    variants: List[str] = [base]
    parts = [p.strip() for p in base.split(",") if p.strip()]

    if len(parts) >= 2:
        variants.append(", ".join(parts[1:]))

    variants.append(_strip_suite_unit(base))
    if len(parts) >= 2:
        variants.append(_strip_suite_unit(", ".join(parts[1:])))

    if len(parts) >= 2:
        variants.append(", ".join(parts[-2:]))
    if len(parts) >= 3:
        variants.append(", ".join(parts[-3:]))

    out: List[str] = []
    seen = set()
    for v in variants:
        v2 = " ".join(v.split())
        if v2 and v2 not in seen:
            seen.add(v2)
            out.append(v2)
    return out


async def _nominatim_get(client: httpx.AsyncClient, params: dict) -> httpx.Response:
    global _nominatim_last_request
    async with _nominatim_lock:
        now = time.monotonic()
        wait = (_nominatim_last_request + NOMINATIM_MIN_INTERVAL) - now
        if wait > 0:
            await asyncio.sleep(wait)
        resp = await client.get(NOMINATIM_URL, params=params)
        _nominatim_last_request = time.monotonic()
        return resp


async def geocode(address: str) -> Optional[Tuple[float, float]]:
    headers = {"User-Agent": NOMINATIM_USER_AGENT}
    candidates = address_variants(address)
    if not candidates:
        return None

    try:
        async with httpx.AsyncClient(timeout=15.0, headers=headers) as client:
            for q in candidates:
                resp = await _nominatim_get(client, {"q": q, "format": "jsonv2", "limit": 1})
                if resp.status_code >= 400:
                    continue
                data = resp.json()
                if data:
                    return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        return None

    return None


async def route(origin: Tuple[float, float], dest: Tuple[float, float]) -> Optional[Tuple[float, float]]:
    lat1, lon1 = origin
    lat2, lon2 = dest
    url = OSRM_URL.format(lon1=lon1, lat1=lat1, lon2=lon2, lat2=lat2)
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, params={"overview": "false"})
            if r.status_code >= 400:
                return None
            js = r.json()
            routes = js.get("routes") or []
            if not routes:
                return None
            return float(routes[0]["distance"]), float(routes[0]["duration"])
    except Exception:
        return None


async def get_coords_cached(state: dict, address: str) -> Optional[Tuple[float, float]]:
    cache = state.get("geocode_cache") or {}
    if address in cache:
        try:
            return float(cache[address]["lat"]), float(cache[address]["lon"])
        except Exception:
            pass

    coords = await geocode(address)
    if coords:
        tz_name = best_timezone_for_coords(coords[0], coords[1])
        cache[address] = {"lat": coords[0], "lon": coords[1], "tz": tz_name}
        state["geocode_cache"] = cache
        async with _state_lock:
            # reload+merge to avoid clobber
            s2 = load_state()
            s2.setdefault("geocode_cache", {})
            s2["geocode_cache"][address] = cache[address]
            save_state(s2)
        return coords
    return None


def get_cached_tz(state: dict, address: str) -> Optional[str]:
    cache = state.get("geocode_cache") or {}
    if address in cache and isinstance(cache[address], dict):
        tz = cache[address].get("tz")
        if isinstance(tz, str) and tz:
            return tz
    return None


async def compute_eta(state: dict, origin: Tuple[float, float], label: str, address: str) -> dict:
    dest = await get_coords_cached(state, address)
    if not dest:
        return {"ok": False, "error": f"Couldn't geocode {label} address."}

    r = await route(origin, dest)
    if r:
        dist_m, dur_s = r
        return {"ok": True, "distance_m": dist_m, "duration_s": dur_s, "method": "osrm"}

    dist_m = haversine_m(origin[0], origin[1], dest[0], dest[1])
    dur_s = fallback_seconds_for_distance_m(dist_m)
    return {"ok": True, "distance_m": dist_m, "duration_s": dur_s, "method": "approx"}


# ------------------ UI: Lifecycle keyboard & cards ------------------

def cb(*parts: str) -> str:
    return "|".join(parts)


def _btn(label: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(label, callback_data=data)


def build_actions_keyboard(state: dict, job: dict) -> InlineKeyboardMarkup:
    stage = state.get("job_stage", "PU")
    del_index = int(state.get("del_index", 0))
    pu = job.get("pickup") or {}
    pu_status = (pu.get("status") or {})
    pu_docs = (pu.get("docs") or {})

    deliveries = job.get("deliveries") or []
    cur_del = deliveries[del_index] if deliveries and 0 <= del_index < len(deliveries) else None
    del_status = (cur_del.get("status") or {}) if cur_del else {}
    del_docs = (cur_del.get("docs") or {}) if cur_del else {}

    rows: List[List[InlineKeyboardButton]] = []

    # Lifecycle row
    if stage == "PU":
        rows.append(
            [
                _btn("✅ Arrived PU" if pu_status.get("arrived_at") else "Arrived PU", cb("PU", "ARRIVED")),
                _btn("✅ Loaded" if pu_status.get("loaded_at") else "Loaded", cb("PU", "LOADED")),
                _btn("✅ Departed PU" if pu_status.get("departed_at") else "Departed PU", cb("PU", "DEPARTED")),
            ]
        )
        rows.append(
            [
                _btn("PTI ✅" if (pu_docs.get("pti_video") or {}).get("done") else "PTI", cb("DOC", "PTI")),
                _btn("BOL ✅" if (pu_docs.get("bol") or {}).get("done") else "BOL", cb("DOC", "BOL")),
                _btn("Skip PU", cb("PU", "SKIP")),
            ]
        )
    else:
        dlabel = f"DEL {del_index+1}/{len(deliveries)}" if deliveries else "DEL"
        rows.append(
            [
                _btn("✅ Arrived " + dlabel if del_status.get("arrived_at") else "Arrived " + dlabel, cb("DEL", "ARRIVED")),
                _btn("✅ Delivered" if del_status.get("delivered_at") else "Delivered", cb("DEL", "DELIVERED")),
                _btn("Next Stop ➡️", cb("DEL", "NEXT")),
            ]
        )
        rows.append(
            [
                _btn("POD ✅" if (del_docs.get("pod") or {}).get("done") else "POD", cb("DOC", "POD")),
                _btn("Skip Stop", cb("DEL", "SKIP")),
                _btn("Departed", cb("DEL", "DEPARTED")),
            ]
        )

    # ETA & views
    rows.append(
        [
            _btn("ETA", cb("ETA", "AUTO")),
            _btn("ETA all", cb("ETA", "ALL")),
            _btn("Stops", cb("SHOW", "STOPS")),
        ]
    )
    rows.append(
        [
            _btn("🖼 Big Card", cb("CARD", "AUTO")),
            _btn("📋 Report", cb("SHOW", "REPORT")),
        ]
    )

    return InlineKeyboardMarkup(rows)


def _text_size(draw: Any, text: str, font: Any) -> Tuple[int, int]:
    if hasattr(draw, "textbbox"):
        box = draw.textbbox((0, 0), text, font=font)
        return box[2] - box[0], box[3] - box[1]
    # fallback
    return draw.textsize(text, font=font)  # type: ignore


def render_card(title: str, lines: List[str]) -> Optional[io.BytesIO]:
    if Image is None:
        return None

    width = 1080
    margin = 64
    title_size = 88
    body_size = 64
    line_gap = 18

    # Try common font paths
    font_candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    ]

    def load_font(size: int) -> Any:
        for p in font_candidates:
            if os.path.exists(p):
                try:
                    return ImageFont.truetype(p, size=size)
                except Exception:
                    continue
        return ImageFont.load_default()

    title_font = load_font(title_size)
    body_font = load_font(body_size)

    # measure height
    tmp = Image.new("RGB", (width, 100), "white")
    d = ImageDraw.Draw(tmp)

    _, th = _text_size(d, title, title_font)
    height = margin + th + line_gap
    for ln in lines:
        _, lh = _text_size(d, ln, body_font)
        height += lh + line_gap
    height += margin
    height = max(500, min(1400, height))

    img = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(img)

    y = margin
    draw.text((margin, y), title, font=title_font, fill="black")
    y += th + line_gap

    for ln in lines:
        draw.text((margin, y), ln, font=body_font, fill="black")
        _, lh = _text_size(draw, ln, body_font)
        y += lh + line_gap

    buf = io.BytesIO()
    buf.name = "eta.png"
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf


# ------------------ Commands ------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    triggers = " / ".join(sorted(TRIGGERS))
    await update.effective_message.reply_text(
        "👋 Dispatch + Driver Assist Bot\n\n"
        f"Triggers in allowed groups: {triggers}\n"
        "Examples:\n"
        "• eta\n"
        "• eta pu | eta del | eta all\n"
        "• eta card (big image)\n\n"
        "Owner setup:\n"
        "1) DM: /claim <code>\n"
        "2) DM: /update (send location OR Share Live Location)\n"
        "3) Group: /allowhere\n\n"
        "Useful:\n"
        "• /panel (shows buttons)\n"
        "• /report (dwell + on-time)\n"
        "• /leave (leave group)\n"
        "• /deleteall (admin only)\n"
    )


async def claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        await update.effective_message.reply_text("Please DM me /claim (for safety).")
        return
    if not CLAIM_CODE:
        await update.effective_message.reply_text("Missing CLAIM_CODE in Railway Variables.")
        return

    code = " ".join(context.args or []).strip()
    if not code:
        await update.effective_message.reply_text("Use: /claim <your_code>")
        return
    if code != CLAIM_CODE:
        await update.effective_message.reply_text("❌ Wrong claim code.")
        return

    async with _state_lock:
        state = load_state()
        state["owner_id"] = update.effective_user.id
        save_state(state)

    await update.effective_message.reply_text("✅ You are now the owner.")


async def allowhere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        if not is_owner(update, state):
            await update.effective_message.reply_text("Only the owner can do that.")
            return
        if not is_group(update):
            await update.effective_message.reply_text("Run this inside the group you want to allow.")
            return

        chat_id = update.effective_chat.id
        allowed = set(state.get("allowed_chats") or [])
        allowed.add(chat_id)
        state["allowed_chats"] = sorted(list(allowed))
        # also: if we don't have a job chat yet, set to here
        state.setdefault("job_chat_id", chat_id)
        save_state(state)

    await update.effective_message.reply_text("✅ This group is allowed.")


async def disallowhere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        if not is_owner(update, state):
            await update.effective_message.reply_text("Only the owner can do that.")
            return
        if not is_group(update):
            await update.effective_message.reply_text("Run this inside the group you want to remove.")
            return

        chat_id = update.effective_chat.id
        allowed = set(state.get("allowed_chats") or [])
        allowed.discard(chat_id)
        state["allowed_chats"] = sorted(list(allowed))
        save_state(state)

    await update.effective_message.reply_text("✅ Group removed from allowed list.")


async def update_loc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        if not is_owner(update, state):
            await update.effective_message.reply_text("Only the owner can update the saved location.")
            return
    if not is_private(update):
        await update.effective_message.reply_text("Please DM me /update (best).")
        return

    kb = [[KeyboardButton("📍 Send my current location", request_location=True)]]
    await update.effective_message.reply_text(
        "Tap the button to send your current location.\n"
        "Tip: you can also Share Live Location (Attach → Location → Share Live Location) and I’ll keep updating.",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True),
    )


async def on_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        if not is_owner(update, state):
            return

        msg = update.effective_message
        if not msg or not msg.location:
            return

        loc = msg.location
        tz_name = best_timezone_for_coords(loc.latitude, loc.longitude)
        state["last_location"] = {
            "lat": loc.latitude,
            "lon": loc.longitude,
            "updated_at": now_utc_iso(),
            "tz": tz_name,
        }
        save_state(state)

    # only confirm on initial location messages (edited live-location updates won't spam)
    if update.message is not None:
        await update.effective_message.reply_text("✅ Saved your location.", reply_markup=ReplyKeyboardRemove())


async def panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        chat = update.effective_chat
        if not chat:
            return
        if is_group(update) and not chat_allowed(state, chat.id):
            return
        if is_private(update) and not is_owner(update, state):
            return

        job = normalize_job(state.get("job"))
        if not job:
            await update.effective_message.reply_text("No active load detected yet.")
            return
        job = init_job_extras(job)
        save_state({**state, "job": job})

    text = await build_status_text(state, job, include_long=False)
    await update.effective_message.reply_text(
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=build_actions_keyboard(state, job),
    )


async def leave(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        if not is_owner(update, state):
            await update.effective_message.reply_text("Only the owner can make me leave.")
            return

        chat = update.effective_chat
        if not chat or chat.type == "private":
            await update.effective_message.reply_text("I can’t leave private chats. Just delete/block the bot chat.")
            return

        allowed = set(state.get("allowed_chats") or [])
        allowed.discard(chat.id)
        state["allowed_chats"] = sorted(list(allowed))
        save_state(state)

    await update.effective_message.reply_text("👋 Leaving this chat.")
    await context.bot.leave_chat(update.effective_chat.id)


async def deleteall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        if not is_owner(update, state):
            await update.effective_message.reply_text("Only the owner can run /deleteall.")
            return

    chat = update.effective_chat
    if not chat:
        return
    if chat.type == "private":
        await update.effective_message.reply_text(
            "Bots can’t “clear” a whole private chat history. You can delete the chat from your side."
        )
        return

    count = DELETEALL_DEFAULT
    if context.args:
        a = context.args[0].strip().lower()
        if a in ("all", "max"):
            count = DELETEALL_MAX
        else:
            try:
                count = int(a)
            except ValueError:
                await update.effective_message.reply_text("Use: /deleteall [number|all]")
                return
    count = max(1, min(count, DELETEALL_MAX))

    notice = await update.effective_message.reply_text(f"🧹 Deleting up to {count} recent messages…")
    start_id = notice.message_id
    end_id = max(1, start_id - count + 1)

    async def delete_one(mid: int) -> None:
        try:
            await context.bot.delete_message(chat_id=chat.id, message_id=mid)
        except Exception:
            pass

    try:
        # Prefer bulk delete if available (PTB 22+), but fall back safely.
        if hasattr(context.bot, "delete_messages"):
            for chunk_end in range(start_id, end_id - 1, -100):
                chunk_start = max(end_id, chunk_end - 99)
                ids = list(range(chunk_start, chunk_end + 1))
                try:
                    await context.bot.delete_messages(chat_id=chat.id, message_ids=ids)  # type: ignore
                except RetryAfter as e:
                    await asyncio.sleep(float(getattr(e, "retry_after", 1.0)) + 0.2)
                    await context.bot.delete_messages(chat_id=chat.id, message_ids=ids)  # type: ignore
                except (Forbidden, BadRequest):
                    # fallback to single deletions
                    for mid in ids:
                        await delete_one(mid)
                        await asyncio.sleep(0.03)
                await asyncio.sleep(0.05)
        else:
            for mid in range(start_id, end_id - 1, -1):
                await delete_one(mid)
                await asyncio.sleep(0.03)
    except Forbidden:
        try:
            await notice.edit_text("❌ I need admin 'Delete messages' permission in this chat.")
        except Exception:
            pass
    except TelegramError:
        try:
            await notice.edit_text("⚠️ Couldn’t delete messages (permissions or Telegram limits).")
        except Exception:
            pass


async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        chat = update.effective_chat
        if not chat:
            return
        if is_group(update) and not chat_allowed(state, chat.id):
            return
        if is_private(update) and not is_owner(update, state):
            return

        job = normalize_job(state.get("job"))
        if not job:
            await update.effective_message.reply_text("No active job.")
            return
        job = init_job_extras(job)

    text = build_report_text(state, job)
    await update.effective_message.reply_text(text, parse_mode="HTML", disable_web_page_preview=True)


# ------------------ Status text ------------------

async def build_status_text(state: dict, job: dict, include_long: bool = True) -> str:
    loc = state.get("last_location")
    tz_now = (loc or {}).get("tz") or "UTC"
    updated_dt = parse_iso((loc or {}).get("updated_at", "")) or now_utc()

    stage = state.get("job_stage", "PU")
    del_index = int(state.get("del_index", 0))
    deliveries = job.get("deliveries") or []

    meta = job.get("meta") or {}
    out: List[str] = ["<b>📦 Load</b>"]
    if meta.get("load_number"):
        out.append(f"<b>Load #:</b> {h(meta['load_number'])}")
    if meta.get("load_date"):
        out.append(f"<b>Date:</b> {h(meta['load_date'])}")
    if meta.get("expected_weight"):
        out.append(f"<b>Weight:</b> {h(meta['expected_weight'])}")

    out.append("")
    out.append("<b>🕒 Your time:</b> " + h(local_time_str(tz_now)))
    out.append("<b>📍 GPS updated:</b> " + h(format_delta(updated_dt)))

    out.append("")
    if stage == "PU":
        out.append("<b>Stage:</b> PICKUP")
    else:
        out.append(
            f"<b>Stage:</b> DELIVERY {del_index+1}/{len(deliveries)}" if deliveries else "<b>Stage:</b> DELIVERY"
        )

    if include_long:
        out.append("")
        out.append(job_html(job, stage, del_index))

    return "\n".join(out)


def job_html(job: dict, stage: str, del_index: int) -> str:
    pu = job.get("pickup") or {}
    deliveries = job.get("deliveries") or []

    out: List[str] = ["<b>Pickup</b>"]
    if pu.get("time"):
        out.append(f"⏱ {h(pu['time'])}")
    for ln in (pu.get("lines") or []):
        out.append(h(ln))

    out.append("")
    for i, d in enumerate(deliveries):
        title = f"Delivery {i+1}"
        if stage == "DEL" and i == int(del_index):
            title = f"➡️ {title} (current)"
        out.append(f"<b>{h(title)}</b>")
        if d.get("time"):
            out.append(f"⏱ {h(d['time'])}")
        for ln in (d.get("lines") or []):
            out.append(h(ln))
        if i != len(deliveries) - 1:
            out.append("")

    return "\n".join(out)


def build_report_text(state: dict, job: dict) -> str:
    stage = state.get("job_stage", "PU")
    del_index = int(state.get("del_index", 0))
    deliveries = job.get("deliveries") or []

    lines: List[str] = ["<b>📋 Report</b>"]

    # Dwell times
    pu = job.get("pickup") or {}
    pu_status = pu.get("status") or {}

    def dwell(label: str, start_iso: Optional[str], end_iso: Optional[str]) -> str:
        sdt = parse_iso(start_iso or "")
        edt = parse_iso(end_iso or "")
        if not sdt:
            return f"{label}: —"
        if not edt:
            seconds = max(0, (now_utc() - sdt).total_seconds())
            return f"{label}: {fmt_duration(seconds)} (so far)"
        seconds = max(0, (edt - sdt).total_seconds())
        return f"{label}: {fmt_duration(seconds)}"

    lines.append(dwell("Pickup dwell", pu_status.get("arrived_at"), pu_status.get("departed_at")))

    for i, d in enumerate(deliveries):
        st = d.get("status") or {}
        lines.append(dwell(f"DEL {i+1} dwell", st.get("arrived_at"), st.get("departed_at")))

    # On-time checks (simple)
    lines.append("")
    lines.append("<b>⏱ On-time (simple)</b>")

    def on_time(name: str, scheduled_raw: Optional[str], actual_iso: Optional[str], tz_name: str) -> str:
        sched_start, sched_end = parse_time_window(scheduled_raw, tz_name)
        act = parse_iso(actual_iso or "")
        if not sched_start or not act:
            return f"{name}: —"
        # compare in same tz
        act_local = act.astimezone(sched_start.tzinfo or timezone.utc)
        grace = timedelta(minutes=SCHEDULE_GRACE_MIN)
        if sched_end:
            ok = act_local <= sched_end
            return f"{name}: {'✅ on time' if ok else '❌ late'}"
        ok = act_local <= (sched_start + grace)
        return f"{name}: {'✅ on time' if ok else '❌ late'}"

    # pickup timezone: try from geocode cache
    pu_tz = get_cached_tz(state, pu.get("address", "")) or ((state.get("last_location") or {}).get("tz") or "UTC")
    lines.append(on_time("PU arrival", pu.get("time"), pu_status.get("arrived_at"), pu_tz))

    for i, d in enumerate(deliveries):
        tz_name = get_cached_tz(state, d.get("address", "")) or ((state.get("last_location") or {}).get("tz") or "UTC")
        st = d.get("status") or {}
        # prefer delivered_at, else arrived_at
        actual = st.get("delivered_at") or st.get("arrived_at")
        lines.append(on_time(f"DEL {i+1}", d.get("time"), actual, tz_name))

    # Total route estimate (pickup -> deliveries)
    lines.append("")
    total_m = 0.0
    coords_ok = True
    cache = state.get("geocode_cache") or {}

    def coords_for(addr: str) -> Optional[Tuple[float, float]]:
        if addr in cache and isinstance(cache[addr], dict) and "lat" in cache[addr] and "lon" in cache[addr]:
            try:
                return float(cache[addr]["lat"]), float(cache[addr]["lon"])
            except Exception:
                return None
        return None

    pu_addr = pu.get("address") or ""
    last = coords_for(pu_addr)
    if not last:
        coords_ok = False
    else:
        for d in deliveries:
            c = coords_for(d.get("address") or "")
            if not c:
                coords_ok = False
                break
            total_m += haversine_m(last[0], last[1], c[0], c[1])
            last = c

    if coords_ok and deliveries:
        total_s = fallback_seconds_for_distance_m(total_m)
        lines.append(f"<b>Route est:</b> {h(fmt_distance_miles(total_m))} · {h(fmt_duration(total_s))} (no traffic)")
    else:
        lines.append("<b>Route est:</b> (needs geocodes — run 'eta all' once to cache)")

    # Current stop summary
    lines.append("")
    if stage == "PU":
        lines.append("<b>Current:</b> Pickup")
    else:
        lines.append(f"<b>Current:</b> Delivery {del_index+1}/{len(deliveries)}" if deliveries else "<b>Current:</b> Delivery")

    return "\n".join(lines)


# ------------------ ETA response ------------------

async def send_eta(update: Update, context: ContextTypes.DEFAULT_TYPE, target: str = "AUTO", card: bool = False):
    async with _state_lock:
        state = load_state()

    chat = update.effective_chat
    msg = update.effective_message
    if not chat or not msg:
        return

    if is_group(update) and not chat_allowed(state, chat.id):
        return
    if is_private(update) and not is_owner(update, state):
        return

    loc = state.get("last_location")
    if not loc:
        await msg.reply_text("No saved location yet. Owner: DM /update (or share Live Location).")
        return

    origin = (float(loc["lat"]), float(loc["lon"]))
    tz_now = loc.get("tz") or "UTC"
    updated_dt = parse_iso(loc.get("updated_at", "")) or now_utc()

    job = normalize_job(state.get("job"))
    if not job:
        await context.bot.send_location(chat_id=chat.id, latitude=origin[0], longitude=origin[1])
        await msg.reply_text(
            "\n".join(
                [
                    "<b>🚚 ETA</b>",
                    f"<b>Local time:</b> {h(local_time_str(tz_now))}",
                    f"<b>GPS updated:</b> {h(format_delta(updated_dt))}",
                    "",
                    "<i>No active load detected yet.</i>",
                ]
            ),
            parse_mode="HTML",
        )
        return

    job = init_job_extras(job)
    stage = state.get("job_stage", "PU")
    del_index = int(state.get("del_index", 0))
    deliveries = job.get("deliveries") or []
    if stage == "DEL" and deliveries:
        del_index = max(0, min(del_index, len(deliveries) - 1))

    # ensure save normalized job + clamped del_index
    async with _state_lock:
        s2 = load_state()
        s2["job"] = job
        s2["del_index"] = del_index
        save_state(s2)

    # Always send live location pin first
    await context.bot.send_location(chat_id=chat.id, latitude=origin[0], longitude=origin[1])

    header = [
        "<b>🚚 ETA</b>",
        f"<b>Local time:</b> {h(local_time_str(tz_now))}",
        f"<b>GPS updated:</b> {h(format_delta(updated_dt))}",
        "",
    ]
    header.append(
        f"<b>Stage:</b> PICKUP" if stage == "PU" else (f"<b>Stage:</b> DELIVERY {del_index+1}/{len(deliveries)}" if deliveries else "<b>Stage:</b> DELIVERY")
    )

    # Decide what to compute
    t = target.upper()
    want_pickup = False
    want_deliveries: List[Tuple[int, dict]] = []
    if t == "AUTO":
        if stage == "PU":
            want_pickup = True
        else:
            cur = get_current_delivery(job, del_index)
            if cur:
                want_deliveries = [cur]
    elif t == "PU":
        want_pickup = True
    elif t == "DEL":
        cur = get_current_delivery(job, del_index)
        if cur:
            want_deliveries = [cur]
    elif t == "BOTH":
        want_pickup = True
        cur = get_current_delivery(job, del_index)
        if cur:
            want_deliveries = [cur]
    elif t == "ALL":
        want_pickup = True
        want_deliveries = list(enumerate(deliveries[: max(1, ETA_ALL_MAX_STOPS)]))
    else:
        want_pickup = True

    lines: List[str] = []
    card_lines: List[str] = []

    # pickup ETA
    if want_pickup:
        pu = job.get("pickup") or {}
        pu_addr = pu.get("address") or ", ".join(pu.get("lines") or [])
        lines += ["", "<b>ETA to Pickup</b>"]
        r = await compute_eta(state, origin, "Pickup", pu_addr)
        if r.get("ok"):
            lines.append(f"🛣 {h(fmt_distance_miles(r['distance_m']))} · ⏳ {h(fmt_duration(r['duration_s']))} ({h(r['method'])})")
            arrive = now_utc().astimezone(ZoneInfo(tz_now) if tz_now else timezone.utc) + timedelta(seconds=float(r["duration_s"]))
            lines.append(f"🕒 Arrive ~ {h(arrive.strftime('%H:%M'))}")
            card_lines = [
                "To PICKUP",
                f"{fmt_duration(r['duration_s'])}  •  {fmt_distance_miles(r['distance_m'])}",
                f"Arrive ~ {arrive.strftime('%H:%M')} ({tz_now})",
            ]
        else:
            lines.append(f"⚠️ {h(r.get('error', 'Could not compute'))}")
            card_lines = ["To PICKUP", "Could not compute ETA"]

    # delivery ETA(s)
    for i, d in want_deliveries:
        addr = d.get("address") or ", ".join(d.get("lines") or [])
        title = f"<b>ETA to Delivery {i+1}</b>" if t == "ALL" else "<b>ETA to Delivery</b>"
        lines += ["", title]
        r = await compute_eta(state, origin, f"Delivery {i+1}", addr)
        if r.get("ok"):
            lines.append(f"🛣 {h(fmt_distance_miles(r['distance_m']))} · ⏳ {h(fmt_duration(r['duration_s']))} ({h(r['method'])})")
            arrive = now_utc().astimezone(ZoneInfo(tz_now) if tz_now else timezone.utc) + timedelta(seconds=float(r["duration_s"]))
            lines.append(f"🕒 Arrive ~ {h(arrive.strftime('%H:%M'))}")
            if not card_lines:
                stop_label = f"To DEL {i+1}/{len(deliveries)}" if deliveries else "To DELIVERY"
                card_lines = [
                    stop_label,
                    f"{fmt_duration(r['duration_s'])}  •  {fmt_distance_miles(r['distance_m'])}",
                    f"Arrive ~ {arrive.strftime('%H:%M')} ({tz_now})",
                ]
        else:
            lines.append(f"⚠️ {h(r.get('error', 'Could not compute'))}")
            if not card_lines:
                card_lines = ["To DELIVERY", "Could not compute ETA"]

    if t == "ALL" and len(deliveries) > ETA_ALL_MAX_STOPS:
        lines += ["", f"<i>Showing first {ETA_ALL_MAX_STOPS} of {len(deliveries)} deliveries.</i>"]

    # Send text with buttons
    status = await build_status_text(state, job, include_long=False)
    text = "\n".join([status] + header + lines)
    await msg.reply_text(
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=build_actions_keyboard(state, job),
    )

    # Send optional big card
    if card:
        buf = render_card("ETA", card_lines or ["No data"])
        if buf is None:
            await msg.reply_text("Big-card images require Pillow. Add `Pillow` to requirements.txt.")
        else:
            await context.bot.send_photo(chat_id=chat.id, photo=buf)


# ------------------ Callback actions ------------------

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()  # stop the loading spinner

    async with _state_lock:
        state = load_state()
    if update.effective_chat and is_group(update) and not chat_allowed(state, update.effective_chat.id):
        return
    if not is_owner(update, state):
        await query.answer("Owner only.", show_alert=False)
        return

    parts = query.data.split("|")
    if not parts:
        return

    action = parts[0]

    # quick views
    if action == "ETA":
        which = parts[1] if len(parts) > 1 else "AUTO"
        await send_eta(update, context, target=which, card=False)
        return

    if action == "CARD":
        await send_eta(update, context, target="AUTO", card=True)
        return

    if action == "SHOW":
        sub = parts[1] if len(parts) > 1 else ""
        if sub == "STOPS":
            await stops_cmd(update, context)
        elif sub == "REPORT":
            await report_cmd(update, context)
        return

    # lifecycle & docs need job
    async with _state_lock:
        state = load_state()
        job = normalize_job(state.get("job"))
        if not job:
            return
        job = init_job_extras(job)

        stage = state.get("job_stage", "PU")
        del_index = int(state.get("del_index", 0))
        deliveries = job.get("deliveries") or []
        if stage == "DEL" and deliveries:
            del_index = max(0, min(del_index, len(deliveries) - 1))
            state["del_index"] = del_index

        changed = False
        uid = update.effective_user.id if update.effective_user else None

        def set_iso(obj: dict, key: str) -> None:
            obj[key] = now_utc_iso()

        if action == "PU":
            sub = parts[1] if len(parts) > 1 else ""
            pu = job.get("pickup") or {}
            st = pu.get("status") or {}
            if sub == "ARRIVED":
                if not st.get("arrived_at"):
                    set_iso(st, "arrived_at")
                    record_event(job, "pickup_arrived", uid)
                    changed = True
            elif sub == "LOADED":
                if not st.get("loaded_at"):
                    set_iso(st, "loaded_at")
                    record_event(job, "pickup_loaded", uid)
                    changed = True
            elif sub == "DEPARTED":
                if not st.get("departed_at"):
                    set_iso(st, "departed_at")
                    record_event(job, "pickup_departed", uid)
                    changed = True
                # switch to deliveries automatically
                state["job_stage"] = "DEL"
                state["del_index"] = 0
                changed = True
            elif sub == "SKIP":
                # skip pickup
                state["job_stage"] = "DEL"
                state["del_index"] = 0
                record_event(job, "pickup_skipped", uid)
                changed = True

            pu["status"] = st
            job["pickup"] = pu

        elif action == "DEL":
            sub = parts[1] if len(parts) > 1 else ""
            if deliveries:
                cur = deliveries[del_index]
                st = cur.get("status") or {}
                if sub == "ARRIVED":
                    if not st.get("arrived_at"):
                        set_iso(st, "arrived_at")
                        record_event(job, f"del_{del_index+1}_arrived", uid)
                        changed = True
                elif sub == "DELIVERED":
                    if not st.get("delivered_at"):
                        set_iso(st, "delivered_at")
                        record_event(job, f"del_{del_index+1}_delivered", uid)
                        changed = True
                elif sub == "DEPARTED":
                    if not st.get("departed_at"):
                        set_iso(st, "departed_at")
                        record_event(job, f"del_{del_index+1}_departed", uid)
                        changed = True
                elif sub == "SKIP":
                    record_event(job, f"del_{del_index+1}_skipped", uid)
                    changed = True
                    # mark departed when skipping
                    if not st.get("departed_at"):
                        set_iso(st, "departed_at")

                cur["status"] = st
                deliveries[del_index] = cur
                job["deliveries"] = deliveries

                if sub in ("NEXT", "SKIP", "DEPARTED"):
                    # advance to next stop on NEXT/SKIP/DEPARTED
                    next_idx = del_index + 1
                    if next_idx >= len(deliveries):
                        # done
                        state["job"] = None
                        state["job_stage"] = "PU"
                        state["del_index"] = 0
                        record_event(job, "job_cleared", uid)
                        changed = True
                    else:
                        state["job_stage"] = "DEL"
                        state["del_index"] = next_idx
                        changed = True

        elif action == "DOC":
            sub = parts[1] if len(parts) > 1 else ""
            if sub == "PTI":
                pu = job.get("pickup") or {}
                docs = pu.get("docs") or {}
                item = docs.get("pti_video") or {"done": False, "done_at": None}
                if not item.get("done"):
                    item["done"] = True
                    item["done_at"] = now_utc_iso()
                    record_event(job, "pti_done", uid)
                    changed = True
                docs["pti_video"] = item
                pu["docs"] = docs
                job["pickup"] = pu

            elif sub == "BOL":
                pu = job.get("pickup") or {}
                docs = pu.get("docs") or {}
                item = docs.get("bol") or {"done": False, "done_at": None}
                if not item.get("done"):
                    item["done"] = True
                    item["done_at"] = now_utc_iso()
                    record_event(job, "bol_done", uid)
                    changed = True
                docs["bol"] = item
                pu["docs"] = docs
                job["pickup"] = pu

            elif sub == "POD":
                deliveries = job.get("deliveries") or []
                if deliveries:
                    cur = deliveries[del_index]
                    docs = cur.get("docs") or {}
                    item = docs.get("pod") or {"done": False, "done_at": None}
                    if not item.get("done"):
                        item["done"] = True
                        item["done_at"] = now_utc_iso()
                        record_event(job, f"pod_{del_index+1}_done", uid)
                        changed = True
                    docs["pod"] = item
                    cur["docs"] = docs
                    deliveries[del_index] = cur
                    job["deliveries"] = deliveries

        if changed:
            state["job"] = job
            save_state(state)

    # Update the keyboard in-place if possible (nice UX)
    try:
        async with _state_lock:
            s3 = load_state()
            job3 = normalize_job(s3.get("job")) or job
            job3 = init_job_extras(job3)
        await query.edit_message_reply_markup(reply_markup=build_actions_keyboard(s3, job3))
    except Exception:
        pass


# ------------------ Stops & stage commands ------------------

async def stops_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        chat = update.effective_chat
        if not chat:
            return
        if is_group(update) and not chat_allowed(state, chat.id):
            return
        if is_private(update) and not is_owner(update, state):
            return

        job = normalize_job(state.get("job"))
        if not job:
            await update.effective_message.reply_text("No active job.")
            return
        job = init_job_extras(job)

        deliveries = job.get("deliveries") or []
        if not deliveries:
            await update.effective_message.reply_text("No deliveries listed.")
            return

        idx = int(state.get("del_index", 0))
        stage = state.get("job_stage", "PU")
        lines = ["<b>Delivery Stops</b>"]
        for i, d in enumerate(deliveries):
            marker = "➡️ " if (stage == "DEL" and i == idx) else ""
            addr = d.get("address") or ", ".join(d.get("lines") or [])
            lines.append(f"{marker}{i+1}. {h(addr)}")
        await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)


async def pickupdone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        if not is_owner(update, state):
            await update.effective_message.reply_text("Only the owner can do that.")
            return
        job = normalize_job(state.get("job"))
        if not job:
            await update.effective_message.reply_text("No active job.")
            return
        job = init_job_extras(job)
        state["job_stage"] = "DEL"
        state["del_index"] = 0
        state["job"] = job
        save_state(state)

    await update.effective_message.reply_text("✅ Stage set to DELIVERIES (stop 1).")


async def nextstop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        if not is_owner(update, state):
            await update.effective_message.reply_text("Only the owner can do that.")
            return

        job = normalize_job(state.get("job"))
        if not job:
            await update.effective_message.reply_text("No active job.")
            return
        job = init_job_extras(job)

        deliveries = job.get("deliveries") or []
        if not deliveries:
            state["job"] = None
            state["job_stage"] = "PU"
            state["del_index"] = 0
            save_state(state)
            await update.effective_message.reply_text("No deliveries found. Cleared current job.")
            return

        if state.get("job_stage", "PU") == "PU":
            state["job_stage"] = "DEL"
            state["del_index"] = 0
            state["job"] = job
            save_state(state)
            await update.effective_message.reply_text(f"✅ Now targeting Delivery 1/{len(deliveries)}.")
            return

        idx = int(state.get("del_index", 0)) + 1
        if idx >= len(deliveries):
            state["job"] = None
            state["job_stage"] = "PU"
            state["del_index"] = 0
            save_state(state)
            await update.effective_message.reply_text("✅ Finished last delivery. Cleared current job.")
            return

        state["del_index"] = idx
        state["job"] = job
        save_state(state)

    await update.effective_message.reply_text(f"➡️ Now targeting Delivery {idx+1}/{len(deliveries)}.")


async def skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        state = load_state()
        if not is_owner(update, state):
            await update.effective_message.reply_text("Only the owner can do that.")
            return
        job = normalize_job(state.get("job"))
        if not job:
            await update.effective_message.reply_text("No active job to skip.")
            return
        job = init_job_extras(job)

        deliveries = job.get("deliveries") or []

        if state.get("job_stage", "PU") == "PU":
            state["job_stage"] = "DEL"
            state["del_index"] = 0
            state["job"] = job
            save_state(state)
            await update.effective_message.reply_text(
                f"⏭️ Skipped PICKUP. Now targeting Delivery 1/{len(deliveries) if deliveries else 0}."
            )
            return

        idx = int(state.get("del_index", 0)) + 1
        if idx >= len(deliveries):
            state["job"] = None
            state["job_stage"] = "PU"
            state["del_index"] = 0
            save_state(state)
            await update.effective_message.reply_text("✅ Skipped last stop and cleared current job.")
            return

        state["del_index"] = idx
        state["job"] = job
        save_state(state)

    await update.effective_message.reply_text(f"⏭️ Skipped to Delivery {idx+1}/{len(deliveries)}.")


# ------------------ Text handler ------------------

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    if not msg or not msg.text or not chat:
        return

    async with _state_lock:
        state = load_state()

    # Only react in allowed groups (or owner DM)
    if is_group(update) and not chat_allowed(state, chat.id):
        return

    # Detect new dispatch posts in allowed groups
    if is_group(update):
        job = parse_job_from_text(msg.text)
        if job:
            async with _state_lock:
                state2 = load_state()
                prev = normalize_job(state2.get("job")) or {}
                if prev.get("job_id") != job.get("job_id"):
                    state2["job"] = job
                    state2["job_stage"] = "PU"
                    state2["del_index"] = 0
                    state2["job_chat_id"] = chat.id
                    save_state(state2)

                triggers = " / ".join(sorted(TRIGGERS))
                await msg.reply_text(
                    "📦 New load detected. Stage reset to PICKUP.\n"
                    f"Type {triggers} for ETA, or use /panel for buttons."
                )
            return

    # Triggers
    low = msg.text.strip()
    if not low:
        return

    parts = low.split()
    first = normalize_trigger_token(parts[0])
    tokens = [p.lower() for p in parts[1:]]

    if first in TRIGGERS:
        target = "AUTO"
        card = False

        if "pu" in tokens or "pickup" in tokens:
            target = "PU"
        if "del" in tokens or "delivery" in tokens:
            target = "DEL"
        if "both" in tokens:
            target = "BOTH"
        if "all" in tokens or "stops" in tokens:
            target = "ALL"
        if "card" in tokens or "big" in tokens or "img" in tokens:
            card = True

        await send_eta(update, context, target=target, card=card)


# ------------------ Reminders & geofence tick ------------------

def _reminder_key(kind: str, idx: Optional[int], name: str) -> str:
    if idx is None:
        return f"{kind}:{name}"
    return f"{kind}{idx}:{name}"


async def _send_reminder(chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> bool:
    try:
        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=reply_markup, disable_web_page_preview=True)
        return True
    except Forbidden:
        return False
    except TelegramError:
        return False


async def tick(context: ContextTypes.DEFAULT_TYPE):
    # Load state snapshot
    async with _state_lock:
        state = load_state()
    job = normalize_job(state.get("job"))
    if not job:
        return
    job = init_job_extras(job)

    chat_id = state.get("job_chat_id")
    if not isinstance(chat_id, int):
        return
    if not chat_allowed(state, chat_id):
        return

    target = get_target_stop(state)
    if not target:
        return

    loc = state.get("last_location")
    if not loc:
        return  # need location for geofence, and for driver-facing reminders too

    origin = (float(loc["lat"]), float(loc["lon"]))
    now_tz = loc.get("tz") or "UTC"

    # Geofence arrival prompt
    kind = target["kind"]
    idx = target["index"]
    stop = target["stop"]
    address = stop.get("address") or ", ".join(stop.get("lines") or [])
    status = stop.get("status") or {}

    arrived_key = "arrived_at"
    already_arrived = bool(status.get(arrived_key))

    # Get destination coords
    dest = await get_coords_cached(state, address)
    if dest and not already_arrived:
        dist_mi = miles_between(origin[0], origin[1], dest[0], dest[1])
        if dist_mi <= GEOFENCE_MILES:
            gf = job.get("geofence") or {}
            if kind == "PU":
                prompted_at = gf.get("pu_prompted_at")
                if not prompted_at:
                    # mark prompted
                    gf["pu_prompted_at"] = now_utc_iso()
                    job["geofence"] = gf
                    async with _state_lock:
                        s2 = load_state()
                        if (normalize_job(s2.get("job")) or {}).get("job_id") == job.get("job_id"):
                            s2["job"] = job
                            save_state(s2)

                    await _send_reminder(
                        chat_id,
                        context,
                        f"📍 <b>Near pickup</b> (within {dist_mi:.1f} mi). Mark arrived?",
                        reply_markup=build_actions_keyboard(state, job),
                    )
            else:
                prompted_map = gf.get("del_prompted_at") or {}
                key = str(idx)
                if key not in prompted_map:
                    prompted_map[key] = now_utc_iso()
                    gf["del_prompted_at"] = prompted_map
                    job["geofence"] = gf
                    async with _state_lock:
                        s2 = load_state()
                        if (normalize_job(s2.get("job")) or {}).get("job_id") == job.get("job_id"):
                            s2["job"] = job
                            save_state(s2)

                    await _send_reminder(
                        chat_id,
                        context,
                        f"📍 <b>Near delivery {idx+1}</b> (within {dist_mi:.1f} mi). Mark arrived?",
                        reply_markup=build_actions_keyboard(state, job),
                    )

    # Schedule time reminders (simple)
    reminders = job.get("reminders") or {}
    sent = reminders.get("sent") or {}

    def was_sent(key: str) -> bool:
        return bool(sent.get(key))

    def mark_sent(key: str) -> None:
        sent[key] = now_utc_iso()

    async def maybe_time_reminders(stop_kind: str, stop_idx: Optional[int], stop_obj: dict, label: str):
        raw_time = stop_obj.get("time")
        addr = stop_obj.get("address") or ""
        tz_name = get_cached_tz(state, addr) or now_tz
        start_dt, end_dt = parse_time_window(raw_time, tz_name)
        if not start_dt:
            return

        now_local = now_utc().astimezone(ZoneInfo(tz_name) if tz_name else timezone.utc)
        minutes_to = (start_dt - now_local).total_seconds() / 60.0

        for th in sorted(REMINDER_THRESHOLDS_MIN, reverse=True):
            key = _reminder_key(stop_kind, stop_idx, f"T-{th}")
            if minutes_to <= th and minutes_to > 0 and not was_sent(key):
                mark_sent(key)
                ok = await _send_reminder(
                    chat_id,
                    context,
                    f"⏰ <b>{label}</b> in ~{int(minutes_to)} min (scheduled {start_dt.strftime('%H:%M')} {tz_name}).",
                    reply_markup=build_actions_keyboard(state, job),
                )
                if not ok:
                    return
                break

        # time reached
        key_due = _reminder_key(stop_kind, stop_idx, "DUE")
        if minutes_to <= 0 and not was_sent(key_due):
            # only if not arrived yet
            st = stop_obj.get("status") or {}
            if not st.get("arrived_at"):
                mark_sent(key_due)
                await _send_reminder(
                    chat_id,
                    context,
                    f"⚠️ <b>{label}</b> time reached ({start_dt.strftime('%H:%M')} {tz_name}).",
                    reply_markup=build_actions_keyboard(state, job),
                )

        # window closing reminders (if end exists)
        if end_dt:
            minutes_to_end = (end_dt - now_local).total_seconds() / 60.0
            for th in (60, 30):
                key_end = _reminder_key(stop_kind, stop_idx, f"END-{th}")
                if minutes_to_end <= th and minutes_to_end > 0 and not was_sent(key_end):
                    mark_sent(key_end)
                    await _send_reminder(
                        chat_id,
                        context,
                        f"⏳ <b>{label}</b> window closes in ~{int(minutes_to_end)} min (ends {end_dt.strftime('%H:%M')} {tz_name}).",
                        reply_markup=build_actions_keyboard(state, job),
                    )
                    break

    stage = state.get("job_stage", "PU")
    del_index = int(state.get("del_index", 0))
    deliveries = job.get("deliveries") or []

    if stage == "PU":
        await maybe_time_reminders("PU", None, job.get("pickup") or {}, "Pickup")
    else:
        cur = get_current_delivery(job, del_index)
        if cur:
            idx2, d = cur
            await maybe_time_reminders("DEL", idx2, d, f"Delivery {idx2+1}")

    # Document reminders ("you forgot")
    pu = job.get("pickup") or {}
    pu_st = pu.get("status") or {}
    pu_docs = pu.get("docs") or {}
    # If loaded or departed, remind for PTI/BOL
    trigger_iso = pu_st.get("loaded_at") or pu_st.get("departed_at")
    if trigger_iso:
        trig = parse_iso(trigger_iso) or now_utc()
        mins_since = (now_utc() - trig).total_seconds() / 60.0
        if mins_since >= REMINDER_DOC_AFTER_MIN:
            if not (pu_docs.get("pti_video") or {}).get("done"):
                key = "DOC:PTI"
                if not was_sent(key):
                    mark_sent(key)
                    await _send_reminder(
                        chat_id,
                        context,
                        "📎 Reminder: PTI video required. Tap PTI when done.",
                        reply_markup=build_actions_keyboard(state, job),
                    )
            if not (pu_docs.get("bol") or {}).get("done"):
                key = "DOC:BOL"
                if not was_sent(key):
                    mark_sent(key)
                    await _send_reminder(
                        chat_id,
                        context,
                        "📎 Reminder: Send BOL. Tap BOL when done.",
                        reply_markup=build_actions_keyboard(state, job),
                    )

    if stage == "DEL" and deliveries:
        cur = get_current_delivery(job, del_index)
        if cur:
            idx2, d = cur
            st = d.get("status") or {}
            docs = d.get("docs") or {}
            # After delivered, remind for POD
            delivered_iso = st.get("delivered_at")
            if delivered_iso:
                dd = parse_iso(delivered_iso) or now_utc()
                mins_since = (now_utc() - dd).total_seconds() / 60.0
                if mins_since >= REMINDER_DOC_AFTER_MIN and not (docs.get("pod") or {}).get("done"):
                    key = f"DOC:POD:{idx2}"
                    if not was_sent(key):
                        mark_sent(key)
                        await _send_reminder(
                            chat_id,
                            context,
                            f"📎 Reminder: Send POD for Delivery {idx2+1}. Tap POD when done.",
                            reply_markup=build_actions_keyboard(state, job),
                        )

    # Save reminder sent state
    reminders["sent"] = sent
    reminders["last_tick"] = now_utc_iso()
    job["reminders"] = reminders

    async with _state_lock:
        s2 = load_state()
        if (normalize_job(s2.get("job")) or {}).get("job_id") == job.get("job_id"):
            s2["job"] = job
            save_state(s2)


# ------------------ Entry point ------------------

def main() -> None:
    if not TOKEN:
        raise RuntimeError("Missing TELEGRAM_TOKEN")

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("claim", claim))
    app.add_handler(CommandHandler("allowhere", allowhere))
    app.add_handler(CommandHandler("disallowhere", disallowhere))
    app.add_handler(CommandHandler("update", update_loc))
    app.add_handler(CommandHandler("panel", panel))
    app.add_handler(CommandHandler("pickupdone", pickupdone))
    app.add_handler(CommandHandler("nextstop", nextstop))
    app.add_handler(CommandHandler("stops", stops_cmd))
    app.add_handler(CommandHandler("skip", skip))
    app.add_handler(CommandHandler("report", report_cmd))
    app.add_handler(CommandHandler("leave", leave))
    app.add_handler(CommandHandler("deleteall", deleteall))
    app.add_handler(CallbackQueryHandler(on_callback))

    # Location updates (works for normal & edited live location because we use effective_message)
    app.add_handler(MessageHandler(filters.LOCATION, on_location))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    if app.job_queue:
        app.job_queue.run_repeating(tick, interval=60, first=10)

    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
