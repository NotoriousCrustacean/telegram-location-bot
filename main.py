"""
Telegram Trucker Dispatch Assistant Bot
Version: 2025-12-11_v2
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
from typing import Any, Dict, List, Optional, Tuple, Set

import httpx
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
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
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

BOT_VERSION = "2025-12-11_v2"

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

# Environment variables
TOKEN = env_str("TELEGRAM_TOKEN")
CLAIM_CODE = env_str("CLAIM_CODE")
STATE_FILE = Path(env_str("STATE_FILE", "state.json"))
TRIGGERS = {t.strip().lower() for t in env_str("TRIGGERS", "eta,1717").split(",") if t.strip()}

NOMINATIM_USER_AGENT = env_str("NOMINATIM_USER_AGENT", "dispatch-bot/1.0")
NOMINATIM_MIN_INTERVAL = env_float("NOMINATIM_MIN_INTERVAL", 1.1)

ETA_ALL_MAX = env_int("ETA_ALL_MAX", 6)
ALERT_TTL_SECONDS = env_int("ALERT_TTL_SECONDS", 25)
DELETEALL_DEFAULT = env_int("DELETEALL_DEFAULT", 300)

GEOFENCE_MILES = env_float("GEOFENCE_MILES", 5.0)
REMINDER_DOC_AFTER_MIN = env_int("REMINDER_DOC_AFTER_MIN", 15)
REMINDER_THRESHOLDS_MIN = env_list_int("REMINDER_THRESHOLDS_MIN", [60, 30, 10])
SCHEDULE_GRACE_MIN = env_int("SCHEDULE_GRACE_MIN", 30)

DEBUG = env_bool("DEBUG")

def log(msg: str):
    if DEBUG:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ============================================================================
# GLOBALS
# ============================================================================
TF = TimezoneFinder()
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
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
    h, m = divmod(secs // 60, 60)
    return f"{h}h {m}m" if h else f"{m}m"

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
        except: pass
    return {}

def save_state(st: dict):
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, indent=2))
        tmp.replace(STATE_FILE)
    except Exception as e:
        log(f"Save error: {e}")

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
# GEOCODING & ROUTING
# ============================================================================
def haversine_miles(lat1, lon1, lat2, lon2) -> float:
    R = 3958.8  # Earth radius in miles
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def haversine_meters(lat1, lon1, lat2, lon2) -> float:
    return haversine_miles(lat1, lon1, lat2, lon2) * METERS_PER_MILE

async def geocode(addr: str, cache: dict) -> Optional[Tuple[float, float, str]]:
    if addr in cache:
        c = cache[addr]
        return c["lat"], c["lon"], c.get("tz", "UTC")
    
    if not NOMINATIM_USER_AGENT:
        return None
    
    variants = [addr]
    if ", USA" not in addr.upper():
        variants.append(addr + ", USA")
    
    async with httpx.AsyncClient(timeout=15) as client:
        for q in variants:
            async with _geo_lock:
                global _geo_last
                wait = _geo_last + NOMINATIM_MIN_INTERVAL - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                _geo_last = time.monotonic()
            
            try:
                r = await client.get(NOMINATIM_URL, 
                    params={"q": q, "format": "jsonv2", "limit": 1},
                    headers={"User-Agent": NOMINATIM_USER_AGENT})
                if r.status_code == 200:
                    data = r.json()
                    if data:
                        lat, lon = float(data[0]["lat"]), float(data[0]["lon"])
                        tz = TF.timezone_at(lat=lat, lng=lon) or "UTC"
                        cache[addr] = {"lat": lat, "lon": lon, "tz": tz}
                        return lat, lon, tz
            except Exception as e:
                log(f"Geocode error: {e}")
    return None

async def get_route(origin: Tuple[float, float], dest: Tuple[float, float]) -> Optional[Tuple[float, float]]:
    url = OSRM_URL.format(lat1=origin[0], lon1=origin[1], lat2=dest[0], lon2=dest[1])
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, params={"overview": "false"})
            if r.status_code == 200:
                data = r.json()
                if data.get("routes"):
                    rt = data["routes"][0]
                    return rt["distance"], rt["duration"]
    except Exception as e:
        log(f"Route error: {e}")
    return None

async def calc_eta(st: dict, origin: Tuple[float, float], addr: str) -> dict:
    cache = st.setdefault("geocode_cache", {})
    geo = await geocode(addr, cache)
    if not geo:
        return {"ok": False, "err": "Location not found"}
    
    dest = (geo[0], geo[1])
    route = await get_route(origin, dest)
    
    if route:
        return {"ok": True, "meters": route[0], "seconds": route[1], "tz": geo[2], "method": "route"}
    
    # Fallback to straight-line estimate
    dist = haversine_meters(origin[0], origin[1], dest[0], dest[1])
    speed = 55 if dist < 80000 else (70 if dist < 300000 else 65)  # mph estimates
    secs = (dist / METERS_PER_MILE / speed) * 3600
    return {"ok": True, "meters": dist, "seconds": secs, "tz": geo[2], "method": "estimate"}

# ============================================================================
# LOAD PARSING - Updated for your format
# ============================================================================
# Patterns for your specific format
LOAD_NUM_PATTERN = re.compile(r"Load\s*#\s*(\S+)", re.I)
RATE_PATTERN = re.compile(r"Rate\s*:\s*\$?([\d,]+(?:\.\d{2})?)", re.I)
MILES_PATTERN = re.compile(r"Total\s*mi\s*:\s*([\d,]+)", re.I)
PU_TIME_PATTERN = re.compile(r"PU\s*time\s*:\s*(.+?)(?:\n|$)", re.I)
DEL_TIME_PATTERN = re.compile(r"DEL\s*time\s*:\s*(.+?)(?:\n|$)", re.I)
PU_ADDR_PATTERN = re.compile(r"PU\s*Address\s*:\s*(.+?)(?=\n\s*\n|\nDEL|\n-{3,}|$)", re.I | re.S)
DEL_ADDR_PATTERN = re.compile(r"DEL\s*Address\s*:\s*(.+?)(?=\n\s*\n|\n-{3,}|\nTotal|$)", re.I | re.S)

def clean_address(addr: str) -> str:
    """Clean up address text."""
    lines = [ln.strip() for ln in addr.strip().split("\n") if ln.strip()]
    # Filter out obvious non-address lines
    cleaned = []
    for ln in lines:
        ln_lower = ln.lower()
        if any(skip in ln_lower for skip in ["---", "===", "total mi", "rate", "trailer", "failure"]):
            break
        cleaned.append(ln)
    return ", ".join(cleaned) if cleaned else ""

def parse_load(text: str) -> Optional[dict]:
    """Parse load information from dispatcher message."""
    
    # Must have both PU and DEL addresses
    if "pu address" not in text.lower() or "del address" not in text.lower():
        return None
    
    # Extract load number
    load_num = None
    m = LOAD_NUM_PATTERN.search(text)
    if m:
        load_num = m.group(1).strip()
    
    # Extract rate
    rate = None
    m = RATE_PATTERN.search(text)
    if m:
        try:
            rate = float(m.group(1).replace(",", ""))
        except: pass
    
    # Extract miles
    miles = None
    m = MILES_PATTERN.search(text)
    if m:
        try:
            miles = int(m.group(1).replace(",", ""))
        except: pass
    
    # Extract PU time
    pu_time = None
    m = PU_TIME_PATTERN.search(text)
    if m:
        pu_time = m.group(1).strip()
    
    # Extract DEL time
    del_time = None
    m = DEL_TIME_PATTERN.search(text)
    if m:
        del_time = m.group(1).strip()
    
    # Extract PU address
    pu_addr = None
    m = PU_ADDR_PATTERN.search(text)
    if m:
        pu_addr = clean_address(m.group(1))
    
    # Extract DEL address
    del_addr = None
    m = DEL_ADDR_PATTERN.search(text)
    if m:
        del_addr = clean_address(m.group(1))
    
    if not pu_addr or not del_addr:
        return None
    
    # Build job structure
    job_id = hashlib.sha1(f"{load_num}|{pu_addr}|{del_addr}".encode()).hexdigest()[:10]
    
    job = {
        "id": job_id,
        "created_at": now_iso(),
        "meta": {
            "load_number": load_num,
            "rate": rate,
            "miles": miles,
        },
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
    
    return job

def get_job(st: dict) -> Optional[dict]:
    """Get normalized job from state."""
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
    """Get current focus: ('PU', 0) or ('DEL', index)."""
    if not is_pu_complete(job):
        return "PU", 0
    
    dels = job.get("del") or []
    idx = st.get("focus_i", 0)
    idx = max(0, min(idx, len(dels) - 1)) if dels else 0
    
    # Find next incomplete if current is done
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
    if len(addr) <= max_len:
        return addr
    return addr[:max_len-3] + "..."

def toggle_timestamp(obj: dict, key: str) -> bool:
    """Toggle a timestamp field. Returns True if turned ON."""
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
            
            # Navigation for multiple stops
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
    """Send auto-deleting alert."""
    try:
        msg = await ctx.bot.send_message(chat_id, text, parse_mode="HTML", disable_notification=True)
        if ALERT_TTL_SECONDS > 0:
            async def delete_later():
                await asyncio.sleep(ALERT_TTL_SECONDS)
                try: await ctx.bot.delete_message(chat_id, msg.message_id)
                except: pass
            task = asyncio.create_task(delete_later())
            _tasks.add(task)
            task.add_done_callback(_tasks.discard)
    except Exception as e:
        log(f"Alert error: {e}")

# ============================================================================
# EXCEL EXPORT
# ============================================================================
def make_catalog_xlsx(records: List[dict], week: str) -> Tuple[bytes, str]:
    wb = Workbook()
    ws = wb.active
    ws.title = week[:31] if week != "ALL" else "All Loads"
    
    # Header
    headers = ["Completed", "Load #", "Pickup", "Delivery", "Rate", "Miles", "$/Mile"]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = PatternFill("solid", fgColor="4472C4")
    
    # Data
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
    
    # Totals
    ws.append([])
    ws.append(["TOTAL", "", "", "", total_rate, total_miles, 
               round(total_rate/total_miles, 2) if total_miles else None])
    for cell in ws[ws.max_row]:
        cell.font = Font(bold=True)
    
    # Format columns
    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 15
    ws.column_dimensions["C"].width = 40
    ws.column_dimensions["D"].width = 40
    ws.column_dimensions["E"].width = 12
    ws.column_dimensions["F"].width = 10
    ws.column_dimensions["G"].width = 10
    
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
1. DM me: /claim &lt;code&gt;
2. DM me: /update (share location)
3. In group: /allowhere

<b>Usage:</b>
• Forward load sheet to group
• Type <code>eta</code> or <code>1717</code> for ETA
• Use /panel for controls
• /finish when done
• /catalog for Excel report

<b>Features:</b>
📍 {GEOFENCE_MILES} mi geofence alerts
⏰ {REMINDER_THRESHOLDS_MIN} min appointment alerts
📋 Doc reminders after {REMINDER_DOC_AFTER_MIN} min"""
    
    await update.message.reply_text(text, parse_mode="HTML")

async def cmd_ping(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🏓 pong • {BOT_VERSION}")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    job = get_job(st)
    loc = st.get("last_location")
    
    text = f"""<b>Status</b>
• Owner: {st.get('owner_id', 'Not set')}
• Your ID: {update.effective_user.id if update.effective_user else '?'}
• Allowed here: {'✅' if chat_allowed(update, st) else '❌'}
• Location: {'✅' if loc else '❌'}
• Active load: {load_label(job) if job else '❌'}
• History: {len(st.get('history', []))} loads"""
    
    await update.message.reply_text(text, parse_mode="HTML")

async def cmd_claim(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.message.reply_text("⚠️ DM me: /claim <code>")
        return
    
    if not CLAIM_CODE:
        await update.message.reply_text("⚠️ CLAIM_CODE not configured")
        return
    
    code = " ".join(ctx.args) if ctx.args else ""
    if code != CLAIM_CODE:
        await update.message.reply_text("❌ Wrong code")
        return
    
    async with _state_lock:
        st = load_state()
        st["owner_id"] = update.effective_user.id
        save_state(st)
    
    await update.message.reply_text("✅ You're now the owner! Use /update to share location.")

async def cmd_allowhere(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        await update.message.reply_text("⚠️ Owner only")
        return
    
    if update.effective_chat.type == "private":
        await update.message.reply_text("⚠️ Run this in a group")
        return
    
    async with _state_lock:
        st = load_state()
        allowed = set(st.get("allowed_chats", []))
        allowed.add(update.effective_chat.id)
        st["allowed_chats"] = list(allowed)
        save_state(st)
    
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
        "Tap to share your location.\n💡 Use 'Live Location' for auto geofence!",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)
    )

async def cmd_panel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not chat_allowed(update, st):
        await update.message.reply_text("⚠️ Not allowed. Owner: /allowhere")
        return
    
    job = get_job(st)
    if not job:
        await update.message.reply_text("📭 No active load. Forward a load sheet!")
        return
    
    meta = job.get("meta", {})
    stage, idx = get_focus(job, st)
    
    lines = [f"<b>{h(load_label(job))}</b>"]
    if meta.get("rate"):
        lines.append(f"💰 {money(meta['rate'])} • {meta.get('miles', '?')} mi")
    
    if stage == "PU":
        pu = job.get("pu", {})
        lines.append(f"\n<b>📍 Pickup</b>")
        lines.append(h(short_addr(pu.get("addr", ""))))
        if pu.get("time"):
            lines.append(f"⏰ {h(pu['time'])}")
    else:
        dels = job.get("del", [])
        if idx < len(dels):
            d = dels[idx]
            lines.append(f"\n<b>📍 Delivery {idx+1}/{len(dels)}</b>")
            lines.append(h(short_addr(d.get("addr", ""))))
            if d.get("time"):
                lines.append(f"⏰ {h(d['time'])}")
    
    msg = await update.message.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=build_panel_keyboard(job, st)
    )
    
    # Save panel message ID
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
        await update.message.reply_text("📭 No completed loads yet")
        return
    
    # Determine week
    tz_name = (st.get("last_location") or {}).get("tz", "UTC")
    current_week = week_key(now_utc().astimezone(safe_tz(tz_name)))
    
    arg = ctx.args[0].lower() if ctx.args else ""
    if arg == "all":
        week = "ALL"
        records = history
    elif arg in ("last", "prev"):
        week = week_key(now_utc().astimezone(safe_tz(tz_name)) - timedelta(days=7))
        records = [r for r in history if r.get("week") == week]
    else:
        week = current_week
        records = [r for r in history if r.get("week") == week]
    
    if not records:
        await update.message.reply_text(f"No loads for {week}")
        return
    
    xlsx_data, filename = make_catalog_xlsx(records, week)
    
    total_rate = sum(r.get("rate", 0) or 0 for r in records)
    total_miles = sum((r.get("posted_miles") or r.get("est_miles") or 0) for r in records)
    
    await update.message.reply_document(
        document=io.BytesIO(xlsx_data),
        filename=filename,
        caption=f"📊 <b>{week}</b>\n{len(records)} loads • {money(total_rate)} • {int(total_miles)} mi",
        parse_mode="HTML"
    )

async def cmd_skip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        await update.message.reply_text("⚠️ Owner only")
        return
    
    job = get_job(st)
    if not job:
        await update.message.reply_text("📭 No active load")
        return
    
    stage, idx = get_focus(job, st)
    if stage != "DEL":
        await update.message.reply_text("⚠️ Complete pickup first")
        return
    
    dels = job.get("del", [])
    if idx >= len(dels):
        await update.message.reply_text("⚠️ No stop to skip")
        return
    
    async with _state_lock:
        st = load_state()
        job = get_job(st)
        dels = job.get("del", [])
        dels[idx]["status"]["skip"] = True
        dels[idx]["status"]["comp"] = now_iso()
        
        # Move to next incomplete
        for i in range(idx + 1, len(dels)):
            if not dels[i].get("status", {}).get("comp"):
                st["focus_i"] = i
                break
        
        st["job"] = job
        save_state(st)
    
    await update.message.reply_text(f"⏭ Skipped stop {idx+1}. Use /panel to continue.")

async def cmd_deleteall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        return
    
    if update.effective_chat.type == "private":
        await update.message.reply_text("Can't clear DM history")
        return
    
    n = int(ctx.args[0]) if ctx.args else DELETEALL_DEFAULT
    n = max(1, min(n, 2000))
    
    msg = await update.message.reply_text(f"🧹 Deleting up to {n} messages...")
    
    for mid in range(msg.message_id, max(1, msg.message_id - n), -1):
        try:
            await ctx.bot.delete_message(update.effective_chat.id, mid)
        except:
            break
        await asyncio.sleep(0.03)

async def cmd_leave(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
    
    if not is_owner(update, st):
        return
    
    if update.effective_chat.type == "private":
        await update.message.reply_text("Use in a group")
        return
    
    async with _state_lock:
        st = load_state()
        allowed = set(st.get("allowed_chats", []))
        allowed.discard(update.effective_chat.id)
        st["allowed_chats"] = list(allowed)
        save_state(st)
    
    await update.message.reply_text("👋 Leaving...")
    try:
        await ctx.bot.leave_chat(update.effective_chat.id)
    except: pass

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
    
    if update.effective_chat.type == "private":
        await update.message.reply_text("✅ Location saved!", reply_markup=ReplyKeyboardRemove())

# ============================================================================
# TEXT HANDLER (Load detection + triggers)
# ============================================================================
async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    
    text = update.message.text
    chat = update.effective_chat
    
    async with _state_lock:
        st = load_state()
    
    # Detect new loads in allowed groups
    if chat.type in ("group", "supergroup"):
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
            lines = [
                f"📦 <b>New Load!</b>",
                f"<b>{h(load_label(job))}</b>",
            ]
            if meta.get("rate"):
                lines.append(f"💰 {money(meta['rate'])} • {meta.get('miles', '?')} mi")
            lines.append(f"\n📍 PU: {h(short_addr(job['pu'].get('addr', ''), 40))}")
            lines.append(f"📍 DEL: {h(short_addr(job['del'][0].get('addr', ''), 40))}")
            lines.append(f"\nType <code>eta</code> or /panel")
            
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
            return
    
    # Check triggers
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
    
    # IMPORTANT: Answer callback immediately to prevent timeout
    await query.answer()
    
    async with _state_lock:
        st = load_state()
    
    if not chat_allowed(update, st):
        return
    
    # Route callbacks
    if data.startswith("ETA:"):
        await handle_eta_callback(update, ctx, st, data)
    elif data == "CATALOG":
        await handle_catalog_callback(update, ctx, st)
    elif data == "FINISH":
        await handle_finish_callback(update, ctx, st)
    elif data.startswith("NAV:"):
        await handle_nav_callback(update, ctx, st, data)
    elif data.startswith("PU:") or data.startswith("DEL:") or data.startswith("DOC:"):
        await handle_status_callback(update, ctx, st, data)

async def handle_eta_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, st: dict, data: str):
    is_all = data == "ETA:ALL"
    await send_eta_response(update, ctx, st, all_stops=is_all)

async def handle_catalog_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, st: dict):
    if not is_owner(update, st):
        return
    
    history = st.get("history", [])
    if not history:
        try:
            await update.callback_query.edit_message_text("📭 No completed loads yet")
        except: pass
        return
    
    tz_name = (st.get("last_location") or {}).get("tz", "UTC")
    week = week_key(now_utc().astimezone(safe_tz(tz_name)))
    records = [r for r in history if r.get("week") == week]
    
    if not records:
        records = history
        week = "ALL"
    
    xlsx_data, filename = make_catalog_xlsx(records, week)
    total_rate = sum(r.get("rate", 0) or 0 for r in records)
    
    await ctx.bot.send_document(
        chat_id=update.effective_chat.id,
        document=io.BytesIO(xlsx_data),
        filename=filename,
        caption=f"📊 {len(records)} loads • {money(total_rate)}",
        parse_mode="HTML"
    )

async def handle_finish_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, st: dict):
    if not is_owner(update, st):
        return
    
    job = get_job(st)
    if not job:
        return
    
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
        
        # Handle PU status
        if data.startswith("PU:"):
            ps = job["pu"].setdefault("status", {})
            
            if data == "PU:ARR":
                if toggle_timestamp(ps, "arr"):
                    alert_msg = f"📍 <b>Arrived at Pickup</b> • {h(ts)}"
            elif data == "PU:LOAD":
                if toggle_timestamp(ps, "load"):
                    alert_msg = f"📦 <b>Loaded</b> • {h(ts)}"
            elif data == "PU:DEP":
                if toggle_timestamp(ps, "dep"):
                    alert_msg = f"🚚 <b>Departed Pickup</b> • {h(ts)}"
            elif data == "PU:COMP":
                if toggle_timestamp(ps, "comp"):
                    alert_msg = f"✅ <b>Pickup Complete</b> • {h(ts)}"
        
        # Handle DEL status
        elif data.startswith("DEL:"):
            if stage != "DEL":
                return
            
            dels = job.get("del", [])
            if idx >= len(dels):
                return
            
            ds = dels[idx].setdefault("status", {})
            lbl = f"Stop {idx+1}/{len(dels)}"
            
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
                    # Advance to next
                    for i in range(idx + 1, len(dels)):
                        if not dels[i].get("status", {}).get("comp"):
                            st["focus_i"] = i
                            break
            elif data == "DEL:SKIP":
                ds["skip"] = True
                ds["comp"] = ds.get("comp") or now_iso()
                alert_msg = f"⏭ <b>Skipped {lbl}</b> • {h(ts)}"
                for i in range(idx + 1, len(dels)):
                    if not dels[i].get("status", {}).get("comp"):
                        st["focus_i"] = i
                        break
        
        # Handle DOC status
        elif data.startswith("DOC:"):
            if data == "DOC:PTI":
                job["pu"].setdefault("docs", {})["pti"] = not job["pu"].get("docs", {}).get("pti", False)
            elif data == "DOC:BOL":
                job["pu"].setdefault("docs", {})["bol"] = not job["pu"].get("docs", {}).get("bol", False)
            elif data == "DOC:POD":
                if stage == "DEL":
                    dels = job.get("del", [])
                    if idx < len(dels):
                        dels[idx].setdefault("docs", {})["pod"] = not dels[idx].get("docs", {}).get("pod", False)
        
        st["job"] = job
        save_state(st)
    
    # Send alert if action was taken
    if alert_msg:
        await send_alert(ctx, update.effective_chat.id, alert_msg)
    
    # Update keyboard
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
        await update.effective_message.reply_text("📍 No location. Owner: /update")
        return
    
    origin = (loc["lat"], loc["lon"])
    tz_name = loc.get("tz", "UTC")
    tz = safe_tz(tz_name)
    
    # Send current location
    await ctx.bot.send_location(update.effective_chat.id, origin[0], origin[1])
    
    job = get_job(st)
    if not job:
        await update.effective_message.reply_text(
            f"⏱ <b>Current Time</b>\n{now_utc().astimezone(tz).strftime('%H:%M')} ({tz_name})\n\n<i>No active load</i>",
            parse_mode="HTML"
        )
        return
    
    if all_stops:
        await send_all_etas(update, ctx, st, job, origin, tz, tz_name)
    else:
        await send_single_eta(update, ctx, st, job, origin, tz, tz_name)

async def send_all_etas(update, ctx, st, job, origin, tz, tz_name):
    lines = [f"<b>{h(load_label(job))}</b>", ""]
    
    # PU
    pu = job.get("pu", {})
    if pu.get("status", {}).get("comp"):
        lines.append(f"✅ <b>PU:</b> <s>{h(short_addr(pu.get('addr', ''), 30))}</s>")
    else:
        eta = await calc_eta(st, origin, pu.get("addr", ""))
        if eta["ok"]:
            arr = (now_utc() + timedelta(seconds=eta["seconds"])).astimezone(tz).strftime("%H:%M")
            lines.append(f"<b>PU:</b> {fmt_dur(eta['seconds'])} • {fmt_mi(eta['meters'])} • ~{arr}")
        else:
            lines.append(f"<b>PU:</b> ⚠️ {eta['err']}")
    
    # DELs
    for i, d in enumerate(job.get("del", [])[:ETA_ALL_MAX]):
        if d.get("status", {}).get("comp"):
            lines.append(f"✅ <b>D{i+1}:</b> <s>{h(short_addr(d.get('addr', ''), 30))}</s>")
        else:
            eta = await calc_eta(st, origin, d.get("addr", ""))
            if eta["ok"]:
                arr = (now_utc() + timedelta(seconds=eta["seconds"])).astimezone(tz).strftime("%H:%M")
                lines.append(f"<b>D{i+1}:</b> {fmt_dur(eta['seconds'])} • {fmt_mi(eta['meters'])} • ~{arr}")
            else:
                lines.append(f"<b>D{i+1}:</b> ⚠️ {eta['err']}")
    
    await update.effective_message.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=build_panel_keyboard(job, st)
    )

async def send_single_eta(update, ctx, st, job, origin, tz, tz_name):
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
    
    if eta["ok"]:
        arr_time = now_utc() + timedelta(seconds=eta["seconds"])
        arr_str = arr_time.astimezone(tz).strftime("%H:%M")
        method = "≈" if eta["method"] == "estimate" else ""
        
        lines = [
            f"⏱ <b>ETA: {fmt_dur(eta['seconds'])}</b> {method}",
            "",
            f"<b>{label}</b> • {h(load_label(job))}",
            f"📍 {h(short_addr(addr))}",
            f"🚚 {fmt_mi(eta['meters'])} • Arrive ~{arr_str}",
        ]
        
        if appt:
            lines.append(f"⏰ Appt: {h(appt)}")
        
        await update.effective_message.reply_text(
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=build_panel_keyboard(job, st)
        )
    else:
        await update.effective_message.reply_text(
            f"⚠️ Could not calculate ETA\n{eta['err']}",
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
    
    # Build history record
    record = {
        "week": wk,
        "completed": dt.strftime("%Y-%m-%d %H:%M"),
        "completed_utc": now_iso(),
        "tz": tz_name,
        "load_number": meta.get("load_number", ""),
        "job_id": job.get("id"),
        "pickup": pu.get("addr", ""),
        "deliveries": " | ".join(d.get("addr", "") for d in dels),
        "rate": meta.get("rate"),
        "posted_miles": meta.get("miles"),
    }
    
    async with _state_lock:
        st = load_state()
        history = st.setdefault("history", [])
        history.append(record)
        st["history"] = history[-1000:]  # Keep last 1000
        st["job"] = None
        st["focus_i"] = 0
        st["reminders_sent"] = {}
        st["geofence_state"] = {}
        save_state(st)
    
    # Calculate week totals
    week_records = [r for r in st["history"] if r.get("week") == wk]
    wk_count = len(week_records)
    wk_rate = sum(r.get("rate", 0) or 0 for r in week_records)
    wk_miles = sum((r.get("posted_miles") or 0) for r in week_records)
    
    rate_txt = money(meta.get("rate"))
    load_txt = meta.get("load_number") or job.get("id", "")[:8]
    
    report = f"""✅ <b>Load Complete!</b>

<b>#{h(load_txt)}</b> • {rate_txt}

📊 <b>Week {wk}:</b>
• {wk_count} loads
• {money(wk_rate)} gross
• {wk_miles} miles"""
    
    # Try to edit panel message, or send new
    chat_id = update.effective_chat.id
    panel_id = st.get("panel_msgs", {}).get(str(chat_id))
    
    if panel_id:
        try:
            await ctx.bot.edit_message_text(
                chat_id=chat_id,
                message_id=panel_id,
                text=report,
                parse_mode="HTML",
                reply_markup=build_done_keyboard()
            )
            return
        except: pass
    
    await ctx.bot.send_message(
        chat_id=chat_id,
        text=report,
        parse_mode="HTML",
        reply_markup=build_done_keyboard()
    )

# ============================================================================
# BACKGROUND JOBS (Reminders & Geofence)
# ============================================================================
async def reminder_job(ctx: ContextTypes.DEFAULT_TYPE):
    """Check for pending reminders."""
    async with _state_lock:
        st = load_state()
    
    job = get_job(st)
    if not job:
        return
    
    tz_name = (st.get("last_location") or {}).get("tz", "UTC")
    sent = st.setdefault("reminders_sent", {})
    chats = get_broadcast_chats(st)
    alerts = []
    
    # Check appointment reminders
    for threshold in REMINDER_THRESHOLDS_MIN:
        # PU appointment
        pu = job.get("pu", {})
        if pu.get("time") and not pu.get("status", {}).get("comp"):
            appt = parse_appt_time(pu["time"], tz_name)
            if appt:
                mins = (appt - now_utc()).total_seconds() / 60
                key = f"appt:pu:{threshold}"
                if threshold - 5 < mins <= threshold and key not in sent:
                    alerts.append((key, f"⏰ <b>PU in ~{int(mins)} min</b>\n{h(pu['time'])}"))
        
        # DEL appointments
        for i, d in enumerate(job.get("del", [])):
            if d.get("time") and not d.get("status", {}).get("comp"):
                appt = parse_appt_time(d["time"], tz_name)
                if appt:
                    mins = (appt - now_utc()).total_seconds() / 60
                    key = f"appt:del{i}:{threshold}"
                    if threshold - 5 < mins <= threshold and key not in sent:
                        alerts.append((key, f"⏰ <b>DEL {i+1} in ~{int(mins)} min</b>\n{h(d['time'])}"))
    
    # Check document reminders
    pu = job.get("pu", {})
    ps = pu.get("status", {})
    pd = pu.get("docs", {})
    
    if ps.get("arr") and not ps.get("comp"):
        try:
            arr = datetime.fromisoformat(ps["arr"])
            mins_since = (now_utc() - arr).total_seconds() / 60
            if mins_since >= REMINDER_DOC_AFTER_MIN:
                if not pd.get("pti") and "doc:pti" not in sent:
                    alerts.append(("doc:pti", f"📋 <b>PTI Reminder</b>\nArrived {int(mins_since)} min ago"))
                if not pd.get("bol") and "doc:bol" not in sent:
                    alerts.append(("doc:bol", f"📋 <b>BOL Reminder</b>\nArrived {int(mins_since)} min ago"))
        except: pass
    
    # Send alerts
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

def parse_appt_time(time_str: str, tz_name: str) -> Optional[datetime]:
    """Parse appointment time string."""
    if not time_str:
        return None
    
    # Try to extract date and time
    # Format: "Dec 10, 2025 08:00 -14:00 FCFS"
    patterns = [
        r"(\w+ \d+,? \d{4})\s+(\d{1,2}:\d{2})",
        r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2})",
    ]
    
    for pattern in patterns:
        m = re.search(pattern, time_str)
        if m:
            date_str, time_part = m.groups()
            try:
                # Parse date
                for fmt in ["%b %d, %Y", "%b %d %Y", "%m/%d/%Y"]:
                    try:
                        dt = datetime.strptime(f"{date_str} {time_part}", f"{fmt} %H:%M")
                        return dt.replace(tzinfo=safe_tz(tz_name))
                    except: pass
            except: pass
    
    return None

async def geofence_job(ctx: ContextTypes.DEFAULT_TYPE):
    """Check geofence status."""
    async with _state_lock:
        st = load_state()
    
    job = get_job(st)
    loc = st.get("last_location")
    
    if not job or not loc:
        return
    
    # Check if location is fresh (< 5 min)
    try:
        loc_time = datetime.fromisoformat(loc["updated_at"])
        if (now_utc() - loc_time).total_seconds() > 300:
            return
    except: return
    
    origin = (loc["lat"], loc["lon"])
    gf_state = st.setdefault("geofence_state", {})
    chats = get_broadcast_chats(st)
    events = []
    
    # Check PU
    pu = job.get("pu", {})
    if pu.get("addr") and not pu.get("status", {}).get("comp"):
        cache = st.setdefault("geocode_cache", {})
        geo = await geocode(pu["addr"], cache)
        if geo:
            dist = haversine_miles(origin[0], origin[1], geo[0], geo[1])
            key = "pu"
            was_in = gf_state.get(key, False)
            is_in = dist <= GEOFENCE_MILES
            
            if is_in and not was_in:
                events.append((key, True, "Pickup", pu["addr"]))
            elif not is_in and was_in:
                events.append((key, False, "Pickup", pu["addr"]))
            gf_state[key] = is_in
    
    # Check DELs
    for i, d in enumerate(job.get("del", [])):
        if d.get("addr") and not d.get("status", {}).get("comp"):
            cache = st.setdefault("geocode_cache", {})
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
    
    # Send alerts and auto-update status
    if events:
        async with _state_lock:
            st = load_state()
            job = get_job(st)
            if not job:
                return
            
            for key, entered, label, addr in events:
                if entered:
                    msg = f"📍 <b>ARRIVED: {label}</b>\n{h(short_addr(addr))}"
                    # Auto-mark arrival
                    if key == "pu":
                        job["pu"].setdefault("status", {})["arr"] = now_iso()
                    elif key.startswith("del"):
                        idx = int(key[3:])
                        if idx < len(job.get("del", [])):
                            job["del"][idx].setdefault("status", {})["arr"] = now_iso()
                else:
                    msg = f"🚚 <b>DEPARTED: {label}</b>\n{h(short_addr(addr))}"
                    # Auto-mark departure
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
            save_state(st)

# ============================================================================
# MAIN
# ============================================================================
async def post_init(app: Application):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        me = await app.bot.get_me()
        log(f"Bot: @{me.username}")
    except Exception as e:
        log(f"Init error: {e}")
    
    # Schedule background jobs
    if app.job_queue:
        app.job_queue.run_repeating(reminder_job, interval=60, first=10)
        app.job_queue.run_repeating(geofence_job, interval=30, first=15)
        log("Background jobs scheduled")
    
    log(f"Ready! v{BOT_VERSION}")

def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN not set")
    
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    
    # Commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("claim", cmd_claim))
    app.add_handler(CommandHandler("allowhere", cmd_allowhere))
    app.add_handler(CommandHandler("update", cmd_update))
    app.add_handler(CommandHandler("panel", cmd_panel))
    app.add_handler(CommandHandler("finish", cmd_finish))
    app.add_handler(CommandHandler("catalog", cmd_catalog))
    app.add_handler(CommandHandler("skip", cmd_skip))
    app.add_handler(CommandHandler("deleteall", cmd_deleteall))
    app.add_handler(CommandHandler("leave", cmd_leave))
    
    # Other handlers
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.LOCATION, on_location))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    
    log(f"Starting v{BOT_VERSION}...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
