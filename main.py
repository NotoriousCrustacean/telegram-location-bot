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

from openpyxl import Workbook
from openpyxl.styles import Font

BOT_VERSION = "2025-12-10_rev1"


# ----------------------------
# Environment helpers
# ----------------------------
def _strip_quotes(s: str) -> str:
    s = (s or "").strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        return s[1:-1].strip()
    return s


def env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    if v is None:
        return default
    return _strip_quotes(v)


def env_int(name: str, default: int) -> int:
    v = env_str(name, "")
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    v = env_str(name, "")
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    v = env_str(name, "")
    if not v:
        return default
    return v.lower() in ("1", "true", "yes", "y", "on")


# ----------------------------
# ENV
# ----------------------------
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
    if DEBUG:
        print(f"[bot {BOT_VERSION}] {msg}", flush=True)


# ----------------------------
# Globals
# ----------------------------
TF = TimezoneFinder()
NOM_URL = "https://nominatim.openstreetmap.org/search"
OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"

_state_lock = asyncio.Lock()
_geo_lock = asyncio.Lock()
_geo_last = 0.0


# ----------------------------
# Time helpers
# ----------------------------
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


# ----------------------------
# State load/save
# ----------------------------
def _migrate_state(st: dict) -> Tuple[dict, bool]:
    changed = False

    # owner aliases
    if st.get("owner_id") is None and st.get("owner") is not None:
        st["owner_id"] = st.get("owner")
        changed = True
    if st.get("owner") is None and st.get("owner_id") is not None:
        st["owner"] = st.get("owner_id")
        changed = True

    # allowed chats aliases
    if (not st.get("allowed_chats")) and st.get("allowed"):
        st["allowed_chats"] = st.get("allowed")
        changed = True
    if (not st.get("allowed")) and st.get("allowed_chats"):
        st["allowed"] = st.get("allowed_chats")
        changed = True

    # last location aliases
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

    # geocode cache aliases
    if (not st.get("geocode_cache")) and st.get("gc"):
        st["geocode_cache"] = st.get("gc")
        changed = True
    if (not st.get("gc")) and st.get("geocode_cache"):
        st["gc"] = st.get("geocode_cache")
        changed = True

    # history aliases
    if (not st.get("history")) and st.get("hist"):
        st["history"] = st.get("hist")
        changed = True
    if (not st.get("hist")) and st.get("history"):
        st["hist"] = st.get("history")
        changed = True

    if st.get("focus_i") is None and st.get("del_index") is not None:
        st["focus_i"] = st.get("del_index")
        changed = True

    st.setdefault("owner_id", None)
    st.setdefault("allowed_chats", [])
    st.setdefault("last_location", None)
    st.setdefault("job", None)
    st.setdefault("focus_i", 0)
    st.setdefault("geocode_cache", {})
    st.setdefault("history", [])

    # mirror legacy keys
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


# ----------------------------
# Geocode/routing
# ----------------------------
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
    if "usa" not in a.lower():
        out.append(a + ", USA")
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
# ----------------------------
# Load parsing
# ----------------------------
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
    job = {
        "id": jid,
        "meta": meta,
        "pu": {"addr": pu_addr, "lines": pu_lines or [pu_addr], "time": pu_time},
        "del": dels,
    }
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
    job = {
        "id": jid,
        "meta": meta,
        "pu": {"addr": pu_addr, "lines": [pu_addr], "time": pu_time},
        "del": dels,
    }
    return init_job(job)


def parse_job(text: str) -> Optional[dict]:
    return parse_detailed(text) or parse_summary(text)


# ----------------------------
# Workflow helpers + UI
# ----------------------------
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
    m = job.get("meta") or {}
    if m.get("load_number"):
        return f"Load {m.get('load_number')}"
    return f"Job {job.get('id', '')}"


def toggle_ts(obj: dict, key: str) -> bool:
    if obj.get(key):
        obj[key] = None
        return False
    obj[key] = now_iso()
    return True


async def send_progress_alert(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str) -> None:
    try:
        m = await ctx.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            disable_notification=True,
        )
    except TelegramError:
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
    for x in reversed(lines or []):
        x = (x or "").strip()
        if x and len(x) <= 70:
            return x
    return (addr or "").strip()


def b(label: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(label, callback_data=data)


def chk(on: bool, label: str) -> str:
    return ("✅ " + label) if on else label


def build_keyboard(job: dict, st: dict) -> InlineKeyboardMarkup:
    stage, i = focus(job, st)
    pu = job["pu"]
    ps = pu["status"]
    pd = pu["docs"]

    rows: List[List[InlineKeyboardButton]] = []

    if stage == "PU":
        rows.append(
            [
                b(chk(bool(ps["arr"]), "Arrived PU"), "PU:A"),
                b(chk(bool(ps["load"]), "Loaded"), "PU:L"),
                b(chk(bool(ps["dep"]), "Departed"), "PU:D"),
            ]
        )
        rows.append(
            [
                b(chk(bool(pd.get("pti")), "PTI"), "DOC:PTI"),
                b(chk(bool(pd.get("bol")), "BOL"), "DOC:BOL"),
                b(chk(bool(ps["comp"]), "PU Complete"), "PU:C"),
            ]
        )
    else:
        dels = job.get("del") or []
        d = dels[i] if dels else {"addr": "", "lines": []}
        ds = d.get("status") or {}
        dd = d.get("docs") or {}
        lbl = f"DEL {i+1}/{len(dels)}" if dels else "DEL"

        rows.append(
            [
                b(chk(bool(ds.get("arr")), f"Arrived {lbl}"), "DEL:A"),
                b(chk(bool(ds.get("del")), "Delivered"), "DEL:DL"),
                b(chk(bool(ds.get("dep")), "Departed"), "DEL:D"),
            ]
        )
        rows.append(
            [
                b(chk(bool(dd.get("pod")), "POD"), "DOC:POD"),
                b(chk(bool(ds.get("comp")), "Stop Complete"), "DEL:C"),
                b("Skip Stop", "DEL:S"),
            ]
        )

    rows.append([b("ETA", "ETA:A"), b("ETA all", "ETA:ALL")])
    rows.append([b("📊 Catalog", "SHOW:CAT"), b("Finish Load", "JOB:FIN")])
    return InlineKeyboardMarkup(rows)
# ----------------------------
# Commands
# ----------------------------
async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        f"Dispatch Bot ({BOT_VERSION})\n"
        f"Triggers: {', '.join(sorted(TRIGGERS))}\n\n"
        "DM setup:\n"
        "1) /claim <code>\n"
        "2) /update (send location)\n\n"
        "Group setup:\n"
        "3) /allowhere (in the group)\n\n"
        "Use: eta / 1717 or /panel\n"
        "Catalog: /finish • /catalog\n"
        "Tools: /leave • /deleteall\n"
        "Debug: /status • /ping"
    )


async def ping_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(f"pong ✅ ({BOT_VERSION})")


async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()

    uid = update.effective_user.id if update.effective_user else None
    allowed_here = chat_allowed(update, st)
    loc = st.get("last_location")
    job = normalize_job(st.get("job"))

    lines = [
        f"<b>Status</b> ({h(BOT_VERSION)})",
        f"<b>Your user id:</b> {h(uid)}",
        f"<b>Owner id:</b> {h(st.get('owner_id'))}",
        f"<b>This chat allowed:</b> {h(allowed_here)}",
        f"<b>Allowed chats:</b> {h(len(st.get('allowed_chats') or []))}",
        f"<b>State file:</b> {h(str(STATE_FILE))}",
        f"<b>Location saved:</b> {'✅' if loc else '❌'}",
        f"<b>Active load:</b> {'✅' if job else '❌'}",
    ]
    await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML")


async def claim_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.effective_message.reply_text("DM me: /claim <code>")
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
    async with _state_lock:
        st = load_state()
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only. DM /claim <code> first.")
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
    async with _state_lock:
        st = load_state()
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only. DM /claim <code> first.")
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
    msg = update.effective_message
    if not msg or not msg.location:
        return

    async with _state_lock:
        st = load_state()
        if not is_owner(update, st):
            return
        lat, lon = msg.location.latitude, msg.location.longitude
        tz = TF.timezone_at(lat=lat, lng=lon) or "UTC"
        st["last_location"] = {"lat": lat, "lon": lon, "tz": tz, "updated_at": now_iso()}
        save_state(st)

    if update.effective_chat.type == "private":
        await msg.reply_text("✅ Location saved.", reply_markup=ReplyKeyboardRemove())


async def panel_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()

    if not chat_allowed(update, st):
        if update.effective_chat.type != "private":
            await update.effective_message.reply_text("This chat isn't allowed yet. Owner: run /allowhere here.")
        return

    job = normalize_job(st.get("job"))
    if not job:
        await update.effective_message.reply_text("No active load detected yet.")
        return

    await update.effective_message.reply_text(
        f"<b>{h(load_id_text(job))}</b>\nTap buttons to update status.",
        parse_mode="HTML",
        reply_markup=build_keyboard(job, st),
    )


# ----------------------------
# ETA
# ----------------------------
async def send_eta(update: Update, ctx: ContextTypes.DEFAULT_TYPE, which: str):
    async with _state_lock:
        st = load_state()

    if not chat_allowed(update, st):
        return

    loc = st.get("last_location")
    if not loc:
        await update.effective_message.reply_text("No saved location yet. Owner: DM /update.")
        return

    origin = (float(loc["lat"]), float(loc["lon"]))
    tz_now = loc.get("tz") or "UTC"
    tz = safe_tz(tz_now)

    await ctx.bot.send_location(chat_id=update.effective_chat.id, latitude=origin[0], longitude=origin[1])

    job = normalize_job(st.get("job"))
    if not job:
        await update.effective_message.reply_text(
            f"<b>⏱ ETA</b>\nNow: {h(datetime.now(tz).strftime('%Y-%m-%d %H:%M'))} ({h(tz_now)})\n\n<i>No active load yet.</i>",
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
                    f"<b>{h(lab)}:</b> <b>{h(fmt_dur(r['s']))}</b>{h(tag)} · {h(fmt_mi(r['m']))} · ~{h(arr)}{h(appt_txt)} — {h(place)}"
                )
            else:
                lines.append(f"<b>{h(lab)}:</b> ⚠️ {h(r.get('err'))} — {h(place)}")

        await update.effective_message.reply_text(
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=build_keyboard(job, st),
        )
        return

    stage, i = focus(job, st)
    if stage == "PU":
        addr = job["pu"]["addr"]
        lines2 = job["pu"].get("lines") or []
        appt = job["pu"].get("time")
        stop_label = "PU"
        label = "Pickup"
    else:
        dels = job.get("del") or []
        d = dels[i] if dels else {"addr": "", "lines": [], "time": None}
        addr = d["addr"]
        lines2 = d.get("lines") or []
        appt = d.get("time")
        stop_label = f"DEL {i+1}/{len(dels)}" if dels else "DEL"
        label = f"Delivery {i+1}/{len(dels)}"

    r = await eta_to(st, origin, label, addr)
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
            reply_markup=build_keyboard(job, st),
        )
    else:
        await update.effective_message.reply_text(
            f"<b>{h(load_id_text(job))}</b>\n<b>⏱ ETA:</b> ⚠️ {h(r.get('err'))}\n<b>Target:</b> {h(place)}",
            parse_mode="HTML",
            reply_markup=build_keyboard(job, st),
        )


# ----------------------------
# Catalog (Excel)
# ----------------------------
def week_key(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


async def estimate_miles(st: dict, job: dict) -> Optional[float]:
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
    for a, b_ in zip(coords, coords[1:]):
        r = await route(a, b_)
        total_m += r[0] if r else hav_m(a[0], a[1], b_[0], b_[1])
    return total_m / 1609.344


def make_xlsx(records: List[dict], title: str) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Loads"

    ws.append([title])
    ws["A1"].font = Font(bold=True, size=14)

    headers = [
        "Week",
        "Completed",
        "Load #",
        "Job ID",
        "Load Date",
        "PU Time",
        "PU Location",
        "DEL Times",
        "DEL Locations",
        "Rate",
        "Posted Miles",
        "Est Miles",
        "Rate/EstMi",
    ]
    ws.append(headers)
    for c in ws[2]:
        c.font = Font(bold=True)

    total_rate = 0.0
    total_est = 0.0

    for r in records:
        rate = r.get("rate")
        est_mi = r.get("est_miles")
        rpm = None
        if rate is not None and est_mi:
            try:
                rpm = float(rate) / float(est_mi)
            except Exception:
                rpm = None

        ws.append(
            [
                r.get("week"),
                r.get("completed"),
                r.get("load_number"),
                r.get("job_id"),
                r.get("load_date"),
                r.get("pu_time"),
                r.get("pickup"),
                r.get("del_times"),
                r.get("deliveries"),
                rate,
                r.get("posted_miles"),
                est_mi,
                rpm,
            ]
        )

        if rate is not None:
            total_rate += float(rate)
        if est_mi is not None:
            total_est += float(est_mi)

    ws.append([])
    ws.append(
        [
            "TOTAL",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            total_rate,
            "",
            total_est,
            (total_rate / total_est) if total_est else None,
        ]
    )
    for c in ws[ws.max_row]:
        c.font = Font(bold=True)

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


async def finish_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only.")
            return
        job = normalize_job(st.get("job"))
        if not job:
            await update.effective_message.reply_text("No active load.")
            return

    loc = st.get("last_location") or {}
    tz_name = loc.get("tz") or "UTC"
    dt_local = now_utc().astimezone(safe_tz(tz_name))
    wk = week_key(dt_local)

    meta = job.get("meta") or {}
    est = await estimate_miles(st, job)

    pu = job["pu"]
    dels = job.get("del") or []
    del_times = " | ".join(((d.get("time") or "").strip() or "-") for d in dels)

    rec = {
        "week": wk,
        "completed": dt_local.strftime("%Y-%m-%d %H:%M"),
        "load_number": meta.get("load_number") or "",
        "job_id": job.get("id"),
        "load_date": meta.get("load_date"),
        "pu_time": pu.get("time"),
        "pickup": (pu.get("addr") or ""),
        "deliveries": " | ".join((d.get("addr") or "") for d in dels),
        "del_times": del_times,
        "rate": meta.get("rate"),
        "posted_miles": meta.get("miles"),
        "est_miles": est,
    }

    async with _state_lock:
        st2 = load_state()
        hist = list(st2.get("history") or [])
        hist.append(rec)
        st2["history"] = hist[-600:]
        st2["job"] = None
        st2["focus_i"] = 0
        save_state(st2)

    await update.effective_message.reply_text("✅ Load archived + cleared.")


async def catalog_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only.")
            return
        if not chat_allowed(update, st):
            await update.effective_message.reply_text("Run /catalog in an allowed chat (or DM as owner).")
            return
        hist = list(st.get("history") or [])
        loc = st.get("last_location") or {}
        tz_name = loc.get("tz") or "UTC"

    if not hist:
        await update.effective_message.reply_text("No finished loads yet. Use /finish when a load is done.")
        return

    wk = week_key(now_utc().astimezone(safe_tz(tz_name)))

    if ctx.args:
        a = ctx.args[0].strip().lower()
        if a == "all":
            wk = "ALL"
        elif re.fullmatch(r"\d{4}-w\d{2}", a):
            wk = a.upper().replace("w", "W")
        elif a in ("last", "prev"):
            wk = week_key(now_utc().astimezone(safe_tz(tz_name)) - timedelta(days=7))

    records = hist if wk == "ALL" else [r for r in hist if r.get("week") == wk]
    if not records:
        await update.effective_message.reply_text("No records for that week.")
        return

    title = f"Weekly Load Catalog ({wk})" if wk != "ALL" else "Load Catalog (ALL)"
    xlsx = make_xlsx(records, title)

    bio = io.BytesIO(xlsx)
    bio.name = f"load_catalog_{wk}.xlsx"
    await ctx.bot.send_document(
        chat_id=update.effective_chat.id,
        document=bio,
        filename=bio.name,
        caption=title,
    )


# ----------------------------
# Admin tools
# ----------------------------
async def deleteall_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only.")
            return

    chat = update.effective_chat
    if not chat or chat.type == "private":
        await update.effective_message.reply_text("Bots can't clear a DM history. Delete the chat from your side.")
        return

    n = DELETEALL_DEFAULT
    if ctx.args:
        try:
            n = max(1, min(2000, int(ctx.args[0])))
        except ValueError:
            pass

    notice = await update.effective_message.reply_text(f"🧹 Deleting up to {n} messages… (bot must be admin)")
    start_id = notice.message_id

    for mid in range(start_id, max(1, start_id - n + 1) - 1, -1):
        try:
            await ctx.bot.delete_message(chat_id=chat.id, message_id=mid)
        except (Forbidden, BadRequest):
            break
        await asyncio.sleep(0.02)


async def leave_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with _state_lock:
        st = load_state()
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only.")
            return

        chat = update.effective_chat
        if not chat or chat.type == "private":
            await update.effective_message.reply_text("Run /leave inside the group you want the bot to leave.")
            return

        allowed = set(st.get("allowed_chats") or [])
        allowed.discard(chat.id)
        st["allowed_chats"] = sorted(list(allowed))
        save_state(st)

    await update.effective_message.reply_text("👋 Leaving this chat…")
    try:
        await ctx.bot.leave_chat(chat.id)
    except Exception:
        pass


# ----------------------------
# Callback handler
# ----------------------------
async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.data:
        return

    data = q.data

    async with _state_lock:
        st = load_state()

    if not chat_allowed(update, st):
        await q.answer("Not allowed here.", show_alert=False)
        return

    if data.startswith("ETA:"):
        await q.answer("Computing ETA…", show_alert=False)
        await send_eta(update, ctx, data.split(":", 1)[1])
        return

    if data == "SHOW:CAT":
        if not is_owner(update, st):
            await q.answer("Owner only.", show_alert=False)
            return
        await q.answer("Building catalog…", show_alert=False)
        await catalog_cmd(update, ctx)
        return

    if data == "JOB:FIN":
        if not is_owner(update, st):
            await q.answer("Owner only.", show_alert=False)
            return
        await q.answer("Finishing load…", show_alert=False)
        await finish_cmd(update, ctx)
        return

    progress_broadcast: Optional[str] = None

    async with _state_lock:
        st2 = load_state()
        job = normalize_job(st2.get("job"))
        if not job:
            await q.answer("No active load.", show_alert=False)
            return

        stage, i = focus(job, st2)
        tz_name = ((st2.get("last_location") or {}).get("tz")) or "UTC"
        ts = local_stamp(tz_name)
        load_label = load_id_text(job)

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
    except Exception:
        pass


# ----------------------------
# Text handler (new load + triggers)
# ----------------------------
async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.text:
        return

    async with _state_lock:
        st = load_state()

    chat = update.effective_chat

    # Detect new loads only in allowed groups
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

    # Triggers in allowed chats
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


# ----------------------------
# Startup: disable webhook so polling works
# ----------------------------
async def _post_init(app):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    try:
        me = await app.bot.get_me()
        log(f"Connected as @{me.username} (id {me.id})")
    except Exception as e:
        log(f"get_me failed: {e}")
    log("Ready.")


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    if not TOKEN:
        raise RuntimeError("Missing TELEGRAM_TOKEN")

    builder = ApplicationBuilder().token(TOKEN)
    try:
        builder = builder.post_init(_post_init)
    except Exception:
        pass

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
    app.add_handler(CommandHandler("deleteall", deleteall_cmd))
    app.add_handler(CommandHandler("leave", leave_cmd))

    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.LOCATION, on_location))

    # Live location updates are edited messages; this handler may not exist in older PTB versions.
    try:
        app.add_handler(MessageHandler(filters.UpdateType.EDITED_MESSAGE & filters.LOCATION, on_location))
    except Exception:
        pass

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    log("Starting polling…")
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES, close_loop=False)


if __name__ == "__main__":
    main()
