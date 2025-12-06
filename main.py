import hashlib
import html
import json
import math
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import httpx
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo

from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ------------ Config / constants ------------

TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CLAIM_CODE = os.environ.get("CLAIM_CODE", "").strip()
STATE_FILE = Path(os.environ.get("STATE_FILE", "state.json"))

# Both triggers work: "eta" and "1717"
TRIGGERS = {"eta", "1717"}

TF = TimezoneFinder()

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"


# ------------ Utility / state helpers ------------

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
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "owner_id": None,
        "allowed_chats": [],
        "last_location": None,  # {"lat","lon","updated_at","tz"}
        "job": None,            # current load
        "job_stage": "PU",      # "PU" or "DEL"
        "geocode_cache": {},    # address str -> {"lat","lon"}
    }


def save_state(state: dict) -> None:
    atomic_write_json(STATE_FILE, state)


def is_private(update: Update) -> bool:
    return update.effective_chat and update.effective_chat.type == "private"


def is_group(update: Update) -> bool:
    return update.effective_chat and update.effective_chat.type in ("group", "supergroup")


def is_owner(update: Update, state: dict) -> bool:
    return (
        state.get("owner_id") is not None
        and update.effective_user is not None
        and update.effective_user.id == state["owner_id"]
    )


def chat_allowed(state: dict, chat_id: int) -> bool:
    return chat_id in set(state.get("allowed_chats") or [])


def h(s: str) -> str:
    return html.escape(s or "", quote=False)


# ------------ Time & formatting helpers ------------

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


# ------------ Dispatch parsing ------------

PU_TIME_RE = re.compile(r"^\s*PU time:\s*(.+?)\s*$", re.IGNORECASE)
DEL_TIME_RE = re.compile(r"^\s*DEL time:\s*(.+?)\s*$", re.IGNORECASE)


def collect_block(lines: List[str], prefix: str) -> Optional[List[str]]:
    prefix_l = prefix.lower()
    for i, raw in enumerate(lines):
        line = raw.strip()
        if line.lower().startswith(prefix_l):
            after = line.split(":", 1)[1].strip() if ":" in line else ""
            block: List[str] = []
            if after:
                block.append(after)
            j = i + 1
            while j < len(lines):
                s = lines[j].strip()
                if not s:
                    break
                low = s.lower()
                if low.startswith(("pu time:", "del time:", "pu address:", "del address:")):
                    break
                if set(s) <= {"-"} or set(s) <= {"="}:
                    break
                block.append(s)
                j += 1
            return block if block else None
    return None


def parse_dispatch_post(text: str) -> Optional[dict]:
    if "pu address:" not in text.lower() or "del address:" not in text.lower():
        return None

    lines = [ln.rstrip() for ln in text.splitlines()]

    pu_time = None
    del_time = None
    for ln in lines:
        m = PU_TIME_RE.match(ln)
        if m:
            pu_time = m.group(1).strip()
        m = DEL_TIME_RE.match(ln)
        if m:
            del_time = m.group(1).strip()

    pu_block = collect_block(lines, "PU Address:")
    del_block = collect_block(lines, "DEL Address:")
    if not pu_block or not del_block:
        return None

    pu_addr = ", ".join(pu_block)
    del_addr = ", ".join(del_block)
    job_key = f"{pu_addr}|{del_addr}|{pu_time or ''}|{del_time or ''}"
    job_id = hashlib.sha1(job_key.encode("utf-8")).hexdigest()[:10]

    return {
        "job_id": job_id,
        "pu_time": pu_time,
        "del_time": del_time,
        "pickup_lines": pu_block,
        "delivery_lines": del_block,
        "pickup_address": pu_addr,
        "delivery_address": del_addr,
        "set_at": now_utc_iso(),
    }


# ------------ Distance / ETA helpers ------------

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


# ------------ Geocoding / routing (with smarter geocoder) ------------

async def geocode(address: str) -> Optional[Tuple[float, float]]:
    """
    Try several progressively simpler variants of the address so we don't
    fail just because of store names / suites / weird punctuation.
    """
    headers = {"User-Agent": os.environ.get("NOMINATIM_USER_AGENT", "telegram-location-bot/1.0")}

    base = address.strip()
    if not base:
        return None

    variants = []

    # 1) Full address as-is
    variants.append(base)

    # Split on commas into parts (store, street, city, state zip, ...)
    parts = [p.strip() for p in re.split(r",", base) if p.strip()]

    # 2) Drop first chunk (often store name)
    if len(parts) >= 2:
        variants.append(", ".join(parts[1:]))

    # 3) Last 2 parts (city + state zip)
    if len(parts) >= 2:
        variants.append(", ".join(parts[-2:]))

    # 4) Last 3 parts if available
    if len(parts) >= 3:
        variants.append(", ".join(parts[-3:]))

    # Deduplicate while preserving order
    seen = set()
    clean_variants = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            clean_variants.append(v)

    try:
        async with httpx.AsyncClient(timeout=12.0, headers=headers) as client:
            for q in clean_variants:
                params = {"q": q, "format": "jsonv2", "limit": 1}
                r = await client.get(NOMINATIM_URL, params=params)
                r.raise_for_status()
                data = r.json()
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
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(url, params={"overview": "false"})
            r.raise_for_status()
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
        cache[address] = {"lat": coords[0], "lon": coords[1]}
        state["geocode_cache"] = cache
        save_state(state)
    return coords


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


# ------------ Commands ------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    triggers_txt = " or ".join(sorted(TRIGGERS))
    await update.effective_message.reply_text(
        "👋 Hi!\n"
        f"• Trigger: type “{triggers_txt}” in an allowed group\n"
        "• I’ll auto-detect dispatch posts with PU/DEL format\n\n"
        "Owner setup:\n"
        "1) DM: /claim <code>\n"
        "2) DM: /update (send current or Live Location)\n"
        "3) Group: /allowhere\n"
        "Stage control: /pickupdone, /pickuppending, /skip\n"
    )


async def claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        await update.effective_message.reply_text("Please DM me /claim.")
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
    state = load_state()
    state["owner_id"] = update.effective_user.id
    save_state(state)
    await update.effective_message.reply_text("✅ You are now the owner.")


async def allowhere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not is_owner(update, state):
        await update.effective_message.reply_text("Only the owner can do that.")
        return
    if not is_group(update):
        await update.effective_message.reply_text("Run this inside the target group.")
        return
    chat_id = update.effective_chat.id
    allowed = set(state.get("allowed_chats", []))
    allowed.add(chat_id)
    state["allowed_chats"] = sorted(list(allowed))
    save_state(state)
    triggers_txt = " or ".join(sorted(TRIGGERS))
    await update.effective_message.reply_text(f"✅ Allowed. Trigger words: {triggers_txt}")


async def disallowhere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not is_owner(update, state):
        await update.effective_message.reply_text("Only the owner can do that.")
        return
    if not is_group(update):
        await update.effective_message.reply_text("Run this inside the target group.")
        return
    chat_id = update.effective_chat.id
    allowed = set(state.get("allowed_chats", []))
    allowed.discard(chat_id)
    state["allowed_chats"] = sorted(list(allowed))
    save_state(state)
    await update.effective_message.reply_text("✅ Group removed from allowed list.")


async def update_loc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not is_owner(update, state):
        await update.effective_message.reply_text("Only the owner can update location.")
        return
    if not is_private(update):
        await update.effective_message.reply_text("DM me /update (best).")
        return

    kb = [[KeyboardButton("📍 Send my current location", request_location=True)]]
    await update.effective_message.reply_text(
        "Tap to send your current location.\n"
        "Tip: you can also send a Live Location (Attach → Location → Share Live Location) and I’ll keep it updated.",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True),
    )


async def on_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    # Confirm only on initial message (avoid spam on live updates)
    if update.message is not None:
        await msg.reply_text("✅ Saved your location.", reply_markup=ReplyKeyboardRemove())


async def pickupdone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not is_owner(update, state):
        await update.effective_message.reply_text("Only the owner can do that.")
        return
    state["job_stage"] = "DEL"
    save_state(state)
    await update.effective_message.reply_text("✅ Stage: DELIVERY")


async def pickuppending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not is_owner(update, state):
        await update.effective_message.reply_text("Only the owner can do that.")
        return
    state["job_stage"] = "PU"
    save_state(state)
    await update.effective_message.reply_text("✅ Stage: PICKUP")


async def skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not is_owner(update, state):
        await update.effective_message.reply_text("Only the owner can do that.")
        return

    if not state.get("job"):
        await update.effective_message.reply_text("No active job to skip.")
        return

    if state.get("job_stage", "PU") == "PU":
        state["job_stage"] = "DEL"
        save_state(state)
        await update.effective_message.reply_text("⏭️ Skipped PICKUP. Now targeting DELIVERY.")
    else:
        state["job"] = None
        state["job_stage"] = "PU"
        save_state(state)
        await update.effective_message.reply_text("✅ Cleared current job.")


# ------------ ETA formatting ------------

def job_html(job: dict) -> str:
    pu_time = job.get("pu_time")
    del_time = job.get("del_time")
    pu_lines = job.get("pickup_lines") or []
    del_lines = job.get("delivery_lines") or []

    out = ["<b>Pickup</b>"]
    if pu_time:
        out.append(f"⏱ {h(pu_time)}")
    out.extend(h(x) for x in pu_lines)
    out.append("")
    out.append("<b>Delivery</b>")
    if del_time:
        out.append(f"⏱ {h(del_time)}")
    out.extend(h(x) for x in del_lines)
    return "\n".join(out)


async def send_eta(update: Update, context: ContextTypes.DEFAULT_TYPE, target: str = "AUTO"):
    state = load_state()
    chat = update.effective_chat
    msg = update.effective_message
    if not chat or not msg:
        return

    if is_group(update) and not chat_allowed(state, chat.id):
        return

    loc = state.get("last_location")
    if not loc:
        await msg.reply_text("No saved location yet. Owner: DM /update (or share Live Location).")
        return

    origin = (float(loc["lat"]), float(loc["lon"]))
    tz_name = loc.get("tz") or "UTC"
    updated_dt = parse_iso(loc.get("updated_at", "")) or now_utc()

    job = state.get("job")
    stage = state.get("job_stage", "PU")

    # Send current pin
    await context.bot.send_location(chat_id=chat.id, latitude=origin[0], longitude=origin[1])

    header = [
        "<b>🚚 ETA</b>",
        f"<b>Local time:</b> {h(local_time_str(tz_name))}",
        f"<b>GPS updated:</b> {h(format_delta(updated_dt))}",
    ]

    if not job:
        await msg.reply_text(
            "\n".join(header + ["", "<i>No active load detected yet.</i>"]),
            parse_mode=ParseMode.HTML,
        )
        return

    header += [
        f"<b>Stage:</b> {'PICKUP' if stage == 'PU' else 'DELIVERY'}",
        "",
        job_html(job),
    ]

    # Decide which ETA(s) to show
    t = target.upper()
    which: List[str]
    if t == "BOTH":
        which = ["PU", "DEL"]
    elif t == "PU":
        which = ["PU"]
    elif t == "DEL":
        which = ["DEL"]
    else:
        which = ["PU" if stage == "PU" else "DEL"]

    lines: List[str] = []
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    if "PU" in which:
        r = await compute_eta(state, origin, "Pickup", job["pickup_address"])
        lines.append("")
        lines.append("<b>ETA to Pickup</b>")
        if r.get("ok"):
            lines.append(
                f"🛣 {h(fmt_distance_miles(r['distance_m']))} · "
                f"⏳ {h(fmt_duration(r['duration_s']))} ({h(r['method'])})"
            )
            arrive = now_utc().astimezone(tz) + timedelta(seconds=float(r["duration_s"]))
            lines.append(f"🕒 Arrive ~ {h(arrive.strftime('%H:%M'))}")
        else:
            lines.append(f"⚠️ {h(r.get('error', 'Could not compute'))}")

    if "DEL" in which:
        r = await compute_eta(state, origin, "Delivery", job["delivery_address"])
        lines.append("")
        lines.append("<b>ETA to Delivery</b>")
        if r.get("ok"):
            lines.append(
                f"🛣 {h(fmt_distance_miles(r['distance_m']))} · "
                f"⏳ {h(fmt_duration(r['duration_s']))} ({h(r['method'])})"
            )
            arrive = now_utc().astimezone(tz) + timedelta(seconds=float(r["duration_s"]))
            lines.append(f"🕒 Arrive ~ {h(arrive.strftime('%H:%M'))}")
        else:
            lines.append(f"⚠️ {h(r.get('error', 'Could not compute'))}")

    await msg.reply_text(
        "\n".join(header + lines),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


# ------------ Text handler (dispatch + triggers) ------------

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    if not msg or not msg.text or not chat:
        return

    state = load_state()
    text = msg.text.strip()
    low = text.lower()

    # Block if group not allowed
    if is_group(update) and not chat_allowed(state, chat.id):
        return

    # 1) Auto-detect dispatch posts in group
    if is_group(update):
        job = parse_dispatch_post(msg.text)
        if job:
            prev = state.get("job") or {}
            if prev.get("job_id") != job["job_id"]:
                state["job"] = job
                state["job_stage"] = "PU"
                save_state(state)

                # Pre-geocode
                await get_coords_cached(state, job["pickup_address"])
                await get_coords_cached(state, job["delivery_address"])

                await msg.reply_text(
                    "📦 New load detected. Stage reset to PICKUP.\n"
                    "Use /pickupdone when loaded, /skip to jump or clear, "
                    "and type “eta” or “1717” for ETA.",
                )
            return

    # 2) Trigger words: "eta", "1717", and variants with arguments
    for trig in TRIGGERS:
        if low == trig or low.startswith(trig + " "):
            arg = low[len(trig):].strip()
            target = "AUTO"
            if arg in ("pu", "pickup"):
                target = "PU"
            elif arg in ("del", "delivery"):
                target = "DEL"
            elif arg in ("both", "all"):
                target = "BOTH"
            await send_eta(update, context, target=target)
            return


# ------------ Main entrypoint ------------

def main():
    if not TOKEN:
        raise RuntimeError("Missing TELEGRAM_TOKEN")

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("claim", claim))
    app.add_handler(CommandHandler("allowhere", allowhere))
    app.add_handler(CommandHandler("disallowhere", disallowhere))
    app.add_handler(CommandHandler("update", update_loc))

    app.add_handler(CommandHandler("pickupdone", pickupdone))
    app.add_handler(CommandHandler("pickuppending", pickuppending))
    app.add_handler(CommandHandler("skip", skip))

    app.add_handler(MessageHandler(filters.LOCATION, on_location))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
