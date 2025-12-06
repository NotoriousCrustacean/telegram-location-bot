import json
import os
from datetime import datetime, timezone
from pathlib import Path

from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CLAIM_CODE = os.environ.get("CLAIM_CODE", "").strip()  # you set this on Railway

STATE_FILE = Path(os.environ.get("STATE_FILE", "state.json"))

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "owner_id": None,
        "allowed_chats": [],   # list of chat_ids where the "!" trigger is allowed
        "last_location": None  # {"lat":..., "lon":..., "updated_at":...}
    }

def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state), encoding="utf-8")

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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "👋 Hi! I can post your saved location when someone sends a single `!`.\n\n"
        "Setup (owner only):\n"
        "1) DM me: /claim <your_code>\n"
        "2) DM me: /update (then tap the location button)\n"
        "3) In your group: /allowhere\n\n"
        "Then anyone in that group can send `!` and I’ll reply with the saved location.\n"
    )
    await update.message.reply_text(msg)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    owner = state.get("owner_id")
    allowed = state.get("allowed_chats", [])
    loc = state.get("last_location")

    text = (
        f"Owner set: {'✅' if owner else '❌'}\n"
        f"Allowed groups: {len(allowed)}\n"
        f"Location saved: {'✅' if loc else '❌'}\n"
    )
    if loc:
        text += f"Last updated (UTC): {loc.get('updated_at')}\n"
    await update.message.reply_text(text)

async def claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        await update.message.reply_text("Please DM me /claim (for safety).")
        return

    if not CLAIM_CODE:
        await update.message.reply_text("Bot is missing CLAIM_CODE on the server. Set it in Railway Variables.")
        return

    args = context.args or []
    if not args:
        await update.message.reply_text("Use: /claim <your_code>")
        return

    code = " ".join(args).strip()
    if code != CLAIM_CODE:
        await update.message.reply_text("❌ Wrong claim code.")
        return

    state = load_state()
    state["owner_id"] = update.effective_user.id
    save_state(state)
    await update.message.reply_text("✅ You are now the owner.")

async def allowhere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not is_owner(update, state):
        await update.message.reply_text("Only the owner can do that.")
        return
    if not is_group(update):
        await update.message.reply_text("Use this command inside the group you want to allow.")
        return

    chat_id = update.effective_chat.id
    allowed = set(state.get("allowed_chats", []))
    allowed.add(chat_id)
    state["allowed_chats"] = sorted(list(allowed))
    save_state(state)
    await update.message.reply_text("✅ This group is now allowed for `!`.")

async def disallowhere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not is_owner(update, state):
        await update.message.reply_text("Only the owner can do that.")
        return
    if not is_group(update):
        await update.message.reply_text("Use this command inside the group you want to remove.")
        return

    chat_id = update.effective_chat.id
    allowed = set(state.get("allowed_chats", []))
    allowed.discard(chat_id)
    state["allowed_chats"] = sorted(list(allowed))
    save_state(state)
    await update.message.reply_text("✅ This group is no longer allowed.")

async def update_loc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not is_owner(update, state):
        await update.message.reply_text("Only the owner can update the saved location.")
        return
    if not is_private(update):
        await update.message.reply_text("Please DM me /update (Telegram location button works best in DMs).")
        return

    kb = [[KeyboardButton("📍 Send my current location", request_location=True)]]
    await update.message.reply_text(
        "Tap the button to send your current location.",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True),
    )

async def on_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not is_owner(update, state):
        return
    if not update.message or not update.message.location:
        return
    loc = update.message.location
    state["last_location"] = {
        "lat": loc.latitude,
        "lon": loc.longitude,
        "updated_at": now_utc_iso(),
    }
    save_state(state)
    await update.message.reply_text("✅ Saved your location.", reply_markup=ReplyKeyboardRemove())

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    if update.message.text.strip() != "1717":
        return

    state = load_state()
    allowed = set(state.get("allowed_chats", []))
    chat_id = update.effective_chat.id if update.effective_chat else None

    if not allowed:
        # safer default: require /allowhere
        await update.message.reply_text("Not configured yet. Owner should run /allowhere in this group.")
        return

    if chat_id not in allowed:
        return

    loc = state.get("last_location")
    if not loc:
        await update.message.reply_text("No saved location yet. Owner should DM me /update first.")
        return

    await context.bot.send_location(
        chat_id=chat_id,
        latitude=loc["lat"],
        longitude=loc["lon"],
    )
    await update.message.reply_text(f"Last updated (UTC): {loc['updated_at']}")

def main():
    if not TOKEN:
        raise RuntimeError("Missing TELEGRAM_TOKEN env var")

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("claim", claim))
    app.add_handler(CommandHandler("allowhere", allowhere))
    app.add_handler(CommandHandler("disallowhere", disallowhere))
    app.add_handler(CommandHandler("update", update_loc))

    app.add_handler(MessageHandler(filters.LOCATION, on_location))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
