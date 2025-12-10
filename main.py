import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "✅ Test bot is alive.\n\n"
        "If you see this, the token and Railway setup are correct."
    )

async def claim(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "This is just the test /claim.\n"
        "We’ll switch to the full dispatch bot after this works."
    )

def main() -> None:
    if not TOKEN:
        raise RuntimeError("Missing TELEGRAM_TOKEN")

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("claim", claim))
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
