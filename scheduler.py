"""
اسکن خودکار واچ‌لیست کاربران در پس‌زمینه
هر SCAN_INTERVAL ثانیه، همه‌ی نمادهای واچ‌شده تحلیل می‌شن و اگه سیگنال
نسبت به آخرین باری که ثبت شده تغییر کرده باشه (و NEUTRAL نباشه)، به
کاربرانی که اون نماد رو واچ کردن و autoscan فعاله پیام می‌فرسته.
"""
import logging
import aiosqlite
from collections import defaultdict
from telegram.ext import Application
from telegram.constants import ParseMode
from exchange import ExchangeClient
from signals import analyze_symbol, SIGNAL_EMOJI, SIGNAL_FA
import database as db
import backup

logger = logging.getLogger(__name__)


async def _background_recipients(user_ids, kind: str) -> set[int]:
    """انتخاب اولیه؛ خطای خواندن سیاست، مجوز ارسال ایجاد نمی‌کند."""
    try:
        return await db.get_background_notification_recipients(user_ids, kind)
    except aiosqlite.Error:
        logger.exception("خواندن سیاست گیرندگان %s ناموفق بود؛ ارسال متوقف شد.", kind)
        return set()


async def _can_send_background(user_id: int, kind: str, symbol: str | None = None) -> bool:
    """بدون کش و درست پیش از send؛ خطای سیاست = عدم ارسال."""
    try:
        return await db.can_receive_background_notification(user_id, kind, symbol)
    except aiosqlite.Error:
        logger.exception("خواندن دسترسی اعلان %s برای %s ناموفق بود؛ پیام ارسال نمی‌شود.", kind, user_id)
        return False


async def scan_job(app: Application):
    logger.info("شروع اسکن خودکار واچ‌لیست‌ها...")
    pairs = await db.get_all_active_watch_pairs()  # [(user_id, symbol), ...]
    if not pairs:
        return
    allowed = await _background_recipients([uid for uid, _ in pairs], "scan")
    pairs = [(uid, symbol) for uid, symbol in pairs if uid in allowed]
    if not pairs:
        return

    symbol_to_users = defaultdict(list)
    for user_id, symbol in pairs:
        symbol_to_users[symbol].append(user_id)

    client = ExchangeClient()
    try:
        for symbol, user_ids in symbol_to_users.items():
            try:
                result = await analyze_symbol(client, symbol)
            except Exception as e:
                logger.warning(f"خطا در تحلیل {symbol}: {e}")
                continue

            last = await db.get_last_signal(symbol)
            should_notify = (
                result.signal_type != "NEUTRAL"
                and (last is None or last["signal_type"] != result.signal_type)
            )

            await db.log_signal(symbol, result.signal_type, result.total_score, result.current_price)

            if not should_notify:
                continue

            emoji = SIGNAL_EMOJI[result.signal_type]
            text = (
                f"{emoji} *هشدار سیگنال جدید*\n\n"
                f"نماد: `{symbol}`\n"
                f"قیمت: `{result.current_price:,.4f}`\n"
                f"سیگنال: *{SIGNAL_FA[result.signal_type]}*\n"
                f"امتیاز: `{result.total_score}`/`{result.max_possible_score}`\n\n"
                f"⚠️ توصیه مالی نیست."
            )
            for user_id in user_ids:
                if not await _can_send_background(user_id, "scan", symbol):
                    continue
                try:
                    await app.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.MARKDOWN)
                except Exception as e:
                    logger.warning(f"ارسال پیام به {user_id} ناموفق بود: {e}")
    finally:
        await client.close()
    logger.info("اسکن خودکار تمام شد.")


async def check_signal_performance_job(app: Application):
    """
    هر سیگنال بازی که از /signal صادر شده رو با قیمت لحظه‌ای مقایسه
    می‌کنه؛ اگه به سطح جدیدی (TP1/TP2/TP3/SL) نسبت به آخرین وضعیت
    ثبت‌شده رسیده باشه، وضعیتش رو به‌روز و به کاربر خبر می‌ده. فقط
    TP3، SL و BREAKEVEN نهایی‌ان (سیگنال می‌بنده)؛ TP1/TP2 سیگنال رو
    باز نگه می‌دارن چون ممکنه به سطح بعدی هم برسه.

    بعد از رسیدن به TP1 یا عبور مستقیم به TP2، اگه BREAKEVEN_AFTER_TP1
    فعال باشه و SL هنوز سربه‌سر نشده باشه، حد ضرر ثبت‌شده در پایش به
    نقطه‌ی ورود منتقل می‌شه (نه سفارش واقعی صرافی)؛ اگه بعداً به
    همون SL جدید (=entry) برخورد کنه، به‌جای SL_HIT، BREAKEVEN_HIT ثبت
    می‌شه (نه برد نه باخت).

    ثبت‌های جدید تا مشاهده‌ی محدوده‌ی ورود WAITING_ENTRY می‌مانند؛
    لمس SL پیش از ورود، ابطال مدل است نه باخت. قیمت ticker صرفاً یک
    مشاهده است و لمس‌های بین دو نوبت پایش یا اجرای سفارش را اثبات نمی‌کند.

    گذار فعال بر اساس رکورد تازه در تراکنش است؛ status/SL/closed_at با
    هم ثبت می‌شوند. خطای SQLite در یک گذار فعال، بقیه را متوقف نمی‌کند.
    اعلان بعد از commit است و تحویل یا ارسال مجدد آن تضمین نمی‌شود.
    محدودیت دسترسی، اعلان را می‌بندد نه پیشرفت سوابق پایش را؛ دسترسی
    بعد از ثبت گذار و درست پیش از ارسال دوباره بررسی می‌شود.
    هر نوبت فقط یک دسته‌ی چرخشی رزرو می‌شود؛ نشانگر پیش از شبکه ثبت
    می‌شود و موفقیت دریافت قیمت را اثبات نمی‌کند. سقف چرخه ثابت است.
    """
    from config import MAX_OPEN_SIGNALS_PER_CHECK, BREAKEVEN_AFTER_TP1

    logger.info("شروع پایش عملکرد سیگنال‌های باز...")
    try:
        open_signals = await db.claim_signal_performance_batch(limit=MAX_OPEN_SIGNALS_PER_CHECK)
    except (aiosqlite.Error, ValueError):
        logger.exception("انتخاب دسته‌ی پایش ناموفق بود؛ این نوبت بدون دریافت قیمت متوقف می‌شود.")
        return
    if not open_signals:
        return

    symbols = {s["symbol"] for s in open_signals}
    client = ExchangeClient()
    prices = {}
    try:
        for symbol in symbols:
            try:
                prices[symbol] = await client.fetch_ticker_price(symbol)
            except Exception as e:
                logger.warning(f"دریافت قیمت {symbol} ناموفق بود: {e}")
    finally:
        await client.close()

    for sig in open_signals:
        price = prices.get(sig["symbol"])
        if not db.is_valid_price(price):
            continue

        if sig["status"] == "WAITING_ENTRY":
            if await db.invalidate_waiting_entry(sig["id"], price):
                entry_note = (
                    "سیگنال پیش از ورود فرضی باطل شد؛ قیمت مشاهده‌شده به SL رسیده یا از آن عبور کرده است.\n"
                    "این وضعیت برد یا باخت معامله نیست؛ مسیر قیمت بین دو مشاهده معلوم نیست."
                )
            elif await db.activate_signal_entry(sig["id"], price):
                entry_note = (
                    "ورود فرضی در مدل پایش فعال شد؛ این پیام تأیید اجرای سفارش واقعی نیست.\n"
                    "Entry برنامه‌ریزی‌شده تغییر نکرده و قیمت مشاهده‌شده جدا ثبت شده است."
                )
            else:
                continue
            if not await _can_send_background(sig["user_id"], "performance"):
                continue
            try:
                await app.bot.send_message(
                    chat_id=sig["user_id"],
                    text=(
                        f"📌 *پایش سیگنال {sig['symbol']}* ({sig['timeframe']})\n"
                        f"{entry_note}\n"
                        f"قیمت مشاهده‌شده: `{price:,.4f}`\n"
                        f"Entry برنامه‌ریزی‌شده: `{sig['entry']:,.4f}`\n\n"
                        "برای مشاهده وضعیت: /mysignals و /mystats"
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.warning(f"ارسال وضعیت ورود به {sig['user_id']} ناموفق بود: {e}")
            continue  # TP/SL فقط در مشاهده‌های بعد از فعال‌سازی پایش می‌شوند.

        try:
            committed = await db.advance_signal_performance(
                sig["id"], price, BREAKEVEN_AFTER_TP1
            )
        except aiosqlite.Error:
            logger.exception("ثبت گذار پایش سیگنال %s ناموفق بود؛ سایر سیگنال‌ها ادامه دارند.", sig["id"])
            continue
        if committed is None:
            continue
        sig = committed
        candidate_status = sig["status"]

        breakeven_note = ""
        if sig["breakeven_changed"]:
            breakeven_note = "\n\n🛡 حد ضرر ثبت‌شده در پایش به نقطه‌ی سربه‌سر (Break-even) منتقل شد؛ این تغییر، سفارش واقعی صرافی را تغییر نمی‌دهد."

        emoji = {"TP1_HIT": "🎯", "TP2_HIT": "🎯", "TP3_HIT": "🎯",
                 "SL_HIT": "🛑", "BREAKEVEN_HIT": "🛡"}[candidate_status]
        status_fa = {
            "TP1_HIT": "به TP1 رسید", "TP2_HIT": "به TP2 رسید",
            "TP3_HIT": "به TP3 رسید (سیگنال بسته شد ✅)",
            "SL_HIT": "به حد ضرر خورد (سیگنال بسته شد ❌)",
            "BREAKEVEN_HIT": "به حد ضرر سربه‌سرِ مدل پایش برگشت (بسته شد ⚪️؛ کارمزد/لغزش محاسبه نشده)",
        }[candidate_status]
        if not await _can_send_background(sig["user_id"], "performance"):
            continue
        try:
            await app.bot.send_message(
                chat_id=sig["user_id"],
                text=(
                    f"{emoji} *بروزرسانی سیگنال {sig['symbol']}* ({sig['timeframe']})\n"
                    f"وضعیت: {status_fa}\n"
                    f"قیمت فعلی: `{price:,.4f}`\n"
                    f"ورود: `{sig['entry']:,.4f}`"
                    f"{breakeven_note}\n\n"
                    f"برای دیدن آمار کلی: /mystats"
                ),
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.warning(f"ارسال بروزرسانی عملکرد به {sig['user_id']} ناموفق بود: {e}")

    logger.info("پایش عملکرد سیگنال‌ها تمام شد.")


async def news_auto_job(app: Application):
    """اگه خبر خیلی مهم (تاثیر >=80%) اومده، برای کاربرانی که ارسال خودکار رو روشن کردن می‌فرسته"""
    try:
        import news as news_module
        user_ids = await db.get_users_with_news_auto()
        allowed = await _background_recipients(user_ids, "news")
        user_ids = [uid for uid in user_ids if uid in allowed]
        if not user_ids:
            return
        items = await news_module.get_combined_news(crypto_limit=6, forex_limit=4)
        hot = [n for n in items if n["impact"] >= 80]
        if not hot:
            return
        # کش خبر به‌تنهایی مانع ارسال تکراری نیست؛ dedup مرحله‌ی جداگانه است.
        text = news_module.format_news_message(hot[:3], max_items=3)
        header = "🚨 *خبر فوری بازار (تاثیر خیلی بالا)*\n\n"
        full = header + text
        for uid in user_ids:
            if not await _can_send_background(uid, "news"):
                continue
            try:
                await app.bot.send_message(chat_id=uid, text=full, parse_mode="Markdown", disable_web_page_preview=True)
            except Exception as e:
                logger.warning(f"ارسال خبر خودکار به {uid} ناموفق: {e}")
    except Exception as e:
        logger.warning(f"news_auto_job failed: {e}")


async def backup_job(app: Application):
    """
    بکاپ خودکار دوره‌ای دیتابیس. اگه موفق بود چیزی به کسی اطلاع
    نمی‌ده (تا اسپم نشه)؛ ولی اگه شکست بخوره، به همه‌ی ادمین‌ها خبر
    می‌ده - چون شکست مکرر بکاپ یعنی موقع نیاز واقعی (خرابی دیسک، حذف
    اشتباهی و...) داده‌ای برای بازگردانی وجود نداره.
    """
    from config import ADMIN_IDS

    try:
        path = await backup.create_backup()
        logger.info(f"بکاپ خودکار دوره‌ای موفق: {path}")
    except Exception as e:
        logger.error(f"بکاپ خودکار دوره‌ای ناموفق بود: {e}")
        for admin_id in ADMIN_IDS:
            if not await _can_send_background(admin_id, "backup"):
                continue
            try:
                await app.bot.send_message(
                    chat_id=admin_id,
                    text=f"⚠️ بکاپ خودکار دیتابیس ناموفق بود:\n`{e}`\n\nلطفاً وضعیت دیسک/فضای ذخیره‌سازی رو چک کن.",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass
