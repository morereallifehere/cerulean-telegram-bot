import sqlite3
import csv
import logging
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)
from telegram.request import HTTPXRequest

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# === CERULEAN LABS BOT CONFIG ===
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [6454727490]  # <-- Replace with your Telegram user ID from @userinfobot
X_LINK = "https://x.com/ceruleanlabs"
TELEGRAM_LINK = "https://t.me/ceruleanlabsgroupchat"
GROUP_CHAT_ID = -1002664797681  # <-- Add your group chat ID here (e.g., -1001234567890) - Optional

POINTS_PER_REFERRAL = 1
ENGAGEMENT_POINTS_PER_MESSAGE = 1

# --- Database Setup ---
def init_database():
    """Initialize database with proper error handling"""
    try:
        db_path = Path.home() / "referrals.db"
        conn = sqlite3.connect(str(db_path), check_same_thread=False, timeout=30.0)
        cur = conn.cursor()
        
        # Ambassadors table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ambassadors (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            points INTEGER DEFAULT 0
        )
        """)
        
        # Users table (for ambassador referrals)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            referrer INTEGER,
            status TEXT DEFAULT 'pending'
        )
        """)
        
        # Regular referrals table (separate from ambassador program)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS referrals (
            user_id INTEGER PRIMARY KEY,
            referrer_id INTEGER,
            username TEXT,
            status TEXT DEFAULT 'pending',
            completed_at TIMESTAMP,
            period TEXT
        )
        """)
        
        # Engagement tracking table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS engagement (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            message_count INTEGER DEFAULT 0,
            last_message_at TIMESTAMP,
            period TEXT,
            UNIQUE(user_id, period)
        )
        """)
        
        # Weekly/Monthly winners table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS winners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT,
            period TEXT,
            user_id INTEGER,
            username TEXT,
            count INTEGER,
            reward TEXT,
            awarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Add missing columns to existing tables
        try:
            cur.execute("ALTER TABLE users ADD COLUMN joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            logger.info("Added joined_at column to users table")
        except sqlite3.OperationalError:
            pass
        
        try:
            cur.execute("ALTER TABLE ambassadors ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            logger.info("Added created_at column to ambassadors table")
        except sqlite3.OperationalError:
            pass
        
        conn.commit()
        logger.info("Database initialized successfully")
        return conn, cur
        
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise

# Initialize database
conn, cur = init_database()

def get_current_period():
    """Get current week/month identifier"""
    now = datetime.now()
    week = f"{now.year}-W{now.isocalendar()[1]:02d}"
    month = f"{now.year}-M{now.month:02d}"
    return week, month

# --- AMBASSADOR PROGRAM HANDLERS ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    try:
        user_id = update.effective_user.id
        username = update.effective_user.username or f"User{user_id}"
        args = context.args

        # Case 1: Ambassador referral link
        if args and args[0].startswith("amb_"):
            referrer_id = int(args[0].replace("amb_", ""))
            cur.execute("SELECT username FROM ambassadors WHERE user_id=?", (referrer_id,))
            referrer_info = cur.fetchone()
            
            if not referrer_info:
                await update.message.reply_text("❌ Invalid ambassador referral link.")
                return

            if referrer_id == user_id:
                await update.message.reply_text("⚠️ You cannot use your own referral link!")
                return

            cur.execute("SELECT referrer, status FROM users WHERE user_id=?", (user_id,))
            existing = cur.fetchone()

            if existing:
                if existing[1] == "completed":
                    await update.message.reply_text("✅ You've already completed ambassador tasks!")
                else:
                    await show_ambassador_tasks(update, context, referrer_id, referrer_info[0])
                return

            cur.execute("INSERT INTO users (user_id, referrer, status) VALUES (?, ?, ?)",
                        (user_id, referrer_id, "pending"))
            conn.commit()
            logger.info(f"New user {user_id} referred by ambassador {referrer_id}")
            await show_ambassador_tasks(update, context, referrer_id, referrer_info[0])

        # Case 2: Regular referral link
        elif args and args[0].startswith("ref_"):
            referrer_id = int(args[0].replace("ref_", ""))
            
            if referrer_id == user_id:
                await update.message.reply_text("⚠️ You cannot use your own referral link!")
                return

            cur.execute("SELECT username FROM referrals WHERE user_id=?", (referrer_id,))
            referrer_info = cur.fetchone()
            referrer_username = referrer_info[0] if referrer_info else "Unknown"

            week, month = get_current_period()
            cur.execute("SELECT status FROM referrals WHERE user_id=? AND period=?", (user_id, month))
            existing = cur.fetchone()

            if existing:
                if existing[0] == "completed":
                    await update.message.reply_text("✅ You've already completed referral tasks this month!")
                else:
                    await show_referral_tasks(update, context, referrer_id, referrer_username)
                return

            cur.execute("""
                INSERT OR REPLACE INTO referrals (user_id, referrer_id, username, status, period) 
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, referrer_id, username, "pending", month))
            conn.commit()
            logger.info(f"New referral user {user_id} referred by {referrer_id}")
            await show_referral_tasks(update, context, referrer_id, referrer_username)

        # Case 3: No referral link
        else:
            keyboard = [
                [InlineKeyboardButton("👑 Become Ambassador", callback_data="become_amb")],
                [InlineKeyboardButton("🔗 Get Referral Link", callback_data="get_ref")],
                [InlineKeyboardButton("📊 My Stats", callback_data="my_stats")],
                [InlineKeyboardButton("🏆 Leaderboards", callback_data="leaderboards")]
            ]
            await update.message.reply_text(
                "🌟 **Welcome to Cerulean Labs!**\n\n"
                "Choose an option below:\n\n"
                "👑 **Ambassador Program**: Earn points for referrals\n"
                "🎯 **Referral Contest**: Win weekly/monthly rewards\n"
                "💬 **Engagement Rewards**: Active group members get rewarded\n\n"
                "Select an option to get started!",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )

    except Exception as e:
        logger.error(f"Error in start: {e}")
        try:
            await update.message.reply_text("❌ An error occurred. Please try again.")
        except:
            pass

async def show_ambassador_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE, referrer_id: int, referrer_username: str):
    """Show ambassador tasks"""
    keyboard = [
        [InlineKeyboardButton("📱 Join Telegram", url=TELEGRAM_LINK)],
        [InlineKeyboardButton("🐦 Follow on X", url=X_LINK)],
        [InlineKeyboardButton("✅ Done!", callback_data=f"amb_done_{referrer_id}")]
    ]
    await update.message.reply_text(
        f"👋 Welcome! You've been invited by @{referrer_username} (Ambassador)!\n\n"
        f"🎯 Complete these tasks:\n"
        f"1️⃣ Join our Telegram channel\n"
        f"2️⃣ Follow us on X\n\n"
        f"Click ✅ when done!",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_referral_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE, referrer_id: int, referrer_username: str):
    """Show referral contest tasks"""
    keyboard = [
        [InlineKeyboardButton("📱 Join Telegram", url=TELEGRAM_LINK)],
        [InlineKeyboardButton("🐦 Follow on X", url=X_LINK)],
        [InlineKeyboardButton("✅ Done!", callback_data=f"ref_done_{referrer_id}")]
    ]
    await update.message.reply_text(
        f"🎁 You've been invited by @{referrer_username}!\n\n"
        f"🎯 Complete these tasks to help them win:\n"
        f"1️⃣ Join our Telegram channel\n"
        f"2️⃣ Follow us on X\n\n"
        f"🏆 Top referrers win rewards weekly/monthly!\n\n"
        f"Click ✅ when done!",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def become_ambassador(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /become_ambassador command"""
    try:
        user = update.effective_user
        user_id = user.id
        username = user.username or f"id{user_id}"

        cur.execute("SELECT points FROM ambassadors WHERE user_id=?", (user_id,))
        existing = cur.fetchone()

        if existing:
            await update.message.reply_text(
                f"👑 You're already an ambassador!\nUse /stats to see your referral link."
            )
            return

        cur.execute("INSERT INTO ambassadors (user_id, username, points) VALUES (?, ?, ?)",
                    (user_id, username, 0))
        conn.commit()
        logger.info(f"New ambassador: {username} ({user_id})")

        try:
            bot_info = await context.bot.get_me()
            bot_username = bot_info.username
        except:
            bot_username = "ceruleanlabsbot"
            
        referral_link = f"https://t.me/{bot_username}?start=amb_{user_id}"

        await update.message.reply_text(
            f"🎉 **You're now a Cerulean Labs Ambassador!**\n\n"
            f"🔗 Your referral link:\n`{referral_link}`\n\n"
            f"📈 Earn {POINTS_PER_REFERRAL} point per completed referral!\n"
            f"Use /stats to track your progress!",
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Error in become_ambassador: {e}")
        try:
            await update.message.reply_text("❌ An error occurred. Please try again.")
        except:
            pass

async def get_referral_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get referral contest link"""
    try:
        # Handle both message and callback query
        if update.callback_query:
            user = update.callback_query.from_user
            send_method = update.callback_query.message.reply_text
        else:
            user = update.effective_user
            send_method = update.message.reply_text
            
        user_id = user.id
        username = user.username or f"User{user_id}"

        week, month = get_current_period()
        
        # Check if user already exists in referrals table
        cur.execute("SELECT user_id FROM referrals WHERE user_id=?", (user_id,))
        existing = cur.fetchone()
        
        if not existing:
            # Insert new user into referrals table
            cur.execute("""
                INSERT INTO referrals (user_id, username, status, period) 
                VALUES (?, ?, ?, ?)
            """, (user_id, username, "completed", month))
            conn.commit()
            logger.info(f"New referral link generated for user {user_id}")

        try:
            bot_info = await context.bot.get_me()
            bot_username = bot_info.username
        except:
            bot_username = "ceruleanlabsbot"
            
        referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"

        # Fixed message without problematic markdown
        message = (
            "🎁 Your Referral Contest Link\n\n"
            f"🔗 Link: {referral_link}\n\n"
            "🏆 This Month's Contest:\n"
            "Top referrers win exclusive rewards!\n\n"
            "💡 Share with friends and climb the leaderboard!\n"
            "Use /referral_leaderboard to see rankings!"
        )
        
        await send_method(message)

    except Exception as e:
        logger.error(f"Error in get_referral_link: {e}")
        try:
            if update.callback_query:
                await update.callback_query.message.reply_text("❌ An error occurred. Please try again.")
            else:
                await update.message.reply_text("❌ An error occurred. Please try again.")
        except:
            pass

# --- ENGAGEMENT TRACKING ---
async def track_engagement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Track user engagement in group chat"""
    try:
        # Only track group messages
        if update.message.chat.type not in ["group", "supergroup"]:
            return

        # Only track if it's the designated group (if set)
        if GROUP_CHAT_ID and update.message.chat.id != GROUP_CHAT_ID:
            return

        user_id = update.effective_user.id
        username = update.effective_user.username or f"User{user_id}"
        
        week, month = get_current_period()

        # Update weekly engagement
        cur.execute("""
            INSERT INTO engagement (user_id, username, message_count, last_message_at, period)
            VALUES (?, ?, 1, CURRENT_TIMESTAMP, ?)
            ON CONFLICT(user_id, period) DO UPDATE SET
                message_count = message_count + 1,
                last_message_at = CURRENT_TIMESTAMP,
                username = ?
        """, (user_id, username, week, username))
        
        conn.commit()

    except Exception as e:
        logger.error(f"Error tracking engagement: {e}")

# --- BUTTON HANDLERS ---
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks"""
    try:
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        data = query.data

        # Ambassador task completion
        if data.startswith("amb_done_"):
            referrer_id = int(data.replace("amb_done_", ""))
            cur.execute("SELECT status FROM users WHERE user_id=? AND referrer=?", (user_id, referrer_id))
            record = cur.fetchone()

            if record and record[0] != "completed":
                cur.execute("UPDATE users SET status=? WHERE user_id=?", ("completed", user_id))
                cur.execute("UPDATE ambassadors SET points = points + ? WHERE user_id=?", 
                           (POINTS_PER_REFERRAL, referrer_id))
                conn.commit()
                logger.info(f"Ambassador referral completed: {user_id} -> {referrer_id}")
                await query.edit_message_text(
                    "🎉 Tasks completed! Thank you for joining!\n"
                    "🚀 Want your own referral link? Send /get_referral_link"
                )
            else:
                await query.edit_message_text("✅ Already completed!")

        # Referral task completion
        elif data.startswith("ref_done_"):
            referrer_id = int(data.replace("ref_done_", ""))
            week, month = get_current_period()
            
            cur.execute("SELECT status FROM referrals WHERE user_id=? AND period=?", (user_id, month))
            record = cur.fetchone()

            if record and record[0] != "completed":
                cur.execute("""
                    UPDATE referrals SET status=?, completed_at=CURRENT_TIMESTAMP 
                    WHERE user_id=? AND period=?
                """, ("completed", user_id, month))
                conn.commit()
                logger.info(f"Referral completed: {user_id} -> {referrer_id}")
                await query.edit_message_text(
                    "🎉 Tasks completed! Your referrer gets credit!\n"
                    "🏆 Want to compete? Send /get_referral_link"
                )
            else:
                await query.edit_message_text("✅ Already completed!")

        # Menu buttons
        elif data == "become_amb":
            await become_ambassador(update, context)
        elif data == "get_ref":
            await get_referral_link(update, context)
        elif data == "my_stats":
            await my_stats(update, context)
        elif data == "leaderboards":
            await show_all_leaderboards(update, context)

    except Exception as e:
        logger.error(f"Error in button: {e}")
        try:
            await query.edit_message_text("❌ An error occurred. Please try again.")
        except:
            pass

# --- STATS & LEADERBOARDS ---
async def my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show comprehensive user stats"""
    try:
        # Handle both message and callback query
        if update.callback_query:
            user_id = update.callback_query.from_user.id
            send_method = update.callback_query.message.reply_text
        else:
            user_id = update.effective_user.id
            send_method = update.message.reply_text

        stats_text = "📊 **Your Statistics**\n\n"

        # Ambassador stats
        cur.execute("SELECT username, points FROM ambassadors WHERE user_id=?", (user_id,))
        amb = cur.fetchone()
        if amb:
            cur.execute("SELECT COUNT(*) FROM users WHERE referrer=? AND status='completed'", (user_id,))
            amb_refs = cur.fetchone()[0]
            
            try:
                bot_info = await context.bot.get_me()
                bot_username = bot_info.username
            except:
                bot_username = "ceruleanlabsbot"
            
            amb_link = f"https://t.me/{bot_username}?start=amb_{user_id}"
            stats_text += f"👑 **Ambassador Program**\n"
            stats_text += f"⭐ Points: {amb[1]}\n"
            stats_text += f"🎯 Referrals: {amb_refs}\n"
            stats_text += f"🔗 Link: `{amb_link}`\n\n"

        # Referral contest stats
        week, month = get_current_period()
        cur.execute("""
            SELECT COUNT(*) FROM referrals 
            WHERE referrer_id=? AND status='completed' AND period=?
        """, (user_id, month))
        ref_count = cur.fetchone()[0]
        
        if ref_count > 0 or not amb:
            try:
                bot_info = await context.bot.get_me()
                bot_username = bot_info.username
            except:
                bot_username = "ceruleanlabsbot"
            
            ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
            stats_text += f"🎁 **Referral Contest (This Month)**\n"
            stats_text += f"👥 Referrals: {ref_count}\n"
            stats_text += f"🔗 Link: `{ref_link}`\n\n"

        # Engagement stats
        cur.execute("SELECT message_count FROM engagement WHERE user_id=? AND period=?", (user_id, week))
        eng = cur.fetchone()
        if eng and eng[0] > 0:
            stats_text += f"💬 **Group Engagement (This Week)**\n"
            stats_text += f"📨 Messages: {eng[0]}\n\n"

        if len(stats_text) == len("📊 **Your Statistics**\n\n"):
            stats_text += "ℹ️ No activity yet. Get started with /start!"

        await send_method(stats_text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error in my_stats: {e}")
        try:
            if update.callback_query:
                await update.callback_query.message.reply_text("❌ Error loading stats.")
            else:
                await update.message.reply_text("❌ Error loading stats.")
        except:
            pass

async def show_all_leaderboards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show all leaderboard options"""
    keyboard = [
        [InlineKeyboardButton("👑 Ambassador Leaderboard", callback_data="lb_amb")],
        [InlineKeyboardButton("🎁 Referral Contest (Monthly)", callback_data="lb_ref")],
        [InlineKeyboardButton("💬 Engagement (Weekly)", callback_data="lb_eng")]
    ]
    
    text = "🏆 Choose a Leaderboard:"
    
    if update.callback_query:
        await update.callback_query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def ambassador_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show ambassador leaderboard"""
    try:
        cur.execute("SELECT username, points FROM ambassadors ORDER BY points DESC LIMIT 10")
        top = cur.fetchall()

        if not top:
            text = "🏆 No ambassadors yet!"
        else:
            text = "👑 Ambassador Leaderboard\n\n"
            for i, (username, points) in enumerate(top, 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                text += f"{medal} @{username} - {points} pts\n"

        if update.callback_query:
            await update.callback_query.message.reply_text(text)
        else:
            await update.message.reply_text(text)

    except Exception as e:
        logger.error(f"Error in ambassador_leaderboard: {e}")

async def referral_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show monthly referral contest leaderboard"""
    try:
        week, month = get_current_period()
        cur.execute("""
            SELECT r.username, COUNT(*) as ref_count
            FROM referrals ref
            JOIN referrals r ON ref.referrer_id = r.user_id
            WHERE ref.status='completed' AND ref.period=?
            GROUP BY ref.referrer_id
            ORDER BY ref_count DESC
            LIMIT 10
        """, (month,))
        top = cur.fetchall()

        if not top:
            text = f"🎁 Monthly Referral Contest ({month})\n\nNo referrals yet this month!"
        else:
            text = f"🎁 Monthly Referral Contest ({month})\n\n"
            for i, (username, count) in enumerate(top, 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                text += f"{medal} @{username} - {count} referrals\n"

        if update.callback_query:
            await update.callback_query.message.reply_text(text)
        else:
            await update.message.reply_text(text)

    except Exception as e:
        logger.error(f"Error in referral_leaderboard: {e}")

async def engagement_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show weekly engagement leaderboard"""
    try:
        week, month = get_current_period()
        cur.execute("""
            SELECT username, message_count 
            FROM engagement 
            WHERE period=? 
            ORDER BY message_count DESC 
            LIMIT 10
        """, (week,))
        top = cur.fetchall()

        if not top:
            text = f"💬 Weekly Engagement ({week})\n\nNo activity yet this week!"
        else:
            text = f"💬 Weekly Engagement ({week})\n\n"
            for i, (username, count) in enumerate(top, 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                text += f"{medal} @{username} - {count} messages\n"

        if update.callback_query:
            await update.callback_query.message.reply_text(text)
        else:
            await update.message.reply_text(text)

    except Exception as e:
        logger.error(f"Error in engagement_leaderboard: {e}")

# --- ADMIN COMMANDS ---
async def report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin report"""
    try:
        if update.effective_user.id not in ADMIN_IDS:
            return

        week, month = get_current_period()
        
        # Ambassador stats
        cur.execute("SELECT COUNT(*), SUM(points) FROM ambassadors")
        amb_data = cur.fetchone()
        
        # Referral contest stats
        cur.execute("SELECT COUNT(*) FROM referrals WHERE status='completed' AND period=?", (month,))
        ref_count = cur.fetchone()[0]
        
        # Engagement stats
        cur.execute("SELECT COUNT(*), SUM(message_count) FROM engagement WHERE period=?", (week,))
        eng_data = cur.fetchone()

        text = (
            f"📊 **Admin Report**\n\n"
            f"👑 **Ambassadors**: {amb_data[0] or 0} total ({amb_data[1] or 0} pts)\n"
            f"🎁 **Referrals (This Month)**: {ref_count}\n"
            f"💬 **Engagement (This Week)**: {eng_data[0] or 0} users ({eng_data[1] or 0} msgs)\n\n"
            f"📋 **Detailed Leaderboards:**\n"
            f"/ambassador_leaderboard\n"
            f"/referral_leaderboard\n"
            f"/engagement_leaderboard\n\n"
            f"📂 Use /export to download CSV data"
        )

        await update.message.reply_text(text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error in report: {e}")
        try:
            await update.message.reply_text("❌ Error generating report.")
        except:
            pass

async def export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export all data"""
    try:
        user_id = update.effective_user.id
        
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("❌ Admin only.")
            return

        await update.message.reply_text("📂 Preparing exports... This may take a moment.")

        home_dir = Path.home()
        timestamp = int(time.time())
        
        # Export ambassadors
        filename1 = home_dir / f"ambassadors_{timestamp}.csv"
        with open(filename1, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["User ID", "Username", "Points"])
            cur.execute("SELECT user_id, username, points FROM ambassadors ORDER BY points DESC")
            writer.writerows(cur.fetchall())
        
        with open(filename1, 'rb') as f:
            await update.message.reply_document(f, caption="📂 Ambassadors Data")
        
        # Export referrals
        week, month = get_current_period()
        filename2 = home_dir / f"referrals_{timestamp}.csv"
        with open(filename2, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["User ID", "Referrer ID", "Username", "Status", "Period", "Completed At"])
            cur.execute("SELECT user_id, referrer_id, username, status, period, completed_at FROM referrals ORDER BY period DESC, completed_at DESC")
            writer.writerows(cur.fetchall())
        
        with open(filename2, 'rb') as f:
            await update.message.reply_document(f, caption="📂 Referrals Data")
        
        # Export engagement
        filename3 = home_dir / f"engagement_{timestamp}.csv"
        with open(filename3, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["User ID", "Username", "Messages", "Period", "Last Message"])
            cur.execute("SELECT user_id, username, message_count, period, last_message_at FROM engagement ORDER BY period DESC, message_count DESC")
            writer.writerows(cur.fetchall())
        
        with open(filename3, 'rb') as f:
            await update.message.reply_document(f, caption="📂 Engagement Data")

        # Cleanup
        filename1.unlink(missing_ok=True)
        filename2.unlink(missing_ok=True)
        filename3.unlink(missing_ok=True)

        await update.message.reply_text("✅ Export complete!")

    except Exception as e:
        logger.error(f"Error in export: {e}")
        try:
            await update.message.reply_text("❌ Export failed. Please try again.")
        except:
            pass

async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset database (Admin only - use with caution)"""
    try:
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("❌ Admin only.")
            return

        # Ask for confirmation
        keyboard = [
            [InlineKeyboardButton("⚠️ Yes, Reset All Data", callback_data="confirm_reset")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_reset")]
        ]
        await update.message.reply_text(
            "⚠️ **WARNING: This will delete ALL data!**\n\n"
            "This includes:\n"
            "• All ambassadors and points\n"
            "• All referrals\n"
            "• All engagement data\n"
            "• All winners history\n\n"
            "Are you sure?",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Error in reset: {e}")

async def reset_weekly_engagement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset weekly engagement (Admin only)"""
    try:
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("❌ Admin only.")
            return

        week, month = get_current_period()
        
        # Archive current week's data before resetting
        cur.execute("""
            INSERT INTO winners (category, period, user_id, username, count, reward)
            SELECT 'engagement', period, user_id, username, message_count, 'Archived'
            FROM engagement
            WHERE period=?
        """, (week,))
        
        # Delete current week's engagement data
        cur.execute("DELETE FROM engagement WHERE period=?", (week,))
        conn.commit()
        
        await update.message.reply_text(
            f"✅ Weekly engagement reset for {week}!\n"
            f"Data has been archived to winners table."
        )
        logger.info(f"Weekly engagement reset by admin {update.effective_user.id}")

    except Exception as e:
        logger.error(f"Error in reset_weekly_engagement: {e}")
        try:
            await update.message.reply_text("❌ Error resetting weekly engagement.")
        except:
            pass

async def view_weekly_archives(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View archived weekly engagement winners (Admin only)"""
    try:
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("❌ Admin only.")
            return

        # Get all archived weeks
        cur.execute("""
            SELECT DISTINCT period 
            FROM winners 
            WHERE category='engagement' 
            ORDER BY period DESC
        """)
        weeks = cur.fetchall()

        if not weeks:
            await update.message.reply_text("📂 No archived weeks yet.")
            return

        # Show list of weeks as buttons
        keyboard = []
        for (week_period,) in weeks[:10]:  # Show last 10 weeks
            keyboard.append([InlineKeyboardButton(
                f"Week {week_period}", 
                callback_data=f"archive_{week_period}"
            )])

        await update.message.reply_text(
            "📂 Archived Weekly Engagement\n\n"
            "Select a week to view:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    except Exception as e:
        logger.error(f"Error in view_weekly_archives: {e}")
        try:
            await update.message.reply_text("❌ Error loading archives.")
        except:
            pass

async def show_archive_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show details of archived week"""
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id not in ADMIN_IDS:
        return
    
    try:
        week_period = query.data.replace("archive_", "")
        
        cur.execute("""
            SELECT username, count 
            FROM winners 
            WHERE category='engagement' AND period=?
            ORDER BY count DESC
            LIMIT 10
        """, (week_period,))
        
        top = cur.fetchall()
        
        if not top:
            text = f"📂 No data for {week_period}"
        else:
            text = f"📂 Archived: {week_period}\n\n"
            for i, (username, count) in enumerate(top, 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                text += f"{medal} @{username} - {count} messages\n"
        
        await query.edit_message_text(text)
        
    except Exception as e:
        logger.error(f"Error showing archive detail: {e}")

async def confirm_reset_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle reset confirmation"""
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id not in ADMIN_IDS:
        return
    
    if query.data == "confirm_reset":
        try:
            cur.execute("DELETE FROM users")
            cur.execute("DELETE FROM ambassadors")
            cur.execute("DELETE FROM referrals")
            cur.execute("DELETE FROM engagement")
            cur.execute("DELETE FROM winners")
            conn.commit()
            
            await query.edit_message_text("🗑 Database has been completely reset.")
            logger.warning(f"Database reset by admin {query.from_user.id}")
        except Exception as e:
            logger.error(f"Error resetting database: {e}")
            await query.edit_message_text("❌ Error resetting database.")
    else:
        await query.edit_message_text("✅ Reset cancelled. Data is safe.")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors caused by updates"""
    logger.error(f"Exception while handling update: {context.error}", exc_info=context.error)

def create_application():
    """Create application with custom request settings"""
    request = HTTPXRequest(
        connection_pool_size=8,
        connect_timeout=60.0,
        read_timeout=60.0,
        write_timeout=60.0,
        pool_timeout=60.0,
    )
    return Application.builder().token(TOKEN).request(request).build()

def main():
    """Main function with retry logic"""
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print("=" * 50)
            print("🤖 Cerulean Labs Bot Initializing...")
            print("=" * 50)
            
            # Create application
            app = create_application()
            app.add_error_handler(error_handler)

            # Command handlers
            app.add_handler(CommandHandler("start", start))
            app.add_handler(CommandHandler("become_ambassador", become_ambassador))
            app.add_handler(CommandHandler("get_referral_link", get_referral_link))
            app.add_handler(CommandHandler("stats", my_stats))
            app.add_handler(CommandHandler("leaderboards", show_all_leaderboards))
            app.add_handler(CommandHandler("ambassador_leaderboard", ambassador_leaderboard))
            app.add_handler(CommandHandler("referral_leaderboard", referral_leaderboard))
            app.add_handler(CommandHandler("engagement_leaderboard", engagement_leaderboard))
            app.add_handler(CommandHandler("report", report))
            app.add_handler(CommandHandler("export", export))
            app.add_handler(CommandHandler("reset", reset))
            app.add_handler(CommandHandler("reset_weekly", reset_weekly_engagement))
            app.add_handler(CommandHandler("weekly_archives", view_weekly_archives))

            # Callback query handlers
            app.add_handler(CallbackQueryHandler(button, pattern="^(amb_done_|ref_done_|become_amb|get_ref|my_stats|leaderboards)"))
            app.add_handler(CallbackQueryHandler(ambassador_leaderboard, pattern="^lb_amb$"))
            app.add_handler(CallbackQueryHandler(referral_leaderboard, pattern="^lb_ref$"))
            app.add_handler(CallbackQueryHandler(engagement_leaderboard, pattern="^lb_eng$"))
            app.add_handler(CallbackQueryHandler(confirm_reset_handler, pattern="^(confirm_reset|cancel_reset)$"))
            app.add_handler(CallbackQueryHandler(show_archive_detail, pattern="^archive_"))

            # Message handler for engagement tracking in groups
            app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, track_engagement))

            print("\n✅ Bot initialized successfully!")
            print("\n📊 Active Features:")
            print("  👑 Ambassador Program - Permanent point system")
            print("  🎁 Referral Contest - Monthly competitions")
            print("  💬 Engagement Tracking - Weekly activity rewards")
            if GROUP_CHAT_ID:
                print(f"  📍 Group Chat ID: {GROUP_CHAT_ID}")
            else:
                print("  ⚠️  Group Chat ID not set (engagement tracking disabled)")
            print("\n🚀 Bot is now running...")
            print("=" * 50)
            
            logger.info("Bot started successfully")
            
            # Run the bot
            app.run_polling(
                drop_pending_updates=True,
                timeout=30,
                bootstrap_retries=5
            )
            
            # If we reach here, bot stopped normally
            break
            
        except Exception as e:
            retry_count += 1
            logger.error(f"Bot startup failed (attempt {retry_count}/{max_retries}): {e}")
            print(f"\n❌ Bot startup failed (attempt {retry_count}/{max_retries})")
            print(f"Error: {e}")
            
            if retry_count < max_retries:
                print(f"🔄 Retrying in 10 seconds...\n")
                time.sleep(10)
            else:
                print("\n❌ Max retries reached.")
                print("Please check:")
                print("  1. Your bot token is correct")
                print("  2. Your internet connection is stable")
                print("  3. Your admin ID is set correctly")
                print("  4. All dependencies are installed")
                break


if __name__ == "__main__":
    main()
