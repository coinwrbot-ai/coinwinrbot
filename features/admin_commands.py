"""
Admin Commands Integration for CoinWinRBot
Complete admin system with payment verification - CIRCULAR IMPORT FIXED
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode
from telegram.ext import CommandHandler, CallbackQueryHandler
from telegram.ext import MessageHandler, filters

from features.config import Config
from database_factory import db

logger = logging.getLogger(__name__)

# ============ HTML ESCAPE UTILITY ============
def html_escape(text: str) -> str:
    """Escape HTML entities - defined locally to avoid circular import"""
    if not text:
        return ""
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

# Admin user IDs - CONFIGURE THESE
ADMIN_USER_IDS = [
    1925544390,  # Replace with your Telegram user ID
    # Add more admin IDs here
]

# Conversation states for admin flows
AWAITING_USER_ID, AWAITING_TIER, AWAITING_DURATION = range(3)
AWAITING_BROADCAST_MESSAGE = 100
AWAITING_BAN_REASON = 200

# Global reference to admin_manager (set during initialization)
admin_manager = None


# ============ UTILITY FUNCTIONS ============

def html_escape(text: str) -> str:
    """Escape HTML entities"""
    if not text:
        return ""
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


# ============ ADMIN DECORATORS ============

def admin_only(func):
    """Decorator to restrict commands to admins only"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        # Check if user is admin
        if user_id not in ADMIN_USER_IDS and not db.is_admin(user_id):
            await update.message.reply_text(
                "⛔ <b>Access Denied</b>\n\n"
                "This command is only available to administrators.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Log admin access
        db.log_admin_action(
            admin_id=user_id,
            action_type='command_access',
            details=f"Command: {func.__name__}"
        )
        
        return await func(update, context)
    
    return wrapper


# ============ ADMIN PANEL ============

@admin_only
async def admin_panel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Enhanced admin panel with real database statistics
    """
    user_id = update.effective_user.id
    
    # Check admin permission
    if user_id not in ADMIN_USER_IDS:
        await update.message.reply_text(
            "🔒 <b>Access Denied</b>\n\n"
            "This command is only available to administrators.",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Show loading message
    loading_msg = await update.message.reply_text(
        "📊 <b>Loading Admin Panel...</b>\n\n"
        "⏳ Fetching statistics from database...",
        parse_mode=ParseMode.HTML
    )
    
    try:
        # Fetch all statistics from database
        stats = db.get_admin_dashboard_stats()
        
        revenue = stats['revenue']
        tier_counts = stats['users_by_tier']
        
        # Build admin panel message
        admin_panel = f"""🔧 <b>ADMIN PANEL</b>

📊 <b>System Statistics:</b>
- Total Users: <b>{stats['total_users']:,}</b>
- Active (7d): <b>{stats['active_7d']:,}</b>
- Today's Analyses: <b>{stats['analyses_today']:,}</b>
- New Users Today: <b>{stats['new_users_today']:,}</b>

💰 <b>Revenue:</b>
- MRR: <b>${revenue['mrr']:,.2f}</b>
- This Month: <b>${revenue['this_month']:,.2f}</b>
- All Time: <b>${revenue['all_time']:,.2f}</b>
- Active Subs: <b>{stats['active_subs']}</b>

👥 <b>Users by Tier:</b>
- Free: <b>{tier_counts['free']:,}</b>
- Basic Weekly: <b>{tier_counts['basic_weekly']:,}</b>
- Basic Monthly: <b>{tier_counts['basic_monthly']:,}</b>
- Premium Weekly: <b>{tier_counts['premium_weekly']:,}</b>
- Premium Monthly: <b>{tier_counts['premium_monthly']:,}</b>

⚡ <b>Quick Actions:</b>
Use the buttons below or commands:
/admin_user - Manage specific user
/admin_broadcast - Send announcement
/pending - View pending verifications
/admin_stats - Detailed statistics

<i>Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"""
        
        # Create action buttons
        keyboard = [
            [
                InlineKeyboardButton("📋 Pending", callback_data='admin_pending'),
                InlineKeyboardButton("🔄 Refresh", callback_data='admin_refresh')
            ],
            [
                InlineKeyboardButton("📊 Details", callback_data='admin_detailed_stats')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Update loading message with stats
        await loading_msg.edit_text(
            admin_panel,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"✅ Admin panel displayed for user {user_id}")
        
    except Exception as e:
        logger.error(f"❌ Admin panel error: {e}", exc_info=True)
        await loading_msg.edit_text(
            f"❌ <b>Error Loading Admin Panel</b>\n\n"
            f"Error: {html_escape(str(e)[:200])}\n\n"
            "Please check the logs.",
            parse_mode=ParseMode.HTML
        )


# ============ USER MANAGEMENT ============

@admin_only
async def admin_user_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get detailed user information"""
    
    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b> /admin_user &lt;user_id or @username&gt;\n\n"
            "<b>Examples:</b>\n"
            "• <code>/admin_user 123456789</code>\n"
            "• <code>/admin_user @johndoe</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Parse user identifier
    user_identifier = context.args[0]
    
    if user_identifier.startswith('@'):
        # Username lookup
        username = user_identifier[1:]
        user = db.get_user_by_username(username)
    else:
        # User ID lookup
        try:
            user_id = int(user_identifier)
            user = db.get_user(user_id)
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid user ID format. Must be a number.",
                parse_mode=ParseMode.HTML
            )
            return
    
    if not user:
        await update.message.reply_text(
            f"❌ User not found: {user_identifier}",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Format user info
    ban_status = "🔴 BANNED" if user['is_banned'] else "🟢 Active"
    ban_reason = f"\n• Ban Reason: {user['ban_reason']}" if user['is_banned'] else ""
    
    user_info = f"""👤 <b>USER DETAILS</b>

<b>Basic Info:</b>
• ID: <code>{user['user_id']}</code>
• Username: @{user['username'] or 'N/A'}
• Name: {user['first_name'] or 'N/A'}
• Status: {ban_status}{ban_reason}

<b>Account:</b>
• Created: {user['created_at']}
• Last Active: {user['last_active'] or 'Never'}
• Total Analyses: {user['total_analyses']:,}

<b>Subscription:</b>
• Tier: {user['tier'].upper()}
• Expires: {user['expires_at'] or 'Never'}

<b>Admin Actions:</b>
• /admin_grant {user['user_id']} - Grant subscription
• /admin_ban {user['user_id']} - Ban user
• /admin_unban {user['user_id']} - Unban user"""
    
    await update.message.reply_text(user_info, parse_mode=ParseMode.HTML)


@admin_only
async def admin_ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ban a user"""
    
    if len(context.args) < 1:
        await update.message.reply_text(
            "❌ <b>Usage:</b> /admin_ban &lt;user_id&gt; [reason]\n\n"
            "<b>Example:</b>\n"
            "<code>/admin_ban 123456789 Spam/abuse</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        user_id = int(context.args[0])
        reason = ' '.join(context.args[1:]) if len(context.args) > 1 else 'No reason provided'
        admin_id = update.effective_user.id
        
        # Check if user exists
        user = db.get_user(user_id)
        if not user:
            await update.message.reply_text(
                f"❌ User not found: {user_id}",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Prevent banning other admins
        if db.is_admin(user_id):
            await update.message.reply_text(
                "❌ Cannot ban another admin!",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Ban the user
        db.ban_user(user_id, reason, admin_id)
        
        # Log action
        db.log_admin_action(
            admin_id=admin_id,
            action_type='ban_user',
            target_user_id=user_id,
            details=f"Reason: {reason}"
        )
        
        await update.message.reply_text(
            f"✅ <b>User Banned</b>\n\n"
            f"• User ID: <code>{user_id}</code>\n"
            f"• Username: @{user.get('username', 'N/A')}\n"
            f"• Reason: {reason}\n\n"
            f"The user will no longer be able to use the bot.",
            parse_mode=ParseMode.HTML
        )
        
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid user ID format. Must be a number.",
            parse_mode=ParseMode.HTML
        )


@admin_only
async def admin_unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unban a user"""
    
    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b> /admin_unban &lt;user_id&gt;\n\n"
            "<b>Example:</b>\n"
            "<code>/admin_unban 123456789</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        user_id = int(context.args[0])
        admin_id = update.effective_user.id
        
        # Check if user exists
        user = db.get_user(user_id)
        if not user:
            await update.message.reply_text(
                f"❌ User not found: {user_id}",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Unban the user
        db.unban_user(user_id)
        
        # Log action
        db.log_admin_action(
            admin_id=admin_id,
            action_type='unban_user',
            target_user_id=user_id,
            details="User unbanned"
        )
        
        await update.message.reply_text(
            f"✅ <b>User Unbanned</b>\n\n"
            f"• User ID: <code>{user_id}</code>\n"
            f"• Username: @{user.get('username', 'N/A')}\n\n"
            f"The user can now use the bot again.",
            parse_mode=ParseMode.HTML
        )
        
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid user ID format. Must be a number.",
            parse_mode=ParseMode.HTML
        )


# ============ SUBSCRIPTION MANAGEMENT ============

@admin_only
async def admin_grant_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Grant subscription to a user"""
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ <b>Usage:</b> /admin_grant &lt;user_id&gt; &lt;tier&gt; [days]\n\n"
            "<b>Tiers:</b>\n"
            "• basic_weekly\n"
            "• basic_monthly\n"
            "• premium_weekly\n"
            "• premium_monthly\n\n"
            "<b>Examples:</b>\n"
            "<code>/admin_grant 123456789 premium_monthly</code>\n"
            "<code>/admin_grant 123456789 basic_weekly 7</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        user_id = int(context.args[0])
        tier = context.args[1].lower()
        
        # Validate tier
        valid_tiers = ['basic_weekly', 'basic_monthly', 'premium_weekly', 'premium_monthly']
        if tier not in valid_tiers:
            await update.message.reply_text(
                f"❌ Invalid tier: {tier}\n\n"
                f"Valid tiers: {', '.join(valid_tiers)}",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Get duration (default based on tier)
        default_durations = {
            'basic_weekly': 7,
            'basic_monthly': 30,
            'premium_weekly': 7,
            'premium_monthly': 30
        }
        
        duration = int(context.args[2]) if len(context.args) > 2 else default_durations[tier]
        
        # Check if user exists, create if not
        user = db.get_user(user_id)
        if not user:
            logger.info(f"🆕 User {user_id} not found, creating placeholder...")
            db.register_user(user_id, username=None, first_name=f"User_{user_id}")
            user = db.get_user(user_id)
        
        # Grant subscription
        success = db.update_subscription(user_id, tier, duration)
        
        if not success:
            await update.message.reply_text(
                f"❌ Failed to update subscription for user {user_id}.\n"
                "Please check the logs.",
                parse_mode=ParseMode.HTML
            )
            return
        
        admin_id = update.effective_user.id
        db.log_admin_action(
            admin_id=admin_id,
            action_type='grant_subscription',
            target_user_id=user_id,
            details=f"Granted {tier} for {duration} days"
        )
        
        expires_at = datetime.now() + timedelta(days=duration)
        
        await update.message.reply_text(
            f"✅ <b>Subscription Granted</b>\n\n"
            f"• User: <code>{user_id}</code>\n"
            f"• Username: @{user.get('username', 'N/A')}\n"
            f"• Tier: <b>{tier.upper()}</b>\n"
            f"• Duration: {duration} days\n"
            f"• Expires: {expires_at.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"{'⚠️ User not registered yet - will activate when they start the bot' if not user.get('username') else '✅ User will be notified'}",
            parse_mode=ParseMode.HTML
        )
        
        # Try to notify the user
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"🎉 <b>Congratulations!</b>\n\n"
                     f"You have been granted <b>{tier.upper()}</b> subscription!\n\n"
                     f"• Duration: {duration} days\n"
                     f"• Expires: {expires_at.strftime('%Y-%m-%d')}\n\n"
                     f"Enjoy your premium features! 🚀",
                parse_mode=ParseMode.HTML
            )
            logger.info(f"✅ Notified user {user_id}")
        except Exception as e:
            logger.warning(f"⚠️ Could not notify user {user_id}: {e}")
        
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid format. User ID and days must be numbers.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"❌ Error granting subscription: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Error: {str(e)[:200]}",
            parse_mode=ParseMode.HTML
        )


# ============ PAYMENT VERIFICATION ============
# These functions delegate to admin_manager which is set during initialization

@admin_only
async def approve_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /approve command (admin only)"""
    global admin_manager
    if admin_manager is None:
        await update.message.reply_text(
            "❌ Admin manager not initialized. Please contact the developer.",
            parse_mode=ParseMode.HTML
        )
        return
    await admin_manager.handle_approve_payment(update, context)


@admin_only
async def reject_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reject command (admin only)"""
    global admin_manager
    if admin_manager is None:
        await update.message.reply_text(
            "❌ Admin manager not initialized. Please contact the developer.",
            parse_mode=ParseMode.HTML
        )
        return
    await admin_manager.handle_reject_payment(update, context)


@admin_only
async def pending_verifications(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /pending command (admin only)"""
    global admin_manager
    if admin_manager is None:
        await update.message.reply_text(
            "❌ Admin manager not initialized. Please contact the developer.",
            parse_mode=ParseMode.HTML
        )
        return
    await admin_manager.show_pending_verifications(update, context)


# ============ BROADCAST SYSTEM ============

@admin_only
async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send broadcast message to all users"""
    
    if not context.args:
        await update.message.reply_text(
            "📢 <b>Broadcast Message</b>\n\n"
            "<b>Usage:</b> /admin_broadcast [tier] &lt;message&gt;\n\n"
            "<b>Tiers (optional):</b>\n"
            "• all - All users (default)\n"
            "• free - Free users only\n"
            "• basic - Basic subscribers\n"
            "• premium - Premium subscribers\n\n"
            "<b>Examples:</b>\n"
            "<code>/admin_broadcast Hello everyone!</code>\n"
            "<code>/admin_broadcast premium New features!</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Parse tier filter
    tier_keywords = ['all', 'free', 'basic', 'premium']
    tier_filter = None
    message_start = 0
    
    if context.args[0].lower() in tier_keywords:
        tier_filter = context.args[0].lower()
        message_start = 1
    
    if len(context.args) <= message_start:
        await update.message.reply_text(
            "❌ Please provide a message to broadcast.",
            parse_mode=ParseMode.HTML
        )
        return
    
    message = ' '.join(context.args[message_start:])
    admin_id = update.effective_user.id
    
    # Get target users
    if tier_filter and tier_filter != 'all':
        if tier_filter == 'basic':
            users = db.get_all_user_ids('basic_weekly') + db.get_all_user_ids('basic_monthly')
        elif tier_filter == 'premium':
            users = db.get_all_user_ids('premium_weekly') + db.get_all_user_ids('premium_monthly')
        else:
            users = db.get_all_user_ids(tier_filter)
    else:
        users = db.get_all_user_ids()
    
    total_users = len(users)
    
    # Confirm broadcast
    confirm_text = f"""📢 <b>CONFIRM BROADCAST</b>

<b>Target:</b> {tier_filter or 'all'} users
<b>Recipients:</b> {total_users:,}

<b>Message:</b>
{message}

<b>⚠️ This will send the message to {total_users:,} users!</b>

Reply with /confirm_broadcast to proceed
or /cancel_broadcast to cancel."""
    
    # Store broadcast data in context
    context.user_data['pending_broadcast'] = {
        'message': message,
        'tier': tier_filter,
        'users': users,
        'admin_id': admin_id
    }
    
    await update.message.reply_text(confirm_text, parse_mode=ParseMode.HTML)


@admin_only
async def confirm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Confirm and execute broadcast"""
    
    broadcast_data = context.user_data.get('pending_broadcast')
    
    if not broadcast_data:
        await update.message.reply_text(
            "❌ No pending broadcast. Use /admin_broadcast first.",
            parse_mode=ParseMode.HTML
        )
        return
    
    message = broadcast_data['message']
    users = broadcast_data['users']
    tier = broadcast_data['tier']
    admin_id = broadcast_data['admin_id']
    
    # Log broadcast
    broadcast_id = db.log_broadcast(message, admin_id, tier, len(users))
    
    # Send progress message
    progress_msg = await update.message.reply_text(
        f"📤 <b>Broadcasting...</b>\n\n"
        f"Sending to {len(users):,} users...\n"
        f"Progress: 0/{len(users)}",
        parse_mode=ParseMode.HTML
    )
    
    # Send messages
    successful = 0
    failed = 0
    
    for i, user_id in enumerate(users):
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"📢 <b>Announcement</b>\n\n{message}",
                parse_mode=ParseMode.HTML
            )
            successful += 1
            
            # Update progress every 10 users
            if (i + 1) % 10 == 0 or i == len(users) - 1:
                await progress_msg.edit_text(
                    f"📤 <b>Broadcasting...</b>\n\n"
                    f"Sending to {len(users):,} users...\n"
                    f"Progress: {i + 1}/{len(users)}\n"
                    f"✅ Sent: {successful}\n"
                    f"❌ Failed: {failed}",
                    parse_mode=ParseMode.HTML
                )
            
            # Rate limiting
            await asyncio.sleep(0.05)
            
        except Exception as e:
            failed += 1
            logger.warning(f"Failed to send to {user_id}: {e}")
    
    # Update broadcast stats
    db.update_broadcast_stats(broadcast_id, successful, failed)
    
    # Log admin action
    db.log_admin_action(
        admin_id=admin_id,
        action_type='broadcast',
        details=f"Sent to {successful}/{len(users)} users"
    )
    
    # Clear pending broadcast
    context.user_data.pop('pending_broadcast', None)
    
    # Final report
    await progress_msg.edit_text(
        f"✅ <b>Broadcast Complete</b>\n\n"
        f"• Target: {tier or 'all'} users\n"
        f"• Total: {len(users):,}\n"
        f"• Successful: {successful:,}\n"
        f"• Failed: {failed:,}\n"
        f"• Success Rate: {(successful/len(users)*100):.1f}%",
        parse_mode=ParseMode.HTML
    )


@admin_only
async def cancel_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel pending broadcast"""
    
    if 'pending_broadcast' in context.user_data:
        context.user_data.pop('pending_broadcast')
        await update.message.reply_text(
            "✅ Broadcast cancelled.",
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text(
            "❌ No pending broadcast to cancel.",
            parse_mode=ParseMode.HTML
        )


# ============ STATISTICS ============

@admin_only
async def admin_detailed_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show detailed system statistics"""
    
    # Get all stats
    total_users = db.get_total_users()
    active_7d = db.get_active_users_7d()
    active_30d = db.get_active_users(30)
    today_analyses = db.get_today_analyses()
    today_registrations = db.get_today_registrations()
    revenue = db.get_revenue_stats()
    tier_stats = db.get_users_by_tier()
    
    # Calculate growth rates
    yesterday_users = total_users - today_registrations
    growth_rate = (today_registrations / yesterday_users * 100) if yesterday_users > 0 else 0
    
    stats_text = f"""📊 <b>DETAILED STATISTICS</b>

<b>👥 User Metrics:</b>
• Total Users: {total_users:,}
• Active (7d): {active_7d:,} ({(active_7d/total_users*100 if total_users > 0 else 0):.1f}%)
• Active (30d): {active_30d:,} ({(active_30d/total_users*100 if total_users > 0 else 0):.1f}%)
• New Today: {today_registrations:,}
• Growth Rate: {growth_rate:.2f}%/day

<b>📈 Activity:</b>
• Analyses Today: {today_analyses:,}
• Avg per Active User: {today_analyses/active_7d if active_7d > 0 else 0:.1f}

<b>💎 Subscription Breakdown:</b>
• Free: {tier_stats.get('free', 0):,} ({tier_stats.get('free', 0)/total_users*100:.1f}%)
• Basic Weekly: {tier_stats.get('basic_weekly', 0):,}
• Basic Monthly: {tier_stats.get('basic_monthly', 0):,}
• Premium Weekly: {tier_stats.get('premium_weekly', 0):,}
• Premium Monthly: {tier_stats.get('premium_monthly', 0):,}

<b>💰 Revenue Analysis:</b>
• MRR: ${revenue['mrr']:,.2f}
• ARR (projected): ${revenue['mrr'] * 12:,.2f}
• This Month: ${revenue['month_revenue']:,.2f}
• All Time: ${revenue['total_revenue']:,.2f}
• Active Subs: {revenue['active_subscriptions']}
• Conversion Rate: {(revenue['active_subscriptions']/total_users*100):.2f}%

<b>📊 ARPU Metrics:</b>
• ARPU: ${revenue['mrr']/total_users if total_users > 0 else 0:.2f}
• ARPPU: ${revenue['mrr']/revenue['active_subscriptions'] if revenue['active_subscriptions'] > 0 else 0:.2f}"""
    
    await update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)


# ============ CALLBACK HANDLERS ============

async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle admin panel button callbacks"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    # Check admin permission
    if user_id not in ADMIN_USER_IDS and not db.is_admin(user_id):
        await query.message.reply_text(
            "⛔ Access Denied",
            parse_mode=ParseMode.HTML
        )
        return
    
    if query.data == 'admin_refresh':
        # Refresh admin panel
        fake_update = Update(update_id=update.update_id, message=query.message)
        await admin_panel_command(fake_update, context)
    
    elif query.data == 'admin_detailed_stats':
        fake_update = Update(update_id=update.update_id, message=query.message)
        await admin_detailed_stats(fake_update, context)
    
    elif query.data == 'admin_pending':
        fake_update = Update(update_id=update.update_id, message=query.message)
        await pending_verifications(fake_update, context)


# ============ REGISTER HANDLERS ============

def set_admin_manager(admin_mgr):
    """Set the admin manager instance (call this before register_admin_handlers)"""
    global admin_manager
    admin_manager = admin_mgr
    logger.info(f"✅ Admin manager set: {admin_mgr}")


def register_admin_handlers(application, database):
    """
    Register all admin command handlers
    
    Args:
        application: The telegram application
        database: Database instance
    
    Note: Call set_admin_manager() before this function to enable payment verification
    """
    
    # Main admin panel
    application.add_handler(CommandHandler("admin", admin_panel_command))
    application.add_handler(CommandHandler("admin_panel", admin_panel_command))
    
    # User management
    application.add_handler(CommandHandler("admin_user", admin_user_info))
    application.add_handler(CommandHandler("admin_ban", admin_ban_user))
    application.add_handler(CommandHandler("admin_unban", admin_unban_user))
    
    # Subscription management
    application.add_handler(CommandHandler("admin_grant", admin_grant_subscription))
    
    # Payment verification
    application.add_handler(CommandHandler("approve", approve_payment))
    application.add_handler(CommandHandler("reject", reject_payment))
    application.add_handler(CommandHandler("pending", pending_verifications))
    
    # Broadcasting - FIXED: Added missing command name
    application.add_handler(CommandHandler("admin_broadcast", admin_broadcast))
    application.add_handler(CommandHandler("confirm_broadcast", confirm_broadcast))
    application.add_handler(CommandHandler("cancel_broadcast", cancel_broadcast))
    
    # Statistics
    application.add_handler(CommandHandler("admin_stats", admin_detailed_stats))
    
    # Callback handlers
    application.add_handler(CallbackQueryHandler(
        admin_callback_handler, 
        pattern='^admin_(?!approve_|reject_)'
    ))
    
    logger.info("✅ Admin handlers registered successfully")