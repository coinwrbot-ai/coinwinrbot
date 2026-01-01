"""
Simple admin_manager.py that creates a basic instance
This avoids circular imports by being minimal
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

logger = logging.getLogger(__name__)


class SimpleAdminManager:
    """Simple admin manager for payment verification"""
    
    def __init__(self, db, bot):
        self.db = db
        self.bot = bot
        logger.info("✅ SimpleAdminManager initialized")
    
    async def handle_approve_payment(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle payment approval"""
        if not context.args:
            await update.message.reply_text(
                "❌ <b>Usage:</b> /approve &lt;verification_id&gt;\n\n"
                "<b>Example:</b> <code>/approve 12345</code>",
                parse_mode=ParseMode.HTML
            )
            return
        
        try:
            verification_id = int(context.args[0])
            
            # Get verification from database
            verification = self.db.get_payment_verification(verification_id)
            
            if not verification:
                await update.message.reply_text(
                    f"❌ Verification not found: {verification_id}",
                    parse_mode=ParseMode.HTML
                )
                return
            
            if verification['status'] != 'pending':
                await update.message.reply_text(
                    f"❌ Verification already processed: {verification['status']}",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Approve payment
            user_id = verification['user_id']
            tier = verification['tier']
            duration = verification['duration']
            
            # Update subscription
            success = self.db.update_subscription(user_id, tier, duration)
            
            if success:
                # Mark as approved
                self.db.update_verification_status(verification_id, 'approved', update.effective_user.id)
                
                await update.message.reply_text(
                    f"✅ <b>Payment Approved</b>\n\n"
                    f"• User: <code>{user_id}</code>\n"
                    f"• Tier: {tier}\n"
                    f"• Duration: {duration} days",
                    parse_mode=ParseMode.HTML
                )
                
                # Notify user
                try:
                    await self.bot.send_message(
                        chat_id=user_id,
                        text=f"🎉 <b>Payment Approved!</b>\n\n"
                             f"Your {tier} subscription is now active!\n"
                             f"Duration: {duration} days\n\n"
                             f"Thank you for upgrading! 🚀",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    pass
            else:
                await update.message.reply_text(
                    "❌ Failed to update subscription",
                    parse_mode=ParseMode.HTML
                )
                
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid verification ID",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Approve error: {e}", exc_info=True)
            await update.message.reply_text(
                f"❌ Error: {str(e)[:200]}",
                parse_mode=ParseMode.HTML
            )
    
    async def handle_reject_payment(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle payment rejection"""
        if not context.args:
            await update.message.reply_text(
                "❌ <b>Usage:</b> /reject &lt;verification_id&gt; [reason]\n\n"
                "<b>Example:</b> <code>/reject 12345 Invalid payment proof</code>",
                parse_mode=ParseMode.HTML
            )
            return
        
        try:
            verification_id = int(context.args[0])
            reason = ' '.join(context.args[1:]) if len(context.args) > 1 else 'Payment rejected'
            
            # Get verification from database
            verification = self.db.get_payment_verification(verification_id)
            
            if not verification:
                await update.message.reply_text(
                    f"❌ Verification not found: {verification_id}",
                    parse_mode=ParseMode.HTML
                )
                return
            
            if verification['status'] != 'pending':
                await update.message.reply_text(
                    f"❌ Verification already processed: {verification['status']}",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Reject payment
            user_id = verification['user_id']
            
            # Mark as rejected
            self.db.update_verification_status(verification_id, 'rejected', update.effective_user.id, reason)
            
            await update.message.reply_text(
                f"✅ <b>Payment Rejected</b>\n\n"
                f"• User: <code>{user_id}</code>\n"
                f"• Reason: {reason}",
                parse_mode=ParseMode.HTML
            )
            
            # Notify user
            try:
                await self.bot.send_message(
                    chat_id=user_id,
                    text=f"❌ <b>Payment Verification Failed</b>\n\n"
                         f"Reason: {reason}\n\n"
                         f"Please contact support if you have questions.",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
                
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid verification ID",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Reject error: {e}", exc_info=True)
            await update.message.reply_text(
                f"❌ Error: {str(e)[:200]}",
                parse_mode=ParseMode.HTML
            )
    
    async def show_pending_verifications(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show all pending payment verifications"""
        try:
            pending = self.db.get_pending_verifications()
            
            if not pending:
                await update.message.reply_text(
                    "✅ No pending verifications",
                    parse_mode=ParseMode.HTML
                )
                return
            
            message = "<b>📋 Pending Payment Verifications</b>\n\n"
            
            for v in pending[:10]:  # Show max 10
                message += f"<b>ID:</b> {v['id']}\n"
                message += f"<b>User:</b> <code>{v['user_id']}</code>\n"
                message += f"<b>Tier:</b> {v['tier']}\n"
                message += f"<b>Amount:</b> ${v['amount']}\n"
                message += f"<b>Date:</b> {v['created_at']}\n"
                message += f"\n/approve {v['id']} | /reject {v['id']}\n\n"
            
            if len(pending) > 10:
                message += f"\n<i>... and {len(pending) - 10} more</i>"
            
            await update.message.reply_text(message, parse_mode=ParseMode.HTML)
            
        except Exception as e:
            logger.error(f"Pending verifications error: {e}", exc_info=True)
            await update.message.reply_text(
                f"❌ Error: {str(e)[:200]}",
                parse_mode=ParseMode.HTML
            )


# Create a singleton instance (will be initialized in bot.py)
admin_manager = None

def initialize_admin_manager(db, bot):
    """Initialize the admin manager"""
    global admin_manager
    admin_manager = SimpleAdminManager(db, bot)
    logger.info("✅ Admin manager initialized globally")
    return admin_manager