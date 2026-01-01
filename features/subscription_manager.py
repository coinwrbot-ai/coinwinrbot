"""
CoinWinRBot Subscription Management System
Handles user subscriptions, limits, and payment verification
"""

from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, List
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import html
import logging

logger = logging.getLogger(__name__)


class SubscriptionManager:
    """Manage user subscriptions and access control"""
    
    def __init__(self, database):
        self.db = database
        
        # Subscription tier configurations
        self.tiers = {
            'free': {
                'name': '📊 FREE',
                'price': 0,
                'daily_limit': 10,
                'batch_limit': 1,
                'chains': ['solana', 'bsc'],
                'features': ['basic_analysis'],
                'duration_days': 999999  # Permanent free tier
            },
            'basic_weekly': {
                'name': '💰 BASIC WEEKLY',
                'price': 10,
                'daily_limit': 10,
                'batch_limit': 5,
                'chains': ['solana', 'bsc', 'polygon', 'arbitrum', 'base'],
                'features': ['basic_analysis', 'batch_analysis'],
                'duration_days': 7
            },
            'basic_monthly': {
                'name': '💰 BASIC MONTHLY',
                'price': 20,
                'daily_limit': 15,
                'batch_limit': 10,
                'chains': ['solana', 'bsc', 'polygon', 'arbitrum', 'base', 'ethereum'],
                'features': ['basic_analysis', 'batch_analysis', 'extended_features'],
                'duration_days': 30
            },
            'premium_weekly': {
                'name': '💎 PREMIUM WEEKLY',
                'price': 10,
                'daily_limit': -1,  # Unlimited
                'batch_limit': 50,
                'chains': ['all'],
                'features': ['all'],
                'duration_days': 7
            },
            'premium_monthly': {
                'name': '💎 PREMIUM MONTHLY',
                'price': 40,
                'daily_limit': -1,  # Unlimited
                'batch_limit': 50,
                'chains': ['all'],
                'features': ['all'],
                'duration_days': 30
            }
        }
        
        # ⭐ PAYMENT INFORMATION - REPLACE WITH YOUR ACTUAL ADDRESSES
        self.payment_info = {
            'crypto': {
                # USDT TRC20 (TRON Network) - Lowest fees, recommended
                'usdt_trc20': 'TYourActualTronAddressHere123456789',  # ✅ YOUR ADDRESS
                
                # USDT ERC20 (Ethereum Network) - Higher fees
                'usdt_erc20': '0xYourActualEthereumAddressHere123456',  # ✅ YOUR ADDRESS
                
                # Bitcoin
                'btc': 'bc1qyourbitcoinaddresshere123456789',  # ✅ YOUR ADDRESS
                
                # Solana
                'sol': 'YourSolanaAddressHere123456789ABCDEF'  # ✅ YOUR ADDRESS
            },
            'manual_verification': True,
            'support_username': 'Leanwell_tech'  # ✅ YOUR USERNAME (no @)
        }
        
        logger.info("✅ SubscriptionManager initialized")
    
    def check_analysis_limit(self, user_id: int, batch_size: int = 1) -> Tuple[bool, str]:
        """Check if user can perform analysis"""
        subscription = self.db.get_user_subscription(user_id)
        
        # Check if subscription is active
        if not subscription['is_active']:
            return False, (
                "❌ Your subscription has expired!\n\n"
                "Upgrade now to continue analyzing wallets: /subscription"
            )
        
        # Check daily limit
        daily_limit = subscription['daily_limit']
        current_usage = subscription['daily_analyses']
        
        # Unlimited tier
        if daily_limit == -1:
            return True, "✅ Analysis approved"
        
        # Check if limit reached
        if current_usage + batch_size > daily_limit:
            remaining = daily_limit - current_usage
            return False, (
                f"❌ Daily limit reached!\n\n"
                f"Used: {current_usage}/{daily_limit}\n"
                f"Remaining: {remaining}\n\n"
                f"Upgrade for more analyses: /subscription"
            )
        
        return True, f"✅ Analysis approved ({current_usage + batch_size}/{daily_limit})"
    
    def check_feature_access(self, user_id: int, feature: str) -> bool:
        """Check if user has access to a feature"""
        subscription = self.db.get_user_subscription(user_id)
        tier = self.tiers.get(subscription['tier'])
        
        if not tier:
            return False
        
        return feature in tier['features'] or 'all' in tier['features']
    
    def check_chain_access(self, user_id: int, chain: str) -> bool:
        """Check if user has access to a specific chain"""
        subscription = self.db.get_user_subscription(user_id)
        tier = self.tiers.get(subscription['tier'])
        
        if not tier:
            return False
        
        return chain in tier['chains'] or 'all' in tier['chains']
    
    def get_tier_emoji(self, tier: str) -> str:
        """Get emoji for subscription tier"""
        tier_emojis = {
            'free': '📊',
            'basic_weekly': '💰',
            'basic_monthly': '💰',
            'premium_weekly': '💎',
            'premium_monthly': '💎'
        }
        return tier_emojis.get(tier, '📊')
    
    async def handle_subscription_callback(self, update: Update, 
                                          context: ContextTypes.DEFAULT_TYPE):
        """Handle subscription tier selection from buttons"""
        query = update.callback_query
        await query.answer()
        
        callback_data = query.data
        
        if callback_data.startswith('sub_'):
            tier = callback_data.replace('sub_', '')
            await self.show_payment_instructions(query, tier)
        elif callback_data == 'payment_info':
            await self.show_general_payment_info(query)
        elif callback_data == 'view_plans':
            await self.show_subscription_plans(query)
    
    async def show_subscription_plans(self, query):
        """Display all subscription plans"""
        payment_info = self.payment_info['crypto']
        
        plans_message = f"""💎 <b>CoinWinRBot Subscription Plans</b>

<b>📈 AVAILABLE PLANS:</b>

<b>📊 FREE</b>
• 10 analyses/day
• Basic chains (SOL, BSC)
• Basic features
• Price: FREE

<b>💰 BASIC WEEKLY</b>
• 10 analyses/day
• Multiple chains
• Batch analysis (5x)
• Price: $10/week

<b>💰 BASIC MONTHLY</b>
• 15 analyses/day
• All major chains
• Batch analysis (10x)
• Token analysis ⭐
• Cabal detection
• Price: $20/month

<b>💎 PREMIUM WEEKLY</b>
• UNLIMITED analyses
• All chains
• Batch analysis (50x)
• All features
• Priority support
• Price: $10/week

<b>💎 PREMIUM MONTHLY</b>
• UNLIMITED analyses
• All chains
• Batch analysis (50x)
• All features
• Priority support
• Price: $40/month

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>💳 PAYMENT METHODS:</b>

<b>1️⃣ USDT (TRC20) - Recommended</b>
<code>{payment_info['usdt_trc20']}</code>

<b>2️⃣ USDT (ERC20)</b>
<code>{payment_info['usdt_erc20']}</code>

<b>3️⃣ Bitcoin (BTC)</b>
<code>{payment_info['btc']}</code>

<b>4️⃣ Solana (SOL)</b>
<code>{payment_info['sol']}</code>

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>📝 HOW TO UPGRADE:</b>

<b>Step 1:</b> Choose your plan
<b>Step 2:</b> Send exact USD amount
<b>Step 3:</b> Use command:
<code>/verify &lt;plan&gt; &lt;tx_hash&gt;</code>

<b>Example:</b>
<code>/verify premium_monthly 0x1234...abcd</code>

<b>Available plans:</b>
• basic_weekly
• basic_monthly
• premium_weekly
• premium_monthly

Select a plan below for detailed instructions!
"""
        
        keyboard = [
            [InlineKeyboardButton("💰 Basic Weekly ($10)", callback_data='sub_basic_weekly')],
            [InlineKeyboardButton("💰 Basic Monthly ($20)", callback_data='sub_basic_monthly')],
            [InlineKeyboardButton("💎 Premium Weekly ($10)", callback_data='sub_premium_weekly')],
            [InlineKeyboardButton("💎 Premium Monthly ($40)", callback_data='sub_premium_monthly')],
            [InlineKeyboardButton("ℹ️ Payment Help", callback_data='payment_info')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            plans_message,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def show_payment_instructions(self, query, tier: str):
        """Show payment instructions for selected tier"""
        tier_config = self.tiers.get(tier)
        
        if not tier_config:
            await query.edit_message_text("❌ Invalid subscription tier")
            return
        
        payment_info = self.payment_info['crypto']
        support = self.payment_info['support_username']
        
        payment_message = f"""💳 <b>Payment Instructions - {html.escape(tier_config['name'])}</b>

<b>💰 Amount:</b> ${tier_config['price']} USD
<b>⏰ Duration:</b> {tier_config['duration_days']} days

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>📍 PAYMENT ADDRESSES:</b>

<b>1️⃣ USDT (TRC20) - Recommended ⚡</b>
<code>{payment_info['usdt_trc20']}</code>
<i>✅ Lowest fees (~$1)</i>

<b>2️⃣ USDT (ERC20)</b>
<code>{payment_info['usdt_erc20']}</code>
<i>⚠️ Higher fees (~$5-20)</i>

<b>3️⃣ Bitcoin (BTC)</b>
<code>{payment_info['btc']}</code>
<i>⚠️ Variable fees</i>

<b>4️⃣ Solana (SOL)</b>
<code>{payment_info['sol']}</code>
<i>✅ Low fees (~$0.01)</i>

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>📝 AFTER PAYMENT:</b>

<b>Step 1:</b> Complete your payment
<b>Step 2:</b> Copy transaction hash/ID
<b>Step 3:</b> Submit verification:

<code>/verify {tier} YOUR_TX_HASH</code>

<b>Example:</b>
<code>/verify {tier} 0x1234567890abcdef...</code>

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>⚠️ IMPORTANT NOTES:</b>

✅ Send <b>exact amount</b> in USD equivalent
✅ Use correct network (TRC20/ERC20/etc)
✅ Keep transaction proof until verified
✅ Verification: Usually 1-24 hours
✅ Instant access after admin approval

<b>Need help?</b> Contact: @{support}
"""
        
        keyboard = [
            [InlineKeyboardButton("✅ I've Made Payment", callback_data=f'paid_{tier}')],
            [InlineKeyboardButton("💬 Contact Support", url=f"https://t.me/{support}")],
            [InlineKeyboardButton("🔙 Back to Plans", callback_data='view_plans')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            payment_message,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def show_general_payment_info(self, query):
        """Show general payment information"""
        payment_info = self.payment_info['crypto']
        support = self.payment_info['support_username']
        
        info_message = f"""💳 <b>CoinWinRBot Payment Information</b>

<b>🔐 PAYMENT ADDRESSES:</b>

<b>USDT (TRC20) - Recommended ⭐</b>
<code>{payment_info['usdt_trc20']}</code>
<i>Lowest fees, fastest confirmation</i>

<b>USDT (ERC20)</b>
<code>{payment_info['usdt_erc20']}</code>
<i>Higher fees, slower confirmation</i>

<b>Bitcoin (BTC)</b>
<code>{payment_info['btc']}</code>
<i>Flexible, variable fees</i>

<b>Solana (SOL)</b>
<code>{payment_info['sol']}</code>
<i>Fast, low fees</i>

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>📋 VERIFICATION PROCESS:</b>

<b>1.</b> Choose your subscription plan
<b>2.</b> Send payment to any address above
<b>3.</b> Submit with <code>/verify</code> command
<b>4.</b> Admin verifies within 1-24 hours
<b>5.</b> Instant access after approval

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>💡 PAYMENT TIPS:</b>

✅ <b>TRC20 USDT</b> is recommended (fast + cheap)
✅ Always send <b>exact amount</b> in USD
✅ Double-check <b>network</b> before sending
✅ Save transaction hash for verification
✅ Contact support if payment stuck

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>🔄 REFUND POLICY:</b>

• Refunds available within 24 hours
• No refunds after subscription activation
• Contact support for payment issues

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>🔒 SECURITY:</b>

✅ All transactions manually verified
✅ Secure payment addresses
✅ No personal information required
✅ Instant activation after approval

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>📞 SUPPORT:</b>
Contact: @{support}
Help: /help
Status: /stats
"""
        
        keyboard = [
            [InlineKeyboardButton("💬 Contact Support", url=f"https://t.me/{support}")],
            [InlineKeyboardButton("🔙 Back to Plans", callback_data='view_plans')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            info_message,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def handle_payment_verification(self, update: Update, 
                                         context: ContextTypes.DEFAULT_TYPE):
        """Handle payment verification command"""
        user_id = update.effective_user.id
        
        if not context.args or len(context.args) < 2:
            await update.message.reply_text(
                "❌ <b>Invalid format!</b>\n\n"
                "<b>Usage:</b> <code>/verify &lt;tier&gt; &lt;transaction_hash&gt;</code>\n\n"
                "<b>Example:</b> <code>/verify premium_monthly 0x1234...abcd</code>\n\n"
                "<b>Available tiers:</b>\n"
                "• basic_weekly\n"
                "• basic_monthly\n"
                "• premium_weekly\n"
                "• premium_monthly",
                parse_mode='HTML'
            )
            return
        
        tier = context.args[0]
        tx_hash = ' '.join(context.args[1:])  # Join in case hash has spaces
        
        # Validate tier
        if tier not in self.tiers or tier == 'free':
            await update.message.reply_text(
                f"❌ Invalid subscription tier: {html.escape(tier)}\n\n"
                "Use /subscription to see available plans.",
                parse_mode='HTML'
            )
            return
        
        tier_config = self.tiers[tier]
        
        # Submit verification request to database
        try:
            verification_id = self.db.submit_payment_proof(
                user_id=user_id,
                tier=tier,
                amount=tier_config['price'],
                proof=tx_hash
            )
            
            support = self.payment_info['support_username']
            
            # Notify user
            await update.message.reply_text(
                f"✅ <b>Payment Verification Submitted!</b>\n\n"
                f"<b>Verification ID:</b> #{verification_id}\n"
                f"<b>Plan:</b> {html.escape(tier_config['name'])}\n"
                f"<b>Amount:</b> ${tier_config['price']}\n"
                f"<b>Transaction:</b> <code>{html.escape(tx_hash[:32])}...</code>\n\n"
                f"⏳ Your payment is being verified by our team.\n"
                f"You'll receive a notification once approved (usually 1-24 hours).\n\n"
                f"<b>Status:</b> Pending ⏰\n"
                f"<b>Check status:</b> /check_verification {verification_id}\n\n"
                f"<b>Need help?</b> Contact @{support}",
                parse_mode='HTML'
            )
            
            logger.info(f"✅ Payment verification submitted: User {user_id}, Tier {tier}, ID #{verification_id}")
            
            # Notify admins (if available)
            await self.notify_admins_new_verification(context, verification_id, user_id, tier, tx_hash, tier_config)
            
        except Exception as e:
            logger.error(f"❌ Payment verification error: {e}", exc_info=True)
            await update.message.reply_text(
                f"❌ <b>Verification Failed</b>\n\n"
                f"Error: {html.escape(str(e)[:200])}\n\n"
                f"Please try again or contact support.",
                parse_mode='HTML'
            )
    
    async def notify_admins_new_verification(self, context, verification_id: int, 
                                            user_id: int, tier: str, tx_hash: str, tier_config: dict):
        """Notify admin users about new payment verification"""
        try:
            # Get admin IDs from database
            admin_ids = self.db.get_admin_ids()
            
            if not admin_ids:
                logger.warning("⚠️ No admin IDs found - skipping admin notification")
                return
            
            # Get user info
            try:
                user = self.db.get_user(user_id)
                username = user.get('username', 'N/A')
            except:
                username = 'N/A'
            
            admin_message = f"""🔔 <b>New Payment Verification</b>

<b>ID:</b> #{verification_id}
<b>User:</b> @{html.escape(username)} (ID: {user_id})
<b>Plan:</b> {html.escape(tier_config['name'])}
<b>Amount:</b> ${tier_config['price']}
<b>Duration:</b> {tier_config['duration_days']} days
<b>TX Hash:</b> <code>{html.escape(tx_hash[:64])}...</code>

<b>Actions:</b>
<code>/approve {verification_id}</code> - Approve
<code>/reject {verification_id} reason</code> - Reject
"""
            
            # Send to all admins
            success_count = 0
            for admin_id in admin_ids:
                try:
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=admin_message,
                        parse_mode='HTML'
                    )
                    success_count += 1
                except Exception as e:
                    logger.error(f"Failed to notify admin {admin_id}: {e}")
            
            logger.info(f"✅ Notified {success_count}/{len(admin_ids)} admins")
            
        except Exception as e:
            logger.error(f"❌ Admin notification error: {e}", exc_info=True)
    
    def get_subscription_status(self, user_id: int) -> Dict:
        """Get detailed subscription status"""
        subscription = self.db.get_user_subscription(user_id)
        tier_config = self.tiers.get(subscription['tier'], self.tiers['free'])
        
        # Calculate days remaining
        expires_at = subscription.get('expires_at')
        if expires_at:
            # Convert string to datetime if necessary
            if isinstance(expires_at, str):
                try:
                    from dateutil import parser
                    expires_at = parser.parse(expires_at)
                except ImportError:
                    # Fallback: parse common datetime formats
                    try:
                        expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            expires_at = datetime.strptime(expires_at, '%Y-%m-%d')
                        except ValueError:
                            expires_at = None
            
            if expires_at:
                days_remaining = max(0, (expires_at - datetime.now()).days)
            else:
                days_remaining = 0
        else:
            days_remaining = 0
            expires_at = None
        
        # Calculate usage percentage
        daily_limit = subscription.get('daily_limit', 10)
        daily_analyses = subscription.get('daily_analyses', 0)
        
        if daily_limit == -1:
            usage_percentage = 0  # Unlimited
        elif daily_limit > 0:
            usage_percentage = min(100, (daily_analyses / daily_limit * 100))
        else:
            usage_percentage = 0
        
        return {
            'tier': subscription.get('tier', 'free'),
            'tier_name': tier_config['name'],
            'is_active': subscription.get('is_active', False),
            'expires_at': expires_at,
            'days_remaining': days_remaining,
            'daily_analyses': daily_analyses,
            'daily_limit': daily_limit,
            'usage_percentage': usage_percentage,
            'features': tier_config['features'],
            'chains': tier_config['chains']
        }


# Simple AdminManager class (kept for compatibility)
class AdminManager:
    """Admin manager - delegates to SimpleAdminManager"""
    
    def __init__(self, database, subscription_manager):
        self.db = database
        self.sub_manager = subscription_manager
        logger.info("✅ AdminManager initialized (compatibility wrapper)")
    
    async def handle_approve_payment(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Redirect to SimpleAdminManager"""
        from features.admin_commands import admin_manager
        if admin_manager:
            await admin_manager.handle_approve_payment(update, context)
        else:
            await update.message.reply_text("❌ Admin manager not initialized")
    
    async def handle_reject_payment(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Redirect to SimpleAdminManager"""
        from features.admin_commands import admin_manager
        if admin_manager:
            await admin_manager.handle_reject_payment(update, context)
        else:
            await update.message.reply_text("❌ Admin manager not initialized")
    
    async def show_pending_verifications(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Redirect to SimpleAdminManager"""
        from features.admin_commands import admin_manager
        if admin_manager:
            await admin_manager.show_pending_verifications(update, context)
        else:
            await update.message.reply_text("❌ Admin manager not initialized")