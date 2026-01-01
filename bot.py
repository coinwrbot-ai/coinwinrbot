"""
CoinWinRBot - FULLY OPTIMIZED Production Bot
✅ Integrated OptimizedTokenFetcher
✅ Unified session management
✅ Smart cache management
✅ Enhanced rate limiting
✅ Optimized CSV export
"""

import os
import sys
import logging
from web3 import Web3
import asyncio
import signal
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional, List, Set
from functools import lru_cache
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, 
    MessageHandler, filters, ContextTypes, ApplicationBuilder
)
from telegram.request import HTTPXRequest
from telegram.constants import ParseMode
import aiohttp
from cachetools import TTLCache
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import io
import csv

#from features.moralis_evm_analyzer import EnhancedPublicEVMAnalyzer
from features.evm_winrate_analyzer import EVMWinRateAnalyzer
from features.admin_commands import admin_panel_command, register_admin_handlers, ADMIN_USER_IDS
from features.config import Config
from database_factory import db
from features.solana_wallet_analyzer import AnalyzerFactory, BirdeyePnLFetcher, health_monitor
from features.token import TokenTraderAnalyzer
from features.subscription_manager import SubscriptionManager, AdminManager
from features.optimized_token_fetcher import OptimizedTokenFetcher, HELIUS_API_KEY, preload_jupiter_tokens
from features.enhanced_token_data import (
    EnhancedTokenDataFetcher
)
from features.solana_wallet_analyzer import FastEnhancedSolanaAnalyzer as EnhancedSolanaAnalyzer

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(), logging.FileHandler('bot.log')]
)
logger = logging.getLogger(__name__)

# ⭐ ADD THESE TWO LINES:
logging.getLogger('telegram.ext.Application').setLevel(logging.ERROR)
logging.getLogger('telegram.ext.Updater').setLevel(logging.ERROR)

CHAIN_MAPPING = {
    # Ethereum
    'ethereum': 'ethereum',
    'eth': 'ethereum',
    
    # BSC (Binance Smart Chain)  # ⭐ More variations
    'bsc': 'bsc',
    'binance': 'bsc',
    'bnb': 'bsc',
    'binance smart chain': 'bsc',
    
    # Polygon
    'polygon': 'polygon',
    'matic': 'polygon',
    'poly': 'polygon',
    
    # Arbitrum
    'arbitrum': 'arbitrum',
    'arb': 'arbitrum',
    
    # Optimism
    'optimism': 'optimism',
    'op': 'optimism',
    
    # Base
    'base': 'base',
    
    # Avalanche
    'avalanche': 'avalanche',
    'avax': 'avalanche',
    
    # Solana
    'solana': 'solana',
    'sol': 'solana'
}

# ============================================================================
# ⭐ NEW: UNIFIED SESSION FACTORY
# ============================================================================

class GlobalSessionFactory:
    """
    Production-ready session factory with:
    - Context manager support
    - Automatic cleanup
    - Connection pooling
    - Health monitoring
    """
    
    def __init__(self):
        self._sessions: Dict[str, aiohttp.ClientSession] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._closed = False
        
    async def get_session(self, pool_name: str = 'default') -> aiohttp.ClientSession:
        """Get or create session with automatic initialization"""
        if self._closed:
            raise RuntimeError("SessionFactory is closed")
        
        if pool_name not in self._locks:
            self._locks[pool_name] = asyncio.Lock()
        
        async with self._locks[pool_name]:
            if pool_name not in self._sessions or self._sessions[pool_name].closed:
                self._sessions[pool_name] = await self._create_session(pool_name)
                logger.info(f"🔗 Session pool created: {pool_name}")
            
            return self._sessions[pool_name]
    
    async def _create_session(self, pool_name: str) -> aiohttp.ClientSession:
        """Create optimized session with proper configuration"""
        configs = {
            'default': {'limit': 100, 'limit_per_host': 30, 'timeout': 30},
            'rpc': {'limit': 50, 'limit_per_host': 10, 'timeout': 10},
            'api': {'limit': 200, 'limit_per_host': 50, 'timeout': 15},
        }
        
        config = configs.get(pool_name, configs['default'])
        
        connector = aiohttp.TCPConnector(
            limit=config['limit'],
            limit_per_host=config['limit_per_host'],
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            force_close=False,
            keepalive_timeout=60
        )
        
        timeout = aiohttp.ClientTimeout(total=config['timeout'])
        
        return aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': f'CoinWinRBot/{pool_name}'}
        )
    
    async def cleanup(self):
        """Graceful cleanup of all sessions"""
        if self._closed:
            return
        
        self._closed = True
        logger.info("🧹 Cleaning up sessions...")
        
        # Close all sessions concurrently
        if self._sessions:
            await asyncio.gather(
                *[session.close() for session in self._sessions.values()],
                return_exceptions=True
            )
        
        self._sessions.clear()
        self._locks.clear()
        
        # Allow connections to close
        await asyncio.sleep(0.5)
        logger.info("✅ All sessions closed")

# Global session factory instance
session_factory = GlobalSessionFactory()
# ============================================================================
# ENHANCED RATE LIMITER
# ============================================================================
class CacheManager:
    """Memory-efficient cache with automatic cleanup"""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.caches: Dict[str, TTLCache] = {}
        self._cleanup_task: Optional[asyncio.Task] = None
        
    def register(self, name: str, cache: TTLCache):
        """Register a cache for management"""
        self.caches[name] = cache
        logger.info(f"📦 Cache registered: {name}")
    
    def total_size(self) -> int:
        """Get total cached items"""
        return sum(len(cache) for cache in self.caches.values())
    
    async def start_cleanup(self):
        """Start periodic cleanup task"""
        async def cleanup_loop():
            while True:
                try:
                    await asyncio.sleep(1800)  # 30 minutes
                    
                    total = self.total_size()
                    threshold = int(self.max_size * 0.8)
                    
                    if total > threshold:
                        target = int(self.max_size * 0.5)
                        to_remove = total - target
                        
                        logger.info(f"🧹 Cleanup: {to_remove} entries")
                        
                        for cache in self.caches.values():
                            if not cache:
                                continue
                            
                            portion = int(to_remove * (len(cache) / total))
                            
                            for _ in range(min(portion, len(cache))):
                                try:
                                    cache.popitem(last=False)
                                except KeyError:
                                    break
                        
                        logger.info(f"✅ Cleaned to {self.total_size()} entries")
                
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Cleanup error: {e}")
        
        self._cleanup_task = asyncio.create_task(cleanup_loop())
    
    async def stop_cleanup(self):
        """Stop cleanup task"""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
# Add to your cache manager:

class EnhancedRateLimiter:
    """Efficient rate limiter with persistence"""
    
    def __init__(
        self,
        per_user_limit: int = 20,
        global_limit: int = 100,
        window_seconds: int = 60,
        persistence_file: str = 'rate_limits.json'
    ):
        self.per_user_limit = per_user_limit
        self.global_limit = global_limit
        self.window = timedelta(seconds=window_seconds)
        self.persistence_file = Path(persistence_file)
        
        self.user_requests: Dict[int, List[datetime]] = defaultdict(list)
        self.global_requests: List[datetime] = []
        
        self._load_state()
    
    def _load_state(self):
        """Load persisted state"""
        if not self.persistence_file.exists():
            return
        
        try:
            with open(self.persistence_file, 'r') as f:
                data = json.load(f)
            
            for user_id, timestamps in data.get('user_requests', {}).items():
                self.user_requests[int(user_id)] = [
                    datetime.fromisoformat(ts) for ts in timestamps
                ]
            
            self.global_requests = [
                datetime.fromisoformat(ts) 
                for ts in data.get('global_requests', [])
            ]
            
            logger.info("✅ Rate limits loaded")
        except Exception as e:
            logger.error(f"Failed to load rate limits: {e}")
    
    async def _save_state(self):
        """Persist state to disk"""
        try:
            data = {
                'user_requests': {
                    str(uid): [ts.isoformat() for ts in timestamps]
                    for uid, timestamps in self.user_requests.items()
                },
                'global_requests': [ts.isoformat() for ts in self.global_requests]
            }
            
            # Use asyncio to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.persistence_file.write_text(json.dumps(data))
            )
        except Exception as e:
            logger.error(f"Failed to save rate limits: {e}")
    
    async def check_limit(self, user_id: int) -> Tuple[bool, str]:
        """Check if user is within rate limits"""
        now = datetime.now()
        cutoff = now - self.window
        
        # Clean old entries
        self.user_requests[user_id] = [
            t for t in self.user_requests[user_id] if t > cutoff
        ]
        self.global_requests = [
            t for t in self.global_requests if t > cutoff
        ]
        
        # Check global limit
        if len(self.global_requests) >= self.global_limit:
            return False, "🔥 System busy. Try again soon."
        
        # Check per-user limit
        if len(self.user_requests[user_id]) >= self.per_user_limit:
            wait = int((self.user_requests[user_id][0] - cutoff).total_seconds())
            return False, f"⏰ Rate limit. Wait {wait}s"
        
        # Allow request
        self.user_requests[user_id].append(now)
        self.global_requests.append(now)
        
        # Periodically save (every 10 requests)
        if len(self.global_requests) % 10 == 0:
            asyncio.create_task(self._save_state())
        
        return True, "OK"
    
    async def cleanup(self):
        """Final cleanup"""
        await self._save_state()
# ============================================================================
# CONCURRENT ANALYZER
# ============================================================================

class ConcurrentAnalyzer:
    """
    Modern task manager using Python 3.11+ TaskGroup
    for structured concurrency
    """
    
    def __init__(self, max_concurrent: int = 100):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.active_tasks: Dict[int, Dict[str, Any]] = {}
    
    async def run_isolated(self, user_id: int, coro):
        """Run coroutine with resource isolation"""
        async with self.semaphore:
            start = datetime.now()
            self.active_tasks[user_id] = {
                'started': start,
                'status': 'running'
            }
            
            try:
                result = await coro
                duration = (datetime.now() - start).total_seconds()
                logger.info(f"✅ User {user_id}: {duration:.2f}s")
                return result
            finally:
                self.active_tasks.pop(user_id, None)
    
    def active_count(self) -> int:
        """Get number of active tasks"""
        return len(self.active_tasks)

# ============================================================================
# SHUTDOWN HANDLER
# ============================================================================
class ShutdownHandler:
    """Handle graceful shutdown with proper cleanup"""
    
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        self._shutting_down = False
    
    def setup_signals(self):
        """Setup SIGTERM and SIGINT handlers"""
        loop = asyncio.get_event_loop()
        
        def signal_handler(sig):
            if self._shutting_down:
                logger.warning("⚠️ Already shutting down...")
                return
            
            sig_name = signal.Signals(sig).name
            logger.info(f"🛑 Received {sig_name}")
            self._shutting_down = True
            self.shutdown_event.set()
        
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
        
        logger.info("✅ Signal handlers ready")
    
    async def wait(self):
        """Wait for shutdown signal"""
        await self.shutdown_event.wait()

# ============================================================================
# INITIALIZATION
# ============================================================================
try:
    Config.validate()
    logger.info("✅ Configuration validated")
except ValueError as e:
    logger.error(f"❌ Config error: {e}")
# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

cache_manager = CacheManager(max_size=10000)
rate_limiter = EnhancedRateLimiter(per_user_limit=20, global_limit=100)
task_manager = ConcurrentAnalyzer(max_concurrent=100)
shutdown_handler = ShutdownHandler()
thread_pool = ThreadPoolExecutor(max_workers=50, thread_name_prefix='bot_worker')
TOKEN_METADATA_CACHE = TTLCache(maxsize=1000, ttl=3600)
cache_manager.register('token_metadata', TOKEN_METADATA_CACHE)
# Initialize caches
ADDRESS_CACHE = TTLCache(maxsize=10000, ttl=3600)
ADDRESS_TYPE_CACHE = TTLCache(maxsize=5000, ttl=1800)
cache_manager.register('address', ADDRESS_CACHE)
cache_manager.register('address_type', ADDRESS_TYPE_CACHE)

# Regex patterns (compiled once)
EVM_ADDRESS_PATTERN = re.compile(r'^0x[a-fA-F0-9]{40}$')
SOLANA_ADDRESS_PATTERN = re.compile(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$')

# Initialize analyzers
try:
    Config.validate()
    
    # ⭐ NEW: Multi-source Solana analyzer with automatic fallback
    
    solana_analyzer = EnhancedSolanaAnalyzer(
        helius_api_key=HELIUS_API_KEY,
        birdeye_api_key=getattr(Config, 'BIRDEYE_API_KEY', None)
    )
    
    # EVM analyzer (unchanged)
    evm_analyzer = EVMWinRateAnalyzer(
    alchemy_api_key=Config.ALCHEMY_API_KEY,
    session_factory=session_factory
)

    logger.info("✅ EVM Win Rate Analyzer initialized")
    
    sub_manager = SubscriptionManager(db)
    token_analyzer = TokenTraderAnalyzer(solana_analyzer, evm_analyzer)
    admin_manager = AdminManager(db, sub_manager)
    
    logger.info("✅ All systems initialized")
    logger.info("   ⚡ Multi-source price fetching enabled")
    logger.info("   📊 Fallback: Birdeye → Jupiter → DexScreener → CoinGecko → Raydium")
    
except Exception as e:
    logger.error(f"❌ Initialization failed: {e}")
    raise

try:
    enhanced_token_fetcher = EnhancedTokenDataFetcher(
        birdeye_key=getattr(Config, 'BIRDEYE_API_KEY', None),
        bitquery_key=getattr(Config, 'BITQUERY_API_KEY', None),
        session_factory=session_factory  # Use your existing session factory
    )
    logger.info("✅ Enhanced token fetcher initialized")
except Exception as e:
    logger.warning(f"⚠️ Enhanced token fetcher init failed: {e}")
    enhanced_token_fetcher = None



# ============================================================================
# UTILITY FUNCTIONS (CACHED & OPTIMIZED)
# ============================================================================
@lru_cache(maxsize=1000)
def html_escape(text: str) -> str:
    """Escape HTML entities"""
    if not text:
        return ""
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def is_valid_address(text: str) -> bool:
    """Validate blockchain address with caching"""
    text = text.strip()
    
    if text in ADDRESS_CACHE:
        return ADDRESS_CACHE[text]
    
    length = len(text)
    
    if length == 42:
        result = bool(EVM_ADDRESS_PATTERN.match(text))
    elif 32 <= length <= 44:
        result = bool(SOLANA_ADDRESS_PATTERN.match(text))
    else:
        result = False
    
    ADDRESS_CACHE[text] = result
    return result

def format_number(num: float) -> str:
    """Format number with K/M suffixes"""
    if num is None or num == 0:
        return "$0.00"
    
    sign = '+' if num > 0 else ''
    abs_num = abs(num)
    
    if abs_num >= 1_000_000:
        return f"{sign}${num/1_000_000:.1f}M"
    elif abs_num >= 1_000:
        return f"{sign}${num/1_000:.1f}K"
    else:
        return f"{sign}${num:,.2f}"

# ============================================================================
# ⭐ NEW: TOKEN HOLDINGS ENHANCEMENT
# ============================================================================

async def enhance_wallet_with_token_holdings(wallet: str, chain: str, base_result: Dict) -> Dict:
    """⭐ Integrates OptimizedTokenFetcher"""
    if chain != 'solana':
        return base_result
    
    try:
        logger.info(f"🪙 Fetching optimized tokens for {wallet[:8]}...")
        fetcher = OptimizedTokenFetcher(helius_key=HELIUS_API_KEY)
        
        tokens = await asyncio.wait_for(
            fetcher.fetch_wallet_top_tokens(wallet, limit=5),
            timeout=15.0
        )
        
        await fetcher.close()
        
        if tokens:
            base_result['top_tokens'] = tokens
            total = sum(t.get('current_value', 0) for t in tokens)
            logger.info(f"✅ Added {len(tokens)} tokens (${total:,.2f})")
        
        return base_result
    except Exception as e:
        logger.error(f"Token holdings error: {e}")
        return base_result

# ============================================================================
# CSV EXPORT (OPTIMIZED)
# ============================================================================
def format_cabal_data_for_csv(cabal_result: Dict, token_address: str, chain: str) -> list:
    """
    Convert trader data to CSV format (LIMITED TO 20 WALLETS)
    
    Export Priority:
    1. Cabal members (highest risk)
    2. Suspicious wallets
    3. Top normal traders (by profit)
    
    Args:
        cabal_result: Results from cabal detection
        token_address: Token contract address
        chain: Blockchain name
        
    Returns:
        List of dictionaries ready for CSV export (max 20 traders)
    """
    csv_data = []
    MAX_EXPORT_WALLETS = 20  # ⭐ HARD LIMIT
    
    # Extract data structures
    cabals = cabal_result.get('cabals', [])
    suspicious_wallets = cabal_result.get('suspicious_wallets', [])
    all_traders = cabal_result.get('all_traders', [])
    
    logger.info(f"📊 CSV Export - Limited to {MAX_EXPORT_WALLETS} wallets:")
    logger.info(f"   Total traders available: {len(all_traders)}")
    logger.info(f"   Cabals: {len(cabals)}")
    logger.info(f"   Suspicious: {len(suspicious_wallets)}")
    
    # Track processed wallets to avoid duplicates
    added_wallets = set()
    
    def format_usd(value):
        """Format number as USD with $ symbol"""
        try:
            return f"${float(value):,.2f}"
        except (ValueError, TypeError):
            return "$0.00"
    
    def calculate_pnl_percentage(total_pnl, total_volume):
        """Calculate PNL percentage based on volume"""
        try:
            if total_pnl and total_volume > 0:
                return f"{(float(total_pnl) / float(total_volume) * 100):.2f}%"
            return "0.00%"
        except (ValueError, TypeError, ZeroDivisionError):
            return "0.00%"
    
    def safe_get(trader: Dict, key: str, default=0):
        """Safely get value from trader dict"""
        try:
            value = trader.get(key, default)
            return float(value) if isinstance(value, (int, float, str)) else default
        except (ValueError, TypeError):
            return default
    
    def add_trader_to_csv(trader_data: Dict, cabal_group: str, risk_level: str, patterns: str):
        """Helper function to add trader to CSV data"""
        wallet_addr = trader_data.get('wallet', 'N/A')
        
        if wallet_addr in added_wallets:
            return False
        
        added_wallets.add(wallet_addr)
        
        total_pnl = safe_get(trader_data, 'total_profit')
        total_volume = safe_get(trader_data, 'total_volume')
        
        csv_data.append({
            'trader': wallet_addr,
            'total_pnl_usd': format_usd(total_pnl),
            'total_pnl_percentage': calculate_pnl_percentage(total_pnl, total_volume),
            'total_realized_pnl_usd': format_usd(safe_get(trader_data, 'realized_pnl')),
            'total_unrealized_pnl_usd': format_usd(safe_get(trader_data, 'unrealized_pnl')),
            '7d_profit_usd': format_usd(safe_get(trader_data, 'profit_7d')),
            '7d_profit_pct': f"{safe_get(trader_data, 'profit_7d_pct'):.2f}%",
            'token1_name': trader_data.get('top_token_1_name', ''),
            'token1_total_pnl_usd': format_usd(safe_get(trader_data, 'top_token_1_profit')),
            'token1_unrealized_pnl_usd': format_usd(safe_get(trader_data, 'top_token_1_unrealized')),
            'token1_trades': int(safe_get(trader_data, 'top_token_1_trades')),
            'token1_winrate': f"{safe_get(trader_data, 'top_token_1_winrate'):.1f}%",
            'token2_name': trader_data.get('top_token_2_name', ''),
            'token2_total_pnl_usd': format_usd(safe_get(trader_data, 'top_token_2_profit')),
            'token2_unrealized_pnl_usd': format_usd(safe_get(trader_data, 'top_token_2_unrealized')),
            'token2_trades': int(safe_get(trader_data, 'top_token_2_trades')),
            'token2_winrate': f"{safe_get(trader_data, 'top_token_2_winrate'):.1f}%",
            'token3_name': trader_data.get('top_token_3_name', ''),
            'token3_total_pnl_usd': format_usd(safe_get(trader_data, 'top_token_3_profit')),
            'total_trades': int(safe_get(trader_data, 'total_trades')),
            'win_rate': f"{safe_get(trader_data, 'win_rate'):.1f}%",
            'roi': f"{safe_get(trader_data, 'roi'):.1f}%",
            'total_volume_usd': format_usd(total_volume),
            'is_holder': str(trader_data.get('is_holder', False)),
            'cabal_group': cabal_group,
            'risk_level': risk_level,
            'patterns_detected': patterns,
            'target_token': token_address,
            'chain': chain.upper(),
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        return True
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 1: Add cabal members
    # ═══════════════════════════════════════════════════════════════
    for cabal_idx, cabal in enumerate(cabals, 1):
        wallets = cabal.get('wallets', [])
        risk_level = cabal.get('risk_level', 'UNKNOWN')
        patterns = ', '.join(cabal.get('patterns', []))
        
        for wallet in wallets:
            wallet_addr = wallet.get('address', 'N/A')
            
            # Find full trader data
            trader_data = next(
                (t for t in all_traders if t.get('wallet') == wallet_addr),
                wallet
            )
            
            add_trader_to_csv(
                trader_data,
                f"Cabal_{cabal_idx}",
                risk_level,
                patterns
            )
    
    logger.info(f"   Added {len(added_wallets)} cabal members")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 2: Add suspicious wallets (not in cabals)
    # ═══════════════════════════════════════════════════════════════
    suspicious_added = 0
    for wallet in suspicious_wallets:
        wallet_addr = wallet.get('address', 'N/A')
        
        if wallet_addr in added_wallets:
            continue
        
        trader_data = next(
            (t for t in all_traders if t.get('wallet') == wallet_addr),
            wallet
        )
        
        if add_trader_to_csv(
            trader_data,
            'Suspicious_Individual',
            'MEDIUM',
            'Individual Suspicious Activity'
        ):
            suspicious_added += 1
    
    logger.info(f"   Added {suspicious_added} suspicious wallets")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 3: Add ALL remaining traders (⭐ NO 100 LIMIT)
    # ═══════════════════════════════════════════════════════════════
    normal_added = 0
    for trader in all_traders:
        wallet_addr = trader.get('wallet', 'N/A')
        
        # Skip if already added
        if wallet_addr in added_wallets:
            continue
        
        # ⭐ REMOVED: if len(csv_data) >= 100: break
        # Now adds ALL traders without limit
        
        if add_trader_to_csv(
            trader,
            'Normal_Trader',
            'LOW',
            'No suspicious activity'
        ):
            normal_added += 1
    
    logger.info(f"   Added {normal_added} normal traders")
    
    # ═══════════════════════════════════════════════════════════════
    # FINAL SUMMARY
    # ═══════════════════════════════════════════════════════════════
    logger.info(f"✅ CSV Export prepared: {len(csv_data)} traders (ALL)")
    logger.info(f"   Cabal members: {sum(1 for r in csv_data if 'Cabal_' in r['cabal_group'])}")
    logger.info(f"   Suspicious: {sum(1 for r in csv_data if r['cabal_group'] == 'Suspicious_Individual')}")
    logger.info(f"   Normal: {sum(1 for r in csv_data if r['cabal_group'] == 'Normal_Trader')}")
    logger.info(f"   ⭐ NO LIMIT APPLIED - All {len(all_traders)} traders included")
    
    return csv_data


def generate_csv_file(csv_data: list) -> io.BytesIO:
    """
    Generate CSV file from data (unchanged)
    
    Returns BytesIO object ready for Telegram upload
    """
    if not csv_data:
        logger.warning("⚠️ No data to export")
        return None
    
    # Create CSV in memory
    output = io.StringIO()
    
    # Complete field list
    fieldnames = [
        'trader',
        'total_pnl_usd',
        'total_pnl_percentage',
        'total_realized_pnl_usd',
        'total_unrealized_pnl_usd',
        '7d_profit_usd',
        '7d_profit_pct',
        'token1_name',
        'token1_total_pnl_usd',
        'token1_unrealized_pnl_usd',
        'token1_trades',
        'token1_winrate',
        'token2_name',
        'token2_total_pnl_usd',
        'token2_unrealized_pnl_usd',
        'token2_trades',
        'token2_winrate',
        'token3_name',
        'token3_total_pnl_usd',
        'total_trades',
        'win_rate',
        'roi',
        'total_volume_usd',
        'is_holder',
        'cabal_group',
        'risk_level',
        'patterns_detected',
        'target_token',
        'chain',
        'analysis_date'
    ]
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(csv_data)
    
    # Convert to BytesIO for Telegram
    output.seek(0)
    csv_bytes = io.BytesIO(output.getvalue().encode('utf-8'))
    csv_bytes.name = f"traders_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    logger.info(f"✅ CSV file generated: {csv_bytes.name}")
    logger.info(f"   Total rows: {len(csv_data)} (ALL traders)")
    
    return csv_bytes

async def export_csv_with_progress(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """⭐ Optimized CSV export with progress"""
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    message = update.message or update.callback_query.message
    
    subscription = db.get_user_subscription(user_id)
    if 'premium' not in subscription['tier']:
        await message.reply_text("🔒 Premium feature only", parse_mode=ParseMode.HTML)
        return
    
    progress = await message.reply_text(
        "📊 <b>Generating CSV...</b>\n⏳ Step 1/3: Loading data...",
        parse_mode=ParseMode.HTML
    )
    
    try:
        all_traders = context.user_data.get('all_traders', [])
        if not all_traders:
            await progress.edit_text("❌ No data. Analyze a token first.", parse_mode=ParseMode.HTML)
            return
        
        await progress.edit_text(
            f"📊 <b>Generating CSV...</b>\n✅ Loaded {len(all_traders)} traders\n⏳ Step 2/3: Formatting...",
            parse_mode=ParseMode.HTML
        )
        
        loop = asyncio.get_event_loop()
        csv_data = await loop.run_in_executor(
            thread_pool,
            format_cabal_data_for_csv,
            context.user_data.get('last_cabal_result', {}),
            context.user_data.get('last_token_address', 'Unknown'),
            context.user_data.get('last_chain', 'Unknown')
        )
        
        await progress.edit_text(
            f"📊 <b>Generating CSV...</b>\n✅ Formatted {len(csv_data)} rows\n⏳ Step 3/3: Creating file...",
            parse_mode=ParseMode.HTML
        )
        
        csv_file = await loop.run_in_executor(thread_pool, generate_csv_file, csv_data)
        
        await progress.delete()
        
        caption = f"""✅ <b>CSV Export Complete</b>

📊 Total: {len(csv_data)} traders
📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}

Thank you for premium! 💎"""
        
        await message.reply_document(document=csv_file, caption=caption, parse_mode=ParseMode.HTML, filename=csv_file.name)
        logger.info(f"✅ CSV exported: {len(csv_data)} traders")
        
    except Exception as e:
        logger.error(f"CSV error: {e}", exc_info=True)
        await progress.edit_text(f"❌ Export failed: {html_escape(str(e)[:100])}", parse_mode=ParseMode.HTML)


from typing import Optional, Callable, Any
import random

async def retry_with_backoff(
    func,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
    exponential_base: float = 2.0,
    acceptable_errors: tuple = (asyncio.TimeoutError, aiohttp.ClientError)
) -> any:
    """Enhanced retry with better error handling"""
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            return await func()
            
        except acceptable_errors as e:
            last_exception = e
            
            if attempt < max_retries - 1:
                delay = min(
                    initial_delay * (exponential_base ** attempt),
                    max_delay
                )
                jitter = random.uniform(0, delay * 0.1)
                wait_time = delay + jitter
                
                logger.info(f"Retry {attempt + 1}/{max_retries} in {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} retries exhausted: {e}")
                
        except Exception as e:
            # Don't retry on unexpected errors
            logger.error(f"Non-retryable error: {e}")
            raise
    
    raise last_exception


class HealthMonitor:
    """Simple health monitor to track system status"""
    
    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.timeout_requests = 0
        self.start_time = datetime.now()
        
    def record_success(self):
        self.total_requests += 1
        self.successful_requests += 1
        
    def record_failure(self):
        self.total_requests += 1
        self.failed_requests += 1
        
    def record_timeout(self):
        self.total_requests += 1
        self.timeout_requests += 1
        
    def get_health_status(self) -> dict:
        """Calculate health status"""
        if self.total_requests == 0:
            success_rate = 100
            timeout_rate = 0
        else:
            success_rate = (self.successful_requests / self.total_requests) * 100
            timeout_rate = (self.timeout_requests / self.total_requests) * 100
        
        # Determine status
        if success_rate >= 95 and timeout_rate < 5:
            status = 'healthy'
        elif success_rate >= 80 and timeout_rate < 15:
            status = 'degraded'
        else:
            status = 'unhealthy'
        
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        return {
            'status': status,
            'success_rate': round(success_rate, 1),
            'timeout_rate': round(timeout_rate, 1),
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'timeout_requests': self.timeout_requests,
            'uptime_seconds': int(uptime),
            'avg_response_time_ms': 0,  # Implement if needed
            'cache_hit_rate': 0  # Implement if needed
        }


# Initialize global health monitor
health_monitor = HealthMonitor()
async def detect_address_type_with_monitoring(address: str, chain: str) -> tuple:
    """Wrapper that tracks health metrics"""
    try:
        result = await detect_address_type(address, chain)
        health_monitor.record_success()
        return result
        
    except asyncio.TimeoutError:
        health_monitor.record_timeout()
        logger.warning(f"Timeout detecting type for {address[:8]}...")
        return ('wallet', {'confidence': 'low', 'error': 'timeout'})
        
    except Exception as e:
        health_monitor.record_failure()
        logger.error(f"Error detecting type: {e}")
        return ('wallet', {'confidence': 'low', 'error': str(e)})

# Compiled regex patterns (better performance)
EVM_ADDRESS_PATTERN = re.compile(r'^0x[a-fA-F0-9]{40}$')
SOLANA_ADDRESS_PATTERN = re.compile(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$')

# ============================================================================
# SUBSCRIPTION TIERS
# ============================================================================
def get_tier_config(tier: str) -> Dict:
    """
    Get tier configuration from SubscriptionManager
    Replaces: SUBSCRIPTION_TIERS[tier]
    """
    return sub_manager.tiers.get(tier, sub_manager.tiers['free'])


def get_tier_display_name(tier: str) -> str:
    """
    Get formatted display name
    Replaces: SUBSCRIPTION_TIERS[tier]['name']
    """
    tier_config = get_tier_config(tier)
    return tier_config['name']


def get_tier_emoji(tier: str) -> str:
    """Get emoji for tier"""
    if 'premium' in tier:
        return '💎'
    elif 'basic' in tier:
        return '💰'
    else:
        return '📊'


def check_feature_access(user_id: int, feature: str) -> bool:
    """
    Check if user has feature access
    Replaces: manual feature checking
    """
    return sub_manager.check_feature_access(user_id, feature)


def check_chain_access(user_id: int, chain: str) -> bool:
    """
    Check if user can access a chain
    Replaces: manual chain checking
    """
    return sub_manager.check_chain_access(user_id, chain)
# ============================================================================
# UTILITY FUNCTIONS (OPTIMIZED)
# ============================================================================

def is_valid_address(text: str) -> bool:
    """
    Optimized address validation using compiled regex and caching
    """
    text = text.strip()
    
    # Check cache first
    if text in ADDRESS_CACHE:
        return ADDRESS_CACHE[text]
    
    # Fast length check before regex
    length = len(text)
    
    # EVM validation
    if length == 42:
        result = bool(EVM_ADDRESS_PATTERN.match(text))
        ADDRESS_CACHE[text] = result
        return result
    
    # Solana validation
    if 32 <= length <= 44:
        result = bool(SOLANA_ADDRESS_PATTERN.match(text))
        ADDRESS_CACHE[text] = result
        return result
    
    ADDRESS_CACHE[text] = False
    return False

@lru_cache(maxsize=500)
def detect_chain(address: str) -> str:
    """
    ⭐ FIXED: Better chain detection
    Note: This only does basic detection. Actual chain is determined by user input or context.
    """
    address = address.strip()
    
    # Solana addresses (base58, 32-44 chars)
    if 32 <= len(address) <= 44 and not address.startswith('0x'):
        return 'solana'
    
    # EVM addresses (0x + 40 hex chars)
    if len(address) == 42 and address.startswith('0x'):
        # Default to ethereum, but this should be overridden by context
        return 'ethereum'
    
    # Fallback
    return 'ethereum'
async def detect_address_type(address: str, chain: str) -> Tuple[str, Dict]:
    """
    ⭐ UPDATED: Pass chain to EVM checker for better RPC selection
    """
    
    normalized_chain = normalize_chain_name(chain)
    
    cache_key = f"{normalized_chain}:{address}"
    
    if cache_key in ADDRESS_TYPE_CACHE:
        logger.debug(f"🎯 Cache hit for address type: {address[:8]}...")
        return ADDRESS_TYPE_CACHE[cache_key]
    
    logger.info(f"🔍 Detecting type for {normalized_chain} address: {address[:8]}...")
    
    try:
        session = await session_factory.get_session(normalized_chain)
        
        if normalized_chain == 'solana':
            result = await check_solana_concurrent(address, session)
        else:
            # ⭐ FIX: Pass chain to EVM checker
            result = await check_evm_concurrent(address, session, normalized_chain)
        
        ADDRESS_TYPE_CACHE[cache_key] = result
        return result
        
    except Exception as e:
        logger.error(f"Address type detection failed: {e}")
        return ('wallet', {'confidence': 'low', 'error': str(e)})

async def check_solana_concurrent(address: str, session: aiohttp.ClientSession) -> tuple:
    """Non-blocking Solana check using shared session"""
    rpc_url = "https://api.mainnet-beta.solana.com"
    
    if solana_analyzer.helius_api_key:
        rpc_url = f"https://mainnet.helius-rpc.com/?api-key={solana_analyzer.helius_api_key}"
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenSupply",
        "params": [address]
    }
    
    try:
        async with session.post(
            rpc_url, 
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            if response.status == 200:
                data = await response.json()
                
                if 'result' in data and data['result'] and 'value' in data['result']:
                    supply_info = data['result']['value']
                    return ('token', {
                        'supply': supply_info.get('uiAmount'),
                        'decimals': supply_info.get('decimals'),
                        'confidence': 'high'
                    })
            
            return ('wallet', {'confidence': 'high'})
            
    except asyncio.TimeoutError:
        return ('wallet', {'confidence': 'medium', 'reason': 'timeout'})
    except Exception as e:
        return ('wallet', {'confidence': 'low', 'error': str(e)})


async def check_evm_concurrent(address: str, session: aiohttp.ClientSession) -> tuple:
    """Non-blocking EVM check with parallel RPC attempts"""
    
    
    rpc_urls = [
        "https://eth.llamarpc.com",
        "https://rpc.ankr.com/eth",
        "https://ethereum.publicnode.com"
    ]
    
    async def try_single_rpc(rpc_url: str):
        try:
            loop = asyncio.get_event_loop()
            
            def sync_check():
                w3 = Web3(Web3.HTTPProvider(
                    rpc_url,
                    request_kwargs={'timeout': 8}
                ))
                
                if not w3.is_connected():
                    return None
                
                checksum_addr = Web3.to_checksum_address(address)
                code = w3.eth.get_code(checksum_addr)
                
                if code == b'' or code == b'\x00':
                    return ('wallet', {'confidence': 'high'})
                
                # Quick ERC20 check
                try:
                    total_supply_selector = '0x18160ddd'
                    result = w3.eth.call({
                        'to': checksum_addr,
                        'data': total_supply_selector
                    }, timeout=5)
                    
                    if result and len(result) > 0:
                        return ('token', {'confidence': 'high'})
                except:
                    pass
                
                return ('contract', {'confidence': 'medium'})
            
            result = await asyncio.wait_for(
                loop.run_in_executor(thread_pool, sync_check),
                timeout=10.0
            )
            return result
            
        except Exception as e:
            logger.debug(f"RPC {rpc_url} failed: {e}")
            return None
    
    # Create tasks
    tasks = [asyncio.create_task(try_single_rpc(url)) for url in rpc_urls]
    
    try:
        # Wait for first successful result
        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                if result:
                    # Cancel remaining tasks
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    return result
            except Exception as e:
                logger.debug(f"RPC attempt failed: {e}")
                continue
        
        # All failed
        return ('wallet', {'confidence': 'low'})
        
    except Exception as e:
        logger.warning(f"All EVM RPCs failed: {e}")
        return ('wallet', {'confidence': 'low'})
    finally:
        # Ensure all tasks are cancelled
        for task in tasks:
            if not task.done():
                task.cancel()
        # Wait for cancellation to complete
        await asyncio.gather(*tasks, return_exceptions=True)

import io
import csv

# ============================================================================
# CSV EXPORT FUNCTIONS
# ============================================================================



async def export_last_cabal_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Export CSV - Premium only with PROPER validation
    """
    if update.callback_query:
        user_id = update.callback_query.from_user.id
        message = update.callback_query.message
    else:
        user_id = update.effective_user.id
        message = update.message
    
    # ⭐ STEP 1: Check premium access
    subscription = db.get_user_subscription(user_id)
    is_premium = 'premium' in subscription['tier']
    
    if not is_premium:
        tier_config = get_tier_config(subscription['tier'])
        await message.reply_text(
            f"🔒 <b>Premium Feature Only</b>\n\n"
            f"Your plan: {tier_config['name']}\n\n"
            f"CSV export requires Premium subscription.\n\n"
            f"Features included:\n"
            f"• Unlimited analyses\n"
            f"• Complete trader data export\n"
            f"• Advanced cabal detection\n"
            f"• All chains supported\n\n"
            f"Upgrade now: /subscription",
            parse_mode=ParseMode.HTML
        )
        return
    
    # ⭐ STEP 2: Check for stored trader data
    all_traders = context.user_data.get('all_traders')
    
    if not all_traders:
        await message.reply_text(
            "❌ <b>No Data Found</b>\n\n"
            "Please analyze a token first using token analysis.\n\n"
            "<b>How to use:</b>\n"
            "1. Paste any token contract address\n"
            "2. Wait for analysis to complete\n"
            "3. Use /export_csv to download data\n\n"
            "<b>Example token:</b>\n"
            "<code>DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # ⭐ STEP 3: Generate CSV
    try:
        await message.reply_text(
            "📊 <b>Generating CSV Export...</b>\n\n"
            "⏳ Processing trader data...",
            parse_mode=ParseMode.HTML
        )
        
        # Get stored data
        cabal_result = context.user_data.get('last_cabal_result', {})
        token_address = context.user_data.get('last_token_address', 'Unknown')
        chain = context.user_data.get('last_chain', 'Unknown')
        
        # ⭐ CRITICAL: Ensure all_traders is in cabal_result
        cabal_result['all_traders'] = all_traders
        
        # Format data for CSV
        csv_data = format_cabal_data_for_csv(cabal_result, token_address, chain)
        
        if not csv_data:
            await message.reply_text(
                "❌ <b>Export Failed</b>\n\n"
                "No valid data to export. This shouldn't happen!\n"
                "Please analyze a token again.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Generate CSV file
        csv_file = generate_csv_file(csv_data)
        
        if not csv_file:
            await message.reply_text(
                "❌ <b>Export Failed</b>\n\n"
                "Could not generate CSV file. Please try again.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Count different trader types
        cabal_count = len(cabal_result.get('cabals', []))
        cabal_members = sum(1 for row in csv_data if 'Cabal_' in row['cabal_group'])
        suspicious = sum(1 for row in csv_data if row['cabal_group'] == 'Suspicious_Individual')
        normal = sum(1 for row in csv_data if row['cabal_group'] == 'Normal_Trader')
        
        # ⭐ Enhanced caption with data breakdown
        caption = f"""✅ <b>CSV Export Complete</b>

<b>📊 Export Details:</b>
• Token: <code>{token_address[:10]}...{token_address[-8:]}</code>
• Chain: {chain.upper()}
• Total Traders: {len(csv_data)}
• Cabals Detected: {cabal_count if cabal_count > 0 else 'None'}
• Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

<b>📁 File Contents:</b>
• Complete wallet addresses
• Win rates & profit/loss data
• Realized + unrealized PNL
• 7-day performance metrics
• Top 3 tokens per trader
• Trading volume & statistics
• Risk levels & classifications

<b>📋 Trader Breakdown:</b>
• <b>Cabal Members:</b> {cabal_members} wallets
• <b>Suspicious:</b> {suspicious} wallets
• <b>Normal Traders:</b> {normal} wallets

<b>💡 Use this data for:</b>
• Excel/Google Sheets analysis
• Data visualization & charting
• Risk assessment & filtering
• Investment decision making
• Identifying top performers
• Tracking trading patterns

<i>Premium feature - Thank you for your support! 💎</i>"""
        
        # Send file
        await message.reply_document(
            document=csv_file,
            caption=caption,
            parse_mode=ParseMode.HTML,
            filename=csv_file.name
        )
        
        logger.info(f"✅ CSV exported for user {user_id}: {len(csv_data)} traders")
        
    except Exception as e:
        logger.error(f"❌ CSV export error: {e}", exc_info=True)
        await message.reply_text(
            f"❌ <b>Export Error</b>\n\n"
            f"An error occurred while generating the CSV:\n"
            f"{html_escape(str(e)[:200])}\n\n"
            "Please try again or contact support.",
            parse_mode=ParseMode.HTML
        )

async def debug_export_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Debug command to check CSV export status (premium only)
    Usage: /debug_export
    """
    user_id = update.effective_user.id
    subscription = db.get_user_subscription(user_id)
    
    is_premium = 'premium' in subscription['tier']
    has_traders = bool(context.user_data.get('all_traders'))
    has_cabal = bool(context.user_data.get('last_cabal_result'))
    has_token = bool(context.user_data.get('last_token_address'))
    
    trader_count = len(context.user_data.get('all_traders', []))
    
    status_text = f"""🔍 <b>CSV Export Debug Status</b>

<b>User Status:</b>
• Premium: {'✅ Yes' if is_premium else '❌ No'}
• Tier: {subscription['tier']}

<b>Data Availability:</b>
• Trader Data: {'✅ Available' if has_traders else '❌ Missing'}
• Trader Count: {trader_count}
• Cabal Results: {'✅ Available' if has_cabal else '❌ Missing'}
• Token Address: {'✅ Available' if has_token else '❌ Missing'}

<b>Can Export CSV:</b> {'✅ YES' if (is_premium and has_traders) else '❌ NO'}

<b>Next Steps:</b>
"""
    
    if not is_premium:
        status_text += "→ Upgrade to Premium: /subscription\n"
    if not has_traders:
        status_text += "→ Analyze a token to generate data\n"
    if is_premium and has_traders:
        status_text += "→ Use /export_csv to download!\n"
    
    await update.message.reply_text(status_text, parse_mode=ParseMode.HTML)


def format_top_tokens(tokens: List[Dict]) -> str:
    """
    ✅ IMPROVED: Better token display with smart filtering
    - Shows winners first, then losers
    - Clearer profit/loss indicators
    - Better visual hierarchy
    - Filters out meaningless $0 positions
    """
    if not tokens:
        return "  <i>No token data available</i>"
    
    # Separate winners and losers
    winners = []
    losers = []
    
    for token in tokens:
        pnl = token.get('pnl', 0)
        current_value = token.get('current_value', 0)
        trades = token.get('trades', 0)
        
        # ⭐ STRICT FILTERING: Skip truly meaningless tokens
        # Must have either:
        # 1. Meaningful PNL (> $0.10 or < -$0.10)
        # 2. Meaningful current value (> $0.10)
        # 3. Multiple trades (suggests real activity)
        is_meaningful = (
            abs(pnl) > 0.10 or 
            current_value > 0.10 or 
            trades >= 2
        )
        
        if not is_meaningful:
            continue
        
        # Categorize
        if pnl > 0 or (pnl == 0 and current_value > 10):
            winners.append(token)
        else:
            losers.append(token)
    
    if not winners and not losers:
        return "  <i>No significant positions found</i>"
    
    # Sort winners by PNL (highest first)
    winners.sort(key=lambda x: x.get('pnl', 0), reverse=True)
    
    # Sort losers by PNL (least negative first, but still negative)
    losers.sort(key=lambda x: x.get('pnl', 0), reverse=True)
    
    lines = []
    
    # Display winners first
    if winners:
        lines.append("  <b>✅ Profitable Positions:</b>")
        for i, token in enumerate(winners[:3], 1):
            symbol = html_escape(token.get('symbol', 'Unknown'))[:15]
            pnl = token.get('pnl', 0)
            current_value = token.get('current_value', 0)
            trades = token.get('trades', 0)
            win_rate = token.get('win_rate', 0)
            
            # Status indicator
            if current_value > 1:
                status = f"💎 Holding {format_number(current_value)}"
            else:
                status = "🚪 Exited"
            
            line = f"  {i}. <b>{symbol}</b>: <b>{format_number(pnl)}</b>"
            
            if win_rate > 0:
                line += f" • WR {win_rate:.0f}%"
            
            if trades > 0:
                line += f" • {trades} trade{'s' if trades != 1 else ''}"
            
            lines.append(line)
            lines.append(f"     {status}")
        lines.append("")
    
    # Display losers (up to 2 most significant losses)
    if losers:
        lines.append("  <b>❌ Losing Positions:</b>")
        for i, token in enumerate(losers[:2], 1):
            symbol = html_escape(token.get('symbol', 'Unknown'))[:15]
            pnl = token.get('pnl', 0)
            current_value = token.get('current_value', 0)
            trades = token.get('trades', 0)
            win_rate = token.get('win_rate', 0)
            
            # Status indicator
            if current_value > 1:
                status = f"📉 Still holding {format_number(current_value)}"
            else:
                status = "🚪 Exited"
            
            line = f"  {i}. <b>{symbol}</b>: <b>{format_number(pnl)}</b>"
            
            if win_rate > 0:
                line += f" • WR {win_rate:.0f}%"
            
            if trades > 0:
                line += f" • {trades} trade{'s' if trades != 1 else ''}"
            
            lines.append(line)
            lines.append(f"     {status}")
        lines.append("")
    
    # Summary
    total_pnl = sum(t.get('pnl', 0) for t in (winners + losers))
    total_value = sum(t.get('current_value', 0) for t in (winners + losers))
    
    lines.append("  <b>💰 Portfolio Summary:</b>")
    lines.append(f"  • Realized P&L: <b>{format_number(total_pnl)}</b>")
    if total_value > 1:
        lines.append(f"  • Current Holdings: <b>{format_number(total_value)}</b>")
    lines.append(f"  • Winners: {len(winners)} | Losers: {len(losers)}")
    
    return '\n'.join(lines)

def format_wallet_analysis(result: Dict, analysis_time: float = 0) -> str:
    """✅ Enhanced wallet analysis with improved token display and timeframe info"""
    
    addr = result.get('address', 'Unknown')
    addr_short = f"{addr[:8]}...{addr[-6:]}"
    chain = result.get('chain', 'Unknown').upper()
    
    # ⭐ Extract timeframe info
    timeframe_days = result.get('timeframe_days', 7)
    data_source = result.get('data_source', '')
    is_recent_activity = result.get('has_recent_activity', True)
    
    if not result.get('success', True):
        return f"""❌ <b>Analysis Failed</b>

<b>Address:</b> <code>{addr_short}</code>
<b>Chain:</b> {chain}
<b>Error:</b> {result.get('message', 'Unknown error')}"""
    
    # Check for empty wallet
    if result.get('total_trades', 0) == 0:
        return f"""📊 <b>Empty Wallet</b>

<b>Address:</b> <code>{addr_short}</code>
<b>Chain:</b> {chain}

No trading activity detected on this chain."""
    
    # Extract metrics
    win_rate = result.get('win_rate', 0)
    total_profit = result.get('total_profit', 0)
    realized = result.get('realized_profit', 0)
    unrealized = result.get('unrealized_profit', 0)
    roi = result.get('roi', 0)
    total_trades = result.get('total_trades', 0)
    winning_tokens = result.get('winning_tokens', 0)
    losing_tokens = result.get('losing_tokens', 0)
    
    # Win rate emoji and label
    if win_rate >= 70:
        wr_emoji = "🔥"
        wr_label = "EXCELLENT"
    elif win_rate >= 60:
        wr_emoji = "✅"
        wr_label = "GOOD"
    elif win_rate >= 50:
        wr_emoji = "⚠️"
        wr_label = "AVERAGE"
    else:
        wr_emoji = "❌"
        wr_label = "BELOW AVERAGE"
    
    # Build response
    # Build response
    response = f"""🎯 <b>Wallet Analysis Complete</b>

<b>👤 Address:</b> <code>{addr_short}</code>
<b>⛓ Chain:</b> {chain}
<b>📅 Timeframe:</b> {timeframe_days} days{' ⚠️ (No recent activity)' if not is_recent_activity else ''}

<b>📊 Performance Rating</b>
Win Rate: {wr_emoji} <b>{win_rate:.1f}%</b> ({wr_label})

<b>💰 Profit & Loss:</b>
- Total P&L: <b>{format_number(total_profit)}</b>
- Realized: {format_number(realized)} 💵
- Unrealized: {format_number(unrealized)} 📊
- ROI: <b>{roi:+.1f}%</b>

<b>📈 Trading Activity:</b>
- Total Trades: {total_trades:,}
- Tokens Traded: {result.get('total_tokens_traded', 0)}
- Winners: {winning_tokens} ✅ | Losers: {losing_tokens} ❌
- Volume: {format_number(result.get('total_volume', 0))}
"""
    
    # 7-day performance
    profit_7d = result.get('profit_7d', 0)
    if profit_7d != 0:
        emoji_7d = "📈" if profit_7d > 0 else "📉"
        response += f"""
<b>📅 Recent Performance (7d):</b>
- Profit: <b>{format_number(profit_7d)}</b> {emoji_7d}
- Change: {result.get('profit_7d_pct', 0):+.1f}%
- Trades: {result.get('trades_7d', 0)}
"""
    
    # ⭐ NEW: Use improved token formatter
    top_tokens = result.get('top_tokens', [])
    if top_tokens:
        response += "\n<b>🏆 Portfolio Breakdown:</b>\n\n"
        response += format_top_tokens(top_tokens)
    
    # Activity footer
    response += f"""

<b>📅 Activity:</b>
- Last Trade: {result.get('last_trade_date', 'N/A')}
- Active Days: {result.get('active_days', 0)}

<b>⏱️ Analysis Time:</b> {analysis_time:.1f}s
<i>📡 Data: Alchemy + DexScreener</i>"""
    
    return response



async def diagnose_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Diagnostic command to debug wallet analysis issues
    Usage: /diagnose <wallet_address> [chain]
    
    Now supports:
    - Solana wallets (enhanced multi-source price fetching)
    - EVM wallets (Moralis-based)
    """
    
    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b> /diagnose &lt;address&gt; [chain]\n\n"
            "<b>Examples:</b>\n"
            "<code>/diagnose 0x05BE1d...829A60 ethereum</code>\n"
            "<code>/diagnose 7xKXt...9dKRq solana</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    address = context.args[0].strip()
    chain = context.args[1].lower() if len(context.args) > 1 else detect_chain(address)
    
    processing_msg = await update.message.reply_text(
        f"🔍 <b>Running Enhanced Diagnostic...</b>\n\n"
        f"Address: <code>{address[:10]}...{address[-8:]}</code>\n"
        f"Chain: {chain.upper()}\n\n"
        f"⏳ Please wait...",
        parse_mode=ParseMode.HTML
    )
    
    try:
        if chain == 'solana':
            # Use Enhanced Solana analyzer with multi-source price fetching
            
            
            debug_analyzer = EnhancedSolanaAnalyzer(
                helius_api_key=HELIUS_API_KEY,
                birdeye_api_key=getattr(Config, 'BIRDEYE_API_KEY', None)
            )
            
            # Run enhanced analysis
            result = await debug_analyzer.analyze_wallet(address)
            
            # Close analyzer
            await debug_analyzer.close()
            
        else:
            # Use Moralis for EVM chains
            from features.moralis_evm_analyzer import MoralisEVMAnalyzerDebug
            
            debug_analyzer = MoralisEVMAnalyzerDebug(Config.MORALIS_API_KEY)
            result = await debug_analyzer.analyze_wallet(address, chain)
    except Exception as e:
        await processing_msg.edit_text(
            f"❌ <b>Diagnostic Failed</b>\n\n"
            f"Error: {html_escape(str(e)[:200])}",
            parse_mode=ParseMode.HTML
        )
        return

async def test_moralis_connection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Test Moralis API connection
    Usage: /test_moralis
    """
    
    processing_msg = await update.message.reply_text(
        "🔍 <b>Testing Moralis API Connection...</b>",
        parse_mode=ParseMode.HTML
    )
    
    try:
        # Test API key validity
        url = "https://deep-index.moralis.io/api/v2.2/erc20/metadata"
        
        headers = {
            'accept': 'application/json',
            'X-API-Key': Config.MORALIS_API_KEY
        }
        
        # Test with USDC address
        params = {
            'chain': 'eth',
            'addresses': ['0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48']
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                
                status = response.status
                
                if status == 200:
                    data = await response.json()
                    
                    report = f"""✅ <b>Moralis API Connection Test</b>

<b>✅ Status:</b> WORKING
<b>📊 HTTP Status:</b> 200 OK
<b>🔑 API Key:</b> Valid

<b>Test Results:</b>
• Endpoint: Token Metadata
• Chain: Ethereum
• Response: Success

<b>💡 Your Moralis API is configured correctly!</b>

If wallets still show no data, they likely have no trading history.

<b>Test Wallets:</b>
• Vitalik: <code>0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045</code>
• Known Trader: <code>0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb</code>"""
                    
                elif status == 401:
                    report = f"""❌ <b>Moralis API Connection Test</b>

<b>❌ Status:</b> AUTHENTICATION FAILED
<b>📊 HTTP Status:</b> 401 Unauthorized
<b>🔑 API Key:</b> Invalid or missing

<b>⚠️ Problem:</b>
Your Moralis API key is not valid.

<b>🔧 Fix:</b>
1. Go to https://moralis.io
2. Sign up/login
3. Get your API key
4. Update Config.MORALIS_API_KEY
5. Restart bot

<b>Current Key:</b> {Config.MORALIS_API_KEY[:20]}..."""
                
                else:
                    error_text = await response.text()
                    report = f"""⚠️ <b>Moralis API Connection Test</b>

<b>⚠️ Status:</b> ERROR
<b>📊 HTTP Status:</b> {status}

<b>Error:</b>
{html_escape(error_text[:300])}

<b>🔧 Troubleshooting:</b>
• Check API key permissions
• Verify account status
• Check rate limits"""
                
                await processing_msg.edit_text(report, parse_mode=ParseMode.HTML)
        
    except asyncio.TimeoutError:
        await processing_msg.edit_text(
            "❌ <b>Connection Timeout</b>\n\n"
            "Could not reach Moralis API.\n\n"
            "<b>Possible causes:</b>\n"
            "• Network connectivity issue\n"
            "• Firewall blocking requests\n"
            "• Moralis API downtime",
            parse_mode=ParseMode.HTML
        )
    
    except Exception as e:
        await processing_msg.edit_text(
            f"❌ <b>Test Failed</b>\n\n"
            f"Error: {html_escape(str(e)[:200])}",
            parse_mode=ParseMode.HTML
        )


# Add these commands to your bot's main() function:
def format_number(num: float, decimals: int = 2) -> str:
    """
    Enhanced number formatting with better sign handling
    """
    if num is None or num == 0:
        return "$0.00"
    
    sign = '+' if num > 0 else ''
    
    abs_num = abs(num)
    
    if abs_num >= 1_000_000:
        formatted = f"{sign}${num/1_000_000:.1f}M"
    elif abs_num >= 1_000:
        formatted = f"{sign}${num/1_000:.1f}K"
    else:
        formatted = f"{sign}${num:,.{decimals}f}"
    
    return formatted

# ============================================================================
# Additional Helper: Token Data Validation
# ============================================================================

def validate_and_clean_token_data(tokens: List[Dict]) -> List[Dict]:
    """
    Validate and clean token data before display
    Ensures all required fields exist and have valid values
    """
    cleaned_tokens = []
    
    for token in tokens:
        # Ensure required fields exist
        cleaned_token = {
            'symbol': token.get('symbol', 'UNKNOWN'),
            'address': token.get('address', token.get('mint', '')),
            'current_value': float(token.get('current_value', 0)),
            'price_change_24h': float(token.get('price_change_24h', 0)),
            'trades': int(token.get('trades', 0)),
            'pnl': float(token.get('pnl', 0)),
            'balance': float(token.get('balance', 0)),
        }
        
        # Only add if token has some meaningful data
        if cleaned_token['current_value'] > 0 or cleaned_token['trades'] > 0 or abs(cleaned_token['pnl']) > 0:
            cleaned_tokens.append(cleaned_token)
    
    return cleaned_tokens

# ============================================================================
# Usage Example in Your Bot
# ============================================================================

async def perform_wallet_analysis_example(address: str, chain: str):
    """
    Example of how to use the enhanced formatting
    """
    try:
        # Get analysis result
        if chain == 'solana':
            result = await solana_analyzer.analyze_wallet(address)
        else:
            result = await evm_analyzer.analyze_wallet(address, chain)
        
        # Clean token data if present
        if result.get('top_tokens'):
            result['top_tokens'] = validate_and_clean_token_data(result['top_tokens'])
        
        # Format and return
        analysis_time = 6.3  # Your actual timing
        formatted_response = format_wallet_analysis(result, analysis_time)
        
        return formatted_response
        
    except Exception as e:
        logger.error(f"Analysis error: {e}", exc_info=True)
        return format_wallet_analysis({
            'success': False,
            'message': str(e),
            'address': address,
            'chain': chain
        })

@lru_cache(maxsize=100)
def get_wallet_rating(win_rate: float) -> str:
    """Cached wallet rating"""
    if win_rate >= 70:
        return "🔥 EXCELLENT - Elite Trader"
    elif win_rate >= 60:
        return "✅ GOOD - Solid Performance"
    elif win_rate >= 50:
        return "⚠️ AVERAGE - Room for Improvement"
    elif win_rate >= 40:
        return "⚡ BELOW AVERAGE - Risky"
    else:
        return "❌ POOR - High Risk"

def format_token_analysis_error(result: Dict, address: str, chain: str) -> str:
    """Format token analysis error message"""
    debug = result.get('debug_info', {})
    error_msg = result.get('message', 'Unknown error')
    
    return f"""❌ <b>Token Analysis Failed</b>

📍 <b>Token:</b> <code>{address[:8]}...{address[-6:]}</code>
⛓ <b>Chain:</b> {chain.upper()}

<b>Error:</b> {html_escape(error_msg)}

<b>💡 Possible Solutions:</b>
• Verify the token address is correct
• Ensure token has trading history
• Try a different token
• Contact support if issue persists"""

# ============================================================================
# COMMAND HANDLERS
# ============================================================================

def get_tier_display_name(tier: str) -> str:
    """Get formatted display name for subscription tier"""
    tier_names = {
        'free': '📊 FREE',
        'basic_weekly': '💰 BASIC WEEKLY',
        'basic_monthly': '💰 BASIC MONTHLY', 
        'premium_weekly': '💎 PREMIUM WEEKLY',
        'premium_monthly': '💎 PREMIUM MONTHLY'
    }
    return tier_names.get(tier, tier.upper())


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command with SubscriptionManager integration"""
    user = update.effective_user
    
    db.register_user(user.id, user.username, user.first_name)
    subscription = db.get_user_subscription(user.id)
    
    # Get tier info from SubscriptionManager
    tier = subscription['tier']
    tier_config = get_tier_config(tier)
    tier_emoji = get_tier_emoji(tier)
    tier_name = tier_config['name']
    
    # Calculate remaining analyses
    if subscription['daily_limit'] == -1:
        daily_limit_str = '∞ Unlimited'
        remaining = '∞'
        usage_bar = '100%'
    else:
        daily_limit_str = str(subscription['daily_limit'])
        remaining = subscription['daily_limit'] - subscription['daily_analyses']
        usage_percent = (subscription['daily_analyses'] / subscription['daily_limit'] * 100) if subscription['daily_limit'] > 0 else 0
        filled = int(usage_percent / 10)
        usage_bar = '█' * filled + '░' * (10 - filled) + f' {usage_percent:.0f}%'
    
    # Premium status badge - using tier config
    if 'premium' in tier:
        status_badge = '👑 <b>PREMIUM MEMBER</b> 👑'
        feature_highlight = """
<b>🌟 Your Premium Benefits:</b>
✅ Unlimited analyses per day
✅ Token trader discovery
✅ Cabal detection enabled
✅ All chains supported
✅ Priority support
✅ Advanced analytics"""
    elif 'basic' in tier:
        status_badge = '💰 <b>BASIC MEMBER</b>'
        has_token_analysis = 'extended_features' in tier_config['features']
        feature_highlight = f"""
<b>✨ Your Basic Benefits:</b>
✅ {subscription['daily_limit']} analyses per day
✅ Wallet analysis
✅ Multi-chain support
{'✅ Token trader analysis' if has_token_analysis else '❌ Token analysis (upgrade)'}
{'✅ Cabal detection' if has_token_analysis else '❌ Cabal detection (upgrade)'}"""
    else:
        status_badge = '📊 <b>FREE MEMBER</b>'
        feature_highlight = """
<b>🎯 Free Tier Features:</b>
✅ 5 analyses per day
✅ Basic wallet analysis
✅ Ethereum & BSC chains
❌ Token trader analysis
❌ Cabal detection
❌ Advanced features"""
    
    # Expiry info
    if subscription.get('expires_at'):
        expires = subscription['expires_at']
        if isinstance(expires, str):
            expires = datetime.fromisoformat(expires)
        days_left = (expires - datetime.now()).days
        
        if days_left > 0:
            expiry_str = f"⏰ Expires in: <b>{days_left} days</b>"
            if days_left <= 3:
                expiry_str += " ⚠️ <i>Renew soon!</i>"
        else:
            expiry_str = "⚠️ <b>EXPIRED</b> - Please renew"
    else:
        expiry_str = "♾️ No expiration"
    
    welcome_message = f"""🤖 <b>Welcome to CoinWinRBot!</b> 🚀

Hello {html_escape(user.first_name)}! 👋

{status_badge}

<b>📊 Account Status:</b>
{tier_emoji} <b>Plan:</b> {tier_name}
📈 <b>Daily Usage:</b> {subscription['daily_analyses']}/{daily_limit_str}
{usage_bar}
💎 <b>Remaining:</b> {remaining} analyses today
{expiry_str}

{feature_highlight}

<b>🔥 What I Can Do:</b>

<b>1️⃣ Analyze Wallets</b>
   • View win rates, profits, patterns
   • Track successful traders
   • Multi-chain support

<b>2️⃣ Find Token's Top Traders</b> {'✅' if check_feature_access(user.id, 'extended_features') or 'all' in tier_config['features'] else '🔒'}
   • Paste any token contract address
   • Get ranked list of profitable traders
   • Discover who's winning!

<b>3️⃣ Cabal Detection</b> {'✅' if check_feature_access(user.id, 'extended_features') or 'all' in tier_config['features'] else '🔒'}
   • Spot coordinated trading groups
   • Identify pump & dump schemes
   • Protect your investments

<b>⚡ Just Paste Any Address!</b>
• <b>Wallet:</b> <code>0x1234...abcd</code>
• <b>Token CA:</b> <code>HtrmuNs4nESVg5i8g...</code>
• <b>I'll detect it automatically!</b>

<b>💡 Quick Test:</b>
<code>DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263</code>
<i>(Bonk token - tap to copy!)</i>

<b>🎯 Supported Chains:</b>
Ethereum • BSC • Polygon • Arbitrum • Base • Solana

Ready to find alpha? 🚀"""
    
    # Dynamic buttons based on tier
    if 'premium' in tier:
        keyboard = [
            [InlineKeyboardButton("🚀 Analyze Now", callback_data='quick_analyze'),
             InlineKeyboardButton("📊 My Stats", callback_data='stats')],
            [InlineKeyboardButton("👑 Premium Features", callback_data='premium_features'),
             InlineKeyboardButton("❓ Help", callback_data='help')]
        ]
    else:
        keyboard = [
            [InlineKeyboardButton("📈 Try It Now", callback_data='quick_analyze'),
             InlineKeyboardButton("💎 Upgrade", callback_data='view_plans')],
            [InlineKeyboardButton("📊 My Stats", callback_data='stats'),
             InlineKeyboardButton("❓ Help", callback_data='help')]
        ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        welcome_message,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )


async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ⭐ UPDATED: Support /analyze <address> <chain>
    
    Examples:
    /analyze 0x123... ethereum
    /analyze 0x456... bsc
    /analyze 7xKX... solana (auto-detected)
    """
    user_id = update.effective_user.id
    
    can_analyze, msg = sub_manager.check_analysis_limit(user_id)
    if not can_analyze:
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        return
    
    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b> /analyze &lt;address&gt; [chain]\n\n"
            "<b>Examples:</b>\n"
            "<code>/analyze 0x123...abc ethereum</code>\n"
            "<code>/analyze 0x456...def bsc</code>\n"
            "<code>/analyze 7xKX...9dK</code> (Solana auto-detected)\n\n"
            "<b>Supported chains:</b>\n"
            "ethereum, bsc, polygon, arbitrum, optimism, base, avalanche, solana",
            parse_mode=ParseMode.HTML
        )
        return
    
    address = context.args[0].strip()
    specified_chain = context.args[1].lower() if len(context.args) > 1 else None
    
    if not is_valid_address(address):
        await update.message.reply_text("❌ Invalid address", parse_mode=ParseMode.HTML)
        return
    
    allowed, limit_msg = await rate_limiter.check_limit(user_id)
    if not allowed:
        await update.message.reply_text(limit_msg, parse_mode=ParseMode.HTML)
        return
    
    await perform_analysis(update, context, address, user_id, specified_chain)

async def handle_direct_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ⭐ UPDATED: Support "<address> <chain>" format
    
    User can paste: 
    - Just address (will ask for chain if EVM)
    - "address chain" (direct analysis)
    """
    text = update.message.text.strip()
    
    # Check if it's "address chain" format
    parts = text.split()
    
    if len(parts) == 2:
        address, chain = parts[0].strip(), parts[1].strip().lower()
        
        if not is_valid_address(address):
            return
        
        # Validate chain
        if chain not in CHAIN_MAPPING:
            await update.message.reply_text(
                f"❌ <b>Unknown chain:</b> {html_escape(chain)}\n\n"
                f"<b>Supported chains:</b>\n"
                f"ethereum, eth, bsc, binance, polygon, matic, arbitrum, arb, "
                f"optimism, op, base, avalanche, avax, solana, sol",
                parse_mode=ParseMode.HTML
            )
            return
        
        user_id = update.effective_user.id
        can_analyze, msg = sub_manager.check_analysis_limit(user_id)
        if not can_analyze:
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
            return
        
        allowed, limit_msg = await rate_limiter.check_limit(user_id)
        if not allowed:
            await update.message.reply_text(limit_msg, parse_mode=ParseMode.HTML)
            return
        
        await perform_analysis(update, context, address, user_id, chain)
    
    elif len(parts) == 1:
        # Just address - original behavior
        address = parts[0]
        
        if not is_valid_address(address):
            return
        
        user_id = update.effective_user.id
        can_analyze, msg = sub_manager.check_analysis_limit(user_id)
        if not can_analyze:
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
            return
        
        allowed, limit_msg = await rate_limiter.check_limit(user_id)
        if not allowed:
            await update.message.reply_text(limit_msg, parse_mode=ParseMode.HTML)
            return
        
        await perform_analysis(update, context, address, user_id, specified_chain=None)

async def handle_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == 'export_csv':
        await export_csv_with_progress(update, context)
    elif query.data == 'quick_analyze':
        await query.message.reply_text("🎯 Paste an address!", parse_mode=ParseMode.HTML)
    elif query.data.startswith('sub_'):
        await sub_manager.handle_subscription_callback(update, context)

# ============================================================================
# TOKEN METADATA FETCHING (Multiple Sources)
# ============================================================================

async def fetch_token_metadata_enhanced(
    token_address: str, 
    chain: str,
    enhanced_token_fetcher
) -> Optional[Dict]:
    """
    ⭐ COMPLETE REWRITE: Universal token metadata fetcher for ALL chains
    
    Supports:
    - Solana: Birdeye, Jupiter, DexScreener, Helius, Raydium
    - Ethereum: Birdeye, DexScreener, Bitquery, CoinGecko
    - BSC: Birdeye, DexScreener, Bitquery, CoinGecko
    - Polygon: Birdeye, DexScreener, Bitquery, CoinGecko
    - Arbitrum: DexScreener, Bitquery, CoinGecko
    - Optimism: DexScreener, Bitquery, CoinGecko
    - Base: DexScreener, Bitquery, CoinGecko
    - Avalanche: DexScreener, Bitquery, CoinGecko
    
    Args:
        token_address: Token contract address
        chain: Blockchain name
        enhanced_token_fetcher: Instance of EnhancedTokenDataFetcher
        
    Returns:
        Dict with comprehensive token metadata or None
    """
    
    # Normalize chain name
    chain_normalized = normalize_chain_name(chain)
    
    logger.info(f"🔍 Fetching token metadata: {token_address[:8]}... on {chain_normalized}")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 1: Try Enhanced Fetcher (Multi-Source)
    # ═══════════════════════════════════════════════════════════════
    
    if enhanced_token_fetcher:
        try:
            logger.info(f"📊 Attempting enhanced fetch for {chain_normalized}...")
            
            metadata = await asyncio.wait_for(
                enhanced_token_fetcher.fetch_enhanced_metadata(
                    token_address, 
                    chain_normalized
                ),
                timeout=20.0
            )
            
            if metadata:
                logger.info(f"✅ Enhanced data fetched: {metadata.symbol} | Source: {metadata.source}")
                
                # Convert EnhancedTokenMetadata to dict for compatibility
                return {
                    'name': metadata.name,
                    'symbol': metadata.symbol,
                    'logo': metadata.logo_url,
                    'price': metadata.price,
                    'price_change_1h': metadata.price_change_1h,
                    'price_change_24h': metadata.price_change_24h,
                    'price_change_7d': metadata.price_change_7d,
                    'market_cap': metadata.market_cap,
                    'fdv': metadata.fdv,
                    'liquidity': metadata.liquidity,
                    'volume_24h': metadata.volume_24h,
                    'holder_count': metadata.holder_count,
                    'top_10_holder_percent': metadata.top_10_holder_percent,
                    'trades_24h': metadata.trades_24h,
                    'buyers_24h': metadata.buyers_24h,
                    'sellers_24h': metadata.sellers_24h,
                    'buy_sell_ratio': metadata.buy_sell_ratio,
                    'is_verified': metadata.is_verified,
                    'is_audited': metadata.is_audited,
                    'honeypot_risk': metadata.honeypot_risk,
                    'risk_level': metadata.risk_level.value,
                    'risk_score': metadata.risk_score,
                    'website': metadata.website,
                    'twitter': metadata.twitter,
                    'telegram': metadata.telegram,
                    'source': metadata.source,
                    'enhanced': True,  # Flag for enhanced display
                    'success': True
                }
                
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ Enhanced fetch timeout for {token_address[:8]}")
        except Exception as e:
            logger.warning(f"⚠️ Enhanced fetch error: {e}")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 2: Fallback to Basic Chain-Specific Fetchers
    # ═══════════════════════════════════════════════════════════════
    
    logger.info(f"🔄 Using basic metadata fetch for {token_address[:8]} on {chain_normalized}")
    
    try:
        if chain_normalized == 'solana':
            # Solana-specific fallbacks
            metadata = await _fetch_solana_metadata_basic(token_address)
            if metadata:
                return metadata
        else:
            # EVM chains: Universal DexScreener
            metadata = await _fetch_evm_metadata_basic(token_address, chain_normalized)
            if metadata:
                return metadata
        
        logger.warning(f"⚠️ No metadata found for {token_address[:8]} on {chain_normalized}")
        return None
        
    except Exception as e:
        logger.error(f"❌ Metadata fetch error: {e}")
        return None

async def _fetch_solana_metadata_basic(token_address: str) -> Optional[Dict]:
    """Basic Solana metadata from Solscan/DexScreener"""
    try:
        # Try DexScreener first (most reliable)
        session = await session_factory.get_session('api')
        
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        timeout = aiohttp.ClientTimeout(total=8.0)
        
        async with session.get(url, timeout=timeout) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = data.get('pairs', [])
                
                if pairs:
                    # Get most liquid Solana pair
                    solana_pairs = [p for p in pairs if p.get('chainId') == 'solana']
                    pair = max(solana_pairs or pairs, 
                              key=lambda p: float(p.get('liquidity', {}).get('usd', 0) or 0))
                    
                    base = pair.get('baseToken', {})
                    
                    return {
                        'name': base.get('name', ''),
                        'symbol': base.get('symbol', ''),
                        'logo': pair.get('info', {}).get('imageUrl'),
                        'price': float(pair.get('priceUsd', 0) or 0),
                        'price_change_24h': float(pair.get('priceChange', {}).get('h24', 0) or 0),
                        'liquidity': float(pair.get('liquidity', {}).get('usd', 0) or 0),
                        'volume_24h': float(pair.get('volume', {}).get('h24', 0) or 0),
                        'fdv': float(pair.get('fdv', 0) or 0),
                        'source': 'DexScreener',
                        'enhanced': False,
                        'success': True
                    }
        
        return None
        
    except Exception as e:
        logger.debug(f"Solana basic fetch error: {e}")
        return None

def normalize_chain_name(chain: str) -> str:
    """
    ✅ FIXED: Better normalization with logging
    """
    if not chain:
        return 'ethereum'
    
    # Clean input
    chain_lower = chain.lower().strip()
    chain_lower = chain_lower.replace('-', ' ').replace('_', ' ')
    
    # Direct mapping
    if chain_lower in CHAIN_MAPPING:
        return CHAIN_MAPPING[chain_lower]
    
    # Check for partial matches
    for key, value in CHAIN_MAPPING.items():
        if key in chain_lower or chain_lower in key:
            logger.info(f"✅ Partial match: '{chain}' → '{value}'")
            return value
    
    # Unknown chain
    logger.warning(f"⚠️ Unknown chain '{chain}', using as-is")
    return chain_lower

async def _fetch_solscan_metadata(token_address: str) -> Optional[Dict]:
    """
    Fetch token metadata from Solscan Pro API
    Free tier available at: https://solscan.io/
    
    Endpoint: GET https://pro-api.solscan.io/v2.0/token/meta
    """
    try:
        session = await session_factory.get_session('api')
        
        # Public API endpoint (no key needed for basic metadata)
        url = "https://public-api.solscan.io/token/meta"
        
        params = {'token': token_address}
        
        timeout = aiohttp.ClientTimeout(total=5.0)
        
        async with session.get(url, params=params, timeout=timeout) as resp:
            if resp.status != 200:
                logger.debug(f"Solscan returned {resp.status} for {token_address[:8]}")
                return None
            
            data = await resp.json()
            
            # Check if response has data
            if not data or not isinstance(data, dict):
                return None
            
            # Extract metadata
            name = data.get('name', '')
            symbol = data.get('symbol', '')
            icon = data.get('icon', '')
            decimals = data.get('decimals', 9)
            
            # Additional info if available
            price = data.get('price')
            market_cap = data.get('market_cap')
            volume_24h = data.get('volume_24h')
            holder_count = data.get('holder')
            
            if not name and not symbol:
                return None
            
            logger.info(f"✅ Solscan: Found {symbol or name}")
            
            return {
                'name': name,
                'symbol': symbol,
                'logo': icon,
                'decimals': decimals,
                'price': price,
                'market_cap': market_cap,
                'volume_24h': volume_24h,
                'holder_count': holder_count,
                'source': 'Solscan'
            }
    
    except asyncio.TimeoutError:
        logger.debug(f"Solscan timeout for {token_address[:8]}")
        return None
    except Exception as e:
        logger.debug(f"Solscan error: {e}")
        return None

def format_multi_source_stats(source_stats: dict, total_tokens: int) -> str:
    """
    Format multi-source statistics for display
    
    Args:
        source_stats: Statistics from FastMultiSourcePriceFetcher.get_stats()
        total_tokens: Total number of unique tokens analyzed
    
    Returns:
        Formatted string for display
    """
    
    lines = []
    
    # ═══════════════════════════════════════════════════════════
    # CACHE STATISTICS
    # ═══════════════════════════════════════════════════════════
    
    cache_stats = source_stats.get('cache', {})
    
    if cache_stats:
        cache_hits = cache_stats.get('hits', 0)
        cache_total = cache_stats.get('total_requests', 0)
        cache_rate = cache_stats.get('hit_rate', 0)
        
        if cache_total > 0:
            cache_emoji = "✅" if cache_rate >= 50 else "⚠️" if cache_rate > 0 else "❌"
            lines.append(f"{cache_emoji} <b>Cache:</b> {cache_hits}/{cache_total} hits ({cache_rate}%)")
    
    # ═══════════════════════════════════════════════════════════
    # SOURCE STATISTICS (with categories)
    # ═══════════════════════════════════════════════════════════
    
    successful_sources = []
    failed_sources = []
    skipped_sources = []
    
    for source_name in ['birdeye', 'jupiter', 'dexscreener', 'raydium', 'coingecko']:
        stats = source_stats.get(source_name)
        
        if not stats:
            # Source not in stats - wasn't used at all
            skipped_sources.append(source_name)
            continue
        
        success = stats.get('success', 0)
        attempted = stats.get('attempted', 0)
        cancelled = stats.get('cancelled', 0)
        success_rate = stats.get('success_rate', 0)
        
        if attempted == 0 and cancelled == 0:
            # Never used
            skipped_sources.append(source_name)
        elif success > 0:
            # Had successes
            successful_sources.append({
                'name': source_name,
                'success': success,
                'attempted': attempted,
                'rate': success_rate,
                'cancelled': cancelled
            })
        else:
            # Tried but all failed
            failed_sources.append({
                'name': source_name,
                'attempted': attempted,
                'rate': success_rate,
                'cancelled': cancelled
            })
    
    # ═══════════════════════════════════════════════════════════
    # DISPLAY SUCCESSFUL SOURCES
    # ═══════════════════════════════════════════════════════════
    
    if successful_sources:
        lines.append("")
        lines.append("<b>✅ Successful Sources:</b>")
        
        for src in successful_sources:
            name = src['name'].capitalize()
            success = src['success']
            attempted = src['attempted']
            rate = src['rate']
            cancelled = src['cancelled']
            
            line = f"  • <b>{name}:</b> {success}/{attempted} ({rate}%)"
            
            if cancelled > 0:
                line += f" <i>[+{cancelled} cancelled]</i>"
            
            lines.append(line)
    
    # ═══════════════════════════════════════════════════════════
    # DISPLAY FAILED SOURCES
    # ═══════════════════════════════════════════════════════════
    
    if failed_sources:
        lines.append("")
        lines.append("<b>❌ Failed Sources:</b>")
        
        for src in failed_sources:
            name = src['name'].capitalize()
            attempted = src['attempted']
            cancelled = src['cancelled']
            
            line = f"  • <b>{name}:</b> 0/{attempted} (0%)"
            
            if cancelled > 0:
                line += f" <i>[+{cancelled} cancelled]</i>"
            
            lines.append(line)
    
    # ═══════════════════════════════════════════════════════════
    # DISPLAY SKIPPED SOURCES
    # ═══════════════════════════════════════════════════════════
    
    if skipped_sources:
        lines.append("")
        lines.append("<b>⏭️ Skipped:</b>")
        
        skipped_names = [name.capitalize() for name in skipped_sources]
        lines.append(f"  <i>{', '.join(skipped_names)}</i>")
    
    # ═══════════════════════════════════════════════════════════
    # OVERALL COVERAGE
    # ═══════════════════════════════════════════════════════════
    
    total_success = sum(src['success'] for src in successful_sources)
    total_attempted = sum(src['attempted'] for src in successful_sources + failed_sources)
    
    if total_attempted > 0:
        coverage = (total_success / total_attempted * 100)
        coverage_emoji = "✅" if coverage >= 80 else "⚠️" if coverage >= 50 else "❌"
        
        lines.append("")
        lines.append(f"{coverage_emoji} <b>Overall Coverage:</b> {total_success}/{total_attempted} ({coverage:.1f}%)")
    
    # ═══════════════════════════════════════════════════════════
    # EFFICIENCY NOTE
    # ═══════════════════════════════════════════════════════════
    
    if total_tokens > 0 and total_attempted > 0:
        efficiency = total_attempted / total_tokens
        lines.append(f"<b>📊 Efficiency:</b> {efficiency:.1f} API calls per token")
        
        if efficiency < 1.5:
            lines.append("  <i>⚡ Excellent - early cancellation working!</i>")
        elif efficiency < 2.5:
            lines.append("  <i>✅ Good - reasonable API usage</i>")
        else:
            lines.append("  <i>⚠️ High - consider optimization</i>")
    
    return '\n'.join(lines)

async def _fetch_dexscreener_metadata(token_address: str, chain: str) -> Optional[Dict]:
    """
    Fetch token metadata from DexScreener
    Works for all chains
    """
    try:
        session = await session_factory.get_session('api')
        
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        
        timeout = aiohttp.ClientTimeout(total=5.0)
        
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            
            data = await resp.json()
            pairs = data.get('pairs', [])
            
            if not pairs:
                return None
            
            pair = max(pairs, key=lambda p: float(p.get('liquidity', {}).get('usd', 0) or 0))
            
            base_token = pair.get('baseToken', {})
            
            name = base_token.get('name', '')
            symbol = base_token.get('symbol', '')
            
            if not name and not symbol:
                return None
            
            price_usd = pair.get('priceUsd')
            liquidity = pair.get('liquidity', {}).get('usd')
            volume_24h = pair.get('volume', {}).get('h24')
            price_change_24h = pair.get('priceChange', {}).get('h24')
            fdv = pair.get('fdv')
            
            logger.info(f"✅ DexScreener: Found {symbol or name}")
            
            return {
                'name': name,
                'symbol': symbol,
                'logo': pair.get('info', {}).get('imageUrl', ''),
                'price': float(price_usd) if price_usd else None,
                'liquidity': liquidity,
                'volume_24h': volume_24h,
                'price_change_24h': float(price_change_24h) if price_change_24h else None,
                'market_cap': fdv,
                'dex': pair.get('dexId', ''),
                'source': 'DexScreener'
            }
    
    except asyncio.TimeoutError:
        logger.debug(f"DexScreener timeout for {token_address[:8]}")
        return None
    except Exception as e:
        logger.debug(f"DexScreener error: {e}")
        return None

async def _fetch_helius_metadata(token_address: str) -> Optional[Dict]:
    """
    Fetch token metadata from Helius (Solana only)
    Requires HELIUS_API_KEY
    """
    if not HELIUS_API_KEY:
        return None
    
    try:
        session = await session_factory.get_session('api')
        
        url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        
        payload = {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "getAsset",
            "params": {
                "id": token_address,
                "displayOptions": {
                    "showFungible": True
                }
            }
        }
        
        timeout = aiohttp.ClientTimeout(total=5.0)
        
        async with session.post(url, json=payload, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            
            data = await resp.json()
            result = data.get('result', {})
            
            content = result.get('content', {})
            metadata = content.get('metadata', {})
            
            name = metadata.get('name', '')
            symbol = metadata.get('symbol', '')
            
            if not name and not symbol:
                return None
            
            # Get token supply info
            token_info = result.get('token_info', {})
            supply = token_info.get('supply')
            decimals = token_info.get('decimals', 9)
            
            logger.info(f"✅ Helius: Found {symbol or name}")
            
            return {
                'name': name,
                'symbol': symbol,
                'logo': content.get('links', {}).get('image', ''),
                'decimals': decimals,
                'supply': supply,
                'source': 'Helius'
            }
    
    except asyncio.TimeoutError:
        logger.debug(f"Helius timeout for {token_address[:8]}")
        return None
    except Exception as e:
        logger.debug(f"Helius error: {e}")
        return None


async def test_all_evm_chains(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Comprehensive test for Enhanced Token Display across all EVM chains
    Usage: /test_all_evm
    
    Tests popular tokens on each chain to verify enhanced data is working
    """
    
    # Test tokens for each chain (well-known, should have good data)
    test_tokens = {
        'ethereum': {
            'address': '0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE',
            'name': 'SHIB',
            'description': 'Shiba Inu'
        },
        'bsc': {
            'address': '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',
            'name': 'WBNB',
            'description': 'Wrapped BNB'
        },
        'polygon': {
            'address': '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270',
            'name': 'WMATIC',
            'description': 'Wrapped MATIC'
        },
        'arbitrum': {
            'address': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',
            'name': 'WETH',
            'description': 'Wrapped ETH'
        },
        'base': {
            'address': '0x4200000000000000000000000000000000000006',
            'name': 'WETH',
            'description': 'Wrapped ETH'
        }
    }
    
    processing_msg = await update.message.reply_text(
        "🔍 <b>Testing Enhanced Token Display Across All EVM Chains...</b>\n\n"
        "⏳ This will take 30-60 seconds...\n\n"
        "Testing:\n"
        "• Ethereum (SHIB)\n"
        "• BSC (WBNB)\n"
        "• Polygon (WMATIC)\n"
        "• Arbitrum (WETH)\n"
        "• Base (WETH)",
        parse_mode=ParseMode.HTML
    )
    
    results = []
    
    for chain, token_info in test_tokens.items():
        try:
            logger.info(f"Testing {chain.upper()}: {token_info['name']}")
            
            # Fetch metadata
            metadata = await asyncio.wait_for(
                fetch_token_metadata_universal(
                    token_info['address'],
                    chain,
                    enhanced_token_fetcher,
                    evm_analyzer
                ),
                timeout=15.0
            )
            
            if metadata:
                tier = metadata.get('tier', 'unknown')
                enhanced = metadata.get('enhanced', False)
                source = metadata.get('source', 'unknown')
                has_price = bool(metadata.get('price'))
                has_liquidity = bool(metadata.get('liquidity'))
                
                # Determine status
                if tier in ['enhanced', 'evm_multi_source']:
                    status = "✅ ENHANCED"
                    emoji = "💎"
                elif tier == 'basic':
                    status = "⚠️ BASIC"
                    emoji = "📊"
                else:
                    status = "❌ MINIMAL"
                    emoji = "⚪"
                
                results.append({
                    'chain': chain,
                    'name': token_info['name'],
                    'status': status,
                    'emoji': emoji,
                    'tier': tier,
                    'source': source,
                    'has_price': has_price,
                    'has_liquidity': has_liquidity,
                    'symbol': metadata.get('symbol', 'N/A'),
                    'price': metadata.get('price', 0)
                })
            else:
                results.append({
                    'chain': chain,
                    'name': token_info['name'],
                    'status': "❌ FAILED",
                    'emoji': "🔴",
                    'tier': 'none',
                    'source': 'none',
                    'has_price': False,
                    'has_liquidity': False
                })
            
        except asyncio.TimeoutError:
            results.append({
                'chain': chain,
                'name': token_info['name'],
                'status': "⏱️ TIMEOUT",
                'emoji': "🕐",
                'tier': 'timeout'
            })
        except Exception as e:
            logger.error(f"Test error for {chain}: {e}")
            results.append({
                'chain': chain,
                'name': token_info['name'],
                'status': "❌ ERROR",
                'emoji': "🔴",
                'tier': 'error',
                'error': str(e)[:50]
            })
    
    # Build comprehensive report
    report = """🔍 <b>Enhanced Token Display Test Results</b>

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

"""
    
    # Results by chain
    enhanced_count = 0
    basic_count = 0
    failed_count = 0
    
    for result in results:
        chain = result['chain'].upper()
        name = result.get('name', 'N/A')
        status = result['status']
        emoji = result.get('emoji', '❓')
        tier = result.get('tier', 'unknown')
        source = result.get('source', 'N/A')
        
        report += f"{emoji} <b>{chain}</b> - {name}\n"
        report += f"   Status: {status}\n"
        report += f"   Tier: {tier}\n"
        
        if tier in ['enhanced', 'evm_multi_source']:
            enhanced_count += 1
            report += f"   Source: {source}\n"
            
            if result.get('has_price'):
                price = result.get('price', 0)
                symbol = result.get('symbol', 'N/A')
                report += f"   Price: ${price:.4f} ({symbol})\n"
            
            report += "   ✅ <b>Enhanced display will work!</b>\n"
        
        elif tier == 'basic':
            basic_count += 1
            report += f"   Source: {source}\n"
            report += "   ⚠️ Basic display (no enhanced features)\n"
        
        else:
            failed_count += 1
            if result.get('error'):
                report += f"   Error: {result['error']}\n"
        
        report += "\n"
    
    # Summary
    total = len(results)
    success_rate = (enhanced_count / total * 100) if total > 0 else 0
    
    report += """━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

<b>📊 SUMMARY:</b>

"""
    
    report += f"• Total Chains Tested: {total}\n"
    report += f"• <b>Enhanced:</b> {enhanced_count} {'✅' if enhanced_count > 0 else '❌'}\n"
    report += f"• <b>Basic:</b> {basic_count} {'⚠️' if basic_count > 0 else ''}\n"
    report += f"• <b>Failed:</b> {failed_count} {'❌' if failed_count > 0 else '✅'}\n"
    report += f"• <b>Success Rate:</b> {success_rate:.1f}%\n\n"
    
    # Overall status
    if enhanced_count >= 4:
        report += "✅ <b>EXCELLENT!</b> Enhanced display working for most chains.\n"
    elif enhanced_count >= 2:
        report += "⚠️ <b>PARTIAL SUCCESS</b> - Some chains have enhanced data.\n"
    elif enhanced_count >= 1:
        report += "🔴 <b>LIMITED</b> - Only {enhanced_count} chain(s) working.\n"
    else:
        report += "❌ <b>FAILED</b> - No enhanced data available.\n"
    
    report += """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

<b>💡 What This Means:</b>

<b>Enhanced Tier:</b> Shows "🪙 Enhanced Token Analysis" header with:
• Verification badges
• 1h/24h/7d price changes
• Holder distribution
• Risk assessment
• Trading activity metrics

<b>Basic Tier:</b> Shows "🪙 Token Information" header with:
• Basic price data
• 24h changes only
• Limited metrics

<b>📋 Next Steps:</b>

If enhanced is working:
✅ Test with: /test_enhanced_evm &lt;address&gt; &lt;chain&gt;

If enhanced not working:
1. Check API keys in Config
2. Verify EnhancedTokenDataFetcher is initialized
3. Check logs for errors
4. Ensure Birdeye/DexScreener APIs are accessible

<i>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"""
    
    await processing_msg.edit_text(report, parse_mode=ParseMode.HTML)


# Add to your main() function:
# application.add_handler(CommandHandler("test_all_evm", test_all_evm_chains))
# ============================================================================
# TOKEN INFO DISPLAY FORMATTING
# ============================================================================

def format_token_info_message_enhanced(
    metadata: Dict, 
    token_address: str, 
    chain: str
) -> str:
    """
    ✅ COMPLETE FIX: Better tier detection with multiple checks
    """
    
    # ⭐ TRIPLE CHECK for enhanced status
    tier = metadata.get('tier', 'basic')
    is_enhanced_flag = metadata.get('enhanced', False)
    
    # Check if actual enhanced data exists
    has_enhanced_data = any([
        metadata.get('price_change_1h') is not None,
        metadata.get('price_change_7d') is not None,
        metadata.get('risk_level') and metadata.get('risk_level') != 'UNKNOWN',
        metadata.get('holder_count') and metadata.get('holder_count') > 0,
        metadata.get('trades_24h') and metadata.get('trades_24h') > 0,
        metadata.get('top_10_holder_percent') is not None
    ])
    
    # Final determination
    is_enhanced = (
        is_enhanced_flag or 
        tier in ['enhanced', 'evm_multi_source'] or 
        has_enhanced_data
    )
    
    name = metadata.get('name', 'Unknown Token')
    symbol = metadata.get('symbol', 'N/A')
    source = metadata.get('source', 'Unknown')
    
    addr_short = f"{token_address[:8]}...{token_address[-6:]}"
    
    lines = []
    
    # ═══════════════════════════════════════════════════════════════
    # HEADER - Show proper tier
    # ═══════════════════════════════════════════════════════════════
    
    if tier == 'minimal':
        lines.append("🪙 <b>Token Information</b>")
        lines.append(f"<i>⚠️ Limited data - token may be new or unlisted</i>\n")
        logger.info(f"📊 DISPLAY: MINIMAL (tier={tier})")
    
    elif is_enhanced:  # ⭐ FIXED: Comprehensive check
        lines.append("🪙 <b>Enhanced Token Analysis</b>\n")
        logger.info(f"📊 DISPLAY: ENHANCED (tier={tier}, flag={is_enhanced_flag}, data={has_enhanced_data})")
    
    else:
        lines.append("🪙 <b>Token Information</b>\n")
        logger.info(f"📊 DISPLAY: BASIC (tier={tier})")
    
    # Basic info - always show
    lines.extend([
        f"<b>📛 {html_escape(name)}</b>",
        f"<b>🏷 Symbol:</b> {html_escape(symbol)}",
        f"<b>⛓ Chain:</b> {chain.upper()}",
        f"<b>📍 Address:</b> <code>{addr_short}</code>",
    ])
    
    # ═══════════════════════════════════════════════════════════════
    # VERIFICATION BADGES (Enhanced tiers only)
    # ═══════════════════════════════════════════════════════════════
    
    if is_enhanced:  # ⭐ FIXED: Check our comprehensive flag
        badges = []
        if metadata.get('is_verified'):
            badges.append("✅ Verified")
        if metadata.get('is_audited'):
            badges.append("🔒 Audited")
        if metadata.get('honeypot_risk'):
            badges.append("⚠️ Honeypot Risk")
        
        if badges:
            lines.append(f"\n<b>🏆 Status:</b> {' • '.join(badges)}")
    
    # ═══════════════════════════════════════════════════════════════
    # PRICE SECTION
    # ═══════════════════════════════════════════════════════════════
    
    price = metadata.get('price')
    has_price_data = price and price > 0
    
    if has_price_data or tier != 'minimal':
        lines.append("\n<b>💰 Price Data:</b>")
        
        if has_price_data:
            if price >= 1:
                price_str = f"${price:,.4f}"
            elif price >= 0.01:
                price_str = f"${price:.6f}"
            else:
                price_str = f"${price:.10f}"
            
            lines.append(f"• Current: {price_str}")
        else:
            lines.append(f"• Current: <i>Price unavailable</i>")
        
        # ⭐ Show all timeframes if enhanced
        if is_enhanced:
            for timeframe, key in [('1h', 'price_change_1h'), 
                                   ('24h', 'price_change_24h'), 
                                   ('7d', 'price_change_7d')]:
                change = metadata.get(key)
                if change is not None:
                    emoji = "📈" if change >= 0 else "📉"
                    sign = "+" if change >= 0 else ""
                    lines.append(f"• {timeframe}: {emoji} {sign}{change:.2f}%")
        else:
            # Basic: just 24h
            change_24h = metadata.get('price_change_24h')
            if change_24h is not None:
                emoji = "📈" if change_24h >= 0 else "📉"
                sign = "+" if change_24h >= 0 else ""
                lines.append(f"• 24h: {emoji} {sign}{change_24h:.2f}%")
    
    # ═══════════════════════════════════════════════════════════════
    # MARKET METRICS
    # ═══════════════════════════════════════════════════════════════
    
    has_market_data = any([
        metadata.get('market_cap'),
        metadata.get('fdv'),
        metadata.get('liquidity'),
        metadata.get('volume_24h')
    ])
    
    if has_market_data:
        lines.append("\n<b>📊 Market Metrics:</b>")
        
        mc = metadata.get('market_cap')
        if mc and mc > 0:
            if mc >= 1_000_000_000:
                lines.append(f"• Market Cap: ${mc/1_000_000_000:.2f}B")
            elif mc >= 1_000_000:
                lines.append(f"• Market Cap: ${mc/1_000_000:.2f}M")
            elif mc >= 1_000:
                lines.append(f"• Market Cap: ${mc/1_000:.2f}K")
            else:
                lines.append(f"• Market Cap: ${mc:.2f}")
        
        fdv = metadata.get('fdv')
        if fdv and fdv > 0 and fdv != mc:
            if fdv >= 1_000_000_000:
                lines.append(f"• FDV: ${fdv/1_000_000_000:.2f}B")
            elif fdv >= 1_000_000:
                lines.append(f"• FDV: ${fdv/1_000_000:.2f}M")
            else:
                lines.append(f"• FDV: ${fdv:,.0f}")
        
        liq = metadata.get('liquidity')
        if liq and liq > 0:
            if liq >= 1_000_000:
                lines.append(f"• Liquidity: ${liq/1_000_000:.2f}M")
            elif liq >= 1_000:
                lines.append(f"• Liquidity: ${liq/1_000:.2f}K")
            else:
                lines.append(f"• Liquidity: ${liq:.2f}")
        
        vol = metadata.get('volume_24h')
        if vol and vol > 0:
            if vol >= 1_000_000:
                lines.append(f"• 24h Volume: ${vol/1_000_000:.2f}M")
            elif vol >= 1_000:
                lines.append(f"• 24h Volume: ${vol/1_000:.2f}K")
            else:
                lines.append(f"• 24h Volume: ${vol:.2f}")
    
    # ═══════════════════════════════════════════════════════════════
    # ENHANCED ONLY: Additional metrics
    # ═══════════════════════════════════════════════════════════════
    
    if is_enhanced:  # ⭐ FIXED: Check comprehensive flag
        # Trading activity
        trades = metadata.get('trades_24h')
        buyers = metadata.get('buyers_24h')
        
        if trades or buyers:
            lines.append("\n<b>📈 Trading Activity (24h):</b>")
            if trades:
                lines.append(f"• Trades: {trades:,}")
            if buyers and metadata.get('sellers_24h'):
                lines.append(f"• Buyers: {buyers:,}")
                lines.append(f"• Sellers: {metadata.get('sellers_24h'):,}")
                
                buy_sell_ratio = metadata.get('buy_sell_ratio')
                if buy_sell_ratio:
                    if 0.8 <= buy_sell_ratio <= 1.2:
                        ratio_emoji = "🟢"
                    elif 0.5 <= buy_sell_ratio <= 2:
                        ratio_emoji = "🟡"
                    else:
                        ratio_emoji = "🔴"
                    lines.append(f"• B/S Ratio: {ratio_emoji} {buy_sell_ratio:.2f}")
        
        # Holder distribution
        holder_count = metadata.get('holder_count')
        top_10_percent = metadata.get('top_10_holder_percent')
        
        if holder_count or top_10_percent:
            lines.append("\n<b>👥 Holder Distribution:</b>")
            
            if holder_count:
                lines.append(f"• Total Holders: {holder_count:,}")
            
            if top_10_percent:
                if top_10_percent > 60:
                    emoji = "🔴"
                    warning = " (High concentration)"
                elif top_10_percent > 40:
                    emoji = "🟡"
                    warning = ""
                else:
                    emoji = "🟢"
                    warning = ""
                
                lines.append(f"• Top 10 Hold: {emoji} {top_10_percent:.1f}%{warning}")
        
        # Risk assessment
        risk_level = metadata.get('risk_level')
        if risk_level and risk_level != 'UNKNOWN':
            lines.append(f"\n<b>⚠️ Risk Assessment:</b>")
            lines.append(f"• Level: {risk_level}")
            
            risk_score = metadata.get('risk_score')
            if risk_score:
                lines.append(f"• Score: {risk_score:.0f}/100")
    
    # ═══════════════════════════════════════════════════════════════
    # FOOTER
    # ═══════════════════════════════════════════════════════════════
    
    lines.append(f"\n<i>📡 Data: {source}</i>")
    
    if tier == 'minimal':
        lines.append("<i>💡 Token may be new or unlisted on major platforms</i>")
    elif is_enhanced:
        lines.append(f"<i>✨ Enhanced analysis active for {chain.upper()}</i>")
    
    lines.append("\n⏳ <b>Starting trader analysis...</b>")
    
    return '\n'.join(lines)

async def display_token_info_before_analysis_fixed(
    message, 
    address: str, 
    chain: str, 
    addr_type: str
):
    """
    ✅ FIXED: Only displays if we get VALID token data
    Returns None if token data is insufficient
    """
    
    # Normalize chain
    normalized_chain = normalize_chain_name(chain)
    
    logger.info(f"🔍 Attempting to fetch token metadata for {address[:8]} on {normalized_chain}...")
    
    try:
        # Use the universal fetch function
        token_data = await asyncio.wait_for(
            fetch_token_metadata_universal(
                address, 
                normalized_chain,
                enhanced_token_fetcher,
                evm_analyzer
            ),
            timeout=20.0
        )
        
        # ⭐ CRITICAL: Only display if we have REAL token data
        has_valid_data = (
            token_data and 
            token_data.get('success', True) and
            token_data.get('price', 0) > 0 and
            token_data.get('symbol') and
            token_data.get('symbol') != address[:10]  # Not just address as symbol
        )
        
        if not has_valid_data:
            logger.info(f"⚠️ Insufficient token data - skipping display")
            return None
        
        # Format and send token info message
        info_message = format_token_info_message_enhanced(token_data, address, normalized_chain)
        
        await message.reply_text(
            info_message,
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"✅ Token info displayed: {token_data.get('symbol', 'N/A')} on {normalized_chain}")
        
        return token_data
        
    except asyncio.TimeoutError:
        logger.warning(f"⏱️ Token metadata fetch timeout for {address[:8]}")
        return None
        
    except Exception as e:
        logger.error(f"❌ Token metadata error: {e}")
        return None


# ============================================================================
# INTEGRATION WITH PERFORM_ANALYSIS
# ============================================================================

    
async def perform_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                          address: str, user_id: int, specified_chain: str = None, 
                          timeframe_days: int = None):
    """
    ✅ ENHANCED: Supports chain selection and timeframe selection (7d/30d) for EVM
    
    Args:
        address: Wallet/token address
        user_id: Telegram user ID
        specified_chain: Chain name (optional)
        timeframe_days: Analysis timeframe in days (7 or 30, only for EVM)
    """
    # Get message object
    if update.message:
        message = update.message
    elif update.callback_query:
        message = update.callback_query.message
    else:
        logger.error("No message found in update")
        return
    
    processing_msg = await message.reply_text(
        "🔍 <b>Analyzing...</b>",
        parse_mode=ParseMode.HTML
    )
    
    try:
        start_time = datetime.now()
        
        # Detect if EVM or Solana
        is_evm_address = len(address) == 42 and address.startswith('0x')
        is_solana_address = 32 <= len(address) <= 44 and not address.startswith('0x')
        
        # If EVM but no chain specified, ask user
        if is_evm_address and not specified_chain:
            keyboard = [
                [InlineKeyboardButton("Ethereum", callback_data=f'analyze_{address}_ethereum'),
                 InlineKeyboardButton("BSC", callback_data=f'analyze_{address}_bsc')],
                [InlineKeyboardButton("Polygon", callback_data=f'analyze_{address}_polygon'),
                 InlineKeyboardButton("Arbitrum", callback_data=f'analyze_{address}_arbitrum')],
                [InlineKeyboardButton("Optimism", callback_data=f'analyze_{address}_optimism'),
                 InlineKeyboardButton("Base", callback_data=f'analyze_{address}_base')],
                [InlineKeyboardButton("Avalanche", callback_data=f'analyze_{address}_avalanche')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await processing_msg.edit_text(
                f"🔍 <b>EVM Address Detected!</b>\n\n"
                f"<b>Address:</b> <code>{address[:10]}...{address[-8:]}</code>\n\n"
                f"⛓ <b>Please select the chain:</b>",
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
            return
        # ⭐ NEW: If EVM wallet and no timeframe specified, ask user
        if is_evm_address and specified_chain and timeframe_days is None:
            # Check if this is likely a wallet (quick detection)
            addr_type, metadata = await asyncio.wait_for(
                detect_address_type(address, specified_chain),
                timeout=10.0
            )
            
            # If it's a wallet, offer timeframe selection
            if addr_type == 'wallet':
                keyboard = [
                    [InlineKeyboardButton("📅 7 Days (Fast)", 
                                         callback_data=f'timeframe_{address}_{specified_chain}_7'),
                     InlineKeyboardButton("📅 30 Days (Detailed)", 
                                         callback_data=f'timeframe_{address}_{specified_chain}_30')]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await processing_msg.edit_text(
                    f"👤 <b>Wallet Detected!</b>\n\n"
                    f"<b>Address:</b> <code>{address[:10]}...{address[-8:]}</code>\n"
                    f"<b>Chain:</b> {specified_chain.upper()}\n\n"
                    f"📊 <b>Select Analysis Timeframe:</b>\n\n"
                    f"• <b>7 Days:</b> Recent activity, faster analysis\n"
                    f"• <b>30 Days:</b> More comprehensive, longer analysis\n\n"
                    f"<i>💡 7-day is recommended for quick checks</i>",
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
                return
        
        # Determine chain
        # Determine chain (if not already set)
        if specified_chain:
            chain = normalize_chain_name(specified_chain)
        elif is_solana_address:
            chain = 'solana'
        else:
            chain = 'ethereum'
        
        # Set default timeframe if not specified
        if timeframe_days is None:
            timeframe_days = 7  # Default to 7 days for EVM
        
        logger.info(f"🔍 Analyzing {address[:8]}... on {chain} ({timeframe_days}d timeframe)")

        # Check chain access
        if not sub_manager.check_chain_access(user_id, chain):
            await processing_msg.edit_text(
                f"🔒 <b>Chain Not Available</b>\n\n"
                f"Chain: {chain.upper()}\n\n"
                f"Upgrade to access this chain: /subscription",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Detect address type
        await processing_msg.edit_text(
            f"🔍 <b>Detecting address type...</b>\n"
            f"⛓ {chain.upper()}\n"
            f"⏳ Please wait...",
            parse_mode=ParseMode.HTML
        )
        
        addr_type, metadata = await asyncio.wait_for(
            detect_address_type(address, chain),
            timeout=10.0
        )
        
        logger.info(f"✅ Address type detected: {addr_type} (confidence: {metadata.get('confidence', 'unknown')})")
        
        # ⭐ FIXED: Only show token info if CONFIRMED as token
        # Check for wallet indicators first
        is_likely_wallet = (
            addr_type == 'wallet' or
            (addr_type == 'contract' and metadata.get('confidence') == 'high' and 
             metadata.get('reason') == 'no_code')
        )
        
        if not is_likely_wallet and chain != 'solana':
            logger.info(f"🪙 Possible token detected - attempting metadata fetch...")
            
            # Update status
            await processing_msg.edit_text(
                f"🪙 <b>Checking Token Data...</b>\n"
                f"⛓ {chain.upper()}\n"
                f"⏳ Fetching enhanced metadata...",
                parse_mode=ParseMode.HTML
            )
            
            # Try to display token info
            token_metadata = await display_token_info_before_analysis_fixed(
                message, address, chain, addr_type
            )
            
            # Only confirm as token if we got valid price data
            if token_metadata and token_metadata.get('price', 0) > 0:
                logger.info(f"✅ Token confirmed via metadata: {token_metadata.get('symbol')}")
                addr_type = 'token'
            else:
                # No valid token data - treat as wallet
                logger.info(f"⚠️ No valid token data - treating as wallet")
                addr_type = 'wallet'
        
        # ═══════════════════════════════════════════════════════════
        # TOKEN ANALYSIS
        # ═══════════════════════════════════════════════════════════
        
        if addr_type == 'token':
            # Check feature access
            if not check_feature_access(user_id, 'extended_features'):
                await processing_msg.edit_text(
                    "🔒 <b>Premium Feature</b>\n\n"
                    "Token analysis requires Basic Monthly or Premium.\n\n"
                    "Upgrade: /subscription",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # For Solana, show token info (EVM already showed it above)
            if chain == 'solana':
                await processing_msg.edit_text(
                    f"🪙 <b>Token Detected!</b>\n"
                    f"⛓ {chain.upper()}\n"
                    f"⏳ Fetching enhanced token data...",
                    parse_mode=ParseMode.HTML
                )
                
                token_metadata = await display_token_info_before_analysis_fixed(
                    message, address, chain, addr_type
                )
                
                if not token_metadata:
                    await processing_msg.edit_text(
                        f"⚠️ <b>Token Info Limited</b>\n\n"
                        f"Couldn't fetch detailed metadata for this token.\n"
                        f"Starting trader analysis anyway...",
                        parse_mode=ParseMode.HTML
                    )
            
            # Start trader analysis
            try:
                await processing_msg.edit_text(
                    f"⏳ <b>Analyzing traders...</b>\n\n"
                    f"This may take 1-5 minutes...",
                    parse_mode=ParseMode.HTML
                )
                
                result = await asyncio.wait_for(
                    token_analyzer.analyze_token_traders(address, chain),
                    timeout=420.0
                )
                
                if not result.get('success'):
                    await processing_msg.edit_text(
                        f"❌ <b>Analysis Failed</b>\n\n"
                        f"{result.get('message', 'Unknown error')}",
                        parse_mode=ParseMode.HTML
                    )
                    return
                
                # Store for CSV export
                context.user_data['all_traders'] = result.get('top_traders', [])
                context.user_data['last_token_address'] = address
                context.user_data['last_chain'] = chain
                context.user_data['last_cabal_result'] = result.get('cabal_analysis', {})
                context.user_data['analysis_timestamp'] = datetime.now().isoformat()
                
                # Format results
                formatted = format_token_trader_results(result)
                
                # Add export button for premium
                subscription = db.get_user_subscription(user_id)
                is_premium = 'premium' in subscription['tier']
                
                if is_premium:
                    keyboard = [[InlineKeyboardButton("📥 Export CSV", callback_data='export_csv')]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                else:
                    reply_markup = None
                
                await processing_msg.edit_text(
                    formatted,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
                
                # Increment usage
                db.increment_daily_analyses(user_id)
                
            except asyncio.TimeoutError:
                await processing_msg.edit_text(
                    "⏱️ <b>Analysis Timeout</b>\n\n"
                    "Token analysis took too long. Please try again.",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Token analysis error: {e}", exc_info=True)
                await processing_msg.edit_text(
                    f"❌ <b>Analysis Error</b>\n\n{html_escape(str(e)[:200])}",
                    parse_mode=ParseMode.HTML
                )
        
        # ═══════════════════════════════════════════════════════════
        # WALLET ANALYSIS
        # ═══════════════════════════════════════════════════════════
        
        else:  # wallet
            await processing_msg.edit_text(
                f"👤 <b>Wallet Detected!</b>\n"
                f"⛓ {chain.upper()}\n"
                f"📅 Timeframe: {timeframe_days} days\n"
                f"⏳ Analyzing trading history...",
                parse_mode=ParseMode.HTML
            )
            
            # Choose analyzer
            if chain == 'solana':
                analyzer = solana_analyzer
                result = await asyncio.wait_for(
                    analyzer.analyze_wallet(address),
                    timeout=60.0
                )
            else:
                analyzer = evm_analyzer
                # ⭐ PASS TIMEFRAME TO EVM ANALYZER
                result = await asyncio.wait_for(
                    analyzer.analyze_wallet(address, chain, days=timeframe_days),
                    timeout=30.0
                )
            # Calculate time
            analysis_time = (datetime.now() - start_time).total_seconds()
            
            # Check success
            if not result.get('success', True):
                await processing_msg.edit_text(
                    format_wallet_analysis(result, analysis_time),
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Format and send
            formatted = format_wallet_analysis(result, analysis_time)
            
            await processing_msg.edit_text(
                formatted,
                parse_mode=ParseMode.HTML
            )
            
            # Increment usage
            db.increment_daily_analyses(user_id)
            
            logger.info(f"✅ Wallet analysis complete: {address[:8]}... in {analysis_time:.1f}s")
    
    except asyncio.TimeoutError:
        await processing_msg.edit_text(
            "⏱️ <b>Analysis Timeout</b>\n\n"
            "The analysis took too long. Please try again.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Analysis error: {e}", exc_info=True)
        await processing_msg.edit_text(
            f"❌ <b>Analysis Error</b>\n\n"
            f"{html_escape(str(e)[:200])}\n\n"
            "Please try again or contact support.",
            parse_mode=ParseMode.HTML
        )


"""
Test command to verify token info displays on all chains
Add this to your bot to test the fix
"""

async def test_token_display(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Test token info display across all chains
    Usage: /test_token_display <address> <chain>
    
    Examples:
    /test_token_display 0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE ethereum
    /test_token_display 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c bsc
    /test_token_display DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263 solana
    """
    
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "❌ <b>Usage:</b> /test_token_display &lt;address&gt; &lt;chain&gt;\n\n"
            "<b>Test Tokens:</b>\n\n"
            "<b>Ethereum - SHIB:</b>\n"
            "<code>/test_token_display 0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE ethereum</code>\n\n"
            "<b>BSC - WBNB:</b>\n"
            "<code>/test_token_display 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c bsc</code>\n\n"
            "<b>Polygon - WMATIC:</b>\n"
            "<code>/test_token_display 0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270 polygon</code>\n\n"
            "<b>Solana - BONK:</b>\n"
            "<code>/test_token_display DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263 solana</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    address = context.args[0].strip()
    chain = context.args[1].lower().strip()
    
    processing_msg = await update.message.reply_text(
        f"🧪 <b>Testing Token Display</b>\n\n"
        f"Address: <code>{address[:10]}...{address[-8:]}</code>\n"
        f"Chain: {chain.upper()}\n\n"
        f"⏳ Running test...",
        parse_mode=ParseMode.HTML
    )
    
    try:
        # Step 1: Detect address type
        logger.info("=" * 60)
        logger.info("TEST: Token Display")
        logger.info("=" * 60)
        
        addr_type, metadata = await asyncio.wait_for(
            detect_address_type(address, chain),
            timeout=10.0
        )
        
        step1_result = f"""<b>STEP 1: Address Type Detection</b>
Type: {addr_type}
Confidence: {metadata.get('confidence', 'unknown')}
Method: {metadata.get('method', 'N/A')}
"""
        
        # Step 2: Fetch token metadata
        logger.info("Fetching token metadata...")
        
        token_data = await asyncio.wait_for(
            fetch_token_metadata_universal(
                address,
                chain,
                enhanced_token_fetcher,
                evm_analyzer
            ),
            timeout=20.0
        )
        
        if token_data:
            step2_result = f"""
<b>STEP 2: Token Metadata Fetch</b>
✅ Success
Symbol: {token_data.get('symbol')}
Source: {token_data.get('source')}
Tier: {token_data.get('tier')}
Enhanced: {token_data.get('enhanced')}
Price: ${token_data.get('price', 0):,.6f}
"""
        else:
            step2_result = """
<b>STEP 2: Token Metadata Fetch</b>
❌ Failed - No data returned
"""
        
        # Step 3: Format message
        logger.info("Formatting display message...")
        
        if token_data:
            formatted = format_token_info_message_enhanced(
                token_data,
                address,
                chain
            )
            
            # Check header
            if "🪙 <b>Enhanced Token Analysis</b>" in formatted:
                step3_result = """
<b>STEP 3: Message Formatting</b>
✅ Will display as ENHANCED"""
            elif "Limited data" in formatted:
                step3_result = """
<b>STEP 3: Message Formatting</b>
⚠️ Will display as MINIMAL"""
            else:
                step3_result = """
<b>STEP 3: Message Formatting</b>
📊 Will display as BASIC"""
        else:
            step3_result = """
<b>STEP 3: Message Formatting</b>
❌ Cannot format - no data"""
            formatted = None
        
        # Build report
        report = f"""🧪 <b>Token Display Test Results</b>

<b>Address:</b> <code>{address[:10]}...{address[-8:]}</code>
<b>Chain:</b> {chain.upper()}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{step1_result}
{step2_result}
{step3_result}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

<b>🎯 FINAL VERDICT:</b>
"""
        
        # Determine verdict
        if formatted and "Enhanced Token Analysis" in formatted:
            report += "✅ <b>PASS</b> - Enhanced display working!\n"
            report += "\nToken info will show BEFORE trader analysis."
        elif formatted:
            report += "⚠️ <b>PARTIAL</b> - Basic display working\n"
            report += "\nToken info shows but without enhanced features."
        else:
            report += "❌ <b>FAIL</b> - No display\n"
            report += "\nToken info won't show. Check:\n"
            report += "• Is token valid?\n"
            report += "• Is chain supported?\n"
            report += "• Are API keys configured?"
        
        await processing_msg.edit_text(report, parse_mode=ParseMode.HTML)
        
        # Show actual formatted message if available
        if formatted:
            await update.message.reply_text(
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "<b>PREVIEW: This is what users will see:</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n" + formatted,
                parse_mode=ParseMode.HTML
            )
        
    except asyncio.TimeoutError:
        await processing_msg.edit_text(
            "⏱️ <b>Test Timeout</b>\n\n"
            "One or more steps took too long.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Test error: {e}", exc_info=True)
        await processing_msg.edit_text(
            f"❌ <b>Test Failed</b>\n\n"
            f"Error: {html_escape(str(e)[:200])}",
            parse_mode=ParseMode.HTML
        )


# ═══════════════════════════════════════════════════════════════════
# Add to your main() function:
# ═══════════════════════════════════════════════════════════════════

"""

Then test with:
/test_token_display 0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE ethereum
/test_token_display 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c bsc
"""

async def fetch_token_metadata_universal(
    token_address: str, 
    chain: str,
    enhanced_token_fetcher,
    evm_analyzer
) -> Optional[Dict]:
    """
    ✅ COMPLETE FIX: Properly detects and sets enhanced flag
    
    The issue was that we were always setting enhanced=True even when
    the fetcher returned basic data. Now we check actual data content.
    """
    
    chain_normalized = normalize_chain_name(chain)
    
    logger.info(f"🔍 Fetching token metadata: {token_address[:8]}... on {chain_normalized}")
    
    # ═══════════════════════════════════════════════════════════════
    # TIER 1: EnhancedTokenDataFetcher (PRIORITY)
    # ═══════════════════════════════════════════════════════════════
    
    if enhanced_token_fetcher:
        try:
            logger.info(f"📊 Tier 1: Enhanced multi-source fetch for {chain_normalized}...")
            
            metadata = await asyncio.wait_for(
                enhanced_token_fetcher.fetch_enhanced_metadata(
                    token_address, 
                    chain_normalized
                ),
                timeout=20.0
            )
            
            if metadata:
                logger.info(f"✅ Tier 1 SUCCESS: {metadata.symbol} | Source: {metadata.source}")
                
                # ⭐ KEY FIX: Analyze what data we actually received
                enhanced_features_present = [
                    metadata.price_change_1h is not None,
                    metadata.price_change_7d is not None,
                    metadata.risk_level and str(metadata.risk_level) != 'UNKNOWN',
                    metadata.risk_score is not None,
                    metadata.holder_count is not None and metadata.holder_count > 0,
                    metadata.top_10_holder_percent is not None,
                    metadata.trades_24h is not None and metadata.trades_24h > 0,
                    metadata.buyers_24h is not None,
                    metadata.is_verified is not None,
                    metadata.is_audited is not None
                ]
                
                enhanced_count = sum(enhanced_features_present)
                
                # Determine tier based on actual data richness
                if enhanced_count >= 5:
                    # Has at least 5 enhanced features
                    tier = 'enhanced'
                    is_enhanced = True
                    logger.info(f"✅ ENHANCED: {enhanced_count}/10 features present")
                elif enhanced_count >= 2:
                    # Has some enhanced features
                    tier = 'evm_multi_source'
                    is_enhanced = True
                    logger.info(f"✅ MULTI-SOURCE: {enhanced_count}/10 features present")
                else:
                    # Only has basic data
                    tier = 'basic'
                    is_enhanced = False
                    logger.warning(f"⚠️ BASIC: Only {enhanced_count}/10 features present")
                
                # Log what we got
                logger.info(f"📊 Feature analysis:")
                logger.info(f"   price_change_1h: {metadata.price_change_1h}")
                logger.info(f"   price_change_7d: {metadata.price_change_7d}")
                logger.info(f"   risk_level: {metadata.risk_level}")
                logger.info(f"   holder_count: {metadata.holder_count}")
                logger.info(f"   trades_24h: {metadata.trades_24h}")
                logger.info(f"   → Tier: {tier}, Enhanced: {is_enhanced}")
                
                return {
                    'name': metadata.name,
                    'symbol': metadata.symbol,
                    'logo': metadata.logo_url,
                    'price': metadata.price,
                    'price_change_1h': metadata.price_change_1h,
                    'price_change_24h': metadata.price_change_24h,
                    'price_change_7d': metadata.price_change_7d,
                    'market_cap': metadata.market_cap,
                    'fdv': metadata.fdv,
                    'liquidity': metadata.liquidity,
                    'volume_24h': metadata.volume_24h,
                    'holder_count': metadata.holder_count,
                    'top_10_holder_percent': metadata.top_10_holder_percent,
                    'trades_24h': metadata.trades_24h,
                    'buyers_24h': metadata.buyers_24h,
                    'sellers_24h': metadata.sellers_24h,
                    'buy_sell_ratio': metadata.buy_sell_ratio,
                    'is_verified': metadata.is_verified,
                    'is_audited': metadata.is_audited,
                    'honeypot_risk': metadata.honeypot_risk,
                    'risk_level': metadata.risk_level.value if hasattr(metadata.risk_level, 'value') else str(metadata.risk_level),
                    'risk_score': metadata.risk_score,
                    'website': metadata.website,
                    'twitter': metadata.twitter,
                    'telegram': metadata.telegram,
                    'source': metadata.source,
                    'enhanced': is_enhanced,  # ⭐ CRITICAL: Based on actual data
                    'success': True,
                    'tier': tier  # ⭐ CRITICAL: Based on feature count
                }
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ Tier 1 timeout for {chain_normalized}")
        except Exception as e:
            logger.warning(f"⚠️ Tier 1 error for {chain_normalized}: {e}")
    
    # ═══════════════════════════════════════════════════════════════
    # TIER 2: EVM Analyzer (EVM chains only - FALLBACK)
    # ═══════════════════════════════════════════════════════════════
    
    if chain_normalized != 'solana' and evm_analyzer:
        try:
            logger.info(f"📊 Tier 2: EVM multi-source fetch...")
            
            evm_data = await asyncio.wait_for(
                evm_analyzer.fetch_token_data_multi_source(
                    token_address,
                    chain_normalized
                ),
                timeout=20.0
            )
            
            if evm_data and evm_data.get('success'):
                logger.info(f"✅ Tier 2 SUCCESS: {evm_data.get('symbol', 'Unknown')}")
                
                return {
                    'name': evm_data.get('name', 'Unknown'),
                    'symbol': evm_data.get('symbol', 'N/A'),
                    'logo': None,
                    'price': evm_data.get('price_usd', 0),
                    'price_change_24h': evm_data.get('price_change_24h'),
                    'market_cap': evm_data.get('market_cap'),
                    'liquidity': evm_data.get('liquidity_usd'),
                    'volume_24h': evm_data.get('volume_24h'),
                    'source': f"Multi-Source ({', '.join(evm_data.get('data_sources', []))})",
                    'enhanced': True,
                    'success': True,
                    'tier': 'evm_multi_source'
                }
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ Tier 2 timeout")
        except Exception as e:
            logger.warning(f"⚠️ Tier 2 error: {e}")
    
    # ═══════════════════════════════════════════════════════════════
    # TIER 3: Basic fallback
    # ═══════════════════════════════════════════════════════════════
    
    logger.info(f"🔄 Tier 3: Basic fallback...")
    return await _fetch_basic_fallback(token_address, chain_normalized)

async def _fetch_basic_fallback(token_address: str, chain: str) -> Dict:
    """
    ✅ FIXED: Fallback with proper tier marking
    """
    if chain == 'solana':
        metadata = await _fetch_solana_metadata_basic(token_address)
    else:
        metadata = await _fetch_evm_metadata_basic(token_address, chain)
    
    if metadata:
        # ⭐ KEY FIX: Mark tier correctly
        metadata['tier'] = 'basic'
        metadata['enhanced'] = False  # Basic tier is NOT enhanced
        return metadata
    
    # Last resort: minimal data
    return _create_minimal_enhanced_metadata(token_address, chain)

def _create_minimal_enhanced_metadata(token_address: str, chain: str) -> Dict:
    """
    ✅ FIXED: Minimal metadata with proper flags
    """
    return {
        'name': 'Unknown Token',
        'symbol': f"{token_address[:6]}...{token_address[-4:]}",
        'logo': None,
        'price': 0,
        'price_change_1h': None,
        'price_change_24h': None,
        'price_change_7d': None,
        'market_cap': None,
        'fdv': None,
        'liquidity': None,
        'volume_24h': None,
        'holder_count': None,
        'top_10_holder_percent': None,
        'trades_24h': None,
        'buyers_24h': None,
        'sellers_24h': None,
        'buy_sell_ratio': None,
        'is_verified': False,
        'is_audited': False,
        'honeypot_risk': False,
        'risk_level': 'UNKNOWN',
        'risk_score': None,
        'website': None,
        'twitter': None,
        'telegram': None,
        'source': 'Minimal (All sources unavailable)',
        'enhanced': False,  # ⭐ Minimal is NOT enhanced
        'success': True,
        'tier': 'minimal',
        'warning': 'Limited data available - token may be new or unlisted'
    }


async def _fetch_evm_metadata_basic(token_address: str, chain: str) -> Optional[Dict]:
    """
    ✅ FIXED: Basic EVM metadata with consistent structure
    """
    try:
        logger.info(f"🔍 Fetching basic EVM data for {chain}...")
        
        session = await session_factory.get_session('api')
        
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        timeout = aiohttp.ClientTimeout(total=8.0)
        
        async with session.get(url, timeout=timeout) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = data.get('pairs', [])
                
                if not pairs:
                    logger.warning(f"⚠️ No pairs found for {token_address[:8]}")
                    return None
                
                # Filter by chain
                chain_pairs = [p for p in pairs if p.get('chainId', '').lower() == chain.lower()]
                relevant_pairs = chain_pairs if chain_pairs else pairs
                
                if not relevant_pairs:
                    logger.warning(f"⚠️ No pairs for chain {chain}")
                    return None
                
                # Get most liquid pair
                pair = max(relevant_pairs, 
                          key=lambda p: float(p.get('liquidity', {}).get('usd', 0) or 0))
                
                base = pair.get('baseToken', {})
                
                result = {
                    'name': base.get('name', ''),
                    'symbol': base.get('symbol', ''),
                    'logo': pair.get('info', {}).get('imageUrl'),
                    'price': float(pair.get('priceUsd', 0) or 0),
                    'price_change_24h': float(pair.get('priceChange', {}).get('h24', 0) or 0),
                    'liquidity': float(pair.get('liquidity', {}).get('usd', 0) or 0),
                    'volume_24h': float(pair.get('volume', {}).get('h24', 0) or 0),
                    'fdv': float(pair.get('fdv', 0) or 0),
                    'market_cap': float(pair.get('marketCap', 0) or 0),
                    'source': f'DexScreener ({chain})',
                    'enhanced': False,  # ⭐ Basic tier
                    'success': True,
                    'tier': 'basic'
                }
                
                logger.info(f"✅ DexScreener found: {result['symbol']}")
                
                return result
        
        logger.warning(f"⚠️ DexScreener returned status {resp.status}")
        return None
        
    except Exception as e:
        logger.error(f"❌ EVM basic fetch error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# UPDATED format_token_trader_results - DISPLAYS TOP 10 ONLY
# ═══════════════════════════════════════════════════════════════════

def format_token_trader_results(result: Dict) -> str:
    """
    ⭐ FIXED: Now displays enhanced token info + trader analysis
    """
    insights = result.get('insights', {})
    
    # Get all traders (up to 20) for context
    all_traders = result.get('top_traders', [])[:20]
    
    # Display only top 10 in bot message
    top_10_display = all_traders[:10]
    
    token_addr = result.get('token_address', '')
    token_short = f"{token_addr[:8]}...{token_addr[-6:]}"
    chain = result.get('chain', 'Unknown').upper()
    
    # ═══════════════════════════════════════════════════════════════
    # ⭐ ENHANCED TOKEN METADATA SECTION
    # ═══════════════════════════════════════════════════════════════
    
    lines = []
    
    # Get token metadata if available
    token_metadata = result.get('token_metadata')
    
    if token_metadata:
        is_enhanced = token_metadata.get('enhanced', False)
        
        if is_enhanced:
            lines.append("🪙 <b>Enhanced Token Analysis</b>\n")
        else:
            lines.append("🪙 <b>Token Information</b>\n")
        
        name = token_metadata.get('name', 'Unknown Token')
        symbol = token_metadata.get('symbol', 'N/A')
        source = token_metadata.get('source', '')
        
        lines.append(f"<b>📛 {html_escape(name)}</b>")
        lines.append(f"<b>🏷 Symbol:</b> {html_escape(symbol)}")
        lines.append(f"<b>⛓ Chain:</b> {chain}")
        lines.append(f"<b>📍 Address:</b> <code>{token_short}</code>")
        
        # ═══════════════════════════════════════════════════════════
        # VERIFICATION BADGES (Enhanced only)
        # ═══════════════════════════════════════════════════════════
        
        if is_enhanced:
            badges = []
            if token_metadata.get('is_verified'):
                badges.append("✅ Verified")
            if token_metadata.get('is_audited'):
                badges.append("🔒 Audited")
            if token_metadata.get('honeypot_risk'):
                badges.append("⚠️ Honeypot Risk")
            
            if badges:
                lines.append(f"\n<b>🏆 Status:</b> {' • '.join(badges)}")
        
        # ═══════════════════════════════════════════════════════════
        # PRICE SECTION (with 1h/24h/7d if enhanced)
        # ═══════════════════════════════════════════════════════════
        
        lines.append("\n<b>💰 Price Data:</b>")
        
        price = token_metadata.get('price')
        if price and price > 0:
            if price >= 1:
                price_str = f"${price:,.4f}"
            elif price >= 0.01:
                price_str = f"${price:.6f}"
            else:
                price_str = f"${price:.10f}"
            lines.append(f"• Current: {price_str}")
        
        if is_enhanced:
            # Show all timeframes
            price_change_1h = token_metadata.get('price_change_1h')
            if price_change_1h is not None:
                emoji = "📈" if price_change_1h >= 0 else "📉"
                sign = "+" if price_change_1h >= 0 else ""
                lines.append(f"• 1h: {emoji} {sign}{price_change_1h:.2f}%")
            
            price_change_24h = token_metadata.get('price_change_24h')
            if price_change_24h is not None:
                emoji = "📈" if price_change_24h >= 0 else "📉"
                sign = "+" if price_change_24h >= 0 else ""
                lines.append(f"• 24h: {emoji} {sign}{price_change_24h:.2f}%")
            
            price_change_7d = token_metadata.get('price_change_7d')
            if price_change_7d is not None:
                emoji = "📈" if price_change_7d >= 0 else "📉"
                sign = "+" if price_change_7d >= 0 else ""
                lines.append(f"• 7d: {emoji} {sign}{price_change_7d:.2f}%")
        else:
            # Basic: just 24h
            price_change = token_metadata.get('price_change_24h')
            if price_change is not None:
                emoji = "📈" if price_change >= 0 else "📉"
                sign = "+" if price_change >= 0 else ""
                lines.append(f"• 24h: {emoji} {sign}{price_change:.2f}%")
        
        # ═══════════════════════════════════════════════════════════
        # MARKET METRICS
        # ═══════════════════════════════════════════════════════════
        
        lines.append("\n<b>📊 Market Metrics:</b>")
        
        market_cap = token_metadata.get('market_cap')
        if market_cap and market_cap > 0:
            if market_cap >= 1_000_000_000:
                mc_str = f"${market_cap/1_000_000_000:.2f}B"
            elif market_cap >= 1_000_000:
                mc_str = f"${market_cap/1_000_000:.2f}M"
            elif market_cap >= 1_000:
                mc_str = f"${market_cap/1_000:.2f}K"
            else:
                mc_str = f"${market_cap:.2f}"
            lines.append(f"• Market Cap: {mc_str}")
        
        fdv = token_metadata.get('fdv')
        if fdv and fdv > 0 and fdv != market_cap:
            if fdv >= 1_000_000_000:
                fdv_str = f"${fdv/1_000_000_000:.2f}B"
            elif fdv >= 1_000_000:
                fdv_str = f"${fdv/1_000_000:.2f}M"
            else:
                fdv_str = f"${fdv:,.0f}"
            lines.append(f"• FDV: {fdv_str}")
        
        liquidity = token_metadata.get('liquidity')
        if liquidity and liquidity > 0:
            if liquidity >= 1_000_000:
                liq_str = f"${liquidity/1_000_000:.2f}M"
            elif liquidity >= 1_000:
                liq_str = f"${liquidity/1_000:.2f}K"
            else:
                liq_str = f"${liquidity:.2f}"
            lines.append(f"• Liquidity: {liq_str}")
        
        volume_24h = token_metadata.get('volume_24h')
        if volume_24h and volume_24h > 0:
            if volume_24h >= 1_000_000:
                vol_str = f"${volume_24h/1_000_000:.2f}M"
            elif volume_24h >= 1_000:
                vol_str = f"${volume_24h/1_000:.2f}K"
            else:
                vol_str = f"${volume_24h:.2f}"
            lines.append(f"• 24h Volume: {vol_str}")
        
        # ═══════════════════════════════════════════════════════════
        # ENHANCED ONLY: Trading Activity
        # ═══════════════════════════════════════════════════════════
        
        if is_enhanced:
            trades_24h = token_metadata.get('trades_24h')
            buyers_24h = token_metadata.get('buyers_24h')
            sellers_24h = token_metadata.get('sellers_24h')
            buy_sell_ratio = token_metadata.get('buy_sell_ratio')
            
            if trades_24h or buyers_24h:
                lines.append("\n<b>📈 Trading Activity (24h):</b>")
                
                if trades_24h:
                    lines.append(f"• Trades: {trades_24h:,}")
                
                if buyers_24h and sellers_24h:
                    lines.append(f"• Buyers: {buyers_24h:,}")
                    lines.append(f"• Sellers: {sellers_24h:,}")
                
                if buy_sell_ratio:
                    if 0.8 <= buy_sell_ratio <= 1.2:
                        ratio_emoji = "🟢"
                    elif 0.5 <= buy_sell_ratio <= 2:
                        ratio_emoji = "🟡"
                    else:
                        ratio_emoji = "🔴"
                    
                    lines.append(f"• B/S Ratio: {ratio_emoji} {buy_sell_ratio:.2f}")
        
        # ═══════════════════════════════════════════════════════════
        # ENHANCED ONLY: Holder Distribution
        # ═══════════════════════════════════════════════════════════
        
        if is_enhanced:
            holder_count = token_metadata.get('holder_count')
            top_10_percent = token_metadata.get('top_10_holder_percent')
            
            if holder_count or top_10_percent:
                lines.append("\n<b>👥 Holder Distribution:</b>")
                
                if holder_count:
                    lines.append(f"• Total Holders: {holder_count:,}")
                
                if top_10_percent:
                    if top_10_percent > 60:
                        emoji = "🔴"
                        warning = " (High concentration)"
                    elif top_10_percent > 40:
                        emoji = "🟡"
                        warning = ""
                    else:
                        emoji = "🟢"
                        warning = ""
                    
                    lines.append(f"• Top 10 Hold: {emoji} {top_10_percent:.1f}%{warning}")
        else:
            # Basic: Just holder count
            holder_count = token_metadata.get('holder_count')
            if holder_count and holder_count > 0:
                lines.append(f"\n<b>👥 Holders:</b> {holder_count:,}")
        
        # ═══════════════════════════════════════════════════════════
        # ENHANCED ONLY: Risk Assessment
        # ═══════════════════════════════════════════════════════════
        
        if is_enhanced:
            risk_level = token_metadata.get('risk_level')
            risk_score = token_metadata.get('risk_score')
            
            if risk_level:
                lines.append(f"\n<b>⚠️ Risk Assessment:</b>")
                lines.append(f"• Level: {risk_level}")
                
                if risk_score:
                    lines.append(f"• Score: {risk_score:.0f}/100")
        
        # Add data source
        if source:
            lines.append(f"\n<i>📡 Token data: {source}</i>")
        
        lines.append("")  # Separator
    
    else:
        # ═══════════════════════════════════════════════════════════
        # FALLBACK: No metadata available
        # ═══════════════════════════════════════════════════════════
        lines.append("🎯 <b>Token Trader Analysis Complete!</b>\n")
        lines.append(f"<b>🪙 Token:</b> <code>{token_short}</code>")
        lines.append(f"<b>⛓️ Chain:</b> {chain}\n")
    
    # ═══════════════════════════════════════════════════════════════
    # TRADER ANALYSIS SUMMARY
    # ═══════════════════════════════════════════════════════════════
    
    lines.extend([
        "<b>📊 Trader Analysis Summary:</b>",
        f"• <b>Total Traders:</b> {result.get('total_traders', 0)}",
        f"• <b>Analyzed:</b> {len(all_traders)} wallets",
        f"• <b>Profitable:</b> {insights.get('profitable_count', 0)}",
        f"• <b>Avg Win Rate:</b> {insights.get('avg_win_rate', 0):.1f}%\n",
    ])
    
    # ═══════════════════════════════════════════════════════════════
    # TOP 10 TRADERS DISPLAY
    # ═══════════════════════════════════════════════════════════════
    
    lines.append("<b>🏆 TOP 10 TRADERS:</b>\n")
    
    for i, trader in enumerate(top_10_display, 1):
        status_emoji = "💎" if trader.get('is_holder') else "✅"
        
        lines.extend([
            f"<b>{i}. Rank #{trader.get('rank', i)}</b>",
            f"   🏦 <code>{trader['wallet']}</code>",
            f"   💰 Profit: <b>{format_number(trader.get('total_profit', 0))}</b>",
            f"   📊 Win Rate: {trader.get('win_rate', 0):.0f}%",
            f"   {status_emoji} {('HOLDING' if trader.get('is_holder') else 'EXITED')}\n"
        ])
    
    # ═══════════════════════════════════════════════════════════════
    # FOOTER
    # ═══════════════════════════════════════════════════════════════
    
    if len(all_traders) > 10:
        lines.append(f"\n<i>💡 Showing top 10 of {len(all_traders)} traders analyzed</i>")
    
    lines.append(f"<i>📥 Use /export_csv to download complete data</i>")
    
    return '\n'.join(lines)


# ═══════════════════════════════════════════════════════════════════
# VERIFICATION COMMAND - Check storage
# ═══════════════════════════════════════════════════════════════════

async def verify_trader_storage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Debug command to verify trader storage
    Usage: /verify_traders
    """
    user_id = update.effective_user.id
    
    all_traders = context.user_data.get('all_traders', [])
    last_token = context.user_data.get('last_token_address')
    last_chain = context.user_data.get('last_chain')
    timestamp = context.user_data.get('analysis_timestamp')
    
    if not all_traders:
        await update.message.reply_text(
            "❌ <b>No Trader Data Stored</b>\n\n"
            "Analyze a token first to generate data.\n\n"
            "<b>Example:</b>\n"
            "<code>DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Count trader types
    cabal_result = context.user_data.get('last_cabal_result', {})
    
    status_text = f"""✅ <b>Trader Storage Verification</b>

<b>📊 Stored Data:</b>
• <b>Total Traders:</b> {len(all_traders)}
• <b>Token:</b> <code>{last_token[:10]}...{last_token[-8:]}</code>
• <b>Chain:</b> {last_chain.upper() if last_chain else 'Unknown'}
• <b>Stored At:</b> {timestamp[:19] if timestamp else 'Unknown'}

<b>📈 Display vs Export:</b>
• <b>Bot displays:</b> Top 10 traders ✅
• <b>CSV exports:</b> All {len(all_traders)} traders ✅

<b>💾 Sample Trader Data:</b>
"""
    
    if all_traders:
        sample = all_traders[0]
        status_text += f"""<b>Rank #1:</b>
• Wallet: <code>{sample.get('wallet', 'N/A')[:20]}...</code>
• Profit: ${sample.get('total_profit', 0):,.2f}
• Win Rate: {sample.get('win_rate', 0):.1f}%
• Trades: {sample.get('total_trades', 0)}
• Status: {'HOLDING' if sample.get('is_holder') else 'EXITED'}
"""
    
    # Check premium status
    subscription = db.get_user_subscription(user_id)
    is_premium = 'premium' in subscription['tier']
    
    status_text += f"\n<b>🔐 Export Status:</b>\n"
    if is_premium:
        status_text += f"✅ Premium access confirmed\n"
        status_text += f"✅ Can export all {len(all_traders)} traders\n"
        status_text += f"✅ Complete data with PNL, win rates, top tokens\n"
        status_text += f"\n→ Use /export_csv to download"
    else:
        status_text += f"❌ Free/Basic tier\n"
        status_text += f"→ Upgrade to Premium for CSV export\n"
        status_text += f"→ /subscription to view plans"
    
    # Add cabal info if available
    if cabal_result:
        cabals = cabal_result.get('cabals', [])
        suspicious = cabal_result.get('suspicious_wallets', [])
        status_text += f"\n\n<b>🔍 Cabal Detection:</b>"
        status_text += f"\n• Cabals: {len(cabals)}"
        status_text += f"\n• Suspicious: {len(suspicious)}"
    
    await update.message.reply_text(status_text, parse_mode=ParseMode.HTML)

async def verify_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /verify command for payment verification"""
    await sub_manager.handle_payment_verification(update, context)

async def check_verification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /check_verification command"""
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b> /check_verification &lt;verification_id&gt;",
            parse_mode=ParseMode.HTML
        )
        return
    
    verification_id = int(context.args[0])
    status = db.get_verification_status(verification_id, user_id)
    
    if not status:
        await update.message.reply_text(
            "❌ Verification not found or you don't have access to it.",
            parse_mode=ParseMode.HTML
        )
        return
    
    status_emoji = {
        'pending': '⏳',
        'approved': '✅',
        'rejected': '❌'
    }.get(status['status'], '❓')
    
    message = f"""
{status_emoji} <b>Payment Verification Status</b>

<b>ID:</b> #{verification_id}
<b>Status:</b> {status['status'].upper()}
<b>Plan:</b> {status['tier']}
<b>Amount:</b> ${status['amount']}
<b>Submitted:</b> {status['submitted_at']}
"""
    
    if status['status'] == 'approved':
        message += f"\n✅ <b>Approved at:</b> {status['verified_at']}"
        message += f"\n🎉 Your subscription is now active!"
    elif status['status'] == 'rejected':
        message += f"\n❌ <b>Rejected at:</b> {status['verified_at']}"
        message += f"\n<b>Reason:</b> {status.get('notes', 'No reason provided')}"
    else:
        message += f"\n⏳ Verification pending. Usually takes 1-24 hours."
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

# Admin commands
async def approve_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /approve command (admin only)"""
    await admin_manager.handle_approve_payment(update, context)

async def reject_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reject command (admin only)"""
    await admin_manager.handle_reject_payment(update, context)

async def pending_verifications(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /pending command (admin only)"""
    await admin_manager.show_pending_verifications(update, context)

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /adminstats command (admin only)"""
    await admin_manager.show_admin_stats(update, context)

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /broadcast command (admin only)"""
    await admin_manager.broadcast_message(update, context)

# ============================================================================
# STEP 5: Update callback handler to include subscription callbacks
# ============================================================================
async def help_command_callback(query, context: ContextTypes.DEFAULT_TYPE):
    """Help command handler for callbacks"""
    help_text = """📚 <b>CoinWinRBot - Complete Guide</b>

<b>🔥 SMART DETECTION:</b>
Just paste any blockchain address - I'll automatically detect if it's a wallet or token!

<b>🎯 POWERFUL FEATURES:</b>

<b>1️⃣ Find Token's Top Traders</b>
   • Paste any token contract address
   • Get ranked list of profitable traders
   • 🆕 <b>Cabal Detection</b> - Spot coordinated groups!
   
<b>2️⃣ Analyze Wallet Performance</b>
   • Paste any wallet address
   • View complete trading statistics
   
<b>3️⃣ Cabal Detection (Premium)</b> 🆕
   • Spot coordinated wallets
   • Identify pump & dump schemes

<b>⚡ AVAILABLE COMMANDS:</b>

<code>/start</code> - Welcome & status
<code>/analyze &lt;address&gt;</code> - Analyze wallet/token
<code>/subscription</code> - View plans
<code>/stats</code> - Your statistics
<code>/help</code> - This guide

<b>❓ Need Help?</b>
Contact: @YourSupport"""
    
    await query.message.reply_text(help_text, parse_mode=ParseMode.HTML)


async def stats_command_callback(query, context: ContextTypes.DEFAULT_TYPE):
    """
    FIXED: Stats callback handler with accurate usage
    """
    user_id = query.from_user.id
    user = query.from_user
    
    try:
        # Get usage info
        usage_info = db.get_user_daily_usage(user_id)
        
        # Get subscription status
        status = sub_manager.get_subscription_status(user_id)
        tier_emoji = get_tier_emoji(status['tier'])
        
        # Calculate usage bar
        if usage_info['daily_limit'] == -1:
            usage_bar = '████████████ 100% (Unlimited)'
            usage_percentage = 100
        elif usage_info['daily_limit'] > 0:
            usage_percentage = min(100, (usage_info['daily_analyses'] / usage_info['daily_limit'] * 100))
            filled = int(usage_percentage / 10)
            usage_bar = '█' * filled + '░' * (10 - filled) + f" {usage_percentage:.0f}%"
        else:
            usage_bar = '░░░░░░░░░░ 0%'
            usage_percentage = 0
        
        # Expiry display
        if status['expires_at']:
            days_left = status['days_remaining']
            if days_left > 0:
                expiry_display = f"{status['expires_at'].strftime('%Y-%m-%d')} ({days_left} days)"
                renewal_warning = "\n⚠️ <i>Renew soon!</i>" if days_left <= 3 else ""
            else:
                expiry_display = "⚠️ EXPIRED"
                renewal_warning = "\n🔔 <i>Renew now!</i>"
        else:
            expiry_display = "Never (Free tier)"
            renewal_warning = ""
        
        # Premium badge
        if 'premium' in status['tier']:
            badge = '👑 <b>PREMIUM STATUS</b> 👑'
        elif 'basic' in status['tier']:
            badge = '💰 <b>PAID MEMBER</b>'
        else:
            badge = '📊 <b>FREE USER</b>'
        
        # Feature checklist
        features = {
            'Wallet Analysis': '✅',
            'Token Analysis': '✅' if check_feature_access(user_id, 'extended_features') or 'all' in status['features'] else '❌',
            'Cabal Detection': '✅' if check_feature_access(user_id, 'extended_features') or 'all' in status['features'] else '❌',
            'CSV Export': '✅' if 'premium' in status['tier'] else '❌',
            'Batch Analysis': '✅' if 'batch_analysis' in status['features'] or 'all' in status['features'] else '❌',
            'All Chains': '✅' if 'all' in status['chains'] else '❌',
            'Priority Support': '✅' if 'premium' in status['tier'] else '❌'
        }
        features_str = '\n'.join([f"  {k}: {v}" for k, v in features.items()])
        
        # Calculate limits and remaining
        if usage_info['daily_limit'] == -1:
            daily_limit_str = '∞'
            remaining_str = 'Unlimited'
            progress_text = f"Used today: {usage_info['daily_analyses']} analyses"
        else:
            daily_limit_str = str(usage_info['daily_limit'])
            remaining = usage_info['remaining']
            remaining_str = str(remaining)
            progress_text = f"{usage_info['daily_analyses']}/{daily_limit_str} used • {remaining} remaining"
        
        # Reset timing
        reset_hours = usage_info.get('reset_in_hours', 24)
        reset_text = f"Resets in: {reset_hours} hours" if reset_hours < 24 else "Resets: Tomorrow"
        
        # Escape user information
        user_name = html_escape(user.first_name or 'Unknown')
        username_display = f"@{html_escape(user.username)}" if user.username else 'Not set'
        
        stats_text = f"""📊 <b>Your Account Statistics</b>

{badge}

<b>👤 Account Details:</b>
• Name: {user_name}
• Username: {username_display}
• User ID: <code>{user_id}</code>

<b>💎 Subscription Info:</b>
• Plan: {tier_emoji} <b>{status['tier_name']}</b>
• Status: {'🟢 Active' if status['is_active'] else '🔴 Inactive'}
• Expires: {expiry_display}{renewal_warning}

<b>📈 Usage Today:</b>
• {progress_text}
• {reset_text}
{usage_bar}

<b>🎯 Available Features:</b>
{features_str}

<b>💡 Quick Actions:</b>
• /subscription - View all plans
• /help - Full guide
{'• /premium_features - See your benefits' if 'premium' in status['tier'] else '• Upgrade to get unlimited access'}

<i>Last updated: {datetime.now().strftime('%H:%M:%S')}</i>"""
        
        keyboard = []
        if 'premium' not in status['tier']:
            keyboard.append([InlineKeyboardButton("💎 Upgrade to Premium", callback_data='view_plans')])
        keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data='stats')])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(
            stats_text, 
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Stats callback error: {e}", exc_info=True)
        try:
            await query.message.edit_text(
                "❌ Error loading stats. Please try /stats again.",
                parse_mode=ParseMode.HTML
            )
        except:
            pass

async def subscription_command_callback(query, context: ContextTypes.DEFAULT_TYPE):
    """Subscription command handler for callbacks"""
    user_id = query.from_user.id
    current_sub = db.get_user_subscription(user_id)
    
    # Get tier info
    tier_config = get_tier_config(current_sub['tier'])
    expires = current_sub.get('expires_at')
    
    # Convert string to datetime if needed
    if expires:
        if isinstance(expires, str):
            try:
                from dateutil import parser
                expires = parser.parse(expires)
            except ImportError:
                try:
                    expires = datetime.strptime(expires, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        expires = datetime.strptime(expires, '%Y-%m-%d')
                    except ValueError:
                        expires = None
    
    expires_str = expires.strftime('%Y-%m-%d %H:%M') if expires else 'Never'
    daily_limit = current_sub['daily_limit'] if current_sub['daily_limit'] > 0 else '∞'
    
    subscription_text = f"""💎 <b>CoinWinRBot Subscription Plans</b>

<b>📊 Your Current Plan:</b>
- <b>Plan:</b> {tier_config['name']}
- <b>Daily Limit:</b> {daily_limit} analyses
- <b>Used Today:</b> {current_sub['daily_analyses']}
- <b>Expires:</b> {expires_str}

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>📈 AVAILABLE PLANS:</b>

"""
    
    # Build improved plan list
    for tier_key, tier_data in sub_manager.tiers.items():
        if tier_key == 'free':
            subscription_text += f"""<b>{tier_data['name']}</b>
- Analyses: {tier_data['daily_limit']}/day
- Chains: Solana, BSC
- Features: Basic wallet analysis
- Support: Community
- Price: <b>FREE</b>

"""
        else:
            # Analyses text
            analyses_text = "Unlimited" if tier_data['daily_limit'] == -1 else f"{tier_data['daily_limit']}/day"
            
            # Features list
            features = ["Wallet analysis"]
            if 'batch_analysis' in tier_data['features']:
                features.append(f"Batch analysis ({tier_data['batch_limit']}x)")
            if 'extended_features' in tier_data['features'] or 'all' in tier_data['features']:
                features.append("Token trader discovery ⭐")
                features.append("Cabal detection 🔍")
                features.append("CSV export 📥")
            
            # Chains
            chains_text = "All chains (8+)" if 'all' in tier_data['chains'] else f"{len(tier_data['chains'])} chains"
            
            # Duration
            duration = "week" if 'weekly' in tier_key else "month"
            
            subscription_text += f"""<b>{tier_data['name']}</b>
- Analyses: <b>{analyses_text}</b>
- Chains: {chains_text}
- Features: {', '.join(features)}
- Price: <b>${tier_data['price']}/{duration}</b>

"""
    
    # Payment information
    payment_info = sub_manager.payment_info['crypto']
    support = sub_manager.payment_info['support_username']
    
    subscription_text += f"""<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>💳 PAYMENT METHODS:</b>

<b>1️⃣ USDT (TRC20)</b> - Recommended ⚡
<code>{payment_info['usdt_trc20']}</code>
<i>Lowest fees (~$1)</i>

<b>2️⃣ USDT (ERC20)</b>
<code>{payment_info['usdt_erc20']}</code>
<i>Higher fees (~$5-20)</i>

<b>3️⃣ Bitcoin (BTC)</b>
<code>{payment_info['btc']}</code>

<b>4️⃣ Solana (SOL)</b>
<code>{payment_info['sol']}</code>

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>📝 HOW TO UPGRADE:</b>

<b>Step 1:</b> Choose your plan
<b>Step 2:</b> Send exact USD amount
<b>Step 3:</b> Submit verification:

<code>/verify &lt;plan&gt; &lt;tx_hash&gt;</code>

<b>Example:</b>
<code>/verify premium_monthly 0x123...abc</code>

<b>Plans:</b> basic_weekly, basic_monthly, premium_weekly, premium_monthly

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>❓ Need Help?</b> Contact @{support}
<b>Questions?</b> Use /help"""
    
    keyboard = [
        [InlineKeyboardButton("💳 View Payment Guide", callback_data='payment_info')],
        [InlineKeyboardButton("💬 Contact Support", url=f"https://t.me/{support}")],
        [InlineKeyboardButton("🔙 Back to Menu", callback_data='back_to_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.message.edit_text(
        subscription_text, 
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )
    
async def start_command_callback(query, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler for callbacks"""
    user = query.from_user
    user_id = user.id
    
    db.register_user(user_id, user.username, user.first_name)
    subscription = db.get_user_subscription(user_id)
    
    # Get tier info from SubscriptionManager
    tier = subscription['tier']
    tier_config = get_tier_config(tier)
    tier_emoji = get_tier_emoji(tier)
    tier_name = tier_config['name']
    
    # Calculate remaining analyses
    if subscription['daily_limit'] == -1:
        daily_limit_str = '∞ Unlimited'
        remaining = '∞'
        usage_bar = '100%'
    else:
        daily_limit_str = str(subscription['daily_limit'])
        remaining = subscription['daily_limit'] - subscription['daily_analyses']
        usage_percent = (subscription['daily_analyses'] / subscription['daily_limit'] * 100) if subscription['daily_limit'] > 0 else 0
        filled = int(usage_percent / 10)
        usage_bar = '█' * filled + '░' * (10 - filled) + f' {usage_percent:.0f}%'
    
    # Premium status badge
    if 'premium' in tier:
        status_badge = '👑 <b>PREMIUM MEMBER</b> 👑'
        feature_highlight = """
<b>🌟 Your Premium Benefits:</b>
✅ Unlimited analyses per day
✅ Token trader discovery
✅ Cabal detection enabled
✅ All chains supported
✅ Priority support
✅ Advanced analytics"""
    elif 'basic' in tier:
        status_badge = '💰 <b>BASIC MEMBER</b>'
        has_token_analysis = 'extended_features' in tier_config['features']
        feature_highlight = f"""
<b>✨ Your Basic Benefits:</b>
✅ {subscription['daily_limit']} analyses per day
✅ Wallet analysis
✅ Multi-chain support
{'✅ Token trader analysis' if has_token_analysis else '❌ Token analysis (upgrade)'}
{'✅ Cabal detection' if has_token_analysis else '❌ Cabal detection (upgrade)'}"""
    else:
        status_badge = '📊 <b>FREE MEMBER</b>'
        feature_highlight = """
<b>🎯 Free Tier Features:</b>
✅ 5 analyses per day
✅ Basic wallet analysis
✅ Ethereum & BSC chains
❌ Token trader analysis
❌ Cabal detection
❌ Advanced features"""
    
    # Expiry info
    if subscription.get('expires_at'):
        expires = subscription['expires_at']
        if isinstance(expires, str):
            expires = datetime.fromisoformat(expires)
        days_left = (expires - datetime.now()).days
        
        if days_left > 0:
            expiry_str = f"⏰ Expires in: <b>{days_left} days</b>"
            if days_left <= 3:
                expiry_str += " ⚠️ <i>Renew soon!</i>"
        else:
            expiry_str = "⚠️ <b>EXPIRED</b> - Please renew"
    else:
        expiry_str = "♾️ No expiration"
    
    welcome_message = f"""🤖 <b>Welcome to CoinWinRBot!</b> 🚀

Hello {html_escape(user.first_name)}! 👋

{status_badge}

<b>📊 Account Status:</b>
{tier_emoji} <b>Plan:</b> {tier_name}
📈 <b>Daily Usage:</b> {subscription['daily_analyses']}/{daily_limit_str}
{usage_bar}
💎 <b>Remaining:</b> {remaining} analyses today
{expiry_str}

{feature_highlight}

<b>🔥 What I Can Do:</b>

<b>1️⃣ Analyze Wallets</b>
   • View win rates, profits, patterns
   • Track successful traders
   • Multi-chain support

<b>2️⃣ Find Token's Top Traders</b> {'✅' if check_feature_access(user_id, 'extended_features') or 'all' in tier_config['features'] else '🔒'}
   • Paste any token contract address
   • Get ranked list of profitable traders
   • Discover who's winning!

<b>3️⃣ Cabal Detection</b> {'✅' if check_feature_access(user_id, 'extended_features') or 'all' in tier_config['features'] else '🔒'}
   • Spot coordinated trading groups
   • Identify pump & dump schemes
   • Protect your investments

<b>⚡ Just Paste Any Address!</b>
• <b>Wallet:</b> <code>0x1234...abcd</code>
• <b>Token CA:</b> <code>HtrmuNs4nESVg5i8g...</code>
• <b>I'll detect it automatically!</b>

<b>💡 Quick Test:</b>
<code>DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263</code>
<i>(Bonk token - tap to copy!)</i>

<b>🎯 Supported Chains:</b>
Ethereum • BSC • Polygon • Arbitrum • Base • Solana

Ready to find alpha? 🚀"""
    
    # Dynamic buttons based on tier
    if 'premium' in tier:
        keyboard = [
            [InlineKeyboardButton("🚀 Analyze Now", callback_data='quick_analyze'),
             InlineKeyboardButton("📊 My Stats", callback_data='stats')],
            [InlineKeyboardButton("👑 Premium Features", callback_data='premium_features'),
             InlineKeyboardButton("❓ Help", callback_data='help')]
        ]
    else:
        keyboard = [
            [InlineKeyboardButton("📈 Try It Now", callback_data='quick_analyze'),
             InlineKeyboardButton("💎 Upgrade", callback_data='view_plans')],
            [InlineKeyboardButton("📊 My Stats", callback_data='stats'),
             InlineKeyboardButton("❓ Help", callback_data='help')]
        ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.message.reply_text(
        welcome_message,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )

async def premium_features_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show premium features (premium users only)"""
    user_id = update.effective_user.id
    subscription = db.get_user_subscription(user_id)
    
    if 'premium' not in subscription['tier']:
        await update.message.reply_text(
            "🔒 This command is only available to premium members.\n\n"
            "Upgrade to premium to unlock all features: /subscription",
            parse_mode=ParseMode.HTML
        )
        return
    
    features_text = """👑 <b>YOUR PREMIUM FEATURES</b> 👑

Congratulations! You have full access to all premium features:

<b>🚀 Unlimited Analysis</b>
✅ No daily limits
✅ Analyze as many wallets as you need
✅ Perfect for serious traders

<b>🎯 Token Trader Discovery</b>
✅ Find top traders for any token
✅ See who's making money
✅ Study winning strategies
✅ Analyze up to 100 traders per token

<b>🕵️ Cabal Detection</b>
✅ Spot coordinated trading groups
✅ Identify pump & dump schemes
✅ See wallet clustering patterns
✅ Protect yourself from manipulation

<b>🌐 All Chains Supported</b>
✅ Ethereum (ETH)
✅ BNB Smart Chain (BSC)
✅ Polygon (MATIC)
✅ Arbitrum (ARB)
✅ Base
✅ Solana (SOL)

<b>⚡ Advanced Features</b>
✅ Batch analysis (up to 50 wallets)
✅ Priority processing
✅ Detailed analytics
✅ Export capabilities
✅ Real-time updates

<b>🎁 Exclusive Benefits</b>
✅ 24/7 Priority support
✅ Early access to new features
✅ Custom analysis requests
✅ No rate limiting
✅ Premium-only insights

<b>💡 Pro Tips:</b>
• Use token analysis to find alpha
• Check for cabals before investing
• Track successful traders
• Analyze before every trade

<b>Need help?</b> Contact @YourSupport
<b>Questions?</b> Use /help

Thank you for being a premium member! 🎉"""
    
    keyboard = [
        [InlineKeyboardButton("🚀 Start Analyzing", callback_data='quick_analyze')],
        [InlineKeyboardButton("📊 View Stats", callback_data='stats')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        features_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )

    """Enhanced callback handler with subscription support"""
    query = update.callback_query
    await query.answer()
    
    if query.data == 'quick_analyze':
        await query.message.reply_text(
            "🎯 <b>Ready to Analyze!</b>\n\n"
            "<b>Just paste an address:</b>\n\n"
            "• <b>Token CA</b> → Find top traders + Cabal detection\n"
            "• <b>Wallet</b> → Analyze performance\n\n"
            "<b>💡 Test Examples:</b>\n\n"
            "<b>Bonk Token (Solana):</b>\n"
            "<code>DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263</code>\n\n"
            "<b>SHIB Token (Ethereum):</b>\n"
            "<code>0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE</code>\n\n"
            "Tap to copy, then paste here!",
            parse_mode=ParseMode.HTML
        )
        
    elif query.data == 'help':
        fake_update = Update(update_id=update.update_id, message=query.message)
        await help_command(fake_update, context)
        
    elif query.data == 'view_plans':
        fake_update = Update(update_id=update.update_id, message=query.message)
        await subscription_command(fake_update, context)
        
    elif query.data == 'stats':
        fake_update = Update(update_id=update.update_id, message=query.message)
        await stats_command(fake_update, context)
        
    elif query.data == 'back_to_menu':
        fake_update = Update(update_id=update.update_id, message=query.message)
        await start(fake_update, context)
    
    # Subscription-specific callbacks
    elif query.data.startswith('sub_'):
        await sub_manager.handle_subscription_callback(update, context)
    
    elif query.data == 'payment_info':
        await sub_manager.show_general_payment_info(query)
    
    elif query.data.startswith('paid_'):
        tier = query.data.replace('paid_', '')
        await query.edit_message_text(
            f"✅ <b>Payment Received!</b>\n\n"
            f"Please submit verification using:\n"
            f"<code>/verify {tier} &lt;transaction_hash&gt;</code>\n\n"
            f"<b>Example:</b>\n"
            f"<code>/verify {tier} 0x1234...abcd</code>",
            parse_mode=ParseMode.HTML
        )       

async def handle_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced callback handler with chain selection"""
    
    if not update.callback_query:
        return
    
    query = update.callback_query
    
    try:
        await query.answer()
    except Exception as e:
        logger.debug(f"Callback answer error (non-critical): {e}")
    
    user_id = query.from_user.id
    
    try:
        # ⭐ Handle chain selection callbacks
        if query.data.startswith('analyze_'):
            # Format: analyze_<address>_<chain>
            parts = query.data.split('_', 2)
            if len(parts) == 3:
                _, address, chain = parts
                
                # Trigger analysis (will ask for timeframe if EVM wallet)
                await perform_analysis(update, context, address, user_id, specified_chain=chain)
                return
        
        # ⭐ NEW: Handle timeframe selection callbacks
        if query.data.startswith('timeframe_'):
            # Format: timeframe_<address>_<chain>_<days>
            parts = query.data.split('_', 3)
            if len(parts) == 4:
                _, address, chain, days_str = parts
                days = int(days_str)
                
                # Start analysis with selected timeframe
                await perform_analysis(update, context, address, user_id, 
                                      specified_chain=chain, timeframe_days=days)
                return
        if query.data == 'quick_analyze':
            await query.message.reply_text(
                "🎯 <b>Ready!</b>\n\n"
                "<b>Paste address:</b>\n"
                "• Just address (will ask chain for EVM)\n"
                "• Or: <code>address chain</code>\n\n"
                "<b>Examples:</b>\n"
                "<code>0x95aD...C4cE ethereum</code>\n"
                "<code>0xe9e7...7D56 bsc</code>",
                parse_mode=ParseMode.HTML
            )
            
        elif query.data == 'export_csv':
            await export_last_cabal_results(update, context)
        
        elif query.data == 'admin_refresh':
            if user_id not in ADMIN_USER_IDS:
                await query.message.reply_text("🔒 Admin only")
                return
            await admin_panel_command(update, context)
        
        elif query.data == 'help':
            await help_command_callback(query, context)
        
        elif query.data == 'stats':
            await stats_command_callback(query, context)
        
        elif query.data == 'view_plans':
            await subscription_command_callback(query, context)
        
        elif query.data == 'back_to_menu':
            await start_command_callback(query, context)
        
        elif query.data.startswith('sub_'):
            await sub_manager.handle_subscription_callback(update, context)
        
        elif query.data == 'payment_info':
            await sub_manager.show_general_payment_info(query)
        
        elif query.data.startswith('paid_'):
            tier = query.data.replace('paid_', '')
            await query.edit_message_text(
                f"✅ <b>Payment Received!</b>\n\n"
                f"Submit verification:\n"
                f"<code>/verify {tier} &lt;tx_hash&gt;</code>",
                parse_mode=ParseMode.HTML
            )
        
    except Exception as e:
        logger.error(f"Callback error: {e}", exc_info=True)
        await query.message.reply_text("❌ Error. Please try again.")


async def check_evm_concurrent(address: str, session: aiohttp.ClientSession, chain: str = None) -> tuple:
    """
    ✅ FIXED: Better token detection for EVM chains
    Now properly identifies ERC20 tokens instead of generic "contract"
    """
    
    # Use chain-specific RPC if available
    if chain:
        chain_rpcs = {
            'ethereum': [
                "https://eth.llamarpc.com",
                "https://rpc.ankr.com/eth",
                "https://ethereum.publicnode.com"
            ],
            'bsc': [
                "https://bsc-dataseed1.binance.org",
                "https://rpc.ankr.com/bsc",
                "https://bsc.publicnode.com"
            ],
            'polygon': [
                "https://polygon-rpc.com",
                "https://rpc.ankr.com/polygon",
                "https://polygon.publicnode.com"
            ],
            'arbitrum': [
                "https://arb1.arbitrum.io/rpc",
                "https://rpc.ankr.com/arbitrum",
                "https://arbitrum.publicnode.com"
            ],
            'optimism': [
                "https://mainnet.optimism.io",
                "https://rpc.ankr.com/optimism",
                "https://optimism.publicnode.com"
            ],
            'base': [
                "https://mainnet.base.org",
                "https://base.publicnode.com"
            ],
            'avalanche': [
                "https://api.avax.network/ext/bc/C/rpc",
                "https://rpc.ankr.com/avalanche",
                "https://avalanche.publicnode.com"
            ]
        }
        
        rpc_urls = chain_rpcs.get(chain, chain_rpcs['ethereum'])
    else:
        rpc_urls = [
            "https://eth.llamarpc.com",
            "https://rpc.ankr.com/eth",
            "https://ethereum.publicnode.com"
        ]
    
    async def try_single_rpc(rpc_url: str):
        try:
            loop = asyncio.get_event_loop()
            
            def sync_check():
                w3 = Web3(Web3.HTTPProvider(
                    rpc_url,
                    request_kwargs={'timeout': 8}
                ))
                
                if not w3.is_connected():
                    return None
                
                checksum_addr = Web3.to_checksum_address(address)
                code = w3.eth.get_code(checksum_addr)
                
                # If no code, it's a wallet
                if code == b'' or code == b'\x00':
                    return ('wallet', {'confidence': 'high', 'reason': 'no_code'})
                
                # Has code - try to detect if it's a token
                # ⭐ IMPROVED: Try multiple ERC20 methods
                try:
                    # 1. totalSupply() - Most reliable
                    total_supply_selector = '0x18160ddd'
                    result = w3.eth.call({
                        'to': checksum_addr,
                        'data': total_supply_selector
                    }, timeout=5)
                    
                    if result and len(result) == 32 and result != b'\x00' * 32:
                        # Has valid totalSupply - definitely a token
                        return ('token', {'confidence': 'high', 'method': 'totalSupply'})
                    
                    # 2. symbol() - Second check
                    symbol_selector = '0x95d89b41'
                    result = w3.eth.call({
                        'to': checksum_addr,
                        'data': symbol_selector
                    }, timeout=5)
                    
                    if result and len(result) > 0:
                        # Has symbol - likely a token
                        return ('token', {'confidence': 'high', 'method': 'symbol'})
                    
                    # 3. decimals() - Third check
                    decimals_selector = '0x313ce567'
                    result = w3.eth.call({
                        'to': checksum_addr,
                        'data': decimals_selector
                    }, timeout=5)
                    
                    if result and len(result) > 0:
                        # Has decimals - likely a token
                        return ('token', {'confidence': 'medium', 'method': 'decimals'})
                    
                    # 4. name() - Last check
                    name_selector = '0x06fdde03'
                    result = w3.eth.call({
                        'to': checksum_addr,
                        'data': name_selector
                    }, timeout=5)
                    
                    if result and len(result) > 0:
                        # Has name - might be a token
                        return ('token', {'confidence': 'medium', 'method': 'name'})
                    
                except Exception as e:
                    logger.debug(f"ERC20 check error: {e}")
                
                # Has code but no token methods - probably a contract
                # ⭐ CHANGED: Return 'token' anyway for safety
                # Better to try token analysis and fail than skip it
                return ('token', {'confidence': 'low', 'reason': 'has_code_no_methods'})
            
            result = await asyncio.wait_for(
                loop.run_in_executor(thread_pool, sync_check),
                timeout=10.0
            )
            return result
            
        except Exception as e:
            logger.debug(f"RPC {rpc_url} failed: {e}")
            return None
    
    # Try RPCs in parallel
    tasks = [asyncio.create_task(try_single_rpc(url)) for url in rpc_urls]
    
    try:
        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                if result:
                    # Cancel remaining tasks
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    
                    logger.info(f"✅ EVM detection: {result[0]} (confidence: {result[1].get('confidence')})")
                    return result
            except Exception as e:
                logger.debug(f"RPC attempt failed: {e}")
                continue
        
        # All failed - assume it might be a token
        logger.warning("All RPCs failed - assuming token for safety")
        return ('token', {'confidence': 'low', 'reason': 'all_rpcs_failed'})
        
    except Exception as e:
        logger.warning(f"All EVM RPCs failed: {e}")
        return ('token', {'confidence': 'low', 'reason': 'exception'})
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command with subscription tier features"""
    
    help_text = """📚 <b>CoinWinRBot - Complete Guide</b>

<b>🔥 SMART DETECTION:</b>
Just paste any blockchain address - I'll automatically detect if it's a wallet or token!

<b>🎯 POWERFUL FEATURES:</b>

<b>1️⃣ Find Token's Top Traders</b>
   • Paste any token contract address
   • Get ranked list of profitable traders
   • 🆕 <b>Cabal Detection</b> - Spot coordinated groups!
   
<b>2️⃣ Analyze Wallet Performance</b>
   • Paste any wallet address
   • View complete trading statistics
   
<b>3️⃣ Cabal Detection (Premium)</b> 🆕
   • Spot coordinated wallets
   • Identify pump & dump schemes

<b>⚡ AVAILABLE COMMANDS:</b>

<code>/start</code> - Welcome & status
<code>/analyze &lt;address&gt;</code> - Analyze wallet/token
<code>/subscription</code> - View plans
<code>/stats</code> - Your statistics
<code>/help</code> - This guide

<b>❓ Need Help?</b>
Contact: @YourSupport"""
    
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    COMPLETE FIX: Show user statistics with proper unlimited display
    Handles both /stats command AND callback queries from buttons
    """
    user_id = update.effective_user.id
    user = update.effective_user
    
    # Determine if this is a callback query or direct command
    is_callback = update.callback_query is not None
    
    try:
        # Get subscription data
        subscription = db.get_user_subscription(user_id)
        status = sub_manager.get_subscription_status(user_id)
        tier_emoji = get_tier_emoji(status['tier'])
        
        # Extract values
        tier = subscription['tier']
        daily_analyses = subscription['daily_analyses']
        daily_limit = subscription['daily_limit']
        
        # Calculate remaining and usage display
        if daily_limit == -1:
            # Unlimited plan
            remaining_str = '∞ Unlimited'
            progress_text = f"<b>{daily_analyses}</b> analyses used today (Unlimited plan)"
            usage_bar = '█' * 12 + ' ∞'
            usage_percentage = 100
        else:
            # Limited plan
            remaining = max(0, daily_limit - daily_analyses)
            remaining_str = f"{remaining}"
            progress_text = f"<b>{daily_analyses}/{daily_limit}</b> used • <b>{remaining}</b> remaining"
            
            # Calculate usage bar
            if daily_limit > 0:
                usage_percentage = min(100, (daily_analyses / daily_limit * 100))
                filled = int(usage_percentage / 10)
                usage_bar = '█' * filled + '░' * (10 - filled) + f" {usage_percentage:.0f}%"
            else:
                usage_bar = '░' * 10 + ' 0%'
                usage_percentage = 0
        
        # Expiry display
        if status['expires_at']:
            days_left = status['days_remaining']
            if days_left > 0:
                expiry_display = f"{status['expires_at'].strftime('%Y-%m-%d')} ({days_left} days)"
                renewal_warning = "\n⚠️ <i>Renew soon!</i>" if days_left <= 3 else ""
            else:
                expiry_display = "⚠️ EXPIRED"
                renewal_warning = "\n🔔 <i>Renew now!</i>"
        else:
            expiry_display = "Never (Free tier)"
            renewal_warning = ""
        
        # Premium badge
        if 'premium' in status['tier']:
            badge = '👑 <b>PREMIUM STATUS</b> 👑'
        elif 'basic' in status['tier']:
            badge = '💰 <b>PAID MEMBER</b>'
        else:
            badge = '📊 <b>FREE USER</b>'
        
        # Feature checklist
        features = {
            'Wallet Analysis': '✅',
            'Token Analysis': '✅' if check_feature_access(user_id, 'extended_features') or 'all' in status['features'] else '❌',
            'Cabal Detection': '✅' if check_feature_access(user_id, 'extended_features') or 'all' in status['features'] else '❌',
            'CSV Export': '✅' if 'premium' in status['tier'] else '❌',
            'Batch Analysis': '✅' if 'batch_analysis' in status['features'] or 'all' in status['features'] else '❌',
            'All Chains': '✅' if 'all' in status['chains'] else '❌',
            'Priority Support': '✅' if 'premium' in status['tier'] else '❌'
        }
        features_str = '\n'.join([f"  {k}: {v}" for k, v in features.items()])
        
        # Escape user information
        user_name = html_escape(user.first_name or 'Unknown')
        username_display = f"@{html_escape(user.username)}" if user.username else 'Not set'
        
        # Build stats message
        stats_text = f"""📊 <b>Your Account Statistics</b>

{badge}

<b>👤 Account Details:</b>
• Name: {user_name}
• Username: {username_display}
• User ID: <code>{user_id}</code>

<b>💎 Subscription Info:</b>
• Plan: {tier_emoji} <b>{status['tier_name']}</b>
• Status: {'🟢 Active' if status['is_active'] else '🔴 Inactive'}
• Expires: {expiry_display}{renewal_warning}

<b>📈 Usage Today:</b>
• {progress_text}
{usage_bar}

<b>🎯 Available Features:</b>
{features_str}

<b>💡 Quick Actions:</b>
• /subscription - View all plans
• /help - Full guide
{'• /premium_features - See your benefits' if 'premium' in status['tier'] else '• Upgrade to get unlimited access'}

<i>⏰ Updated: {datetime.now().strftime('%H:%M:%S')}</i>"""
        
        # Build keyboard
        keyboard = []
        if 'premium' not in status['tier']:
            keyboard.append([InlineKeyboardButton("💎 Upgrade to Premium", callback_data='view_plans')])
        keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data='stats')])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Send response based on update type
        if is_callback:
            # Answer callback query first
            await update.callback_query.answer("Stats refreshed! ✅")
            # Edit the existing message
            await update.callback_query.message.edit_text(
                stats_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
        else:
            # Reply to the message command
            await update.message.reply_text(
                stats_text, 
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
        
    except Exception as e:
        logger.error(f"Stats command error: {e}", exc_info=True)
        
        error_message = "❌ Error loading stats. Please try again."
        
        # Send error based on update type
        if is_callback:
            await update.callback_query.answer("Error loading stats!", show_alert=True)
            try:
                await update.callback_query.message.edit_text(
                    error_message,
                    parse_mode=ParseMode.HTML
                )
            except:
                pass  # Message might be too old to edit
        else:
            await update.message.reply_text(
                error_message,
                parse_mode=ParseMode.HTML
            )

async def debug_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Simple test command to verify bot is responding"""
    await update.message.reply_text(
        "✅ <b>Bot is working!</b>\n\n"
        "Commands are being received properly.",
        parse_mode=ParseMode.HTML
    )

async def subscription_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /subscription command with improved display"""
    user_id = update.effective_user.id
    current_sub = db.get_user_subscription(user_id)
    
    # Get tier info
    tier_config = get_tier_config(current_sub['tier'])
    expires = current_sub.get('expires_at')
    
    # Convert string to datetime if needed
    if expires:
        if isinstance(expires, str):
            try:
                from dateutil import parser
                expires = parser.parse(expires)
            except ImportError:
                try:
                    expires = datetime.strptime(expires, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        expires = datetime.strptime(expires, '%Y-%m-%d')
                    except ValueError:
                        expires = None
    
    expires_str = expires.strftime('%Y-%m-%d %H:%M') if expires else 'Never'
    daily_limit = current_sub['daily_limit'] if current_sub['daily_limit'] > 0 else '∞'
    
    subscription_text = f"""💎 <b>CoinWinRBot Subscription Plans</b>

<b>📊 Your Current Plan:</b>
- <b>Plan:</b> {tier_config['name']}
- <b>Daily Limit:</b> {daily_limit} analyses
- <b>Used Today:</b> {current_sub['daily_analyses']}
- <b>Expires:</b> {expires_str}

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>📈 AVAILABLE PLANS:</b>

"""
    
    # Build improved plan list
    for tier_key, tier_data in sub_manager.tiers.items():
        if tier_key == 'free':
            subscription_text += f"""<b>{tier_data['name']}</b>
- Analyses: {tier_data['daily_limit']}/day
- Chains: Solana, BSC
- Features: Basic wallet analysis
- Support: Community
- Price: <b>FREE</b>

"""
        else:
            # Analyses text
            analyses_text = "Unlimited" if tier_data['daily_limit'] == -1 else f"{tier_data['daily_limit']}/day"
            
            # Features list
            features = ["Wallet analysis"]
            if 'batch_analysis' in tier_data['features']:
                features.append(f"Batch analysis ({tier_data['batch_limit']}x)")
            if 'extended_features' in tier_data['features'] or 'all' in tier_data['features']:
                features.append("Token trader discovery ⭐")
                features.append("Cabal detection 🔍")
                features.append("CSV export 📥")
            
            # Chains
            chains_text = "All chains (8+)" if 'all' in tier_data['chains'] else f"{len(tier_data['chains'])} chains"
            
            # Duration
            duration = "week" if 'weekly' in tier_key else "month"
            
            subscription_text += f"""<b>{tier_data['name']}</b>
- Analyses: <b>{analyses_text}</b>
- Chains: {chains_text}
- Features: {', '.join(features)}
- Price: <b>${tier_data['price']}/{duration}</b>

"""
    
    # Payment information
    payment_info = sub_manager.payment_info['crypto']
    support = sub_manager.payment_info['support_username']
    
    subscription_text += f"""<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>💳 PAYMENT METHODS:</b>

<b>1️⃣ USDT (TRC20)</b> - Recommended ⚡
<code>{payment_info['usdt_trc20']}</code>
<i>Lowest fees (~$1)</i>

<b>2️⃣ USDT (ERC20)</b>
<code>{payment_info['usdt_erc20']}</code>
<i>Higher fees (~$5-20)</i>

<b>3️⃣ Bitcoin (BTC)</b>
<code>{payment_info['btc']}</code>

<b>4️⃣ Solana (SOL)</b>
<code>{payment_info['sol']}</code>

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>📝 HOW TO UPGRADE:</b>

<b>Step 1:</b> Choose your plan
<b>Step 2:</b> Send exact USD amount
<b>Step 3:</b> Submit verification:

<code>/verify &lt;plan&gt; &lt;tx_hash&gt;</code>

<b>Example:</b>
<code>/verify premium_monthly 0x123...abc</code>

<b>Plans:</b> basic_weekly, basic_monthly, premium_weekly, premium_monthly

<b>━━━━━━━━━━━━━━━━━━━━━━━━</b>

<b>❓ Need Help?</b> Contact @{support}
<b>Questions?</b> Use /help"""
    
    keyboard = [
        [InlineKeyboardButton("💳 View Payment Guide", callback_data='payment_info')],
        [InlineKeyboardButton("💬 Contact Support", url=f"https://t.me/{support}")],
        [InlineKeyboardButton("🔙 Back to Menu", callback_data='back_to_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        subscription_text, 
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )

async def health_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show system health status"""
    
    health = health_monitor.get_health_status()
    
    status_emoji = "✅" if health['status'] == 'healthy' else "⚠️"
    
    uptime_seconds = health['uptime_seconds']
    hours = uptime_seconds // 3600
    minutes = (uptime_seconds % 3600) // 60
    
    health_text = f"""🏥 <b>System Health Status</b>

<b>{status_emoji} Overall Status:</b> {health['status'].upper()}

<b>📊 Performance Metrics:</b>
• Total Requests: {health['total_requests']:,}
• Success Rate: {health['success_rate']}%
• Uptime: {hours}h {minutes}m

<b>🔧 Components:</b>
• Solana Analyzer: {'✅' if solana_analyzer.helius_api_key else '⚠️ Limited'}
• EVM Analyzer: ✅
• Database: ✅
• Token Analyzer: ✅

Current Status: <b>{status_emoji} {health['status'].upper()}</b>"""
    
    await update.message.reply_text(health_text, parse_mode=ParseMode.HTML)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Global error handler"""
    logger.error(f"Exception: {context.error}", exc_info=context.error)
    
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "❌ <b>An error occurred</b>\n\n"
                "Please try again or contact support.\n\n"
                "Support: @YourSupport",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to send error message: {e}")

async def verify_csv_data_storage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    NEW: Verify that CSV data is properly stored
    Usage: /verify_csv_storage (for testing)
    """
    user_id = update.effective_user.id
    
    # Get stored data
    all_traders = context.user_data.get('all_traders', [])
    last_token = context.user_data.get('last_token_address')
    last_chain = context.user_data.get('last_chain')
    cabal_result = context.user_data.get('last_cabal_result')
    timestamp = context.user_data.get('analysis_timestamp')
    
    # Get subscription
    subscription = db.get_user_subscription(user_id)
    is_premium = 'premium' in subscription['tier']
    
    # Build verification message
    status_text = f"""🔍 <b>CSV Data Storage Verification</b>

<b>User Info:</b>
• User ID: <code>{user_id}</code>
• Premium: {'✅ Yes' if is_premium else '❌ No'}
• Tier: {subscription['tier']}

<b>Stored Data:</b>
• Trader Data: {'✅ Yes' if all_traders else '❌ No'}
• Trader Count: {len(all_traders)}
• Token Address: {'✅ Yes' if last_token else '❌ No'}
• Chain: {last_chain or 'N/A'}
• Cabal Results: {'✅ Yes' if cabal_result else '❌ No'}
• Analysis Time: {timestamp or 'N/A'}

<b>Can Export CSV:</b> {'✅ YES' if (is_premium and all_traders) else '❌ NO'}

<b>Details:</b>"""
    
    if all_traders:
        # Sample first trader
        sample = all_traders[0]
        status_text += f"""
• First trader: <code>{sample.get('wallet', 'N/A')[:16]}...</code>
• First trader profit: ${sample.get('total_profit', 0):,.2f}
• Data structure: {', '.join(list(sample.keys())[:5])}..."""
    else:
        status_text += "\n• No trader data available"
    
    if last_token:
        status_text += f"\n• Token: <code>{last_token[:10]}...{last_token[-8:]}</code>"
    
    if cabal_result:
        cabals = cabal_result.get('cabals', [])
        status_text += f"\n• Cabals detected: {len(cabals)}"
    
    status_text += "\n\n<b>Next Steps:</b>"
    
    if not is_premium:
        status_text += "\n→ Upgrade to Premium for CSV export"
    elif not all_traders:
        status_text += "\n→ Analyze a token to generate data"
    else:
        status_text += "\n→ Use /export_csv to download!"
    
    await update.message.reply_text(status_text, parse_mode=ParseMode.HTML)



# ============================================================================
# DEBUGGING: Add this command to check what data is actually in result
# ============================================================================

async def debug_result_structure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Debug command to see result structure
    Usage: /debug_result <wallet>
    """
    
    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b> /debug_result &lt;wallet&gt;",
            parse_mode=ParseMode.HTML
        )
        return
    
    wallet = context.args[0].strip()
    
    if not is_valid_address(wallet):
        await update.message.reply_text("❌ Invalid address", parse_mode=ParseMode.HTML)
        return
    
    processing_msg = await update.message.reply_text(
        "🔍 <b>Running debug analysis...</b>",
        parse_mode=ParseMode.HTML
    )
    
    try:
        result = await solana_analyzer.analyze_wallet(wallet)
        
        # Build debug report
        report = f"""🔍 <b>Result Structure Debug</b>

<b>Top-level keys:</b>
{', '.join(result.keys())}

<b>Source Stats:</b>
"""
        
        source_stats = result.get('source_stats', {})
        if source_stats:
            report += f"Keys: {', '.join(source_stats.keys())}\n\n"
            
            for source, stats in source_stats.items():
                report += f"<b>{source}:</b>\n"
                report += f"  Keys: {', '.join(str(k) for k in stats.keys())}\n"
                report += f"  Values: {stats}\n\n"
        else:
            report += "<i>No source_stats in result</i>\n\n"
        
        # Show debug_stats if available
        debug_stats = result.get('debug_stats', {})
        if debug_stats:
            report += f"<b>Debug Stats:</b>\n{debug_stats}\n\n"
        
        # Show sample of actual data
        report += f"<b>Sample Data:</b>\n"
        report += f"success: {result.get('success')}\n"
        report += f"total_profit: {result.get('total_profit')}\n"
        report += f"total_volume: {result.get('total_volume')}\n"
        report += f"win_rate: {result.get('win_rate')}\n"
        
        await processing_msg.edit_text(report, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Debug error: {e}", exc_info=True)
        await processing_msg.edit_text(
            f"❌ <b>Debug Failed</b>\n\n{str(e)}",
            parse_mode=ParseMode.HTML
        )
async def periodic_cleanup():
    while True:
        try:
            await asyncio.sleep(1800)
            if cache_manager.should_cleanup():
                cache_manager.cleanup_oldest()
            logger.info(f"📊 Active: {task_manager.get_concurrent_count()} | Cache: {cache_manager.get_total_size()}")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

def ensure_admin_tables():
    """
    ✅ ENHANCED: Automatically creates missing tables
    Works for both SQLite and MySQL
    """
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Detect database type
        db_type = getattr(db, 'db_type', 'sqlite')
        
        logger.info(f"🔍 Checking database schema ({db_type})...")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 1: Check and add created_at column to users table
        # ═══════════════════════════════════════════════════════════
        
        if db_type == 'mysql':
            cursor.execute("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'users'
            """)
            columns = [row[0] for row in cursor.fetchall()]
        else:
            cursor.execute("PRAGMA table_info(users)")
            columns = [column[1] for column in cursor.fetchall()]
        
        if 'created_at' not in columns:
            logger.info("➕ Adding created_at column to users table...")
            
            if db_type == 'mysql':
                cursor.execute("""
                    ALTER TABLE users 
                    ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                """)
                cursor.execute("""
                    UPDATE users 
                    SET created_at = NOW() 
                    WHERE created_at IS NULL
                """)
            else:
                cursor.execute("ALTER TABLE users ADD COLUMN created_at TEXT")
                cursor.execute("""
                    UPDATE users 
                    SET created_at = datetime('now') 
                    WHERE created_at IS NULL
                """)
            
            conn.commit()
            logger.info("✅ created_at column added")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 2: Get list of existing tables
        # ═══════════════════════════════════════════════════════════
        
        if db_type == 'mysql':
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = DATABASE()
            """)
            existing_tables = [row[0] for row in cursor.fetchall()]
        else:
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table'
            """)
            existing_tables = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"📋 Found tables: {existing_tables}")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 3: Create admin_logs table if missing
        # ═══════════════════════════════════════════════════════════
        
        if 'admin_logs' not in existing_tables:
            logger.info("🔨 Creating admin_logs table...")
            
            if db_type == 'mysql':
                cursor.execute("""
                    CREATE TABLE admin_logs (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        admin_id BIGINT NOT NULL,
                        action_type VARCHAR(50) NOT NULL,
                        target_user_id BIGINT,
                        details TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_admin_id (admin_id),
                        INDEX idx_created_at (created_at),
                        INDEX idx_action_type (action_type)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
            else:
                cursor.execute("""
                    CREATE TABLE admin_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        admin_id INTEGER NOT NULL,
                        action_type TEXT NOT NULL,
                        target_user_id INTEGER,
                        details TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                cursor.execute("""
                    CREATE INDEX idx_admin_logs_admin_id 
                    ON admin_logs(admin_id)
                """)
                cursor.execute("""
                    CREATE INDEX idx_admin_logs_created_at 
                    ON admin_logs(created_at)
                """)
            
            conn.commit()
            logger.info("✅ admin_logs table created")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 4: Create broadcasts table if missing (optional but useful)
        # ═══════════════════════════════════════════════════════════
        
        if 'broadcasts' not in existing_tables:
            logger.info("🔨 Creating broadcasts table...")
            
            if db_type == 'mysql':
                cursor.execute("""
                    CREATE TABLE broadcasts (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        message TEXT NOT NULL,
                        admin_id BIGINT NOT NULL,
                        tier VARCHAR(50),
                        total_users INT DEFAULT 0,
                        successful INT DEFAULT 0,
                        failed INT DEFAULT 0,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_admin_id (admin_id),
                        INDEX idx_created_at (created_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
            else:
                cursor.execute("""
                    CREATE TABLE broadcasts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        message TEXT NOT NULL,
                        admin_id INTEGER NOT NULL,
                        tier TEXT,
                        total_users INTEGER DEFAULT 0,
                        successful INTEGER DEFAULT 0,
                        failed INTEGER DEFAULT 0,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            
            conn.commit()
            logger.info("✅ broadcasts table created")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 5: Final verification
        # ═══════════════════════════════════════════════════════════
        
        required_tables = [
            'users',
            'subscriptions', 
            'payment_verifications',
            'admin_logs'
        ]
        
        # Re-check tables
        if db_type == 'mysql':
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = DATABASE()
            """)
            final_tables = [row[0] for row in cursor.fetchall()]
        else:
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table'
            """)
            final_tables = [row[0] for row in cursor.fetchall()]
        
        missing = [t for t in required_tables if t not in final_tables]
        
        if missing:
            logger.error(f"❌ Still missing tables: {missing}")
            logger.error("   These tables should be created by your database initialization")
            logger.error("   Check your database.py or database_mysql.py")
        else:
            logger.info(f"✅ All required tables exist: {required_tables}")
        
        logger.info(f"✅ Database schema verified ({db_type})")
        
    except Exception as e:
        logger.error(f"❌ Database schema error: {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise


# ═══════════════════════════════════════════════════════════════════
# BONUS: Function to check if database needs migration
# ═══════════════════════════════════════════════════════════════════

def check_database_migration_needed():
    """
    Check if database needs migration
    Returns list of missing tables/columns
    """
    issues = []
    
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        db_type = getattr(db, 'db_type', 'sqlite')
        
        # Check tables
        if db_type == 'mysql':
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = DATABASE()
            """)
            tables = [row[0] for row in cursor.fetchall()]
        else:
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table'
            """)
            tables = [row[0] for row in cursor.fetchall()]
        
        required_tables = ['users', 'subscriptions', 'payment_verifications', 'admin_logs']
        
        for table in required_tables:
            if table not in tables:
                issues.append(f"Missing table: {table}")
        
        # Check users table columns
        if 'users' in tables:
            if db_type == 'mysql':
                cursor.execute("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = 'users'
                """)
                columns = [row[0] for row in cursor.fetchall()]
            else:
                cursor.execute("PRAGMA table_info(users)")
                columns = [col[1] for col in cursor.fetchall()]
            
            if 'created_at' not in columns:
                issues.append("Missing column: users.created_at")
        
        return issues
        
    except Exception as e:
        logger.error(f"Migration check error: {e}")
        return [f"Check failed: {str(e)}"]
    
class ImprovedShutdownHandler:
    """
    Improved shutdown handler that:
    1. Prevents hanging
    2. Handles multiple SIGINT gracefully
    3. Forces exit after timeout
    4. Cleans up resources properly
    """
    
    def __init__(self, timeout: float = 10.0):
        self.shutdown_event = asyncio.Event()
        self._shutting_down = False
        self._shutdown_count = 0
        self.timeout = timeout
        self._tasks_to_cancel = []
    
    def setup_signals(self):
        """Setup signal handlers with force-exit on second SIGINT"""
        loop = asyncio.get_event_loop()
        
        def signal_handler(sig):
            sig_name = signal.Signals(sig).name
            self._shutdown_count += 1
            
            if self._shutdown_count == 1:
                # First signal: graceful shutdown
                if not self._shutting_down:
                    logger.info(f"🛑 Received {sig_name} - Starting graceful shutdown...")
                    logger.info("   Press Ctrl+C again to force exit")
                    self._shutting_down = True
                    self.shutdown_event.set()
            elif self._shutdown_count == 2:
                # Second signal: force exit
                logger.warning("⚠️ Force exit requested!")
                logger.warning("   Cancelling all tasks...")
                
                # Cancel all running tasks
                for task in asyncio.all_tasks(loop):
                    if not task.done():
                        task.cancel()
                
                # Force exit after brief delay
                loop.call_later(1.0, lambda: sys.exit(1))
            else:
                # Third+ signal: immediate exit
                logger.error("❌ IMMEDIATE EXIT!")
                sys.exit(1)
        
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
        
        logger.info("✅ Signal handlers ready (Ctrl+C twice to force exit)")
    
    async def wait(self):
        """Wait for shutdown signal"""
        await self.shutdown_event.wait()
    
    async def cleanup_with_timeout(self, coro, name: str, timeout: float = None):
        """Run cleanup coroutine with timeout"""
        timeout = timeout or self.timeout
        
        try:
            logger.info(f"🧹 Cleaning up: {name}...")
            await asyncio.wait_for(coro, timeout=timeout)
            logger.info(f"✅ {name} cleaned up")
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ {name} cleanup timeout after {timeout}s")
        except Exception as e:
            logger.error(f"❌ {name} cleanup error: {e}")

async def improved_shutdown(application):
    """
    IMPROVED SHUTDOWN - Never hangs!
    
    Features:
    - 10 second total timeout
    - Cancels hanging tasks
    - Logs each step
    - Guarantees exit
    """
    
    shutdown_start = datetime.now()
    logger.info("=" * 60)
    logger.info("🛑 SHUTDOWN SEQUENCE STARTED")
    logger.info("=" * 60)
    
    # Create shutdown helper
    handler = ImprovedShutdownHandler(timeout=3.0)
    
    try:
        # 1. Stop accepting new updates
        logger.info("1️⃣ Stopping Telegram updates...")
        try:
            await asyncio.wait_for(
                application.updater.stop(),
                timeout=2.0
            )
            logger.info("✅ Updates stopped")
        except asyncio.TimeoutError:
            logger.warning("⏱️ Update stop timeout")
        except Exception as e:
            logger.error(f"❌ Update stop error: {e}")
        
        # 2. Close session factory
        await handler.cleanup_with_timeout(
            session_factory.cleanup(),
            "Session Factory",
            timeout=3.0
        )
        
        # 3. Close analyzers
        await handler.cleanup_with_timeout(
            AnalyzerFactory.cleanup_all(),
            "Analyzers",
            timeout=3.0
        )
        
        # 4. Close enhanced token fetcher
        if enhanced_token_fetcher:
            await handler.cleanup_with_timeout(
                enhanced_token_fetcher.close(),
                "Enhanced Token Fetcher",
                timeout=2.0
            )
        
        # 5. Stop cache cleanup
        await handler.cleanup_with_timeout(
            cache_manager.stop_cleanup(),
            "Cache Manager",
            timeout=1.0
        )
        await handler.cleanup_with_timeout(
        evm_analyzer.close(),
        "EVM Analyzer",
        timeout=2.0
        )

        # 6. Save rate limiter state
        await handler.cleanup_with_timeout(
            rate_limiter.cleanup(),
            "Rate Limiter",
            timeout=1.0
        )
        
        # 7. Shutdown thread pool
        logger.info("🧹 Shutting down thread pool...")
        try:
            thread_pool.shutdown(wait=False)  # Don't wait!
            logger.info("✅ Thread pool shut down")
        except Exception as e:
            logger.error(f"❌ Thread pool error: {e}")
        
        # 8. Close database
        logger.info("🧹 Closing database...")
        try:
            if hasattr(db, 'close'):
                db.close()
            logger.info("✅ Database closed")
        except Exception as e:
            logger.error(f"❌ Database error: {e}")
        
        # 9. Stop application
        logger.info("🧹 Stopping application...")
        try:
            await asyncio.wait_for(
                application.stop(),
                timeout=2.0
            )
            logger.info("✅ Application stopped")
        except asyncio.TimeoutError:
            logger.warning("⏱️ Application stop timeout")
        except Exception as e:
            logger.error(f"❌ Application stop error: {e}")
        
        # 10. Final shutdown
        logger.info("🧹 Final application shutdown...")
        try:
            await asyncio.wait_for(
                application.shutdown(),
                timeout=2.0
            )
            logger.info("✅ Application shutdown complete")
        except asyncio.TimeoutError:
            logger.warning("⏱️ Application shutdown timeout")
        except Exception as e:
            logger.error(f"❌ Application shutdown error: {e}")
        
    except Exception as e:
        logger.error(f"❌ Shutdown error: {e}")
    
    finally:
        # Calculate shutdown time
        shutdown_time = (datetime.now() - shutdown_start).total_seconds()
        
        logger.info("=" * 60)
        logger.info(f"✅ SHUTDOWN COMPLETE in {shutdown_time:.1f}s")
        logger.info("=" * 60)
        
        # Small delay to flush logs
        await asyncio.sleep(0.5)

async def improved_main():
    """
    IMPROVED MAIN - Guaranteed to exit!
    
    Replace your main() function with this
    """
    
    token = getattr(Config, 'TELEGRAM_BOT_TOKEN', None)
    if not token:
        logger.error("❌ Missing TELEGRAM_BOT_TOKEN")
        return
    
    logger.info("🚀 Starting CoinWinRBot on Railway...")
    
    # Railway-specific: Check if we're running on Railway
    is_railway = os.getenv('RAILWAY_ENVIRONMENT') is not None
    
    if is_railway:
        logger.info("🚂 Detected Railway environment")
        # Railway provides better connection limits
        if Config.DATABASE_TYPE == 'mysql':
            Config.MYSQL_POOL_SIZE = 10 
    # Pre-load Jupiter tokens
    logger.info("🚀 Pre-loading Jupiter token list...")
    try:
        await preload_jupiter_tokens()
        logger.info("✅ Jupiter token list cached")
    except Exception as e:
        logger.warning(f"⚠️ Token list preload failed: {e}")
    
    # Create improved shutdown handler
    shutdown_handler = ImprovedShutdownHandler(timeout=10.0)
    shutdown_handler.setup_signals()
    
    # Start cache cleanup
    await cache_manager.start_cleanup()
    
    # Build application
    from telegram.request import HTTPXRequest
    from telegram.ext import ApplicationBuilder
    from telegram import Update
    
    request = HTTPXRequest(
        connection_pool_size=100,
        connect_timeout=10.0,
        read_timeout=30.0
    )
    
    application = (
        ApplicationBuilder()
        .token(token)
        .request(request)
        .concurrent_updates(True)
        .build()
    )
    
    try:
        # Initialize
        await application.initialize()
        ensure_admin_tables()
        
        # Register all handlers (your existing code)
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("subscription", subscription_command))
        application.add_handler(CommandHandler("stats", stats_command))
        application.add_handler(CommandHandler("analyze", analyze))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("health", health_command))
        application.add_handler(CommandHandler("verify", verify_payment))
        application.add_handler(CommandHandler("check_verification", check_verification))
        application.add_handler(CommandHandler("approve", approve_payment))
        application.add_handler(CommandHandler("reject", reject_payment))
        application.add_handler(CommandHandler("pending", pending_verifications))
        application.add_handler(CommandHandler("admin", admin_panel_command))
        application.add_handler(CommandHandler("adminstats", admin_stats))
        application.add_handler(CommandHandler("broadcast", broadcast))
        application.add_handler(CommandHandler("premium_features", premium_features_command))
        application.add_handler(CommandHandler("export_csv", export_last_cabal_results))
        application.add_handler(CommandHandler("debug_export", debug_export_status))
        application.add_handler(CommandHandler("verify_csv_storage", verify_csv_data_storage))
        application.add_handler(CommandHandler("verify_traders", verify_trader_storage))
        application.add_handler(CommandHandler("debug_result", debug_result_structure))
        application.add_handler(CommandHandler("test_all_evm", test_all_evm_chains))
        application.add_handler(CommandHandler("test_token_display", test_token_display))


        
        register_admin_handlers(application, db)
        
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_direct_address
        ))
        
        application.add_handler(CallbackQueryHandler(handle_callbacks))
        application.add_error_handler(error_handler)
        
        logger.info("="*60)
        logger.info("✅ Bot Ready on Railway")
        logger.info(f"🌍 Environment: {os.getenv('RAILWAY_ENVIRONMENT', 'development')}")
        logger.info("="*60)
        
        # Start bot
        await application.start()
        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
        
        logger.info("🤖 Bot is running! Press Ctrl+C to stop")
        logger.info("   (Press Ctrl+C twice to force exit)")
        
        # Wait for shutdown signal
        await shutdown_handler.wait()
        
    except KeyboardInterrupt:
        logger.info("\n⚠️ KeyboardInterrupt caught")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
    finally:
        # Always run shutdown
        await improved_shutdown(application)

if __name__ == '__main__':
    """
    Main entry point with proper exception handling
    """
    try:
        # Run with proper signal handling
        if sys.platform == 'win32':
            # Windows needs special handling
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        asyncio.run(improved_main())
        
    except KeyboardInterrupt:
        # Clean exit on Ctrl+C
        print("\n👋 Stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)
