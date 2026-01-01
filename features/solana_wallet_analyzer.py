"""
Production-Ready Blockchain Analyzers - FIXED VERSION
Enhanced with proper error handling, caching, rate limiting, and monitoring
"""

import aiohttp
import asyncio
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import logging
from functools import wraps
from features.config import Config
from web3 import Web3
import hashlib
from collections import deque
import json
from features.optimized_token_fetcher import OptimizedTokenFetcher

logger = logging.getLogger(__name__)
from dataclasses import dataclass
import time

logger = logging.getLogger(__name__)

#Api Configuration
try:
    BIRDEYE_API_KEY = getattr(Config, 'BIRDEYE_API_KEY', None)
except ImportError:
    HELIUS_API_KEY = None
    BIRDEYE_API_KEY = None

    
@dataclass
class CachedPrice:
    price: float
    source: str
    timestamp: float
    
    def is_valid(self, ttl: int = 300) -> bool:
        """Check if cache entry is still valid (5 min default)"""
        return (time.time() - self.timestamp) < ttl


class FastCache:
    """Ultra-fast in-memory cache with automatic cleanup"""
    
    def __init__(self, max_size: int = 5000, ttl: int = 300):
        self.cache: Dict[str, CachedPrice] = {}
        self.max_size = max_size
        self.ttl = ttl
        self.hits = 0
        self.misses = 0
    
    def get(self, key: str) -> Optional[Tuple[float, str]]:
        """Get cached price if valid"""
        if key in self.cache:
            entry = self.cache[key]
            if entry.is_valid(self.ttl):
                self.hits += 1
                return (entry.price, entry.source)
            else:
                del self.cache[key]
        
        self.misses += 1
        return None
    
    def set(self, key: str, price: float, source: str):
        """Set cache entry"""
        # Auto-cleanup if cache is full
        if len(self.cache) >= self.max_size:
            # Remove 20% oldest entries
            to_remove = int(self.max_size * 0.2)
            oldest_keys = sorted(
                self.cache.keys(), 
                key=lambda k: self.cache[k].timestamp
            )[:to_remove]
            for k in oldest_keys:
                del self.cache[k]
        
        self.cache[key] = CachedPrice(price, source, time.time())
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            'size': len(self.cache),
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': round(hit_rate, 1)
        }

class EnhancedPositionAnalyzer:
    """
    Enhanced position analyzer that handles both open and closed positions
    """
    
    @staticmethod
    def analyze_position_status(token_trades: List[Dict]) -> Dict:
        """
        Analyze position to determine if open/closed and profitability
        """
        
        buys = [t for t in token_trades if t.get('type') == 'buy']
        sells = [t for t in token_trades if t.get('type') == 'sell']
        
        total_bought = sum(t.get('amount', 0) for t in buys)
        total_sold = sum(t.get('amount', 0) for t in sells)
        
        # Calculate position status
        if total_bought == 0:
            status = 'unknown'
            sell_ratio = 0
        elif total_sold == 0:
            status = 'open'
            sell_ratio = 0
        else:
            sell_ratio = total_sold / total_bought
            
            if sell_ratio >= 0.90:
                status = 'closed'
            elif sell_ratio > 0.10:
                status = 'partial'
            else:
                status = 'open'
        
        # Calculate USD values
        buy_value = sum(t.get('usd_value', t.get('amount', 0)) for t in buys)
        sell_value = sum(t.get('usd_value', t.get('amount', 0)) for t in sells)
        
        profit_usd = sell_value - buy_value
        is_profitable = profit_usd > 0
        
        return {
            'status': status,
            'total_bought': total_bought,
            'total_sold': total_sold,
            'sell_ratio': sell_ratio,
            'is_profitable': is_profitable,
            'profit_usd': profit_usd,
            'buy_value_usd': buy_value,
            'sell_value_usd': sell_value,
            'remaining_amount': total_bought - total_sold,
            'position_type': 'long' if total_bought > 0 else 'unknown'
        }

# ============================================================================
# ULTRA-FAST MULTI-SOURCE PRICE FETCHER
# ============================================================================

class FastMultiSourcePriceFetcher:
    """
    Optimized price fetcher with parallel processing and smart fallback
    """
    
    def __init__(self, birdeye_api_key: Optional[str] = None):
        self.birdeye_api_key = birdeye_api_key
        self.cache = FastCache(max_size=10000, ttl=300)
        
        # Track source performance
        self.source_stats = {
            'birdeye': {'success': 0, 'failures': 0, 'total_time': 0},
            'jupiter': {'success': 0, 'failures': 0, 'total_time': 0},
            'dexscreener': {'success': 0, 'failures': 0, 'total_time': 0},
            'coingecko': {'success': 0, 'failures': 0, 'total_time': 0},
            'raydium': {'success': 0, 'failures': 0, 'total_time': 0}
        }
        
        self.semaphore = asyncio.Semaphore(20)
    
    async def get_token_price_fast(
        self, 
        token_mint: str, 
        timestamp: int,
        session: aiohttp.ClientSession
    ) -> Tuple[float, str]:
        """
        FIXED: Properly handles historical vs current prices
        
        Strategy:
        1. Try Birdeye for historical price (if API key available)
        2. If historical fails, get CURRENT price from fast sources
        3. Cache aggressively to avoid repeated failures
        """
        
        # Check cache first
        cache_key = f"{token_mint}_{timestamp // 300}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        # ═══════════════════════════════════════════════════════════
        # STEP 1: Try Historical Price (Birdeye only)
        # ═══════════════════════════════════════════════════════════
        
        if self.birdeye_api_key:
            try:
                price = await asyncio.wait_for(
                    self._fetch_birdeye_price(token_mint, timestamp, session),
                    timeout=5.0
                )
                
                if price and price > 0:
                    self.cache.set(cache_key, price, 'birdeye')
                    self.source_stats['birdeye']['success'] += 1
                    return (price, 'birdeye')
                else:
                    self.source_stats['birdeye']['failures'] += 1
            
            except asyncio.TimeoutError:
                self.source_stats['birdeye']['failures'] += 1
                logger.debug(f"Birdeye timeout for {token_mint[:8]}")
            except Exception as e:
                self.source_stats['birdeye']['failures'] += 1
                logger.debug(f"Birdeye error: {e}")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 2: Fallback to CURRENT Prices (Parallel)
        # ═══════════════════════════════════════════════════════════
        
        # Define current-price sources
        current_sources = [
            ('jupiter', self._fetch_jupiter_current),
            ('dexscreener', self._fetch_dexscreener_current),
            ('raydium', self._fetch_raydium_current),
            ('coingecko', self._fetch_coingecko_current)
        ]
        
        # Create tasks for all current-price sources
        tasks = [
            asyncio.create_task(
                self._fetch_with_tracking(source_name, fetch_func, token_mint, session)
            )
            for source_name, fetch_func in current_sources
        ]
        
        try:
            # Wait for FIRST success
            done, pending = await asyncio.wait(
                tasks,
                timeout=6.0,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Check completed tasks
            for task in done:
                try:
                    price, source = task.result()
                    
                    if price > 0:
                        # Success! Cache and cancel remaining
                        self.cache.set(cache_key, price, source)
                        
                        for pending_task in pending:
                            pending_task.cancel()
                        
                        await asyncio.gather(*pending, return_exceptions=True)
                        
                        return (price, source)
                except Exception:
                    continue
            
            # First batch failed, wait for more
            if pending:
                done2, pending2 = await asyncio.wait(
                    pending,
                    timeout=3.0,
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in done2:
                    try:
                        price, source = task.result()
                        
                        if price > 0:
                            self.cache.set(cache_key, price, source)
                            
                            for p in pending2:
                                p.cancel()
                            
                            await asyncio.gather(*pending2, return_exceptions=True)
                            
                            return (price, source)
                    except Exception:
                        continue
                
                # Cancel remaining
                for task in pending2:
                    task.cancel()
                
                await asyncio.gather(*pending2, return_exceptions=True)
            
            # All sources failed
            return (0, 'none')
        
        except asyncio.TimeoutError:
            # Cancel all on timeout
            for task in tasks:
                if not task.done():
                    task.cancel()
            
            await asyncio.gather(*tasks, return_exceptions=True)
            return (0, 'timeout')
        
        except Exception as e:
            logger.error(f"Price fetch error: {e}")
            for task in tasks:
                if not task.done():
                    task.cancel()
            
            await asyncio.gather(*tasks, return_exceptions=True)
            return (0, 'error')
    
    # ═══════════════════════════════════════════════════════════
    # CURRENT PRICE FETCHERS (No timestamp needed)
    # ═══════════════════════════════════════════════════════════
    
    async def _fetch_with_tracking(
        self,
        source_name: str,
        fetch_func,
        token_mint: str,
        session: aiohttp.ClientSession
    ) -> Tuple[float, str]:
        """Wrapper for current-price fetchers (no timestamp)"""
        
        start_time = time.time()
        
        try:
            async with self.semaphore:
                price = await fetch_func(token_mint, session)
            
            elapsed = time.time() - start_time
            
            if price and price > 0:
                self.source_stats[source_name]['success'] += 1
                self.source_stats[source_name]['total_time'] += elapsed
                return (price, source_name)
            else:
                self.source_stats[source_name]['failures'] += 1
                return (0, source_name)
        
        except Exception as e:
            elapsed = time.time() - start_time
            self.source_stats[source_name]['failures'] += 1
            self.source_stats[source_name]['total_time'] += elapsed
            logger.debug(f"{source_name} error: {e}")
            return (0, source_name)
    
    async def _fetch_jupiter_current(
        self,
        token_mint: str,
        session: aiohttp.ClientSession
    ) -> Optional[float]:
        """Jupiter - current price only"""
        url = "https://price.jup.ag/v4/price"
        
        try:
            async with session.get(
                url,
                params={"ids": token_mint},
                timeout=aiohttp.ClientTimeout(total=3)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = data.get("data", {}).get(token_mint, {}).get("price")
                    return float(price) if price else None
        except Exception as e:
            logger.debug(f"Jupiter error: {e}")
        
        return None
    
    async def _fetch_dexscreener_current(
        self,
        token_mint: str,
        session: aiohttp.ClientSession
    ) -> Optional[float]:
        """DexScreener - current price"""
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint}"
        
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs", [])
                    if pairs:
                        # Get pair with highest liquidity
                        best_pair = max(
                            pairs,
                            key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0)
                        )
                        price = best_pair.get("priceUsd")
                        return float(price) if price else None
        except Exception as e:
            logger.debug(f"DexScreener error: {e}")
        
        return None
    
    async def _fetch_raydium_current(
        self,
        token_mint: str,
        session: aiohttp.ClientSession
    ) -> Optional[float]:
        """Raydium - current price"""
        url = "https://api.raydium.io/v2/main/price"
        
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = data.get(token_mint)
                    return float(price) if price and price > 0 else None
        except Exception as e:
            logger.debug(f"Raydium error: {e}")
        
        return None
    
    async def _fetch_coingecko_current(
        self,
        token_mint: str,
        session: aiohttp.ClientSession
    ) -> Optional[float]:
        """CoinGecko - current price (major tokens only)"""
        url = "https://api.coingecko.com/api/v3/simple/token_price/solana"
        
        try:
            async with session.get(
                url,
                params={
                    "contract_addresses": token_mint,
                    "vs_currencies": "usd"
                },
                timeout=aiohttp.ClientTimeout(total=4)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get(token_mint.lower(), {}).get("usd")
        except Exception as e:
            logger.debug(f"CoinGecko error: {e}")
        
        return None
    
    # ═══════════════════════════════════════════════════════════
    # HISTORICAL PRICE FETCHER (Birdeye only)
    # ═══════════════════════════════════════════════════════════
    
    async def _fetch_birdeye_price(
        self,
        token_mint: str,
        timestamp: int,
        session: aiohttp.ClientSession
    ) -> Optional[float]:
        """Birdeye - historical price with timestamp"""
        url = "https://public-api.birdeye.so/defi/history_price"
        
        headers = {"X-API-KEY": self.birdeye_api_key}
        params = {
            "address": token_mint,
            "time_from": timestamp - 60,
            "time_to": timestamp + 60,
            "type": "1m"
        }
        
        try:
            async with session.get(
                url,
                headers=headers,
                params=params,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = data.get("data", {}).get("items", [])
                    if items:
                        return items[0].get("value", 0)
        except Exception as e:
            logger.debug(f"Birdeye error: {e}")
        
        return None
    
    def get_stats(self) -> Dict:
        """Get performance statistics"""
        stats = {}
        
        for source, counts in self.source_stats.items():
            total = counts['success'] + counts['failures']
            if total > 0:
                success_rate = (counts['success'] / total * 100)
                avg_time = (counts['total_time'] / total) if total > 0 else 0
                
                stats[source] = {
                    'success': counts['success'],
                    'failures': counts['failures'],
                    'success_rate': round(success_rate, 1),
                    'total_attempts': total,
                    'avg_time_ms': round(avg_time * 1000, 0)
                }
        
        # Add cache stats
        stats['cache'] = self.cache.get_stats()
        
        return stats

class BirdeyeRateLimiter:
    """
    Birdeye-specific rate limiter: 60 requests per minute
    
    Features:
    - Enforces 60 RPM hard limit
    - Tracks requests across 1-minute sliding window
    - Auto-waits when limit reached
    - Provides status reporting
    """
    
    def __init__(self, max_rpm: int = 60):
        """
        Args:
            max_rpm: Maximum requests per minute (default: 60 for Birdeye)
        """
        self.max_rpm = max_rpm
        self.requests = deque()  # Timestamp queue
        self.lock = asyncio.Lock()
        
        # Statistics
        self.total_requests = 0
        self.total_waits = 0
        self.total_wait_time = 0.0
        
        logger.info(f"🔐 Birdeye Rate Limiter initialized: {max_rpm} RPM")
    
    async def acquire(self):
        """
        Acquire permission to make a request
        Blocks until request can be made without exceeding rate limit
        """
        async with self.lock:
            now = datetime.now()
            one_minute_ago = now - timedelta(minutes=1)
            
            # Remove requests older than 1 minute
            while self.requests and self.requests[0] < one_minute_ago:
                self.requests.popleft()
            
            # Check if we're at limit
            current_count = len(self.requests)
            
            if current_count >= self.max_rpm:
                # Calculate wait time
                oldest_request = self.requests[0]
                wait_seconds = (oldest_request - one_minute_ago).total_seconds()
                wait_seconds = max(0.1, wait_seconds + 0.1)  # Add small buffer
                
                logger.warning(
                    f"⚠️ Birdeye Rate Limit: {current_count}/{self.max_rpm} RPM "
                    f"- Waiting {wait_seconds:.1f}s"
                )
                
                self.total_waits += 1
                self.total_wait_time += wait_seconds
                
                await asyncio.sleep(wait_seconds)
                
                # Recurse to re-check after waiting
                return await self.acquire()
            
            # Add current request
            self.requests.append(now)
            self.total_requests += 1
            
            # Log status every 10 requests
            if self.total_requests % 10 == 0:
                logger.info(
                    f"📊 Birdeye API: {len(self.requests)} requests in last minute "
                    f"({self.max_rpm - len(self.requests)} remaining)"
                )
    
    def get_status(self) -> dict:
        """Get current rate limiter status"""
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)
        
        # Clean old requests
        while self.requests and self.requests[0] < one_minute_ago:
            self.requests.popleft()
        
        current_rpm = len(self.requests)
        available = self.max_rpm - current_rpm
        
        return {
            'current_rpm': current_rpm,
            'max_rpm': self.max_rpm,
            'available': available,
            'utilization_pct': (current_rpm / self.max_rpm * 100),
            'total_requests': self.total_requests,
            'total_waits': self.total_waits,
            'avg_wait_time': (self.total_wait_time / self.total_waits) if self.total_waits > 0 else 0
        }
    
    def reset_stats(self):
        """Reset statistics"""
        self.total_requests = 0
        self.total_waits = 0
        self.total_wait_time = 0.0



class SimpleCache:
    """Simple in-memory cache with TTL"""
    
    def __init__(self, ttl_seconds: int = 300):
        self.cache = {}
        self.ttl = ttl_seconds
    
    def get(self, key: str) -> Optional[Dict]:
        """Get cached value if not expired"""
        if key in self.cache:
            data, timestamp = self.cache[key]
            if (datetime.now() - timestamp).total_seconds() < self.ttl:
                logger.debug(f"Cache hit: {key[:16]}...")
                return data
            else:
                del self.cache[key]
        return None
    
    def set(self, key: str, value: Dict):
        """Set cache value"""
        self.cache[key] = (value, datetime.now())
        logger.debug(f"Cache set: {key[:16]}...")
    
    def clear_old(self):
        """Clear expired entries"""
        now = datetime.now()
        expired = [k for k, (_, ts) in self.cache.items() 
                  if (now - ts).total_seconds() > self.ttl]
        for key in expired:
            del self.cache[key]


def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """Decorator to retry failed operations"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        wait_time = delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Retry {attempt + 1}/{max_retries} after {wait_time}s: {e}")
                        await asyncio.sleep(wait_time)
            raise last_error
        return wrapper
    return decorator

class BirdeyePnLFetcher:
    """Fetch wallet PnL from Birdeye API with proper rate limiting"""
    
    # ⭐ SHARED RATE LIMITER across all instances
    _rate_limiter = None
    
    def __init__(self, api_key: str = BIRDEYE_API_KEY):
        self.api_key = api_key
        self.base_url = "https://public-api.birdeye.so"
        self.session = None
        
        # ⭐ Initialize shared rate limiter (only once)
        if BirdeyePnLFetcher._rate_limiter is None:
            BirdeyePnLFetcher._rate_limiter = BirdeyeRateLimiter(max_rpm=60)
    
    async def get_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self.session
    
    async def fetch_wallet_pnl_summary(self, wallet: str, duration: str = "7d"):
        """
        Fetch PnL with proper rate limiting
        """
        # ⭐ CRITICAL: Acquire rate limit permission BEFORE request
        await self._rate_limiter.acquire()
        
        url = "https://public-api.birdeye.so/wallet/v2/pnl/summary"
        
        params = {
            'wallet': wallet,
            'duration': duration
        }
        
        headers = {
            'X-API-KEY': self.api_key,
            "accept": "application/json",
            "x-chain": "solana"
        }
        
        logger.info(f"🔍 Fetching {duration} PnL for {wallet[:10]}...")
        
        try:
            session = await self.get_session()
            async with session.get(url, headers=headers, params=params) as resp:
                
                status = resp.status
                logger.info(f"📡 Response: HTTP {status}")
                
                if status == 429:
                    # ⭐ Rate limit hit - log and retry after delay
                    logger.error("❌ 429 Rate Limit - This shouldn't happen with rate limiter!")
                    logger.error("📊 Rate Limiter Status: " + str(self._rate_limiter.get_status()))
                    await asyncio.sleep(5)  # Extra safety delay
                    return None
                
                if status == 401:
                    logger.error("❌ Unauthorized (401)")
                    return None
                
                if status != 200:
                    error_text = await resp.text()
                    logger.error(f"❌ HTTP {status}: {error_text[:300]}")
                    return None
                
                # Parse response
                data = await resp.json()
                
                logger.info(f"📦 Response keys: {list(data.keys())}")
                
                pnl_data = data.get('data')
                
                if not pnl_data:
                    logger.warning("⚠️ No 'data' field")
                    return None
                
                # Log summary
                logger.info(f"✅ {duration} PnL data received:")
                logger.info(f"   Total PnL: ${pnl_data.get('totalPnl', 0):,.2f}")
                logger.info(f"   Win Rate: {pnl_data.get('winRate', 0):.1f}%")
                logger.info(f"   Trades: {pnl_data.get('totalTradeCount', 0)}")
                
                return pnl_data
        
        except asyncio.TimeoutError:
            logger.error("⏱️ Timeout")
            return None
        except Exception as e:
            logger.error(f"❌ Error: {e}", exc_info=True)
            return None
    
    @classmethod
    def get_rate_limit_status(cls) -> dict:
        """Get current rate limit status"""
        if cls._rate_limiter:
            return cls._rate_limiter.get_status()
        return {'error': 'Rate limiter not initialized'}
    
    async def close(self):
        """Close session"""
        if self.session and not self.session.closed:
            await self.session.close()
            await asyncio.sleep(0.25)
            self.session = None
            logger.debug("🔒 Closed Birdeye session")


def format_number(value: float) -> str:
    """
    Format number for display with appropriate suffix
    Examples:
        50 -> "$50.00"
        1500 -> "+$1.5K"
        2500000 -> "+$2.50M"
        -750 -> "-$750.00"
    """
    abs_value = abs(value)
    sign = "+" if value > 0 else ("-" if value < 0 else "")
    
    if abs_value >= 1_000_000:
        return f"{sign}${abs_value/1_000_000:.2f}M"
    elif abs_value >= 1_000:
        return f"{sign}${abs_value/1_000:.1f}K"
    else:
        return f"{sign}${abs_value:.2f}"


class FastEnhancedSolanaAnalyzer:
    """
    Ultra-fast analyzer optimized for speed
    
    Performance optimizations:
    - Parallel price fetching (concurrent for all tokens)
    - Early data truncation (analyze only recent 100 transactions)
    - Batch processing
    - Aggressive caching
    - Concurrent metric calculation
    """
    
    def __init__(self, helius_api_key: str, birdeye_api_key: Optional[str] = None):
        self.helius_api_key = helius_api_key
        self.birdeye_api_key = birdeye_api_key or BIRDEYE_API_KEY
        self.pnl_fetcher = BirdeyePnLFetcher(birdeye_api_key or BIRDEYE_API_KEY)
        self.helius_url = "https://api-mainnet.helius-rpc.com/v0/addresses"
        
        # Fast price fetcher
        self.price_fetcher = FastMultiSourcePriceFetcher(self.birdeye_api_key)
        
        # Session management
        self.session = None
        
        # Debug stats
        self.debug_stats = {
            'tokens_attempted': 0,
            'tokens_with_prices': 0,
            'source_breakdown': {},
            'analysis_time': 0,
            'fetch_time': 0,
            'parse_time': 0,
            'calc_time': 0
        }
        
        logger.info("⚡ FAST Enhanced Solana Analyzer initialized")

    def _is_token_profitable(self, token_trades: List[Dict]) -> bool:
        """GMGN.ai-style profitability check"""
        total_buy_value = sum(
            t.get('usd_value', t.get('amount', 0)) 
            for t in token_trades if t.get('type') == 'buy'
        )
        total_sell_value = sum(
            t.get('usd_value', t.get('amount', 0)) 
            for t in token_trades if t.get('type') == 'sell'
        )
        return total_sell_value > total_buy_value
    
    def _is_position_closed(self, token_trades: List[Dict]) -> bool:
        """Check if position is closed (90%+ sold)"""
        total_bought = sum(
            t.get('amount', 0) 
            for t in token_trades if t.get('type') == 'buy'
        )
        total_sold = sum(
            t.get('amount', 0) 
            for t in token_trades if t.get('type') == 'sell'
        )
        
        if total_bought == 0:
            return False
        
        return (total_sold / total_bought) >= 0.90
    
    async def _ensure_session(self):
        """Ensure session exists"""
        if not self.session or self.session.closed:
            connector = aiohttp.TCPConnector(
                limit=50,
                limit_per_host=20,
                ttl_dns_cache=300
            )
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=15)
            )
    
    async def analyze_wallet(self, wallet: str) -> Dict:
        """
        ✅ COMPLETE FIX: Enhanced wallet analysis with token metadata included
        
        Now fetches token symbols/names for top tokens automatically
        
        Strategy:
        1. Try Birdeye 7d PnL API first (most accurate)
        2. If fails, use fast transaction analysis
        3. Fetch metadata for top tokens in parallel
        4. Return standardized result format
        """
        
        total_start = time.time()
        logger.info(f"⚡ Analyzing {wallet[:8]}...")
        
        # Check cache first
        cache_key = f"solana_{wallet}"
        cached = self.cache.get(cache_key) if hasattr(self, 'cache') else None
        if cached:
            logger.info("💾 Returning cached result")
            return cached
        
        try:
            # ═══════════════════════════════════════════════════════════
            # PRIORITY 1: Birdeye 7-Day PnL API
            # ═══════════════════════════════════════════════════════════
            
            if self.birdeye_api_key:
                logger.info("📊 Attempting Birdeye 7-day PnL API...")
                
                try:
                    pnl_data_7d = await asyncio.wait_for(
                        self.pnl_fetcher.fetch_wallet_pnl_summary(wallet, duration="7d"),
                        timeout=10.0
                    )
                    
                    if pnl_data_7d:
                        logger.info("✅ Using Birdeye 7-day data")
                        result = await self._process_birdeye_pnl(wallet, pnl_data_7d, "7d")
                        
                        if hasattr(self, 'cache'):
                            self.cache.set(cache_key, result)
                        
                        return result
                    
                    logger.info("⚠️ Birdeye 7d returned no data")
                    
                except asyncio.TimeoutError:
                    logger.warning("⏱️ Birdeye 7d timeout")
                except Exception as e:
                    logger.warning(f"⚠️ Birdeye 7d error: {e}")
            
            # ═══════════════════════════════════════════════════════════
            # PRIORITY 2: Fast Transaction Analysis (Fallback)
            # ═══════════════════════════════════════════════════════════
            
            logger.info("🔄 Fallback: Fast transaction analysis...")
            
            await self._ensure_session()
            
            # Fetch transactions (FAST - only 2 pages)
            fetch_start = time.time()
            txs = await self.fetch_transactions_fast(wallet, max_pages=2)
            self.debug_stats['fetch_time'] = time.time() - fetch_start
            
            if not txs:
                return self._error_response(wallet, "No transactions found")
            
            # Parse trades (FAST)
            parse_start = time.time()
            trades = self.parse_trades_fast(txs, wallet)
            self.debug_stats['parse_time'] = time.time() - parse_start
            
            if not trades:
                return self._error_response(wallet, "No trades found")
            
            # Filter to 7-day window
            seven_days_ago = datetime.now() - timedelta(days=7)
            cutoff_timestamp = int(seven_days_ago.timestamp())
            
            trades_7d = [t for t in trades if t.get('timestamp', 0) >= cutoff_timestamp]
            
            logger.info(f"📊 Total: {len(trades)} | Last 7 days: {len(trades_7d)}")
            
            if not trades_7d:
                return self._error_response(wallet, "No trades in last 7 days")
            
            trades_7d = sorted(trades_7d, key=lambda t: t.get('timestamp', 0), reverse=True)
            logger.info(f"⚡ Processing {len(trades_7d)} trades from last 7 days...")
            
            # Calculate metrics (PARALLEL price fetching)
            calc_start = time.time()
            metrics = await self._calculate_metrics_parallel(trades_7d, wallet)
            self.debug_stats['calc_time'] = time.time() - calc_start
            
            # ═══════════════════════════════════════════════════════════
            # ⭐ STEP 4: FETCH TOKEN METADATA FOR TOP TOKENS (THE FIX!)
            # ═══════════════════════════════════════════════════════════
            
            top_tokens = metrics.get('top_tokens', [])
            
            if top_tokens:
                logger.info(f"🔍 Fetching metadata for {len(top_tokens)} top tokens...")
                
                # Create fetch tasks for all tokens
                fetch_tasks = []
                for token in top_tokens:
                    mint = token.get('address', '')
                    if mint:
                        task = self._fetch_token_metadata(mint)
                        fetch_tasks.append((token, task))
                
                # Wait for all fetches with timeout
                for token, task in fetch_tasks:
                    try:
                        metadata = await asyncio.wait_for(task, timeout=3.0)
                        
                        if metadata:
                            token['symbol'] = metadata.get('symbol', 'Unknown')
                            token['name'] = metadata.get('name', '')
                            token['logo'] = metadata.get('logo', '')
                            
                            logger.info(f"✅ {token['symbol']} ({token.get('address', '')[:8]}...)")
                        else:
                            mint = token.get('address', '')
                            token['symbol'] = f"{mint[:6]}...{mint[-4:]}" if mint else 'Unknown'
                            logger.warning(f"⚠️ No metadata for {mint[:8]}...")
                    
                    except asyncio.TimeoutError:
                        mint = token.get('address', '')
                        token['symbol'] = f"{mint[:6]}...{mint[-4:]}" if mint else 'Unknown'
                        logger.warning(f"⏱️ Timeout for {mint[:8]}...")
                    except Exception as e:
                        mint = token.get('address', '')
                        token['symbol'] = f"{mint[:6]}...{mint[-4:]}" if mint else 'Unknown'
                        logger.error(f"❌ Error for {mint[:8]}: {e}")
            
            # ═══════════════════════════════════════════════════════════
            # STEP 5: Build Response
            # ═══════════════════════════════════════════════════════════
            
            self.debug_stats['analysis_time'] = time.time() - total_start
            
            result = {
                'success': True,
                'wallet': wallet,
                'address': wallet,
                'chain': 'solana',
                'source': 'fast_transaction_analysis',
                'data_quality': 'medium',
                'data_note': 'Fallback: Fast transaction analysis (7-day window)',
                **metrics,
                'debug_stats': self.debug_stats,
                'source_stats': self.price_fetcher.get_stats(),
                'performance': {
                    'total_time': round(self.debug_stats['analysis_time'], 2),
                    'fetch_time': round(self.debug_stats['fetch_time'], 2),
                    'parse_time': round(self.debug_stats['parse_time'], 2),
                    'calc_time': round(self.debug_stats['calc_time'], 2)
                }
            }
            
            if hasattr(self, 'cache'):
                self.cache.set(cache_key, result)
            
            logger.info(f"⚡ DONE in {self.debug_stats['analysis_time']:.2f}s")
            
            return result
            
        except asyncio.TimeoutError:
            logger.error("⏱️ Analysis timeout")
            return self._error_response(wallet, "Analysis timeout")
        except Exception as e:
            logger.error(f"❌ Analysis error: {e}", exc_info=True)
            return self._error_response(wallet, str(e))


    # ═══════════════════════════════════════════════════════════════════════
    # ⭐ ADD THIS NEW METHOD TO YOUR FastEnhancedSolanaAnalyzer CLASS
    # ═══════════════════════════════════════════════════════════════════════

    async def _fetch_token_metadata(self, token_address: str) -> Optional[Dict]:
        """
        ⭐ NEW: Fetch token symbol and name from multiple sources
        
        Priority:
        1. Jupiter Token List (fast, comprehensive)
        2. DexScreener API (fallback with price data)
        3. Helius RPC (last resort, on-chain)
        
        Args:
            token_address: Solana token mint address
        
        Returns:
            Dict with symbol, name, logo or None
        """
        
        # Check cache first
        if not hasattr(self, '_metadata_cache'):
            self._metadata_cache = {}
        
        cache_key = f"token_meta:{token_address}"
        if cache_key in self._metadata_cache:
            logger.debug(f"🎯 Cache hit for {token_address[:8]}")
            return self._metadata_cache[cache_key]
        
        # ═══════════════════════════════════════════════════════════
        # SOURCE 1: Jupiter Token List (BEST for Solana)
        # ═══════════════════════════════════════════════════════════
        
        try:
            async with self.session.get(
                f"https://token.jup.ag/token/{token_address}",
                timeout=aiohttp.ClientTimeout(total=3.0)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    metadata = {
                        'symbol': data.get('symbol', 'Unknown'),
                        'name': data.get('name', ''),
                        'logo': data.get('logoURI', ''),
                        'decimals': data.get('decimals', 9)
                    }
                    
                    # Cache it
                    self._metadata_cache[cache_key] = metadata
                    
                    logger.debug(f"✅ Jupiter: {metadata['symbol']}")
                    return metadata
        
        except Exception as e:
            logger.debug(f"Jupiter fetch failed for {token_address[:8]}: {e}")
        
        # ═══════════════════════════════════════════════════════════
        # SOURCE 2: DexScreener (FALLBACK with price data)
        # ═══════════════════════════════════════════════════════════
        
        try:
            async with self.session.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{token_address}",
                timeout=aiohttp.ClientTimeout(total=3.0)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get('pairs', [])
                    
                    if pairs:
                        # Get most liquid Solana pair
                        solana_pairs = [p for p in pairs if p.get('chainId') == 'solana']
                        pair = max(solana_pairs or pairs, 
                                key=lambda p: float(p.get('liquidity', {}).get('usd', 0) or 0))
                        
                        base = pair.get('baseToken', {})
                        
                        metadata = {
                            'symbol': base.get('symbol', 'Unknown'),
                            'name': base.get('name', ''),
                            'logo': pair.get('info', {}).get('imageUrl', ''),
                            'price_change_24h': float(pair.get('priceChange', {}).get('h24', 0) or 0)
                        }
                        
                        # Cache it
                        self._metadata_cache[cache_key] = metadata
                        
                        logger.debug(f"✅ DexScreener: {metadata['symbol']}")
                        return metadata
        
        except Exception as e:
            logger.debug(f"DexScreener fetch failed for {token_address[:8]}: {e}")
        
        # ═══════════════════════════════════════════════════════════
        # SOURCE 3: Helius RPC (LAST RESORT - on-chain data)
        # ═══════════════════════════════════════════════════════════
        
        if hasattr(self, 'helius_api_key') and self.helius_api_key:
            try:
                async with self.session.post(
                    f"https://mainnet.helius-rpc.com/?api-key={self.helius_api_key}",
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getAsset",
                        "params": {
                            "id": token_address,
                            "displayOptions": {"showFungible": True}
                        }
                    },
                    timeout=aiohttp.ClientTimeout(total=3.0)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        result = data.get('result', {})
                        content = result.get('content', {})
                        metadata_raw = content.get('metadata', {})
                        
                        metadata = {
                            'symbol': metadata_raw.get('symbol', 'Unknown'),
                            'name': metadata_raw.get('name', ''),
                            'logo': content.get('links', {}).get('image', '')
                        }
                        
                        # Cache it
                        self._metadata_cache[cache_key] = metadata
                        
                        logger.debug(f"✅ Helius: {metadata['symbol']}")
                        return metadata
            
            except Exception as e:
                logger.debug(f"Helius fetch failed for {token_address[:8]}: {e}")
        
        # ═══════════════════════════════════════════════════════════
        # ALL FAILED: Return None
        # ═══════════════════════════════════════════════════════════
        
        logger.warning(f"⚠️ All sources failed for {token_address[:8]}")
        return None
    async def _process_birdeye_pnl(self, wallet: str, pnl: Dict, period: str = "7d") -> Dict:
        """
        Process Birdeye PnL data (from second script's method)
        """
        
        summary = pnl.get('summary', pnl)
        
        # Core Metrics
        counts = summary.get('counts', {})
        total_trades = int(counts.get('total_trade', 0))
        total_wins = int(counts.get('total_win', 0))
        total_losses = int(counts.get('total_loss', 0))
        win_rate = float(counts.get('win_rate', 0)) * 100
        
        # Investment & Volume
        cashflow = summary.get('cashflow_usd', {})
        total_invested = float(cashflow.get('total_invested', 0))
        total_sold = float(cashflow.get('total_sold', 0))
        current_value = float(cashflow.get('current_value', 0))
        
        # PnL
        pnl_data = summary.get('pnl', {})
        realized = float(pnl_data.get('realized_profit_usd', 0))
        realized_pct = float(pnl_data.get('realized_profit_percent', 0))
        unrealized = float(pnl_data.get('unrealized_usd', 0))
        total_pnl = float(pnl_data.get('total_usd', 0))
        avg_profit_per_trade = float(pnl_data.get('avg_profit_per_trade_usd', 0))
        
        # Extract top tokens
        top_tokens = self._extract_top_tokens_from_pnl(pnl, wallet)
        
        # Fallback: Fetch top tokens separately if not in PnL
        if not top_tokens:
            logger.info("💡 Fetching top tokens separately...")
            try:
                top_tokens = await asyncio.wait_for(
                    self._fetch_wallet_top_tokens(wallet, period),
                    timeout=5.0
                )
            except:
                top_tokens = []
        
        # Calculate derived metrics
        roi = realized_pct
        total_volume = total_sold
        avg_trade_size = total_invested / total_trades if total_trades > 0 else 0
        
        # Period-specific handling
        profit_7d = total_pnl
        profit_7d_pct = (profit_7d / total_volume * 100) if total_volume > 0 else 0
        win_rate_7d = win_rate
        trades_7d = total_trades
        
        return {
            'success': True,
            'address': wallet,
            'chain': 'solana',
            'source': 'birdeye_7d_pnl_api',
            'data_quality': 'high',
            'data_note': '7-day window with accurate Birdeye metrics',
            
            # Core metrics
            'total_profit': total_pnl,
            'realized_profit': realized,
            'unrealized_profit': unrealized,
            'roi': roi,
            'win_rate': win_rate,
            'total_trades': total_trades,
            'profitable_trades': total_wins,
            'losing_trades': total_losses,
            'avg_trade_size': avg_trade_size,
            'total_volume': total_volume,
            'total_invested': total_invested,
            
            # 7-day specific
            'profit_7d': profit_7d,
            'profit_7d_pct': profit_7d_pct,
            'win_rate_7d': win_rate_7d,
            'trades_7d': trades_7d,
            
            # Top tokens
            'top_tokens': top_tokens,
            
            # Metadata
            'last_trade_time': datetime.now().isoformat(),
            'active_days': 7,
            'recent_win_rate': win_rate_7d,
            'analysis_timestamp': datetime.now().isoformat(),
            'has_7d_data': True,
            'has_token_breakdown': len(top_tokens) > 0
        }


    def _extract_top_tokens_from_pnl(self, pnl: Dict, wallet: str) -> List[Dict]:
        """
        Extract top tokens from Birdeye PnL response
        """
        
        try:
            data = pnl.get('data', pnl)
            tokens_list = (
                data.get('tokens', []) or 
                data.get('token_list', []) or
                data.get('items', []) or
                []
            )
            
            if not tokens_list:
                return []
            
            top_tokens = []
            for token in tokens_list[:10]:
                try:
                    symbol = token.get('symbol', 'UNKNOWN')
                    total_profit = float(token.get('pnl', 0) or 0)
                    realized = float(token.get('realized_profit', 0) or 0)
                    unrealized = float(token.get('unrealized_profit', 0) or 0)
                    trades = int(token.get('total_trade', 0) or 0)
                    win_rate = float(token.get('win_rate', 0) or 0)
                    
                    if win_rate <= 1.0:
                        win_rate *= 100
                    
                    if trades > 0 or total_profit != 0:
                        top_tokens.append({
                            'symbol': symbol[:15],
                            'profit': total_profit,
                            'realized_profit': realized,
                            'unrealized_profit': unrealized,
                            'trades': trades,
                            'win_rate': win_rate,
                            'address': token.get('address', '')
                        })
                except:
                    continue
            
            top_tokens.sort(key=lambda x: x['profit'], reverse=True)
            return top_tokens[:5]
            
        except Exception as e:
            logger.error(f"Token extraction error: {e}")
            return []
        
    async def _fetch_wallet_top_tokens(self, wallet: str, duration: str = "all") -> List[Dict]:
        """
        ⭐ FIXED - Uses OptimizedTokenFetcher with proper error handling
        """
        
        fetcher = OptimizedTokenFetcher(helius_key=self.helius_api_key)
        
        try:
            # ✅ Use timeout wrapper to prevent hanging
            tokens = await asyncio.wait_for(
                fetcher.fetch_wallet_top_tokens(wallet, limit=15),
                timeout=15.0  # 15 second timeout
            )
            
            if tokens:
                logger.info(f"✅ Fetched {len(tokens)} top tokens")
                return tokens
            else:
                logger.warning("⚠️ OptimizedTokenFetcher returned empty list")
                return []
                
        except asyncio.TimeoutError:
            logger.warning("⏱️ Token fetch timeout after 15s")
            return []
            
        except Exception as e:
            logger.error(f"❌ Token fetch error: {e}")
            return []
            
        finally:
            # ✅ CRITICAL: Always close to prevent connection leaks
            try:
                await asyncio.wait_for(fetcher.close(), timeout=2.0)
            except:
                pass 
    async def fetch_transactions_fast(self, wallet: str, max_pages: int = 2) -> List[Dict]:
        """
        FAST transaction fetching - only 2 pages (40 transactions)
        This is usually enough for recent analysis
        """
        
        all_txs = []
        before = None
        
        for page in range(max_pages):
            url = f"{self.helius_url}/{wallet}/transactions"
            params = {
                'api-key': self.helius_api_key,
                'limit': 20
            }
            if before:
                params['before'] = before
            
            try:
                async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status != 200:
                        break
                    
                    data = await resp.json()
                    if not data:
                        break
                    
                    all_txs.extend(data)
                    before = data[-1]["signature"]
                    
            except:
                break
        
        logger.info(f"⚡ Fetched {len(all_txs)} transactions")
        return all_txs
    
    def parse_trades_fast(self, transactions: List[Dict], wallet: str) -> List[Dict]:
        """FAST parsing - no complex validation"""
        
        trades = []
        wallet_lower = wallet.lower()
        seen = set()
        
        for tx in transactions:
            try:
                sig = tx.get('signature', '')
                ts = tx.get('timestamp', 0)
                
                for transfer in tx.get('tokenTransfers', []):
                    from_addr = transfer.get('fromUserAccount', '').lower()
                    to_addr = transfer.get('toUserAccount', '').lower()
                    token = transfer.get('mint', '')
                    symbol = transfer.get('symbol', 'Unknown')
                    amount = float(transfer.get('tokenAmount', 0))
                    
                    if not token or amount <= 0:
                        continue
                    
                    # Quick dedup
                    trade_key = f"{sig}_{token}"
                    if trade_key in seen:
                        continue
                    seen.add(trade_key)
                    
                    # Determine type
                    if to_addr == wallet_lower:
                        trade_type = 'buy'
                    elif from_addr == wallet_lower:
                        trade_type = 'sell'
                    else:
                        continue
                    
                    trades.append({
                        'timestamp': ts,
                        'token': token,
                        'symbol': symbol,
                        'type': trade_type,
                        'amount': amount
                    })
                    
            except:
                continue
        
        return trades
    
    async def _calculate_metrics_parallel(self, trades: List[Dict], wallet: str) -> Dict:
        """ENHANCED: Win rate with open position handling"""
        
        if not trades:
            return self._empty_metrics()
        
        # Group by token
        token_positions = defaultdict(list)
        for trade in trades:
            token = trade.get('token')
            if token:
                token_positions[token].append(trade)
        
        logger.info(f"⚡ Analyzing {len(token_positions)} token positions...")
        
        # Analyze each position in parallel
        tasks = []
        for token, token_trades in token_positions.items():
            task = self._analyze_token_position_enhanced(token, token_trades)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Categorize positions by status
        closed_positions = []
        partial_positions = []
        open_positions = []
        
        for result in results:
            if isinstance(result, Exception) or not result:
                continue
            
            status = result.get('status')
            if status == 'closed':
                closed_positions.append(result)
            elif status == 'partial':
                partial_positions.append(result)
            else:
                open_positions.append(result)
        
        # Calculate win rates
        all_positions = closed_positions + partial_positions + open_positions
        all_wins = sum(1 for p in all_positions if p['is_profitable'])
        all_win_rate = (all_wins / len(all_positions) * 100) if all_positions else 0
        
        closed_wins = sum(1 for p in closed_positions if p['is_profitable'])
        closed_win_rate = (closed_wins / len(closed_positions) * 100) if closed_positions else 0
        
        # Calculate P&L metrics
        realized_profit = sum(p['realized_profit'] for p in all_positions)
        unrealized_profit = sum(p['unrealized_profit'] for p in all_positions)
        total_profit = realized_profit + unrealized_profit
        
        total_volume = sum(p['volume'] for p in all_positions)
        total_invested = sum(p.get('buy_value_usd', 0) for p in all_positions)
        roi = (total_profit / total_volume * 100) if total_volume > 0 else 0
        
        # Build top tokens list
        top_tokens = []
        for pos in all_positions:
            top_tokens.append({
                'symbol': pos['symbol'],
                'profit': pos['total_profit'],
                'realized': pos['realized_profit'],
                'unrealized': pos['unrealized_profit'],
                'status': pos['status'],
                'trades': pos['trade_count'],
                'win_rate': 100 if pos['is_profitable'] else 0,
                'address': pos['token_address']
            })
        
        top_tokens.sort(key=lambda x: x['profit'], reverse=True)
        
        logger.info(f"📊 Closed: {len(closed_positions)} | Partial: {len(partial_positions)} | Open: {len(open_positions)}")
        logger.info(f"📊 Win Rate: All={all_win_rate:.1f}% | Closed={closed_win_rate:.1f}%")
        
        return {
            'total_trades': len(all_positions),
            'profitable_trades': all_wins,
            'losing_trades': len(all_positions) - all_wins,
            'win_rate': round(all_win_rate, 2),
            'total_profit': round(total_profit, 2),
            'realized_profit': round(realized_profit, 2),
            'unrealized_profit': round(unrealized_profit, 2),
            'total_volume': round(total_volume, 2),
            'total_invested': round(total_invested, 2),
            'roi': round(roi, 2),
            'avg_trade_size': round(total_volume / len(all_positions), 2) if all_positions else 0,
            'closed_positions': len(closed_positions),
            'partial_positions': len(partial_positions),
            'open_positions': len(open_positions),
            'win_rate_closed_only': round(closed_win_rate, 2),
            'top_tokens': top_tokens[:10],
            'tokens_analyzed': len(token_positions),
            'analysis_timestamp': datetime.now().isoformat()
        }


    async def _analyze_token_position_enhanced(self, token: str, trades: List[Dict]) -> Optional[Dict]:
        """Enhanced position analysis with open position handling"""
        
        try:
            self.debug_stats['tokens_attempted'] += 1
            
            # Get position status
            analyzer = EnhancedPositionAnalyzer()
            position_info = analyzer.analyze_position_status(trades)
            
            status = position_info['status']
            total_bought = position_info['total_bought']
            total_sold = position_info['total_sold']
            remaining = position_info['remaining_amount']
            
            # Fetch prices for all trades
            price_tasks = []
            for trade in trades:
                ts = trade.get('timestamp', 0)
                price_tasks.append(
                    self.price_fetcher.get_token_price_fast(token, ts, self.session)
                )
            
            prices = await asyncio.gather(*price_tasks, return_exceptions=True)
            
            # Calculate USD values
            buy_value_usd = 0.0
            sell_value_usd = 0.0
            current_price = 0.0
            prices_found = 0
            
            for i, trade in enumerate(trades):
                if i >= len(prices):
                    break
                
                price_result = prices[i]
                if isinstance(price_result, Exception):
                    continue
                
                price, source = price_result
                
                if price > 0:
                    prices_found += 1
                    self.debug_stats['tokens_with_prices'] += 1
                    
                    if source not in self.debug_stats['source_breakdown']:
                        self.debug_stats['source_breakdown'][source] = 0
                    self.debug_stats['source_breakdown'][source] += 1
                    
                    amount = trade.get('amount', 0)
                    usd_value = amount * price
                    
                    if trade.get('type') == 'buy':
                        buy_value_usd += usd_value
                    else:
                        sell_value_usd += usd_value
                    
                    current_price = price
            
            if prices_found == 0:
                return None
            
            # Calculate P&L
            avg_buy_price = buy_value_usd / total_bought if total_bought > 0 else 0
            
            if total_sold > 0 and total_bought > 0:
                cost_basis_sold = avg_buy_price * total_sold
                realized_profit = sell_value_usd - cost_basis_sold
            else:
                realized_profit = 0.0
            
            unrealized_profit = 0.0
            if remaining > 0 and current_price > 0:
                current_value = current_price * remaining
                cost_basis_remaining = avg_buy_price * remaining
                unrealized_profit = current_value - cost_basis_remaining
            
            total_profit = realized_profit + unrealized_profit
            volume = buy_value_usd + sell_value_usd
            
            return {
                'token_address': token,
                'symbol': trades[0].get('symbol', 'Unknown')[:15],
                'status': status,
                'is_profitable': total_profit > 0,
                'total_bought': total_bought,
                'total_sold': total_sold,
                'remaining': remaining,
                'sell_ratio': position_info['sell_ratio'],
                'total_profit': total_profit,
                'realized_profit': realized_profit,
                'unrealized_profit': unrealized_profit,
                'volume': volume,
                'buy_value_usd': buy_value_usd,
                'sell_value_usd': sell_value_usd,
                'trade_count': len(trades),
                'avg_buy_price': avg_buy_price,
                'current_price': current_price
            }
            
        except Exception as e:
            logger.debug(f"Position analysis error for {token[:8]}: {e}")
            return None
        
    def _empty_metrics(self) -> Dict:
        """Enhanced empty metrics"""
        return {
            'total_trades': 0,
            'profitable_trades': 0,
            'losing_trades': 0,
            'win_rate': 0,
            'total_profit': 0,
            'realized_profit': 0,
            'unrealized_profit': 0,
            'total_volume': 0,
            'total_invested': 0,
            'roi': 0,
            'avg_trade_size': 0,
            'closed_positions': 0,
            'partial_positions': 0,
            'open_positions': 0,
            'win_rate_closed_only': 0,
            'top_tokens': [],
            'tokens_analyzed': 0
        }

    
    def _error_response(self, wallet: str, message: str) -> Dict:
        """Error response"""
        return {
            'success': False,
            'wallet': wallet,
            'address': wallet,
            'message': message,
            **self._empty_metrics()
        }
    
    async def close(self):
        """Cleanup"""
        if self.session and not self.session.closed:
            await self.session.close()
            await asyncio.sleep(0.25)
            logger.info("⚡ Fast analyzer closed")



class EVMAnalyzer:
    """Production-ready EVM wallet analyzer"""
    
    def __init__(self):
        self.chains = {
            'ethereum': {
                'rpc': 'https://eth.llamarpc.com',
                'explorer_api': 'https://api.etherscan.io/api',
                'native_symbol': 'ETH'
            },
            'bsc': {
                'rpc': 'https://bsc-dataseed1.binance.org',
                'explorer_api': 'https://api.bscscan.com/api',
                'native_symbol': 'BNB'
            },
            'polygon': {
                'rpc': 'https://polygon-rpc.com',
                'explorer_api': 'https://api.polygonscan.com/api',
                'native_symbol': 'MATIC'
            },
            'arbitrum': {
                'rpc': 'https://arb1.arbitrum.io/rpc',
                'explorer_api': 'https://api.arbiscan.io/api',
                'native_symbol': 'ETH'
            },
            'base': {
                'rpc': 'https://mainnet.base.org',
                'explorer_api': 'https://api.basescan.org/api',
                'native_symbol': 'ETH'
            }
        }
        
        self.api_keys = {}
        self.session = None
        self.cache = SimpleCache(ttl_seconds=300)
        self.rate_limiter = BirdeyeRateLimiter(max_calls=5, time_window=1)
    
    async def analyze_wallet(self, wallet_address: str, 
                            chain: str = 'ethereum',
                            timeout: int = 45) -> Dict:
        """Analyze EVM wallet with caching"""
        
        logger.info(f"🔍 Analyzing {chain.upper()} wallet: {wallet_address[:8]}...")
        
        # Validate address
        try:
            wallet_address = Web3.to_checksum_address(wallet_address)
        except Exception as e:
            return self._create_error_response(wallet_address, chain, f"Invalid address: {e}")
        
        # Check cache
        cache_key = f"{chain}_{wallet_address}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        try:
            result = await asyncio.wait_for(
                self._analyze_wallet_internal(wallet_address, chain),
                timeout=timeout
            )
            
            if result.get('success', True):
                self.cache.set(cache_key, result)
            
            return result
        
        except asyncio.TimeoutError:
            return self._create_timeout_response(wallet_address, chain)
        except Exception as e:
            logger.error(f"EVM analysis error: {e}", exc_info=True)
            return self._create_error_response(wallet_address, chain, str(e))
    
    async def _analyze_wallet_internal(self, address: str, chain: str) -> Dict:
        """Internal EVM analysis logic"""
        
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        
        # Fetch token transfers
        transactions = await self.fetch_token_transfers(address, chain)
        
        if not transactions:
            return self._create_no_data_response(address, chain)
        
        logger.info(f"✅ Found {len(transactions)} token transfers")
        
        # Parse trades
        trades = self.parse_evm_trades(transactions, address)

        profit_7d, profit_7d_pct = self.calculate_7d_profit(trades)
        
        if not trades:
            return self._create_no_trades_response(address, chain)
        
        # Calculate metrics
        metrics = self.calculate_evm_metrics(trades)
        
        return {
            'success': True,
            'address': address,
            'chain': chain,
            'profit_7d': profit_7d,        # ⭐ ADD THIS
            'profit_7d_pct': profit_7d_pct, 
            **metrics,
            'analysis_timestamp': datetime.now().isoformat()
        }
    
    @retry_on_failure(max_retries=2, delay=1.0)
    async def fetch_token_transfers(self, address: str, chain: str) -> List[Dict]:
        """Fetch ERC20 token transfers"""
        
        chain_config = self.chains.get(chain)
        if not chain_config:
            return []
        
        await self.rate_limiter.acquire()
        
        try:
            params = {
                'module': 'account',
                'action': 'tokentx',
                'address': address,
                'page': 1,
                'offset': 100,
                'sort': 'desc'
            }
            
            # Add API key if available
            api_key = self.api_keys.get(f"{chain}scan")
            if api_key:
                params['apikey'] = api_key
            
            async with self.session.get(chain_config['explorer_api'], params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('status') == '1' and data.get('result'):
                        return data['result']
                    else:
                        logger.warning(f"Explorer API: {data.get('message', 'Unknown error')}")
        
        except Exception as e:
            logger.warning(f"Fetch error: {e}")
        
        return []
    
    def parse_evm_trades(self, transactions: List[Dict], wallet: str) -> List[Dict]:
        """Parse EVM token transfers into trades"""
        
        trades = []
        wallet_lower = wallet.lower()
        
        for tx in transactions:
            try:
                from_addr = tx.get('from', '').lower()
                to_addr = tx.get('to', '').lower()
                
                # Determine trade type
                if to_addr == wallet_lower:
                    trade_type = 'buy'
                elif from_addr == wallet_lower:
                    trade_type = 'sell'
                else:
                    continue
                
                trades.append({
                    'timestamp': int(tx.get('timeStamp', 0)),
                    'token': tx.get('contractAddress'),
                    'token_symbol': tx.get('tokenSymbol', 'Unknown'),
                    'type': trade_type,
                    'amount': float(tx.get('value', 0)) / (10 ** int(tx.get('tokenDecimal', 18))),
                    'hash': tx.get('hash')
                })
            
            except Exception as e:
                logger.debug(f"Parse error: {e}")
                continue
        
        return trades

    def calculate_7d_profit(self, trades: List[Dict]) -> Tuple[float, float]:
        """Calculate 7-day profit for EVM chains"""
        try:
            seven_days_ago = datetime.now() - timedelta(days=7)
            seven_days_ago_ts = int(seven_days_ago.timestamp())
            
            recent_profit = 0.0
            recent_volume = 0.0
            token_trades_7d = {}
            
            for trade in trades:
                trade_time = int(trade.get('timestamp', 0))
                
                if trade_time >= seven_days_ago_ts:
                    token = trade.get('token', 'unknown')
                    if token not in token_trades_7d:
                        token_trades_7d[token] = []
                    token_trades_7d[token].append(trade)
            
            for token, token_trade_list in token_trades_7d.items():
                if self._is_token_profitable(token_trade_list):
                    recent_profit += len(token_trade_list) * 100
                else:
                    recent_profit += len(token_trade_list) * -50
                
                for trade in token_trade_list:
                    recent_volume += float(trade.get('amount', 0))
            
            profit_pct = (recent_profit / recent_volume * 100) if recent_volume > 0 else 0.0
            return recent_profit, profit_pct
            
        except Exception as e:
            logger.error(f"7d profit calc error: {e}")
            return 0.0, 0.0
    
    def calculate_evm_metrics(self, trades: List[Dict]) -> Dict:
        """Calculate EVM trading metrics"""
        
        if not trades:
            return self._empty_metrics()
        
        # Group by token
        token_trades = defaultdict(list)
        for trade in trades:
            token = trade.get('token')
            if token:
                token_trades[token].append(trade)
        
        # Calculate stats
        total_tokens = len(token_trades)
        profitable_count = sum(1 for token_list in token_trades.values()
                              if self._is_token_profitable(token_list))
        
        win_rate = (profitable_count / total_tokens * 100) if total_tokens > 0 else 0
        
        # Get top tokens
        top_tokens = []
        for token, token_list in token_trades.items():
            symbol = token_list[0].get('token_symbol', 'Unknown')
            top_tokens.append({
                'symbol': symbol,
                'trades': len(token_list),
                'profit': 100 if self._is_token_profitable(token_list) else -50
            })
        
        top_tokens.sort(key=lambda x: x['profit'], reverse=True)
        
        # Time metrics
        timestamps = [t.get('timestamp', 0) for t in trades if t.get('timestamp')]
        if timestamps:
            active_days = int((max(timestamps) - min(timestamps)) / 86400) + 1
            last_trade_time = datetime.fromtimestamp(max(timestamps)).strftime('%Y-%m-%d %H:%M')
        else:
            active_days = 0
            last_trade_time = 'N/A'
        
        return {
            'total_trades': total_tokens,
            'profitable_trades': profitable_count,
            'win_rate': win_rate,
            'total_profit': profitable_count * 100 - (total_tokens - profitable_count) * 50,
            'avg_trade_size': sum(t.get('amount', 0) for t in trades) / len(trades),
            'total_volume': sum(t.get('amount', 0) for t in trades),
            'top_tokens': top_tokens[:5],
            'last_trade_time': last_trade_time,
            'active_days': max(active_days, 1),
            'recent_win_rate': win_rate  # Simplified
        }
    
    
    def _empty_metrics(self) -> Dict:
        """Empty metrics template"""
        return {
            'total_trades': 0,
            'profitable_trades': 0,
            'win_rate': 0,
            'total_profit': 0,
            'avg_trade_size': 0,
            'total_volume': 0,
            'top_tokens': [],
            'last_trade_time': 'N/A',
            'active_days': 0,
            'recent_win_rate': 0
        }
    
    def _create_timeout_response(self, address: str, chain: str) -> Dict:
        """Timeout response"""
        return {
            'success': False,
            'error': 'timeout',
            'address': address,
            'chain': chain,
            'message': 'Analysis timed out',
            'total_trades': 0,
            'win_rate': 0,
            'total_profit': 0,
            'top_tokens': []
        }
    
    def _create_error_response(self, address: str, chain: str, error: str) -> Dict:
        """Error response"""
        return {
            'success': False,
            'error': 'analysis_error',
            'address': address,
            'chain': chain,
            'message': f'Analysis failed: {error[:100]}',
            'total_trades': 0,
            'win_rate': 0,
            'total_profit': 0,
            'top_tokens': []
        }
    
    def _create_no_data_response(self, address: str, chain: str) -> Dict:
        """No data response"""
        return {
            'success': False,
            'error': 'no_data',
            'address': address,
            'chain': chain,
            'message': 'No token transfers found',
            'total_trades': 0,
            'win_rate': 0,
            'total_profit': 0,
            'top_tokens': []
        }
    
    def _create_no_trades_response(self, address: str, chain: str) -> Dict:
        """No trades response"""
        return {
            'success': False,
            'error': 'no_trades',
            'address': address,
            'chain': chain,
            'message': 'No token trades found',
            'total_trades': 0,
            'win_rate': 0,
            'total_profit': 0,
            'top_tokens': []
        }
    
    async def close(self):
        """Cleanup resources"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None


class AnalyzerFactory:
    """Factory for creating enhanced analyzers"""
    
    _enhanced_solana = None
    
    @classmethod
    def get_enhanced_solana_analyzer(
        cls, 
        helius_api_key: str,
        birdeye_api_key: Optional[str] = None
    ) -> FastEnhancedSolanaAnalyzer:
        """Get or create enhanced Solana analyzer"""
        if cls._enhanced_solana is None:
            cls._enhanced_solana = FastEnhancedSolanaAnalyzer(
                helius_api_key=helius_api_key,
                birdeye_api_key=birdeye_api_key
            )
        
        return cls._enhanced_solana
    
    @classmethod
    async def cleanup_all(cls):
        """Cleanup all analyzers"""
        if cls._enhanced_solana:
            await cls._enhanced_solana.close()
            cls._enhanced_solana = None

# Export
__all__ = ['FastEnhancedSolanaAnalyzer', 'FastMultiSourcePriceFetcher','AnalyzerFactory']

class AnalyzerHealthMonitor:
    """Monitor analyzer health and performance"""
    
    def __init__(self):
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'cache_hits': 0,
            'avg_response_time': 0,
            'last_reset': datetime.now()
        }
        self.response_times = []
    
    def record_request(self, success: bool, response_time: float, cached: bool = False):
        """Record request metrics"""
        self.stats['total_requests'] += 1
        
        if success:
            self.stats['successful_requests'] += 1
        else:
            self.stats['failed_requests'] += 1
        
        if cached:
            self.stats['cache_hits'] += 1
        
        self.response_times.append(response_time)
        
        # Keep only last 100 response times
        if len(self.response_times) > 100:
            self.response_times.pop(0)
        
        # Update average
        if self.response_times:
            self.stats['avg_response_time'] = sum(self.response_times) / len(self.response_times)
    
    def get_health_status(self) -> Dict:
        """Get current health status"""
        uptime = (datetime.now() - self.stats['last_reset']).total_seconds()
        
        success_rate = 0
        if self.stats['total_requests'] > 0:
            success_rate = (self.stats['successful_requests'] / self.stats['total_requests']) * 100
        
        cache_hit_rate = 0
        if self.stats['total_requests'] > 0:
            cache_hit_rate = (self.stats['cache_hits'] / self.stats['total_requests']) * 100
        
        return {
            'status': 'healthy' if success_rate > 80 else 'degraded',
            'uptime_seconds': int(uptime),
            'total_requests': self.stats['total_requests'],
            'success_rate': round(success_rate, 2),
            'cache_hit_rate': round(cache_hit_rate, 2),
            'avg_response_time_ms': round(self.stats['avg_response_time'] * 1000, 2),
            'last_reset': self.stats['last_reset'].isoformat()
        }
    
    def reset(self):
        """Reset statistics"""
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'cache_hits': 0,
            'avg_response_time': 0,
            'last_reset': datetime.now()
        }
        self.response_times = []


# Global health monitor instance
health_monitor = AnalyzerHealthMonitor()


def track_performance(func):
    """Decorator to track analyzer performance"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = datetime.now()
        success = False
        cached = False
        
        try:
            result = await func(*args, **kwargs)
            success = result.get('success', True) if isinstance(result, dict) else True
            
            # Check if result was cached
            if isinstance(result, dict) and 'cached' in result:
                cached = result['cached']
            
            return result
        
        except Exception as e:
            logger.error(f"Performance tracking error: {e}")
            raise
        
        finally:
            response_time = (datetime.now() - start_time).total_seconds()
            health_monitor.record_request(success, response_time, cached)
    
    return wrapper




# Export production-ready analyzers
__all__ = [
    'AnalyzerFactory',
    'AnalyzerHealthMonitor',
    'health_monitor',
    'RateLimiter',
    'SimpleCache'
]