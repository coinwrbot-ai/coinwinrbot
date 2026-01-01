"""
Enhanced Solana Token Fetcher v2.2 - Transaction Fetch Fix
✅ Fixed transaction fetching from Helius API
✅ Added fallback transaction methods
✅ Better error handling for transaction data
✅ Support for both parsed and raw transaction formats
"""

import aiohttp
import asyncio
from typing import List, Dict, Optional, Set
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from contextlib import asynccontextmanager
from collections import defaultdict

logger = logging.getLogger(__name__)

# API Configuration
try:
    from features.config import Config
    HELIUS_API_KEY = getattr(Config, 'HELIUS_API_KEY', None)
    BIRDEYE_API_KEY = getattr(Config, 'BIRDEYE_API_KEY', None)
except ImportError:
    HELIUS_API_KEY = None
    BIRDEYE_API_KEY = None


class PriceSource(Enum):
    """Price data sources in priority order"""
    DEXSCREENER = "DexScreener"
    JUPITER = "Jupiter"
    BIRDEYE = "Birdeye"
    HELIUS = "Helius"
    CACHED = "Cached"
    NONE = "None"


@dataclass
class CacheEntry:
    """Single cache entry with metadata"""
    data: Dict
    timestamp: datetime
    ttl_seconds: int = 300
    
    def is_valid(self) -> bool:
        age = (datetime.now() - self.timestamp).total_seconds()
        return age < self.ttl_seconds


@dataclass
class SmartCache:
    """Multi-layer cache with TTL management"""
    metadata: Dict[str, CacheEntry] = field(default_factory=dict)
    prices: Dict[str, CacheEntry] = field(default_factory=dict)
    jupiter_tokens: Dict[str, Dict[str, str]] = field(default_factory=dict)
    jupiter_loaded_at: Optional[datetime] = None
    jupiter_ttl_hours: int = 24
    
    # In-flight request tracking to prevent duplicates
    _inflight_requests: Dict[str, asyncio.Future] = field(default_factory=dict)
    
    def get_metadata(self, mint: str) -> Optional[Dict]:
        entry = self.metadata.get(mint)
        if entry and entry.is_valid():
            return entry.data
        return None
    
    def set_metadata(self, mint: str, data: Dict, ttl: int = 3600):
        self.metadata[mint] = CacheEntry(data, datetime.now(), ttl)
    
    def get_price(self, mint: str) -> Optional[float]:
        entry = self.prices.get(mint)
        if entry and entry.is_valid():
            return entry.data.get('price')
        return None
    
    def set_price(self, mint: str, price: float, ttl: int = 300):
        self.prices[mint] = CacheEntry({'price': price}, datetime.now(), ttl)
    
    def get_jupiter_symbol(self, mint: str) -> Optional[str]:
        if self.is_jupiter_fresh() and mint in self.jupiter_tokens:
            return self.jupiter_tokens[mint].get('symbol')
        return None
    
    def is_jupiter_fresh(self) -> bool:
        if not self.jupiter_loaded_at:
            return False
        age = datetime.now() - self.jupiter_loaded_at
        return age < timedelta(hours=self.jupiter_ttl_hours)
    
    def cleanup_expired(self):
        now = datetime.now()
        self.metadata = {k: v for k, v in self.metadata.items() if v.is_valid()}
        self.prices = {k: v for k, v in self.prices.items() if v.is_valid()}
        # Clean up old inflight tracking
        self._inflight_requests = {k: v for k, v in self._inflight_requests.items() if not v.done()}


_CACHE = SmartCache()


@dataclass
class CircuitBreaker:
    """Simple circuit breaker for API endpoints"""
    failure_threshold: int = 3
    reset_timeout: int = 60
    failures: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    last_failure: Dict[str, datetime] = field(default_factory=dict)
    
    def is_open(self, endpoint: str) -> bool:
        """Check if circuit is open (too many failures)"""
        if endpoint not in self.failures:
            return False
        
        if self.failures[endpoint] >= self.failure_threshold:
            last_fail = self.last_failure.get(endpoint)
            if last_fail and (datetime.now() - last_fail).seconds < self.reset_timeout:
                return True
            else:
                # Reset after timeout
                self.failures[endpoint] = 0
                return False
        return False
    
    def record_failure(self, endpoint: str):
        self.failures[endpoint] += 1
        self.last_failure[endpoint] = datetime.now()
    
    def record_success(self, endpoint: str):
        self.failures[endpoint] = 0


class OptimizedTokenFetcher:
    """High-performance token fetcher with timeout protection"""
    
    # RPC Configuration
    DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"
    SOLANA_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    
    # Adaptive Timeouts (more generous)
    TIMEOUT_RPC = 8.0
    TIMEOUT_QUICK_API = 5.0
    TIMEOUT_ENRICHMENT = 20.0
    TIMEOUT_BACKGROUND = 15.0  # Increased for transaction fetching
    
    # Batch Processing
    MAX_CONCURRENT_ENRICHMENTS = 15
    BATCH_SIZE = 5
    
    # Connection Pool
    CONNECTOR_LIMIT = 40
    CONNECTOR_LIMIT_PER_HOST = 8
    
    def __init__(self, helius_key: Optional[str] = None):
        self.helius_key = helius_key or HELIUS_API_KEY
        self.birdeye_key = BIRDEYE_API_KEY
        self.session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._cache = _CACHE
        self._circuit_breaker = CircuitBreaker()
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_ENRICHMENTS)
        self._resolver = aiohttp.AsyncResolver() 
        
    @asynccontextmanager
    async def _get_session(self):
        """Context manager for session with automatic cleanup"""
        async with self._session_lock:
            if not self.session or self.session.closed:
                connector = aiohttp.TCPConnector(
                    limit=self.CONNECTOR_LIMIT,
                    limit_per_host=self.CONNECTOR_LIMIT_PER_HOST,
                    ttl_dns_cache=300,
                    enable_cleanup_closed=True,
                    force_close=False,
                    resolver=self._resolver,
                    family=0
                )
                
                self.session = aiohttp.ClientSession(
                    connector=connector,
                    headers={
                        'Accept': 'application/json',
                        'User-Agent': 'Mozilla/5.0 (compatible; TokenFetcher/2.2)'
                    },
                    timeout=aiohttp.ClientTimeout(total=30, connect=5)
                )
        
        try:
            yield self.session
        finally:
            pass
    
    def _get_rpc_url(self) -> str:
        if self.helius_key:
            return f"https://mainnet.helius-rpc.com/?api-key={self.helius_key}"
        return self.DEFAULT_RPC_URL
    
    async def fetch_wallet_top_tokens(
        self, 
        wallet: str, 
        limit: int = 15,
        include_zero_balance: bool = False
    ) -> List[Dict]:
        """
        Fetch and enrich wallet's top tokens with timeout protection
        """
        logger.info(f"🔍 Fetching tokens for {wallet[:8]}... (limit={limit})")
        start_time = datetime.now()
        
        try:
            # Cleanup
            self._cache.cleanup_expired()
            
            # Step 1: Get token accounts with retry
            tokens = await self._get_token_accounts_with_retry(wallet, include_zero_balance)
            
            if not tokens:
                logger.warning("No tokens found")
                return []
            
            logger.info(f"✅ Found {len(tokens)} token accounts")
            
            # Step 2: Pre-load Jupiter cache (non-blocking)
            if not self._cache.is_jupiter_fresh():
                asyncio.create_task(self._load_jupiter_cache())
            
            # Step 3: Smart enrichment with early return strategy
            candidates = tokens[:min(40, limit * 8)]
            
            try:
                enriched = await asyncio.wait_for(
                    self._enrich_tokens_smart(candidates, target_count=limit),
                    timeout=self.TIMEOUT_ENRICHMENT
                )
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Enrichment timeout, using partial results")
                enriched = await self._get_partial_enrichment(candidates[:limit])
            
            if not enriched:
                logger.warning("Enrichment failed, returning basic format")
                return self._format_basic_tokens(tokens[:limit])
            
            # Step 4: Sort and select top tokens
            enriched.sort(key=lambda x: x.get('current_value', 0), reverse=True)
            result = enriched[:limit]
            
            # Step 5: Background enhancements (fire and forget)
            asyncio.create_task(self._enhance_tokens_background(result, wallet))
            
            elapsed = (datetime.now() - start_time).total_seconds()
            self._log_summary(result, elapsed)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error: {e}", exc_info=True)
            if 'tokens' in locals() and tokens:
                return self._format_basic_tokens(tokens[:limit])
            return []
    
    async def _get_token_accounts_with_retry(
        self, 
        wallet: str, 
        include_zero: bool,
        max_retries: int = 2
    ) -> List[Dict]:
        """Get token accounts with retry logic"""
        for attempt in range(max_retries):
            try:
                return await asyncio.wait_for(
                    self._get_token_accounts(wallet, include_zero),
                    timeout=self.TIMEOUT_RPC
                )
            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    logger.warning(f"RPC timeout, retry {attempt + 1}/{max_retries}")
                    await asyncio.sleep(0.5)
                else:
                    logger.error("RPC timeout after all retries")
                    return []
            except Exception as e:
                logger.error(f"RPC error: {e}")
                return []
        return []
    
    async def _get_token_accounts(self, wallet: str, include_zero: bool) -> List[Dict]:
        """Fetch SPL token accounts via RPC"""
        try:
            async with self._get_session() as session:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenAccountsByOwner",
                    "params": [
                        wallet,
                        {"programId": self.SOLANA_TOKEN_PROGRAM},
                        {"encoding": "jsonParsed"}
                    ]
                }
                
                timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_RPC)
                async with session.post(
                    self._get_rpc_url(), 
                    json=payload, 
                    timeout=timeout
                ) as resp:
                    if resp.status != 200:
                        logger.error(f"RPC error: {resp.status}")
                        return []
                    
                    data = await resp.json()
                    if 'error' in data:
                        logger.error(f"RPC error: {data['error']}")
                        return []
                    
                    return self._parse_token_accounts(data, include_zero)
        
        except Exception as e:
            logger.error(f"Failed to get token accounts: {e}")
            return []
    
    def _parse_token_accounts(self, rpc_data: Dict, include_zero: bool) -> List[Dict]:
        """Parse RPC response into sorted token list"""
        accounts = rpc_data.get('result', {}).get('value', [])
        tokens = []
        
        for account in accounts:
            try:
                info = account['account']['data']['parsed']['info']
                token_amount = info['tokenAmount']
                balance = float(token_amount.get('uiAmount', 0) or 0)
                
                if balance > 0 or include_zero:
                    tokens.append({
                        'address': info['mint'],
                        'balance': balance,
                        'decimals': int(token_amount.get('decimals', 9))
                    })
            except (KeyError, TypeError, ValueError):
                continue
        
        tokens.sort(key=lambda x: x['balance'], reverse=True)
        return tokens
    
    async def _enrich_tokens_smart(
        self, 
        tokens: List[Dict],
        target_count: int
    ) -> List[Dict]:
        """
        Smart enrichment with early termination once we have enough good tokens
        """
        logger.info(f"💎 Enriching {len(tokens)} tokens (target: {target_count})...")
        
        enriched = []
        good_tokens = 0
        
        for i in range(0, len(tokens), self.BATCH_SIZE):
            batch = tokens[i:i + self.BATCH_SIZE]
            
            batch_results = await asyncio.gather(
                *[self._enrich_single_token_safe(token) for token in batch],
                return_exceptions=True
            )
            
            for result in batch_results:
                if isinstance(result, dict) and result.get('symbol'):
                    enriched.append(result)
                    if result.get('price', 0) > 0:
                        good_tokens += 1
            
            if good_tokens >= target_count * 3:
                logger.info(f"✅ Early exit: {good_tokens} priced tokens")
                break
            
            await asyncio.sleep(0.1)
        
        logger.info(f"✅ Enriched {len(enriched)} tokens ({good_tokens} priced)")
        return enriched
    
    async def _enrich_single_token_safe(self, token: Dict) -> Optional[Dict]:
        """Enrich with semaphore protection and error isolation"""
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._enrich_single_token(token),
                    timeout=6.0
                )
            except asyncio.TimeoutError:
                logger.debug(f"Token {token['address'][:8]} timeout")
                return self._create_fallback_token(token)
            except Exception as e:
                logger.debug(f"Token {token['address'][:8]} error: {e}")
                return self._create_fallback_token(token)
    
    async def _enrich_single_token(self, token: Dict) -> Optional[Dict]:
        """Enrich single token with deduplication"""
        mint = token['address']
        balance = token['balance']
        
        # Check if request already in flight
        if mint in self._cache._inflight_requests:
            try:
                cached_data = await asyncio.wait_for(
                    self._cache._inflight_requests[mint],
                    timeout=5.0
                )
                if cached_data:
                    return self._build_token_result(mint, balance, cached_data)
            except:
                pass
        
        # Check cache
        cached_meta = self._cache.get_metadata(mint)
        cached_price = self._cache.get_price(mint)
        
        if cached_meta and cached_price:
            return self._build_token_result(
                mint, 
                balance, 
                {
                    'symbol': cached_meta['symbol'],
                    'name': cached_meta.get('name', ''),
                    'price': cached_price
                }
            )
        
        # Create future for deduplication
        future = asyncio.Future()
        self._cache._inflight_requests[mint] = future
        
        try:
            # Fetch fresh data
            data = await self._fetch_token_data_multi_source(mint)
            
            if data:
                symbol = data.get('symbol')
                name = data.get('name', '')
                price = data.get('price', 0.0)
                
                # Cache results
                if symbol:
                    self._cache.set_metadata(mint, {'symbol': symbol, 'name': name})
                if price > 0:
                    self._cache.set_price(mint, price)
                
                result = self._build_token_result(mint, balance, data)
                future.set_result(data)
                return result
            
            # Fallback to Jupiter cache
            symbol = self._cache.get_jupiter_symbol(mint)
            if symbol:
                result = self._build_token_result(mint, balance, {'symbol': symbol})
                future.set_result({'symbol': symbol})
                return result
            
            future.set_result(None)
            return self._create_fallback_token(token)
            
        except Exception as e:
            future.set_exception(e)
            return self._create_fallback_token(token)
        finally:
            if mint in self._cache._inflight_requests:
                del self._cache._inflight_requests[mint]
    
    def _build_token_result(self, mint: str, balance: float, data: Dict) -> Dict:
        """Build standardized token result"""
        symbol = data.get('symbol', f"{mint[:4]}...{mint[-4:]}")
        price = data.get('price', 0.0)
        value = balance * price
        
        return {
            'symbol': symbol[:15],
            'name': data.get('name', symbol)[:30],
            'address': mint,
            'mint': mint,
            'balance': balance,
            'price': price,
            'price_change_24h': data.get('price_change_24h', 0.0),
            'current_value': value,
            'profit': 0.0,
            'realized_profit': 0.0,
            'unrealized_profit': value,
            'trades': 0,
            'win_rate': 0.0,
            '_source': data.get('source', PriceSource.NONE.value)
        }
    
    def _create_fallback_token(self, token: Dict) -> Dict:
        """Create fallback token with basic info"""
        mint = token['address']
        symbol = self._cache.get_jupiter_symbol(mint) or f"{mint[:4]}...{mint[-4:]}"
        
        return {
            'symbol': symbol[:15],
            'name': symbol,
            'address': mint,
            'mint': mint,
            'balance': token['balance'],
            'price': 0.0,
            'price_change_24h': 0.0,
            'current_value': 0.0,
            'profit': 0.0,
            'realized_profit': 0.0,
            'unrealized_profit': 0.0,
            'trades': 0,
            'win_rate': 0.0,
            '_source': PriceSource.NONE.value
        }
    
    async def _get_partial_enrichment(self, tokens: List[Dict]) -> List[Dict]:
        """Quick enrichment from cache only"""
        results = []
        for token in tokens:
            mint = token['address']
            cached_meta = self._cache.get_metadata(mint)
            cached_price = self._cache.get_price(mint)
            
            if cached_meta:
                results.append(self._build_token_result(
                    mint,
                    token['balance'],
                    {
                        'symbol': cached_meta['symbol'],
                        'name': cached_meta.get('name', ''),
                        'price': cached_price or 0.0
                    }
                ))
            else:
                results.append(self._create_fallback_token(token))
        
        return results
    
    async def _fetch_token_data_multi_source(self, mint: str) -> Optional[Dict]:
        """Fetch with circuit breaker protection"""
        
        # Try DexScreener first
        if not self._circuit_breaker.is_open('dexscreener'):
            try:
                data = await asyncio.wait_for(
                    self._fetch_dexscreener(mint),
                    timeout=self.TIMEOUT_QUICK_API
                )
                if data:
                    self._circuit_breaker.record_success('dexscreener')
                    return {**data, 'source': PriceSource.DEXSCREENER}
            except:
                self._circuit_breaker.record_failure('dexscreener')
        
        # Try Jupiter
        if not self._circuit_breaker.is_open('jupiter'):
            try:
                data = await asyncio.wait_for(
                    self._fetch_price_jupiter(mint),
                    timeout=self.TIMEOUT_QUICK_API
                )
                if data and data.get('price', 0) > 0:
                    self._circuit_breaker.record_success('jupiter')
                    return {**data, 'source': PriceSource.JUPITER}
            except:
                self._circuit_breaker.record_failure('jupiter')
        
        # Try Helius metadata
        if self.helius_key and not self._circuit_breaker.is_open('helius'):
            try:
                data = await asyncio.wait_for(
                    self._fetch_helius_metadata(mint),
                    timeout=self.TIMEOUT_QUICK_API
                )
                if data:
                    self._circuit_breaker.record_success('helius')
                    return {**data, 'source': PriceSource.HELIUS}
            except:
                self._circuit_breaker.record_failure('helius')
        
        # Try Birdeye as last resort
        if self.birdeye_key and not self._circuit_breaker.is_open('birdeye'):
            try:
                price = await asyncio.wait_for(
                    self._fetch_price_birdeye(mint),
                    timeout=self.TIMEOUT_QUICK_API
                )
                if price > 0:
                    self._circuit_breaker.record_success('birdeye')
                    return {'price': price, 'source': PriceSource.BIRDEYE}
            except:
                self._circuit_breaker.record_failure('birdeye')
        
        return None
    
    async def _fetch_dexscreener(self, mint: str) -> Optional[Dict]:
        """Fetch from DexScreener"""
        try:
            async with self._get_session() as session:
                url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
                timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_QUICK_API)
                
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status != 200:
                        return None
                    
                    data = await resp.json()
                    pairs = data.get('pairs', [])
                    
                    if not pairs:
                        return None
                    
                    pair = pairs[0]
                    base = pair.get('baseToken', {})
                    
                    return {
                        'symbol': base.get('symbol', ''),
                        'name': base.get('name', ''),
                        'price': float(pair.get('priceUsd', 0) or 0),
                        'price_change_24h': float(pair.get('priceChange', {}).get('h24', 0) or 0)
                    }
        except Exception:
            return None
    
    async def _fetch_price_jupiter(self, mint: str) -> Optional[Dict]:
        """Fetch price from Jupiter"""
        try:
            async with self._get_session() as session:
                url = "https://price.jup.ag/v4/price"
                timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_QUICK_API)
                
                async with session.get(url, params={'ids': mint}, timeout=timeout) as resp:
                    if resp.status != 200:
                        return None
                    
                    data = await resp.json()
                    price = float(data.get('data', {}).get(mint, {}).get('price', 0) or 0)
                    
                    if price > 0:
                        return {'price': price}
        except Exception:
            pass
        return None
    
    async def _fetch_helius_metadata(self, mint: str) -> Optional[Dict]:
        """Fetch metadata from Helius"""
        if not self.helius_key:
            return None
        
        try:
            async with self._get_session() as session:
                url = f"https://mainnet.helius-rpc.com/?api-key={self.helius_key}"
                
                payload = {
                    "jsonrpc": "2.0",
                    "id": "1",
                    "method": "getAsset",
                    "params": {"id": mint, "displayOptions": {"showFungible": True}}
                }
                
                timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_QUICK_API)
                async with session.post(url, json=payload, timeout=timeout) as resp:
                    if resp.status != 200:
                        return None
                    
                    data = await resp.json()
                    metadata = data.get('result', {}).get('content', {}).get('metadata', {})
                    
                    symbol = metadata.get('symbol', '')
                    if symbol and symbol != 'UNKNOWN':
                        return {
                            'symbol': symbol,
                            'name': metadata.get('name', '')
                        }
        except Exception:
            pass
        return None
    
    async def _fetch_price_birdeye(self, mint: str) -> float:
        """Fetch price from Birdeye"""
        if not self.birdeye_key:
            return 0.0
        
        try:
            async with self._get_session() as session:
                url = "https://public-api.birdeye.so/defi/price"
                timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_QUICK_API)
                
                async with session.get(
                    url,
                    params={'address': mint},
                    headers={'X-API-KEY': self.birdeye_key, 'x-chain': 'solana'},
                    timeout=timeout
                ) as resp:
                    if resp.status != 200:
                        return 0.0
                    
                    data = await resp.json()
                    if data.get('success'):
                        return float(data.get('data', {}).get('value', 0) or 0)
        except Exception:
            pass
        return 0.0
    
    async def _enhance_tokens_background(self, tokens: List[Dict], wallet: str):
        """Add trade counts in background (fire and forget)"""
        try:
            addresses = [t['address'] for t in tokens]
            trade_counts = await asyncio.wait_for(
                self._fetch_trade_counts(wallet, addresses),
                timeout=self.TIMEOUT_BACKGROUND
            )
            
            for token in tokens:
                token['trades'] = trade_counts.get(token['address'], 0)
            
            if any(trade_counts.values()):
                logger.info(f"📊 Added trade counts: {sum(trade_counts.values())} total trades")
            else:
                logger.debug("No trade data found")
                
        except Exception as e:
            logger.debug(f"Background task failed: {e}")
    
    async def _fetch_trade_counts(self, wallet: str, addresses: List[str]) -> Dict[str, int]:
        """
        ✅ FIXED: Fetch trade history counts with multiple fallback methods
        """
        if not self.helius_key:
            logger.debug("No Helius API key, skipping trade counts")
            return {}
        
        # Try multiple endpoints in order
        methods = [
            self._fetch_trades_parsed,
            self._fetch_trades_enhanced,
            self._fetch_trades_signatures
        ]
        
        for method in methods:
            try:
                counts = await method(wallet, addresses)
                if counts and any(counts.values()):
                    logger.info(f"✅ Found trades using {method.__name__}")
                    return counts
            except Exception as e:
                logger.debug(f"{method.__name__} failed: {e}")
                continue
        
        logger.warning("⚠️ All transaction fetch methods failed")
        return {}
    
    async def _fetch_trades_parsed(self, wallet: str, addresses: List[str]) -> Dict[str, int]:
        """Method 1: Helius parsed transactions API (v0/addresses/{address}/transactions)"""
        try:
            async with self._get_session() as session:
                url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions"
                
                params = {
                    'api-key': self.helius_key,
                    'type': 'SWAP',  # Focus on swaps
                    'limit': 100
                }
                
                timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_BACKGROUND)
                async with session.get(url, params=params, timeout=timeout) as resp:
                    if resp.status != 200:
                        logger.debug(f"Parsed API returned {resp.status}")
                        return {}
                    
                    data = await resp.json()
                    
                    # Handle both list and dict responses
                    if isinstance(data, dict):
                        txs = data.get('transactions', []) or data.get('result', [])
                    else:
                        txs = data if isinstance(data, list) else []
                    
                    if not txs:
                        logger.debug("No transactions in parsed response")
                        return {}
                    
                    counts = {addr: 0 for addr in addresses}
                    address_set = set(addresses)
                    
                    for tx in txs:
                        # Check tokenTransfers
                        for transfer in tx.get('tokenTransfers', []):
                            mint = transfer.get('mint') or transfer.get('tokenAddress')
                            if mint in address_set:
                                counts[mint] += 1
                        
                        # Also check native transfers and instructions
                        for event in tx.get('events', {}).get('swap', []):
                            for mint in [event.get('tokenInputs', {}).get('mint'), 
                                        event.get('tokenOutputs', {}).get('mint')]:
                                if mint in address_set:
                                    counts[mint] += 1
                    
                    logger.debug(f"Parsed API: {sum(counts.values())} trades found")
                    return counts
                    
        except Exception as e:
            logger.debug(f"Parsed method error: {e}")
            return {}
    
    async def _fetch_trades_enhanced(self, wallet: str, addresses: List[str]) -> Dict[str, int]:
        """Method 2: Enhanced transactions with pagination"""
        try:
            async with self._get_session() as session:
                # Use the enhanced API endpoint
                url = "https://api.helius.xyz/v0/addresses/transactions"
                
                payload = {
                    'addresses': [wallet],
                    'limit': 100
                }
                
                headers = {
                    'Content-Type': 'application/json'
                }
                
                params = {'api-key': self.helius_key}
                
                timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_BACKGROUND)
                async with session.post(url, json=payload, params=params, headers=headers, timeout=timeout) as resp:
                    if resp.status != 200:
                        logger.debug(f"Enhanced API returned {resp.status}")
                        return {}
                    
                    data = await resp.json()
                    txs = data if isinstance(data, list) else []
                    
                    if not txs:
                        return {}
                    
                    counts = {addr: 0 for addr in addresses}
                    address_set = set(addresses)
                    
                    for tx in txs:
                        # Parse all possible token transfer formats
                        transfers = (tx.get('tokenTransfers', []) or 
                                   tx.get('token_transfers', []) or [])
                        
                        for transfer in transfers:
                            mint = (transfer.get('mint') or 
                                  transfer.get('tokenAddress') or
                                  transfer.get('token_address'))
                            if mint in address_set:
                                counts[mint] += 1
                    
                    logger.debug(f"Enhanced API: {sum(counts.values())} trades found")
                    return counts
                    
        except Exception as e:
            logger.debug(f"Enhanced method error: {e}")
            return {}
    
    async def _fetch_trades_signatures(self, wallet: str, addresses: List[str]) -> Dict[str, int]:
        """Method 3: Use RPC getSignaturesForAddress + getTransaction"""
        try:
            async with self._get_session() as session:
                # First get signatures
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignaturesForAddress",
                    "params": [wallet, {"limit": 50}]
                }
                
                timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_BACKGROUND)
                async with session.post(self._get_rpc_url(), json=payload, timeout=timeout) as resp:
                    if resp.status != 200:
                        return {}
                    
                    sig_data = await resp.json()
                    signatures = sig_data.get('result', [])
                    
                    if not signatures:
                        return {}
                    
                    counts = {addr: 0 for addr in addresses}
                    address_set = set(addresses)
                    
                    # Fetch details for each transaction (limit to first 20 to avoid timeout)
                    for sig_info in signatures[:20]:
                        sig = sig_info.get('signature')
                        if not sig:
                            continue
                        
                        tx_payload = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "getTransaction",
                            "params": [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
                        }
                        
                        try:
                            async with session.post(self._get_rpc_url(), json=tx_payload, timeout=aiohttp.ClientTimeout(total=3)) as tx_resp:
                                if tx_resp.status == 200:
                                    tx_data = await tx_resp.json()
                                    tx_result = tx_data.get('result', {})
                                    
                                    # Parse pre and post token balances
                                    meta = tx_result.get('meta', {})
                                    pre_balances = meta.get('preTokenBalances', [])
                                    post_balances = meta.get('postTokenBalances', [])
                                    
                                    for balance in pre_balances + post_balances:
                                        mint = balance.get('mint')
                                        if mint in address_set:
                                            counts[mint] += 1
                                            break  # Count once per transaction
                        except:
                            continue
                    
                    logger.debug(f"Signatures method: {sum(counts.values())} trades found")
                    return counts
                    
        except Exception as e:
            logger.debug(f"Signatures method error: {e}")
            return {}
    
    async def _load_jupiter_cache(self):
        """Load Jupiter token list with fallback"""
        if self._cache.is_jupiter_fresh():
            return
        
        endpoints = [
            "https://token.jup.ag/strict",
            "https://tokens.jup.ag/tokens?tags=verified",
            "https://cache.jup.ag/tokens"
        ]
        
        for url in endpoints:
            try:
                async with self._get_session() as session:
                    timeout = aiohttp.ClientTimeout(total=10, connect=5)
                    
                    async with session.get(url, timeout=timeout) as resp:
                        if resp.status != 200:
                            continue
                        
                        tokens = await resp.json()
                        if not isinstance(tokens, list):
                            continue
                        
                        token_map = {}
                        for token in tokens:
                            addr = token.get('address')
                            if addr:
                                token_map[addr] = {
                                    'symbol': token.get('symbol', 'UNKNOWN'),
                                    'name': token.get('name', '')
                                }
                        
                        if token_map:
                            self._cache.jupiter_tokens = token_map
                            self._cache.jupiter_loaded_at = datetime.now()
                            logger.info(f"✅ Cached {len(token_map)} Jupiter tokens from {url}")
                            return
                            
            except Exception as e:
                logger.debug(f"Failed to load from {url}: {e}")
                continue
        
        logger.warning("⚠️ All Jupiter endpoints failed - continuing without cache")

    def _format_basic_tokens(self, tokens: List[Dict]) -> List[Dict]:
        """Fallback formatting for basic token info"""
        return [{
            'symbol': self._cache.get_jupiter_symbol(t['address']) or f"{t['address'][:4]}...{t['address'][-4:]}",
            'name': '',
            'address': t['address'],
            'mint': t['address'],
            'balance': t['balance'],
            'price': 0.0,
            'price_change_24h': 0.0,
            'current_value': 0.0,
            'profit': 0.0,
            'realized_profit': 0.0,
            'unrealized_profit': 0.0,
            'trades': 0,
            'win_rate': 0.0,
            '_source': PriceSource.NONE.value
        } for t in tokens]
    
    def _log_summary(self, tokens: List[Dict], elapsed: float):
        """Log result summary"""
        total = sum(t.get('current_value', 0) for t in tokens)
        with_price = sum(1 for t in tokens if t.get('price', 0) > 0)
        with_trades = sum(1 for t in tokens if t.get('trades', 0) > 0)
        total_trades = sum(t.get('trades', 0) for t in tokens)
        
        logger.info(
            f"✅ {len(tokens)} tokens | ${total:,.2f} total | "
            f"{with_price} priced | {with_trades} with trades ({total_trades} total) | {elapsed:.2f}s"
        )
    
    async def close(self):
        """Cleanup resources"""
        if self.session and not self.session.closed:
            await self.session.close()
            await asyncio.sleep(0.1)
    
    @staticmethod
    def format_display(tokens: List[Dict]) -> str:
        """Format tokens for display"""
        if not tokens:
            return "No tokens found"
        
        lines = ["=" * 80, "🪙 WALLET TOKENS", "=" * 80]
        
        for i, t in enumerate(tokens, 1):
            lines.extend([
                f"\n{i}. {t['symbol']}",
                f"   Address: {t['address']}"
            ])
            
            if t.get('name') and t['name'] != t['symbol']:
                lines.append(f"   Name: {t['name']}")
            
            if t.get('price', 0) > 0:
                lines.append(f"   Price: ${t['price']:.8f}")
                
                if t.get('price_change_24h'):
                    change = t['price_change_24h']
                    sign = '+' if change >= 0 else ''
                    lines.append(f"   24h: {sign}{change:.2f}%")
            
            if t.get('current_value', 0) > 0:
                lines.append(f"   Value: ${t['current_value']:,.2f}")
            
            lines.append(f"   Balance: {t['balance']:,.4f}")
            
            if t.get('trades', 0) > 0:
                lines.append(f"   Trades: {t['trades']}")
            
            if t.get('_source'):
                lines.append(f"   Source: {t['_source']}")
        
        lines.append("\n" + "=" * 80)
        return "\n".join(lines)


async def preload_jupiter_tokens():
    """Preload Jupiter tokens on startup"""
    logger.info("🚀 Preloading Jupiter token list...")
    
    fetcher = OptimizedTokenFetcher()
    try:
        await fetcher._load_jupiter_cache()
        
        if _CACHE.jupiter_tokens:
            logger.info(f"✅ Preloaded {len(_CACHE.jupiter_tokens)} tokens")
        else:
            logger.warning("⚠️ Preload failed")
    except Exception as e:
        logger.warning(f"⚠️ Preload error: {e}")
    finally:
        await fetcher.close()