"""
Enhanced Token Data Fetcher - Multiple Premium Sources
✅ Birdeye API (Advanced metrics, trending, liquidity analysis)
✅ Bitquery GraphQL (Real-time trades, DEX analytics)
✅ DexScreener Enhanced (Audit info, holder distribution)
✅ Smart caching and fallback chains
✅ Risk scoring and market analysis
"""

import aiohttp
import asyncio
from typing import List, Dict, Optional, Tuple
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

# ============================================================================
# API CONFIGURATION
# ============================================================================

try:
    from features.config import Config
    BIRDEYE_API_KEY = getattr(Config, 'BIRDEYE_API_KEY', None)
    BITQUERY_API_KEY = getattr(Config, 'BITQUERY_API_KEY', None)  # Get from https://bitquery.io
except ImportError:
    BIRDEYE_API_KEY = None
    BITQUERY_API_KEY = None


class RiskLevel(Enum):
    """Token risk assessment levels"""
    VERY_LOW = "🟢 VERY LOW"
    LOW = "🟢 LOW"
    MEDIUM = "🟡 MEDIUM"
    HIGH = "🟠 HIGH"
    VERY_HIGH = "🔴 VERY HIGH"
    CRITICAL = "🔴 CRITICAL"


@dataclass
class EnhancedTokenMetadata:
    """Comprehensive token metadata"""
    # Basic info
    address: str
    symbol: str
    name: str
    chain: str
    
    # Price data
    price: float
    price_change_1h: Optional[float] = None
    price_change_24h: Optional[float] = None
    price_change_7d: Optional[float] = None
    
    # Market data
    market_cap: Optional[float] = None
    fdv: Optional[float] = None
    liquidity: Optional[float] = None
    volume_24h: Optional[float] = None
    volume_7d: Optional[float] = None
    
    # Holder data
    holder_count: Optional[int] = None
    top_10_holder_percent: Optional[float] = None
    
    # Trading metrics
    trades_24h: Optional[int] = None
    buyers_24h: Optional[int] = None
    sellers_24h: Optional[int] = None
    buy_sell_ratio: Optional[float] = None
    
    # Security & Risk
    is_verified: bool = False
    is_audited: bool = False
    risk_level: RiskLevel = RiskLevel.MEDIUM
    risk_score: Optional[float] = None
    honeypot_risk: bool = False
    
    # Social & Links
    logo_url: Optional[str] = None
    website: Optional[str] = None
    twitter: Optional[str] = None
    telegram: Optional[str] = None
    
    # Metadata
    source: str = "Unknown"
    fetched_at: datetime = None
    
    def __post_init__(self):
        if self.fetched_at is None:
            self.fetched_at = datetime.now()


class EnhancedTokenDataFetcher:
    """Advanced token data fetcher with multiple premium sources"""
    
    # API Endpoints
    BIRDEYE_BASE = "https://public-api.birdeye.so"
    BITQUERY_BASE = "https://graphql.bitquery.io"
    DEXSCREENER_BASE = "https://api.dexscreener.com/latest"
    
    # Timeouts
    TIMEOUT_QUICK = 5.0
    TIMEOUT_STANDARD = 10.0
    TIMEOUT_EXTENDED = 15.0
    
    def __init__(
        self, 
        birdeye_key: Optional[str] = None,
        bitquery_key: Optional[str] = None,
        session_factory=None
    ):
        self.birdeye_key = birdeye_key or BIRDEYE_API_KEY
        self.bitquery_key = bitquery_key or BITQUERY_API_KEY
        self.session_factory = session_factory
        self._cache = {}
        self._cache_ttl = 300  # 5 minutes
    

    CHAIN_CONFIG = {
            'solana': {
                'birdeye_supported': True,
                'bitquery_network': 'solana',
                'dexscreener_chain': 'solana',
                'coingecko_platform': 'solana'
            },
            'ethereum': {
                'birdeye_supported': True,
                'bitquery_network': 'ethereum',
                'dexscreener_chain': 'ethereum',
                'coingecko_platform': 'ethereum'
            },
            'bsc': {
                'birdeye_supported': True,
                'bitquery_network': 'bsc',
                'dexscreener_chain': 'bsc',
                'coingecko_platform': 'binance-smart-chain'
            },
            'polygon': {
                'birdeye_supported': True,
                'bitquery_network': 'matic',
                'dexscreener_chain': 'polygon',
                'coingecko_platform': 'polygon-pos'
            },
            'arbitrum': {
                'birdeye_supported': False,
                'bitquery_network': 'arbitrum',
                'dexscreener_chain': 'arbitrum',
                'coingecko_platform': 'arbitrum-one'
            },
            'optimism': {
                'birdeye_supported': False,
                'bitquery_network': 'optimism',
                'dexscreener_chain': 'optimism',
                'coingecko_platform': 'optimistic-ethereum'
            },
            'base': {
                'birdeye_supported': False,
                'bitquery_network': 'base',
                'dexscreener_chain': 'base',
                'coingecko_platform': 'base'
            },
            'avalanche': {
                'birdeye_supported': False,
                'bitquery_network': 'avalanche',
                'dexscreener_chain': 'avalanche',
                'coingecko_platform': 'avalanche'
            }
        }
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get session from factory or create new one"""
        if self.session_factory:
            return await self.session_factory.get_session('api')
        
        # Fallback: create standalone session
        if not hasattr(self, '_session') or self._session.closed:
            connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session
    
    def _get_cache_key(self, address: str, chain: str) -> str:
        """Generate cache key"""
        return f"{chain}:{address}"
    
    def _get_cached(self, address: str, chain: str) -> Optional[EnhancedTokenMetadata]:
        """Get from cache if fresh"""
        key = self._get_cache_key(address, chain)
        if key in self._cache:
            data, timestamp = self._cache[key]
            if (datetime.now() - timestamp).total_seconds() < self._cache_ttl:
                return data
        return None
    
    def _set_cache(self, address: str, chain: str, data: EnhancedTokenMetadata):
        """Store in cache"""
        key = self._get_cache_key(address, chain)
        self._cache[key] = (data, datetime.now())
    
    # ========================================================================
    # MAIN FETCH METHOD
    # ========================================================================
    
    async def fetch_enhanced_metadata(
        self, 
        address: str, 
        chain: str = 'solana'
    ) -> Optional[EnhancedTokenMetadata]:
        """
        ⭐ UPDATED: Fetch comprehensive token metadata from all sources
        Now supports ALL EVM chains + Solana
        
        Args:
            address: Token contract address
            chain: Blockchain (solana, ethereum, bsc, polygon, arbitrum, optimism, base, avalanche)
        
        Returns:
            EnhancedTokenMetadata with all available data
        """
        # Normalize chain name
        chain = chain.lower().strip()
        
        # Check if chain is supported
        if chain not in self.CHAIN_CONFIG:
            logger.warning(f"⚠️ Chain '{chain}' not in config, treating as generic EVM")
            chain_config = {
                'birdeye_supported': False,
                'bitquery_network': chain,
                'dexscreener_chain': chain,
                'coingecko_platform': chain
            }
        else:
            chain_config = self.CHAIN_CONFIG[chain]
        
        # Check cache first
        cached = self._get_cached(address, chain)
        if cached:
            logger.info(f"✅ Cache hit for {address[:8]} on {chain}")
            return cached
        
        logger.info(f"🔍 Fetching enhanced data for {address[:8]} on {chain}")
        
        # Fetch from multiple sources in parallel
        tasks = []
        
        # 1. Birdeye (if supported for this chain)
        if chain_config['birdeye_supported'] and self.birdeye_key:
            if chain == 'solana':
                tasks.append(self._fetch_birdeye_complete(address))
            else:
                # Birdeye EVM support
                logger.info(f"📡 Fetching Birdeye for {chain}")
                tasks.append(self._fetch_birdeye_evm(address, chain))
        
        # 2. DexScreener (all chains)
        tasks.append(self._fetch_dexscreener_enhanced(address, chain_config['dexscreener_chain']))
        
        # 3. Bitquery (if API key available)
        if self.bitquery_key:
            tasks.append(self._fetch_bitquery_data(address, chain_config['bitquery_network']))
        
        # 4. CoinGecko (as fallback)
        tasks.append(self._fetch_coingecko_data(address, chain_config['coingecko_platform']))
        
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Merge all data sources
            merged = self._merge_metadata(results, address, chain)
            
            if merged:
                # Calculate risk score
                merged.risk_level, merged.risk_score = self._calculate_risk(merged)
                
                # Cache result
                self._set_cache(address, chain, merged)
                
                logger.info(f"✅ Enhanced metadata fetched: {merged.symbol} on {chain}")
                return merged
            
            logger.warning(f"⚠️ No enhanced data available for {address[:8]} on {chain}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Enhanced fetch error: {e}")
            return None


    # ============================================================================
    # NEW: Birdeye EVM Support
    # ============================================================================

    async def _fetch_birdeye_evm(self, address: str, chain: str) -> Optional[Dict]:
        """
        ✅ FIXED: Fetch Birdeye data for EVM chains
        Complete implementation with all 3 endpoints
        """
        try:
            session = await self._get_session()
            
            birdeye_chains = {
                'ethereum': 'ethereum',
                'bsc': 'bsc',
                'polygon': 'polygon'
            }
            
            birdeye_chain = birdeye_chains.get(chain)
            if not birdeye_chain:
                logger.debug(f"Birdeye doesn't support chain: {chain}")
                return None
            
            headers = {
                'X-API-KEY': self.birdeye_key,
                'x-chain': birdeye_chain
            }
            
            # ⭐ FIX: All 3 endpoints
            tasks = [
                self._birdeye_request(session, '/defi/token_overview', {'address': address}, headers),
                self._birdeye_request(session, '/defi/token_security', {'address': address}, headers),
                self._birdeye_request(session, '/defi/v3/token/holder', {'address': address}, headers),
            ]
            
            overview, security, holders = await asyncio.gather(*tasks, return_exceptions=True)
            
            result = {}
            
            # Parse overview
            if isinstance(overview, dict) and overview.get('success'):
                data = overview.get('data', {})
                result.update({
                    'symbol': data.get('symbol'),
                    'name': data.get('name'),
                    'price': data.get('price'),
                    'price_change_1h': data.get('priceChange1h'),
                    'price_change_24h': data.get('priceChange24h'),
                    'price_change_7d': data.get('priceChange7d'),
                    'volume_24h': data.get('volume24h'),
                    'liquidity': data.get('liquidity'),
                    'market_cap': data.get('mc'),
                    'fdv': data.get('fdv'),
                    'logo_url': data.get('logoURI'),
                    'source': f'Birdeye ({chain})'
                })
            
            # Parse security
            if isinstance(security, dict) and security.get('success'):
                data = security.get('data', {})
                result.update({
                    'is_verified': data.get('isVerified', False),
                    'is_audited': data.get('isAudited', False),
                    'honeypot_risk': data.get('isHoneypot', False),
                    'top_10_holder_percent': data.get('top10HolderPercent'),
                })
            
            # Parse holders
            if isinstance(holders, dict) and holders.get('success'):
                data = holders.get('data', {})
                holder_count = data.get('totalHolders', 0)
                if holder_count > 0:
                    result['holder_count'] = holder_count
            
            return result if result else None
            
        except Exception as e:
            logger.error(f"Birdeye EVM error for {chain}: {e}")
            return None
    # ============================================================================
    # NEW: CoinGecko Data Fetcher
    # ============================================================================

    async def _fetch_coingecko_data(self, address: str, platform: str) -> Optional[Dict]:
        """
        Fetch token data from CoinGecko
        Free tier: 10-30 calls/minute
        """
        try:
            session = await self._get_session()
            
            # CoinGecko API endpoint
            url = f"https://api.coingecko.com/api/v3/coins/{platform}/contract/{address}"
            
            headers = {}
            if hasattr(self, 'coingecko_key') and self.coingecko_key:
                headers['x-cg-pro-api-key'] = self.coingecko_key
            
            timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_STANDARD)
            
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                market_data = data.get('market_data', {})
                
                result = {
                    'symbol': data.get('symbol', '').upper(),
                    'name': data.get('name'),
                    'logo_url': data.get('image', {}).get('large'),
                    'price': market_data.get('current_price', {}).get('usd'),
                    'price_change_24h': market_data.get('price_change_percentage_24h'),
                    'price_change_7d': market_data.get('price_change_percentage_7d'),
                    'market_cap': market_data.get('market_cap', {}).get('usd'),
                    'volume_24h': market_data.get('total_volume', {}).get('usd'),
                    'liquidity': market_data.get('total_value_locked', {}).get('usd'),
                    'website': data.get('links', {}).get('homepage', [None])[0],
                    'twitter': data.get('links', {}).get('twitter_screen_name'),
                    'source': 'CoinGecko'
                }
                
                # Add Twitter link if available
                if result['twitter']:
                    result['twitter'] = f"https://twitter.com/{result['twitter']}"
                
                return result
            
        except Exception as e:
            logger.debug(f"CoinGecko error: {e}")
            return None


    # ========================================================================
    # BIRDEYE API (Most comprehensive for Solana)
    # ========================================================================
    
    async def _fetch_birdeye_complete(self, address: str) -> Optional[Dict]:
        """
        Fetch complete Birdeye data (price, security, trader stats)
        
        Endpoints:
        - /defi/token_overview
        - /defi/token_security
        - /defi/token_trending
        - /trader/gainers_losers
        """
        try:
            session = await self._get_session()
            
            headers = {
                'X-API-KEY': self.birdeye_key,
                'x-chain': 'solana'
            }
            
            # Parallel fetch multiple endpoints
            tasks = [
                self._birdeye_request(session, '/defi/token_overview', {'address': address}, headers),
                self._birdeye_request(session, '/defi/token_security', {'address': address}, headers),
                self._birdeye_request(session, '/defi/v3/token/holder', {'address': address}, headers),
            ]
            
            overview, security, holders = await asyncio.gather(*tasks, return_exceptions=True)
            
            result = {}
            
            # Parse overview
            if isinstance(overview, dict) and overview.get('success'):
                data = overview.get('data', {})
                result.update({
                    'symbol': data.get('symbol'),
                    'name': data.get('name'),
                    'price': data.get('price'),
                    'price_change_24h': data.get('priceChange24h'),
                    'volume_24h': data.get('volume24h'),
                    'liquidity': data.get('liquidity'),
                    'market_cap': data.get('mc'),
                    'logo_url': data.get('logoURI'),
                    'source': 'Birdeye'
                })
            
            # Parse security
            if isinstance(security, dict) and security.get('success'):
                data = security.get('data', {})
                result.update({
                    'is_verified': data.get('isVerified', False),
                    'honeypot_risk': data.get('isHoneypot', False),
                    'top_10_holder_percent': data.get('top10HolderPercent'),
                })
            
            # Parse holders
            if isinstance(holders, dict) and holders.get('success'):
                data = holders.get('data', {})
                result['holder_count'] = data.get('totalHolders', 0)
            
            return result if result else None
            
        except Exception as e:
            logger.debug(f"Birdeye error: {e}")
            return None
    
    async def _birdeye_request(
        self, 
        session: aiohttp.ClientSession, 
        endpoint: str, 
        params: Dict,
        headers: Dict
    ) -> Optional[Dict]:
        """Make Birdeye API request"""
        try:
            url = f"{self.BIRDEYE_BASE}{endpoint}"
            timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_QUICK)
            
            async with session.get(url, params=params, headers=headers, timeout=timeout) as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception:
            return None
    
    # ========================================================================
    # DEXSCREENER ENHANCED (Audit info, social links)
    # ========================================================================
    
    async def _fetch_dexscreener_enhanced(self, address: str, chain: str) -> Optional[Dict]:
        """
        ⭐ UPDATED: Fetch enhanced DexScreener data with chain-specific handling
        """
        try:
            session = await self._get_session()
            
            url = f"{self.DEXSCREENER_BASE}/dex/tokens/{address}"
            timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_QUICK)
            
            async with session.get(url, timeout=timeout) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                pairs = data.get('pairs', [])
                
                if not pairs:
                    return None
                
                # Filter by chain if multiple pairs exist
                chain_pairs = [p for p in pairs if p.get('chainId', '').lower() == chain.lower()]
                
                # Use chain-specific pairs if available, otherwise use all pairs
                relevant_pairs = chain_pairs if chain_pairs else pairs
                
                # Get most liquid pair
                pair = max(relevant_pairs, key=lambda p: float(p.get('liquidity', {}).get('usd', 0) or 0))
                
                base = pair.get('baseToken', {})
                info = pair.get('info', {})
                
                result = {
                    'symbol': base.get('symbol'),
                    'name': base.get('name'),
                    'price': float(pair.get('priceUsd', 0) or 0),
                    'price_change_1h': float(pair.get('priceChange', {}).get('h1', 0) or 0),
                    'price_change_24h': float(pair.get('priceChange', {}).get('h24', 0) or 0),
                    'price_change_7d': float(pair.get('priceChange', {}).get('h7d', 0) or 0),
                    'volume_24h': float(pair.get('volume', {}).get('h24', 0) or 0),
                    'liquidity': float(pair.get('liquidity', {}).get('usd', 0) or 0),
                    'fdv': float(pair.get('fdv', 0) or 0),
                    'trades_24h': pair.get('txns', {}).get('h24', {}).get('buys', 0) + pair.get('txns', {}).get('h24', {}).get('sells', 0),
                    'logo_url': info.get('imageUrl'),
                    'website': info.get('websites', [{}])[0].get('url') if info.get('websites') else None,
                    'twitter': next((s.get('url') for s in info.get('socials', []) if s.get('type') == 'twitter'), None),
                    'telegram': next((s.get('url') for s in info.get('socials', []) if s.get('type') == 'telegram'), None),
                    'source': f'DexScreener ({chain})'
                }
                
                return result
            
        except Exception as e:
            logger.debug(f"DexScreener enhanced error: {e}")
            return None

    # ========================================================================
    # BITQUERY (Real-time trades, DEX analytics)
    # ========================================================================
    
    async def _fetch_bitquery_data(self, address: str, chain: str) -> Optional[Dict]:
        """
        Fetch Bitquery GraphQL data
        Provides: real-time trades, DEX analytics, smart money tracking
        """
        if not self.bitquery_key:
            return None
        
        try:
            session = await self._get_session()
            
            # Map chain names
            chain_map = {
                'solana': 'solana',
                'ethereum': 'ethereum',
                'bsc': 'bsc',
                'polygon': 'matic'
            }
            
            network = chain_map.get(chain.lower(), 'solana')
            
            # GraphQL query for token analytics
            query = """
            query TokenAnalytics($address: String!, $network: String!) {
              ethereum(network: $network) {
                dexTrades(
                  baseCurrency: {is: $address}
                  options: {limit: 100, desc: "block.timestamp.time"}
                ) {
                  count
                  buyers: count(uniq: buyers)
                  sellers: count(uniq: sellers)
                  volume: tradeAmount(in: USD)
                  trades {
                    block {
                      timestamp {
                        time
                      }
                    }
                    buyer {
                      address
                    }
                    seller {
                      address
                    }
                    transaction {
                      hash
                    }
                  }
                }
              }
            }
            """
            
            headers = {
                'Content-Type': 'application/json',
                'X-API-KEY': self.bitquery_key
            }
            
            payload = {
                'query': query,
                'variables': {
                    'address': address,
                    'network': network
                }
            }
            
            timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_STANDARD)
            
            async with session.post(
                self.BITQUERY_BASE, 
                json=payload, 
                headers=headers, 
                timeout=timeout
            ) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                if 'errors' in data:
                    logger.debug(f"Bitquery error: {data['errors']}")
                    return None
                
                trades_data = data.get('data', {}).get('ethereum', {}).get('dexTrades', [])
                
                if not trades_data:
                    return None
                
                result = {
                    'trades_24h': trades_data[0].get('count', 0),
                    'buyers_24h': trades_data[0].get('buyers', 0),
                    'sellers_24h': trades_data[0].get('sellers', 0),
                    'volume_24h': trades_data[0].get('volume', 0),
                    'source': 'Bitquery'
                }
                
                # Calculate buy/sell ratio
                if result['sellers_24h'] > 0:
                    result['buy_sell_ratio'] = result['buyers_24h'] / result['sellers_24h']
                
                return result
            
        except Exception as e:
            logger.debug(f"Bitquery error: {e}")
            return None
    
    # ========================================================================
    # DATA MERGING & RISK ANALYSIS
    # ========================================================================
    
    def _merge_metadata(
        self, 
        results: List, 
        address: str, 
        chain: str
    ) -> Optional[EnhancedTokenMetadata]:
        """Merge data from multiple sources into single metadata object"""
        
        merged = {}
        
        for result in results:
            if isinstance(result, dict) and result:
                # Merge with priority to first non-null value
                for key, value in result.items():
                    if value is not None and key not in merged:
                        merged[key] = value
        
        if not merged:
            return None
        
        # Ensure required fields
        if not merged.get('symbol'):
            merged['symbol'] = f"{address[:4]}...{address[-4:]}"
        if not merged.get('name'):
            merged['name'] = merged['symbol']
        
        try:
            return EnhancedTokenMetadata(
                address=address,
                chain=chain,
                symbol=merged.get('symbol', 'UNKNOWN'),
                name=merged.get('name', ''),
                price=merged.get('price', 0.0),
                price_change_1h=merged.get('price_change_1h'),
                price_change_24h=merged.get('price_change_24h'),
                price_change_7d=merged.get('price_change_7d'),
                market_cap=merged.get('market_cap'),
                fdv=merged.get('fdv'),
                liquidity=merged.get('liquidity'),
                volume_24h=merged.get('volume_24h'),
                volume_7d=merged.get('volume_7d'),
                holder_count=merged.get('holder_count'),
                top_10_holder_percent=merged.get('top_10_holder_percent'),
                trades_24h=merged.get('trades_24h'),
                buyers_24h=merged.get('buyers_24h'),
                sellers_24h=merged.get('sellers_24h'),
                buy_sell_ratio=merged.get('buy_sell_ratio'),
                is_verified=merged.get('is_verified', False),
                is_audited=merged.get('is_audited', False),
                honeypot_risk=merged.get('honeypot_risk', False),
                logo_url=merged.get('logo_url'),
                website=merged.get('website'),
                twitter=merged.get('twitter'),
                telegram=merged.get('telegram'),
                source=merged.get('source', 'Multiple')
            )
        except Exception as e:
            logger.error(f"Merge error: {e}")
            return None
    
    def _calculate_risk(self, metadata: EnhancedTokenMetadata) -> Tuple[RiskLevel, float]:
        """
        Calculate comprehensive risk score
        
        Risk factors:
        - Liquidity (low = high risk)
        - Holder concentration (high top 10% = high risk)
        - Trading volume (low = high risk)
        - Honeypot detection
        - Verification status
        - Buy/sell ratio (extreme = high risk)
        
        Returns:
            (RiskLevel, risk_score_0_to_100)
        """
        risk_score = 0.0
        max_score = 100.0
        
        # Liquidity check (30 points)
        if metadata.liquidity:
            if metadata.liquidity < 1000:
                risk_score += 30
            elif metadata.liquidity < 10000:
                risk_score += 20
            elif metadata.liquidity < 50000:
                risk_score += 10
            elif metadata.liquidity < 100000:
                risk_score += 5
        else:
            risk_score += 15  # Unknown liquidity = medium risk
        
        # Holder concentration (25 points)
        if metadata.top_10_holder_percent:
            if metadata.top_10_holder_percent > 80:
                risk_score += 25
            elif metadata.top_10_holder_percent > 60:
                risk_score += 18
            elif metadata.top_10_holder_percent > 40:
                risk_score += 10
            elif metadata.top_10_holder_percent > 20:
                risk_score += 5
        
        # Trading volume (20 points)
        if metadata.volume_24h:
            if metadata.volume_24h < 1000:
                risk_score += 20
            elif metadata.volume_24h < 10000:
                risk_score += 12
            elif metadata.volume_24h < 50000:
                risk_score += 5
        else:
            risk_score += 10
        
        # Honeypot detection (15 points)
        if metadata.honeypot_risk:
            risk_score += 15
        
        # Verification (10 points)
        if not metadata.is_verified:
            risk_score += 10
        
        # Buy/sell ratio (10 points)
        if metadata.buy_sell_ratio:
            if metadata.buy_sell_ratio > 3 or metadata.buy_sell_ratio < 0.33:
                risk_score += 10  # Extreme ratios indicate manipulation
            elif metadata.buy_sell_ratio > 2 or metadata.buy_sell_ratio < 0.5:
                risk_score += 5
        
        # Determine risk level
        if risk_score >= 80:
            level = RiskLevel.CRITICAL
        elif risk_score >= 65:
            level = RiskLevel.VERY_HIGH
        elif risk_score >= 50:
            level = RiskLevel.HIGH
        elif risk_score >= 35:
            level = RiskLevel.MEDIUM
        elif risk_score >= 20:
            level = RiskLevel.LOW
        else:
            level = RiskLevel.VERY_LOW
        
        return level, risk_score
    
    async def close(self):
        """Cleanup"""
        if hasattr(self, '_session') and not self._session.closed:
            await self._session.close()


# ============================================================================
# FORMATTING UTILITIES
# ============================================================================

def format_enhanced_token_display(metadata: EnhancedTokenMetadata) -> str:
    """
    Format enhanced metadata for beautiful Telegram display
    """
    addr_short = f"{metadata.address[:8]}...{metadata.address[-6:]}"
    
    lines = [
        "🪙 <b>Enhanced Token Analysis</b>\n",
        f"<b>📛 {metadata.name}</b>",
        f"<b>🏷 Symbol:</b> {metadata.symbol}",
        f"<b>⛓ Chain:</b> {metadata.chain.upper()}",
        f"<b>📍 Address:</b> <code>{addr_short}</code>",
    ]
    
    # Verification badges
    badges = []
    if metadata.is_verified:
        badges.append("✅ Verified")
    if metadata.is_audited:
        badges.append("🔒 Audited")
    if metadata.honeypot_risk:
        badges.append("⚠️ Honeypot Risk")
    
    if badges:
        lines.append(f"\n<b>🏆 Status:</b> {' • '.join(badges)}")
    
    # Price section
    lines.append("\n<b>💰 Price Data:</b>")
    
    if metadata.price > 0:
        if metadata.price >= 1:
            price_str = f"${metadata.price:,.4f}"
        elif metadata.price >= 0.01:
            price_str = f"${metadata.price:.6f}"
        else:
            price_str = f"${metadata.price:.10f}"
        lines.append(f"• Current: {price_str}")
    
    if metadata.price_change_1h is not None:
        emoji = "📈" if metadata.price_change_1h >= 0 else "📉"
        sign = "+" if metadata.price_change_1h >= 0 else ""
        lines.append(f"• 1h: {emoji} {sign}{metadata.price_change_1h:.2f}%")
    
    if metadata.price_change_24h is not None:
        emoji = "📈" if metadata.price_change_24h >= 0 else "📉"
        sign = "+" if metadata.price_change_24h >= 0 else ""
        lines.append(f"• 24h: {emoji} {sign}{metadata.price_change_24h:.2f}%")
    
    if metadata.price_change_7d is not None:
        emoji = "📈" if metadata.price_change_7d >= 0 else "📉"
        sign = "+" if metadata.price_change_7d >= 0 else ""
        lines.append(f"• 7d: {emoji} {sign}{metadata.price_change_7d:.2f}%")
    
    # Market metrics
    lines.append("\n<b>📊 Market Metrics:</b>")
    
    if metadata.market_cap:
        mc = metadata.market_cap
        if mc >= 1_000_000_000:
            lines.append(f"• Market Cap: ${mc/1_000_000_000:.2f}B")
        elif mc >= 1_000_000:
            lines.append(f"• Market Cap: ${mc/1_000_000:.2f}M")
        else:
            lines.append(f"• Market Cap: ${mc:,.0f}")
    
    if metadata.liquidity:
        liq = metadata.liquidity
        if liq >= 1_000_000:
            lines.append(f"• Liquidity: ${liq/1_000_000:.2f}M")
        else:
            lines.append(f"• Liquidity: ${liq:,.0f}")
    
    if metadata.volume_24h:
        vol = metadata.volume_24h
        if vol >= 1_000_000:
            lines.append(f"• 24h Volume: ${vol/1_000_000:.2f}M")
        else:
            lines.append(f"• 24h Volume: ${vol:,.0f}")
    
    # Trading activity
    if metadata.trades_24h or metadata.buyers_24h:
        lines.append("\n<b>📈 Trading Activity (24h):</b>")
        
        if metadata.trades_24h:
            lines.append(f"• Trades: {metadata.trades_24h:,}")
        
        if metadata.buyers_24h and metadata.sellers_24h:
            lines.append(f"• Buyers: {metadata.buyers_24h:,}")
            lines.append(f"• Sellers: {metadata.sellers_24h:,}")
        
        if metadata.buy_sell_ratio:
            ratio_emoji = "🟢" if 0.8 <= metadata.buy_sell_ratio <= 1.2 else "🟡"
            if metadata.buy_sell_ratio > 2 or metadata.buy_sell_ratio < 0.5:
                ratio_emoji = "🔴"
            lines.append(f"• B/S Ratio: {ratio_emoji} {metadata.buy_sell_ratio:.2f}")
    
    # Holder distribution
    if metadata.holder_count or metadata.top_10_holder_percent:
        lines.append("\n<b>👥 Holder Distribution:</b>")
        
        if metadata.holder_count:
            lines.append(f"• Total Holders: {metadata.holder_count:,}")
        
        if metadata.top_10_holder_percent:
            concentration = metadata.top_10_holder_percent
            if concentration > 60:
                emoji = "🔴"
                warning = " (High concentration)"
            elif concentration > 40:
                emoji = "🟡"
                warning = ""
            else:
                emoji = "🟢"
                warning = ""
            lines.append(f"• Top 10 Hold: {emoji} {concentration:.1f}%{warning}")
    
    # Risk assessment
    lines.append(f"\n<b>⚠️ Risk Assessment:</b>")
    lines.append(f"• Level: {metadata.risk_level.value}")
    if metadata.risk_score:
        lines.append(f"• Score: {metadata.risk_score:.0f}/100")
    
    # Social links
    social_links = []
    if metadata.website:
        social_links.append(f"<a href='{metadata.website}'>Website</a>")
    if metadata.twitter:
        social_links.append(f"<a href='{metadata.twitter}'>Twitter</a>")
    if metadata.telegram:
        social_links.append(f"<a href='{metadata.telegram}'>Telegram</a>")
    
    if social_links:
        lines.append(f"\n<b>🔗 Links:</b> {' • '.join(social_links)}")
    
    # Footer
    lines.append(f"\n<i>📡 Data: {metadata.source} • {metadata.fetched_at.strftime('%H:%M:%S')}</i>")
    
    return '\n'.join(lines)
