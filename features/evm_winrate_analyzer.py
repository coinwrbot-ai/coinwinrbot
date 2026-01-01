"""
✅ FIXED: EVM Win Rate Analyzer - GMGN.ai Methodology
Uses 7-day transaction window to calculate win rate like GMGN.ai
"""

import asyncio
import aiohttp
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class TokenPosition:
    """Track token position with buys/sells"""
    address: str
    symbol: str
    name: str
    decimals: int
    
    buys: List[Dict]
    sells: List[Dict]
    
    total_bought: float = 0
    total_sold: float = 0
    current_balance: float = 0
    
    total_cost_usd: float = 0
    avg_buy_price: float = 0
    
    realized_pnl: float = 0
    unrealized_pnl: float = 0
    total_pnl: float = 0
    
    current_price: float = 0
    current_value: float = 0
    first_trade: Optional[str] = None
    last_trade: Optional[str] = None
    is_winner: bool = False


class EVMWinRateAnalyzer:
    """
    ✅ GMGN.ai Win Rate Methodology:
    
    Win Rate Calculation:
    1. Look at all tokens with ANY transaction in last 7 days
    2. For each token: Check if total P&L is positive
    3. Win rate = (profitable tokens / active tokens) * 100
    
    Example:
    - 10 tokens traded in last 7 days
    - 6 have positive P&L → Winners
    - 4 have negative P&L → Losers
    - Win rate = 60%
    """
    
    ALCHEMY_ENDPOINTS = {
        'ethereum': 'https://eth-mainnet.g.alchemy.com/v2',
        'polygon': 'https://polygon-mainnet.g.alchemy.com/v2',
        'arbitrum': 'https://arb-mainnet.g.alchemy.com/v2',
        'optimism': 'https://opt-mainnet.g.alchemy.com/v2',
        'base': 'https://base-mainnet.g.alchemy.com/v2',
        'bsc': 'https://bsc-dataseed1.binance.org',
    }
    
    NATIVE_TOKENS = {
        'ethereum': {'symbol': 'ETH', 'decimals': 18},
        'polygon': {'symbol': 'MATIC', 'decimals': 18},
        'arbitrum': {'symbol': 'ETH', 'decimals': 18},
        'optimism': {'symbol': 'ETH', 'decimals': 18},
        'base': {'symbol': 'ETH', 'decimals': 18},
        'bsc': {'symbol': 'BNB', 'decimals': 18},
    }
    
    def __init__(self, alchemy_api_key: str, session_factory=None):
        self.api_key = alchemy_api_key
        self.session_factory = session_factory
        self.price_cache = {}
    
    
    async def analyze_wallet(self, address: str, chain: str = 'ethereum', days: int = 7) -> Dict:
        """
        ✅ ENHANCED: Calculate win rate with configurable timeframe
        
        Args:
            address: Wallet address
            chain: Blockchain name
            days: Number of days to look back (7 or 30, default 7)
        """
        
        try:
            chain = chain.lower()
            address = address.lower()
            
            if chain not in self.ALCHEMY_ENDPOINTS:
                supported = ', '.join(self.ALCHEMY_ENDPOINTS.keys())
                return {
                    'success': False,
                    'message': f'Chain {chain} not supported. Available: {supported}',
                    'address': address,
                    'chain': chain
                }
            
            logger.info(f"🔍 Analyzing {address[:10]}... on {chain}")
            
            # Get 7-day transfers first
            # Get transfers with specified timeframe
            transfers_with_timeframe = await self._get_all_transfers(address, chain, days=days)
            
            logger.info(f"📊 Found {len(transfers_with_timeframe)} transfers in last {days} days")
            
            # If no activity in timeframe, get all-time transfers for context
            if not transfers_with_timeframe:
                logger.info(f"📊 No {days}-day activity, fetching all-time transfers for context...")
                transfers_all = await self._get_all_transfers(address, chain, days=None)
                logger.info(f"📊 Found {len(transfers_all)} total all-time transfers")
                
                if not transfers_all:
                    return self._build_empty_result(address, chain, "No trading activity found")
                
                # Process all-time data but mark as no recent activity
                positions = self._process_transfers(transfers_all, address)
                logger.info(f"💼 Tracking {len(positions)} token positions (all-time)")
                
                await self._enrich_with_prices(positions, chain)
                
                analyzed_tokens = []
                for token_addr, position in positions.items():
                    token_stats = self._calculate_token_pnl_with_winrate(position)
                    if token_stats:
                        analyzed_tokens.append(token_stats)
                
                result = self._calculate_overall_metrics(
                    analyzed_tokens, address, chain, has_recent_activity=False
                )
                result['timeframe_days'] = days
                return result
            
            # Process transfers within timeframe
            positions = self._process_transfers(transfers_with_timeframe, address)
            logger.info(f"💼 Tracking {len(positions)} token positions in last {days} days")
            # Enrich with current prices
            await self._enrich_with_prices(positions, chain)
            
            # Calculate token P&L with GMGN-style win rate
            analyzed_tokens = []
            for token_addr, position in positions.items():
                token_stats = self._calculate_token_pnl_with_winrate(position)
                if token_stats:
                    analyzed_tokens.append(token_stats)
            
            # Calculate overall metrics with GMGN win rate
            # Calculate overall metrics
            result = self._calculate_overall_metrics(
                analyzed_tokens, address, chain, has_recent_activity=True
            )
            result['timeframe_days'] = days
            return result
            
        except Exception as e:
            logger.error(f"Analysis error: {e}", exc_info=True)
            return {
                'success': False,
                'message': str(e),
                'address': address,
                'chain': chain
            }
    
    async def _get_all_transfers(self, address: str, chain: str, days: Optional[int] = 7) -> List[Dict]:
        """
        Get transfers for the address
        
        Args:
            address: Wallet address
            chain: Blockchain name
            days: Number of days to look back (None = all time)
        """
        
        try:
            session = await self._get_session()
            
            if chain == 'bsc':
                return await self._get_bsc_transfers(address, session, days)
            else:
                return await self._get_alchemy_transfers(address, chain, session, days)
            
        except Exception as e:
            logger.error(f"Transfer fetch error: {e}")
            return []
    
    async def _get_alchemy_transfers(
        self, 
        address: str, 
        chain: str, 
        session: aiohttp.ClientSession,
        days: Optional[int] = 7
    ) -> List[Dict]:
        """
        Get transfers using Alchemy API
        
        Args:
            days: Number of days to look back (None = all time)
        """
        
        base_url = self.ALCHEMY_ENDPOINTS[chain]
        url = f"{base_url}/{self.api_key}"
        
        all_transfers = []
        
        # Build params based on days filter
        if days is not None:
            cutoff_date = datetime.now() - timedelta(days=days)
            cutoff_hex = hex(int(cutoff_date.timestamp()))
            date_str = cutoff_date.strftime('%Y-%m-%d')
            logger.info(f"📥 Fetching incoming transfers (last {days} days from {date_str})...")
            from_block = cutoff_hex
        else:
            logger.info(f"📥 Fetching all incoming transfers (all-time)...")
            from_block = "0x0"
        
        # Incoming transfers
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": [{
                "toAddress": address,
                "category": ["erc20"],
                "withMetadata": True,
                "maxCount": "0x3e8",  # 1000 transfers
                "fromBlock": from_block,
                "toBlock": "latest"
            }]
        }
        
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status == 200:
                data = await resp.json()
                result = data.get('result', {})
                transfers = result.get('transfers', [])
                
                for transfer in transfers:
                    all_transfers.append({
                        **transfer,
                        'direction': 'in',
                        'is_buy': True
                    })
                
                logger.info(f"   Found {len(transfers)} incoming transfers")
        
        # Outgoing transfers
        if days is not None:
            logger.info(f"📤 Fetching outgoing transfers (last {days} days from {date_str})...")
        else:
            logger.info(f"📤 Fetching all outgoing transfers (all-time)...")
            
        payload['params'][0] = {
            "fromAddress": address,
            "category": ["erc20"],
            "withMetadata": True,
            "maxCount": "0x3e8",  # 1000 transfers
            "fromBlock": from_block,
            "toBlock": "latest"
        }
        
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status == 200:
                data = await resp.json()
                result = data.get('result', {})
                transfers = result.get('transfers', [])
                
                for transfer in transfers:
                    all_transfers.append({
                        **transfer,
                        'direction': 'out',
                        'is_buy': False
                    })
                
                logger.info(f"   Found {len(transfers)} outgoing transfers")
        
        all_transfers.sort(key=lambda x: x.get('metadata', {}).get('blockTimestamp', ''))
        
        if days is not None:
            logger.info(f"✅ Retrieved {len(all_transfers)} total transfers in {days}-day window")
        else:
            logger.info(f"✅ Retrieved {len(all_transfers)} total all-time transfers")
        
        return all_transfers
    
    async def _get_bsc_transfers(
        self, 
        address: str, 
        session: aiohttp.ClientSession,
        days: Optional[int] = 7
    ) -> List[Dict]:
        """
        Get BSC transfers using BscScan API
        
        Args:
            days: Number of days to look back (None = all time)
        """
        
        logger.info("🔍 Using BscScan for BSC transfers...")
        
        all_transfers = []
        
        if days is not None:
            cutoff_date = datetime.now() - timedelta(days=days)
            cutoff_timestamp = int(cutoff_date.timestamp())
            logger.info(f"📅 Filtering BSC transfers from {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            cutoff_timestamp = 0
            logger.info(f"📅 Fetching all BSC transfers (all-time)")
        
        url = "https://api.bscscan.com/api"
        
        params = {
            'module': 'account',
            'action': 'tokentx',
            'address': address,
            'startblock': 0,
            'endblock': 99999999,
            'sort': 'asc'
        }
        
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    if data.get('status') == '1':
                        results = data.get('result', [])
                        
                        # Filter by date if needed
                        filtered_count = 0
                        for tx in results:
                            tx_timestamp = int(tx['timeStamp'])
                            
                            # Only include transfers after cutoff
                            if tx_timestamp < cutoff_timestamp:
                                continue
                            
                            filtered_count += 1
                            is_incoming = tx['to'].lower() == address.lower()
                            
                            all_transfers.append({
                                'rawContract': {
                                    'address': tx['contractAddress'],
                                    'decimal': int(tx.get('tokenDecimal', 18))
                                },
                                'asset': tx.get('tokenSymbol', 'UNKNOWN'),
                                'value': float(tx['value']) / (10 ** int(tx.get('tokenDecimal', 18))),
                                'metadata': {
                                    'blockTimestamp': datetime.fromtimestamp(tx_timestamp).isoformat()
                                },
                                'direction': 'in' if is_incoming else 'out',
                                'is_buy': is_incoming
                            })
                        
                        if days is not None:
                            logger.info(f"✅ Found {filtered_count} BSC transfers in last {days} days (total: {len(results)})")
                        else:
                            logger.info(f"✅ Found {len(results)} BSC transfers (all-time)")
                    else:
                        logger.warning(f"⚠️ BscScan: {data.get('message')}")
        
        except Exception as e:
            logger.error(f"BSC transfer error: {e}")
        
        return all_transfers
    
    def _process_transfers(self, transfers: List[Dict], wallet: str) -> Dict[str, TokenPosition]:
        """Process transfers into token positions"""
        
        positions = {}
        wallet = wallet.lower()
        
        for transfer in transfers:
            try:
                token_addr = transfer.get('rawContract', {}).get('address', '').lower()
                if not token_addr:
                    continue
                
                amount = float(transfer.get('value', 0))
                if amount == 0:
                    continue
                
                is_buy = transfer.get('is_buy', False)
                timestamp = transfer.get('metadata', {}).get('blockTimestamp', '')
                
                if token_addr not in positions:
                    asset = transfer.get('asset', 'Unknown')
                    
                    positions[token_addr] = TokenPosition(
                        address=token_addr,
                        symbol=asset,
                        name=asset,
                        decimals=transfer.get('rawContract', {}).get('decimal', 18),
                        buys=[],
                        sells=[]
                    )
                
                position = positions[token_addr]
                
                if not position.first_trade:
                    position.first_trade = timestamp
                position.last_trade = timestamp
                
                if is_buy:
                    position.buys.append({
                        'amount': amount,
                        'cost_usd': 0,
                        'timestamp': timestamp
                    })
                    position.total_bought += amount
                else:
                    position.sells.append({
                        'amount': amount,
                        'proceeds_usd': 0,
                        'timestamp': timestamp
                    })
                    position.total_sold += amount
                
            except Exception as e:
                logger.debug(f"Transfer processing error: {e}")
                continue
        
        for position in positions.values():
            position.current_balance = position.total_bought - position.total_sold
        
        return positions
    
    async def _enrich_with_prices(self, positions: Dict[str, TokenPosition], chain: str):
        """Enrich positions with current prices"""
        
        if not positions:
            return
        
        try:
            session = await self._get_session()
            
            tasks = [
                self._get_token_price(pos.address, chain, session)
                for pos in positions.values()
            ]
            
            prices = await asyncio.gather(*tasks, return_exceptions=True)
            
            for position, price in zip(positions.values(), prices):
                if isinstance(price, (int, float)) and price > 0:
                    position.current_price = price
                    position.current_value = position.current_balance * price
                    
        except Exception as e:
            logger.error(f"Price enrichment error: {e}")
    
    async def _get_token_price(
        self,
        token_address: str,
        chain: str,
        session: aiohttp.ClientSession
    ) -> float:
        """Get token price from DexScreener"""
        
        cache_key = f"{chain}:{token_address}"
        if cache_key in self.price_cache:
            return self.price_cache[cache_key]
        
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get('pairs', [])
                    
                    if not pairs:
                        return 0
                    
                    best_pair = max(
                        pairs,
                        key=lambda p: float(p.get('liquidity', {}).get('usd', 0) or 0)
                    )
                    
                    price = float(best_pair.get('priceUsd', 0) or 0)
                    self.price_cache[cache_key] = price
                    
                    return price
            
            return 0
            
        except Exception as e:
            logger.debug(f"Price fetch error: {e}")
            return 0
    
    def _calculate_token_pnl_with_winrate(self, position: TokenPosition) -> Optional[Dict]:
        """
        ⭐ GMGN.ai: Calculate P&L with 7-day win rate flag
        
        Win rate is calculated at overall level, not per token
        Here we just flag if token had 7-day activity
        """
        
        try:
            if position.current_price == 0:
                logger.debug(f"❌ Token {position.symbol}: No price data, skipping")
                return None
            
            # Calculate P&L (existing logic)
            estimated_buy_price = position.current_price * 1.2
            total_cost = position.total_bought * estimated_buy_price
            position.total_cost_usd = total_cost
            position.avg_buy_price = estimated_buy_price
            
            estimated_sell_price = position.current_price * 0.8
            total_proceeds = position.total_sold * estimated_sell_price
            
            if position.total_sold > 0:
                sold_cost_basis = (position.total_sold / position.total_bought) * total_cost
                position.realized_pnl = total_proceeds - sold_cost_basis
            else:
                position.realized_pnl = 0
            
            if position.current_balance > 0:
                held_cost_basis = (position.current_balance / position.total_bought) * total_cost
                position.unrealized_pnl = position.current_value - held_cost_basis
            else:
                position.unrealized_pnl = 0
            
            position.total_pnl = position.realized_pnl + position.unrealized_pnl
            position.is_winner = position.total_pnl > 0
            
            # Check if token has 7-day activity
            win_rate_flag = self._calculate_7d_winrate(position)
            
            logger.info(
                f"💰 {position.symbol}: "
                f"P&L=${position.total_pnl:.2f} ({'WIN' if position.is_winner else 'LOSS'}), "
                f"Last trade: {position.last_trade}, "
                f"7d active: {win_rate_flag >= 0}"
            )
            
            return {
                'symbol': position.symbol,
                'name': position.name,
                'address': position.address,
                'mint': position.address,
                'total_bought': position.total_bought,
                'total_sold': position.total_sold,
                'current_balance': position.current_balance,
                'is_holder': position.current_balance > 0,
                'avg_buy_price': position.avg_buy_price,
                'current_price': position.current_price,
                'current_value': position.current_value,
                'total_cost': position.total_cost_usd,
                'realized_pnl': position.realized_pnl,
                'unrealized_pnl': position.unrealized_pnl,
                'total_profit': position.total_pnl,
                'pnl': position.total_pnl,
                'trades': len(position.buys) + len(position.sells),
                'buy_count': len(position.buys),
                'sell_count': len(position.sells),
                'first_trade': position.first_trade,
                'last_trade': position.last_trade,
                'is_winner': position.is_winner,
                'win_rate': win_rate_flag,  # -1 = no 7d activity, 0/100 = loser/winner
            }
            
        except Exception as e:
            logger.debug(f"PnL calculation error: {e}")
            return None
    
    def _calculate_7d_winrate(self, position: TokenPosition) -> float:
        """
        ⭐ GMGN.ai METHOD: Calculate win rate using 7-day transactions
        
        GMGN Logic:
        1. Look at ALL tokens traded in last 7 days
        2. For each token: Is final P&L positive or negative?
        3. Win rate = (tokens with profit / total tokens) * 100
        
        Per-token logic:
        - If completely sold: Use realized P&L
        - If still holding: Use total P&L (realized + unrealized)
        - If no 7-day activity: Exclude from calculation
        """
        
        try:
            cutoff_date = datetime.now() - timedelta(days=7)
            
            # Check if token has ANY activity in last 7 days
            has_recent_activity = False
            recent_trade_type = None
            
            for buy in position.buys:
                if self._is_after_date(buy['timestamp'], cutoff_date):
                    has_recent_activity = True
                    recent_trade_type = 'buy'
                    logger.debug(f"Token {position.symbol}: Found recent buy at {buy['timestamp']}")
                    break
            
            if not has_recent_activity:
                for sell in position.sells:
                    if self._is_after_date(sell['timestamp'], cutoff_date):
                        has_recent_activity = True
                        recent_trade_type = 'sell'
                        logger.debug(f"Token {position.symbol}: Found recent sell at {sell['timestamp']}")
                        break
            
            # If no 7-day activity, don't include in win rate
            if not has_recent_activity:
                logger.debug(
                    f"Token {position.symbol}: No 7-day activity "
                    f"(last trade: {position.last_trade})"
                )
                return -1.0  # Signal to exclude from calculation
            
            # Determine if this token is a winner
            # GMGN uses simple logic: Is the token profitable?
            is_profitable = position.total_pnl > 0
            
            logger.info(
                f"✅ Token {position.symbol}: "
                f"P&L=${position.total_pnl:.2f}, "
                f"{'WINNER' if is_profitable else 'LOSER'} "
                f"(recent {recent_trade_type}, balance={position.current_balance:.4f})"
            )
            
            return 100.0 if is_profitable else 0.0
            
        except Exception as e:
            logger.error(f"Win rate calculation error for {position.symbol}: {e}")
            return -1.0  # Exclude from calculation
    
    def _is_after_date(self, timestamp: Optional[str], cutoff: datetime) -> bool:
        """Check if timestamp is after cutoff date"""
        if not timestamp:
            return False
        try:
            trade_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return trade_time >= cutoff
        except:
            return False
    
    def _calculate_overall_metrics(
        self,
        tokens: List[Dict],
        address: str,
        chain: str,
        has_recent_activity: bool = True  # ⭐ CHANGED from has_7d_activity
    ) -> Dict:
        """
        ⭐ GMGN.ai METHOD: Calculate overall win rate from 7-day active tokens
        
        Win Rate Formula:
        1. Filter tokens with activity in last 7 days
        2. Count how many have positive total P&L
        3. Win rate = (profitable tokens / active tokens) * 100
        """
        
        if not tokens:
            message = "No recent trading activity" if has_recent_activity else "No trading activity detected"
            return self._build_empty_result(address, chain, message)
        
        total_profit = sum(t['total_profit'] for t in tokens)
        realized_profit = sum(t['realized_pnl'] for t in tokens)
        unrealized_profit = sum(t['unrealized_pnl'] for t in tokens)
        
        total_cost = sum(t['total_cost'] for t in tokens)
        current_value = sum(t['current_value'] for t in tokens)
        total_trades = sum(t['trades'] for t in tokens)
        
        winning_tokens = [t for t in tokens if t['is_winner']]
        losing_tokens = [t for t in tokens if not t['is_winner']]
        
        # ⭐ GMGN.ai WIN RATE CALCULATION
        if has_recent_activity:
            # Only count tokens with 7-day activity (win_rate != -1)
            active_tokens_7d = [t for t in tokens if t.get('win_rate', -1) >= 0]
            
            logger.info(f"📊 Total tokens: {len(tokens)}")
            logger.info(f"📊 Tokens with 7-day activity: {len(active_tokens_7d)}")
            
            if active_tokens_7d:
                # Count winners among active tokens
                winners_7d = [t for t in active_tokens_7d if t['is_winner']]
                losers_7d = [t for t in active_tokens_7d if not t['is_winner']]
                win_rate = (len(winners_7d) / len(active_tokens_7d)) * 100
                
                logger.info(
                    f"📊 GMGN Win Rate: {win_rate:.1f}% = "
                    f"{len(winners_7d)} winners / {len(active_tokens_7d)} active tokens"
                )
                logger.info(f"   Winners: {[t['symbol'] for t in winners_7d]}")
                logger.info(f"   Losers: {[t['symbol'] for t in losers_7d]}")
            else:
                # No 7-day activity within the fetched data
                win_rate = 0
                logger.info(f"📊 No tokens with 7-day activity detected")
        else:
            # Using all-time data (no 7-day activity)
            win_rate = (len(winning_tokens) / len(tokens) * 100) if tokens else 0
            logger.info(
                f"📊 All-time Win Rate: {win_rate:.1f}% = "
                f"{len(winning_tokens)} winners / {len(tokens)} total tokens"
            )
            logger.info(f"   All tokens: {[t['symbol'] for t in tokens]}")
            logger.info(f"   Winners: {[t['symbol'] for t in winning_tokens]}")
            logger.info(f"   Losers: {[t['symbol'] for t in losing_tokens]}")
        
        roi = (total_profit / total_cost * 100) if total_cost > 0 else 0
        
        # 7-day metrics
        tokens_7d = [
            t for t in tokens
            if self._is_within_days(t.get('last_trade'), 7)
        ]
        
        profit_7d = sum(t['total_profit'] for t in tokens_7d)
        profit_7d_pct = (profit_7d / sum(t['total_cost'] for t in tokens_7d) * 100) if tokens_7d else 0
        
        tokens.sort(key=lambda x: x['total_profit'], reverse=True)
        top_tokens = tokens[:10]
        
        active_days = self._calculate_active_days(tokens)
        last_trade_date = self._get_last_trade_date(tokens)
        
        return {
            'success': True,
            'address': address,
            'chain': chain,
            'total_profit': total_profit,
            'realized_profit': realized_profit,
            'unrealized_profit': unrealized_profit,
            'win_rate': win_rate,  # ⭐ GMGN-style win rate
            'roi': roi,
            'total_volume': total_cost + current_value,
            'total_trades': total_trades,
            'total_tokens_traded': len(tokens),
            'winning_tokens': len(winning_tokens),
            'losing_tokens': len(losing_tokens),
            'profit_7d': profit_7d,
            'profit_7d_pct': profit_7d_pct,
            'trades_7d': len(tokens_7d),
            'tokens_7d_active': len([t for t in tokens if t.get('win_rate', -1) >= 0]) if has_recent_activity else 0,
            'top_tokens': top_tokens,
            'active_days': active_days,
            'last_trade_date': last_trade_date,
            'has_recent_activity': has_recent_activity and len(tokens) > 0,
            'timestamp': datetime.now().isoformat(),
            'data_source': 'alchemy_gmgn_method'
        }
    
    def _is_within_days(self, timestamp: Optional[str], days: int) -> bool:
        """Check if timestamp is within N days"""
        if not timestamp:
            return False
        try:
            trade_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            cutoff = datetime.now() - timedelta(days=days)
            return trade_time >= cutoff
        except:
            return False
    
    def _calculate_active_days(self, tokens: List[Dict]) -> int:
        """Calculate number of active trading days"""
        dates = set()
        for token in tokens:
            try:
                if token.get('first_trade'):
                    dt = datetime.fromisoformat(token['first_trade'].replace('Z', '+00:00'))
                    dates.add(dt.date())
                if token.get('last_trade'):
                    dt = datetime.fromisoformat(token['last_trade'].replace('Z', '+00:00'))
                    dates.add(dt.date())
            except:
                continue
        return len(dates)
    
    def _get_last_trade_date(self, tokens: List[Dict]) -> str:
        """Get most recent trade date"""
        last_date = None
        for token in tokens:
            try:
                if token.get('last_trade'):
                    dt = datetime.fromisoformat(token['last_trade'].replace('Z', '+00:00'))
                    if not last_date or dt > last_date:
                        last_date = dt
            except:
                continue
        return last_date.strftime('%Y-%m-%d') if last_date else 'N/A'
    
    def _build_empty_result(self, address: str, chain: str, message: str = "No trading activity detected") -> Dict:
        """
        Build empty result for wallets with no activity
        
        Args:
            address: Wallet address
            chain: Blockchain name
            message: Custom message to display (default: "No trading activity detected")
        
        Returns:
            Dict with empty/zero values and success=True
        """
        return {
            'success': True,
            'address': address,
            'chain': chain,
            'total_profit': 0,
            'realized_profit': 0,
            'unrealized_profit': 0,
            'win_rate': 0,
            'roi': 0,
            'total_volume': 0,
            'total_trades': 0,
            'total_tokens_traded': 0,
            'winning_tokens': 0,
            'losing_tokens': 0,
            'profit_7d': 0,
            'profit_7d_pct': 0,
            'trades_7d': 0,
            'tokens_7d_active': 0,
            'top_tokens': [],
            'active_days': 0,
            'last_trade_date': 'N/A',
            'message': message,  # ⭐ Now accepts custom messages
            'has_recent_activity': False,
            'timestamp': datetime.now().isoformat(),
            'data_source': 'alchemy_gmgn_method'
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session"""
        if self.session_factory:
            return await self.session_factory.get_session('api')
        
        if not hasattr(self, '_session') or self._session.closed:
            connector = aiohttp.TCPConnector(limit=100)
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout
            )
        
        return self._session
    
    async def close(self):
        """Close HTTP session"""
        if hasattr(self, '_session') and not self._session.closed:
            await self._session.close()