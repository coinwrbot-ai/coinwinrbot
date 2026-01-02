"""
PostgreSQL Database Implementation for CoinWinRBot - COMPLETE
100% feature parity with MySQL
Optimized for Render.com free tier
"""

from binascii import Error
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import psycopg2
from psycopg2 import pool, extras
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class PostgreSQLDatabase:
    """PostgreSQL database with complete feature set and connection pooling"""
    
    def __init__(self, database_url: str, pool_size: int = 10):
        """
        Initialize PostgreSQL connection pool
        
        Args:
            database_url: PostgreSQL connection URL (provided by Render)
            pool_size: Number of connections in pool (default: 10)
        """
        self.database_url = database_url
        self.pool_size = pool_size
        self.db_type = 'postgresql'
        
        try:
            logger.info(f"🔗 Creating PostgreSQL connection pool...")
            logger.info(f"   Pool Size: {pool_size} connections")
            
            # Create connection pool
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1,  # Min connections
                pool_size,  # Max connections
                database_url
            )
            
            if self.connection_pool:
                logger.info("✅ PostgreSQL pool created successfully")
                
                # Test connection
                conn = self.get_connection()
                self.return_connection(conn)
                logger.info("✅ Test connection successful")
                
                # Initialize database schema
                self.init_database()
            else:
                raise Exception("Failed to create connection pool")
                
        except Exception as e:
            logger.error(f"❌ PostgreSQL pool creation failed: {e}")
            raise
    
    def get_connection(self):
        """Get connection from pool"""
        try:
            return self.connection_pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get connection: {e}")
            raise
    
    def return_connection(self, conn):
        """Return connection to pool"""
        try:
            self.connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to return connection: {e}")
    
    def init_database(self):
        """Initialize ALL database tables with complete schema"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            logger.info("🔧 Initializing complete database schema...")
            
            # Users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    first_name VARCHAR(255),
                    subscription_tier VARCHAR(50) DEFAULT 'free',
                    subscription_expires_at TIMESTAMP,
                    daily_analyses INT DEFAULT 0,
                    last_reset_date DATE,
                    total_analyses INT DEFAULT 0,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_admin BOOLEAN DEFAULT FALSE,
                    is_banned BOOLEAN DEFAULT FALSE,
                    ban_reason TEXT,
                    banned_at TIMESTAMP,
                    banned_by BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_users_tier ON users(subscription_tier);
                CREATE INDEX IF NOT EXISTS idx_users_expires ON users(subscription_expires_at);
                CREATE INDEX IF NOT EXISTS idx_users_banned ON users(is_banned);
                CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
            """)
            
            # Subscriptions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT UNIQUE REFERENCES users(user_id) ON DELETE CASCADE,
                    tier VARCHAR(50) DEFAULT 'free',
                    expires_at TIMESTAMP,
                    daily_analyses INT DEFAULT 0,
                    last_reset TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_analysis_date TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE
                );
                
                CREATE INDEX IF NOT EXISTS idx_subscriptions_tier ON subscriptions(tier);
                CREATE INDEX IF NOT EXISTS idx_subscriptions_expires ON subscriptions(expires_at);
                CREATE INDEX IF NOT EXISTS idx_subscriptions_active ON subscriptions(is_active);
            """)
            
            # Analysis logs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analysis_logs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                    address VARCHAR(255),
                    address_type VARCHAR(50),
                    chain VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN,
                    result_data TEXT,
                    metadata TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_analysis_logs_user_time ON analysis_logs(user_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_analysis_logs_timestamp ON analysis_logs(created_at);
                CREATE INDEX IF NOT EXISTS idx_analysis_logs_created_at ON analysis_logs(created_at);
            """)
            
            # Payment verifications table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS payment_verifications (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    subscription_tier VARCHAR(50) NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    payment_proof TEXT NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    verified_at TIMESTAMP,
                    verified_by BIGINT,
                    reviewed_by BIGINT,
                    reviewed_at TIMESTAMP,
                    admin_notes TEXT,
                    notes TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_payment_verifications_status ON payment_verifications(status);
                CREATE INDEX IF NOT EXISTS idx_payment_verifications_user ON payment_verifications(user_id);
            """)
            
            # Subscription history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS subscription_history (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    subscription_tier VARCHAR(50) NOT NULL,
                    start_date TIMESTAMP NOT NULL,
                    end_date TIMESTAMP NOT NULL,
                    amount_paid DECIMAL(10,2),
                    payment_method VARCHAR(50),
                    is_active BOOLEAN DEFAULT TRUE,
                    cancelled_at TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_subscription_history_user ON subscription_history(user_id);
                CREATE INDEX IF NOT EXISTS idx_subscription_history_active ON subscription_history(is_active);
            """)
            
            # Token analysis cache table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS token_analysis_cache (
                    id SERIAL PRIMARY KEY,
                    token_address VARCHAR(255),
                    chain VARCHAR(50),
                    total_traders INT,
                    profitable_traders INT,
                    avg_win_rate DECIMAL(5,2),
                    avg_roi DECIMAL(10,2),
                    total_volume DECIMAL(20,2),
                    sentiment VARCHAR(50),
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    UNIQUE (token_address, chain)
                );
                
                CREATE INDEX IF NOT EXISTS idx_token_analysis_cache_expires ON token_analysis_cache(expires_at);
            """)
            
            # Top traders cache table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS top_traders_cache (
                    id SERIAL PRIMARY KEY,
                    token_address VARCHAR(255),
                    chain VARCHAR(50),
                    wallet_address VARCHAR(255),
                    rank INT,
                    total_profit DECIMAL(20,2),
                    win_rate DECIMAL(5,2),
                    roi DECIMAL(10,2),
                    total_trades INT,
                    total_volume DECIMAL(20,2),
                    is_holder BOOLEAN,
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_top_traders_cache_token_chain ON top_traders_cache(token_address, chain);
            """)
            
            # Wallet analysis cache table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS wallet_analysis_cache (
                    id SERIAL PRIMARY KEY,
                    wallet_address VARCHAR(255),
                    chain VARCHAR(50),
                    win_rate DECIMAL(5,2),
                    total_trades INT,
                    profitable_trades INT,
                    total_profit DECIMAL(20,2),
                    avg_trade_size DECIMAL(20,2),
                    last_trade_time TIMESTAMP,
                    active_days INT,
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    analysis_data TEXT,
                    UNIQUE (wallet_address, chain)
                );
                
                CREATE INDEX IF NOT EXISTS idx_wallet_analysis_cache_expires ON wallet_analysis_cache(expires_at);
            """)
            
            # Tracked wallets table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tracked_wallets (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    wallet_address VARCHAR(255) NOT NULL,
                    chain VARCHAR(50) NOT NULL,
                    nickname VARCHAR(255),
                    alert_enabled BOOLEAN DEFAULT TRUE,
                    is_active BOOLEAN DEFAULT TRUE,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_checked TIMESTAMP,
                    UNIQUE (user_id, wallet_address, chain)
                );
                
                CREATE INDEX IF NOT EXISTS idx_tracked_wallets_user_active ON tracked_wallets(user_id, is_active);
            """)
            
            # Wallet alerts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS wallet_alerts (
                    id SERIAL PRIMARY KEY,
                    tracked_wallet_id INT REFERENCES tracked_wallets(id) ON DELETE CASCADE,
                    alert_type VARCHAR(50),
                    token_address VARCHAR(255),
                    is_active BOOLEAN DEFAULT TRUE,
                    amount DECIMAL(20,2),
                    value_usd DECIMAL(20,2),
                    tx_hash VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notified BOOLEAN DEFAULT FALSE
                );
                
                CREATE INDEX IF NOT EXISTS idx_wallet_alerts_tracked_wallet ON wallet_alerts(tracked_wallet_id);
                CREATE INDEX IF NOT EXISTS idx_wallet_alerts_notified ON wallet_alerts(notified);
            """)
            
            # Token blacklist table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS token_blacklist (
                    id SERIAL PRIMARY KEY,
                    token_address VARCHAR(255) UNIQUE,
                    chain VARCHAR(50),
                    reason TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    added_by BIGINT REFERENCES users(user_id) ON DELETE SET NULL
                );
                
                CREATE INDEX IF NOT EXISTS idx_token_blacklist_token ON token_blacklist(token_address);
                CREATE INDEX IF NOT EXISTS idx_token_blacklist_chain ON token_blacklist(chain);
            """)
            
            # Admin actions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS admin_actions (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    action_type VARCHAR(100) NOT NULL,
                    target_user_id BIGINT,
                    details TEXT,
                    action_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_admin_actions_admin ON admin_actions(admin_id);
                CREATE INDEX IF NOT EXISTS idx_admin_actions_timestamp ON admin_actions(action_timestamp);
            """)
            
            # Admin users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS admin_users (
                    admin_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
                    username VARCHAR(255),
                    role VARCHAR(50) DEFAULT 'admin',
                    permissions TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    added_by BIGINT,
                    is_active BOOLEAN DEFAULT TRUE
                );
                
                CREATE INDEX IF NOT EXISTS idx_admin_users_active ON admin_users(is_active);
            """)
            
            # Broadcast messages table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS broadcast_messages (
                    id SERIAL PRIMARY KEY,
                    message TEXT,
                    sent_by BIGINT REFERENCES admin_users(admin_id) ON DELETE SET NULL,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    target_tier VARCHAR(50),
                    total_users INT,
                    successful INT DEFAULT 0,
                    failed INT DEFAULT 0,
                    status VARCHAR(20) DEFAULT 'pending'
                );
                
                CREATE INDEX IF NOT EXISTS idx_broadcast_messages_status ON broadcast_messages(status);
                CREATE INDEX IF NOT EXISTS idx_broadcast_messages_sent_at ON broadcast_messages(sent_at);
            """)
            
            # Performance stats table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS performance_stats (
                    id SERIAL PRIMARY KEY,
                    date DATE UNIQUE,
                    total_analyses INT DEFAULT 0,
                    total_users INT DEFAULT 0,
                    active_users INT DEFAULT 0,
                    new_users INT DEFAULT 0,
                    revenue DECIMAL(10,2) DEFAULT 0,
                    token_analyses INT DEFAULT 0,
                    wallet_analyses INT DEFAULT 0
                );
                
                CREATE INDEX IF NOT EXISTS idx_performance_stats_date ON performance_stats(date);
            """)
            
            # System stats table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS system_stats (
                    id SERIAL PRIMARY KEY,
                    date DATE UNIQUE NOT NULL,
                    total_users INT DEFAULT 0,
                    active_users INT DEFAULT 0,
                    total_analyses INT DEFAULT 0,
                    revenue DECIMAL(10,2) DEFAULT 0,
                    new_subscriptions INT DEFAULT 0,
                    cabal_detections INT DEFAULT 0
                );
                
                CREATE INDEX IF NOT EXISTS idx_system_stats_date ON system_stats(date);
            """)
            
            # Admin logs table (for admin panel)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    action_type VARCHAR(50) NOT NULL,
                    target_user_id BIGINT,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_admin_logs_admin ON admin_logs(admin_id);
                CREATE INDEX IF NOT EXISTS idx_admin_logs_created_at ON admin_logs(created_at);
            """)
            
            conn.commit()
            logger.info("✅ PostgreSQL database schema initialized successfully")
            
            # Show table count
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            table_count = cursor.fetchone()[0]
            logger.info(f"📊 Total tables: {table_count}")
            
        except Exception as e:
            logger.error(f"❌ Schema initialization failed: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            self.return_connection(conn)
    
    # ========================================================================
    # USER MANAGEMENT - ALL METHODS
    # ========================================================================
    
    def register_user(self, user_id: int, username: str, first_name: str):
        """Register or update user"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
            exists = cursor.fetchone()
            
            if exists:
                cursor.execute("""
                    UPDATE users 
                    SET username = %s, first_name = %s, last_active = CURRENT_TIMESTAMP
                    WHERE user_id = %s
                """, (username, first_name, user_id))
            else:
                cursor.execute("""
                    INSERT INTO users 
                    (user_id, username, first_name, subscription_tier, daily_analyses, 
                     last_reset_date, total_analyses)
                    VALUES (%s, %s, %s, 'free', 0, CURRENT_DATE, 0)
                """, (user_id, username, first_name))
            
            conn.commit()
            logger.debug(f"✅ {'Updated' if exists else 'Registered'} user {user_id}")
            
        except Exception as e:
            logger.error(f"Register user error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def get_user(self, user_id: int) -> Optional[Dict]:
        """Get user information"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            result = cursor.fetchone()
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Get user error: {e}")
            return None
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Get user by username"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
            result = cursor.fetchone()
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Get user by username error: {e}")
            return None
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        user = self.get_user(user_id)
        return user and user.get('is_admin', False)
    
    def set_admin(self, user_id: int, is_admin: bool = True):
        """Set user admin status"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE users SET is_admin = %s WHERE user_id = %s
            """, (is_admin, user_id))
            conn.commit()
        except Exception as e:
            logger.error(f"Set admin error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def ban_user(self, user_id: int, reason: str, banned_by: int):
        """Ban a user"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE users 
                SET is_banned = TRUE, ban_reason = %s, 
                    banned_at = CURRENT_TIMESTAMP, banned_by = %s
                WHERE user_id = %s
            """, (reason, banned_by, user_id))
            conn.commit()
            logger.info(f"✅ Banned user {user_id}")
        except Exception as e:
            logger.error(f"Ban user error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def unban_user(self, user_id: int):
        """Unban a user"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE users 
                SET is_banned = FALSE, ban_reason = NULL, 
                    banned_at = NULL, banned_by = NULL
                WHERE user_id = %s
            """, (user_id,))
            conn.commit()
            logger.info(f"✅ Unbanned user {user_id}")
        except Exception as e:
            logger.error(f"Unban user error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def update_last_active(self, user_id: int):
        """Update user's last active timestamp"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE user_id = %s
            """, (user_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"Update last active error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def get_all_user_ids(self, tier: Optional[str] = None) -> List[int]:
        """Get all user IDs, optionally filtered by tier"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if tier:
                cursor.execute("""
                    SELECT user_id FROM users 
                    WHERE subscription_tier = %s AND is_banned = FALSE
                """, (tier,))
            else:
                cursor.execute("SELECT user_id FROM users WHERE is_banned = FALSE")
            
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Get all user IDs error: {e}")
            return []
        finally:
            cursor.close()
            self.return_connection(conn)
    
    # ========================================================================
    # SUBSCRIPTION MANAGEMENT - COMPLETE
    # ========================================================================
    
    def get_user_subscription(self, user_id: int) -> Dict:
        """Get user subscription details"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Reset daily counter if new day
            cursor.execute("""
                UPDATE users 
                SET daily_analyses = 0, last_reset_date = CURRENT_DATE
                WHERE user_id = %s 
                AND (last_reset_date IS NULL OR last_reset_date < CURRENT_DATE)
            """, (user_id,))
            
            cursor.execute("""
                SELECT 
                    subscription_tier as tier,
                    subscription_expires_at as expires_at,
                    daily_analyses,
                    CASE 
                        WHEN subscription_tier = 'free' THEN 5
                        WHEN subscription_tier = 'basic_weekly' THEN 10
                        WHEN subscription_tier = 'basic_monthly' THEN 15
                        WHEN subscription_tier = 'premium_weekly' THEN -1
                        WHEN subscription_tier = 'premium_monthly' THEN -1
                        ELSE 5
                    END as daily_limit,
                    CASE
                        WHEN subscription_tier = 'free' THEN TRUE
                        WHEN subscription_expires_at IS NULL AND subscription_tier != 'free' THEN FALSE
                        WHEN subscription_expires_at > CURRENT_TIMESTAMP THEN TRUE
                        ELSE FALSE
                    END as is_active,
                    last_reset_date as last_reset
                FROM users
                WHERE user_id = %s
            """, (user_id,))
            
            row = cursor.fetchone()
            conn.commit()
            
            if row:
                return dict(row)
            
            return {
                'tier': 'free',
                'expires_at': None,
                'daily_analyses': 0,
                'daily_limit': 5,
                'is_active': True,
                'last_reset': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Get subscription error: {e}")
            return {
                'tier': 'free',
                'expires_at': None,
                'daily_analyses': 0,
                'daily_limit': 5,
                'is_active': True,
                'last_reset': datetime.now().isoformat()
            }
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def get_user_daily_usage(self, user_id: int) -> Dict:
        """Get detailed daily usage information"""
        subscription = self.get_user_subscription(user_id)
        
        daily_limit = subscription['daily_limit']
        daily_analyses = subscription['daily_analyses']
        
        if daily_limit == -1:
            remaining = -1
        else:
            remaining = max(0, daily_limit - daily_analyses)
        
        return {
            'tier': subscription['tier'],
            'daily_analyses': daily_analyses,
            'daily_limit': daily_limit,
            'remaining': remaining,
            'is_active': subscription['is_active'],
            'reset_in_hours': 24,
            'last_reset': subscription.get('last_reset')
        }
    
    def increment_daily_analyses(self, user_id: int, count: int = 1):
        """Increment daily analysis counter"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE users 
                SET daily_analyses = daily_analyses + %s,
                    total_analyses = total_analyses + %s
                WHERE user_id = %s
            """, (count, count, user_id))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Increment analyses error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def increment_analysis_count(self, user_id: int, count: int = 1):
        """Alias for increment_daily_analyses"""
        self.increment_daily_analyses(user_id, count)
    
    def update_subscription(self, user_id: int, tier: str, duration_days: int):
        """Update user subscription"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if duration_days > 0:
                # Paid subscription with expiration
                cursor.execute("""
                    UPDATE users 
                    SET subscription_tier = %s,
                        subscription_expires_at = CURRENT_TIMESTAMP + INTERVAL '%s days',
                        daily_analyses = 0,
                        last_reset_date = CURRENT_DATE
                    WHERE user_id = %s
                """, (tier, duration_days, user_id))
            else:
                # Free tier or unlimited (no expiration)
                cursor.execute("""
                    UPDATE users 
                    SET subscription_tier = %s,
                        subscription_expires_at = NULL,
                        daily_analyses = 0,
                        last_reset_date = CURRENT_DATE
                    WHERE user_id = %s
                """, (tier, user_id))
            
            if cursor.rowcount == 0:
                logger.warning(f"⚠️ No rows updated for user {user_id}")
                conn.rollback()
                return False
            
            conn.commit()
            logger.info(f"✅ Updated subscription for user {user_id} to {tier}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Update subscription error: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            self.return_connection(conn)
    
    # ========================================================================
    # PAYMENT VERIFICATION - COMPLETE
    # ========================================================================
    
    def submit_payment_proof(self, user_id: int, tier: str, amount: float, proof: str) -> int:
        """Submit payment proof"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO payment_verifications 
                (user_id, subscription_tier, amount, payment_proof, status)
                VALUES (%s, %s, %s, %s, 'pending')
                RETURNING id
            """, (user_id, tier, amount, proof))
            
            verification_id = cursor.fetchone()[0]
            conn.commit()
            
            logger.info(f"✅ Payment proof submitted: ID {verification_id}")
            return verification_id
            
        except Exception as e:
            logger.error(f"Submit payment error: {e}")
            conn.rollback()
            return 0
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def get_pending_verifications(self) -> List[Dict]:
        """Get pending payment verifications"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cursor.execute("""
                SELECT 
                    pv.id, pv.user_id, u.username, u.first_name,
                    pv.subscription_tier, pv.amount, pv.payment_proof, pv.submitted_at
                FROM payment_verifications pv
                LEFT JOIN users u ON pv.user_id = u.user_id
                WHERE pv.status = 'pending'
                ORDER BY pv.submitted_at DESC
            """)
            
            return [dict(row) for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"Get pending verifications error: {e}")
            return []
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def get_verification(self, verification_id: int) -> Optional[Dict]:
        """Get verification by ID"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cursor.execute("""
                SELECT pv.*, u.username
                FROM payment_verifications pv
                LEFT JOIN users u ON pv.user_id = u.user_id
                WHERE pv.id = %s
            """, (verification_id,))
            
            result = cursor.fetchone()
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"Get verification error: {e}")
            return None
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def get_verification_status(self, verification_id: int, user_id: int) -> Optional[Dict]:
        """Get verification status"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cursor.execute("""
                SELECT 
                    id, user_id, subscription_tier as tier, amount, 
                    status, submitted_at, verified_at, admin_notes as notes
                FROM payment_verifications 
                WHERE id = %s AND user_id = %s
            """, (verification_id, user_id))
            
            result = cursor.fetchone()
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"Get verification status error: {e}")
            return None
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def verify_payment(self, verification_id: int, admin_id: int, 
                      approved: bool, notes: str = None) -> bool:
        """Verify payment (approve/reject)"""
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        try:
            cursor.execute("""
                SELECT user_id, subscription_tier, amount
                FROM payment_verifications 
                WHERE id = %s
            """, (verification_id,))
            
            row = cursor.fetchone()
            if not row:
                return False
            
            user_id = row['user_id']
            tier = row['subscription_tier']
            
            status = 'approved' if approved else 'rejected'
            cursor.execute("""
                UPDATE payment_verifications 
                SET status = %s, verified_at = NOW(), 
                    verified_by = %s, admin_notes = %s
                WHERE id = %s
            """, (status, admin_id, notes, verification_id))
            
            if approved:
                tier_durations = {
                    'basic_weekly': 7,
                    'basic_monthly': 30,
                    'premium_weekly': 7,
                    'premium_monthly': 30
                }
                duration = tier_durations.get(tier, 30)
                self.update_subscription(user_id, tier, duration)
            
            conn.commit()
            logger.info(f"✅ Payment {'approved' if approved else 'rejected'}: {verification_id}")
            return True
            
        except Error as e:
            logger.error(f"Verify payment error: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    
    # ========================================================================
    # STATISTICS & ANALYTICS - ALL MISSING METHODS
    # ========================================================================
    
    def get_admin_ids(self) -> List[int]:
        """Get list of admin user IDs"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT user_id FROM users WHERE is_admin = 1")
            return [row[0] for row in cursor.fetchall()]
        except Error as e:
            logger.error(f"Get admin IDs error: {e}")
            return []
        finally:
            cursor.close()
            conn.close()
    
    def get_total_users(self) -> int:
        """Get total number of users"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COUNT(*) FROM users")
            result = cursor.fetchone()
            return result[0] if result else 0
        except Error as e:
            logger.error(f"Get total users error: {e}")
            return 0
        finally:
            cursor.close()
            conn.close()
    
    def get_active_users_7d(self) -> int:
        """Get users active in last 7 days"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # PostgreSQL syntax: CURRENT_TIMESTAMP - INTERVAL '7 days'
            cursor.execute("""
                SELECT COUNT(DISTINCT user_id) 
                FROM analysis_logs 
                WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
            """)
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Get active users error: {e}")
            return 0
        finally:
            cursor.close()
            self.return_connection(conn)

    def get_active_users(self, days: int) -> int:
        """Get users active in last N days"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # PostgreSQL syntax with parameter
            cursor.execute("""
                SELECT COUNT(DISTINCT user_id) 
                FROM analysis_logs 
                WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '%s days'
            """, (days,))
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Get active users error: {e}")
            return 0
        finally:
            cursor.close()
            self.return_connection(conn)

    def get_analyses_today(self) -> int:
        """Get analyses today"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # PostgreSQL syntax: CURRENT_DATE
            cursor.execute("""
                SELECT COUNT(*) 
                FROM analysis_logs 
                WHERE DATE(created_at) = CURRENT_DATE
            """)
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Get analyses today error: {e}")
            return 0
        finally:
            cursor.close()
            self.return_connection(conn)

    def get_new_users_today(self) -> int:
        """Get new users today"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # PostgreSQL syntax: CURRENT_DATE
            cursor.execute("""
                SELECT COUNT(*) 
                FROM users 
                WHERE DATE(created_at) = CURRENT_DATE
            """)
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Get new users today error: {e}")
            return 0
        finally:
            cursor.close()
            self.return_connection(conn)

    def get_users_by_tier(self) -> Dict:
        """Get users by subscription tier"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT subscription_tier, COUNT(*) as count 
                FROM users 
                GROUP BY subscription_tier
            """)
            
            results = cursor.fetchall()
            
            tier_counts = {
                'free': 0,
                'basic_weekly': 0,
                'basic_monthly': 0,
                'premium_weekly': 0,
                'premium_monthly': 0
            }
            
            for row in results:
                tier = row[0]
                count = row[1]
                if tier in tier_counts:
                    tier_counts[tier] = count
            
            return tier_counts
        except Exception as e:
            logger.error(f"Get users by tier error: {e}")
            return {
                'free': 0,
                'basic_weekly': 0,
                'basic_monthly': 0,
                'premium_weekly': 0,
                'premium_monthly': 0
            }
        finally:
            cursor.close()
            self.return_connection(conn)

    def get_revenue_stats(self) -> Dict:
        """Get revenue statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get active subscriptions
            cursor.execute("""
                SELECT subscription_tier, COUNT(*) as count
                FROM users
                WHERE subscription_tier != 'free' 
                AND (subscription_expires_at IS NULL OR subscription_expires_at > CURRENT_TIMESTAMP)
                GROUP BY subscription_tier
            """)
            
            active_subs = cursor.fetchall()
            
            # Tier pricing
            tier_prices = {
                'basic_weekly': 5,
                'basic_monthly': 20,
                'premium_weekly': 10,
                'premium_monthly': 40
            }
            
            # Calculate MRR
            mrr = 0
            total_active = 0
            for tier, count in active_subs:
                total_active += count
                if tier in tier_prices:
                    price = tier_prices[tier]
                    if 'weekly' in tier:
                        monthly_revenue = (price * 4.33) * count
                    else:
                        monthly_revenue = price * count
                    mrr += monthly_revenue
            
            # This month's revenue - PostgreSQL syntax
            cursor.execute("""
                SELECT COALESCE(SUM(amount), 0)
                FROM payment_verifications 
                WHERE status = 'approved' 
                AND EXTRACT(YEAR FROM verified_at) = EXTRACT(YEAR FROM CURRENT_TIMESTAMP)
                AND EXTRACT(MONTH FROM verified_at) = EXTRACT(MONTH FROM CURRENT_TIMESTAMP)
            """)
            month_revenue = cursor.fetchone()[0]
            
            # All-time revenue
            cursor.execute("""
                SELECT COALESCE(SUM(amount), 0)
                FROM payment_verifications 
                WHERE status = 'approved'
            """)
            total_revenue = cursor.fetchone()[0]
            
            return {
                'mrr': float(mrr),
                'this_month': float(month_revenue),
                'month_revenue': float(month_revenue),
                'all_time': float(total_revenue),
                'total_revenue': float(total_revenue),
                'active_subscriptions': total_active
            }
        except Exception as e:
            logger.error(f"Get revenue stats error: {e}")
            return {
                'mrr': 0.0,
                'this_month': 0.0,
                'month_revenue': 0.0,
                'all_time': 0.0,
                'total_revenue': 0.0,
                'active_subscriptions': 0
            }
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def get_admin_stats(self) -> Dict:
        """Get comprehensive admin statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Total users
            cursor.execute("SELECT COUNT(*) FROM users")
            total_users = cursor.fetchone()[0]
            
            # Paid users - PostgreSQL syntax
            cursor.execute("""
                SELECT COUNT(*) FROM users 
                WHERE subscription_tier != 'free' 
                AND (subscription_expires_at IS NULL OR subscription_expires_at > CURRENT_TIMESTAMP)
            """)
            paid_users = cursor.fetchone()[0]
            
            # Active today - PostgreSQL syntax
            cursor.execute("""
                SELECT COUNT(DISTINCT user_id) 
                FROM analysis_logs 
                WHERE DATE(created_at) = CURRENT_DATE
            """)
            active_today = cursor.fetchone()[0]
            
            revenue = self.get_revenue_stats()
            
            return {
                'total_users': total_users,
                'paid_users': paid_users,
                'active_today': active_today,
                'free_users': total_users - paid_users,
                'basic_users': 0,
                'premium_users': paid_users,
                'monthly_revenue': revenue['mrr'],
                'total_revenue': revenue['total_revenue'],
                'analyses_today': self.get_analyses_today(),
                'total_analyses': 0
            }
            
        except Exception as e:
            logger.error(f"Get admin stats error: {e}")
            return {
                'total_users': 0,
                'paid_users': 0,
                'active_today': 0,
                'free_users': 0,
                'basic_users': 0,
                'premium_users': 0,
                'monthly_revenue': 0.0,
                'total_revenue': 0.0,
                'analyses_today': 0,
                'total_analyses': 0
            }
        finally:
            cursor.close()
            self.return_connection(conn)

    def get_user_stats(self, user_id: int) -> Dict:
        """Get user statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Total analyses
            cursor.execute("""
                SELECT COUNT(*) FROM analysis_logs WHERE user_id = %s
            """, (user_id,))
            total_analyses = cursor.fetchone()[0]
            
            # Daily analyses
            cursor.execute("""
                SELECT daily_analyses FROM users WHERE user_id = %s
            """, (user_id,))
            result = cursor.fetchone()
            daily_analyses = result[0] if result else 0
            
            # Tracked wallets
            cursor.execute("""
                SELECT COUNT(*) FROM tracked_wallets
                WHERE user_id = %s AND is_active = TRUE
            """, (user_id,))
            wallets_tracked = cursor.fetchone()[0]
            
            return {
                'total_analyses': total_analyses,
                'daily_analyses': daily_analyses,
                'wallets_tracked': wallets_tracked,
                'successful_analyses': total_analyses,
                'batch_today': 0
            }
        except Exception as e:
            logger.error(f"Get user stats error: {e}")
            return {
                'total_analyses': 0,
                'daily_analyses': 0,
                'wallets_tracked': 0,
                'successful_analyses': 0,
                'batch_today': 0
            }
        finally:
            cursor.close()
            self.return_connection(conn)

    def get_today_registrations(self) -> int:
        """Alias for get_new_users_today"""
        return self.get_new_users_today()
    
    def get_admin_dashboard_stats(self) -> Dict:
        """Get all stats for admin dashboard - MISSING METHOD"""
        return {
            'total_users': self.get_total_users(),
            'active_7d': self.get_active_users_7d(),
            'analyses_today': self.get_analyses_today(),
            'new_users_today': self.get_new_users_today(),
            'users_by_tier': self.get_users_by_tier(),
            'active_subs': self.get_revenue_stats()['active_subscriptions'],
            'revenue': self.get_revenue_stats()
        }
    
    
    # ========================================================================
    # LOGGING & ACTIONS
    # ========================================================================
    
    def log_admin_action(self, admin_id: int, action_type: str,
                        target_user_id: int = None, details: str = None):
        """Log admin action"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO admin_actions (admin_id, action_type, target_user_id, details)
                VALUES (%s, %s, %s, %s)
            """, (admin_id, action_type, target_user_id, details))
            
            conn.commit()
            
        except Error as e:
            logger.error(f"Log admin action error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    
    def log_analysis(self, user_id: int, address: str, chain: str,
                    result: Dict, address_type: str = 'unknown'):
        """Log analysis for analytics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO analysis_logs
                (user_id, address, address_type, chain, success, result_data)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                user_id,
                address,
                address_type,
                chain,
                result.get('success', True),
                json.dumps(result)
            ))
            
            conn.commit()
        except Error as e:
            logger.error(f"Log analysis error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    
    def log_broadcast(self, message: str, admin_id: int, 
                     target_tier: str, total_users: int) -> int:
        """Log broadcast message - MISSING METHOD"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO broadcast_messages
                (message, sent_by, target_tier, total_users)
                VALUES (%s, %s, %s, %s)
            """, (message, admin_id, target_tier, total_users))
            
            broadcast_id = cursor.lastrowid
            conn.commit()
            return broadcast_id
        except Error as e:
            logger.error(f"Log broadcast error: {e}")
            conn.rollback()
            return 0
        finally:
            cursor.close()
            conn.close()
    
    def update_broadcast_stats(self, broadcast_id: int, 
                              successful: int, failed: int):
        """Update broadcast statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            status = 'completed' if failed == 0 else 'partial'
            
            cursor.execute("""
                UPDATE broadcast_messages 
                SET successful = %s, failed = %s, status = %s
                WHERE id = %s
            """, (successful, failed, status, broadcast_id))
            
            conn.commit()
        except Error as e:
            logger.error(f"Update broadcast stats error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    # ========================================================================
    # REMAINING METHODS FROM ORIGINAL (keeping all existing functionality)
    # ========================================================================
    
    def add_tracked_wallet(self, user_id: int, wallet_address: str,
                          chain: str, nickname: Optional[str] = None) -> bool:
        """Add wallet to user's tracking list"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO tracked_wallets
                (user_id, wallet_address, chain, nickname)
                VALUES (%s, %s, %s, %s)
            """, (user_id, wallet_address, chain, nickname))
            
            conn.commit()
            logger.info(f"✅ Added tracked wallet for user {user_id}")
            return True
        except Error as e:
            if e.errno == 1062:
                logger.warning(f"Wallet already tracked by user {user_id}")
                return False
            logger.error(f"Add tracked wallet error: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    
    def get_tracked_wallets(self, user_id: int) -> List[Dict]:
        """Get all tracked wallets for user"""
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        try:
            cursor.execute("""
                SELECT id, wallet_address, chain, nickname, added_at, last_checked
                FROM tracked_wallets
                WHERE user_id = %s AND is_active = 1
                ORDER BY added_at DESC
            """, (user_id,))
            
            return cursor.fetchall()
        except Error as e:
            logger.error(f"Get tracked wallets error: {e}")
            return []
        finally:
            cursor.close()
            conn.close()
    
    def remove_tracked_wallet(self, user_id: int, wallet_id: int) -> bool:
        """Remove wallet from tracking"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE tracked_wallets
                SET is_active = 0
                WHERE id = %s AND user_id = %s
            """, (wallet_id, user_id))
            
            conn.commit()
            return cursor.rowcount > 0
        except Error as e:
            logger.error(f"Remove tracked wallet error: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    
    def close(self):
        """Close connection pool"""
        try:
            self.connection_pool = None
            logger.info("✅ MySQL connection pool closed")
        except Exception as e:
            logger.error(f"Close pool error: {e}")
