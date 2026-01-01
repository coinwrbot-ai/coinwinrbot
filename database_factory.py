"""
Database Factory - Automatic database initialization based on config
Supports both SQLite and MySQL with seamless switching
"""
import logging
import os
from typing import Union


logger = logging.getLogger(__name__)


class Config:
    """
    Simplified config loader for database factory
    Falls back to environment variables if features.config is not available
    """
    # Telegram
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    ADMIN_USER_IDS = os.getenv('ADMIN_USER_IDS', '')
    SUPER_ADMIN_IDS = os.getenv('SUPER_ADMIN_IDS', '')

    DATABASE_TYPE = os.getenv('DATABASE_TYPE', 'mysql')
    
    # SQLite
    SQLITE_DB_PATH = os.getenv('SQLITE_DB_PATH', 'coinwinrbot.db')
    
    # MySQL
    MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
    MYSQL_USER = os.getenv('MYSQL_USER', 'coinwinr_user')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '65433020@aCoin')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'coinwinrbot')
    MYSQL_POOL_SIZE = int(os.getenv('MYSQL_POOL_SIZE', '20'))


def initialize_database() -> Union['Database', 'MySQLDatabase']:
    """
    Initialize database based on configuration
    
    Returns:
        Database instance (SQLite or MySQL)
    
    Raises:
        ValueError: If database type is invalid
        ImportError: If required database module is not installed
        Exception: If database connection fails
    """
    db_type = Config.DATABASE_TYPE.lower()
    
    logger.info("=" * 60)
    logger.info(f"🗄️  Initializing {db_type.upper()} Database")
    logger.info("=" * 60)
    
    try:
        if db_type == 'sqlite':
            return _initialize_sqlite()
            
        elif db_type == 'mysql':
            return _initialize_mysql()
        elif db_type == 'postgresql' or db_type == 'postgres':
            return _initialize_postgresql()
            
        else:
            raise ValueError(
                f"❌ Unknown database type: {db_type}\n"
                f"   Supported types: 'sqlite', 'mysql', 'postgresql'\n"
                f"   Set DATABASE_TYPE in .env file"
            )
            
    except ImportError as e:
        logger.error(f"❌ Missing database module: {e}")
        logger.error("   Install required packages:")
        if db_type == 'mysql':
            logger.error("   pip install mysql-connector-python")
        raise
        
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        logger.error("   Check your database configuration in .env")
        raise

    
def _initialize_postgresql():
    """Initialize PostgreSQL database for Render"""
    try:
        from database_postgres import PostgreSQLDatabase
        
        # Render provides DATABASE_URL automatically
        database_url = os.getenv('DATABASE_URL')
        
        if not database_url:
            raise ValueError(
                "❌ DATABASE_URL not found\n"
                "   Render should provide this automatically"
            )
        
        db = PostgreSQLDatabase(
            database_url=database_url,
            pool_size=int(os.getenv('PG_POOL_SIZE', '10'))
        )
        
        # Verify database is working
        total_users = db.get_total_users()
        
        logger.info(f"✅ PostgreSQL Database Connected")
        logger.info(f"   🗄️  Database: Render PostgreSQL")
        logger.info(f"   👥 Total Users: {total_users}")
        logger.info(f"   🔗 Pool Size: {os.getenv('PG_POOL_SIZE', '10')} connections")
        logger.info("=" * 60)
        
        return db
        
    except Exception as e:
        logger.error(f"❌ PostgreSQL initialization error: {e}")
        raise


def _initialize_sqlite():
    """Initialize SQLite database"""
    try:
        from database import Database
        
        db_path = Config.SQLITE_DB_PATH
        db = Database(db_path=db_path)
        
        # Verify database is working
        total_users = db.get_total_users()
        
        logger.info(f"✅ SQLite Database Connected")
        logger.info(f"   📁 Path: {db_path}")
        logger.info(f"   👥 Total Users: {total_users}")
        logger.info(f"   ⚡ Mode: WAL (Write-Ahead Logging)")
        logger.info(f"   💾 Cache: 64MB")
        logger.info("=" * 60)
        
        return db
        
    except FileNotFoundError:
        logger.error(f"❌ Database file not found: {Config.SQLITE_DB_PATH}")
        logger.error("   The database will be created automatically")
        raise
    except Exception as e:
        logger.error(f"❌ SQLite initialization error: {e}")
        raise


def _initialize_mysql():
    """Initialize MySQL database"""
    try:
        from database_mysql import MySQLDatabase
        
        # Check if password is set
        if not Config.MYSQL_PASSWORD:
            raise ValueError(
                "❌ MYSQL_PASSWORD is required\n"
                "   Set MYSQL_PASSWORD in your .env file"
            )
        
        db = MySQLDatabase(
            host=Config.MYSQL_HOST,
            port=Config.MYSQL_PORT,
            user=Config.MYSQL_USER,
            password=Config.MYSQL_PASSWORD,
            database=Config.MYSQL_DATABASE,
            pool_size=Config.MYSQL_POOL_SIZE
        )
        
        # Verify database is working
        total_users = db.get_total_users()
        
        logger.info(f"✅ MySQL Database Connected")
        logger.info(f"   🌐 Host: {Config.MYSQL_HOST}:{Config.MYSQL_PORT}")
        logger.info(f"   🗄️  Database: {Config.MYSQL_DATABASE}")
        logger.info(f"   👤 User: {Config.MYSQL_USER}")
        logger.info(f"   👥 Total Users: {total_users}")
        logger.info(f"   🔗 Pool Size: {Config.MYSQL_POOL_SIZE} connections")
        logger.info("=" * 60)
        
        return db
        
    except ImportError:
        logger.error("❌ MySQL connector not installed")
        logger.error("   Install it with: pip install mysql-connector-python")
        raise
    except Exception as e:
        logger.error(f"❌ MySQL initialization error: {e}")
        logger.error("\n🔧 Troubleshooting:")
        logger.error("   1. Check MySQL server is running")
        logger.error("   2. Verify credentials in .env file")
        logger.error("   3. Ensure database exists:")
        logger.error(f"      CREATE DATABASE {Config.MYSQL_DATABASE};")
        logger.error("   4. Grant user permissions:")
        logger.error(f"      GRANT ALL ON {Config.MYSQL_DATABASE}.* TO '{Config.MYSQL_USER}'@'localhost';")
        raise




def get_database_info() -> dict:
    """
    Get current database configuration info
    
    Returns:
        dict: Database configuration details
    """
    return {
        'type': Config.DATABASE_TYPE,
        'sqlite': {
            'path': Config.SQLITE_DB_PATH,
            'enabled': Config.DATABASE_TYPE == 'sqlite'
        },
        'mysql': {
            'host': Config.MYSQL_HOST,
            'port': Config.MYSQL_PORT,
            'database': Config.MYSQL_DATABASE,
            'user': Config.MYSQL_USER,
            'pool_size': Config.MYSQL_POOL_SIZE,
            'enabled': Config.DATABASE_TYPE == 'mysql'
        }
    }


# ============================================================================
# GLOBAL DATABASE INSTANCE
# ============================================================================

# Initialize database on module import
try:
    db = initialize_database()
    logger.info("🚀 Database ready for bot operations")
    
except Exception as e:
    logger.critical(f"💥 CRITICAL: Database initialization failed!")
    logger.critical(f"   Error: {e}")
    logger.critical(f"   The bot cannot start without a working database")
    logger.critical(f"   Please fix the database configuration and restart")
    raise SystemExit(1)


# ============================================================================
# HEALTH CHECK FUNCTION
# ============================================================================

def check_database_health() -> dict:
    """
    Perform database health check
    
    Returns:
        dict: Health status information
    """
    try:
        # Test basic query
        total_users = db.get_total_users()
        
        return {
            'status': 'healthy',
            'type': Config.DATABASE_TYPE,
            'total_users': total_users,
            'connected': True,
            'error': None
        }
        
    except Exception as e:
        logger.error(f"❌ Database health check failed: {e}")
        return {
            'status': 'unhealthy',
            'type': Config.DATABASE_TYPE,
            'total_users': 0,
            'connected': False,
            'error': str(e)
        }


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    """
    Test database connection
    
    Usage:
        python database_factory.py
    """
    print("\n" + "=" * 60)
    print("🧪 Database Factory Test")
    print("=" * 60 + "\n")
    
    # Show current configuration
    info = get_database_info()
    print(f"📊 Configuration:")
    print(f"   Type: {info['type'].upper()}")
    
    if info['type'] == 'sqlite':
        print(f"   Path: {info['sqlite']['path']}")
    else:
        print(f"   Host: {info['mysql']['host']}:{info['mysql']['port']}")
        print(f"   Database: {info['mysql']['database']}")
    
    print()
    
    # Perform health check
    health = check_database_health()
    print(f"🏥 Health Check:")
    print(f"   Status: {health['status'].upper()}")
    print(f"   Connected: {'✅ Yes' if health['connected'] else '❌ No'}")
    print(f"   Total Users: {health['total_users']}")
    
    if health['error']:
        print(f"   Error: {health['error']}")
    
    print("\n" + "=" * 60)
    print("✅ Test Complete")
    print("=" * 60 + "\n")
