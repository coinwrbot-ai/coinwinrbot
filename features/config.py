import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Bot configuration"""
    
    # Telegram
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    ADMIN_USER_IDS = os.getenv('ADMIN_USER_IDS', '1925544390')
    SUPER_ADMIN_IDS = os.getenv('SUPER_ADMIN_IDS', '1925544390')
    
    # API Keys
    HELIUS_API_KEY = os.getenv('HELIUS_API_KEY')
    MORALIS_API_KEY = os.getenv('MORALIS_API_KEY')
    BITQUERY_API_KEY =  os.getenv('BITQUERY_API_KEY')
    BIRDEYE_API_KEY = os.getenv('BIRDEYE_API_KEY')
    ALCHEMY_API_KEY = os.getenv('ALCHEMY_API_KEY')
    MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
    MYSQL_USER = os.getenv('MYSQL_USER', 'coinwinr_user')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '65433020@aCoin')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'coinwinrbot')
    MYSQL_POOL_SIZE = int(os.getenv('MYSQL_POOL_SIZE', '20'))
    
    # Explorer Keys
    ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY', '')
    BSCSCAN_API_KEY = os.getenv('BSCSCAN_API_KEY', '')
    POLYGONSCAN_API_KEY = os.getenv('POLYGONSCAN_API_KEY', '')
    ARBISCAN_API_KEY = os.getenv('ARBISCAN_API_KEY', '')
    OPTIMISM_ETHERSCAN_API_KEY = os.getenv('OPTIMISM_ETHERSCAN_API_KEY', '')
    BASESCAN_API_KEY = os.getenv('BASESCAN_API_KEY', '')
    SNOWTRACE_API_KEY = os.getenv('SNOWTRACE_API_KEY', '')
    
    # Admin
    ADMIN_USER_IDS = [
        int(uid) for uid in os.getenv('ADMIN_USER_IDS', '').split(',')
        if uid.strip()
    ]
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        required = {
            'TELEGRAM_BOT_TOKEN': cls.TELEGRAM_BOT_TOKEN,
            'HELIUS_API_KEY': cls.HELIUS_API_KEY,
            'MORALIS_API_KEY': cls.MORALIS_API_KEY
        }
        
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"Missing required config: {', '.join(missing)}")
        
        return True