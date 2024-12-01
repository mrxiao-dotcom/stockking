# 数据库配置
MYSQL_CONFIG = {
    'host': '10.17.31.104',
    'user': 'root',
    'password': 'Xj774913@',
    'port': 3306,
    'database': 'stock',
    'charset': 'utf8mb4',
    'use_pure': True,
    'ssl_disabled': True,
    'auth_plugin': 'mysql_native_password',
    'pool_name': 'mypool',
    'pool_size': 5,
    'connection_timeout': 10,
    'allow_local_infile': True,
    'get_warnings': False,
    'raise_on_warnings': False,
    'consume_results': True
}

# Tushare配置
TUSHARE_TOKEN = "f27227e18d0ee9d6e0e2430dc1eca3e56e9ea70d0b3e24d72f72a174"

# 其他全局配置
DEFAULT_PAGE_SIZE = 15 