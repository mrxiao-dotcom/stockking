import mysql.connector
import time
import logging
from config import MYSQL_CONFIG

class DatabaseOperations:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        """连接数据库"""
        try:
            # 添加重试机制
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    # 创建新的连接配置，禁用警告
                    config = MYSQL_CONFIG.copy()
                    config['get_warnings'] = False
                    config['raise_on_warnings'] = False
                    
                    self.connection = mysql.connector.connect(**config)
                    self.cursor = self.connection.cursor(dictionary=True)
                    
                    # 设置会话变量
                    session_variables = [
                        "SET SESSION wait_timeout = 28800",
                        "SET SESSION interactive_timeout = 28800",
                        "SET SESSION net_read_timeout = 30",
                        "SET SESSION net_write_timeout = 60",
                        "SET SESSION innodb_lock_wait_timeout = 50",
                        "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'"
                    ]
                    
                    for query in session_variables:
                        try:
                            self.cursor.execute(query)
                            self.connection.commit()
                        except Exception as e:
                            logging.warning(f"设置会话变量失败: {query} - {str(e)}")
                            continue
                    
                    logging.info("数据库连接成功！")
                    break
                    
                except mysql.connector.Error as err:
                    retry_count += 1
                    if retry_count == max_retries:
                        raise
                    logging.warning(f"连接失败，正在重试 ({retry_count}/{max_retries})...")
                    time.sleep(1)  # 等待1秒后重试
                    
        except mysql.connector.Error as err:
            logging.error(f"数据库连接错误: {err}")
            raise
        except Exception as e:
            logging.error(f"连接过程中发生错误: {str(e)}")
            raise

    def reconnect(self):
        """重新连接数据库"""
        try:
            if self.connection and self.connection.is_connected():
                self.connection.close()
            self.connect()
        except Exception as e:
            logging.error(f"重新连接失败: {str(e)}")
            raise

    def ensure_connection(func):
        """确保数据库连接的装饰器"""
        def wrapper(self, *args, **kwargs):
            try:
                if not self.connection or not self.connection.is_connected():
                    self.reconnect()
                return func(self, *args, **kwargs)
            except mysql.connector.errors.OperationalError as e:
                if "Lost connection" in str(e):
                    self.reconnect()
                    return func(self, *args, **kwargs)
                raise
            except Exception as e:
                logging.error(f"操作执行失败: {str(e)}")
                raise
        return wrapper

    def disconnect(self):
        """断开数据库连接"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection and self.connection.is_connected():
                self.connection.close()
                logging.info("数据库连接已关闭")
        except Exception as e:
            logging.error(f"关闭连接失败: {str(e)}")

    def __del__(self):
        """析构函数"""
        self.disconnect()

    def get_all_sectors(self):
        try:
            self.cursor.execute("SELECT * FROM sectors")
            return self.cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"查询sectors表错误: {err}")
            return []
            
    def get_sector_stocks(self, sector_id):
        try:
            self.cursor.execute("""
                SELECT * FROM sector_stocks 
                WHERE sector_id = %s
            """, (sector_id,))
            return self.cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"查询sector_stocks错误: {err}")
            return []
            
    def add_sector(self, sector_name, sector_type, sector_code):
        try:
            # 获取新的sector_id
            self.cursor.execute("SELECT MAX(sector_id) as max_id FROM sectors")
            result = self.cursor.fetchone()
            new_sector_id = 1 if result['max_id'] is None else result['max_id'] + 1
            
            # 插入sectors表
            self.cursor.execute("""
                INSERT INTO sectors (sector_id, sector_code, sector_name, sector_type) 
                VALUES (%s, %s, %s, %s)
            """, (new_sector_id, sector_code, sector_name, sector_type))
            self.connection.commit()
            return True
        except mysql.connector.Error as err:
            print(f"添加sector错误: {err}")
            return False
            
    def add_stock_to_sector(self, sector_id, stock_code):
        try:
            # 获取新的id
            self.cursor.execute("SELECT MAX(id) as max_id FROM sector_stocks")
            result = self.cursor.fetchone()
            new_id = 1 if result['max_id'] is None else result['max_id'] + 1
            
            # 插入sector_stocks表
            self.cursor.execute("""
                INSERT INTO sector_stocks (id, sector_id, stock_code) 
                VALUES (%s, %s, %s)
            """, (new_id, sector_id, stock_code))
            self.connection.commit()
            return True
        except mysql.connector.Error as err:
            print(f"添加stock到sector错误: {err}")
            return False 

    def get_last_sector_id(self):
        try:
            self.cursor.execute("SELECT MAX(sector_id) as last_id FROM sectors")
            result = self.cursor.fetchone()
            return result['last_id']
        except mysql.connector.Error as err:
            print(f"获取最后sector_id错误: {err}")
            return None 

    def update_sector(self, sector_id, sector_name, sector_type, sector_code):
        try:
            self.cursor.execute("""
                UPDATE sectors 
                SET sector_name = %s, sector_type = %s, sector_code = %s
                WHERE sector_id = %s
            """, (sector_name, sector_type, sector_code, sector_id))
            self.connection.commit()
            return True
        except mysql.connector.Error as err:
            print(f"更新sector错误: {err}")
            return False
            
    def delete_stock_from_sector(self, sector_id, stock_code):
        try:
            self.cursor.execute("""
                DELETE FROM sector_stocks 
                WHERE sector_id = %s AND stock_code = %s
            """, (sector_id, stock_code))
            self.connection.commit()
            return True
        except mysql.connector.Error as err:
            print(f"删除stock错误: {err}")
            return False 

    def update_stock_in_sector(self, sector_id, old_stock_code, new_stock_code):
        try:
            # 更新sector_stocks表
            self.cursor.execute("""
                UPDATE sector_stocks 
                SET stock_code = %s
                WHERE sector_id = %s AND stock_code = %s
            """, (new_stock_code, sector_id, old_stock_code))
            self.connection.commit()
            return True
        except mysql.connector.Error as err:
            print(f"更新stock错误: {err}")
            return False 

    def get_sector_stock_performance(self, sector_id, start_date='2023-09-20'):
        try:
            # 使用JOIN优化查询
            self.cursor.execute("""
                SELECT sd.ts_code, sd.trade_date, sd.close, sd.open
                FROM sector_stocks ss
                JOIN stock_data sd ON ss.stock_code = sd.ts_code
                WHERE ss.sector_id = %s 
                AND sd.trade_date >= %s
                ORDER BY sd.ts_code, sd.trade_date
            """, (sector_id, start_date))
            return self.cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"查询股票表现错误: {err}")
            return []
            
    def get_sector_daily_amount(self, sector_id, start_date='2023-09-20'):
        try:
            self.cursor.execute("""
                SELECT sd.trade_date, SUM(sd.amount) as total_amount
                FROM sector_stocks ss
                JOIN stock_data sd ON ss.stock_code = sd.ts_code
                WHERE ss.sector_id = %s 
                AND sd.trade_date >= %s
                GROUP BY sd.trade_date
                ORDER BY sd.trade_date ASC
            """, (sector_id, start_date))
            return self.cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"查询板块成交额错误: {err}")
            return [] 

    def delete_sector(self, sector_id):
        try:
            # 先删除板块关联的所有股票
            self.cursor.execute("""
                DELETE FROM sector_stocks 
                WHERE sector_id = %s
            """, (sector_id,))
            
            # 再删除板块
            self.cursor.execute("""
                DELETE FROM sectors 
                WHERE sector_id = %s
            """, (sector_id,))
            
            self.connection.commit()
            return True
        except mysql.connector.Error as err:
            print(f"删除板块错误: {err}")
            return False 

    def get_stock_name(self, stock_code):
        """获取股票名称"""
        try:
            self.cursor.execute("""
                SELECT 证券简称
                FROM stocks
                WHERE 证券代码 = %s
            """, (stock_code,))
            result = self.cursor.fetchone()
            return result['证券简称'] if result else "未知"
        except mysql.connector.Error as err:
            print(f"获取股票名称错误: {err}")
            return "未知"

    def get_stock_code(self, stock_name_or_code):
        """根据股票名称或代码获取股票代码"""
        try:
            self.cursor.execute("""
                SELECT 证券代码
                FROM stocks 
                WHERE 证券代码 = %s 
                OR 证券简称 = %s
            """, (stock_name_or_code, stock_name_or_code))
            result = self.cursor.fetchone()
            return result['证券代码'] if result else None
        except mysql.connector.Error as err:
            print(f"查询股票代码错误: {err}")
            return None

    def get_stock_code_by_name(self, stock_name):
        """根据股票名称获取股票代码"""
        try:
            self.cursor.execute("""
                SELECT 证券代码
                FROM stocks
                WHERE 证券简称 = %s
            """, (stock_name,))
            result = self.cursor.fetchone()
            return result['证券代码'] if result else None
        except mysql.connector.Error as err:
            logging.error(f"查询股票代码错误: {err}")
            return None