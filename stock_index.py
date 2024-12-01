import tkinter as tk
from tkinter import ttk, messagebox
import tushare as ts
from config import TUSHARE_TOKEN, MYSQL_CONFIG
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import logging
from rate_limiter import rate_limiter  # 导入频率限制器
import time
from concurrent.futures import ThreadPoolExecutor
from mysql.connector import pooling

# 配置tushare
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

# 指数代码映射
INDEX_CODES = {
    "上证综指": "000001.SH",
    "深证成指": "399001.SZ",
    "上证50": "000016.SH",
    "中证500": "000905.SH",
    "中小板指": "399005.SZ",
    "创业板指": "399006.SZ"
}

class StockIndex:
    def __init__(self, master=None):
        if master is None:
            master = tk.Tk()
        self.master = master
        
        # 创建数据库连接池
        self.db_pool = self.create_db_pool()
        
        # 创建主框架
        self.main_frame = ttk.Frame(self.master)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # 创建导航区
        self.create_navigation()
        
        # 创建内容区域
        self.content_frame = ttk.Frame(self.main_frame)
        self.content_frame.pack(fill=tk.BOTH, expand=True, pady=(20, 0))
        
        # 默认显示大盘指标页面
        self.show_market_indicators()
        
        # 添加自动更新标志
        self.auto_update_enabled = False
        
        # 检查是否需要启动自动更新
        self.check_auto_update()

    def create_db_pool(self):
        """创建数据库连接池"""
        try:
            pool_config = {
                'pool_name': 'mypool',
                'pool_size': 10,
                'pool_reset_session': True,
                **MYSQL_CONFIG
            }
            return mysql.connector.pooling.MySQLConnectionPool(**pool_config)
        except Exception as e:
            logging.error(f"创建数据库连接池失败: {str(e)}")
            raise

    def get_db_connection(self):
        """获取数据库连接"""
        try:
            return self.db_pool.get_connection()
        except Exception as e:
            logging.error(f"获取数据库连接失败: {str(e)}")
            raise

    def create_navigation(self):
        """创建导航区域"""
        # 创建导航卡片
        nav_card = ttk.Frame(self.main_frame, style="Card.TFrame")
        nav_card.pack(fill=tk.X)
        
        # 创建按钮组 - 添加左边距
        button_frame = ttk.Frame(nav_card)
        button_frame.pack(fill=tk.X, padx=20, pady=10)
        
        # 创建导航按钮
        self.market_btn = ttk.Button(
            button_frame,
            text="大盘指标",
            command=self.show_market_indicators,
            style="Operation.TButton",
            width=12
        )
        self.market_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        self.index_btn = ttk.Button(
            button_frame,
            text="指数行情",
            command=self.show_index_quotes,
            style="Operation.TButton",
            width=12
        )
        self.index_btn.pack(side=tk.LEFT, padx=5)
        
        # 设置按钮样式
        style = ttk.Style()
        style.configure(
            "Operation.TButton",
            font=("Microsoft YaHei UI", 9),
            padding=6
        )
        
        # 添加选中状态样式
        style.map(
            "Operation.TButton",
            background=[('pressed', '#1890ff'), ('active', '#40a9ff')],
            foreground=[('pressed', 'white'), ('active', 'white')]
        )
        
        # 默认选中大盘指标按钮
        self.update_button_state(self.market_btn)

    def update_button_state(self, active_button):
        """更新按钮状态"""
        # 重置所有按钮样式
        for btn in [self.market_btn, self.index_btn]:
            btn.state(['!pressed'])
            btn.configure(style="Operation.TButton")
        
        # 设置选中按钮样式
        active_button.state(['pressed'])
        active_button.configure(style="Operation.TButton")

    def show_market_indicators(self):
        """显示大盘指标页面"""
        # 清除当前内容
        for widget in self.content_frame.winfo_children():
            widget.destroy()
        
        # 更新按钮状态
        self.update_button_state(self.market_btn)
        
        # 创建数据展示区域的容器
        self.market_container = ttk.Frame(self.content_frame)
        self.market_container.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 创建指数选择区域
        select_frame = ttk.LabelFrame(self.market_container, text="指数选择", padding="10")
        select_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 创建按钮容器
        button_container = ttk.Frame(select_frame)
        button_container.pack(fill=tk.X, padx=5, pady=5)
        
        # 创建指数按钮
        for index_name in INDEX_CODES.keys():
            ttk.Button(
                button_container,
                text=index_name,
                command=lambda name=index_name: self.show_index_data(name),
                style="Index.TButton",
                width=15
            ).pack(side=tk.LEFT, padx=5)
        
        # 创建按钮区域
        button_frame = ttk.Frame(select_frame)
        button_frame.pack(side=tk.RIGHT, padx=5)
        
        # 添加历史数据按钮
        ttk.Button(
            button_frame,
            text="获取历史数据",
            command=self.show_daily_date_picker,
            style="Primary.TButton",
            width=15
        ).pack(side=tk.RIGHT, padx=5)
        
        # 添加更新按钮
        ttk.Button(
            button_frame,
            text="更新数据",
            command=self.update_market_data,
            style="Primary.TButton",
            width=15
        ).pack(side=tk.RIGHT, padx=5)
        
        # 创建数据表格区域
        self.data_frame = ttk.LabelFrame(self.market_container, text="指标数据", padding="10")
        self.data_frame.pack(fill="both", expand=True)
        
        # 创建表格容器
        table_container = ttk.Frame(self.data_frame)
        table_container.pack(fill="both", expand=True, pady=(0, 10))
        
        # 创建表格
        columns = ("交易日期", "总市值(亿)", "流通市值(亿)", "总股本(亿)", "流通股本(亿)", 
                  "自由流通股本(亿)", "换手率(%)", "换手率(自由流通)(%)", "市盈率", "市盈率TTM", "市净率")
        
        self.market_tree = ttk.Treeview(
            table_container,
            columns=columns,
            show='headings',
            height=15,
            style="Custom.Treeview"
        )
        
        # 设置列宽和对齐方式
        for col in columns:
            self.market_tree.heading(col, text=col)
            self.market_tree.column(col, width=100, anchor='center')
        
        # 添加垂直滚动条
        vsb = ttk.Scrollbar(table_container, orient="vertical", command=self.market_tree.yview)
        self.market_tree.configure(yscrollcommand=vsb.set)
        
        # 添加水平滚动条
        hsb = ttk.Scrollbar(table_container, orient="horizontal", command=self.market_tree.xview)
        self.market_tree.configure(xscrollcommand=hsb.set)
        
        # 布局表格和滚动条
        self.market_tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        
        # 配置表格容器的网格权重
        table_container.grid_columnconfigure(0, weight=1)
        table_container.grid_rowconfigure(0, weight=1)
        
        # 设置颜色标签
        self.market_tree.tag_configure('up', foreground='#ff4d4f')
        self.market_tree.tag_configure('down', foreground='#52c41a')
        self.market_tree.tag_configure('zero', foreground='#666666')

    def get_latest_trade_date(self):
        """获取最新交易日期"""
        now = datetime.now()
        current_time = now.time()
        current_weekday = now.weekday()  # 0-6 表示周一到周日

        # 如果是周末（周六或周日）
        if current_weekday >= 5:
            # 获取上周五的日期
            days_to_subtract = current_weekday - 4  # 减去到上周五的天数
            latest_date = now - timedelta(days=days_to_subtract)
            logging.info(f"当前是周末，使用上周五 {latest_date.strftime('%Y%m%d')} 作为最新交易日")
            return latest_date.strftime('%Y%m%d')

        # 工作日判断
        if current_time.hour < 15 or (current_time.hour == 15 and current_time.minute < 30):  # 15:30前
            # 获取前一天的日期
            if current_weekday == 0:  # 如果是周一
                # 获取上周五的日期
                latest_date = now - timedelta(days=3)
                logging.info(f"当前是周一15:30前，使用上周五 {latest_date.strftime('%Y%m%d')} 作为最新交易日")
            else:
                # 获取前一天的日期
                latest_date = now - timedelta(days=1)
                logging.info(f"当前是工作日15:30前，使用昨天 {latest_date.strftime('%Y%m%d')} 作为最新交易日")
        else:  # 15:30后
            # 使用当天日期
            latest_date = now
            logging.info(f"当前是工作日15:30后，使用今天 {latest_date.strftime('%Y%m%d')} 作为最新交易日")

        return latest_date.strftime('%Y%m%d')

    def update_market_data(self):
        """更新大盘指标数据"""
        try:
            # 创建进度窗口
            progress_window = tk.Toplevel(self.master)
            progress_window.title("更新进度")
            progress_window.geometry("500x400")
            progress_window.transient(self.master)
            
            # 创建进度条
            progress_frame = ttk.Frame(progress_window, padding="10")
            progress_frame.pack(fill=tk.X)
            
            progress_bar = ttk.Progressbar(
                progress_frame,
                mode='determinate',
                length=300
            )
            progress_bar.pack(pady=10)
            
            # 创建进度标签
            progress_label = ttk.Label(
                progress_frame,
                text="准备更新...",
                font=("Microsoft YaHei UI", 9)
            )
            progress_label.pack(pady=5)
            
            # 创建文本区域
            text_frame = ttk.Frame(progress_window)
            text_frame.pack(fill=tk.BOTH, expand=True, padx=10)
            
            text_area = tk.Text(
                text_frame,
                height=15,
                width=50,
                font=("Microsoft YaHei UI", 9)
            )
            text_area.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            
            # 添加滚动条
            scrollbar = ttk.Scrollbar(text_frame)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            text_area.config(yscrollcommand=scrollbar.set)
            scrollbar.config(command=text_area.yview)
            
            # 获取当前最新交易日期
            latest_trade_date = self.get_latest_trade_date()
            
            # 获取所有指数代码
            total_indices = len(INDEX_CODES)
            progress_bar['maximum'] = total_indices
            current_progress = 0
            
            # 更新每个指数的数据
            for index_name, ts_code in INDEX_CODES.items():
                try:
                    # 使用频率限制器
                    rate_limiter.wait()
                    
                    # 使用正确的接口名 index_dailybasic
                    df = pro.index_dailybasic(
                        ts_code=ts_code,
                        trade_date=latest_trade_date,
                        fields='ts_code,trade_date,total_mv,float_mv,total_share,float_share,free_share,turnover_rate,turnover_rate_f,pe,pe_ttm,pb'
                    )
                    
                    if not df.empty:
                        # 准备插入语句
                        insert_sql = """
                            INSERT INTO stock_market_index (
                                ts_code, trade_date, total_mv, float_mv, total_share,
                                float_share, free_share, turnover_rate, turnover_rate_f,
                                pe, pe_ttm, pb
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            ) ON DUPLICATE KEY UPDATE
                                total_mv = VALUES(total_mv),
                                float_mv = VALUES(float_mv),
                                total_share = VALUES(total_share),
                                float_share = VALUES(float_share),
                                free_share = VALUES(free_share),
                                turnover_rate = VALUES(turnover_rate),
                                turnover_rate_f = VALUES(turnover_rate_f),
                                pe = VALUES(pe),
                                pe_ttm = VALUES(pe_ttm),
                                pb = VALUES(pb)
                        """
                        
                        conn = self.get_db_connection()
                        cursor = conn.cursor()
                        
                        for _, row in df.iterrows():
                            values = (
                                row['ts_code'],
                                datetime.strptime(row['trade_date'], '%Y%m%d'),
                                float(row['total_mv']),
                                float(row['float_mv']),
                                float(row['total_share']),
                                float(row['float_share']),
                                float(row['free_share']),
                                float(row['turnover_rate']),
                                float(row['turnover_rate_f']),
                                float(row['pe']),
                                float(row['pe_ttm']),
                                float(row['pb'])
                            )
                            cursor.execute(insert_sql, values)
                        
                        conn.commit()
                        text_area.insert(tk.END, f"成功更新 {index_name} 的数据\n")
                    else:
                        text_area.insert(tk.END, f"未找到 {index_name} 的最新数据\n")
                    
                    current_progress += 1
                    progress_bar['value'] = current_progress
                    progress_label.config(text=f"正在更新... ({current_progress}/{total_indices})")
                    text_area.see(tk.END)
                    progress_window.update()
                    
                except Exception as e:
                    text_area.insert(tk.END, f"更新 {index_name} 失败: {str(e)}\n")
                    logging.error(f"更新 {index_name} 数据失败: {str(e)}")
                
                finally:
                    if 'cursor' in locals():
                        cursor.close()
                    if 'conn' in locals():
                        conn.close()
            
            # 添加确认按钮
            ttk.Button(
                progress_window,
                text="确定",
                command=progress_window.destroy,
                style="Primary.TButton",
                width=10
            ).pack(pady=10)
            
            # 如果正在查看某个指数的数据，刷新显示
            if hasattr(self, 'current_ts_code'):
                self.load_market_page_data()
            
        except Exception as e:
            logging.error(f"更新数据失败: {str(e)}")
            messagebox.showerror("错误", f"更新数据失败: {str(e)}")

    def check_auto_update(self):
        """检查是否需要启动自动更新"""
        now = datetime.now()
        current_time = now.time()
        
        # 只在工作日的收盘后（15:30-16:00）启动自动更新
        if (now.weekday() < 5 and 
            current_time.hour == 15 and 
            30 <= current_time.minute < 60 and 
            not self.auto_update_enabled):
            
            # 启动自动更新
            self.start_auto_update()
        
        # 每5分钟检查一次
        self.master.after(300000, self.check_auto_update)  # 300000毫秒 = 5分钟

    def start_auto_update(self):
        """启动自动更新"""
        if not self.auto_update_enabled:
            self.auto_update_enabled = True
            self.update_market_data()
            logging.info("启动自动更新")

    def stop_auto_update(self):
        """停止自动更新"""
        self.auto_update_enabled = False
        logging.info("停止自动更新")

    def show_index_data(self, index_name):
        """显示指定指数的数据"""
        try:
            # 获取指数代码
            ts_code = INDEX_CODES[index_name]
            
            # 保存当前查看的指数信息
            self.current_ts_code = ts_code
            self.current_index_name = index_name
            
            # 设置分页参数
            self.page_size = 20  # 每页显示记录数
            self.current_page = 1
            
            # 从数据库获取总记录数
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            count_query = """
                SELECT COUNT(*) as total
                FROM stock_market_index 
                WHERE ts_code = %s
            """
            cursor.execute(count_query, (ts_code,))
            total_records = cursor.fetchone()['total']
            
            # 计算总页数
            self.total_pages = (total_records + self.page_size - 1) // self.page_size
            
            # 更新标题显示当前选中的指数
            self.data_frame.configure(text=f"指标数据 - {index_name}")
            
            # 创建或更新分页控件
            self.create_or_update_pagination()
            
            # 加载第一页数据
            self.load_market_page_data()
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logging.error(f"获取指数数据失败: {str(e)}")
            messagebox.showerror("错误", f"获取数据失败: {str(e)}")

    def create_or_update_pagination(self):
        """创建或更新分页控件"""
        # 如果已存在分页框架，先移除
        if hasattr(self, 'pagination_frame'):
            self.pagination_frame.destroy()
        
        # 创建分页框架
        self.pagination_frame = ttk.Frame(self.data_frame)
        self.pagination_frame.pack(fill=tk.X, padx=10, pady=(0, 5))
        
        # 左侧按钮组
        left_buttons = ttk.Frame(self.pagination_frame)
        left_buttons.pack(side=tk.LEFT)
        
        ttk.Button(
            left_buttons,
            text="首页",
            command=lambda: self.change_page('first'),
            width=8
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            left_buttons,
            text="上一页",
            command=lambda: self.change_page('prev'),
            width=8
        ).pack(side=tk.LEFT, padx=2)
        
        self.page_label = ttk.Label(
            left_buttons,
            text=f"第 {self.current_page}/{self.total_pages} 页",
            font=("Microsoft YaHei UI", 9)
        )
        self.page_label.pack(side=tk.LEFT, padx=10)
        
        ttk.Button(
            left_buttons,
            text="下一页",
            command=lambda: self.change_page('next'),
            width=8
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            left_buttons,
            text="末页",
            command=lambda: self.change_page('last'),
            width=8
        ).pack(side=tk.LEFT, padx=2)
        
        # 右侧跳转控件
        right_controls = ttk.Frame(self.pagination_frame)
        right_controls.pack(side=tk.RIGHT)
        
        ttk.Label(
            right_controls,
            text="跳转到:",
            font=("Microsoft YaHei UI", 9)
        ).pack(side=tk.LEFT, padx=5)
        
        self.page_entry = ttk.Entry(right_controls, width=5)
        self.page_entry.pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            right_controls,
            text="跳转",
            command=self.jump_to_page,
            width=6
        ).pack(side=tk.LEFT, padx=2)
        
        # 显示总记录数
        self.total_label = ttk.Label(
            right_controls,
            text=f"共 {(self.total_pages-1)*self.page_size} 条记录",
            font=("Microsoft YaHei UI", 9)
        )
        self.total_label.pack(side=tk.LEFT, padx=10)

    def change_page(self, action):
        """切换页面"""
        if action == 'first':
            self.current_page = 1
        elif action == 'prev':
            self.current_page = max(1, self.current_page - 1)
        elif action == 'next':
            self.current_page = min(self.total_pages, self.current_page + 1)
        elif action == 'last':
            self.current_page = self.total_pages
        
        self.load_market_page_data()

    def jump_to_page(self):
        """跳转到指定页"""
        try:
            page = int(self.page_entry.get())
            if 1 <= page <= self.total_pages:
                self.current_page = page
                self.load_market_page_data()
            else:
                messagebox.showwarning("警告", f"页码必须在 1 到 {self.total_pages} 之间")
        except ValueError:
            messagebox.showwarning("警告", "请输入有效的页码")

    def load_market_page_data(self):
        """加载大盘指标分页数据"""
        try:
            # 检查界面元素是否存在
            if not hasattr(self, 'market_tree'):
                logging.error("界面元素不存在")
                return
            
            # 清空现有数据
            for item in self.market_tree.get_children():
                self.market_tree.delete(item)
            
            # 计算偏移量
            offset = (self.current_page - 1) * self.page_size
            
            # 从数据库获取数据
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = """
                SELECT * FROM stock_market_index 
                WHERE ts_code = %s 
                ORDER BY trade_date DESC
                LIMIT %s OFFSET %s
            """
            
            try:
                cursor.execute(query, (self.current_ts_code, self.page_size, offset))
                results = cursor.fetchall()
                
                # 插入数据
                for row in results:
                    values = (
                        row['trade_date'].strftime('%Y-%m-%d'),
                        f"{float(row['total_mv'])/100000000:.2f}",
                        f"{float(row['float_mv'])/100000000:.2f}",
                        f"{float(row['total_share'])/100000000:.2f}",
                        f"{float(row['float_share'])/100000000:.2f}",
                        f"{float(row['free_share'])/100000000:.2f}",
                        f"{float(row['turnover_rate']):.2f}%",
                        f"{float(row['turnover_rate_f']):.2f}%",
                        f"{float(row['pe']):.2f}",
                        f"{float(row['pe_ttm']):.2f}",
                        f"{float(row['pb']):.2f}"
                    )
                    
                    # 检查界面元素是否仍然存在
                    if hasattr(self, 'market_tree'):
                        self.market_tree.insert("", "end", values=values)
                
                # 更新页码显示
                if hasattr(self, 'page_label'):
                    self.page_label.config(text=f"第 {self.current_page}/{self.total_pages} 页")
                
            except Exception as e:
                logging.error(f"执行查询失败: {str(e)}")
                raise
                
            finally:
                cursor.close()
                conn.close()
            
        except Exception as e:
            logging.error(f"加载页面数据失败: {str(e)}")
            if hasattr(self, 'master'):
                messagebox.showerror("错误", f"加载数据失败: {str(e)}")

    def show_index_quotes(self):
        """显示指数行情页面"""
        # 清除当前内容
        for widget in self.content_frame.winfo_children():
            widget.destroy()
        
        # 更新按钮状态
        self.update_button_state(self.index_btn)
        
        # 创建数据展示区域的容器
        self.quote_container = ttk.Frame(self.content_frame)
        self.quote_container.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 创建指数选择区域
        select_frame = ttk.LabelFrame(self.quote_container, text="指数选择", padding="10")
        select_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 创建按钮容器
        button_container = ttk.Frame(select_frame)
        button_container.pack(fill=tk.X, padx=5, pady=5)
        
        # 创建指数按钮
        for index_name, ts_code in INDEX_CODES.items():
            ttk.Button(
                button_container,
                text=index_name,
                command=lambda code=ts_code, name=index_name: self.show_index_quote_data(code, name),
                style="Index.TButton",
                width=15
            ).pack(side=tk.LEFT, padx=5)
        
        # 创建按钮区域
        button_frame = ttk.Frame(select_frame)
        button_frame.pack(side=tk.RIGHT, padx=5)
        
        # 添加历史数据按钮
        ttk.Button(
            button_frame,
            text="获取历史数据",
            command=self.show_daily_date_picker,
            style="Primary.TButton",
            width=15
        ).pack(side=tk.RIGHT, padx=5)
        
        # 添加更新按钮
        ttk.Button(
            button_frame,
            text="更新数据",
            command=self.update_quote_data,
            style="Primary.TButton",
            width=15
        ).pack(side=tk.RIGHT, padx=5)
        
        # 创建数据表格区域
        self.quote_frame = ttk.LabelFrame(self.quote_container, text="指数行情数据", padding="10")
        self.quote_frame.pack(fill="both", expand=True)
        
        # 创建表格容器
        table_container = ttk.Frame(self.quote_frame)
        table_container.pack(fill="both", expand=True, pady=(0, 10))
        
        # 创建表格
        columns = ("交易日期", "开盘价", "收盘价", "最高价", "最低价", 
                  "昨收价", "涨跌额", "涨跌幅", "成交量(万)", "成交额(万)")
        
        self.quote_tree = ttk.Treeview(
            table_container,
            columns=columns,
            show='headings',
            height=15,
            style="Custom.Treeview"
        )
        
        # 设置列宽和对齐方式
        for col in columns:
            self.quote_tree.heading(col, text=col)
            self.quote_tree.column(col, width=100, anchor='center')
        
        # 添加垂直滚动条
        vsb = ttk.Scrollbar(table_container, orient="vertical", command=self.quote_tree.yview)
        self.quote_tree.configure(yscrollcommand=vsb.set)
        
        # 添加水平滚动条
        hsb = ttk.Scrollbar(table_container, orient="horizontal", command=self.quote_tree.xview)
        self.quote_tree.configure(xscrollcommand=hsb.set)
        
        # 布局表格和滚动条
        self.quote_tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        
        # 配置表格容器的网格权重
        table_container.grid_columnconfigure(0, weight=1)
        table_container.grid_rowconfigure(0, weight=1)
        
        # 设置颜色标签
        self.quote_tree.tag_configure('up', foreground='#ff4d4f')
        self.quote_tree.tag_configure('down', foreground='#52c41a')
        self.quote_tree.tag_configure('zero', foreground='#666666')

    def fetch_and_store_data(self, ts_code):
        """获取存储数据"""
        try:
            # 使用频率限制器
            rate_limiter.wait()
            
            # 获取数据
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = '20230920'  # 或其他起始日期
            
            df = pro.index_daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if not df.empty:
                # 连接数据库
                conn = mysql.connector.connect(**MYSQL_CONFIG)
                cursor = conn.cursor()
                
                # 准备插入语句
                insert_sql = """
                    INSERT INTO stock_index_daily (
                        ts_code, trade_date, `close`, `open`, high, low,
                        pre_close, `change`, pct_chg, vol, amount
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON DUPLICATE KEY UPDATE
                        `close` = VALUES(`close`),
                        `open` = VALUES(`open`),
                        high = VALUES(high),
                        low = VALUES(low),
                        pre_close = VALUES(pre_close),
                        `change` = VALUES(`change`),
                        pct_chg = VALUES(pct_chg),
                        vol = VALUES(vol),
                        amount = VALUES(amount)
                """
                
                # 插入数据
                values_list = []
                for _, row in df.iterrows():
                    values = (
                        row['ts_code'],
                        datetime.strptime(row['trade_date'], '%Y%m%d'),
                        float(row['close']),
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['pre_close']),
                        float(row['change']),
                        float(row['pct_chg']),
                        float(row['vol']),
                        float(row['amount'])
                    )
                    values_list.append(values)
                
                cursor.executemany(insert_sql, values_list)
                conn.commit()
                cursor.close()
                conn.close()
                
        except Exception as e:
            logging.error(f"获取并存储数据失败: {str(e)}")
            raise

    def store_historical_data(self, start_date=None, end_date=None, index_name=None):
        """存储历史指标数据"""
        try:
            # 显示加载提示
            self.show_loading_message()
            
            # 创建进度窗口
            progress_window = tk.Toplevel(self.master)
            progress_window.title("数据获取进度")
            progress_window.geometry("500x400")
            progress_window.transient(self.master)
            
            # 进度标签
            progress_label = ttk.Label(
                progress_window,
                text="正在获取数据...",
                font=("Microsoft YaHei UI", 10)
            )
            progress_label.pack(pady=10)
            
            # 进度条
            progress_bar = ttk.Progressbar(
                progress_window,
                length=400,
                mode='determinate'
            )
            progress_bar.pack(pady=10)
            
            # 详细信息文本框
            text_area = tk.Text(
                progress_window,
                height=15,
                width=50,
                font=("Microsoft YaHei UI", 9)
            )
            text_area.pack(pady=10, padx=10)
            
            # 添加滚动条
            scrollbar = ttk.Scrollbar(progress_window)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            text_area.config(yscrollcommand=scrollbar.set)
            scrollbar.config(command=text_area.yview)
            
            # 要处理的指数列表
            indices = [(name, code) for name, code in INDEX_CODES.items()]
            if index_name:
                indices = [(name, code) for name, code in indices if name == index_name]
            
            total_steps = len(indices)
            current_step = 0
            success_count = 0
            failed_records = []
            
            # 更新进度条配置
            progress_bar['maximum'] = total_steps
            
            # 处理每个指数
            for idx_name, ts_code in indices:
                try:
                    current_step += 1
                    progress = (current_step / total_steps) * 100
                    progress_bar['value'] = progress
                    
                    msg = f"正在处理: {idx_name} ({current_step}/{total_steps})\n"
                    text_area.insert(tk.END, msg)
                    text_area.see(tk.END)
                    progress_window.update()
                    
                    # 使用频率限制器
                    rate_limiter.wait()
                    
                    # 获取历史数据
                    df = pro.index_dailybasic(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if not df.empty:
                        # 连接数据库
                        conn = mysql.connector.connect(**MYSQL_CONFIG)
                        cursor = conn.cursor()
                        
                        # 准备插入语句
                        insert_sql = """
                            INSERT INTO stock_market_index (
                                ts_code, trade_date, total_mv, float_mv, 
                                total_share, float_share, free_share,
                                turnover_rate, turnover_rate_f, pe, pe_ttm, pb
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            ) ON DUPLICATE KEY UPDATE
                                total_mv = VALUES(total_mv),
                                float_mv = VALUES(float_mv),
                                total_share = VALUES(total_share),
                                float_share = VALUES(float_share),
                                free_share = VALUES(free_share),
                                turnover_rate = VALUES(turnover_rate),
                                turnover_rate_f = VALUES(turnover_rate_f),
                                pe = VALUES(pe),
                                pe_ttm = VALUES(pe_ttm),
                                pb = VALUES(pb)
                        """
                        
                        # 插入数据
                        values_list = []
                        for _, row in df.iterrows():
                            values = (
                                row['ts_code'],
                                datetime.strptime(row['trade_date'], '%Y%m%d'),
                                str(row['total_mv']),
                                str(row['float_mv']),
                                str(row['total_share']),
                                str(row['float_share']),
                                str(row['free_share']),
                                str(row['turnover_rate']),
                                str(row['turnover_rate_f']),
                                str(row['pe']),
                                str(row['pe_ttm']),
                                str(row['pb'])
                            )
                            values_list.append(values)
                        
                        cursor.executemany(insert_sql, values_list)
                        conn.commit()
                        
                        success_count += len(values_list)
                        text_area.insert(tk.END, f"成功存储 {len(values_list)} 条记录\n")
                        
                        cursor.close()
                        conn.close()
                    else:
                        text_area.insert(tk.END, f"未找到 {idx_name} 的历史数据\n")
                        failed_records.append((idx_name, "无数据"))
                    
                except Exception as e:
                    error_msg = str(e)
                    logging.error(f"处理 {idx_name} 数据失败: {error_msg}")
                    text_area.insert(tk.END, f"处理失败: {error_msg}\n")
                    failed_records.append((idx_name, error_msg))
                
                text_area.see(tk.END)
            
            # 显示结果
            result_message = f"\n数据获取完成！\n\n"
            result_message += f"成功存储: {success_count} 条记录\n"
            if failed_records:
                result_message += f"\n处理失败的指数:\n"
                for idx_name, reason in failed_records:
                    result_message += f"{idx_name}: {reason}\n"
            
            text_area.insert(tk.END, result_message)
            text_area.see(tk.END)
            
            # 添加确认按钮
            ttk.Button(
                progress_window,
                text="确定",
                command=progress_window.destroy,
                style="Primary.TButton",
                width=15
            ).pack(pady=10)
            
            # 如果正在查看某个指数的数据，刷新显示
            if hasattr(self, 'current_ts_code'):
                self.load_market_page_data()
            
        except Exception as e:
            logging.error(f"获取历史数据失败: {str(e)}")
            messagebox.showerror("错误", f"获取历史数据失败: {str(e)}")
        finally:
            # 移除加载提示
            self.remove_loading_message()

    def show_daily_date_picker(self):
        """显示日期选择对话框"""
        try:
            # 创建日期选择窗口
            date_window = tk.Toplevel(self.master)
            date_window.title("选择日期范围")
            date_window.geometry("400x250")
            date_window.transient(self.master)
            
            # 创建日期选择框架
            frame = ttk.Frame(date_window, padding="20")
            frame.pack(fill=tk.BOTH, expand=True)
            
            # 获取数据库中的最小日期
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # 根据当前页面选择正确的表
            if hasattr(self, 'quote_frame'):
                table_name = 'stock_index_daily'
            else:
                table_name = 'stock_market_index'
                
            cursor.execute(f"""
                SELECT MIN(trade_date) as min_date 
                FROM {table_name}
            """)
            min_date = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            # 设置日期范围
            if min_date:
                end_date = min_date  # 终止期为数据表小日期
                start_date = end_date - timedelta(days=30)  # 起始日期为最小日期前一个月
            else:
                # 如果数据表为空，使用默认日期
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
            
            # 起始日期
            ttk.Label(frame, text="起始日期:", font=("Microsoft YaHei UI", 10)).grid(row=0, column=0, padx=5, pady=10, sticky="e")
            start_entry = ttk.Entry(frame, width=15, font=("Microsoft YaHei UI", 10))
            start_entry.grid(row=0, column=1, padx=5, pady=10)
            start_entry.insert(0, start_date.strftime('%Y-%m-%d'))
            
            # 结束日期
            ttk.Label(frame, text="结束日期:", font=("Microsoft YaHei UI", 10)).grid(row=1, column=0, padx=5, pady=10, sticky="e")
            end_entry = ttk.Entry(frame, width=15, font=("Microsoft YaHei UI", 10))
            end_entry.grid(row=1, column=1, padx=5, pady=10)
            end_entry.insert(0, end_date.strftime('%Y-%m-%d'))
            
            # 添加日期说明
            if min_date:
                ttk.Label(
                    frame,
                    text=f"数据表中最早日期: {min_date.strftime('%Y-%m-%d')}",
                    font=("Microsoft YaHei UI", 9),
                    foreground="#666666"
                ).grid(row=2, column=0, columnspan=2, pady=5)
            
            # 日期格式提示
            ttk.Label(
                frame,
                text="格式: YYYY-MM-DD",
                font=("Microsoft YaHei UI", 9),
                foreground="#666666"
            ).grid(row=3, column=0, columnspan=2, pady=5)
            
            def validate_date(date_str):
                """验证日期格式"""
                try:
                    datetime.strptime(date_str, '%Y-%m-%d')
                    return True
                except ValueError:
                    return False
            
            def confirm():
                """确认日期选择"""
                start_date = start_entry.get().strip()
                end_date = end_entry.get().strip()
                
                if not validate_date(start_date) or not validate_date(end_date):
                    messagebox.showerror("错误", "请输入正确的日期格式：YYYY-MM-DD")
                    return
                
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                
                if start_dt > end_dt:
                    messagebox.showerror("错误", "起始日期不能大于束日期")
                    return
                
                if min_date and end_dt > min_date:
                    messagebox.showerror("错误", f"结束日期不能晚于数据表中的最早日期: {min_date.strftime('%Y-%m-%d')}")
                    return
                
                date_window.destroy()
                # 调用数据获取函数
                self.store_historical_data(
                    start_date=start_date.replace('-', ''),
                    end_date=end_date.replace('-', '')
                )
            
            # 按钮区域
            button_frame = ttk.Frame(frame)
            button_frame.grid(row=4, column=0, columnspan=2, pady=20)
            
            ttk.Button(
                button_frame,
                text="确定",
                command=confirm,
                style="Primary.TButton",
                width=10
            ).pack(side=tk.LEFT, padx=5)
            
            ttk.Button(
                button_frame,
                text="取消",
                command=date_window.destroy,
                width=10
            ).pack(side=tk.LEFT, padx=5)
            
        except Exception as e:
            logging.error(f"创建日期选择窗口失败: {str(e)}")
            messagebox.showerror("错误", f"创建窗口失败: {str(e)}")

    def show_loading_message(self):
        """显示加载提示"""
        try:
            # 创建加载提示框架
            if hasattr(self, 'content_frame'):
                parent = self.content_frame
                
                # 创建加载提示框架
                self.loading_frame = ttk.Frame(parent)
                self.loading_frame.place(relx=0.5, rely=0.5, anchor="center")
                
                # 添加加载动画
                self.loading_label = ttk.Label(
                    self.loading_frame,
                    text="数据加载中...",
                    font=("Microsoft YaHei UI", 12)
                )
                self.loading_label.pack(pady=10)
                
                # 添加进度条
                self.progress = ttk.Progressbar(
                    self.loading_frame,
                    mode='indeterminate',
                    length=200
                )
                self.progress.pack(pady=5)
                self.progress.start(10)
                
                # 更新界面
                self.master.update()
                
        except Exception as e:
            logging.error(f"创建加载提示失败: {str(e)}")

    def remove_loading_message(self):
        """移除加载提示"""
        try:
            if hasattr(self, 'loading_frame'):
                if hasattr(self, 'progress'):
                    self.progress.stop()
                self.loading_frame.destroy()
                delattr(self, 'loading_frame')
                
        except Exception as e:
            logging.error(f"移除加载提示失败: {str(e)}")

    def __del__(self):
        """析构函数，确保资源释放"""
        try:
            if hasattr(self, 'db_pool'):
                # 关闭所有连接
                for conn in self.db_pool._cnx_queue:
                    if conn.is_connected():
                        conn.close()
        except Exception as e:
            logging.error(f"关闭数据库连接池失败: {str(e)}")

    def update_quote_data(self):
        """更新指数行情数据"""
        try:
            # 显示加载提示
            self.show_loading_message()
            
            # 获取当前最新交易日期
            latest_trade_date = self.get_latest_trade_date()
            
            # 从数据库获取最新日期
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT MAX(trade_date) as max_date
                FROM stock_index_daily
            """)
            db_latest_date = cursor.fetchone()[0]
            
            if db_latest_date:
                db_latest_str = db_latest_date.strftime('%Y%m%d')
                if db_latest_str >= latest_trade_date:
                    messagebox.showinfo("提示", "数据已是最新！")
                    return
            
            # 创建进度窗口
            progress_window = tk.Toplevel(self.master)
            progress_window.title("数据更新进度")
            progress_window.geometry("400x300")
            progress_window.transient(self.master)
            
            # 进度标签
            progress_label = ttk.Label(
                progress_window,
                text="正在更新数据...",
                font=("Microsoft YaHei UI", 10)
            )
            progress_label.pack(pady=10)
            
            # 进度条
            progress_bar = ttk.Progressbar(
                progress_window,
                length=300,
                mode='determinate'
            )
            progress_bar.pack(pady=10)
            
            # 详细信息文本框
            text_area = tk.Text(
                progress_window,
                height=10,
                width=40,
                font=("Microsoft YaHei UI", 9)
            )
            text_area.pack(pady=10, padx=10)
            
            # 添加滚动条
            scrollbar = ttk.Scrollbar(progress_window)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            text_area.config(yscrollcommand=scrollbar.set)
            scrollbar.config(command=text_area.yview)
            
            # 准备更新的指数列表
            total_indices = len(INDEX_CODES)
            progress_bar['maximum'] = total_indices
            current_progress = 0
            
            # 更新每个指数的数据
            for index_name, ts_code in INDEX_CODES.items():
                try:
                    # 使用频率限制器
                    rate_limiter.wait()
                    
                    # 获取数据
                    df = pro.index_daily(
                        ts_code=ts_code,
                        trade_date=latest_trade_date
                    )
                    
                    if not df.empty:
                        # 准备插入语句
                        insert_sql = """
                            INSERT INTO stock_index_daily (
                                ts_code, trade_date, `close`, `open`, high, low,
                                pre_close, `change`, pct_chg, vol, amount
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            ) ON DUPLICATE KEY UPDATE
                                `close` = VALUES(`close`),
                                `open` = VALUES(`open`),
                                high = VALUES(high),
                                low = VALUES(low),
                                pre_close = VALUES(pre_close),
                                `change` = VALUES(`change`),
                                pct_chg = VALUES(pct_chg),
                                vol = VALUES(vol),
                                amount = VALUES(amount)
                        """
                        
                        for _, row in df.iterrows():
                            values = (
                                row['ts_code'],
                                datetime.strptime(row['trade_date'], '%Y%m%d'),
                                float(row['close']),
                                float(row['open']),
                                float(row['high']),
                                float(row['low']),
                                float(row['pre_close']),
                                float(row['change']),
                                float(row['pct_chg']),
                                float(row['vol']),
                                float(row['amount'])
                            )
                            cursor.execute(insert_sql, values)
                        
                        conn.commit()
                        text_area.insert(tk.END, f"成功更新 {index_name} 的数据\n")
                    else:
                        text_area.insert(tk.END, f"未找到 {index_name} 的最新数据\n")
                    
                    current_progress += 1
                    progress_bar['value'] = current_progress
                    progress_label.config(text=f"正在更新... ({current_progress}/{total_indices})")
                    text_area.see(tk.END)
                    progress_window.update()
                    
                except Exception as e:
                    text_area.insert(tk.END, f"更新 {index_name} 失败: {str(e)}\n")
                    logging.error(f"更新 {index_name} 数据失败: {str(e)}")
            
            cursor.close()
            conn.close()
            
            # 添加确认按钮
            ttk.Button(
                progress_window,
                text="确定",
                command=progress_window.destroy,
                style="Primary.TButton",
                width=10
            ).pack(pady=10)
            
            # 如果正在查看某个数的数据，刷新显示
            if hasattr(self, 'current_quote_code'):
                self.load_quote_page_data()
            
        except Exception as e:
            logging.error(f"更新数据失败: {str(e)}")
            messagebox.showerror("错误", f"更新数据失败: {str(e)}")
        finally:
            self.remove_loading_message()

    def auto_update_quote_data(self):
        """自动更新指数行情数据（收盘后）"""
        now = datetime.now()
        current_time = now.time()
        
        # 如果是工作日且在收盘后（15:30后）
        if now.weekday() < 5 and current_time.hour >= 15 and current_time.minute >= 30:
            self.update_quote_data()
        
        # 每小时检查一次
        self.master.after(3600000, self.auto_update_quote_data)  # 3600000毫秒 = 1小时

    def show_index_quote_data(self, ts_code, index_name):
        """显示指数行情数据"""
        try:
            # 清空现有数据
            for item in self.quote_tree.get_children():
                self.quote_tree.delete(item)
            
            # 保存当前查看的指数信息
            self.current_quote_code = ts_code
            self.current_quote_name = index_name
            
            # 设置分页参数
            self.quote_page_size = 20  # 每页显示记录数
            self.quote_current_page = 1
            
            # 从数据库获取总记录数
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            count_query = """
                SELECT COUNT(*) as total
                FROM stock_index_daily 
                WHERE ts_code = %s
            """
            cursor.execute(count_query, (ts_code,))
            total_records = cursor.fetchone()['total']
            
            # 计算总页数
            self.quote_total_pages = (total_records + self.quote_page_size - 1) // self.quote_page_size
            
            # 更新标题显示当前选中的指数
            self.quote_frame.configure(text=f"指数行情数据 - {index_name}")
            
            # 创建或更新分页控件
            self.create_or_update_quote_pagination()
            
            # 加载第一页数据
            self.load_quote_page_data()
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logging.error(f"获取指数行情数据失败: {str(e)}")
            messagebox.showerror("错误", f"获取数据失败: {str(e)}")

    def create_or_update_quote_pagination(self):
        """创建或更新指数行情分页控件"""
        # 如已存在分页框架，先移除
        if hasattr(self, 'quote_pagination'):
            self.quote_pagination.destroy()
        
        # 创建分页框架
        self.quote_pagination = ttk.Frame(self.quote_frame)
        self.quote_pagination.pack(fill=tk.X, padx=10, pady=(0, 5))
        
        # 左侧按钮组
        left_buttons = ttk.Frame(self.quote_pagination)
        left_buttons.pack(side=tk.LEFT)
        
        ttk.Button(
            left_buttons,
            text="首页",
            command=lambda: self.change_quote_page('first'),
            width=8
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            left_buttons,
            text="上一页",
            command=lambda: self.change_quote_page('prev'),
            width=8
        ).pack(side=tk.LEFT, padx=2)
        
        self.quote_page_label = ttk.Label(
            left_buttons,
            text=f"第 {self.quote_current_page}/{self.quote_total_pages} 页",
            font=("Microsoft YaHei UI", 9)
        )
        self.quote_page_label.pack(side=tk.LEFT, padx=10)
        
        ttk.Button(
            left_buttons,
            text="下一页",
            command=lambda: self.change_quote_page('next'),
            width=8
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            left_buttons,
            text="末页",
            command=lambda: self.change_quote_page('last'),
            width=8
        ).pack(side=tk.LEFT, padx=2)
        
        # 右侧跳转控件
        right_controls = ttk.Frame(self.quote_pagination)
        right_controls.pack(side=tk.RIGHT)
        
        ttk.Label(
            right_controls,
            text="跳转到:",
            font=("Microsoft YaHei UI", 9)
        ).pack(side=tk.LEFT, padx=5)
        
        self.quote_page_entry = ttk.Entry(right_controls, width=5)
        self.quote_page_entry.pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            right_controls,
            text="跳转",
            command=self.jump_to_quote_page,
            width=6
        ).pack(side=tk.LEFT, padx=2)
        
        # 显示总记录数
        self.quote_total_label = ttk.Label(
            right_controls,
            text=f"共 {(self.quote_total_pages-1)*self.quote_page_size} 条记录",
            font=("Microsoft YaHei UI", 9)
        )
        self.quote_total_label.pack(side=tk.LEFT, padx=10)

    def load_quote_page_data(self):
        """加载指数行情分页数据"""
        try:
            # 清空现有数据
            for item in self.quote_tree.get_children():
                self.quote_tree.delete(item)
            
            # 计算偏移量
            offset = (self.quote_current_page - 1) * self.quote_page_size
            
            # 从数据库获取数据
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = """
                SELECT * FROM stock_index_daily 
                WHERE ts_code = %s 
                ORDER BY trade_date DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (self.current_quote_code, self.quote_page_size, offset))
            results = cursor.fetchall()
            
            # 插入数据
            for row in results:
                # 确定颜色标签
                tag = 'up' if row['pct_chg'] > 0 else 'down' if row['pct_chg'] < 0 else 'zero'
                
                # 插入数据
                self.quote_tree.insert("", tk.END, values=(
                    row['trade_date'].strftime('%Y-%m-%d'),
                    f"{row['open']:.2f}",
                    f"{row['close']:.2f}",
                    f"{row['high']:.2f}",
                    f"{row['low']:.2f}",
                    f"{row['pre_close']:.2f}",
                    f"{row['change']:+.2f}",
                    f"{row['pct_chg']:+.2f}%",
                    f"{row['vol']/10000:.2f}",
                    f"{row['amount']/10000:.2f}"
                ), tags=(tag,))
            
            # 更新页码显示
            self.quote_page_label.config(text=f"第 {self.quote_current_page}/{self.quote_total_pages} 页")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logging.error(f"加载页面数据失败: {str(e)}")
            messagebox.showerror("错误", f"加载数据失败: {str(e)}")

    def change_quote_page(self, action):
        """切换指数行情页面"""
        if action == 'first':
            self.quote_current_page = 1
        elif action == 'prev':
            self.quote_current_page = max(1, self.quote_current_page - 1)
        elif action == 'next':
            self.quote_current_page = min(self.quote_total_pages, self.quote_current_page + 1)
        elif action == 'last':
            self.quote_current_page = self.quote_total_pages
        
        self.load_quote_page_data()

    def jump_to_quote_page(self):
        """跳转到指定的指数行情页"""
        try:
            page = int(self.quote_page_entry.get())
            if 1 <= page <= self.quote_total_pages:
                self.quote_current_page = page
                self.load_quote_page_data()
            else:
                messagebox.showwarning("警告", f"页码必须在 1 到 {self.quote_total_pages} 之间")
        except ValueError:
            messagebox.showwarning("警告", "请输入有效的页码")