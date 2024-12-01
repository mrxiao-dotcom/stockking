import tkinter as tk
from tkinter import ttk, messagebox
import tushare as ts
from config import TUSHARE_TOKEN
from db_operations import DatabaseOperations
import pandas as pd
from datetime import datetime, timedelta
import threading
import logging
import time
from threading import Lock

# 配置tushare
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

# 添加频率限制器
class RateLimiter:
    def __init__(self, calls_per_minute):
        self.calls_per_minute = calls_per_minute
        self.interval = 60.0 / calls_per_minute
        self.last_call = 0
        self.lock = Lock()
        self.call_count = 0
        self.last_reset = time.time()

    def wait(self):
        with self.lock:
            current_time = time.time()

            # 检查是否需要重置计数器（每分钟重置一次）
            if current_time - self.last_reset >= 60:
                self.call_count = 0
                self.last_reset = current_time
                logging.info("API调用计数器已重置")

            # 检查是否达到限制
            if self.call_count >= self.calls_per_minute:
                sleep_time = 60 - (current_time - self.last_reset)
                if sleep_time > 0:
                    logging.info(f"达到API调用限制，等待 {sleep_time:.2f} 秒")
                    time.sleep(sleep_time)
                    self.call_count = 0
                    self.last_reset = time.time()

            # 计算需要等待的时间
            elapsed = current_time - self.last_call
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

            self.last_call = time.time()
            self.call_count += 1
            logging.info(f"当前分钟API调用次数: {self.call_count}/{self.calls_per_minute}")

# 创建频率限制器，设置为每分钟170次（留出余量）
rate_limiter = RateLimiter(calls_per_minute=170)

class StockFundFlow:
    def __init__(self, master=None):
        if master is None:
            master = tk.Tk()
        self.master = master
        
        # 初始化数据库连接
        self.db = DatabaseOperations()
        self.db.connect()
        
        # 创建主框架
        self.main_frame = ttk.Frame(self.master)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # 创建左右分割框架
        self.left_frame = ttk.Frame(self.main_frame)
        self.left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))
        
        self.right_frame = ttk.Frame(self.main_frame)
        self.right_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # 创建板块列表区域
        self.create_sector_area()
        
        # 创建个股资金流向区域
        self.create_fund_flow_area()

    def create_sector_area(self):
        """创建板块列表区域"""
        # 板块列表框架
        sector_frame = ttk.LabelFrame(self.left_frame, text="板块列表", padding="10")
        sector_frame.pack(fill=tk.BOTH, expand=True)
        
        # 创建板块列表
        self.sector_listbox = tk.Listbox(
            sector_frame,
            width=40,
            font=("Microsoft YaHei UI", 10),
            selectmode=tk.SINGLE
        )
        self.sector_listbox.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # 添加滚动条
        scrollbar = ttk.Scrollbar(sector_frame, orient=tk.VERTICAL)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.sector_listbox.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.sector_listbox.yview)
        
        # 加载板块数据
        self.load_sectors()
        
        # 绑定选择事件
        self.sector_listbox.bind('<<ListboxSelect>>', self.on_sector_select)

    def create_fund_flow_area(self):
        """创建资金流向展示区域"""
        # 搜索条件区域 - 美化
        search_frame = ttk.LabelFrame(self.right_frame, text="搜索条件", padding="15")
        search_frame.pack(fill=tk.X, pady=(0, 15))
        
        # 创建搜索条件的子框架
        search_content = ttk.Frame(search_frame)
        search_content.pack(fill=tk.X, padx=10, pady=5)
        
        # 日期选择
        date_frame = ttk.Frame(search_content)
        date_frame.pack(side=tk.LEFT)
        
        ttk.Label(date_frame, 
                 text="起始日期:", 
                 font=("Microsoft YaHei UI", 9)).pack(side=tk.LEFT, padx=5)
        
        self.date_entry = ttk.Entry(date_frame, 
                                   width=15, 
                                   font=("Microsoft YaHei UI", 9))
        self.date_entry.pack(side=tk.LEFT, padx=5)
        self.date_entry.insert(0, (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        
        # 日期格式提示
        ttk.Label(date_frame,
                 text="(YYYY-MM-DD)",
                 font=("Microsoft YaHei UI", 8),
                 foreground="#666666").pack(side=tk.LEFT, padx=5)
        
        # 查询按
        self.search_btn = ttk.Button(
            search_content,
            text="查询",
            command=self.search_fund_flow,
            style="Primary.TButton",
            width=10
        )
        self.search_btn.pack(side=tk.LEFT, padx=15)
        
        # 个股列表区域 - 美化
        stock_frame = ttk.LabelFrame(self.right_frame, text="板块个股列表", padding="15")
        stock_frame.pack(fill=tk.X, pady=(0, 15))
        
        # 创建个股列表
        self.stock_listbox = tk.Listbox(
            stock_frame,
            height=6,
            width=50,  # 增加宽度以容纳代码和名称
            font=("Microsoft YaHei UI", 9),
            selectmode=tk.SINGLE,
            activestyle='dotbox'
        )
        self.stock_listbox.pack(fill=tk.X, expand=True, pady=5)
        
        # 添加滚动条
        stock_scrollbar = ttk.Scrollbar(stock_frame)
        stock_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.stock_listbox.config(yscrollcommand=stock_scrollbar.set)
        stock_scrollbar.config(command=self.stock_listbox.yview)
        
        # 绑定选择事件
        self.stock_listbox.bind('<<ListboxSelect>>', self.on_stock_select)
        
        # 资金流向数据展示区域 - 美化
        data_frame = ttk.LabelFrame(self.right_frame, text="资金流向数据", padding="15")
        data_frame.pack(fill=tk.BOTH, expand=True)

        # 添加按钮区域
        button_frame = ttk.Frame(data_frame)
        button_frame.pack(fill=tk.X, pady=(0, 10))

        # 添加查看板块资金流向按钮
        self.sector_flow_btn = ttk.Button(
            button_frame,
            text="查看板块资金流向",
            command=self.show_sector_fund_flow,
            style="Primary.TButton",
            width=20
        )
        self.sector_flow_btn.pack(side=tk.LEFT, padx=5)
        self.sector_flow_btn['state'] = 'disabled'  # 初始状态为禁用

        # 修改表格列定义
        columns = ("股票代码", "股票名称", "交易日期", "收盘价", "涨跌幅", "主力净流入", "小单净流入", "中单净流入", "大单净流入", "超大单净流入")
        self.tree = ttk.Treeview(
            data_frame,
            columns=columns,
            show='headings',
            height=10,
            style="Custom.Treeview"
        )
        
        # 设置列标题和宽度
        column_widths = {
            "股票代码": 100,
            "股票名称": 100,
            "交易日期": 100,  # 添加日期列宽度
            "收盘价": 80,
            "涨跌幅": 80,
            "主力净流入": 120,
            "小单净流入": 120,
            "中单净流入": 120,
            "大单净流入": 120,
            "超大单净流入": 120
        }
        
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=column_widths[col], anchor='center')
        
        # 添加表格样式
        style = ttk.Style()
        style.configure("Custom.Treeview",
                       font=("Microsoft YaHei UI", 11),
                       rowheight=35)
        style.configure("Custom.Treeview.Heading",
                       font=("Microsoft YaHei UI", 11, "bold"))
        
        # 添加颜色标签
        self.tree.tag_configure("positive", foreground="#ff4d4f")
        self.tree.tag_configure("negative", foreground="#52c41a")
        self.tree.tag_configure("zero", foreground="#666666")
        
        # 添加滚动条
        tree_scrollbar = ttk.Scrollbar(data_frame, orient=tk.VERTICAL)
        self.tree.configure(yscrollcommand=tree_scrollbar.set)
        
        # 放置表格和滚动条
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, pady=10)
        tree_scrollbar.pack(side=tk.RIGHT, fill=tk.Y, pady=10)
        
        # 添加按钮样式
        style.configure("Primary.TButton",
                       font=("Microsoft YaHei UI", 9),
                       padding=5)
        style.map("Primary.TButton",
                  background=[("active", "#40a9ff")],
                  foreground=[("active", "white")])

    def load_sectors(self):
        """加载板块数据"""
        sectors = self.db.get_all_sectors()
        for sector in sectors:
            sector_info = f"{sector['sector_id']} - {sector['sector_name']} ({sector['sector_type']})"
            self.sector_listbox.insert(tk.END, sector_info)

    def on_sector_select(self, event):
        """处理板块选择事件"""
        selection = self.sector_listbox.curselection()
        if not selection:
            return
        
        # 获取选中的板块ID
        sector_text = self.sector_listbox.get(selection[0])
        sector_id = int(sector_text.split(' - ')[0])
        
        # 清空个股列表
        self.stock_listbox.delete(0, tk.END)
        
        # 加载板块内的个股
        stocks = self.db.get_sector_stocks(sector_id)
        for stock in stocks:
            # 获取股票名称
            stock_name = self.db.get_stock_name(stock['stock_code'])
            # 显示代码和名称
            display_text = f"{stock['stock_code']} - {stock_name}"
            self.stock_listbox.insert(tk.END, display_text)
        
        # 启用板块资金流向按钮
        self.sector_flow_btn['state'] = 'normal'

    def on_stock_select(self, event):
        """处理个股选择事件"""
        selection = self.stock_listbox.curselection()
        if not selection:
            return
        
        # 从显示文本中提取股票代码
        stock_text = self.stock_listbox.get(selection[0])
        stock_code = stock_text.split(' - ')[0]  # 获取代码部分
        
        # 自动触发资金流向查询
        self.search_fund_flow(stock_code)

    def search_fund_flow(self, stock_code=None):
        """查询资金流向数据"""
        if stock_code is None:
            selection = self.stock_listbox.curselection()
            if not selection:
                messagebox.showwarning("警告", "请先选择股票！")
                return
            stock_code = self.stock_listbox.get(selection[0])
        
        start_date = self.date_entry.get().strip()
        
        # 禁用查询按钮
        self.search_btn['state'] = 'disabled'
        
        # 清空现有数据
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        def fetch_data():
            try:
                # 转换日期格式
                start_date_str = start_date.replace('-', '')
                end_date = datetime.now().strftime('%Y%m%d')
                
                # 使用频率限制器
                rate_limiter.wait()
                
                # 获取资金流向数据和日线数据
                df_flow = pro.moneyflow(
                    ts_code=stock_code,
                    start_date=start_date_str,
                    end_date=end_date
                )
                
                # 获取日线数据
                df_daily = pro.daily(
                    ts_code=stock_code,
                    start_date=start_date_str,
                    end_date=end_date
                )
                
                if df_flow.empty:
                    self.master.after(0, lambda: messagebox.showinfo("提示", "未找到数据！"))
                    return
                
                # 合并资金流向数据和日线数据
                df = pd.merge(df_flow, df_daily[['trade_date', 'close', 'pct_chg']], 
                             on=['trade_date'], how='left')
                
                # 处理数据
                df = df.sort_values('trade_date', ascending=True)
                
                # 更新表格
                for _, row in df.iterrows():
                    try:
                        # 获取日期
                        trade_date = row['trade_date']
                        formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
                        
                        # 获取股票名称
                        stock_name = self.db.get_stock_name(stock_code)
                        
                        # 计算资金流向（单位：万元）
                        buy_sm_amount = float(row.get('buy_sm_vol', 0) or 0) * float(row.get('buy_sm_amount', 0) or 0) / 10000
                        sell_sm_amount = float(row.get('sell_sm_vol', 0) or 0) * float(row.get('sell_sm_amount', 0) or 0) / 10000
                        buy_md_amount = float(row.get('buy_md_vol', 0) or 0) * float(row.get('buy_md_amount', 0) or 0) / 10000
                        sell_md_amount = float(row.get('sell_md_vol', 0) or 0) * float(row.get('sell_md_amount', 0) or 0) / 10000
                        buy_lg_amount = float(row.get('buy_lg_vol', 0) or 0) * float(row.get('buy_lg_amount', 0) or 0) / 10000
                        sell_lg_amount = float(row.get('sell_lg_vol', 0) or 0) * float(row.get('sell_lg_amount', 0) or 0) / 10000
                        buy_elg_amount = float(row.get('buy_elg_vol', 0) or 0) * float(row.get('buy_elg_amount', 0) or 0) / 10000
                        sell_elg_amount = float(row.get('sell_elg_vol', 0) or 0) * float(row.get('sell_elg_amount', 0) or 0) / 10000

                        # 计算净流入
                        sm_net = buy_sm_amount - sell_sm_amount  # 小单净流入
                        md_net = buy_md_amount - sell_md_amount  # 中单净流入
                        lg_net = buy_lg_amount - sell_lg_amount  # 大单净流入
                        elg_net = buy_elg_amount - sell_elg_amount  # 特大单净流入
                        net_mf = sm_net + md_net + lg_net + elg_net  # 总净流入
                        
                        # 获取价格和涨跌幅
                        close_price = float(row.get('close', 0) or 0)  # 收盘价
                        change_pct = float(row.get('pct_chg', 0) or 0)  # 涨跌幅
                        
                        # 插入数据到表格
                        self.tree.insert("", tk.END, values=(
                            stock_code,
                            stock_name,
                            formatted_date,  # 添加日期列
                            f"{close_price:.2f}",
                            f"{change_pct:+.2f}%",
                            f"{net_mf:+.2f}万",
                            f"{sm_net:+.2f}万",
                            f"{md_net:+.2f}万",
                            f"{lg_net:+.2f}万",
                            f"{elg_net:+.2f}万"
                        ), tags=(
                            "positive" if net_mf > 0 else "negative",
                        ))
                        
                    except Exception as row_error:
                        logging.error(f"处理行数据失败: {str(row_error)}")
                        continue
            
            except Exception as e:
                logging.error(f"获取资金流向数失败: {str(e)}", exc_info=True)
                self.master.after(0, lambda: messagebox.showerror("错误", f"获取��据失败: {str(e)}"))
            finally:
                # 恢复查询按钮
                self.master.after(0, lambda: self.search_btn.configure(state='normal'))
        
        # 在新线程中获取数据
        threading.Thread(target=fetch_data, daemon=True).start()

    def show_sector_fund_flow(self):
        """显示板块内所有个股的资金流向"""
        # 获取当前选中的板块
        selection = self.sector_listbox.curselection()
        if not selection:
            messagebox.showwarning("警告", "请先选择板块！")
            return

        sector_text = self.sector_listbox.get(selection[0])
        sector_id = int(sector_text.split(' - ')[0])
        start_date = self.date_entry.get().strip()

        # 禁用按钮
        self.sector_flow_btn['state'] = 'disabled'
        self.search_btn['state'] = 'disabled'

        # 清空现有数
        for item in self.tree.get_children():
            self.tree.delete(item)

        # 添加查询状态标志
        self.current_query = True

        def fetch_sector_data():
            try:
                # 获取板块内所有股票
                stocks = self.db.get_sector_stocks(sector_id)
                total_stocks = len(stocks)
                processed_count = 0

                # 创建进度窗口
                progress_window = tk.Toplevel(self.master)
                progress_window.title("数据获取进度")
                progress_window.geometry("300x150")
                progress_window.transient(self.master)
                
                # 添加窗口关闭事件处理
                def on_closing():
                    self.current_query = False
                    progress_window.destroy()
                    # 恢复按钮状态
                    self.sector_flow_btn['state'] = 'normal'
                    self.search_btn['state'] = 'normal'
                
                progress_window.protocol("WM_DELETE_WINDOW", on_closing)

                # 添加进度条
                progress_label = ttk.Label(progress_window, text="正在获取数据...")
                progress_label.pack(pady=10)
                progress_bar = ttk.Progressbar(progress_window, length=200, mode='determinate')
                progress_bar.pack(pady=10)
                progress_bar['maximum'] = total_stocks

                # 更新进度显示
                status_label = ttk.Label(progress_window, text="")
                status_label.pack(pady=5)

                for stock in stocks:
                    # 检查是否需要停止查询
                    if not self.current_query:
                        break
                    
                    try:
                        stock_code = stock['stock_code']
                        
                        # 使用频率限制器
                        rate_limiter.wait()

                        # 获取股票日线数据
                        df_daily = pro.daily(
                            ts_code=stock_code,
                            start_date=start_date.replace('-', ''),
                            end_date=datetime.now().strftime('%Y%m%d')
                        )

                        # 获取资金流向数据
                        df_flow = pro.moneyflow(
                            ts_code=stock_code,
                            start_date=start_date.replace('-', ''),
                            end_date=datetime.now().strftime('%Y%m%d')
                        )

                        if not df_flow.empty and not df_daily.empty:
                            # 获取最新一天的数据
                            latest_flow = df_flow.iloc[0]
                            latest_daily = df_daily.iloc[0]

                            # 获取股票名称
                            stock_name = self.db.get_stock_name(stock_code)
                            
                            # 计算资金流向
                            buy_sm_amount = float(latest_flow.get('buy_sm_vol', 0) or 0) * float(latest_flow.get('buy_sm_amount', 0) or 0) / 10000
                            sell_sm_amount = float(latest_flow.get('sell_sm_vol', 0) or 0) * float(latest_flow.get('sell_sm_amount', 0) or 0) / 10000
                            buy_md_amount = float(latest_flow.get('buy_md_vol', 0) or 0) * float(latest_flow.get('buy_md_amount', 0) or 0) / 10000
                            sell_md_amount = float(latest_flow.get('sell_md_vol', 0) or 0) * float(latest_flow.get('sell_md_amount', 0) or 0) / 10000
                            buy_lg_amount = float(latest_flow.get('buy_lg_vol', 0) or 0) * float(latest_flow.get('buy_lg_amount', 0) or 0) / 10000
                            sell_lg_amount = float(latest_flow.get('sell_lg_vol', 0) or 0) * float(latest_flow.get('sell_lg_amount', 0) or 0) / 10000
                            buy_elg_amount = float(latest_flow.get('buy_elg_vol', 0) or 0) * float(latest_flow.get('buy_elg_amount', 0) or 0) / 10000
                            sell_elg_amount = float(latest_flow.get('sell_elg_vol', 0) or 0) * float(latest_flow.get('sell_elg_amount', 0) or 0) / 10000

                            # 计算净流入
                            sm_net = buy_sm_amount - sell_sm_amount
                            md_net = buy_md_amount - sell_md_amount
                            lg_net = buy_lg_amount - sell_lg_amount
                            elg_net = buy_elg_amount - sell_elg_amount
                            net_mf = sm_net + md_net + lg_net + elg_net

                            # 在主线程中更新UI
                            self.master.after(0, lambda: self.tree.insert("", tk.END, values=(
                                stock_code,
                                stock_name,  # 添加股票名称
                                f"{latest_daily['trade_date']}",  # 使用日线数据的收盘价
                                f"{latest_daily['close']:.2f}",  # 使用日线数据的收盘价
                                f"{latest_daily['pct_chg']:+.2f}%",  # 使用日线数据的涨跌幅
                                f"{net_mf:+.2f}万",
                                f"{sm_net:+.2f}万",
                                f"{md_net:+.2f}万",
                                f"{lg_net:+.2f}万",
                                f"{elg_net:+.2f}万"
                            ), tags=(
                                "positive" if net_mf > 0 else "negative",
                            )))

                    except Exception as e:
                        logging.error(f"获取股票 {stock_code} 数据失败: {str(e)}")

                    # 更新进度
                    processed_count += 1
                    if hasattr(progress_window, 'winfo_exists') and progress_window.winfo_exists():
                        self.master.after(0, lambda: progress_bar.step())
                        self.master.after(0, lambda: status_label.config(
                            text=f"已处理: {processed_count}/{total_stocks}"
                        ))

                # 完成后关闭进度窗口
                if hasattr(progress_window, 'winfo_exists') and progress_window.winfo_exists():
                    self.master.after(0, progress_window.destroy)

            except Exception as e:
                logging.error(f"获取板块资金流向数据失败: {str(e)}", exc_info=True)
                self.master.after(0, lambda: messagebox.showerror("错误", f"获取数据失败: {str(e)}"))
            finally:
                # 恢复按钮状态
                self.current_query = False
                self.master.after(0, lambda: self.sector_flow_btn.configure(state='normal'))
                self.master.after(0, lambda: self.search_btn.configure(state='normal'))

        # 在新线程中获取数据
        threading.Thread(target=fetch_sector_data, daemon=True).start()

    def __del__(self):
        """析构函数，断开数据库连接"""
        if hasattr(self, 'db'):
            self.db.disconnect()

if __name__ == "__main__":
    try:
        # 创建主窗口
        root = tk.Tk()
        root.title("资金流向查询")
        root.geometry("1200x800")
        
        # 创建应用实例
        app = StockFundFlow(root)
        
        # 运行主循环
        root.mainloop()
        
    except Exception as e:
        logging.error(f"程序运行失败: {str(e)}")
        print(f"程序运行失败: {str(e)}")
        input("按回车键退出...")