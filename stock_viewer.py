import tkinter as tk
from tkinter import ttk
import mysql.connector
from tkinter import messagebox
import logging
import tushare as ts
from datetime import datetime, timedelta
import pandas as pd
import threading
import time
from threading import Lock
from config import MYSQL_CONFIG, TUSHARE_TOKEN
from concurrent.futures import ThreadPoolExecutor

# 配置logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s'
)

# 添加tushare token配置
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()


# 添加访问频率控制
class RateLimiter:
  def __init__(self, calls_per_minute):
    self.calls_per_minute = calls_per_minute
    self.interval = 60.0 / calls_per_minute
    self.last_call = 0
    self.lock = Lock()
    self.call_count = 0  # 添加调用计数
    self.last_reset = time.time()  # 添加最后重置时间

  def wait(self):
    with self.lock:
      current_time = time.time()

      # 检查是否需要重置计数器（每分钟重置一次）
      if current_time - self.last_reset >= 60:
        self.call_count = 0
        self.last_reset = current_time
        logging.info("API调用计数器已重置")

      # 检查是否到限制
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


# 创建频率限制器，设置为每分钟190次，留出一些余量
rate_limiter = RateLimiter(calls_per_minute=190)


def get_latest_trade_date():
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
  if current_time.hour < 17:  # 17点前
    # 获取前一天的日期
    if current_weekday == 0:  # 如果是周一
      # 获取上周五的日期
      latest_date = now - timedelta(days=3)
      logging.info(f"当前是周一17点前，使用上周五 {latest_date.strftime('%Y%m%d')} 作为最新交易日")
    else:
      # 获取前一天的日期
      latest_date = now - timedelta(days=1)
      logging.info(f"当前是工作日17点前，使用昨天 {latest_date.strftime('%Y%m%d')} 作为最新交易日")
  else:  # 17点后
    # 使用当天日期
    latest_date = now
    logging.info(f"当前是工作日17点后，使用今天 {latest_date.strftime('%Y%m%d')} 作为最新交易日")

  return latest_date.strftime('%Y%m%d')


class StockViewer:
  def __init__(self, master=None):
    if master is None:
      master = tk.Tk()
    self.master = master
    
    # 设置样式
    self.setup_styles()
    
    # 初始化定时器相关属性
    self.timer_active = False
    self.timer_job = None
    self.scheduled_time = None
    
    # 初始化按钮引用
    self.update_btn = None
    self.timer_btn = None
    
    # 创建主框架
    self.main_frame = ttk.Frame(self.master)
    self.main_frame.pack(fill=tk.BOTH, expand=True, padx=0, pady=0)
    
    # 创建顶部按钮区域 - 这里会始化 update_btn 和 timer_btn
    self.create_top_buttons()
    
    # 创建数据显示区域
    self.create_data_view()
    
    # 创建分页控件
    self.create_pagination()
    
    # 初始化数据库连接
    self.db_connection = None
    
    self.update_running = False  # 添加更新状态标志
    self.update_window = None   # 添加更新窗口引用

  def setup_styles(self):
    """设置自定义样式"""
    style = ttk.Style()
    
    # 卡片样式
    style.configure(
        "Card.TFrame",
        background="white",
        relief="solid",
        borderwidth=1
    )
    
    # 卡片标题样式
    style.configure(
        "CardTitle.TLabel",
        font=("Microsoft YaHei UI", 14, "bold"),
        background="white",
        foreground="#333333"
    )
    
    # 操作按钮样式
    style.configure(
        "Operation.TButton",
        font=("Microsoft YaHei UI", 9),
        padding=(5, 3)
    )
    
    # 分页按钮样式
    style.configure(
        "Page.TButton",
        font=("Microsoft YaHei UI", 9),
        padding=5
    )
    
    # 信息文本样式
    style.configure(
        "Info.TLabel",
        font=("Microsoft YaHei UI", 9),
        background="white"
    )
    
    # 树形视图样式
    style.configure(
        "Custom.Treeview",
        font=("Microsoft YaHei UI", 10),
        rowheight=30
    )
    style.configure(
        "Custom.Treeview.Heading",
        font=("Microsoft YaHei UI", 10, "bold")
    )

  def create_top_buttons(self):
    # 创建操作区域卡片
    operation_card = ttk.Frame(self.main_frame, style="Card.TFrame")
    operation_card.pack(fill=tk.X, pady=(0, 20))
    
    # 创建按钮组 - 添加左边距
    button_frame = ttk.Frame(operation_card)
    button_frame.pack(fill=tk.X, padx=20, pady=10)  # 减小垂直内边距
    
    # 数据库相关按钮
    db_settings_btn = ttk.Button(
        button_frame,
        text="数据库设置",
        command=self.show_db_settings,
        style="Operation.TButton",
        width=12
    )
    db_settings_btn.pack(side=tk.LEFT, padx=(0, 5))
    
    db_connect_btn = ttk.Button(
        button_frame,
        text="数据库连接",
        command=self.connect_database,
        style="Operation.TButton",
        width=12
    )
    db_connect_btn.pack(side=tk.LEFT, padx=5)
    
    # 更新按钮 - 初始状态为禁用
    self.update_btn = ttk.Button(
        button_frame,
        text="开始更新",
        command=self.start_update,
        style="Operation.TButton",
        width=12,
        state='disabled'  # 初始状态为禁用
    )
    self.update_btn.pack(side=tk.LEFT, padx=5)
    
    # 添加定时更新按钮 - 初始状态为禁用
    self.timer_btn = ttk.Button(
        button_frame,
        text="定时更新: 关闭",
        command=self.toggle_timer,
        style="Operation.TButton",
        width=15,
        state='disabled'  # 初始状态为禁用
    )
    self.timer_btn.pack(side=tk.LEFT, padx=5)

  def create_data_view(self):
    # 创建数据显示卡片
    data_card = ttk.Frame(self.main_frame, style="Card.TFrame")
    data_card.pack(fill=tk.BOTH, expand=True, pady=(0, 20))
    
    # 创建标题栏
    title_frame = ttk.Frame(data_card)
    title_frame.pack(fill=tk.X, padx=20, pady=10)
    
    ttk.Label(
        title_frame,
        text="股票数据列表",
        style="CardTitle.TLabel"
    ).pack(side=tk.LEFT)
    
    # 创建Treeview来显示数据
    tree_frame = ttk.Frame(data_card)
    tree_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 20))
    
    columns = ("股票名称", "股票代码", "最新时间")
    self.tree = ttk.Treeview(
        tree_frame,
        columns=columns,
        show='headings',
        style="Custom.Treeview"
    )
    
    # 设置列标题和宽度
    for col in columns:
        self.tree.heading(col, text=col)
        self.tree.column(col, width=150)
    
    # 添加滚动条
    scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
    self.tree.configure(yscrollcommand=scrollbar.set)
    
    self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

  def create_pagination(self):
    # 创建分页卡片
    pagination_card = ttk.Frame(self.main_frame, style="Card.TFrame")
    pagination_card.pack(fill=tk.X)
    
    # 分页控件容器
    pagination_frame = ttk.Frame(pagination_card)
    pagination_frame.pack(padx=20, pady=10)
    
    # 添加页码信息显示
    self.page_info = ttk.Label(
        pagination_frame,
        text="",
        style="Info.TLabel"
    )
    self.page_info.pack(side=tk.RIGHT, padx=20)
    
    # 创建翻页按钮
    self.prev_btn = ttk.Button(
        pagination_frame,
        text="上一页",
        command=self.previous_page,
        style="Page.TButton"
    )
    self.prev_btn.pack(side=tk.LEFT, padx=5)
    
    self.next_btn = ttk.Button(
        pagination_frame,
        text="下一页",
        command=self.next_page,
        style="Page.TButton"
    )
    self.next_btn.pack(side=tk.LEFT, padx=5)
    
    # 初始化分页变量
    self.current_page = 1
    self.total_records = 0
    self.per_page = 20
    self.is_loading = False

  def connect_database(self):
    # 禁用连接按钮
    for widget in self.main_frame.winfo_children():
      if isinstance(widget, ttk.Frame):
        for btn in widget.winfo_children():
          if isinstance(btn, ttk.Button):
            btn['state'] = 'disabled'

    # 创建加载提示窗口
    loading_window = tk.Toplevel(self.master)
    loading_window.title("连接中")
    loading_window.geometry("300x100")
    loading_window.transient(self.master)

    loading_label = ttk.Label(loading_window, text="正在连接数据库...")
    loading_label.pack(pady=20)

    progress = ttk.Progressbar(loading_window, mode='indeterminate')
    progress.pack(fill=tk.X, padx=20)
    progress.start()

    # 在新线程中执行连接操作
    def connect_thread():
      error_msg = None
      try:
        if self.db_connection and hasattr(self.db_connection, 'is_connected'):
          if self.db_connection.is_connected():
            self.db_connection.close()

        # 设置连接参数
        config = MYSQL_CONFIG.copy()
        config.update({
          'use_pure': True,  # 强制使用纯Python实现
          'auth_plugin': 'mysql_native_password'
        })

        try:
          self.db_connection = mysql.connector.connect(**config)
        except ImportError:
            # 如果mysql.connector不可用，使用pymysql
            import pymysql
            self.db_connection = pymysql.connect(
              host=config['host'],
              user=config['user'],
              password=config['password'],
              database=config['database'],
              port=config['port'],
              charset=config['charset']
            )

        # 设置会话变量以优化性能
        cursor = self.db_connection.cursor()
        cursor.execute("SET SESSION wait_timeout = 28800")
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 50")
        cursor.execute("SET SESSION sql_mode = 'STRICT_TRANS_TABLES'")
        cursor.close()

        # 在主线程中更新UI
        self.master.after(0, lambda: self._after_connect(True, loading_window))

      except Exception as e:
        error_msg = str(e)
        logging.error(f"数据库连接失败: {error_msg}", exc_info=True)
        self.master.after(0, lambda: self._after_connect(False, loading_window, error_msg))

    threading.Thread(target=connect_thread, daemon=True).start()

  def _after_connect(self, success, loading_window, error_msg=None):
    # 关闭加载窗口
    loading_window.destroy()
    
    # 重新启用数据库设置和连接按钮
    for widget in self.main_frame.winfo_children():
        if isinstance(widget, ttk.Frame):
            for btn in widget.winfo_children():
                if isinstance(btn, ttk.Button):
                    if btn['text'] in ["数据库设置", "数据库连接"]:
                        btn['state'] = 'normal'
    
    if success:
        # 连接成功后启用更新和定时更新按钮
        self.update_btn['state'] = 'normal'
        self.timer_btn['state'] = 'normal'
        messagebox.showinfo("成功", "数据库连接成功！")
        # 在新线程中加载数据
        threading.Thread(target=self._load_data_thread, daemon=True).start()
    else:
        messagebox.showerror("错误", f"数据库连接失败，详细错误信息请查看控制台\n{error_msg}")

  def _load_data_thread(self, page=1, per_page=20):
    try:
        # 在主线程中显示加载提示
        self.master.after(0, self._show_loading_message, "正在加载数据...")

        cursor = self.db_connection.cursor(buffered=True)  # 使用缓冲游标

        # 获取总记录数
        cursor.execute("SELECT COUNT(1) FROM stocks")
        self.total_records = cursor.fetchone()[0]

        # 计算偏移量
        offset = (page - 1) * per_page

        # 优化查询语
        query = """
            SELECT 
                s.证券简称,
                s.证券代码,
                COALESCE(latest.trade_date, '暂无数据') as latest_date
            FROM stocks s
            LEFT JOIN (
                SELECT ts_code, MAX(trade_date) as trade_date
                FROM stock_data
                GROUP BY ts_code
            ) latest ON s.证券代码 = latest.ts_code
            LIMIT %s OFFSET %s
        """

        cursor.execute(query, (per_page, offset))
        results = cursor.fetchall()
        cursor.close()

        # 在主线程中更��UI
        self.master.after(0, lambda: self._update_tree_data(results))

    except Exception as e:
        logging.error(f"加载数据失败: {str(e)}", exc_info=True)
        self.master.after(0, lambda: messagebox.showerror("错误", "加载数据失败，详细错误信息查看控制台"))
    finally:
        # 重新启用翻页按钮
        self.master.after(0, self._finish_page_loading)

  def _finish_page_loading(self):
    """完成加载后的处理"""
    # 重新用翻页按钮
    self.prev_btn['state'] = 'normal'
    self.next_btn['state'] = 'normal'
    self.is_loading = False
    
    # 根据页码禁用相应按钮
    if self.current_page <= 1:
        self.prev_btn['state'] = 'disabled'
    
    total_pages = (self.total_records + self.per_page - 1) // self.per_page
    if self.current_page >= total_pages:
        self.next_btn['state'] = 'disabled'

  def _show_loading_message(self, message):
    # 清除现有数据
    for item in self.tree.get_children():
      self.tree.delete(item)

    # 插入加载消息
    self.tree.insert("", tk.END, values=(message, "", ""))

  def _update_tree_data(self, results):
    # 清除现有数据
    for item in self.tree.get_children():
      self.tree.delete(item)

    # 插入新数据
    for row in results:
      stock_name, stock_code, latest_date = row
      if latest_date and latest_date != '暂无数据':
        formatted_date = f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:]}"
      else:
        formatted_date = "暂无数据"

      self.tree.insert("", tk.END, values=(stock_name, stock_code, formatted_date))

    # 更新分页信息
    total_pages = (self.total_records + self.per_page - 1) // self.per_page
    current_start = (self.current_page - 1) * self.per_page + 1
    current_end = min(current_start + self.per_page - 1, self.total_records)

    # 更新页码信息显示
    self.page_info.config(
      text=f"第 {current_start}-{current_end} 条，共 {self.total_records} 条记录 "
           f"({self.current_page}/{total_pages}页)"
    )

  def start_update(self):
    """开始更新数据"""
    try:
        if self.update_running:
            messagebox.showwarning("警告", "更新已在进行中！")
            return
        
        # 禁用更新按钮
        self._disable_update_button()
        
        # 创建更新窗口
        self.update_window = tk.Toplevel(self.master)
        self.update_window.title("更新进度")
        self.update_window.geometry("500x400")
        self.update_window.transient(self.master)
        
        # 添加窗口关闭事件处理
        def on_closing():
            if messagebox.askyesno("确认", "确定要停止更新吗？"):
                self.stop_update()
        
        self.update_window.protocol("WM_DELETE_WINDOW", on_closing)
        
        # 创建进度条
        progress_frame = ttk.Frame(self.update_window, padding="10")
        progress_frame.pack(fill=tk.X)
        
        self.progress_bar = ttk.Progressbar(
            progress_frame,
            mode='determinate',
            length=300
        )
        self.progress_bar.pack(pady=10)
        
        # 创建进度标签
        self.progress_label = ttk.Label(
            progress_frame,
            text="准备更新...",
            font=("Microsoft YaHei UI", 9)
        )
        self.progress_label.pack(pady=5)
        
        # 创建文本区域
        text_frame = ttk.Frame(self.update_window)
        text_frame.pack(fill=tk.BOTH, expand=True, padx=10)
        
        self.fail_text = tk.Text(
            text_frame,
            height=15,
            width=50,
            font=("Microsoft YaHei UI", 9)
        )
        self.fail_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # 添加滚动条
        scrollbar = ttk.Scrollbar(text_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.fail_text.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.fail_text.yview)
        
        # 设置更新状态
        self.update_running = True
        
        # 在新线程中执行更新
        threading.Thread(target=self._update_data, daemon=True).start()
        
    except Exception as e:
        logging.error(f"启动更新失败: {str(e)}")
        messagebox.showerror("错误", f"启动更新失败: {str(e)}")
        self._enable_update_button()

  def toggle_timer(self):
    """切换定时更新状态"""
    if self.timer_active:
        self.stop_timer()
    else:
        self.show_timer_settings()

  def show_timer_settings(self):
    """显示定时设置窗口"""
    # 创建定时设置窗口
    timer_window = tk.Toplevel(self.master)
    timer_window.title("定时更新设置")
    timer_window.geometry("400x300")
    timer_window.transient(self.master)
    timer_window.grab_set()
    
    # 创建设置框架
    settings_frame = ttk.Frame(timer_window, padding="10")
    settings_frame.pack(fill=tk.BOTH, expand=True)
    
    # 添加时间段选择
    time_frame = ttk.LabelFrame(settings_frame, text="更新时间设置", padding="5")
    time_frame.pack(fill=tk.X, pady=5)
    
    # 小时选择
    hour_frame = ttk.Frame(time_frame)
    hour_frame.pack(fill=tk.X, pady=5)
    ttk.Label(hour_frame, text="时:").pack(side=tk.LEFT)
    hour_var = tk.StringVar(value="15")  # 默认设置为15点
    hour_spinbox = ttk.Spinbox(hour_frame, from_=0, to=23, width=5, textvariable=hour_var)
    hour_spinbox.pack(side=tk.LEFT, padx=5)
    
    # 分钟选择
    minute_frame = ttk.Frame(time_frame)
    minute_frame.pack(fill=tk.X, pady=5)
    ttk.Label(minute_frame, text="分:").pack(side=tk.LEFT)
    minute_var = tk.StringVar(value="30")  # 默认设置为30分
    minute_spinbox = ttk.Spinbox(minute_frame, from_=0, to=59, width=5, textvariable=minute_var)
    minute_spinbox.pack(side=tk.LEFT, padx=5)
    
    # 当前时间显示
    current_time_label = ttk.Label(settings_frame, text="")
    current_time_label.pack(pady=5)
    
    def update_current_time():
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_time_label.config(text=f"当前时间: {current_time}")
        timer_window.after(1000, update_current_time)
    
    update_current_time()
    
    def start_timer():
        try:
            hour = int(hour_var.get())
            minute = int(minute_var.get())
            
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError("无效的时间设置")
            
            self.timer_active = True
            self.scheduled_time = {'hour': hour, 'minute': minute}
            self.timer_btn.config(text=f"定时更新: {hour:02d}:{minute:02d}")
            
            # 计算下次更新时间
            self.schedule_next_update()
            
            timer_window.destroy()
            messagebox.showinfo("成功", f"已设置定时更新时间: {hour:02d}:{minute:02d}")
            
        except ValueError as e:
            messagebox.showerror("错误", f"无效的时间设置: {str(e)}", parent=timer_window)
    
    # 添加按钮
    button_frame = ttk.Frame(settings_frame)
    button_frame.pack(pady=20)
    
    ttk.Button(button_frame, text="启动", command=start_timer).pack(side=tk.LEFT, padx=5)
    ttk.Button(button_frame, text="取消", command=timer_window.destroy).pack(side=tk.LEFT, padx=5)

  def schedule_next_update(self):
    if not self.timer_active:
      return
    
    now = datetime.now()
    scheduled_time = now.replace(
      hour=self.scheduled_time['hour'],
      minute=self.scheduled_time['minute'],
      second=0,
      microsecond=0
    )
    
    # 如果当前时间已过今天的计划时间，则安排在明天的同一时间
    if now >= scheduled_time:
      scheduled_time = scheduled_time + timedelta(days=1)
    
    # 计算等待时间（毫秒）
    wait_time = int((scheduled_time - now).total_seconds() * 1000)
    
    # 取消现有的定时任
    if hasattr(self, 'timer_job') and self.timer_job:
      self.master.after_cancel(self.timer_job)
    
    # 安排下次更新
    self.timer_job = self.master.after(wait_time, self.execute_scheduled_update)
    
    # 记录日志
    logging.info(f"下次更新计划时间: {scheduled_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 更新按钮显示
    next_update = scheduled_time.strftime("%H:%M")
    self.timer_btn.config(text=f"定时更新: {next_update}")

  def execute_scheduled_update(self):
    if self.timer_active:
      logging.info("执行定时更新任务")
      self.start_update()
      # 安排下一次更新
      self.schedule_next_update()

  def stop_timer(self):
    if hasattr(self, 'timer_job') and self.timer_job:
      self.master.after_cancel(self.timer_job)
      self.timer_job = None
    
    self.timer_active = False
    if hasattr(self, 'scheduled_time'):
      del self.scheduled_time
    
    self.timer_btn.config(text="定时更新: 关闭")
    messagebox.showinfo("提示", "已停止定时更新")

  def load_data(self):
    if not self.is_loading:
      self._start_page_loading()

  def previous_page(self):
    if self.current_page > 1 and not self.is_loading:
      self.current_page -= 1
      self._start_page_loading()

  def next_page(self):
    total_pages = (self.total_records + self.per_page - 1) // self.per_page
    if self.current_page < total_pages and not self.is_loading:
      self.current_page += 1
      self._start_page_loading()

  def _start_page_loading(self):
    if not self.db_connection or not self.db_connection.is_connected():
      messagebox.showwarning("警告", "数据库连接已断开，请重新连接")
      return
    
    # 禁用翻页按钮
    self.prev_btn['state'] = 'disabled'
    self.next_btn['state'] = 'disabled'
    self.is_loading = True
    
    # 显示加载提示
    self._show_loading_message("正在加载数据...")
    
    # 在新线程中加载数据
    threading.Thread(target=lambda: self._load_data_thread(self.current_page, self.per_page), daemon=True).start()

  def show_db_settings(self):
    """显示数据库设置窗口"""
    # 创建数据库设置窗口
    settings_window = tk.Toplevel(self.master)
    settings_window.title("数据库设置")
    settings_window.geometry("400x300")
    settings_window.resizable(False, False)
    # 设置模态窗口
    settings_window.transient(self.master)
    settings_window.grab_set()

    # 创建输入框和标签
    input_frame = ttk.Frame(settings_window, padding="10")
    input_frame.pack(fill=tk.BOTH, expand=True)

    # 数据库配置输入框
    fields = [
        ("主机地址:", MYSQL_CONFIG['host']),
        ("端口:", str(MYSQL_CONFIG['port'])),
        ("用户名:", MYSQL_CONFIG['user']),
        ("密码:", MYSQL_CONFIG['password']),
        ("数据库名:", MYSQL_CONFIG['database'])
    ]

    entries = {}
    for i, (label_text, default_value) in enumerate(fields):
        ttk.Label(input_frame, text=label_text).grid(row=i, column=0, sticky=tk.W, pady=5)
        entry = ttk.Entry(input_frame, width=30)
        entry.insert(0, default_value)
        entry.grid(row=i, column=1, padx=5, pady=5)
        entries[label_text] = entry

    # 密码框特殊处理
    entries["密码:"].configure(show="*")

    def test_connection():
        """测试数据库连接"""
        # 获取输入的配置
        config = {
            'host': entries["主机地址:"].get(),
            'port': int(entries["端口:"].get()),
            'user': entries["用户名:"].get(),
            'password': entries["密码:"].get(),
            'database': entries["数据库名:"].get(),
            'charset': 'utf8mb4'
        }

        try:
            # 测试连接
            test_conn = mysql.connector.connect(**config)
            if test_conn.is_connected():
                # 使用父窗口作为消息框的父窗口
                messagebox.showinfo("成功", "数据库连接测试成功！", parent=settings_window)
                test_conn.close()
        except Exception as e:
            logging.error(f"数据库连接测试失败: {str(e)}", exc_info=True)
            messagebox.showerror("错误", "连接测试失败，详细错误信息请查看控制台", parent=settings_window)

    def save_settings():
        """保存数据库设置"""
        try:
            # 更新全局配置
            global MYSQL_CONFIG
            MYSQL_CONFIG.update({
                'host': entries["主机地址:"].get(),
                'port': int(entries["端口:"].get()),
                'user': entries["用户名:"].get(),
                'password': entries["密码:"].get(),
                'database': entries["数据库名:"].get()
            })

            # 如果当前有连接，断开它
            if self.db_connection and self.db_connection.is_connected():
                self.db_connection.close()
                self.db_connection = None

            messagebox.showinfo("成功", "配置已保存！")
            settings_window.destroy()
        except Exception as e:
            messagebox.showerror("错误", f"保存配置失败: {str(e)}")

    # 创建按钮区域
    button_frame = ttk.Frame(settings_window)
    button_frame.pack(pady=10)

    # 创建保存按钮
    ttk.Button(
        button_frame,
        text="保存",
        command=save_settings,
        style="Primary.TButton",
        width=10
    ).pack(side=tk.LEFT, padx=5)

    # 创建测试连接按钮
    ttk.Button(
        button_frame,
        text="测试连接",
        command=test_connection,
        style="Primary.TButton",
        width=10
    ).pack(side=tk.LEFT, padx=5)

  def _disable_update_button(self):
    """禁用更新按钮"""
    if hasattr(self, 'update_btn'):
        self.update_btn['state'] = 'disabled'
    if hasattr(self, 'timer_btn'):
        self.timer_btn['state'] = 'disabled'
    self.update_running = True

  def _enable_update_button(self):
    """启用更新按钮"""
    if hasattr(self, 'update_btn'):
        self.update_btn['state'] = 'normal'
    if hasattr(self, 'timer_btn'):
        self.timer_btn['state'] = 'normal'
    self.update_running = False

  def _update_data(self):
    """执行数据更新"""
    try:
        if not self.db_connection or not self.db_connection.is_connected():
            self.db_connection = mysql.connector.connect(**MYSQL_CONFIG)

        # 获取当前最新交易日期
        latest_trade_date = get_latest_trade_date()
        cursor = self.db_connection.cursor()

        # 获取所有股票代码及其最新日期
        cursor.execute("""
            SELECT s.证券代码,
                   COALESCE(MAX(sd.trade_date), '19900101') as last_update_date
            FROM stocks s
            LEFT JOIN stock_data sd ON s.证券代码 = sd.ts_code
            GROUP BY s.证券代码
        """)
        stock_dates = cursor.fetchall()

        # 检查是否有需要更新的股票
        stocks_to_update = []
        for code, last_date in stock_dates:
            if str(last_date) == latest_trade_date:
                logging.info(f"股票 {code} 已有最新数据 ({latest_trade_date})，跳过更新")
                continue
            stocks_to_update.append((code, last_date))

        total_stocks = len(stocks_to_update)

        if not stocks_to_update:
            logging.info(f"所有股票数据都是最新的 (最新交易日: {latest_trade_date})")
            messagebox.showinfo("提示", "所有股票数据都是最新的！")
            return

        # 更新进度条最大值
        self.progress_bar['maximum'] = total_stocks

        # 准备插入语句
        insert_query = """
            INSERT INTO stock_data (
                trade_date, ts_code, open, high, low, close, vol, amount
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                vol = VALUES(vol),
                amount = VALUES(amount)
        """

        # 更新每只股票的数据
        success_count = 0
        for i, (stock_code, last_date) in enumerate(stocks_to_update):
            try:
                # 检查是否需要停止更新
                if not self.update_running or not hasattr(self, 'update_window'):
                    logging.info("更新已被用户终止")
                    break
                
                # 使用频率限制器
                rate_limiter.wait()

                # 获取股票数据
                df = pro.daily(
                    ts_code=stock_code,
                    start_date=str(last_date),
                    end_date=latest_trade_date
                )

                if df.empty:
                    self.fail_text.insert(tk.END, f"股票 {stock_code} 无新数据\n")
                else:
                    values = []
                    for _, row in df.iterrows():
                        values.append((
                            row['trade_date'],
                            row['ts_code'],
                            row['open'],
                            row['high'],
                            row['low'],
                            row['close'],
                            row['vol'],
                            row['amount']
                        ))
                    
                    cursor.executemany(insert_query, values)
                    self.db_connection.commit()
                    self.fail_text.insert(tk.END, f"成功更新 {stock_code} 的 {len(values)} 条数据\n")
                    success_count += 1

                # 更新进度
                if hasattr(self, 'update_window'):
                    self.progress_bar['value'] = i + 1
                    self.progress_label.config(text=f"已处理: {i+1}/{total_stocks}")
                    self.fail_text.see(tk.END)
                    self.update_window.update()

            except Exception as e:
                if hasattr(self, 'update_window'):
                    self.fail_text.insert(tk.END, f"更新失败: {stock_code} - {str(e)}\n")
                    self.fail_text.see(tk.END)

        cursor.close()

        # 更新完成后的处理
        if hasattr(self, 'update_window'):
            summary = (f"\n更新完成！\n"
                      f"总计: {total_stocks} 只股票\n"
                      f"更新成功: {success_count} 只\n"
                      f"更新失败: {total_stocks - success_count} 只")
            
            self.fail_text.insert(tk.END, summary)
            self.fail_text.see(tk.END)
            
            # 等待3秒后自动关闭窗口并刷新数据
            self.master.after(3000, lambda: self.finish_update())

    except Exception as e:
        logging.error(f"更新数据失败: {str(e)}")
        if hasattr(self, 'update_window'):
            messagebox.showerror("错误", f"更新数据失败: {str(e)}")
    finally:
        self._enable_update_button()

  def finish_update(self):
    """完成更新后的处理"""
    try:
        # 关闭更新窗口
        if hasattr(self, 'update_window'):
            self.update_window.destroy()
            delattr(self, 'update_window')
        
        # 刷新数据显示
        self.current_page = 1  # 重置到第一页
        self._start_page_loading()
        
        # 显示更新完成提示
        messagebox.showinfo("提示", "数据更新完成！")
        
    except Exception as e:
        logging.error(f"完成更新处理失败: {str(e)}")

  def stop_update(self):
    """停止更新进程"""
    try:
        # 停止更新
        self.update_running = False
        
        # 等待一小段时间确保线程停止
        self.master.after(100, self.close_update_window)
        
    except Exception as e:
        logging.error(f"停止更新失败: {str(e)}")

  def close_update_window(self):
    """安全关闭更新窗口"""
    try:
        if hasattr(self, 'update_window'):
            self.update_window.destroy()
            delattr(self, 'update_window')
            
        # 恢复按钮状态
        self._enable_update_button()
        
    except Exception as e:
        logging.error(f"关闭更新窗口失败: {str(e)}")