import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import tushare as ts
from config import TUSHARE_TOKEN
from db_operations import DatabaseOperations
import threading
from datetime import datetime
import logging
import re
from rate_limiter import rate_limiter  # 确保导入频率限制器

# 配置tushare
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()  # 确保在类外部初始化pro

class StockSectorApp:
    def __init__(self, master=None):
        if master is None:
            master = tk.Tk()
        self.master = master
        self.db = DatabaseOperations()
        self.db.connect()
        
        # 添加状态标志
        self.current_operation = None
        self.is_loading = False
        
        # 设置主题样式
        self.setup_styles()
        
        # 创建主框架
        self.main_frame = ttk.Frame(self.master)
        self.main_frame.pack(expand=True, fill="both")
        
        self.create_widgets()
        
        # Initialize sector_listbox as an instance variable
        self.sector_listbox = None  # This will be set in show_edit_frame

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
        
        # 内容区域样式
        style.configure(
            "Content.TFrame",
            background="white"
        )
        
        # 标签框样式
        style.configure(
            "Custom.TLabelframe",
            background="white"
        )
        style.configure(
            "Custom.TLabelframe.Label",
            font=("Microsoft YaHei UI", 10, "bold"),
            background="white",
            foreground="#333333"
        )
        
        # 列表框样式
        style.configure(
            "Custom.Listbox",
            font=("Microsoft YaHei UI", 9),
            background="white",
            selectbackground="#1890ff",
            selectforeground="white"
        )

    def create_widgets(self):
        # 创建操作区域卡片
        operation_card = ttk.Frame(self.main_frame, style="Card.TFrame")
        operation_card.pack(fill=tk.X, pady=(0, 20))
        
        # 创建按钮组 - 添加左边距
        button_frame = ttk.Frame(operation_card)
        button_frame.pack(fill=tk.X, padx=20, pady=10)  # 减小垂直内边距

        # 新增板块按钮
        self.add_sector_btn = ttk.Button(
            button_frame,
            text="新增板块",
            command=self.show_add_sector_frame,
            style="Operation.TButton",
            width=12  # 减小按钮宽度
        )
        self.add_sector_btn.pack(side=tk.LEFT, padx=(0, 5))  # 第一个按钮左边距为0

        # 板块列表按钮
        self.sector_list_btn = ttk.Button(
            button_frame,
            text="板块列表",
            command=self.show_sector_list,
            style="Operation.TButton",
            width=12
        )
        self.sector_list_btn.pack(side=tk.LEFT, padx=5)

        # 版块及个股编辑按钮
        self.edit_sector_btn = ttk.Button(
            button_frame,
            text="版块及个股编辑",
            command=self.show_edit_frame,
            style="Operation.TButton",
            width=15  # 稍宽一点，因为字较长
        )
        self.edit_sector_btn.pack(side=tk.LEFT, padx=5)

        # 内容框架
        self.content_frame = ttk.Frame(self.main_frame, style="Content.TFrame")
        self.content_frame.pack(expand=True, fill="both")

        # 添加分隔线
        separator = ttk.Separator(self.main_frame, orient="horizontal")
        separator.pack(fill="x", pady=10)

        self.current_frame = None

    def stop_current_operation(self):
        """停止当前操作"""
        try:
            # 停止数据加载
            self.is_loading = False
            
            # 停止加载动画
            if hasattr(self, 'canvas') and hasattr(self.canvas, 'loading'):
                self.canvas.after_cancel(self.canvas.loading)
                delattr(self.canvas, 'loading')
            
            # 清除当前操作标记
            self.current_operation = None
            
            logging.info("已停止当前操作")
            
        except Exception as e:
            logging.error(f"停止当前操作失败: {str(e)}")

    def show_add_sector_frame(self):
        """显示新增板块页面"""
        # 停止当前操作
        self.stop_current_operation()
        
        # 设置当前操作标记
        self.current_operation = 'add_sector'
        
        # 清除当前内容
        if self.current_frame:
            self.current_frame.destroy()

        # 创建新内容框架
        self.current_frame = ttk.Frame(self.content_frame)
        self.current_frame.pack(expand=True, fill="both")

        # 板块信息
        sector_frame = ttk.LabelFrame(self.current_frame, text="板块信息", padding="10")
        sector_frame.pack(fill="x", pady=(0, 10))

        ttk.Label(sector_frame, text="板块名称:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        name_entry = ttk.Entry(sector_frame, width=50)
        name_entry.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(sector_frame, text="板块类型:").grid(row=1, column=0, padx=5, pady=5, sticky="e")
        type_entry = ttk.Entry(sector_frame, width=50)
        type_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(sector_frame, text="板块介绍:").grid(row=2, column=0, padx=5, pady=5, sticky="e")
        code_entry = ttk.Entry(sector_frame, width=50)
        code_entry.grid(row=2, column=1, padx=5, pady=5)

        # 股票代码输入框
        ttk.Label(sector_frame, text="输入股票代码（用逗号或空格分隔）:").grid(row=3, column=0, padx=5, pady=5, sticky="e")
        self.stock_codes_entry = ttk.Entry(sector_frame, width=50)
        self.stock_codes_entry.grid(row=3, column=1, padx=5, pady=5)

        # 股票代码查询按钮
        query_button = ttk.Button(sector_frame, text="查询股票", command=self.query_stocks)
        query_button.grid(row=4, column=0, columnspan=2, pady=(10, 0))

        # 个股信息
        stocks_frame = ttk.LabelFrame(self.current_frame, text="个股信息", padding="10")
        stocks_frame.pack(fill="both", expand=True)

        ttk.Label(stocks_frame, text="股票代码/名称:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        codes_text = tk.Text(stocks_frame, height=3, width=50)
        codes_text.grid(row=0, column=1, padx=5, pady=5)
        ttk.Label(stocks_frame, text="示例: 600001，平安银行,中国平安，000001").grid(row=1, column=1, sticky="w", padx=5)

        def convert_stock_name_to_code(stock_input):
            """将股票名称转换为股票代码"""
            try:
                cursor = self.db.connection.cursor(dictionary=True)
                cursor.execute("""
                    SELECT 证券代码
                    FROM stocks 
                    WHERE 证券代码 LIKE %s 
                    OR 证券简称 = %s
                """, (f"{stock_input}%", stock_input))  # 使用 LIKE 和 % 进行模糊匹配
                result = cursor.fetchone()
                if result:
                    return result['证券代码']
                else:
                    # 如果数据库中没有，尝试通过Tushare获取
                    df = pro.stock_basic()
                    # 过滤出与输入名称匹配的股票
                    stock_code = df[df['name'].str.contains(stock_input, na=False)]
                    if not stock_code.empty:
                        return stock_code.iloc[0]['ts_code']  # 返回第一个匹配的股票代码
                    
                    # 如果仍然没有找到，返回 None
                    return None
            except Exception as e:
                logging.error(f"转换股票代码失败: {str(e)}")
                return None

        def submit():
            sector_name = name_entry.get().strip()
            sector_type = type_entry.get().strip()
            sector_code = code_entry.get().strip()
            
            # 处理股票输入，支持中英文逗号
            stock_text = codes_text.get("1.0", "end-1c")
            stock_inputs = [
                input.strip() 
                for input in stock_text.replace('，', ',').split(',') 
                if input.strip()
            ]
            
            stock_codes = []
            failed_inputs = []
            
            # 转换股票名称为代码
            for input_text in stock_inputs:
                stock_code = convert_stock_name_to_code(input_text)
                if stock_code is None:
                    failed_inputs.append(f"无法识别股票: {input_text}")  # 记录失败信息
                else:
                    stock_codes.append(stock_code)
            
            # 如果有无法识别的输入，显示警告
            if failed_inputs:
                failed_list = '\n'.join(failed_inputs)
                messagebox.showwarning("警告", f"以下输入无法识别:\n{failed_list}")
            
            # 去重
            stock_codes = list(set(stock_codes))
            
            if not sector_name:
                messagebox.showwarning("警告", "请填写完整的板块信息！")
                return
            
            if not stock_codes:
                messagebox.showwarning("警告", "请至少输入一个有效的股票！")
                return
            
            if self.db.add_sector(sector_name, sector_type, sector_code):
                sector_id = self.db.get_last_sector_id()
                
                success = True
                failed_codes = []
                for code in stock_codes:
                    if not self.db.add_stock_to_sector(sector_id, code):
                        success = False
                        failed_codes.append(code)
                
                if success:
                    messagebox.showinfo("成功", 
                                      f"板块添加成功！\n"
                                      f"已添加 {len(stock_codes)} 只股票\n"
                                      f"以下股票已被过滤掉:\n{failed_list if failed_inputs else '无'}")
                    name_entry.delete(0, tk.END)
                    type_entry.delete(0, tk.END)
                    code_entry.delete(0, tk.END)
                    codes_text.delete("1.0", tk.END)
                else:
                    failed_list = '、'.join(failed_codes)
                    messagebox.showerror("错误", 
                                       f"以下股票添加失败:\n{failed_list}")
            else:
                messagebox.showerror("错误", "添加板块失败！")

        # 按钮框架
        button_frame = ttk.Frame(self.current_frame)
        button_frame.pack(pady=20)

        ttk.Button(button_frame, text="确认", command=submit).pack(side=tk.LEFT, padx=10)
        ttk.Button(button_frame, text="清空",
                   command=lambda: [
                       name_entry.delete(0, tk.END),
                       type_entry.delete(0, tk.END),
                       code_entry.delete(0, tk.END),
                       codes_text.delete("1.0", tk.END)
                   ]).pack(side=tk.LEFT, padx=10)

    def query_stocks(self):
        """查询用户输入的股票在Tushare中的存在性"""
        stock_codes_input = self.stock_codes_entry.get().strip()
        
        # 使用空格和其他符号分隔输入
        stock_codes = [code.strip() for code in re.split(r'[ ,;]+', stock_codes_input) if code.strip()]
        
        if not stock_codes:
            messagebox.showwarning("警告", "请输入股票代码或名称！")
            return

        results = []
        for code in stock_codes:
            try:
                # 使用频率限制器
                rate_limiter.wait()
                
                # 使用Tushare查询股票基本信息
                df = pro.stock_basic()
                # 检查是否为中文名称
                stock_info = df[df['name'].str.contains(code, na=False)]
                if not stock_info.empty:
                    result_message = f"股票名称 '{code}' 存在于Tushare中，股票代码为: {stock_info.iloc[0]['ts_code']}"
                    results.append(result_message)
                    logging.info(result_message)  # 记录到日志
                else:
                    result_message = f"股票名称 '{code}' 不存在于Tushare中。"
                    results.append(result_message)
                    logging.warning(result_message)  # 记录到日志
            except Exception as e:
                logging.error(f"查询股票 {code} 失败: {str(e)}")
                results.append(f"查询股票名称 '{code}' 时发生错误: {str(e)}")

        # 显示查询结果
        result_message = "\n".join(results)
        messagebox.showinfo("查询结果", result_message)

    def show_sector_list(self):
        """显示板块列表"""
        # 停止当前操作
        self.stop_current_operation()
        
        # 设置当前操作标记
        self.current_operation = 'sector_list'
        
        # 清除当前内容
        if self.current_frame:
            self.current_frame.destroy()

        # 创建新的内容框架，添加padding
        self.current_frame = ttk.Frame(self.content_frame, padding="20")
        self.current_frame.pack(expand=True, fill="both")

        # 左侧板块列表框架 - 添加样式
        left_frame = ttk.LabelFrame(self.current_frame, text="板块列表", padding="15")
        left_frame.pack(side=tk.LEFT, fill="both", expand=True, padx=(0, 10))

        # 创建板块列表框 - 使用更好的字体
        sector_listbox = tk.Listbox(left_frame, width=40, height=25, 
                                   font=("Microsoft YaHei UI", 10),
                                   selectmode=tk.SINGLE,
                                   activestyle='dotbox',
                                   selectbackground='#0078D7',
                                   selectforeground='white')
        sector_listbox.pack(pady=5, fill="both", expand=True)

        # 右侧框架布局
        right_frame = ttk.Frame(self.current_frame)
        right_frame.pack(side=tk.LEFT, fill="both", expand=True)

        # 日期选择区域 - 美化
        date_frame = ttk.LabelFrame(right_frame, text="日期选择", padding="10")
        date_frame.pack(fill="x", pady=(0, 10))
        
        ttk.Label(date_frame, text="起始日期:", font=("Microsoft YaHei UI", 9)).pack(side=tk.LEFT, padx=5)
        date_entry = ttk.Entry(date_frame, width=15, font=("Microsoft YaHei UI", 9))
        date_entry.pack(side=tk.LEFT, padx=5)
        date_entry.insert(0, "2023-09-20")

        # 个股列表区域 - 美化
        stock_list_frame = ttk.LabelFrame(right_frame, text="个股列表", padding="10")
        stock_list_frame.pack(fill="x", pady=(0, 10))

        # 创建个股列表框 - 在这里创建 stock_listbox
        stock_listbox = tk.Listbox(stock_list_frame, height=8,
                                  font=("Microsoft YaHei UI", 9),
                                  selectmode=tk.SINGLE,
                                  activestyle='dotbox')
        stock_listbox.pack(fill="x", expand=True, pady=5)

        # 统计展示区域
        stats_frame = ttk.Frame(right_frame)
        stats_frame.pack(fill="both", expand=True)

        # 个股涨幅示 - 美化
        performance_frame = ttk.LabelFrame(stats_frame, text="个股涨幅统计", padding="15")
        performance_frame.pack(fill="both", expand=True, pady=(0, 10))
        
        # 创建画布容器
        canvas_container = ttk.Frame(performance_frame)
        canvas_container.pack(fill="both", expand=True)

        # 创建水平滚动条
        h_scrollbar = ttk.Scrollbar(canvas_container, orient=tk.HORIZONTAL)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)

        # 创建画布
        canvas = tk.Canvas(canvas_container, height=220, bg='white',
                          xscrollcommand=h_scrollbar.set)
        canvas.pack(fill="both", expand=True)

        # 配置滚动条
        h_scrollbar.config(command=canvas.xview)

        # 添加缩放功能
        def on_canvas_mousewheel(event):
            if event.state == 4:  # Ctrl键被按下
                # 缩放因子
                scale_factor = 1.1 if event.delta > 0 else 0.9
                # 获取当前滚动区域
                bbox = canvas.bbox("all")
                if bbox:
                    # 计算新的宽度
                    new_width = int(bbox[2] * scale_factor)
                    # 限制最小和最大宽度
                    min_width = canvas.winfo_width()
                    max_width = min_width * 5  # 最大放大5倍
                    new_width = max(min_width, min(new_width, max_width))
                    # 重新配置滚动区域
                    canvas.configure(scrollregion=(0, 0, new_width, bbox[3]))

        # 绑定鼠滚轮事件
        canvas.bind("<Control-MouseWheel>", on_canvas_mousewheel)

        # 添加日期新按钮
        def update_date():
            selection = sector_listbox.curselection()
            if selection:
                # 发选择事件来新数据
                on_sector_select(None)
            else:
                messagebox.showwarning("警告", "请先选择一个板块！")

        update_btn = ttk.Button(
            date_frame,
            text="更新数据",
            command=update_date,
            style="Operation.TButton",
            width=10
        )
        update_btn.pack(side=tk.LEFT, padx=10)

        # 添加日期格式提示
        ttk.Label(
            date_frame,
            text="格式: YYYY-MM-DD",
            font=("Microsoft YaHei UI", 8),
            foreground="#666666"
        ).pack(side=tk.LEFT, padx=5)

        def validate_date(date_str):
            """验证期式"""
            try:
                if len(date_str) != 10:
                    return False
                year = int(date_str[:4])
                month = int(date_str[5:7])
                day = int(date_str[8:])
                if date_str[4] != '-' or date_str[7] != '-':
                    return False
                if not (1900 <= year <= 9999):
                    return False
                if not (1 <= month <= 12):
                    return False
                if not (1 <= day <= 31):
                    return False
                return True
            except:
                return False

        def on_sector_select(event):
            """处理板块选择事件"""
            # 如果当前操作已改变，不执行后续操作
            if self.current_operation != 'sector_list':
                return
            
            # 如果正在加载数据，不处理新的选择
            if self.is_loading:
                return
            
            selection = sector_listbox.curselection()
            if not selection:
                return
            
            # 设置加载状态
            self.is_loading = True
            
            try:
                sector_text = sector_listbox.get(selection[0])
                sector_id = int(sector_text.split(' - ')[0])
                start_date = date_entry.get().strip()
                
                # 验证日期格式
                if not validate_date(start_date):
                    messagebox.showerror("错误", "请输入正确的日期格式：YYYY-MM-DD")
                    return
                
                # 转换日期格式为数据库格式
                start_date = start_date.replace('-', '')
                
                # 更新个股列表
                stock_listbox.delete(0, tk.END)
                stocks = self.db.get_sector_stocks(sector_id)
                for stock in stocks:
                    # 获取股票名称
                    stock_name = self.db.get_stock_name(stock['stock_code'])
                    # 显示代码和名称
                    stock_listbox.insert(tk.END, f"{stock['stock_code']} - {stock_name}")
                
                # 显加载动画
                canvas.delete("all")
                loading_text = canvas.create_text(
                    canvas.winfo_width()/2,
                    canvas.winfo_height()/2,
                    text="数据加载中...",
                    font=("Microsoft YaHei UI", 12),
                    fill="#666666"
                )
                
                # 在新线程中处理数
                def process_data():
                    try:
                        # 获取并展示个股涨幅
                        stock_data = self.db.get_sector_stock_performance(sector_id, start_date)
                        # 获取成交额数据
                        amount_data = self.db.get_sector_daily_amount(sector_id, start_date)
                        
                        if not stock_data:
                            self.master.after(0, lambda: canvas.create_text(
                                canvas.winfo_width()/2,
                                canvas.winfo_height()/2,
                                text="暂无数据",
                                font=("Microsoft YaHei UI", 12),
                                fill="#666666"
                            ))
                            return
                        
                        # 处理股票数据，计算涨幅
                        stock_perf = {}
                        for data in stock_data:
                            ts_code = data['ts_code']
                            if ts_code not in stock_perf:
                                stock_perf[ts_code] = {
                                    'start_price': float(data['open']),
                                    'latest_price': float(data['close'])
                                }
                            else:
                                stock_perf[ts_code]['latest_price'] = float(data['close'])
                        
                        # 计算涨幅并转换为列表格式
                        performance_data = []
                        for code, prices in stock_perf.items():
                            if prices['start_price'] and prices['start_price'] != 0:
                                perf = (prices['latest_price'] - prices['start_price']) / prices['start_price'] * 100
                                performance_data.append((code, perf))
                        
                        # 按涨幅排序
                        performance_data.sort(key=lambda x: x[1], reverse=True)
                        
                        # 在主线程UI
                        def update_ui():
                            # 更新涨幅图表
                            canvas.delete("all")
                            width = max(canvas.winfo_width(), len(performance_data) * 100)
                            height = canvas.winfo_height()
                            y_center = height / 2
                            padding = 40
                            
                            # 添加日期标题
                            canvas.create_text(
                                width/2, 20,
                                text=f"统计区间: {start_date[:4]}-{start_date[4:6]}-{start_date[6:]} 至今",
                                font=("Microsoft YaHei UI", 10, "bold"),
                                fill="#333333"
                            )
                            
                            # 计算 y_scale
                            max_perf = max(abs(perf) for _, perf in performance_data)
                            y_scale = (height/2 - padding * 1.5) / max_perf if max_perf > 0 else 1
                            
                            # 绘制柱状图
                            for i, (code, perf) in enumerate(performance_data):
                                x = padding + i * 100
                                bar_height = abs(perf) * y_scale
                                
                                # 获取股票名称
                                stock_name = self.db.get_stock_name(code)
                                
                                if perf > 0:
                                    canvas.create_rectangle(
                                        x-15, y_center-bar_height,
                                        x+15, y_center,
                                        fill="#ff4d4f", outline="#ff3333"
                                    )
                                    canvas.create_text(
                                        x, y_center-bar_height-10,
                                        text=f"+{perf:.2f}%",
                                        font=("Microsoft YaHei UI", 9),
                                        fill="#ff4d4f"
                                    )
                                else:
                                    canvas.create_rectangle(
                                        x-15, y_center,
                                        x+15, y_center+bar_height,
                                        fill="#52c41a", outline="#449a1c"
                                    )
                                    canvas.create_text(
                                        x, y_center+bar_height+10,
                                        text=f"{perf:.2f}%",
                                        font=("Microsoft YaHei UI", 9),
                                        fill="#52c41a"
                                    )
                                
                                # 显示股票代码和名称
                                canvas.create_text(
                                    x, height-20,
                                    text=f"{code}\n{stock_name}",
                                    font=("Microsoft YaHei UI", 8),
                                    fill="#666666",
                                    justify=tk.CENTER
                                )
                            
                            # 更新成交额图表
                            amount_canvas.delete("all")
                            if amount_data:
                                # 计算数据围
                                amounts = [data['total_amount'] / 100000000 for data in amount_data]  # 转换为亿元
                                max_amount = max(amounts) if amounts else 0
                                min_amount = min(amounts) if amounts else 0
                                
                                # 设置边距和图表尺寸
                                padding = 40
                                base_width = amount_canvas.winfo_width()
                                min_spacing = 100  # 每个数据点的最小间距
                                chart_width = max(base_width, len(amount_data) * min_spacing)
                                chart_height = amount_canvas.winfo_height()
                                
                                # 配置画布滚动区域
                                amount_canvas.configure(scrollregion=(0, 0, chart_width, chart_height))
                                
                                # 绘制背景网格
                                for i in range(0, chart_width, 50):
                                    amount_canvas.create_line(i, 0, i, chart_height, fill="#f0f0f0")
                                for i in range(0, chart_height, 50):
                                    amount_canvas.create_line(0, i, chart_width, i, fill="#f0f0f0")
                                
                                # 计算比例
                                y_scale = (chart_height - padding * 2) / (max_amount - min_amount) if max_amount != min_amount else 1
                                x_scale = (chart_width - padding * 2) / (len(amount_data) - 1) if len(amount_data) > 1 else chart_width
                                
                                # 准备所有点的坐标
                                points = []
                                point_info = []
                                
                                for i, data in enumerate(amount_data):
                                    amount = data['total_amount'] / 100000000  # 转换为亿元
                                    x = padding + i * x_scale
                                    y = chart_height - padding - (amount - min_amount) * y_scale
                                    points.extend([x, y])
                                    
                                    # 处日期格式
                                    trade_date = str(data['trade_date'])
                                    formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
                                    
                                    point_info.append({
                                        'x': x,
                                        'y': y,
                                        'amount': amount,
                                        'date': formatted_date
                                    })
                                
                                # 先绘制折线
                                if len(points) >= 4:
                                    amount_canvas.create_line(points, fill="#1890ff", smooth=True, width=2)
                                
                                # 绘制数据点和标签
                                for point in point_info:
                                    # 添加数据点
                                    amount_canvas.create_oval(
                                        point['x']-3, point['y']-3,
                                        point['x']+3, point['y']+3,
                                        fill="#1890ff",
                                        outline="#1890ff"
                                    )
                                    
                                    # 添加金额标签
                                    amount_canvas.create_text(
                                        point['x'], point['y']-15,
                                        text=f"{point['amount']:.2f}亿",
                                        font=("Microsoft YaHei UI", 8),
                                        fill="#666666"
                                    )
                                    
                                    # 添加日期标签
                                    amount_canvas.create_text(
                                        point['x'], chart_height-padding+15,
                                        text=point['date'],
                                        font=("Microsoft YaHei UI", 8),
                                        fill="#666666",
                                        angle=45
                                    )
                                
                                # 添加标题
                                amount_canvas.create_text(
                                    chart_width/2, 20,
                                    text=f"日成交额统计 ({start_date[:4]}-{start_date[4:6]}-{start_date[6:]} 至今)",
                                    font=("Microsoft YaHei UI", 10, "bold"),
                                    fill="#333333"
                                )
                            else:
                                amount_canvas.create_text(
                                    amount_canvas.winfo_width()/2,
                                    amount_canvas.winfo_height()/2,
                                    text="暂无交额数据",
                                    font=("Microsoft YaHei UI", 12),
                                    fill="#666666"
                                )
                            
                            # 更新画布配置
                            canvas.configure(scrollregion=canvas.bbox("all"))
                            amount_canvas.configure(scrollregion=amount_canvas.bbox("all"))
                        
                        self.master.after(0, update_ui)
                        
                    except Exception as e:
                        logging.error(f"处理据失败: {str(e)}")
                        self.master.after(0, lambda: messagebox.showerror("错误", f"数据处理失败: {str(e)}"))
                    finally:
                        self.is_loading = False
                
                # 启数据处理线程
                threading.Thread(target=process_data, daemon=True).start()
                
            except Exception as e:
                logging.error(f"选择处理失败: {str(e)}")
                messagebox.showerror("错误", f"处理失败: {str(e)}")
                self.is_loading = False

        # 绑定选择事件
        sector_listbox.bind('<<ListboxSelect>>', on_sector_select)

        # 加载板块数据
        sectors = self.db.get_all_sectors()
        for sector in sectors:
            sector_info = f"{sector['sector_id']} - {sector['sector_name']} ({sector['sector_type']})"
            sector_listbox.insert(tk.END, sector_info)

        # 添加滚条
        def add_scrollbar(listbox, frame):
            scrollbar = ttk.Scrollbar(frame)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            listbox.config(yscrollcommand=scrollbar.set)
            scrollbar.config(command=listbox.yview)

        add_scrollbar(sector_listbox, left_frame)
        add_scrollbar(stock_listbox, stock_list_frame)

        # 在 show_sector_list 方法中添加交额统计区域

        # 成交额统计展示 - 美化
        amount_frame = ttk.LabelFrame(stats_frame, text="每日成交额统计", padding="15")
        amount_frame.pack(fill="both", expand=True)

        # 创建画布容器
        amount_container = ttk.Frame(amount_frame)
        amount_container.pack(fill="both", expand=True)

        # 创建水平滚动条
        h_scrollbar = ttk.Scrollbar(amount_container, orient=tk.HORIZONTAL)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)

        # 创建画布
        amount_canvas = tk.Canvas(amount_container, height=220, bg='white',
                         xscrollcommand=h_scrollbar.set)
        amount_canvas.pack(fill="both", expand=True)

        # 配置滚动条
        h_scrollbar.config(command=amount_canvas.xview)

        # 添加缩放功能
        def on_amount_mousewheel(event):
            if event.state == 4:  # Ctrl键被按下
                # 缩放因子
                scale_factor = 1.1 if event.delta > 0 else 0.9
                # 获取当前滚动区域
                bbox = amount_canvas.bbox("all")
                if bbox:
                    # 计算新的宽度
                    new_width = int(bbox[2] * scale_factor)
                    # 限制最小和最大宽度
                    min_width = amount_canvas.winfo_width()
                    max_width = min_width * 5  # 最大放大5倍
                    new_width = max(min_width, min(new_width, max_width))
                    # 重新配置滚动区域
                    amount_canvas.configure(scrollregion=(0, 0, new_width, bbox[3]))

        # 绑定鼠标滚轮事件
        amount_canvas.bind("<Control-MouseWheel>", on_amount_mousewheel)

    def show_edit_frame(self):
        """显示板块及个股编辑界面"""
        # 停止当前操作
        self.stop_current_operation()
        
        # 设置当前操作标记
        self.current_operation = 'edit_frame'
        
        # 清除当前内容
        if self.current_frame:
            self.current_frame.destroy()

        # 创建新的内容框架
        self.current_frame = ttk.Frame(self.content_frame)
        self.current_frame.pack(expand=True, fill="both")

        # 定义变量
        current_sector_id = None

        def save_all():
            """保存所有修改"""
            if not current_sector_id:
                messagebox.showwarning("警告", "请先选择板块！")
                return

            # 保存板块信息
            sector_name = name_entry.get().strip()
            sector_type = type_entry.get().strip()
            sector_code = code_entry.get().strip()

            if not all([sector_name, sector_type, sector_code]):
                messagebox.showwarning("警告", "请填写完整的板块信息！")
                return

            # 保存板块信息
            if self.db.update_sector(current_sector_id, sector_name, sector_type, sector_code):
                messagebox.showinfo("成功", "板块信息保存成功！")
            else:
                messagebox.showerror("错误", "保存失败！")

        def clear_all_inputs():
            """清空所有输入"""
            name_entry.delete(0, tk.END)
            type_entry.delete(0, tk.END)
            code_entry.delete(0, tk.END)
            stocks_listbox.delete(0, tk.END)
            nonlocal current_sector_id
            current_sector_id = None
            self.sector_listbox.selection_clear(0, tk.END)

        def delete_sector():
            """删除当前板块"""
            nonlocal current_sector_id  # Use nonlocal to access the variable from the outer scope
            if not current_sector_id:
                messagebox.showwarning("警告", "请先选择板块！")
                return
            
            # 获取板块名称
            sector_name = name_entry.get().strip()
            
            # 确认删除
            if not messagebox.askyesno("确认删除", 
                                      f"确定要删除板块 '{sector_name}' 及其所有关联的股票吗？\n"
                                      "此操作不可恢复！"):
                return
            
            # 执行删除操作
            if self.db.delete_sector(current_sector_id):
                messagebox.showinfo("成功", f"已删除板块 '{sector_name}' 及其关联的所有股票！")
                clear_all_inputs()  # 清空输入框
                self.refresh_sector_list()  # 刷新板块列表
            else:
                messagebox.showerror("错误", "删除板块失败！")

        def refresh_stocks_list(sector_id):
            """刷新股票列表"""
            stocks_listbox.delete(0, tk.END)
            stocks = self.db.get_sector_stocks(sector_id)
            for stock in stocks:
                # 获取股票名称
                stock_name = self.db.get_stock_name(stock['stock_code'])
                # 显示代码和名称
                stocks_listbox.insert(tk.END, f"{stock['stock_code']} - {stock_name}")

        def remove_stock():
            """删除股票"""
            if not current_sector_id:
                messagebox.showwarning("警告", "请先选择板块")
                return
            
            selection = stocks_listbox.curselection()
            if not selection:
                messagebox.showwarning("警告", "请选择要删除的股票！")
                return
            
            stock_text = stocks_listbox.get(selection[0])
            stock_code = stock_text.split(' - ')[0]  # 取代码部分
            
            if messagebox.askyesno("确认", f"确定要删除股票 {stock_text} 吗？"):
                if self.db.delete_stock_from_sector(current_sector_id, stock_code):
                    messagebox.showinfo("成功", "股票删除成功！")
                    refresh_stocks_list(current_sector_id)
                else:
                    messagebox.showerror("错误", "删除股票失败！")

        def on_sector_select(event):
            """处理板块选择事件"""
            selection = self.sector_listbox.curselection()
            if selection:
                sector_text = self.sector_listbox.get(selection[0])
                sector_id = int(sector_text.split(' - ')[0])
                nonlocal current_sector_id
                current_sector_id = sector_id
                
                sectors = self.db.get_all_sectors()
                sector = next((s for s in sectors if s['sector_id'] == sector_id), None)
                
                if sector:
                    # 填充板块信息
                    name_entry.delete(0, tk.END)
                    name_entry.insert(0, sector['sector_name'])
                    
                    type_entry.delete(0, tk.END)
                    type_entry.insert(0, sector['sector_type'])
                    
                    code_entry.delete(0, tk.END)
                    code_entry.insert(0, sector['sector_code'])
                    
                    # 更新个股列表
                    refresh_stocks_list(sector_id)

        # 创建按钮区域（移到顶部）
        button_frame = ttk.Frame(self.current_frame)
        button_frame.pack(fill=tk.X, padx=10, pady=10)

        # 顶部按钮
        ttk.Button(button_frame, text="保存", width=15, command=save_all,
                   style="Primary.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="取消", width=15, command=clear_all_inputs,
                   style="Normal.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="删除股票", width=15,
                   command=remove_stock,
                   style="Danger.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="删除板块", width=15,
                   command=delete_sector,
                   style="Danger.TButton").pack(side=tk.LEFT, padx=5)

        # 创建左右分割的框架
        content_container = ttk.Frame(self.current_frame)
        content_container.pack(fill="both", expand=True, padx=10)

        # 左侧板块列表
        left_frame = ttk.LabelFrame(content_container, text="板块表", padding="10")
        left_frame.pack(side=tk.LEFT, fill="both", expand=True, padx=(0, 5))

        self.sector_listbox = tk.Listbox(left_frame, width=40, height=20,
                                          font=("Microsoft YaHei UI", 10))
        self.sector_listbox.pack(pady=10, fill="both", expand=True)

        # 右侧内容
        right_frame = ttk.Frame(content_container)
        right_frame.pack(side=tk.LEFT, fill="both", expand=True, padx=(5, 0))

        # 板块信息编辑区域
        sector_info_frame = ttk.LabelFrame(right_frame, text="板块信息", padding="10")
        sector_info_frame.pack(fill="x", pady=(0, 10))

        # 板块信息输入框
        ttk.Label(sector_info_frame, text="板块名称:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        name_entry = ttk.Entry(sector_info_frame, width=40)
        name_entry.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(sector_info_frame, text="板块类型:").grid(row=1, column=0, padx=5, pady=5, sticky="e")
        type_entry = ttk.Entry(sector_info_frame, width=40)
        type_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(sector_info_frame, text="板块介绍:").grid(row=2, column=0, padx=5, pady=5, sticky="e")
        code_entry = ttk.Entry(sector_info_frame, width=40)
        code_entry.grid(row=2, column=1, padx=5, pady=5)

        # 个股列表编辑区域
        stocks_frame = ttk.LabelFrame(right_frame, text="个股列表", padding="10")
        stocks_frame.pack(fill="both", expand=True)

        # 个股列表
        stocks_listbox = tk.Listbox(
            stocks_frame,
            height=8,
            width=50,  # 增加宽度以容纳代码和名称
            font=("Microsoft YaHei UI", 10)
        )
        stocks_listbox.pack(fill="both", expand=True, pady=(0, 10))

        # 加载板块列表
        sectors = self.db.get_all_sectors()
        for sector in sectors:
            sector_info = f"{sector['sector_id']} - {sector['sector_name']}"
            self.sector_listbox.insert(tk.END, sector_info)

        # 绑定事件
        self.sector_listbox.bind('<<ListboxSelect>>', on_sector_select)

    def refresh_sector_list(self):
        """刷新板块列表"""
        # 清空现有的板块列表
        self.sector_listbox.delete(0, tk.END)
        
        # 从数据库获取所有板块
        sectors = self.db.get_all_sectors()
        for sector in sectors:
            sector_info = f"{sector['sector_id']} - {sector['sector_name']} ({sector['sector_type']})"
            self.sector_listbox.insert(tk.END, sector_info)

    def show_sector_fund_flow(self):
        """显示板块资金流向"""
        try:
            # 获取板块内的股票列表
            stocks = self.db.get_sector_stocks(sector_id)
            if not stocks:
                messagebox.showinfo("提示", "该板块暂无股票！")
                return
            
            # 创建新窗口
            self.fund_flow_window = tk.Toplevel(self.master)
            self.fund_flow_window.title(f"{sector_name} - 资金流向")
            self.fund_flow_window.geometry("800x600")
            self.fund_flow_window.transient(self.master)
            
            # 设置查询状标志
            self.current_query = True
            self.window_exists = True  # 添加窗口存在标志
            
            # 添加窗口关闭事件处理
            def on_closing():
                self.current_query = False
                self.window_exists = False  # 设置窗口不存在标志
                self.fund_flow_window.destroy()
                # 恢复按钮状态
                self.sector_flow_btn['state'] = 'normal'
                self.search_btn['state'] = 'normal'
            
            self.fund_flow_window.protocol("WM_DELETE_WINDOW", on_closing)
            
            # 创建主容器
            main_frame = ttk.Frame(self.fund_flow_window, padding="10")
            main_frame.pack(fill=tk.BOTH, expand=True)
            
            # 创建进度区域
            progress_frame = ttk.Frame(main_frame)
            progress_frame.pack(fill=tk.X, pady=(0, 10))
            
            # 进度条
            self.progress_var = tk.DoubleVar()
            self.progress_bar = ttk.Progressbar(
                progress_frame,
                length=300,
                mode='determinate',
                variable=self.progress_var
            )
            self.progress_bar.pack(side=tk.LEFT, padx=5)
            
            # 状态标签
            self.status_label = ttk.Label(progress_frame, text="正在查询...")
            self.status_label.pack(side=tk.LEFT, padx=5)
            
            # 创建文本区域
            text_frame = ttk.Frame(main_frame)
            text_frame.pack(fill=tk.BOTH, expand=True)
            
            self.text_area = tk.Text(
                text_frame,
                height=20,
                width=80,
                font=("Microsoft YaHei UI", 10)
            )
            self.text_area.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            
            # 添加滚动条
            scrollbar = ttk.Scrollbar(text_frame)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            # 配置文本区域和滚动条
            self.text_area.config(yscrollcommand=scrollbar.set)
            scrollbar.config(command=self.text_area.yview)
            
            # 开始查询
            self.text_area.insert(tk.END, f"开始查询板块 {sector_name} 的资金流向...\n\n")
            self.fund_flow_window.after(100, lambda: self.update_fund_flow_data(stocks))
            
        except Exception as e:
            logging.error(f"显示板块资金流向失败: {str(e)}")
            messagebox.showerror("错误", f"显示资金流向失败: {str(e)}")
            self.stop_sector_fund_flow()

    def update_fund_flow_data(self, stocks):
        """更新资金流向数据"""
        try:
            if not hasattr(self, 'fund_flow_window') or not self.current_query:
                return
            
            total_stocks = len(stocks)
            processed_stocks = 0
            
            for stock in stocks:
                # 检查窗口是否还存在
                if not self.window_exists or not self.current_query:
                    break
                
                try:
                    # 使用频率限制器
                    rate_limiter.wait()
                    
                    # 获取资金流向数据
                    df = pro.moneyflow(
                        ts_code=stock['stock_code'],
                        start_date=(datetime.now() - timedelta(days=30)).strftime('%Y%m%d'),
                        end_date=datetime.now().strftime('%Y%m%d')
                    )
                    
                    if not df.empty:
                        # 计算资金流向统计
                        net_mf = df['net_mf_amount'].sum() / 10000  # 转换为万元
                        main_net_mf = df['net_mf_main'].sum() / 10000
                        retail_net_mf = df['net_mf_retail'].sum() / 10000
                        
                        # 更新显示（检查窗口是否存在）
                        if self.window_exists:
                            self.text_area.insert(tk.END, 
                                f"股票: {stock['stock_code']}\n"
                                f"净流入: {net_mf:,.2f}万\n"
                                f"主力净流入: {main_net_mf:,.2f}万\n"
                                f"散户净流入: {retail_net_mf:,.2f}万\n"
                                f"{'-'*40}\n"
                            )
                            self.text_area.see(tk.END)
                    else:
                        if self.window_exists:
                            self.text_area.insert(tk.END, 
                                f"股票: {stock['stock_code']} - 无数据\n"
                                f"{'-'*40}\n"
                            )
                            self.text_area.see(tk.END)
                
                    # 更新进度（检查窗口是否存在）
                    processed_stocks += 1
                    if self.window_exists:
                        self.progress_var.set(processed_stocks / total_stocks * 100)
                        self.status_label.config(text=f"已处理: {processed_stocks}/{total_stocks}")
                        self.fund_flow_window.update()
                
                except Exception as e:
                    if self.window_exists:
                        logging.error(f"获取股票 {stock['stock_code']} 资金流向失败: {str(e)}")
                        self.text_area.insert(tk.END, 
                            f"股票: {stock['stock_code']} - 获取失败: {str(e)}\n"
                            f"{'-'*40}\n"
                        )
                        self.text_area.see(tk.END)
                    continue
            
            # 完成处理（检查窗是否存在）
            if self.window_exists and self.current_query:
                self.text_area.insert(tk.END, "\n查询完成！\n")
                self.text_area.see(tk.END)
                self.status_label.config(text="查询完成")
        
        except Exception as e:
            logging.error(f"更新资金流向数据失败: {str(e)}")
            if self.window_exists:
                self.text_area.insert(tk.END, f"\n更新失败: {str(e)}\n")
                self.text_area.see(tk.END)
                self.status_label.config(text="查询失败")

    def stop_sector_fund_flow(self):
        """停止板块资金流向查询"""
        try:
            # 停止查询
            self.current_query = False
            self.window_exists = False  # 设置窗口不存在标志
            
            # 关闭窗口
            if hasattr(self, 'fund_flow_window'):
                self.fund_flow_window.destroy()
                delattr(self, 'fund_flow_window')
            
            logging.info("板块资金流向查询已停止")
        
        except Exception as e:
            logging.error(f"停止板块资金流向查询失败: {str(e)}")

    def __del__(self):
        """析构数"""
        # 停止所有操作
        self.stop_current_operation()
        # 断开数据库连接
        self.db.disconnect()

if __name__ == "__main__":
    root = tk.Tk()
    app = StockSectorApp(root)
    root.mainloop()