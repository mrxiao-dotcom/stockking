import tkinter as tk
from tkinter import ttk
import tushare as ts
from config import TUSHARE_TOKEN
import pandas as pd
from datetime import datetime, timedelta
import threading
import logging

# 配置tushare
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

class ComputingPower:
    def __init__(self, master=None):
        if master is None:
            master = tk.Tk()
        self.master = master
        
        # 创建主框架
        self.main_frame = ttk.Frame(self.master)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # 创建标题
        self.create_title()
        
        # 创建股票列表
        self.create_stock_list()
        
        # 加载数据
        self.load_data()

    def create_title(self):
        """创建标题区域"""
        title_frame = ttk.Frame(self.main_frame)
        title_frame.pack(fill=tk.X, pady=(0, 20))
        
        ttk.Label(
            title_frame,
            text="算力租赁概念股",
            font=("Microsoft YaHei UI", 16, "bold"),
            foreground="#333333"
        ).pack(side=tk.LEFT)
        
        # 添加更新时间
        self.update_time_label = ttk.Label(
            title_frame,
            text="",
            font=("Microsoft YaHei UI", 10),
            foreground="#666666"
        )
        self.update_time_label.pack(side=tk.RIGHT)
        
        # 更新时间显示
        self.update_time()

    def create_stock_list(self):
        """创建股票列表"""
        # 创建表格
        columns = ("股票代码", "股票名称", "最新价", "涨跌幅", "成交额(万)")
        self.tree = ttk.Treeview(
            self.main_frame,
            columns=columns,
            show='headings',
            height=20,
            style="Custom.Treeview"
        )
        
        # 设置列标题和宽度
        column_widths = {
            "股票代码": 100,
            "股票名称": 120,
            "最新价": 80,
            "涨跌幅": 80,
            "成交额(万)": 100
        }
        
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=column_widths[col], anchor='center')
        
        # 设置表格样式
        style = ttk.Style()
        style.configure(
            "Custom.Treeview",
            font=("Microsoft YaHei UI", 11),
            rowheight=35
        )
        style.configure(
            "Custom.Treeview.Heading",
            font=("Microsoft YaHei UI", 11, "bold")
        )
        
        # 添加颜色标签
        self.tree.tag_configure("up", foreground="#ff4d4f")    # 上涨显示红色
        self.tree.tag_configure("down", foreground="#52c41a")  # 下跌显示绿色
        self.tree.tag_configure("zero", foreground="#666666")  # 平盘显示灰色
        
        # 添加滚动条
        scrollbar = ttk.Scrollbar(self.main_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        # 放置表格和滚动条
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def update_time(self):
        """更新时间显示"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.update_time_label.config(text=f"更新时间：{current_time}")
        self.master.after(1000, self.update_time)  # 每秒更新一次

    def load_data(self):
        """加载股票数据"""
        # 算力租赁概念股列表
        stocks = [
            ("立昂技术", "300603"),
            ("寒武纪-U", "688256"),
            ("常山北明", "000158"),
            ("青云科技-U", "688316"),
            ("佳力图", "603912"),
            ("中科曙光", "603019"),
            ("浙大网新", "600797"),
            ("科泰电源", "300153"),
            ("拓维信息", "002261"),
            ("美利云", "000815"),
            ("曙光数创", "301248"),
            ("网宿科技", "300017"),
            ("浪潮信息", "000977"),
            ("中青宝", "300052"),
            ("梦网科技", "002123"),
            ("华星创业", "300025"),
            ("盛弘股份", "300693"),
            ("润建股份", "002929"),
            ("铜牛信息", "300895"),
            ("科华数据", "002335"),
            ("ST易事特", "300376"),
            ("润泽科技", "301035"),
            ("东方国信", "300166"),
            ("新易盛", "300502"),
            ("中贝通信", "603220"),
            ("光环新网", "300383"),
            ("真视通", "002771"),
            ("数据港", "603881"),
            ("新炬网络", "605398"),
            ("超讯通信", "603322"),
            ("宝信软件", "600845"),
            ("杭钢股份", "600126"),
            ("中际旭创", "300308"),
            ("城地香江", "603887"),
            ("云赛智联", "600602"),
            ("派能科技", "688063"),
            ("ST证通", "002197"),
            ("中国联通", "600050"),
            ("深信服", "300454"),
            ("莲花控股", "600186"),
            ("奥飞数据", "300738"),
            ("首都在线", "300846"),
            ("万马科技", "300698"),
            ("*ST鹏博", "600804"),
            ("南兴股份", "002757")
        ]
        
        def update_stock_data():
            try:
                # 清空现有数据
                for item in self.tree.get_children():
                    self.tree.delete(item)

                # 显示加载提示
                loading_text = self.tree.insert("", tk.END, values=(
                    "数据加载中...", "", "", "", ""
                ))

                for name, code in stocks:
                    try:
                        # 获取实时行情数据
                        df = ts.get_realtime_quotes(code)
                        if not df.empty:
                            price = float(df['price'].iloc[0])
                            pre_close = float(df['pre_close'].iloc[0])
                            amount = float(df['amount'].iloc[0]) / 10000  # 转换为万元
                            
                            # 计算涨跌幅
                            change_pct = (price - pre_close) / pre_close * 100
                            
                            # 确定显示颜色
                            if change_pct > 0:
                                tag = "up"
                            elif change_pct < 0:
                                tag = "down"
                            else:
                                tag = "zero"
                            
                            # 插入数据
                            self.tree.insert("", tk.END, values=(
                                code,
                                name,
                                f"{price:.2f}",
                                f"{change_pct:+.2f}%",
                                f"{amount:.2f}"
                            ), tags=(tag,))
                
                    except Exception as e:
                        logging.error(f"获取股票 {code} 数据失败: {str(e)}")
                        # 插入错误提示行
                        self.tree.insert("", tk.END, values=(
                            code,
                            name,
                            "获取失败",
                            "-",
                            "-"
                        ))

                # 删除加载提示
                self.tree.delete(loading_text)
                
                # 更新时间显示
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.update_time_label.config(text=f"更新时间：{current_time}")
                
            except Exception as e:
                logging.error(f"更新股票数据失败: {str(e)}")
            finally:
                # 一分钟后再次更新
                self.master.after(60000, self.load_data)  # 60000毫秒 = 1分钟
        
        # 在新线程中更新数据
        threading.Thread(target=update_stock_data, daemon=True).start()
        
        # 每60秒更新一次数据
        self.master.after(60000, self.load_data) 