import tkinter as tk
from tkinter import ttk, messagebox
from stock_viewer import StockViewer
from stock_sector import StockSectorApp
from stock_fund_flow import StockFundFlow
from computing_power import ComputingPower
from stock_index import StockIndex
import logging
import sys
import traceback

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class StockManagementSystem(tk.Tk):
    def __init__(self):
        try:
            super().__init__()
            logging.info("开始初始化股票管理系统...")
            
            # 添加析构标记
            self.is_shutting_down = False
            
            # 添加关闭窗口的处理
            self.protocol("WM_DELETE_WINDOW", self.on_closing)
            
            self.title("股票管理系统")
            self.geometry("1400x800")
            self.minsize(1200, 700)
            
            logging.info("设置主题样式...")
            self.setup_styles()
            
            logging.info("创建主布局...")
            self.create_layout()
            
            # 初始化功能模块
            self.stock_viewer = None
            self.sector_manager = None
            self.fund_flow = None
            self.computing = None
            self.stock_index = None
            
            self.report_callback_exception = self.handle_exception
            self.current_module = None
            self.notebook.bind('<<NotebookTabChanged>>', self.on_tab_change)
            
            logging.info("股票管理系统初始化完成")
            
        except Exception as e:
            error_msg = traceback.format_exc()
            logging.error(f"初始化失败: \n{error_msg}")
            messagebox.showerror("错误", f"程序初始化失败: {str(e)}\n请查看日志文件获取详细信息。")
            raise

    def handle_exception(self, exc_type, exc_value, exc_traceback):
        """处理未捕获的异常"""
        error_msg = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        # 获取发生异常的文件名和行号
        tb = traceback.extract_tb(exc_traceback)
        filename, lineno, func, text = tb[-1]
        logging.error(f"未捕获的异常 [在文件 {filename} 第 {lineno} 行]:\n{error_msg}")
        messagebox.showerror("错误", 
                           "程序发生错误，详细信息已写入日志文件。\n" +
                           f"错误位置: {filename}:{lineno}\n" +
                           f"错误类型: {exc_type.__name__}\n" +
                           f"错误信息: {str(exc_value)}")

    def setup_styles(self):
        """设置主题样式"""
        style = ttk.Style()
        
        # 全局字体和颜色
        bg_color = "#f0f2f5"
        primary_color = "#1890ff"
        text_color = "#333333"
        
        # 主框架样式
        style.configure("Main.TFrame", background=bg_color)
        
        # 侧边栏样式
        style.configure("Sidebar.TFrame", background=bg_color)
        style.configure("Sidebar.TLabel",
                       background=bg_color,
                       foreground=text_color,
                       font=("Microsoft YaHei UI", 12))
        
        # 导航按钮样式
        style.configure("Nav.TButton",
                       font=("Microsoft YaHei UI", 10),
                       background=bg_color,
                       foreground=text_color,
                       padding=(15, 8),
                       width=20)
        style.map("Nav.TButton",
                 background=[('selected', primary_color), ('active', '#e6f7ff')],
                 foreground=[('selected', 'white'), ('active', primary_color)])
        
        # 内容区域样式
        style.configure("Content.TFrame", background="white")
        style.configure("Content.TLabel",
                       background="white",
                       font=("Microsoft YaHei UI", 12))
        
        # 页面标题样式
        style.configure("Title.TLabel",
                       background="white",
                       font=("Microsoft YaHei UI", 16, "bold"),
                       foreground=text_color)
        
        # 卡片样式
        style.configure("Card.TFrame",
                       background="white",
                       relief="solid",
                       borderwidth=1)
        
        # Notebook 样式
        style.configure("lefttab.TNotebook", 
                       background=bg_color,
                       tabposition='wn')  # west-north
        style.configure("lefttab.TNotebook.Tab",
                       font=("Microsoft YaHei UI", 10),
                       padding=(15, 8),
                       width=20)
        style.map("lefttab.TNotebook.Tab",
                 background=[("selected", primary_color), ("active", "#40a9ff")],
                 foreground=[("selected", "white"), ("active", "white")])
    
    def create_layout(self):
        """创建主布局"""
        # 创建主容器
        self.container = ttk.Frame(self, style="Main.TFrame")
        self.container.pack(fill=tk.BOTH, expand=True)
        
        # 创建左侧导航栏
        self.sidebar = ttk.Frame(self.container, style="Sidebar.TFrame", width=160)
        self.sidebar.pack(side=tk.LEFT, fill=tk.Y)
        self.sidebar.pack_propagate(False)
        
        # 创建导航按钮
        nav_buttons = [
            ("股票管理系统", None),  # 标题
            ("欢迎", 0),
            ("股票数据管理", 1),
            ("板块管理", 2),
            ("资金流向", 3),
            ("股票指数", 4)
        ]
        
        for text, tab_id in nav_buttons:
            if tab_id is None:
                # 标题使用Label
                ttk.Label(
                    self.sidebar,
                    text=text,
                    style="Sidebar.TLabel",
                    font=("Microsoft YaHei UI", 12, "bold")
                ).pack(pady=(15, 20))
            else:
                # 导航按钮
                btn = ttk.Button(
                    self.sidebar,
                    text=text,
                    style="Nav.TButton",
                    command=lambda t=tab_id: self.notebook.select(t)
                )
                btn.pack(pady=2)
                
                # 如果是第一个按钮（欢迎），默认选中
                if tab_id == 0:
                    btn.state(['selected'])
        
        # 创建内容区域
        self.content_container = ttk.Frame(self.container, style="Main.TFrame")
        self.content_container.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 20))
        
        # 创建 Notebook
        self.notebook = ttk.Notebook(self.content_container)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=(0, 20), pady=20)
        
        # 隐藏Notebook的选项卡
        style = ttk.Style()
        style.layout('TNotebook.Tab', [])  # 隐藏选项卡
        
        # 创建欢迎页面
        self.welcome_frame = self.create_welcome_frame()
        self.notebook.add(self.welcome_frame, text="欢迎")
        
        # 创建股票数据管理页面
        self.viewer_frame = ttk.Frame(self.notebook, style="Content.TFrame")
        self.notebook.add(self.viewer_frame, text="股票数据管理")
        self.stock_viewer = StockViewer(self.viewer_frame)
        
        # 创建板块管理页面
        self.sector_frame = ttk.Frame(self.notebook, style="Content.TFrame")
        self.notebook.add(self.sector_frame, text="板块管理")
        self.sector_manager = StockSectorApp(self.sector_frame)
        
        # 创建资金流向页面
        self.fund_flow_frame = ttk.Frame(self.notebook, style="Content.TFrame")
        self.notebook.add(self.fund_flow_frame, text="资金流向")
        self.fund_flow = StockFundFlow(self.fund_flow_frame)
        
        # 创建股票指数页面
        self.index_frame = ttk.Frame(self.notebook, style="Content.TFrame")
        self.notebook.add(self.index_frame, text="股票指数")
        self.stock_index = StockIndex(self.index_frame)
        
        # 保存按钮引用
        self.nav_buttons = []
        for widget in self.sidebar.winfo_children():
            if isinstance(widget, ttk.Button):
                self.nav_buttons.append(widget)
    
    def on_tab_change(self, event):
        """处理标签页切换事件"""
        try:
            current_tab = self.notebook.select()
            tab_id = self.notebook.index(current_tab)
            tab_name = self.notebook.tab(current_tab)['text']
            logging.info(f"切换到标签页: {tab_name} (ID: {tab_id})")
            
            logging.info("停止当前模块功能...")
            self.stop_current_module()
            
            # 更新当前模块标记
            if tab_id == 1:
                self.current_module = 'stock_viewer'
                logging.info("进入股票数据管理模块")
                if hasattr(self.stock_viewer, 'update_running') and self.stock_viewer.update_running:
                    logging.info("停止股票数据更新")
                    self.stock_viewer.stop_update()
            elif tab_id == 2:
                self.current_module = 'sector_manager'
                if hasattr(self.sector_manager, 'current_operation'):
                    self.sector_manager.stop_current_operation()
            elif tab_id == 3:
                self.current_module = 'fund_flow'
                if hasattr(self.fund_flow, 'current_query'):
                    self.fund_flow.stop_current_query()
            elif tab_id == 4:
                self.current_module = 'stock_index'
            else:
                self.current_module = None
            
        except Exception as e:
            error_msg = traceback.format_exc()
            logging.error(f"标签页切换失败:\n{error_msg}")
            messagebox.showerror("错误", f"标签页切换失败: {str(e)}")
    
    def stop_current_module(self):
        """停止当前模块的功能"""
        if self.is_shutting_down:
            return  # 如果正在关闭程序，不执行模块停止
            
        try:
            if self.current_module == 'stock_viewer':
                if hasattr(self.stock_viewer, 'update_running') and self.stock_viewer.update_running:
                    logging.info("停止股票数据更新")
                    self.stock_viewer.stop_update()
                    
            elif self.current_module == 'sector_manager':
                if hasattr(self.sector_manager, 'current_operation'):
                    logging.info("停止板块管理操作")
                    self.sector_manager.stop_current_operation()
                    
            elif self.current_module == 'fund_flow':
                if hasattr(self.fund_flow, 'current_query'):
                    logging.info("停止资金流向查询")
                    self.fund_flow.stop_current_query()
                    
        except Exception as e:
            logging.warning(f"停止当前模块时发生警告: {str(e)}")
    
    def create_welcome_frame(self):
        """创建欢迎页面"""
        welcome_frame = ttk.Frame(self.notebook, style="Content.TFrame")
        
        # 创建欢迎卡片
        card = ttk.Frame(welcome_frame, style="Card.TFrame")
        card.pack(padx=20, pady=20, fill=tk.BOTH, expand=True)
        
        # 欢迎标题
        ttk.Label(card,
                 text="欢迎使用股票管理系统",
                 font=("Microsoft YaHei UI", 24, "bold"),
                 foreground="#1890ff").pack(pady=(40, 20))
        
        # 功能说明
        features = [
            ("股票数据管理", [
                "• 实时查看股票数据和最新更新时间",
                "• 支持数据库连接配置和测试",
                "• 一键更新所有股票数据",
                "• 支持定时自动更新功能",
                "• 分页显示，方便浏览",
                "• 自动跳过已有最新数据的股票"
            ]),
            ("板块管理", [
                "• 创建和管理自定义股票板块",
                "• 灵活添加和编辑板块内的股票",
                "• 支持单个和批量添加股票",
                "• 查看板块内个股涨跌幅统计",
                "• 展示板块每日成交额趋势",
                "• 可视化数据展示，支持缩放"
            ]),
            ("资金流向", [
                "• 查看个股资金流向详情",
                "• 展示主力资金净流入/流出",
                "• 分析大单、中单、小单资金",
                "• 支持自定义日期范围查询",
                "• 支持板块内所有个股资金分析",
                "• 数据可视化，直观展示趋势"
            ]),
            ("股票指数", [
                "• 功能开发中",
                "• 敬请期待..."
            ])
        ]
        
        for title, desc_list in features:
            feature_frame = ttk.Frame(card, style="Card.TFrame")
            feature_frame.pack(padx=40, pady=10, fill=tk.X)
            
            # 功能模块标题
            ttk.Label(feature_frame,
                     text=title,
                     font=("Microsoft YaHei UI", 16, "bold"),
                     foreground="#333333").pack(anchor=tk.W)
            
            # 功能描述列表
            desc_frame = ttk.Frame(feature_frame)
            desc_frame.pack(anchor=tk.W, pady=(5, 10))
            
            # 分两列显示功能描述
            left_frame = ttk.Frame(desc_frame)
            left_frame.pack(side=tk.LEFT, padx=(0, 20))
            right_frame = ttk.Frame(desc_frame)
            right_frame.pack(side=tk.LEFT)
            
            for i, desc in enumerate(desc_list[:3]):
                ttk.Label(left_frame,
                         text=desc,
                         font=("Microsoft YaHei UI", 11),
                         foreground="#666666").pack(anchor=tk.W, pady=3)
            
            for desc in desc_list[3:]:
                ttk.Label(right_frame,
                         text=desc,
                         font=("Microsoft YaHei UI", 11),
                         foreground="#666666").pack(anchor=tk.W, pady=3)
        
        # 添加版本信息
        version_frame = ttk.Frame(card)
        version_frame.pack(fill=tk.X, padx=40, pady=(20, 10))
        ttk.Label(version_frame,
                 text="版本: 1.0.0",
                 font=("Microsoft YaHei UI", 9),
                 foreground="#999999").pack(side=tk.LEFT)
        
        # 添加使用提示
        tip_frame = ttk.Frame(card)
        tip_frame.pack(fill=tk.X, padx=40, pady=(0, 20))
        ttk.Label(tip_frame,
                 text="提示: 使用左侧导航栏切换不同功能模块",
                 font=("Microsoft YaHei UI", 9),
                 foreground="#999999").pack(side=tk.LEFT)
        
        return welcome_frame

    def on_closing(self):
        """处理窗口关闭事件"""
        try:
            logging.info("开始关闭程序...")
            self.is_shutting_down = True
            
            # 安全地关闭各个模块
            self.safe_shutdown_module('stock_viewer')
            self.safe_shutdown_module('sector_manager')
            self.safe_shutdown_module('fund_flow')
            self.safe_shutdown_module('stock_index')
            
            logging.info("程序关闭完成")
            self.destroy()
            
        except Exception as e:
            error_msg = traceback.format_exc()
            logging.error(f"程序关闭时发生错误:\n{error_msg}")
            self.destroy()  # 确保窗口关闭

    def safe_shutdown_module(self, module_name):
        """安全地关闭指定模块"""
        try:
            module = getattr(self, module_name)
            if module is not None:
                logging.info(f"正在关闭模块: {module_name}")
                
                # 检查并关闭数据库连接
                if hasattr(module, 'db_connection') and module.db_connection:
                    try:
                        if hasattr(module.db_connection, 'is_connected') and module.db_connection.is_connected():
                            module.db_connection.close()
                            logging.info(f"{module_name} 数据库连接已关闭")
                    except Exception as e:
                        logging.warning(f"{module_name} 关闭数据库连接时发生警告: {str(e)}")
                
                # 检查并关闭连接池
                if hasattr(module, 'connection_pool'):
                    try:
                        if module.connection_pool:
                            while not module.connection_pool.empty():
                                conn = module.connection_pool.get()
                                if conn and hasattr(conn, 'is_connected') and conn.is_connected():
                                    conn.close()
                            logging.info(f"{module_name} 连接池已清空并关闭")
                    except Exception as e:
                        logging.warning(f"{module_name} 关闭连接池时发生警告: {str(e)}")
                
                # 停止所有运行中的操作
                if hasattr(module, 'stop_all_operations'):
                    module.stop_all_operations()
                
        except Exception as e:
            logging.warning(f"关闭模块 {module_name} 时发生警告: {str(e)}")

def main():
    try:
        logging.info("启动股票管理系统...")
        app = StockManagementSystem()
        logging.info("进入主循环...")
        app.mainloop()
    except Exception as e:
        error_msg = traceback.format_exc()
        logging.error(f"程序运行失败:\n{error_msg}")
        messagebox.showerror("错误", 
                           "程序运行失败，详细信息已写入日志文件。\n" +
                           f"错误信息: {str(e)}")
        input("按回车键退出...")
        sys.exit(1)

if __name__ == "__main__":
    main() 