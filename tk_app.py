import tkinter as tk
from tkinter import ttk, messagebox
import multiprocessing
import sv_ttk  # 导入 Sun-Valley-ttk-theme 样式美化
from datetime import datetime
from tkinter import messagebox
import os
import json
from tkinter import filedialog
import judge_stock
from backend import backend_process_shanghai
from backend_shenzhen import backend_process_shenzhen
import sys

#定义两个全局变量#储存左边选中的文件路径
submit_button = None
selected_pdf_path = None


class MainApp:
    def __init__(self, root):
        self.root = root  #Tkinter 主窗口对象
        self.root.title("上交所深交所年报自动提取程序")   #主窗口标题和大小
        self.root.geometry("1300x800")

        #设置Sun-Valley主题
        sv_ttk.set_theme("light")

        # 创建主窗口的三个区域
        self.left_frame = tk.Frame(self.root, width=300, height=800, bg="white")
        self.middle_frame = tk.Frame(self.root, width=350, height=800, bg="white")
        self.right_frame = tk.Frame(self.root, width=450, height=800, bg="white")



        # 使用 grid 布局
        self.left_frame.grid(row=0, column=0, sticky="nsew")
        self.middle_frame.grid(row=0, column=1, sticky="nsew")
        self.right_frame.grid(row=0, column=2, sticky="nsew")

        # 配置列权重
        self.root.grid_columnconfigure(0, weight=1)
        self.root.grid_columnconfigure(1, weight=1)
        self.root.grid_columnconfigure(2, weight=1)
        self.root.grid_rowconfigure(0, weight=1)

        # 初始化三个区域 增加参数在这个位置
        self.left_area = LeftArea(self.left_frame, self)
        # self.middle_area = MiddleArea(self.middle_frame, parent_conn)# 直接传递管道
        # # 将 right_area 传递给 MiddleArea
        #self.middle_area = MiddleArea(self.middle_frame, parent_conn, self.right_frame)
        # self.right_area = RightArea(self.right_frame)
        self.right_area = RightArea(self.right_frame)
        self.middle_area = MiddleArea(self.middle_frame,parent_conn,self.right_area) # 传递 RightArea 实例


        #绑定全局快捷键
        self.bind_global_shortcuts()


        #加载上次保存的文件夹路径
        self.load_last_folder_path()
        # # 加载用户设置
        # self.load_settings()

        # # 启动消息监听
        # self.poll_messages()



        self.bind_global_shortcuts()
        self.load_last_folder_path()

    def load_last_folder_path(self):
        """加载上次保存的文件路径，编码如果不对可以去掉"""
        try:
            with open("folder_path.json", "r", encoding="utf-8") as f:
                folder_path = json.load(f)
            self.left_area.update_folder_path(folder_path)
            self.left_area.refresh_files()
        except FileNotFoundError:
            pass


    # def save_settings(self):
    #     """保存参数设置"""
    #     settings = {
    #         "annual_params":{
    #             param : entry.get() if isinstance(entry,ttk.Entry) else entry.get()
    #             for
    #
    #     }



    def bind_global_shortcuts(self):
        # 绑定快捷键到root（根窗口）上，快捷键要对全局使用，无论是左、中、还是右的区域，ctrl+c复制选中内容，ctrl+a全选
        self.root.bind("<Control-a>", self.global_select_all)
        self.root.bind("<Control-A>", self.global_select_all)
        self.root.bind("<Control-c>", self.global_copy_selected)
        self.root.bind("<Control-C>", self.global_copy_selected)

    def global_select_all(self, event):
        """全局选择 widget=控件 """
        focused_widget = self.root.focus_get() #检查焦点所在区域
        if focused_widget in self.left_area.treeview.get_children():
            self.left_area.select_all(event)
        elif focused_widget.winfo_parent() == str(self.middle_frame):
            self.middle_area.select_all(event)
        elif focused_widget.winfo_parent() == str(self.right_frame):
            self.right_area.select_all(event)

    def global_copy_selected(self, event):
        """"全局全选"""
        focused_widget = self.root.focus_get()
        if focused_widget in self.left_area.treeview.get_children():
            self.left_area.copy_selected_row(event)
        elif focused_widget.winfo_parent() == str(self.middle_frame):
            self.middle_area.copy_selected_row(event)
        elif focused_widget.winfo_parent() == str(self.right_frame):
            self.right_area.copy_selected_row(event)
        #winfo方法、返回当前控件的父控件路径（字符串形式表示），比较父控件路径是否与某个区域frame相同

class LeftArea:
    def __init__(self,frame,main_app): #Python 中的构造函数必须是 __init__
        self.frame = frame
        self.main_app =main_app  #获取Mainapp实例，用于更新全局变量
        self.folder_path = r"C:"       #默认文件夹路径，如果没有名称为C盘路径，每次打开都会报错
        self.selected_file_path = None #当前选中文件路径，默认为None
        self.create_widgets () #创建桌面控件

    def on_file_selected(self, event):
        """当用户选中文件时，更新 MiddleArea 的输入框"""
        selected_items = self.treeview.selection()
        if selected_items:  # 如果有选中的项目
            filename = self.treeview.item(selected_items[0], "values")[0]
            file_path = os.path.join(self.folder_path, filename)
            file_path = file_path.replace('\\', '/')  # 将反斜杠替换为正斜杠，确保路径安全
            # 直接调用 MiddleArea 的方法更新输入框
            self.main_app.middle_area.update_pdf_path(file_path)
        else:
            # 如果没有选中文件，清空中间区域的输入框
            self.main_app.middle_area.update_pdf_path("")


    def update_folder_path(self,folder_path):
        """"
        更新文件夹路径并刷新文件列表
        param folder_path:新的文件夹路径
        """
        self.folder_path = folder_path   #更新文件夹路径
        self.path_entry.delete(0, tk.END)  #清空路径输入框
        self.path_entry.insert(0,self.folder_path) #在路径输入框，显示新的文件夹路径
        self.refresh_files() #刷新文件列表

    def create_widgets(self):
        """创建桌面控件：文件夹显示，文件列表，按钮区域"""
        #文件夹路径显示
        self.path_label = ttk.Label(self.frame,text="当前文件夹路径")
        self.path_label.pack(pady=5)
        self.path_entry = ttk.Entry(self.frame,width=40) #创建路径输入框

        self.path_entry.insert(0,self.folder_path)#在输入框中显示默认文件夹路径
        self.path_entry.pack(pady=5)

        #文件列表
        treeview_frame = tk.Frame(self.frame,width=300, height=800, bg="white") #文件列表的容器
        treeview_frame.pack(fill="both",expand=True) #设置容器的填充和扩展方式

        # Treeview内一共两列，一列显示文件名，一列显示时间
        self.treeview = ttk.Treeview(treeview_frame,show ="headings",columns=("filename","modified_date"))

        #设置两列的文件名
        self.treeview.heading("filename",text="文件名")
        self.treeview.heading("modified_date",text="修改日期")
        #设置两列的宽度,第三个参数,stretch=tk.NO 就代表不会随着窗口伸缩
        self.treeview.column("filename",width=170)
        self.treeview.column("modified_date",width=130)

        self.treeview.grid(row=0,column=0,sticky="nsew") #使用网格布局放置Treeview

        #创建滚动条
        yscroll = ttk.Scrollbar(treeview_frame,orient="vertical",command=self.treeview.yview)
        yscroll.grid(row=0,column=1,sticky="ns")
        xscroll = ttk.Scrollbar(treeview_frame,orient="horizontal",command=self.treeview.xview)
        xscroll.grid(row=1,column=0,sticky="ew")

        #将水平滚动条和竖直滚动条与Treeview关联
        self.treeview.configure(yscrollcommand=yscroll.set,xscrollcommand=xscroll.set)
        #设置网格区域的列和行权重
        treeview_frame.rowconfigure(0,weight=1)
        treeview_frame.columnconfigure(0,weight=1)

        #按钮区域
        button_frame = ttk.Frame(self.frame) #创建按钮容器
        button_frame.pack(pady=10)           #设置按钮容器的外边距

        #创建刷新按钮
        self.refresh_button = ttk.Button(button_frame,text="文件刷新",command=self.refresh_files)
        self.refresh_button.pack(side="left",padx=5) #使用左侧布局并设置外边距

        #创建更改文件夹路径按钮
        self.change_folder_button =ttk.Button(button_frame,text="更改文件夹路径",command = self.change_folder_path)
        self.change_folder_button.pack(side="left",padx=5)  #使用左侧并设置按钮外边距

        #刷新文件列表
        self.refresh_files() #在初始化时刷新文件列表

        #绑定Treeview选中事件
        self.treeview.bind("<<TreeviewSelect>>",self.on_file_selected)

    def refresh_files(self):
        """刷新文件列表，从当前文件夹路径中获取文件信息并显示在Treeview"""
        self.treeview.delete(*self.treeview.get_children())

        try:
            files = os.listdir(self.folder_path) #获取当前的文件路径
            #按照文件修改时间降序排序，将文件名和修改日期插入到Treeview控件

            #过滤出文件，并获取修改时间
            files = [(f,os.path.getmtime(os.path.join(self.folder_path,f)))for f in files if os.path.isfile(os.path.join(self.folder_path,f))]
            #sort 排序/整理,按照日期降序排序
            files.sort(key=lambda x:x[1], reverse=True)
            for filename,_ in files:
                # modified_date = datetime.fromtimestamp(os.path.getmtime(os.path.join(self.folder_path, filename))).strftime("%Y-%m-%d %H:%M")
                modified_date = datetime.fromtimestamp(os.path.getmtime(os.path.join(self.folder_path, filename))).strftime("%Y-%m-%d %H:%M")#格式化修改日期
                #将文件信息插入Treeview
                self.treeview.insert("","end",values=(filename,modified_date))

        except FileNotFoundError:
            messagebox.showerror("错误","文件夹路径无效，请重新选择!")

    def change_folder_path(self):
        """更改文件夹路径，通过文件对话框选择新的文件夹路径"""
        new_folder_path = filedialog.askdirectory() #打开文件夹选择对话框
        if new_folder_path: #如果用户选了新的文件夹路径
            self.folder_path = new_folder_path      #更新文件夹路径
            self.path_entry.delete(0,tk.END)   #清空路径输入框

            self.path_entry.insert(0,self.folder_path)  #在路径输入框显示新的文件夹路径
            self.refresh_files()

            #保存新的文件夹路径
            with open ("folder_path.json","w") as f:
                json.dump(self.folder_path,f)  #将新的文件夹路径保存到文件中


class MiddleArea:
    def __init__(self, frame,conn, right_area):  # 添加 right_area 参数
        global submit_button  # 声明使用全局变量
        self.frame = frame
        self.create_widgets()
        self.conn = conn    #利用管道定义一个类，实现消息传递  用于与后端通信的管道

        self.right_area = right_area  # 保存 RightArea 的实例
        # 启动消息监听
        self.poll_messages()

    def poll_messages(self):
        """定时检查管道是否有新消息方式：轮询"""
        if self.conn.poll():  #设置超时时间为0.1秒
            #从管道中获取消息
            message = self.conn.recv()
            if isinstance(message,str): #如果是字符串，认为是print消息
                self.right_area.display_message(message)
            else:
                # messagebox.showinfo("处理结果", f"后端返回: {message}")
                pass
        #从root改成什么？frame？
        self.frame.after(100, self.poll_messages)



    #创建界面：create_widgets
    def create_widgets(self):
        daily_label = ttk.Label(self.frame, text="每日参数", font=("Simhei", 20, 'bold'))
        daily_label.pack(pady=10)

        current_date = datetime.now().date()
        sql_date = datetime(current_date.year - 2, 12, 31)
        JJRQ_date = datetime(current_date.year - 1, 12, 31)

        self.daily_params = {}
        daily_params_data = [
            ("EPBH", "EP编号"),
            ("pdf_path", "文件路径"),
            ("XXFBRQ", "发布日期"),
            ("XXLL", "信息来源"),
        ]

        #获取当前文件夹路径
        def get_resource_path(relative_path):
            try:
                # PyInstaller 创建的临时文件夹
                base_path = sys._MEIPASS
            except Exception:
                # 脚本运行时的当前目录
                base_path = os.path.abspath(".")
            return os.path.join(base_path, relative_path)

        for param, param_type in daily_params_data:
            frame = ttk.Frame(self.frame)
            frame.pack(fill="x", padx=20, pady=5)

            #param_type作为显示的名称
            label = ttk.Label(frame, text=f"{param_type}:")
            label.pack(side="left", padx=10)

            if param_type == "EP编号":
                entry = tk.Entry(frame, width=30)
                entry.pack(side="left", padx=10)
            elif param_type == "文件路径":
                entry = ttk.Entry(frame, width=30)
                entry.pack(side="left", padx=10)
                self.pdf_entry = entry
                # # 如果全局变量 selected_pdf_path 有值，则设置到 pdf_path 输入框中
                # if selected_pdf_path is not None:
                #     entry.insert(0,selected_pdf_path)

            elif param_type == "发布日期":
                entry = ttk.Entry(frame, width=30)
                entry.insert(0, datetime.now().strftime("%Y-%m-%d"))
                entry.pack(side="left", padx=10)
            elif param_type == "信息来源":
                entry = ttk.Entry(frame, width=30)
                entry.pack(side="left", padx=10)

            self.daily_params[param] = entry

        self.submit_button = ttk.Button(self.frame, text="提交", command=self.submit_params)
        self.submit_button.pack(pady=20)

        annual_label = ttk.Label(self.frame, text="年度参数配置", font=("SimHei", 14, 'bold'))
        annual_label.pack(pady=10)

        self.annual_params = {}
        annual_params_data = [
            ("JJRQ", "sql日期筛选", sql_date.strftime("%Y-%m-%d")),
            ("JZRQ", "截止日期", JJRQ_date.strftime("%Y-%m-%d")),
            ("output_file", "文件输出路径", "D:\\WenJianDaoChu"),
            ("mapping_path", "映射表路径", get_resource_path("mapping_table.xlsx")),
            ("threshold", "高阈值", "75"),
            ("low_threshold", "低阈值", "20"),
            ("threshold_double", "成本分析表的阈值", "60"),
            ("get_high", "是否获取高值", True),
            ("get_medium", "是否获取中值", True),
            ("get_low", "是否获取低值", False),
            ("message_only_wrong", "是否仅显示错误信息", False),
        ]

        for param, param_type, default in annual_params_data:
            frame = ttk.Frame(self.frame)
            frame.pack(fill="x", padx=20, pady=5)

            label = ttk.Label(frame, text=f"{param_type}:")
            label.pack(side="left")

            if param_type in ["文本输入", "文件输出路径", "映射表路径", "sql日期筛选", "截止日期", "高阈值", "低阈值", "成本分析表的阈值"]:
                entry = ttk.Entry(frame, width=30)
                entry.insert(0, default)
                entry.pack(side="left", padx=10)
                self.annual_params[param] = entry
            elif param_type in ["是否获取高值", "是否获取中值", "是否获取低值", "是否仅显示错误信息"]:
                var = tk.BooleanVar(value=default)
                checkbox = ttk.Checkbutton(frame, variable=var, onvalue=True, offvalue=False)
                checkbox.pack(side="left", padx=10)
                self.annual_params[param] = var

        #AttributeError: 'MiddleArea' object has no attribute 'accurcy_data'
        #确保在定义一个对象之前，它已被初始化
        self.accuracy_data = ttk.Frame(self.frame) #初始化 self.accuracy_data

        # #这里开辟一个窗口我期望可视化的展示 冗余率：53.94%覆盖率: 86.34%正确率: 86.34%错误率: 0.00%
        # self.accuracy_data.pack(fill="x", padx=20, pady=5)
        # #创建数据框展示区域
        # ttk.Label(self.accuracy_data,text="精确度",font=("SimHei", 1, 'bold')).pack(pady=0)

        # #创建一个 Text 控件用于显示精确度数据
        # self.accuracy_text =tk.Text(self.accuracy_data,height=3,width=80,state="disabled",bg="white")
        # self.accuracy_text.pack(side="left", padx=1)

    # def update_accracy(self,accuracy_data):
    #     """更新精确度数据显示,在回传回来数据之后会记录精确度的数值"""
    #     self.accuracy_text.configure(state="normal")
    #     self.accuracy_text.delete(1.0, tk.END)  #清空旧数据
    #     for accuracy,cover in accuracy_data.items():
    #         self.accuracy_text.insert(tk.END,f"{accuracy}:{cover}\n")
    #     self.accuracy_text.configure(state="disabled")


    def update_pdf_path(self, new_path):
        """动态更新 pdf_path 输入框的内容"""
        if self.pdf_entry:
            self.pdf_entry.delete(0, tk.END)
            self.pdf_entry.insert(0, new_path)


    def submit_params(self):
        daily_params = {param: entry.get() for param, entry in self.daily_params.items()}
        annual_params = {param: entry.get() if isinstance(entry, ttk.Entry) else entry.get() for param, entry in self.annual_params.items()}
        all_params = {**daily_params, **annual_params}

        if not all_params:
            messagebox.showerror("错误", "请输入内容")
            return

        # 获取 pdf_path
        pdf_path = daily_params.get("pdf_path")
        if not pdf_path or not os.path.exists(pdf_path):
            messagebox.showerror("错误", f"PDF 文件路径无效或文件不存在：{pdf_path}")
            return

        try:
            # 判断交易所
            is_shanghai = judge_stock.judge_stock_change(pdf_path)
        except ValueError as e:
            messagebox.showerror("错误", str(e))
            return

            # 根据交易所选择后端进程
            #动态导入会导致打包的时候报错  那么就不用-F
        # if is_shanghai:
        #     from backend import backend_process
        # else:
        #     from backend_shenzhen import backend_process
        if is_shanghai:
            backend_process = backend_process_shanghai
        else:
            backend_process = backend_process_shenzhen

        # 启动后端进程
        backend_process = multiprocessing.Process(target=backend_process, args=(child_conn,))
        backend_process.start()


        # 发送数据到后端
        self.conn.send(all_params)
        self.submit_button.config(state="disabled")  # 禁用提交按钮

        # 接收后端处理结果
        result = self.conn.recv()
        messagebox.showinfo("自动化助手", f"{result}")

        #后端复原
        # 确保后端进程和管道在任务完成后正确关闭
        # parent_conn.close()
        # child_conn.close()  管道应该可以不关闭
        backend_process.terminate()  # 强制终止后端进程
        # 重新启用提交按钮
        self.submit_button.config(state="normal")


#更新
 #在此调用函数判断函数，然后根据return的结果选择不同的后端进程
class RightArea:
    def __init__(self, frame):
        self.frame = frame
        self.create_widgets()

    def create_widgets(self):
        self.text_widget = tk.Text(self.frame, wrap="word", state="disabled", bg="white")
        self.text_widget.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)  # 使用grid布局

        scrollbar = ttk.Scrollbar(self.frame, command=self.text_widget.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")  # 使用grid布局

        self.text_widget.configure(yscrollcommand=scrollbar.set)

        # 配置frame的网格布局权重，确保text_widget可以扩展
        self.frame.grid_rowconfigure(0, weight=1)
        self.frame.grid_columnconfigure(0, weight=1)

        # 定义颜色标签
        self.text_widget.tag_configure("green", foreground="green")
        self.text_widget.tag_configure("red", foreground="red")
        self.text_widget.tag_configure("black", foreground="black")


    def display_message(self, message):
        """将消息显示在消息区域"""
        self.text_widget.configure(state="normal")
        self.apply_color(self.parse_ansi(message))  # 使用 apply_color 方法处理彩色文本
        self.text_widget.configure(state="disabled")
        self.text_widget.see("end")

    def parse_ansi(self, text):
        """解析 ANSI 色彩代码并返回文本和颜色的列表"""
        color_map = {
            "\033[92m": "green",  # 绿色
            "\033[91m": "red",    # 红色
            "\033[0m": "black" ,   # 重置颜色
        }
        result = []
        current_color = "black"  # 默认颜色
        i = 0
        while i < len(text):
            if text[i:i + 5] in color_map:
                current_color = color_map[text[i:i + 5]]
                i += 5
            else:
                result.append((text[i], current_color))
                i += 1
        return result

    def apply_color(self, text_color_pairs):
        """将文本和颜色应用到 Text 组件"""
        for text, color in text_color_pairs:
            self.text_widget.insert("end", text, color)
        self.text_widget.insert("end", "\n")  # 添加换行

    def flush(self):
        """实现 flush 方法，避免 AttributeError"""
        pass  # 这里可以什么都不做


class RedirectText:
    """重定向输出到 Text 组件的类"""
    def __init__(self, text_widget):
        self.text_widget = text_widget

    def write(self, message):
        """将消息写入 Text 组件"""
        self.text_widget.configure(state="normal")
        self.text_widget.insert("end", message)
        self.text_widget.configure(state="disabled")
        self.text_widget.see("end")

    def flush(self):
        """实现文件接口的 flush 方法"""
        pass

#定义居中函数，注意//才表示除以
def center_window(root):
    root.update_idletasks()  # 确保窗口尺寸已更新
    width = root.winfo_width()
    height = root.winfo_height()
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = (screen_width - width) // 2
    y = (screen_height - height) // 2
    root.geometry(f"+{x}+{y}")

if __name__ == '__main__':
    parent_conn, child_conn = multiprocessing.Pipe()  # 创建双向管道
    # 前端界面部分
    # 创建 Tkinter 主窗口
    root = tk.Tk()
    app = MainApp(root)  # 创建主界面

    # 启动 Tkinter 主循环
    root.mainloop()


