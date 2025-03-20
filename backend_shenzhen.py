import re
import os
#自定义模块
import pdf_load
import match
import export
import sql_normalize
from queue import Queue
import sys

#上海的后端程序,重定向版本
def backend_process_shenzhen(pipe_conn):
    print("后端程序启动")
    message_queue = Queue()

    class RedirectedPrint:
        def write(self, message):
            message_queue.put(message)

        def flush(self):
            pass

    sys.stdout = RedirectedPrint()

    try:
        while True:
            # 检查队列是否有消息需要发送
            if not message_queue.empty():
                messages = []
                while not message_queue.empty():
                    messages.append(message_queue.get())
                pipe_conn.send("\n".join(messages))

            # 接收数据并处理
            data = pipe_conn.recv()
            pdf_path = data.get("pdf_path")
            if not pdf_path or not os.path.exists(pdf_path):
                error_message = f"错误：pdf文件不存在{pdf_path}"
                print(error_message)
                pipe_conn.send(error_message)
                continue

            # print(f"后端接收到数据: {data}")
            result = main_shenzhen(data, pdf_file=None)
            print(f"处理结果: {result}")
            pipe_conn.send(result)
    except Exception as e:
        print(f"发生错误: {e}")
        pipe_conn.send(f"错误:{e}")
    finally:
        pipe_conn.close()
        print("后端进程退出")

#测试版
# import os
# import sys
#
# def backend_process(pipe_conn):
#     print("后端程序启动")
#
#     try:
#         while True:
#             # 接收数据并处理
#             data = pipe_conn.recv()
#             pdf_path = data.get("pdf_path")
#
#             # 检查 PDF 文件是否存在
#             if not pdf_path or not os.path.exists(pdf_path):
#                 error_message = f"错误：PDF 文件不存在 {pdf_path}"
#                 print(error_message, file=sys.stderr)  # 打印到标准错误输出
#                 pipe_conn.send(error_message)
#                 continue
#
#             # 打印接收到的数据（可选）
#             print(f"后端接收到数据: {data}")
#
#             # 调用处理函数
#             result = main_shenzhen(data, pdf_file=None)
#             print(f"处理结果: {result}")
#
#             # 将结果发送回前端
#             pipe_conn.send(result)
#     except Exception as e:
#         error_message = f"发生错误: {e}"
#         print(error_message, file=sys.stderr)  # 打印到标准错误输出
#         pipe_conn.send(error_message)
#     finally:
#         pipe_conn.close()
#         print("后端进程退出")


def main_shenzhen(params,pdf_file):
    """
    主函数，接收参数并进行处理。
    :param params: 一个字典，包含所有表单参数。
    :return: 处理结果的字符串。
    """
    # 提取参数
    EPBH = params['EPBH']
    pdf_path = params['pdf_path']
    XXFBRQ = params['XXFBRQ']
    XXLL = params['XXLL']
    JJRQ = params['JJRQ']
    JZRQ = params['JZRQ']
    output_file = params['output_file']
    mapping_path = params['mapping_path']
    threshold = int(params['threshold'])
    low_threshold = int(params['low_threshold'])
    threshold_double = int(params['threshold_double'])
    get_high = params['get_high']
    get_medium = params['get_medium']
    get_low = params['get_low']
    message_only_wrong = params['message_only_wrong']
    # columns_score_chose = params['columns_score_chose']  # 使用传入的

    # 处理逻辑
    result = (
        f"处理结果：\n"
        f"EPBH = {EPBH}\n"
        f"PDF路径 = {pdf_path}\n"
        f"信息发布日期 = {XXFBRQ}\n"
        f"信息来源 = {XXLL}\n"
        f"SQL筛选日期 = {JJRQ}\n"
        f"覆盖报告日期 = {JZRQ}\n"
        f"文件夹导出路径 = {output_file}\n"
        f"Threshold = {threshold}\n"
        f"Low Threshold = {low_threshold}\n"
        f"Threshold Double = {threshold_double}\n"
        f"获取高值 = {get_high}\n"
        f"获取中值 = {get_medium}\n"
        f"获取低值 = {get_low}\n"
        f"仅显示错误信息 = {message_only_wrong}"
    )

    mapping_file_path = mapping_path
    # 此模式是比较模式，数据更加容易比较
    columns_score1 = [
        "EP编号", "行业代码", "信息来源", "信息来源编码", "信息发布日期", "截止日期", "财政年度",
        "经营业务类型代码", "数据类目一", "数据类目一名称", "数据类目一代码", "数据类目二",
        "数据类目二名称", "数据类目二代码", "数据类目三", "数据类目三名称", "主体原始名称",
        "指标代码", "标准名称", "指标名称", "指标数据", "指标单位", "相似度分数", "精度", "pdf原始名称", "匹配代码-单位",
        "统计口径", "统计期间", "页码", "指标内容", "是否有效", "备注说明", "行编码"
    ]
    # 此模式是挂尾模式，数据更加容易比较
    columns_score2 = [
        "EP编号", "行业代码", "信息来源", "信息来源编码", "信息发布日期", "截止日期", "财政年度",
        "经营业务类型代码", "数据类目一", "数据类目一名称", "数据类目一代码", "数据类目二",
        "数据类目二名称", "数据类目二代码", "数据类目三", "数据类目三名称", "主体原始名称",
        "指标代码", "标准名称", "指标名称", "指标数据", "指标单位", "匹配代码-单位",
        "统计口径", "统计期间", "页码", "指标内容", "是否有效", "备注说明", "行编码", "相似度分数", "精度", "pdf原始名称",
    ]

    # 自定义偏好设置
    columns_score_chose = columns_score2  # excel导出的格式，可选 (columns_score1，columns_score2，columns)

    #定义颜色输出
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"

    # 检查文件夹路径是否存在，如果不存在则创建
    XXLL_processed = re.sub(r'[<>:"/\\|?*\s]', '', XXLL)
    if not os.path.exists(output_file):
        os.makedirs(output_file)  # 使用 os.makedirs() 可以创建多级目录
        print(f"文件夹路径之前不存在,故繇繇已自动创建 '{output_file}' 。")
    else:
        pass
    # ————————————————————————————————————————————1.读取sql数据————————————————————————————————————————————————
    data = sql_normalize.querysql(EPBH)
    # 定义需要清洗的字段
    fields_to_clean = {"数据类目一名称", "数据类目二名称", "数据类目三名称"}
    # 加载映射表
    mapping_dict = sql_normalize.load_mapping_table_sql(mapping_path)
    # 数据清洗和标准化
    sql_dic = sql_normalize.clean_and_standardize_data(data, fields_to_clean, mapping_dict)
    # 保存原始数据到 Excel
    max_page = export.sql_export(output_file, data, JJRQ, EPBH, XXLL_processed)

    # #————————————————————————————————————————————1.5读取pdf数据————————————————————————————————————————————————
    # 调用封装函数提取和清洗 PDF 数据
    mapping_dict_pdf = pdf_load.load_mapping_table(mapping_path)

    pdf_data = pdf_load.extract_clean_pdf(pdf_path, mapping_dict_pdf, 11, 35)  # 按照深交所的排版结构，至少需要到第10页；结束可能要35页


    # ————————————————————————————————深交所 2.营业收入构成———————————————————————————————————————
    # 筛选符合条件的 SQL 数据
    # 对数据进行分块，按照标准名称分成六类；对每一类的数据进行单独处理才不会导致数据混杂
    sql_filtered_table2_shenzhen1 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '销售收入' and item.get('指标单位') != '%')
        )
    ]
    sql_filtered_table2_shenzhen2 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '销售收入' and item.get('指标单位') == '%')
        )
    ]
    # 定义第一组模式（强匹配）
    start_pattern1 = [
        re.compile(r'^分产品$'),
    ]
    # 定义第二组备选模式（弱匹配）#其实这个要作为第三组备选,先测试
    start_pattern2 = [
        re.compile(r'^分行业$'),
    ]
    # 定义第三组备选模式（最终备用方案）
    start_pattern3 = [

        re.compile(r'^营业收入合计$'),
    ]

    # 结束匹配的字段
    # 截取第一个表格,分产品不一定有;分行业一定有
    # 主要产品|单位|生产量|销售量|库存量 来自产销表
    end_pattern = re.compile(r"分行业|营业收入|项目")

    # 抽取pdf表格,产品的类目一般不会超过15行
    pattern,end_index = pdf_load.extract_pdf_table2_shenzhen(pdf_data, start_pattern1, start_pattern2, start_pattern3, end_pattern,
                                                   强匹配最大行数=15, 弱匹配最大行数=16, 弱匹配偏移量=1, 备用偏移量=1)

    # 解析pdf表格:列表字典数据结构
    pdf_dic = pdf_load.analysis_pdf_table2_shenzhen(pattern)

    # ——————————————————————————————————开始匹配————————————————————————————————————

    # 前面的六个分类筛选，逐个提取
    sql_filtered = sql_filtered_table2_shenzhen1
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table2_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path)
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table2_shenzhen2
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table2_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )

    # 输出未匹配的 SQL 名称
    if unmatched_sql_names:
        print(f"❗未匹配的 SQL 条目: {RED}{unmatched_sql_names}{RESET}")
    else:
        pass
    # 输出未匹配的 PDF 名称
    if unmatched_pdf_names:
        print(f"❗未匹配的 PDF 条目: {RED}{unmatched_pdf_names}{RESET}")
    else:
        pass

    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="🌍🌍🌍🌍🌍 2️⃣经营表抵达")

    # ——————————————————深交所 2.5主营业务分产品表格 占公司营业收入或营业利润 10%以上的行业、产品、地区、销售模式的情况———————————————————————————————————
    # 筛选符合条件的 SQL 数据
    # 对数据进行分块，按照标准名称分成六类；对每一类的数据进行单独处理才不会导致数据混杂
    sql_filtered_table2_5_shenzhen1 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '销售收入' and item.get('指标单位') != '%')
        )
    ]
    sql_filtered_table2_5_shenzhen2 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '销售收入' and item.get('指标单位') == '%')
        )
    ]
    sql_filtered_table2_5_shenzhen3 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '营业成本' and item.get('指标单位') != '%')
        )
    ]
    sql_filtered_table2_5_shenzhen4 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '营业成本' and item.get('指标单位') == '%')
        )
    ]
    sql_filtered_table2_5_shenzhen5 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '毛利率' and item.get('指标单位') == '%')
        )
    ]
    sql_filtered_table2_5_shenzhen6 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '毛利率' and item.get('指标单位') == '百分点')
        )
    ]
    # 定义第一组模式（强匹配）
    start_pattern1 = [
        re.compile(r'^分产品$'),

    ]
    # 定义第二组备选模式（弱匹配）#其实这个要作为第三组备选,先测试
    start_pattern2 = [
        re.compile(r'.*分行业.*')
    ]
    # 定义第三组备选模式（最终备用方案）
    start_pattern3 = [
        re.compile(r'.*营业收入.*'),
        re.compile(r'.*毛利率.*'),
    ]

    # 结束匹配的字段
    # 截取第一个表格,分产品不一定有;分行业一定有
    # 主要产品|单位|生产量|销售量|库存量 来自产销表
    end_pattern = re.compile(r"产品类型|行业分类|项目|单位|生产量|销售量|库存量")

    # 抽取pdf表格,产品的类目一般不会超过12行
    pattern = pdf_load.extract_pdf_table2_5_shenzhen(pdf_data, start_pattern1, start_pattern2, start_pattern3, end_pattern,
                                                   强匹配最大行数=12, 弱匹配最大行数=13, 弱匹配偏移量=1, 备用偏移量=1,start_index=end_index)

    # 解析pdf表格:列表字典数据结构
    pdf_dic = pdf_load.analysis_pdf_table2_5_shenzhen(pattern)

    # ——————————————————————————————————开始匹配————————————————————————————————————
    # 前面的六个分类筛选，逐个提取
    sql_filtered = sql_filtered_table2_5_shenzhen1
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table2_5_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path)
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table2_5_shenzhen2
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table2_5_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table2_5_shenzhen3
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table2_5_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table2_5_shenzhen4
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table2_5_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold,
        get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table2_5_shenzhen5
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table2_5_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL, threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table2_5_shenzhen6
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table2_5_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL, threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low, message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path)
    # 输出未匹配的 SQL 名称
    if unmatched_sql_names:
        print(f"❗未匹配的 SQL 条目: {RED}{unmatched_sql_names}{RESET}")
    else:
        pass
    # 输出未匹配的 PDF 名称
    if unmatched_pdf_names:
        print(f"❗未匹配的 PDF 条目: {RED}{unmatched_pdf_names}{RESET}")
    else:
        pass

    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="🌍🌍🌍🌍🌍 2️⃣⭕5️⃣前10%经营表抵达")

    # —————————————————————————————————————————————上交所 3.产销表  产销量情况分析表———————————————————————————————————————————————
    # 筛选符合条件的 SQL 数据
    # 对数据进行分块，按照标准名称分成六类；对每一类的数据进行单独处理才不会导致数据混杂
    sql_filtered_table3_shenzhen1 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '销量' and item.get('指标单位') != '%')
        )
    ]
    sql_filtered_table3_shenzhen2 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '产量' and item.get('指标单位') != '%')
        )
    ]
    sql_filtered_table3_shenzhen3 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '库存量' and item.get('指标单位') != '%')
        )
    ]
    sql_filtered_table3_shenzhen4 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '销量' and item.get('指标单位') == '%')
        )
    ]
    sql_filtered_table3_shenzhen5 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '产量' and item.get('指标单位') == '%')
        )
    ]
    sql_filtered_table3_shenzhen6 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '库存量' and item.get('指标单位') == '%')
        )
    ]

    # 截取产销表
    # 定义第一组模式（强匹配）
    start_pattern1 = [
        re.compile(r'^行业分类$'),
        re.compile(r'^项目$'),
        re.compile(r'^单位$'),
    ]
    # 定义第二组备选模式（弱匹配）#其实这个要作为第三组备选,先测试
    start_pattern2 = [
        re.compile(r'.*行业分类.*'),
        re.compile(r'.*项目.*'),
        re.compile(r'.*单位.*'),
    ]
    # 定义第三组备选模式（最终备用方案）
    start_pattern3 = [
        re.compile(r'行业分类'),
        re.compile(r'项目')
    ]

    # 结束匹配的字段
    end_pattern = re.compile(r"产品分类|营业成本|营业成本比重|成本构成|行业分类")#!!行业分类可能误匹配，注意

    # 抽取pdf表格,产销售的类目一般不会超过12行，强匹配默认为1，硬编码
    pattern = pdf_load.extract_pdf_table3_shenzhen(pdf_data, start_pattern1, start_pattern2, start_pattern3,
                                                   end_pattern,
                                                   强匹配最大行数=12, 弱匹配最大行数=12, 弱匹配偏移量=1, 备用偏移量=1)

    # 解析pdf表格:列表字典数据结构
    pdf_dic = pdf_load.analysis_pdf_table3_shenzhen(pattern)
    # ——————————————————————————————————3.产销表 开始匹配————————————————————————————————————
    # 前面的六个分类筛选，逐个提取
    sql_filtered = sql_filtered_table3_shenzhen1
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table3_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path)
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table3_shenzhen2
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table3_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table3_shenzhen3
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table3_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table3_shenzhen4
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table3_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold,
        get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table3_shenzhen5
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table3_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL, threshold=threshold, low_threshold=low_threshold, get_high=get_high,
        get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table3_shenzhen6
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table3_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL, threshold=threshold, low_threshold=low_threshold, get_high=get_high,
        get_medium=get_medium, get_low=get_low, message_only_wrong=message_only_wrong,
        mapping_file_path=mapping_file_path)

    # 输出未匹配的 SQL 名称
    if unmatched_sql_names:
        print(f"❗未匹配的 SQL 条目: {RED}{unmatched_sql_names}{RESET}")
    else:
        print("产销表 ❗ 所有 SQL 条目均已匹配  ❗ 或不存在")
    # 输出未匹配的 PDF 名称
    if unmatched_pdf_names:
        print(f"❗未匹配的 PDF 条目: {RED}{unmatched_pdf_names}{RESET}")
    else:
        print("产销表 ❗ 所有 PDF 条目均已匹配  ❗ 或不存在")

    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="🌍🌍🌍🌍🌍 3️⃣产销表抵达")

    # —————————————————————————————————————————————深交所 4.成本分析表 ———————————————————————————————————————————————
    # 筛选符合条件的 SQL 数据
    # 对数据进行分块，按照标准名称分成六类；对每一类的数据进行单独处理才不会导致数据混杂
    sql_filtered_table4_shenzhen1 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '成本' and item.get('指标单位') != '%')
        )
    ]
    sql_filtered_table4_shenzhen2 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '成本占比' and item.get('指标单位') == '%')
        )
    ]
    sql_filtered_table4_shenzhen3 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '成本' and item.get('指标单位') == '%')
        )
    ]

    # 截取成本分析表
    # 定义第一组模式（强匹配）
    start_pattern1 = [
        re.compile(r'^产品分类$'),
        re.compile(r'^项目$'),
        re.compile(r'^20.*年$'),

    ]
    # 定义第二组备选模式（弱匹配） #匹配不到分产品，只有分行业
    start_pattern2 = [
        re.compile(r'.*行业.*'),
        re.compile(r'.*项目.*'),
        re.compile(r'.*2.*'),
    ]
    # 定义第三组备选模式（最终备用方案）
    start_pattern3 = [
        re.compile(r'.*分类.*'),
        re.compile(r'.*项目.*'),
    ]

    # 结束匹配的字段
    end_pattern = re.compile(r"前五名客户.*|序号|客户名称|销售额")

    # 抽取pdf表格,成本的类目一般不会超过18行，强匹配默认为1，硬编码
    pattern,end_index = pdf_load.extract_pdf_table4_shenzhen(pdf_data, start_pattern1, start_pattern2, start_pattern3,
                                                   end_pattern,
                                                   强匹配最大行数=16, 弱匹配最大行数=18, 弱匹配偏移量=1, 备用偏移量=1)

    # 解析pdf表格:列表字典数据结构
    pdf_dic = pdf_load.analysis_pdf_table4_shenzhen(pattern)
    # ——————————————————————————————————4.成本分析表 开始匹配————————————————————————————————————
    # 注意：要注意小计、合计，这两个
    # 前面的六个分类筛选，逐个提取
    sql_filtered = sql_filtered_table4_shenzhen1
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table4_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, threshold_double=threshold_double, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path)
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table4_shenzhen2
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table4_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, threshold_double=threshold_double, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table4_shenzhen3
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table4_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, threshold_double=threshold_double, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="🌍🌍🌍🌍🌍 4️成本分析表抵达")

    # —————————————————————————————————————————————深交所 5.前五大客户 5.公司前五名客户 ———————————————————————————————————————————————

    # 筛选符合条件的 SQL 数据
    # 对数据进行分块，按照标准名称分成六类；对每一类的数据进行单独处理才不会导致数据混杂
    sql_filtered_table5_shenzhen1 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '销售收入' and item.get('指标单位') != '%')
        )
    ]
    sql_filtered_table5_shenzhen2 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '销售收入占比' and item.get('指标单位') == '%')
        )
    ]
    # 截取成本分析表
    # 定义第一组模式（强匹配）
    start_pattern1 = [
        re.compile(r'^1$'),  # 第一个单元格：严格匹配 '1'
        re.compile(r'客户1.*|客户A.*|客户一.*|A'),
        re.compile(r'^\d*\.\d*$'),  # 第三个单元格：匹配任何数字（包括小数）
        re.compile(r'^\d+\.\d*|^\d+\.\d*%$')  # 第四个单元格：匹配任何百分比格式的值
    ]
    # 定义第二组备选模式（弱匹配）#其实这个要作为第三组备选,先测试
    start_pattern2 = [
        re.compile(r'^1$'),  # 第一个单元格：严格匹配 '1'
        re.compile(r'.*公司.*|第一名.*|单位1.*|A'),  # 这些格式是弱匹配格式，因为无法主动区分客户和供应商
        re.compile(r'^\d*\.\d*$'),  # 第三个单元格：匹配任何数字（包括小数）

    ]
    # 定义第三组备选模式（最终备用方案）
    start_pattern3 = [
        re.compile(r'^序号$'),
        re.compile(r'^客户名称$'),
        re.compile(r'^销售额$'),
    ]

    # 结束匹配的字段
    end_pattern = re.compile(r"序号|供应商名称|采购额")

    # 抽取pdf表格,成本的类目一般不会超过22行，强匹配默认为1，硬编码
    pattern = pdf_load.extract_pdf_table5_shenzhen(pdf_data, start_pattern1, start_pattern2, start_pattern3,
                                                   end_pattern,
                                                   强匹配最大行数=7, 弱匹配最大行数=7, 弱匹配偏移量=0, 备用偏移量=0)
    # 强匹配最大行数=6, 弱匹配最大行数=15, 弱匹配偏移量=10, 备用偏移量=1)
    # 优化抽取速度，传递end_index 参数！！！！
    if pattern == "5.text_start_signal":
        pdf_dic = pdf_load.text_analysis_pdf_table6(pdf_path, start_page=end_index, end_page=end_index + 10)

    else:
        # 解析pdf表格:列表字典数据结构
        pdf_dic = pdf_load.analysis_pdf_table5_shenzhen(pattern)
    # ——————————————————————————————————5.前五大客户表 开始匹配————————————————————————————————————
    # 注意：要注意小计、合计，这两个
    sql_filtered = sql_filtered_table5_shenzhen1
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table5_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path)
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table5_shenzhen2
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table5_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 输出未匹配的 SQL 名称
    if unmatched_sql_names:
        print(f"❗未匹配的 SQL 条目: {RED}{unmatched_sql_names}{RESET}")
    else:
        pass
    # 输出未匹配的 PDF 名称
    if unmatched_pdf_names:
        print(f"❗未匹配的 PDF 条目: {RED}{unmatched_pdf_names}{RESET}")
    else:
        pass
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="🌍🌍🌍🌍🌍 5️⃣前五大客户表抵达")

    # ————————————————————————————————————————上交所 6.前五大供应商 6.公司前五名供应商 ———————————————————————————————————————————————
    # 筛选符合条件的 SQL 数据
    # 对数据进行分块，按照标准名称分；对每一类的数据进行单独处理才不会导致数据混杂
    sql_filtered_table6_shenzhen1 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '采购金额' and item.get('指标单位') != '%')
        )
    ]
    sql_filtered_table6_shenzhen2 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '采购金额占比' and item.get('指标单位') == '%')
        )
    ]
    # 截取成本分析表
    # 定义第一组模式（强匹配）
    start_pattern1 = [
        re.compile(r'^1$'),  # 第一个单元格：严格匹配 '1'
        re.compile(r'供应商1.*|供应商A.*|供应商一.*|A'),
        re.compile(r'^\d*\.\d*$'),  # 第三个单元格：匹配任何数字（包括小数）
        re.compile(r'^\d+\.\d*|^\d+\.\d*%$')  # 第四个单元格：匹配任何百分比格式的值
    ]
    # 定义第二组备选模式（弱匹配）#其实这个要作为第三组备选,先测试
    start_pattern2 = [
        re.compile(r'^1$'),  # 第一个单元格：严格匹配 '1'
        re.compile(r'.*公司.*|第一名.*|单位1.*|A'),  # 这些格式是弱匹配格式，因为无法主动区分客户和供应商
        re.compile(r'^\d*\.\d*$'),  # 第三个单元格：匹配任何数字（包括小数）

    ]
    # 定义第三组备选模式（最终备用方案）
    start_pattern3 = [
        re.compile(r'序号'),
        re.compile(r'.*供应商.*'),
        re.compile(r'.*销售额.*'),
    ]

    # 结束匹配的字段
    end_pattern = re.compile(r".*科目.*|.*本期数.*|.*上年同期.*|.*变动.*")

    # 抽取pdf表格,成本的类目一般不会超过22行，强匹配默认为1，硬编码
    pattern = pdf_load.extract_pdf_table6_shenzhen(pdf_data, start_pattern1, start_pattern2, start_pattern3,
                                                   end_pattern,强匹配最大行数=7, 弱匹配最大行数=15, 弱匹配偏移量=10, 备用偏移量=1)


    if pattern == "6.text_start_signal":
        if end_index is None:
            pdf_dic = pdf_load.text_analysis_pdf_table6(pdf_path, start_page=12, end_page=38)
        else:
            pdf_dic = pdf_load.text_analysis_pdf_table6(pdf_path, start_page=end_index, end_page=end_index+10)
    else:
        # 解析pdf表格:列表字典数据结构
        pdf_dic = pdf_load.analysis_pdf_table6_shenzhen(pattern)
    # ——————————————————————————————————6.前五大供应商表 开始匹配————————————————————————————————————
    # 注意：要注意小计、合计，这两个
    sql_filtered = sql_filtered_table6_shenzhen1
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table6_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path)
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table6_shenzhen2
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table6_shenzhen(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 输出未匹配的 SQL 名称
    if unmatched_sql_names:
        print(f"❗未匹配的 SQL 条目: {RED}{unmatched_sql_names}{RESET}")
    else:
        pass
    # 输出未匹配的 PDF 名称
    if unmatched_pdf_names:
        print(f"❗未匹配的 PDF 条目: {RED}{unmatched_pdf_names}{RESET}")
    else:
        pass
    # 保存匹配数据到 Excel
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="🌍🌍🌍🌍🌍 6️⃣前五大供应商表抵达")

    # ————————————————————————————————————深圳交所 7.专利表  7.报告期内获得的知识产权列表表——————————————————————————————————————————
    # 筛选符合条件的 SQL 数据
    # 对数据进行分块，按照标准名称分；对每一类的数据进行单独处理才不会导致数据混杂
    sql_filtered_table7_shanghai1 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '新增知识产权申请数量')
        )
    ]
    sql_filtered_table7_shanghai2 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '新增知识产权有效数量')
        )
    ]
    sql_filtered_table7_shanghai3 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '累计知识产权申请数量')
        )
    ]
    sql_filtered_table7_shanghai4 = [
        item for item in sql_dic
        if item.get('截止日期') == JJRQ and (
            (item.get('标准名称') == '累计知识产权有效数量')
        )
    ]
    # ——————————————————————————————————抽取pdf数据————————————————————————
    pdf_dic = pdf_load.analysis_pdf_table7_shenzhen(pdf_path, start_page=10, end_page=20)
    print(pdf_dic)

    # ——————————————————————————————————开始匹配————————————————————————————————————
    # 前面的六个分类筛选，逐个提取
    sql_filtered = sql_filtered_table7_shanghai1
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table7_shanghai(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path)
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table7_shanghai2
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table7_shanghai(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table7_shanghai3
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table7_shanghai(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold, get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )
    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="")

    sql_filtered = sql_filtered_table7_shanghai4
    mix_dic, unmatched_pdf_names, unmatched_sql_names = match.match_table7_shanghai(
        sql_filtered, pdf_dic, JZRQ, XXFBRQ, XXLL,
        threshold=threshold, low_threshold=low_threshold,
        get_high=get_high, get_medium=get_medium, get_low=get_low,
        message_only_wrong=message_only_wrong, mapping_file_path=mapping_file_path
    )

    # 输出未匹配的 SQL 名称
    if unmatched_sql_names:
        print(f"❗未匹配的 SQL 条目: {RED}{unmatched_sql_names}{RESET}")
    else:
        pass
    # 输出未匹配的 PDF 名称
    if unmatched_pdf_names:
        print(f"❗未匹配的 PDF 条目: {RED}{unmatched_pdf_names}{RESET}")
    else:
        pass

    # 保存匹配数据到 Excel
    output_file_path = f"{output_file}\\{EPBH}_{XXLL_processed}.xlsx"
    output = export.append_to_excel(mix_dic, output_file_path, columns_score_chose, message="🌍🌍🌍🌍🌍 7️⃣专利表抵达")

    print("深交所")
    # 对文件进行高亮处理
    export.highlight_and_clean_excel(max_page,output_file_path)

    return output_file_path #传回结果