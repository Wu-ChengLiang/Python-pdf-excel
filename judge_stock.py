
# #先简单测试  如果== ，返回的是无法判断交易所，此时扩大搜索范围 截取40到90页 根据出现的次数谁多，判断选择哪一个交易所。3

import pdfplumber

def judge_stock_change(pdf_path):
    count_shanghai = 0
    count_shenzhen = 0

    # 尝试打开 PDF 文件并提取表格数据
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # 首先检查第 4 到第 9 页
            for page in pdf.pages[3:9]:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        # 清洗每一行的数据：替换换行符、逗号、中文括号，但保留空格
                        cleaned_row = [
                            cell.replace('\n', '').replace(',', '').replace('（', '(').replace('）', ')')
                            if cell is not None else ''
                            for cell in row
                        ]
                        cleaned_row = [cell for cell in cleaned_row if cell.strip()]  # 删除空值和仅包含空格的单元格

                        # 在清洗的同时进行统计
                        for cell in cleaned_row:
                            if "上海证券交易所" in cell or "上海证券交易所科创板" in cell or "www.sse.com.cn" in cell:
                                count_shanghai += 1
                            if "深圳证券交易所" in cell or "深圳证券交易所科创板" in cell:
                                count_shenzhen += 1

            # 如果无法判断，扩大搜索范围到第 40 到第 90 页
            if count_shanghai == count_shenzhen:
                for page in pdf.pages[1:50]:
                    tables = page.extract_tables()
                    for table in tables:
                        for row in table:
                            # 清洗每一行的数据：替换换行符、逗号、中文括号，但保留空格
                            cleaned_row = [
                                cell.replace('\n', '').replace(',', '').replace('（', '(').replace('）', ')')
                                if cell is not None else ''
                                for cell in row
                            ]
                            cleaned_row = [cell for cell in cleaned_row if cell.strip()]  # 删除空值和仅包含空格的单元格

                            # 在清洗的同时进行统计
                            for cell in cleaned_row:
                                if "上海证券交易所" in cell or "上海证券交易所科创板" in cell or "www.sse.com.cn" in cell:
                                    count_shanghai += 1
                                if "深圳证券交易所" in cell or "深圳证券交易所科创板" in cell:
                                    count_shenzhen += 1
            # 如果无法判断，扩大搜索范围到第 40 到第 90 页
            if count_shanghai == count_shenzhen:
                    for page in pdf.pages[51:120]:
                        tables = page.extract_tables()
                        for table in tables:
                            for row in table:
                                # 清洗每一行的数据：替换换行符、逗号、中文括号，但保留空格
                                cleaned_row = [
                                    cell.replace('\n', '').replace(',', '').replace('（', '(').replace('）', ')')
                                    if cell is not None else ''
                                    for cell in row
                                ]
                                cleaned_row = [cell for cell in cleaned_row if cell.strip()]  # 删除空值和仅包含空格的单元格

                                # 在清洗的同时进行统计
                                for cell in cleaned_row:
                                    if "上海证券交易所" in cell or "上海证券交易所科创板" in cell or "www.sse.com.cn" in cell:
                                        count_shanghai += 1
                                    if "深圳证券交易所" in cell or "深圳证券交易所科创板" in cell:
                                        count_shenzhen += 1

    except Exception as e:
        raise ValueError(f"无法打开 PDF 文件或文件无效：{e}")

    # 根据关键词出现次数判断交易所
    if count_shanghai > count_shenzhen:
        return True  # 上交所
    elif count_shenzhen > count_shanghai:
        return False  # 深交所
    else:
        #raise ValueError("无法判断交易所，请检查 PDF 文件内容")
        return False