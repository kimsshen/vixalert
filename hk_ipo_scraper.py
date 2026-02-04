import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime, timedelta


def fetch_hk_ipo_data(page_num=1):
    """
    从aastocks网站获取港股IPO信息
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.aastocks.com/',
    }
    
    url = f'https://www.aastocks.com/tc/stocks/market/ipo/listedipo.aspx?s=3&o=0&page={page_num}'
    
    try:
        response = requests.get(url, headers=headers)
        response.encoding = 'utf-8'
        
        if response.status_code != 200:
            print(f"请求失败，状态码: {response.status_code}")
            print(f"响应内容: {response.text[:500]}...")  # 显示前500字符用于调试
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 查找页面标题确认是否成功访问
        title_tag = soup.find('title')
        if title_tag:
            print(f"页面标题: {title_tag.get_text().strip()}")
        
        # 查找包含IPO数据的主要容器
        # 根据AASTOCKS网站的常见结构进行搜索
        ipo_data_container = soup.find('div', {'id': re.compile(r'(?i)(ipo|new|stock)', re.IGNORECASE)}) or \
                           soup.find('table', {'class': re.compile(r'(?i)(ipo|new|stock)', re.IGNORECASE)}) or \
                           soup.find_all('div', {'class': re.compile(r'(?i)(content|main|data)', re.IGNORECASE)})
        
        if ipo_data_container:
            if hasattr(ipo_data_container, '__iter__'):
                containers = ipo_data_container
            else:
                containers = [ipo_data_container]
                
            for container in containers:
                # 查找表格
                tables = container.find_all('table') if hasattr(container, 'find_all') else soup.find_all('table')
                
                for table in tables:
                    # 获取表格头部
                    headers = []
                    header_row = table.find('thead')
                    if header_row:
                        headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
                    else:
                        # 如果没有thead，查找第一行作为标题
                        first_row = table.find('tr')
                        if first_row:
                            headers = [th.get_text().strip() for th in first_row.find_all(['th', 'td'])]
                    
                    if headers:
                        print(f"发现表格，列名: {headers}")
                        break
                        
                if headers:
                    break
        
        # 获取所有表格行数据
        all_rows_data = []
        
        # 查找所有表格
        tables = soup.find_all('table')
        for table_idx, table in enumerate(tables):
            print(f"正在处理第 {table_idx+1} 个表格...")
            
            rows = table.find_all('tr')
            for row_idx, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                if cells:
                    row_data = [cell.get_text().strip() for cell in cells]
                    if any(cell_data for cell_data in row_data if cell_data):  # 确保不是全空行
                        all_rows_data.append(row_data)
                        print(f"  行 {row_idx+1}: {row_data}")
        
        if all_rows_data:
            return all_rows_data
        else:
            print("未找到有效的表格数据")
            # 返回整个页面内容用于调试
            return response.text
        
    except requests.exceptions.RequestException as e:
        print(f"网络请求错误: {str(e)}")
        return None
    except Exception as e:
        print(f"获取数据时发生错误: {str(e)}")
        return None


def fetch_hk_ipo_with_requests_session(page_num=1):
    """
    使用Session保持连接，模拟更真实的浏览器行为
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.aastocks.com/',
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    # 先访问主页建立会话
    try:
        main_page_url = 'https://www.aastocks.com/tc/'
        session.get(main_page_url)
        time.sleep(1)  # 等待
        
        # 访问IPO页面
        url = f'https://www.aastocks.com/tc/stocks/market/ipo/listedipo.aspx?s=3&o=0&page={page_num}'
        response = session.get(url)
        response.encoding = 'utf-8'
        
        if response.status_code != 200:
            print(f"请求失败，状态码: {response.status_code}")
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 查找IPO相关表格
        tables = soup.find_all('table')
        all_rows_data = []
        
        for table_idx, table in enumerate(tables):
            print(f"处理表格 {table_idx+1}")
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if cells:
                    row_data = [cell.get_text().strip() for cell in cells]
                    if any(data.strip() for data in row_data):  # 确保不是空行
                        all_rows_data.append(row_data)
        
        return all_rows_data if all_rows_data else response.text
        
    except Exception as e:
        print(f"Session请求时发生错误: {str(e)}")
        return None


def fetch_hk_ipo_with_selenium():
    """
    使用Selenium获取IPO数据（当普通请求无效时）
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.chrome.options import Options
        
        # 设置Chrome选项
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # 无头模式
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
        driver = webdriver.Chrome(options=chrome_options)
        
        url = 'https://www.aastocks.com/tc/stocks/market/ipo/listedipo.aspx?s=3&o=0&page=1'
        driver.get(url)
        
        # 等待页面加载
        try:
            # 等待主要的IPO表格加载
            wait = WebDriverWait(driver, 10)
            table_element = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            
            # 提取表格数据
            rows = driver.find_elements(By.TAG_NAME, "tr")
            data = []
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if cells:
                    row_data = [cell.text.strip() for cell in cells]
                    if row_data:  # 只添加非空行
                        data.append(row_data)
            
            driver.quit()
            return data
            
        except Exception as e:
            print(f"Selenium等待元素时发生错误: {str(e)}")
            driver.quit()
            return None
            
    except ImportError:
        print("Selenium未安装，请运行: pip install selenium")
        return None
    except Exception as e:
        print(f"Selenium操作时发生错误: {str(e)}")
        return None


def parse_ipo_data(raw_data):
    """
    解析原始IPO数据，提取所需字段
    """
    # 定义需要的列名（按用户要求的顺序）
    columns = [
        '名稱', '代號', '上市日期', '每手股數', '上市市值(億元)', 
        '招股價', '上市價', '超額倍數', '穩中一手', '中籤率', 
        '現價', '首日表現', '累積表現'
    ]
    
    parsed_data = []
    
    # 这里需要根据实际页面结构调整解析逻辑
    # 目前我们先构建基本框架
    if isinstance(raw_data, str):
        # 如果是原始HTML文本，需要进一步解析
        soup = BeautifulSoup(raw_data, 'html.parser')
        # 尝试查找表格数据
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            # 查找包含IPO数据的表格（通过表头识别）
            for row_idx, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                if cells:
                    row_data = [cell.get_text().strip() for cell in cells]
                    
                    # 检查是否是表头行（包含"名稱"和"代號"）
                    if len(row_data) > 1 and '名稱' in row_data[1] and '代號' in row_data[1]:
                        # 找到表头，继续读取后续数据行
                        for data_row in rows[row_idx+1:]:
                            data_cells = data_row.find_all(['td', 'th'])
                            if data_cells and len(data_cells) >= len(columns):
                                data_row_data = [cell.get_text().strip() for cell in data_cells]
                                # 过滤掉说明行和分页行
                                if data_row_data[1] and '.HK' in data_row_data[1] and '延遲報價' not in data_row_data[1] and '下一頁' not in data_row_data[1]:
                                    # 提取公司名称和代号（它们在同一个单元格）
                                    name_code = data_row_data[1]
                                    # 分离名称和代号
                                    parts = name_code.split('.HK')
                                    if len(parts) >= 1:
                                        # 找到代号（通常是数字）
                                        import re
                                        code_match = re.search(r'(\d{5})', parts[0])
                                        if code_match:
                                            code = code_match.group(1)
                                            name = parts[0].replace(code, '').strip()
                                            # 移除"跌穿上市價"等标记
                                            name = re.sub(r'(跌穿上市價|認購不足)', '', name).strip()
                                            
                                            # 构建清洗后的数据行
                                            cleaned_row = [
                                                name,  # 名稱
                                                code,  # 代號
                                                data_row_data[2] if len(data_row_data) > 2 else '',  # 上市日期
                                                data_row_data[3] if len(data_row_data) > 3 else '',  # 每手股數
                                                data_row_data[4] if len(data_row_data) > 4 else '',  # 上市市值
                                                data_row_data[5] if len(data_row_data) > 5 else '',  # 招股價
                                                data_row_data[6] if len(data_row_data) > 6 else '',  # 上市價
                                                data_row_data[7] if len(data_row_data) > 7 else '',  # 超額倍數
                                                data_row_data[8] if len(data_row_data) > 8 else '',  # 穩中一手
                                                data_row_data[9] if len(data_row_data) > 9 else '',  # 中籤率
                                                data_row_data[10] if len(data_row_data) > 10 else '',  # 現價
                                                data_row_data[11] if len(data_row_data) > 11 else '',  # 首日表現
                                                data_row_data[12] if len(data_row_data) > 12 else '',  # 累積表現
                                            ]
                                            parsed_data.append(cleaned_row)
                        break
    elif isinstance(raw_data, list):
        # 如果已经是解析后的数据列表
        for row_data in raw_data:
            if isinstance(row_data, list) and len(row_data) > 1:
                # 检查是否是数据行（包含.HK）
                if '.HK' in str(row_data[1]) and '延遲報價' not in str(row_data[1]) and '下一頁' not in str(row_data[1]):
                    name_code = row_data[1]
                    parts = name_code.split('.HK')
                    if len(parts) >= 1:
                        import re
                        code_match = re.search(r'(\d{5})', parts[0])
                        if code_match:
                            code = code_match.group(1)
                            name = parts[0].replace(code, '').strip()
                            name = re.sub(r'(跌穿上市價|認購不足)', '', name).strip()
                            
                            cleaned_row = [
                                name,
                                code,
                                row_data[2] if len(row_data) > 2 else '',
                                row_data[3] if len(row_data) > 3 else '',
                                row_data[4] if len(row_data) > 4 else '',
                                row_data[5] if len(row_data) > 5 else '',
                                row_data[6] if len(row_data) > 6 else '',
                                row_data[7] if len(row_data) > 7 else '',
                                row_data[8] if len(row_data) > 8 else '',
                                row_data[9] if len(row_data) > 9 else '',
                                row_data[10] if len(row_data) > 10 else '',
                                row_data[11] if len(row_data) > 11 else '',
                                row_data[12] if len(row_data) > 12 else '',
                            ]
                            parsed_data.append(cleaned_row)
    
    # 创建DataFrame
    if parsed_data:
        df = pd.DataFrame(parsed_data, columns=columns)
    else:
        df = pd.DataFrame(columns=columns)
    
    return df


def main():
    """
    主函数：获取港股IPO数据并保存到CSV
    """
    print("开始获取港股IPO数据...")
    
    all_data = []  # 存储所有页面的数据
    
    # 遍历从page=1到page=11的所有页面
    for page_num in range(1, 12):  # 1到11（包含）
        print(f"正在获取第 {page_num} 页数据...")
        
        # 首先尝试使用普通请求获取数据
        raw_data = fetch_hk_ipo_data(page_num)
        
        if not raw_data or isinstance(raw_data, str):
            print(f"第 {page_num} 页普通请求未获取到有效数据，尝试使用Session方式...")
            raw_data = fetch_hk_ipo_with_requests_session(page_num)
        
        if not raw_data or isinstance(raw_data, str):
            print(f"第 {page_num} 页Session方式也未获取到有效数据，尝试使用Selenium...")
            raw_data = fetch_hk_ipo_with_selenium(page_num)
        
        if raw_data:
            print(f"第 {page_num} 页获取到 {len(raw_data) if isinstance(raw_data, list) else 'raw text'} 条记录")
            all_data.extend(raw_data)  # 将当前页数据添加到总数据中
        else:
            print(f"第 {page_num} 页未能获取到任何数据")
    
    if all_data:
        print(f"总共获取到 {len(all_data)} 条记录")
        
        # 解析所有数据
        df = parse_ipo_data(all_data)
        
        # 打印前几行以检查数据
        print("获取的数据预览:")
        if not df.empty:
            print(df.head())
        else:
            print("DataFrame为空")
        
        # 生成文件名（包含当前时间）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'hk_ipo_data_{timestamp}.csv'
        
        # 保存到CSV
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"数据已保存到 {filename}")
        
        # 显示数据摘要
        print(f"\n数据摘要:")
        print(f"总记录数: {len(df)}")
        print(f"列数: {len(df.columns)}")
        print(f"列名: {list(df.columns)}")
    else:
        print("未能获取到任何数据，请检查网络连接和网站访问权限")
        
        # 创建一个示例CSV文件以展示预期格式
        print("创建示例CSV文件以展示预期格式...")
        sample_data = {
            '名稱': ['示例公司A', '示例公司B'],
            '代號': ['01234', '05678'],
            '上市日期': ['2023-01-15', '2023-02-20'],
            '每手股數': ['100', '200'],
            '上市市值(億元)': ['50.5', '120.8'],
            '招股價': ['9.80', '24.50'],
            '上市價': ['10.50', '25.80'],
            '超額倍數': ['10.5倍', '15.2倍'],
            '穩中一手': ['60手', '120手'],
            '中籤率': ['15.0%', '8.5%'],
            '現價': ['12.30', '22.50'],
            '首日表現': ['+17.1%', '-12.8%'],
            '累積表現': ['+25.4%', '-5.2%']
        }
        
        sample_df = pd.DataFrame(sample_data)
        sample_filename = f'hk_ipo_sample_format.csv'
        sample_df.to_csv(sample_filename, index=False, encoding='utf-8-sig')
        print(f"示例文件已保存到 {sample_filename}")


if __name__ == "__main__":
    main()