import pandas as pd
import time
import random
import json
from datetime import datetime
import logging
import urllib3
import requests
from typing import List, Dict, Optional
import os
import threading
from queue import Queue, Empty

# 禁用警告
urllib3.disable_warnings()
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('usps_tracker_requests.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class TrackingState:
    """跟踪状态管理类，用于断点续传"""
    def __init__(self, state_file: str = "tracking_state_requests.json"):
        self.state_file = state_file
        self.lock = threading.Lock()
        self.state = self._load_state()
    
    def _load_state(self) -> Dict:
        """加载状态文件"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def save_state(self, processed_numbers: List[str], failed_numbers: List[str]):
        """保存处理状态"""
        with self.lock:
            self.state['processed'] = processed_numbers
            self.state['failed'] = failed_numbers
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
    
    def get_processed_numbers(self) -> List[str]:
        """获取已处理的单号"""
        return self.state.get('processed', [])
    
    def get_failed_numbers(self) -> List[str]:
        """获取失败的单号"""
        return self.state.get('failed', [])

# 线程安全的队列和结果存储
results_queue = Queue()
lock = threading.Lock()

def setup_session():
    """设置requests会话"""
    session = requests.Session()
    
    # 隧道域名:端口号
    tunnel = "j441.kdltpspro.com:15818"
    
    # 用户名密码方式
    username = "t14382074795872"
    password = "388m4xvh"
    proxies = {
        "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel},
        "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
    }
    # 设置代理
    session.proxies.update(proxies)
    
    # 设置请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    }
    session.headers.update(headers)
    
    # 设置cookie
    cookies = {
        'TLTSID': '516d4317d77f16638b0600e0ed96ae55',
        'NSC_u.tt1_443': 'ffffffff2198ff1e45525d5f4f58455e445a4a42378b',
        'NSC_uppmt-usvf-ofx': 'ffffffff3b22377f45525d5f4f58455e445a4a4212d3',
        'o59a9A4Gx': 'A9aXM_mVAQAABgxLUwUW_-jKBaCAEBgdp7IP_4q2pL7cUXnpLa9nCvHksHVFAWJi6KeuckX5wH8AAEB3AAAAAA|1|1|a51cc4cb5d55569497477996f666a1a3a6f1a329',
        'NSC_uppmt-hp': 'ffffffff3b22378745525d5f4f58455e445a4a4212d3',
        '_gcl_au': '1.1.1017027227.1743642654',
        'mdLogger': 'false',
        '_gid': 'GA1.2.1578774596.1743649843',
        'mab_usps': '29',
        'tmab_usps': '69',
        '_ga_QM3XHZ2B95': 'GS1.1.1743774386.1.0.1743774388.0.0.0',
        'ak_bmsc': 'ABE0A763A72B0E23CE407387F494EA73~000000000000000000000000000000~YAAQtSE1F0VNSv+VAQAAWpDKARsF7nkMbenAxvvoi8Ltquc0unB6vMR/sCG5cBwg6rugxHfFt7Fow7lKrLThTP5BaUstFB3mH2JJEOFeGTMdYa2uUolR9LDqPsqVptIOH0yVmn72TaDANqqzVxandRF2tX6yH93zaP6TG7JG+FdsMU1KY0QlVDITjMbJlSISlku0vJXh2f5uGpdHsQTvSIHglob4P2VO/iDESBFhfRkH8AM18vUvx9p9IRzsbe5TcZoGrQhLUiWQBvydh0a3ffSa1LDFWke536VOWVTuc8dap2gX2A5qHVNpLexR6fR9HxhiglCdc9NLJEeozER96lAa6tayVWJQkwOttiFuEbhEYPvQmvxEG6F1bP9eeubUhlKt25qSYGg=',
        'JSESSIONID': '0000fcsKxV8roKcs57FBsJzg-1E:1e8uh35kb',
        '_ga': 'GA1.1.690027187.1743642655',
        'SameSite': 'None',
        'w3IsGuY1': 'A85-0wGWAQAAfVKDvg-8eKEanf31EmuEdng7fkgCqWnv9z58ZoZEsvavSqsHAWJi6KeuckX5wH8AAEB3AAAAAA==',
        'kampyleUserSession': '1743787345967',
        'kampyleUserSessionsCount': '29',
        'kampyleSessionPageCounter': '1',
        '_ga_3NXP3C8S9V': 'GS1.1.1743786776.14.1.1743787369.0.0.0',
        '_ga_CSLL4ZEK4L': 'GS1.1.1743786777.14.1.1743787369.0.0.0',
        'bm_sv': 'C080FC376C597D2BA99777FE8663D80E~YAAQtSE1F8mJSv+VAQAANurTARsjqmaXCAQLSKXPf8mq78YDUalSu4kYDCZi5l6pQjhwvF01e9p4mEjEOJ4S3Rws95TbCjEQQVVNojQV5BWSQsH3kPpqcRjdD1wCyq8iX9IwA+x4jBdCrI2jUdWpk1LsZU5P2UGPYYYsCOTl9NPXVKYGaI213vq7I2kcOYyHQm2nnhNpj7YJl5uGL9q71cDgtN79NTldoLKWGAonHO4wpd7GG+WHgklvcqEfUrw=~1'
    }
    session.cookies.update(cookies)
    
    return session

def save_results(results_filename: str, is_first_batch: bool = False):
    """保存结果到文件，支持追加模式"""
    with lock:
        results = {}
        while not results_queue.empty():
            tracking_number, result = results_queue.get()
            results[tracking_number] = result
    
    if results:
        with lock:
            mode = 'w' if is_first_batch else 'a'
            with open(results_filename, mode, encoding='utf-8') as f:
                if is_first_batch:
                    f.write('{\n')
                else:
                    f.write(',\n')
                
                for i, (tracking_number, result) in enumerate(results.items()):
                    if i > 0:
                        f.write(',\n')
                    f.write(f'  "{tracking_number}": {json.dumps(result, ensure_ascii=False, indent=2)}')
                
                if is_first_batch:
                    f.write('\n}')
        
        logging.info(f"已保存 {len(results)} 个成功结果到 {results_filename}")
    
    return list(results.keys())

def browser_worker(worker_id: int):
    """浏览器工作线程"""
    session = setup_session()
    
    while True:
        try:
            batch = batch_queue.get_nowait()
        except Empty:
            break
        
        try:
            # 访问跟踪页面
            track_url = f"https://tools.usps.com/go/TrackConfirmAction?tLabels={','.join(batch)}"
            
            # 添加额外的请求头
            headers = {
                'Referer': 'https://www.usps.com/',
                'Origin': 'https://www.usps.com',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-User': '?1',
                'Sec-Fetch-Dest': 'document'
            }
            
            response = session.get(track_url, headers=headers, verify=False)
            time.sleep(random.uniform(1, 2))
            
            # 打印响应内容用于调试
            logging.debug(f"跟踪页面响应内容: {response.text[:500]}")
            
            # 将响应内容作为容器放入解析队列
            for tracking_number in batch:
                container = {
                    'tracking_number': tracking_number,
                    'html_content': response.text,
                    'url': track_url
                }
                parse_queue.put(container)
            
            # 更新进度
            with progress_counter:
                nonlocal total_processed
                total_processed += len(batch)
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 浏览器线程 {worker_id} 进度: {total_processed}/{len(tracking_numbers)} 个单号已处理")
            
        except Exception as e:
            logging.error(f"浏览器线程 {worker_id} 处理批次时出错: {str(e)}")
            continue
        
        time.sleep(random.uniform(2, 3))

def parse_worker(worker_id: int, parse_queue: Queue):
    """解析工作线程"""
    session = setup_session()
    
    while True:
        try:
            container = parse_queue.get_nowait()
        except Empty:
            time.sleep(1)
            break
        
        try:
            tracking_number = container['tracking_number']
            track_url = container['url']
            
            # 获取跟踪状态
            status_url = "https://tools.usps.com/UspsToolsRestServices/rest/idCrossSell/getIDStatus"
            params = {
                '_': int(time.time() * 1000)
            }
            headers = {
                'Referer': track_url,
                'Origin': 'https://tools.usps.com',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Content-Type': 'application/json;charset=utf-8',
                'X-Requested-With': 'XMLHttpRequest',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty'
            }
            
            data = {
                'trackingNumber': tracking_number
            }
            
            response = session.post(
                status_url,
                params=params,
                json=data,
                headers=headers,
                verify=False
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # 检查返回结果是否有效
                if result and isinstance(result, dict):
                    # 使用锁保护结果写入
                    with lock:
                        results_queue.put((tracking_number, result))
                else:
                    logging.warning(f"跟踪号码 {tracking_number} 返回无效数据: {result}")
                    
            else:
                logging.warning(f"无法获取跟踪号码 {tracking_number} 的信息，状态码: {response.status_code}")
                logging.debug(f"响应内容: {response.text}")
            
        except Exception as e:
            logging.error(f"解析线程 {worker_id} 处理单号 {tracking_number} 时出错: {str(e)}")
            continue
        
        time.sleep(random.uniform(0.5, 1))

def process_tracking_numbers(tracking_numbers: List[str], batch_size: int = 35, num_workers: int = 10):
    """使用多线程处理跟踪号码"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_filename = f"tracking_results_requests_{timestamp}.json"
    
    # 初始化状态管理
    state = TrackingState()
    processed_numbers = set(state.get_processed_numbers())
    failed_numbers = set(state.get_failed_numbers())
    
    # 优先处理之前失败的订单
    if failed_numbers:
        logging.info(f"发现 {len(failed_numbers)} 个之前失败的订单，优先处理")
        tracking_numbers = list(failed_numbers) + [num for num in tracking_numbers if num not in processed_numbers]
    else:
        tracking_numbers = [num for num in tracking_numbers if num not in processed_numbers]
    
    if len(tracking_numbers) == 0:
        logging.info("没有剩余待处理单号")
        return
    
    logging.info(f"剩余待处理单号: {len(tracking_numbers)} 个")
    
    # 创建批次队列和解析队列
    batch_queue = Queue()
    parse_queue = Queue()
    
    # 将跟踪号码分批放入批次队列
    for i in range(0, len(tracking_numbers), batch_size):
        batch = tracking_numbers[i:i + batch_size]
        if batch:
            batch_queue.put(batch)
    
    # 创建进度计数器
    progress_counter = threading.Lock()
    total_processed = 0
    
    # 创建并启动浏览器线程
    browser_threads = []
    for i in range(num_workers):
        thread = threading.Thread(target=browser_worker, args=(i+1,))
        thread.start()
        browser_threads.append(thread)
    
    # 创建并启动解析线程
    parse_threads = []
    for i in range(num_workers * 2):  # 解析线程数量是浏览器线程的2倍
        thread = threading.Thread(target=parse_worker, args=(i+1, parse_queue))
        thread.start()
        parse_threads.append(thread)
    
    # 等待所有线程完成
    for thread in browser_threads:
        thread.join()
    
    for thread in parse_threads:
        thread.join()
    
    # 保存最终结果
    processed_batch = save_results(results_filename, is_first_batch=True)
    if processed_batch:
        with lock:
            processed_numbers.update(processed_batch)
            failed_numbers.difference_update(processed_batch)
            state.save_state(list(processed_numbers), list(failed_numbers))
    
    # 输出最终统计信息
    logging.info(f"处理完成！成功处理: {len(processed_numbers)} 个，失败: {len(failed_numbers)} 个")

def main():
    try:
        # 读取Excel文件
        df = pd.read_excel('数据列表.xlsx')
        tracking_numbers = df.iloc[:, 0].astype(str).tolist()
        logging.info(f"成功读取 {len(tracking_numbers)} 个跟踪号码")
        
        # 处理跟踪号码
        process_tracking_numbers(
            tracking_numbers[:1],
            batch_size=1,  # 保持35个一批
            num_workers=1
        )
        
    except Exception as e:
        logging.error(f"程序执行出错: {str(e)}")
    finally:
        logging.info("程序执行完成")

if __name__ == "__main__":
    main() 