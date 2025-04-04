import pandas as pd
import time
import random
import json
from datetime import datetime
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import undetected_chromedriver as uc
import threading
from queue import Queue
from selenium_stealth import stealth
import requests
from typing import List, Dict, Optional
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('usps_tracker.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class ProxyPool:
    """代理IP池管理类"""
    def __init__(self, proxy_list: List[str], check_url: str = "https://tools.usps.com/go/TrackConfirmAction"):
        self.proxy_list = proxy_list
        self.check_url = check_url
        self.available_proxies = Queue()
        self.lock = threading.Lock()
        self._initialize_proxies()
    
    def _initialize_proxies(self):
        """初始化代理池，验证代理可用性"""
        for proxy in self.proxy_list:
            if self._check_proxy(proxy):
                self.available_proxies.put(proxy)
    
    def _check_proxy(self, proxy: str) -> bool:
        """检查代理是否可用"""
        try:
            proxies = {
                'http': f'http://{proxy}',
                'https': f'http://{proxy}'
            }
            response = requests.get(self.check_url, proxies=proxies, timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def get_proxy(self) -> Optional[str]:
        """获取一个可用的代理"""
        with self.lock:
            if not self.available_proxies.empty():
                return self.available_proxies.get()
            return None
    
    def release_proxy(self, proxy: str):
        """释放代理回池"""
        if proxy and self._check_proxy(proxy):
            self.available_proxies.put(proxy)

class TrackingState:
    """跟踪状态管理类，用于断点续传"""
    def __init__(self, state_file: str = "tracking_state.json"):
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

def setup_driver(proxy: Optional[str] = None):
    """设置Chrome驱动"""
    try:
        options = uc.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.binary_location = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
        
        if proxy:
            options.add_argument(f'--proxy-server=http://{proxy}')
        
        driver = uc.Chrome(
            options=options,
            version_main=134,
            use_subprocess=True
        )
        
        # 设置stealth模式
        stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )
        
        return driver
    except Exception as e:
        logging.error(f"设置Chrome驱动失败: {str(e)}")
        return None

def save_results(results_filename: str, is_first_batch: bool = False):
    """保存结果到文件，支持追加模式"""
    # 使用锁保护队列操作
    with lock:
        results = {}
        while not results_queue.empty():
            tracking_number, result = results_queue.get()
            results[tracking_number] = result
    
    # 保存成功的结果
    if results:
        with lock:  # 使用锁保护文件写入
            mode = 'w' if is_first_batch else 'a'
            with open(results_filename, mode, encoding='utf-8') as f:
                if is_first_batch:
                    f.write('{\n')
                else:
                    f.write(',\n')
                
                # 写入当前批次的结果
                for i, (tracking_number, result) in enumerate(results.items()):
                    if i > 0:
                        f.write(',\n')
                    f.write(f'  "{tracking_number}": {json.dumps(result, ensure_ascii=False, indent=2)}')
                
                if is_first_batch:
                    f.write('\n}')
        
        logging.info(f"已保存 {len(results)} 个成功结果到 {results_filename}")
    
    return list(results.keys())

def process_single_tracking(driver, tracking_number: str):
    """处理单个跟踪号码"""
    try:
        url = f"https://tools.usps.com/go/TrackConfirmAction?tLabels={tracking_number}"
        driver.get(url)
        
        # 减少页面加载等待时间
        time.sleep(random.uniform(0.5, 1))
        
        # 检查是否有跟踪结果
        try:
            tracking_container = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.CLASS_NAME, "track-bar-container"))
            )
            
            # 获取状态
            try:
                status = tracking_container.find_element(By.CLASS_NAME, "tb-status").text.strip()
            except NoSuchElementException:
                status = ""
            
            # 获取历史记录
            history = []
            history_entries = tracking_container.find_elements(By.CLASS_NAME, "tb-step")
            
            for entry in history_entries:
                history_entry = {
                    "date_time": "",
                    "location": "",
                    "status": ""
                }
                
                try:
                    date_element = entry.find_element(By.CLASS_NAME, "tb-date")
                    history_entry["date_time"] = date_element.text.strip()
                except NoSuchElementException:
                    pass
                
                try:
                    location_element = entry.find_element(By.CLASS_NAME, "tb-location")
                    history_entry["location"] = location_element.text.strip()
                except NoSuchElementException:
                    pass
                
                try:
                    status_element = entry.find_element(By.CLASS_NAME, "tb-status-detail")
                    history_entry["status"] = status_element.text.strip()
                except NoSuchElementException:
                    pass
                
                history.append(history_entry)
            
            result = {
                "status": status,
                "history": history
            }
            
            # 使用锁保护结果写入
            with lock:
                results_queue.put((tracking_number, result))
                
        except (TimeoutException, NoSuchElementException):
            logging.warning(f"无法获取跟踪号码 {tracking_number} 的信息")
            
    except Exception as e:
        logging.error(f"处理跟踪号码 {tracking_number} 时出错: {str(e)}")

def process_batch(batch: List[str], proxy_pool: ProxyPool, num_workers: int = 10):
    """处理一个批次的跟踪号码"""
    proxy = proxy_pool.get_proxy()
    driver = setup_driver(proxy)
    if not driver:
        logging.error("无法创建浏览器实例")
        return
    
    try:
        # 构建查询URL，一次查询所有单号
        tracking_numbers_str = ",".join(batch)
        url = f"https://tools.usps.com/go/TrackConfirmAction?tLabels={tracking_numbers_str}"
        driver.get(url)
        
        # 等待页面加载
        time.sleep(random.uniform(1, 2))
        
        # 获取所有跟踪结果容器
        tracking_containers = WebDriverWait(driver, 5).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "track-bar-container"))
        )
        
        # 创建结果解析队列
        parse_queue = Queue()
        for container in tracking_containers:
            parse_queue.put(container)
        
        def parse_worker():
            """解析工作线程，处理跟踪结果直到队列为空"""
            while True:
                try:
                    # 从队列中获取容器，如果队列为空则退出
                    container = parse_queue.get_nowait()
                except Queue.Empty:
                    break
                
                try:
                    # 获取对应的跟踪号码
                    tracking_number = container.get_attribute("data-tracking-number")
                    if not tracking_number:
                        continue
                    
                    # 获取状态
                    try:
                        status = container.find_element(By.CLASS_NAME, "tb-status").text.strip()
                    except NoSuchElementException:
                        status = ""
                    
                    # 获取历史记录
                    history = []
                    history_entries = container.find_elements(By.CLASS_NAME, "tb-step")
                    
                    for entry in history_entries:
                        history_entry = {
                            "date_time": "",
                            "location": "",
                            "status": ""
                        }
                        
                        try:
                            date_element = entry.find_element(By.CLASS_NAME, "tb-date")
                            history_entry["date_time"] = date_element.text.strip()
                        except NoSuchElementException:
                            pass
                        
                        try:
                            location_element = entry.find_element(By.CLASS_NAME, "tb-location")
                            history_entry["location"] = location_element.text.strip()
                        except NoSuchElementException:
                            pass
                        
                        try:
                            status_element = entry.find_element(By.CLASS_NAME, "tb-status-detail")
                            history_entry["status"] = status_element.text.strip()
                        except NoSuchElementException:
                            pass
                        
                        history.append(history_entry)
                    
                    result = {
                        "status": status,
                        "history": history
                    }
                    
                    # 使用锁保护结果写入
                    with lock:
                        results_queue.put((tracking_number, result))
                        
                except Exception as e:
                    logging.error(f"解析跟踪结果时出错: {str(e)}")
        
        # 创建并启动解析线程
        parse_threads = []
        for _ in range(num_workers):
            thread = threading.Thread(target=parse_worker)
            thread.start()
            parse_threads.append(thread)
        
        # 等待所有解析线程完成
        for thread in parse_threads:
            thread.join()
            
    except Exception as e:
        logging.error(f"处理批次时出错: {str(e)}")
    finally:
        try:
            driver.quit()
        except:
            pass
        if proxy:
            proxy_pool.release_proxy(proxy)

def process_tracking_numbers(tracking_numbers: List[str], batch_size: int = 35, num_browsers: int = 4, 
                           num_workers: int = 10, proxy_list: List[str] = None):
    """使用多浏览器和多线程处理跟踪号码"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_filename = f"tracking_results_{timestamp}.json"
    
    # 初始化代理池
    proxy_pool = ProxyPool(proxy_list) if proxy_list else None
    
    # 初始化状态管理
    state = TrackingState()
    processed_numbers = set(state.get_processed_numbers())
    failed_numbers = set(state.get_failed_numbers())
    
    # 过滤已处理的单号
    remaining_numbers = [num for num in tracking_numbers if num not in processed_numbers]
    logging.info(f"剩余待处理单号: {len(remaining_numbers)} 个")
    
    batch_queue = Queue()
    for i in range(0, len(remaining_numbers), batch_size):
        batch_queue.put(remaining_numbers[i:i + batch_size])
    
    def browser_worker():
        """浏览器工作线程，处理批次直到队列为空"""
        while True:
            try:
                # 从队列中获取批次，如果队列为空则退出
                batch = batch_queue.get_nowait()
            except Queue.Empty:
                break
                
            # 处理当前批次
            process_batch(batch, proxy_pool, num_workers)
            processed_batch = save_results(results_filename, 
                                        is_first_batch=(batch_queue.qsize() == 0))
            
            # 更新状态
            processed_numbers.update(processed_batch)
            # 计算失败的单号（批次中未成功处理的单号）
            failed_batch = set(batch) - set(processed_batch)
            failed_numbers.update(failed_batch)
            state.save_state(list(processed_numbers), list(failed_numbers))
            
            if failed_batch:
                logging.warning(f"当前批次有 {len(failed_batch)} 个单号处理失败")
            
            time.sleep(random.uniform(1, 2))
    
    # 创建并启动浏览器工作线程
    browser_threads = []
    for _ in range(num_browsers):
        thread = threading.Thread(target=browser_worker)
        thread.start()
        browser_threads.append(thread)
    
    # 等待所有浏览器线程完成
    for thread in browser_threads:
        thread.join()
    
    # 输出最终统计信息
    logging.info(f"处理完成！成功处理: {len(processed_numbers)} 个，失败: {len(failed_numbers)} 个")

def main():
    try:
        # 读取Excel文件
        df = pd.read_excel('数据列表.xlsx')
        tracking_numbers = df.iloc[:, 0].astype(str).tolist()
        logging.info(f"成功读取 {len(tracking_numbers)} 个跟踪号码")
        
        # 配置代理列表（示例）
        proxy_list = [
            "proxy1.example.com:8080",
            "proxy2.example.com:8080",
            # 添加更多代理...
        ]
        
        # 处理跟踪号码
        process_tracking_numbers(tracking_numbers, proxy_list=proxy_list)
        
    except Exception as e:
        logging.error(f"程序执行出错: {str(e)}")
    finally:
        logging.info("程序执行完成")

if __name__ == "__main__":
    main() 