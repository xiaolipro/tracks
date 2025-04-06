import pandas as pd
import time
import random
import json
from datetime import datetime
import logging
import urllib3
urllib3.disable_warnings()

# 禁用特定的连接池警告
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
logging.getLogger('selenium').setLevel(logging.ERROR)
logging.getLogger('undetected_chromedriver').setLevel(logging.ERROR)

from bs4 import BeautifulSoup
import threading
from queue import Queue, Empty
from typing import List, Dict
import os
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext
import asyncio
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('usps_tracker.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

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
batch_queue = Queue()
parse_queue = Queue()
progress_counter = threading.Lock()
total_processed = 0

async def setup_browser(browser_id: int = 1) -> tuple[Browser, BrowserContext]:
    """设置Playwright浏览器"""
    try:
        playwright = await async_playwright().start()
        
        # 设置代理
        tunnel = "j441.kdltpspro.com:15818"
        username = "t14382074795872"
        password = "388m4xvh"
        
        # 随机生成浏览器参数
        viewport_width = random.randint(1280, 1920)
        viewport_height = random.randint(720, 1080)
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        ]
        user_agent = random.choice(user_agents)
        
        # 随机生成硬件参数
        hardware_concurrency = random.randint(4, 16)
        device_memory = random.choice([4, 8, 16])
        effective_type = random.choice(['4g', '3g', '2g'])
        rtt = random.randint(20, 100)
        downlink = random.randint(5, 20)
        
        # 随机生成地理位置
        latitude = random.uniform(39.0, 40.0)
        longitude = random.uniform(116.0, 117.0)
        
        browser = await playwright.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-http-keep-alive',
                '--disable-http2',
                '--disable-http-pipelining',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-site-isolation-trials',
                '--disable-web-security',
                '--disable-features=BlockInsecurePrivateNetworkRequests',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-gpu',
                '--disable-software-rasterizer',
                '--disable-extensions',
                '--disable-notifications',
                '--disable-popup-blocking',
                '--disable-background-networking',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-breakpad',
                '--disable-component-extensions-with-background-pages',
                '--disable-default-apps',
                '--disable-features=TranslateUI',
                '--disable-ipc-flooding-protection',
                '--disable-renderer-backgrounding',
                '--enable-features=NetworkService,NetworkServiceInProcess',
                '--metrics-recording-only',
                '--mute-audio',
                '--no-default-browser-check',
                '--no-first-run',
                '--password-store=basic',
                '--use-mock-keychain',
                f'--window-size={viewport_width},{viewport_height}',
            ]
        )
        
        # 创建上下文
        context = await browser.new_context(
            viewport={'width': viewport_width, 'height': viewport_height},
            user_agent=user_agent,
            locale='zh-CN',
            timezone_id='Asia/Shanghai',
            geolocation={'latitude': latitude, 'longitude': longitude},
            permissions=['geolocation'],
            #proxy={
            #    'server': f'http://{tunnel}',
            #    'username': username,
            #    'password': password
            #}
        )
        
        # 设置请求头
        await context.set_extra_http_headers({
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cache-control': 'no-cache, max-age=0',
            'pragma': 'no-cache',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'upgrade-insecure-requests': '1',
            'x-forwarded-for': f'{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}',
            'x-forwarded-proto': 'https',
        })
        
        # 注入反检测脚本
        await context.add_init_script(f"""
            Object.defineProperty(navigator, 'webdriver', {{
                get: () => undefined
            }});
            Object.defineProperty(navigator, 'plugins', {{
                get: () => [1, 2, 3, 4, 5]
            }});
            Object.defineProperty(navigator, 'languages', {{
                get: () => ['zh-CN', 'zh']
            }});
            Object.defineProperty(navigator, 'platform', {{
                get: () => 'Win32'
            }});
            Object.defineProperty(navigator, 'maxTouchPoints', {{
                get: () => 0
            }});
            Object.defineProperty(navigator, 'hardwareConcurrency', {{
                get: () => {hardware_concurrency}
            }});
            Object.defineProperty(navigator, 'deviceMemory', {{
                get: () => {device_memory}
            }});
            Object.defineProperty(navigator, 'connection', {{
                get: () => ({{
                    effectiveType: '{effective_type}',
                    rtt: {rtt},
                    downlink: {downlink},
                    saveData: false
                }})
            }});
            Object.defineProperty(navigator, 'permissions', {{
                query: () => Promise.resolve({{ state: 'granted' }})
            }});
            window.chrome = {{
                runtime: {{}},
                app: {{}},
                loadTimes: function(){{}},
                csi: function(){{}},
                webstore: {{}},
                app: {{
                    isInstalled: false,
                    InstallState: {{
                        DISABLED: 'disabled',
                        INSTALLED: 'installed',
                        NOT_INSTALLED: 'not_installed'
                    }},
                    RunningState: {{
                        CANNOT_RUN: 'cannot_run',
                        READY_TO_RUN: 'ready_to_run',
                        RUNNING: 'running'
                    }}
                }}
            }};
        """)
        
        return browser, context
    except Exception as e:
        logging.error(f"设置Playwright浏览器失败: {str(e)}")
        return None, None

def save_results(results_filename: str, is_first_batch: bool = False):
    """保存结果到文件，支持追加模式"""
    results = {}
    while not results_queue.empty():
        tracking_number, result = results_queue.get()
        results[tracking_number] = result
    
    # 保存成功的结果
    if results:
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

def parse_worker(worker_id: int):
    """解析工作线程，持续处理跟踪结果"""
    print(f"解析工作线程 {worker_id} 已启动")
    while True:
        try:
            # 从队列中获取容器数据，如果队列为空则退出
            container_data = parse_queue.get_nowait()
        except Empty:
            time.sleep(random.randint(1, 3)) # forever not return
            continue
        
        try:
            tracking_number = container_data['tracking_number']
            container_html = container_data['html']
            
            # 使用BeautifulSoup解析HTML
            soup = BeautifulSoup(container_html, 'html.parser')
            
            # 获取状态
            status_element = soup.find(class_="tb-status")
            status = status_element.text.strip() if status_element else ""
            
            # 获取历史记录
            history = []
            history_entries = soup.find_all(class_="tb-step")
            
            for entry in history_entries:
                if "toggle-history-container" in entry.get('class', []):
                    continue
                    
                history_entry = {
                    "date_time": "",
                    "location": "",
                    "status": ""
                }
                
                date_element = entry.find(class_="tb-date")
                if date_element:
                    history_entry["date_time"] = date_element.text.strip()
                
                location_element = entry.find(class_="tb-location")
                if location_element:
                    history_entry["location"] = location_element.text.strip()
                
                status_element = entry.find(class_="tb-status-detail")
                if status_element:
                    history_entry["status"] = status_element.text.strip()
                
                history.append(history_entry)
            
            result = {
                "status": status,
                "history": history
            }
            
            # 使用锁保护结果写入
            print(f"解析工作线程 {worker_id} 成功处理单号: {tracking_number}，状态: {status}，轨迹数量: {len(history)}")
            results_queue.put((tracking_number, result))
                
        except Exception as e:
            logging.error(f"解析工作线程 {worker_id} 处理跟踪结果时出错: {str(e)}")
            continue

async def page_worker(page_id: int, context: BrowserContext, total_numbers: int, results_filename: str):
    """页面工作线程，每个线程运行一个独立的页面"""
    global total_processed
    try:
        print(f"页面 {page_id} 已启动")
        
        # 创建并保持页面打开
        page = await context.new_page()
            
        while True:
            try:
                # 从队列中获取批次，如果队列为空则退出
                batch = batch_queue.get_nowait()
            except Empty:
                break
            
            try:
                # 构建查询URL，一次查询所有单号
                tracking_numbers_str = ",".join(batch)
                print(f"页面 {page_id} 正在处理: {len(batch)} 个单号")
                
                # 1. 访问首页
                await page.goto("https://www.usps.com/")
                
                # 2. 点击导航菜单
                # 等待导航菜单出现
                await page.wait_for_selector(".nav-first-element", timeout=10000)
                print(f"页面 {page_id} 成功加载导航菜单")
                                    
                # 点击导航菜单
                await page.click(".nav-first-element")
                # 点击第一个选项
                await page.click(".menuheader ul li:first-child a")
                
                # 3. 等待输入框出现
                await page.wait_for_selector("#tracking-input", timeout=10000)  # 等待10秒
                print(f"页面 {page_id} 成功加载输入框")
                
                # 4. 输入跟踪号码
                await page.fill("#tracking-input", tracking_numbers_str)
                
                # 5. 点击提交按钮
                await page.click(".tracking-btn")
                
                # 等待跟踪结果容器出现
                await page.wait_for_selector(".track-bar-container", timeout=10000)  # 等待10秒
                print(f"页面 {page_id} 成功加载跟踪结果容器")
                                    
                # 获取所有容器
                containers = await page.query_selector_all(".track-bar-container")
                
                # 将容器的HTML放入解析队列
                for container in containers:
                    # 获取对应的跟踪号码
                    tracking_number = await container.query_selector(".tracking-number")
                    tracking_number_text = await tracking_number.inner_text()
                    tracking_number_text = tracking_number_text.strip()
                    
                    if not tracking_number_text:
                        logging.error(f"找不到跟踪单号，请联系管理员是否usps网站更新")
                        await page.close()
                        return
                        
                    container_html = await container.inner_html()
                    container_data = {
                        'tracking_number': tracking_number_text,
                        'html': container_html
                    }
                    parse_queue.put(container_data)
                
                # 更新进度
                with progress_counter:
                    total_processed += len(batch)
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 页面 {page_id} 进度: {total_processed}/{total_numbers} 个单号已处理")
                
                # 每处理完一批就保存结果
                processed_batch = save_results(results_filename, is_first_batch=(total_processed == len(batch)))
                if processed_batch:
                    # 获取当前状态
                    state = TrackingState()
                    processed_numbers = set(state.get_processed_numbers())
                    failed_numbers = set(state.get_failed_numbers())
                    
                    # 更新处理状态
                    processed_numbers.update(processed_batch)
                    # 从失败列表中移除成功处理的单号
                    failed_numbers.difference_update(processed_batch)
                    # 保存状态
                    state.save_state(list(processed_numbers), list(failed_numbers))
                    logging.info(f"已保存 {len(processed_batch)} 个结果到文件")
                
            except Exception as e:
                logging.error(f"页面 {page_id} 处理批次时出错: {str(e)}")
                
                # 检查是否被重定向到维护页面
                if "anyapp_outage_apology" in page.url:
                    logging.error(f"页面 {page_id} 被重定向到维护页面，请联系管理员")
                    # 如果被重定向，重新创建页面
                    time.sleep(60)
                
                # 如果页面出现问题，重新创建页面
                await page.close()
                page = await context.new_page()
                continue
                
    except Exception as e:
        logging.error(f"页面 {page_id} 工作线程出错: {str(e)}")
    finally:
        # 最后才关闭页面
        if page:
            await page.close()

async def process_tracking_numbers(tracking_numbers: List[str], batch_size: int = 35, num_pages: int = 5, 
                           num_workers: int = 5):
    """使用多页面和多线程处理跟踪号码"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_filename = f"tracking_results_{timestamp}.json"
    
    # 初始化状态管理
    state = TrackingState()
    processed_numbers = set(state.get_processed_numbers())
    failed_numbers = set(state.get_failed_numbers())
    
    # 优先处理之前失败的订单
    if failed_numbers:
        logging.info(f"发现 {len(failed_numbers)} 个之前失败的订单，优先处理")
        # 将失败的订单添加到待处理列表的开头
        tracking_numbers = list(failed_numbers) + [num for num in tracking_numbers if num not in processed_numbers]
    else:
        # 过滤已处理的单号
        tracking_numbers = [num for num in tracking_numbers if num not in processed_numbers]
    
    if len(tracking_numbers) == 0:
        logging.info("没有剩余待处理单号")
        return
    
    total_numbers = len(tracking_numbers)
    logging.info(f"剩余待处理单号: {total_numbers} 个")
    
    # 将跟踪号码分批放入批次队列
    for i in range(0, len(tracking_numbers), batch_size):
        batch = tracking_numbers[i:i + batch_size]
        if batch:  # 确保批次不为空
            batch_queue.put(batch)
    
    # 创建并启动浏览器
    browser, context = await setup_browser(1)
    if not browser or not context:
        logging.error("浏览器创建失败")
        return
    
    try:
        # 创建并启动页面任务
        page_tasks = []
        for i in range(num_pages):
            task = asyncio.create_task(page_worker(i+1, context, total_numbers, results_filename))
            page_tasks.append(task)
        
        # 创建并启动解析线程
        parse_threads = []
        for i in range(num_workers):
            thread = threading.Thread(target=parse_worker, args=(i+1,))
            thread.start()
            parse_threads.append(thread)
        
        # 等待所有页面任务完成
        await asyncio.gather(*page_tasks)
        
        # 等待所有解析线程完成
        for thread in parse_threads:
            thread.join()
        
        # 保存最终结果并更新状态
        processed_batch = save_results(results_filename, is_first_batch=False)
        if processed_batch:
            # 更新处理状态
            processed_numbers.update(processed_batch)
            # 从失败列表中移除成功处理的单号
            failed_numbers.difference_update(processed_batch)
            # 更新失败列表
            failed_batch = set(tracking_numbers) - set(processed_batch)
            failed_numbers.update(failed_batch)
            # 保存状态
            state.save_state(list(processed_numbers), list(failed_numbers))
            
            if failed_batch:
                logging.warning(f"当前批次有 {len(failed_batch)} 个单号处理失败")
        
        # 输出最终统计信息
        logging.info(f"处理完成！成功处理: {len(processed_numbers)} 个，失败: {len(failed_numbers)} 个")
    
    finally:
        # 关闭浏览器
        await context.close()
        await browser.close()

def main():
    try:
        # 读取Excel文件
        df = pd.read_excel('数据列表.xlsx')
        tracking_numbers = df.iloc[:, 0].astype(str).tolist()
        logging.info(f"成功读取 {len(tracking_numbers)} 个跟踪号码")
        
        test_numbers = tracking_numbers
        
        # 处理跟踪号码
        asyncio.run(process_tracking_numbers(
            test_numbers,
            batch_size=35,
            num_pages=1,  # 使用5个页面
            num_workers=1,
        ))
        
    except Exception as e:
        logging.error(f"程序执行出错: {str(e)}")
    finally:
        logging.info("程序执行完成")

if __name__ == "__main__":
    main() 