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
batch_queue = Queue()
parse_queue = Queue()
results_queue = Queue()  # 重新添加结果队列
file_lock = threading.Lock()  # 添加文件锁
progress_counter = threading.Lock()
total_processed = 0

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
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'zh-CN,zh;q=0.9',
        'akamai-origin-hop': '2',
        'cache-control': 'no-cache, max-age=0',
        'cdn-loop': 'akamai;v=1.0;c=1',
        'client-ip': '23.59.160.169',
        'http-x-ec-geodata': 'geo_asnum=21859,geo_city=SINGAPORE,geo_continent=AS,geo_country=SG,geo_latitude=1.29,geo_longitude=103.86,geo_postal_code=,geo_region=,wireless=false',
        'istl-infinite-loop': '1',
        'ns-client-ip': '98.98.232.167',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'x-akamai-config-log-detail': 'true',
        'x-cloud-trace-context': 'c7bcdd69725b99447d14bd5908292ce4/15545007584333298721',
        'x-forwarded-for': '98.98.232.167, 23.193.104.209, 56.0.33.9, 34.144.225.15',
        'x-forwarded-proto': 'https',
        'x-jfuguzwb-a': 'CH1befPahApq5qGhiq99_wG1QlGrA4X21IXG=DVOix1=Rv22qGhNwotggJ=shMkX8Fu0VphrQncaEgVU9HHCN=H7IIwDwfGKd19CzQ0SDd5jOd04NNnUf04RngKc9VtIMgMMarQ4lw2P9O9IKkBodZ4WL=Dj1VNfN1FcNejl4FhYGx5w7zr8BcitAp7ss5kS-hFGkGlHtPA82G09t=B=ZeISdPlYHR7FFrJlCgiRmnQgCdeNzyHBw4X5tyiVNfpGl2GWnjZ3jWeirFrqZ7JPnshs3YWlyVUBGxYInRxsA-1EEwXt8rmQUgihRS8KLzhiBmXzpCKNAvMbrYlZXj99n1-WCKJPu2=LwRgiahANxBcuvu8kqLZXavElIdIOFHiEbtYVCebWFVb7dFl==cg4FKxGCqoXNO28EPEjnHLnQZsw-Mv7vYcvcwMBaxO2UUI6aqtSMqa4HAwLaNCMmyWcmzjaAGH6L3HAY9oP=z3K9f9Q1vd4j7G-XLzJEbqC6LsJIz6k9aXhgsFD1CcKBs3SsHIZ=YUrF35WSXLqltqpKzarrYwFnE5BUi2G_meeFwzsGMj2QiNOQ31WFaZKPRNc2XbiLElnrdO6ULLqiXCdKza7vrLnq0Lq_mbSKm0avzffUkKnuwEPxWa6Lhu1k6z4NfXJuqq3UOKew4atXZaNFsMpaQvuypgJpa3rgb_sAwsE=ovcHcI2VLanqjNiJk7puF06zCSyU4diJHDWZweipa3CrSx7RJ9AM_XGsyVyhlXB6-8YpoOeLicIRRP_A0ooJUYZ4oCzotqI8Heg6AIQLtxjzB6nE=4ASgB=qQ4Hu2tavONmEs5Ovg7pigecgqGZUhMMO1wK_XnreqW3Cxu9M7KFZ-K1yB2Z2UQZ3wweiNPOSGjFuFIKzSiK6VdSq5f=9CG==Z1Kf-KDH6m3NCklK7lVa1HtkwPMB5AgIBrnDpSdA6IV7ewZdNF-BCOB0noxfsMQFcEdGhEUlUq=8LYsKl=C0tyRiFPRA1RYDIYHz2d5pkBRwVSg676FX7ahZ9sDpthkgwPrh93BW775wShN6grIkePVtGAaqRP0K_edjZh=dILedNvq4UFG2ac_8P6Nht9_jKlINjLJ9GA-vxH7skVrIYAeV8CDvdd4vdWSUYwbd1yUXtU9ESFO0y4dUVfy9SyzURu1u81z1zC1N=B7Q3VqRdbjXI6ZYbSw2wX0bO2KZujafRpvMHzmapva74uxC1_zF0opkMVW_9vLnoFzEfXrm4M1UVIwlygcq5EqEEWS3BkBNOorgrBS=xKY9imJrkzhcWx72qLA=EX3IVXqKSHlkPonkce9SMpw6_FpK3qZv7qYXlrgELbgazvW1ZS=IzcG6pHb99kRKBofKVaQRjCCp5lx0DR0D=xlg8F26b0dwZFNz8pJqDH5PboAs96ZeGGnCGbrWqHJeEE3_RikYAj9GsaCjnz2YiXQf7lAtj1KJdmhKvxkk1m-7kIsR3tMuQxmy2h_dxajtnpvK2pKn626Javu=p5ECAai=VMivm_oNniHb21XZSHEdPaw1ofq6fYqNLvr8Mh7NNIoM9xkUwh63ipvXrYecSYzdMiEXk7pHON=7JLl0BNx5GyWuQgGFi8DNlNu_rAivwOqR5OFtqYrIi0EDzlsESqVv0Ngv1ChqkIwlPyznMjjb7UOiQMPC3G_ItrJuzVC=w=9kJ5Gb64IAp6hKOpp=QswA7BcX9-6wLha0gXCVPXNcfKZ8jD-OyhB7S8QBYulF4lGLhIqyPBMqogwF9aG5E8uYcQ97Fa87ztPXJOZL7MQvanhS8Y6o0dKm1tWRaXbaU3BgFXO4iUJcmzL0PwcPkJjbuKo-OEwheowkcjPGZxgLcKeXEUMQXhCOfIqnDjawpwfxV7jFBnMkNPayHCzcpaNB0ZPucJL=EYZqKnDJVosIXt0P3LhOJuJmFJ5v5U1dhE5lbEEu7KqYb6t_dAcm=KkjcraP1Mr5SBflePV5rIjL8RquGsi_-LaBsAjcZdL6AOcxhQlOtbwiy-Z2Bjpm2Vnv7Om5h3-klSCfN1tJuu7XmFvce6UCGI4xe2N1FoBV6lAPL5IL0eY4wLgU_f53Bt4CKFNy1OzrqApa70zbYDhwuJUcrKIxglLMNW8AUWemVRtywwosNw5w1-vlIDVJOkkMz0QzFfL6ydHMv7DCjn5akjjUdnycCPPFz1=_UP=LE7KELsHbWdqaxYHoqB4s-Ffdi2Jxa9hxcK-MMU_JmOUrxnkA68VrA0QRNcFY7POm94MGm7QoHBjH==GDqPY6gONQ5H3ZU9Ymt9pPUSeoLXROy0w_r-_bi42DKaUApDQOW-vEqch6A66dPqfn8qEBR3wRSoiuHly7EXyI4-W4w96OglwujythmxgOm7wlHsFO6G=C084HxvBB4iVR8r9A1s=0EqemYtx_oHa0CfUDdcA1=xqIhpZNBRDNwnL6OYfKDVMv97reU9Kr6AG=tzKk3Ela8CIXPjd0ksJXvd3V4tyoDh-gt=ULC5dPpmyM-fQcKSSzca-sUVOCognrxADaH9xlylfYiP60FBWJuVUvfl0HY7V3cbW4IxR2mVIA4DXCDPDK0kVdSzN=S8AQi547tlzklxQUE=RlqxdJ-yQY7-VXHON1MtNI3aE9ZYBeO4FuLtb3S3=naazXGn7UjeZ7X9xI48_3R_8N7CXfaVuYDLy7nHafyD8mlPvUMy6eysbuatZH8VBcAvWx8iAmMCVbNglPsu3QLWh6YvIgzBx9=x4WddEGs-buqbXP_G1cFJ778uysRwCkz1XB1N1ndx_H8l9mI5CnM2dqkMkWjtH8NP-b1WSu8Iv4SlN4eQkDrKBFsRvqDOhocmwuG6U4bowEjfHKGNhYSVMkeaa7M=Z4B2t=6CjZx3dAGU=zv5LHzIu1Hdnq-SbiImgbCVLO9Iy1ZUznBniY654o1VsI1JMFwSQFyC0MGAnGSRC6iDXgdsSQUQH3sumfI3g6-GQ8Csp5EhIb9NBdSUfUqv0ymhhvChrhhU_JzkpcqNUz-j4E7JMBiHB36cH2NpW_9eKUtiBEB5Ap9maWHwHWVOBGGJFVX1lFKVL5=ti5FSQG5Vbfjn=aO-8noZkzExNfd_-sqWQYMCVOncCLs6d2Wp=nyCpM_FJH738Qydv2NOsRp_Iv53orZ8tCsqymUCUKbGxC8x0aZ0meU60sv45E0Hs25dbYM8aNLQ67-z0KqbKY6q2Vje5=BjMluveKLRDmF4gomG9oWNKgwz20WyIJjoUvqSinNQlndGhg6xszrdei2cDcBfbeiQ1jvacyo4qL0ZBHABMv0yQ20GpEYbEjG3gKqhAaiJu4zRzmwxkPL357qFQUbSD9dKiCAgbOKi1Uqmtz0oDUaDCsVuffOZ4NVYxk_egQaEMqO8I=XfFneDwfngxwGuVVoWuptH7zrv2-Kvm8VJl38ceJLhmZD=kpnnUxlwOKVVcvBHLBleR8_dle5XhZCE6-3jEiIXl9JuFzJfO4Oc_XK6D4g03C8dB8Vkj6OZiEJoL6qUjeVSkqV8mZB3',
        'x-jfuguzwb-b': '-kz9nf1',
        'x-jfuguzwb-c': 'AACVZAWWAQAAmQeJttVxR560nM0vdn4EJQPW2lBIDxr3YeaDRmG7FCtO2_8s',
        'x-jfuguzwb-d': 'ADaAhACBAKCBgQGAAYAQgICCAKIAwAGABJhCgAyVEIIhkIDJCABhuxQrTtv_LAAAAABhOejTA5kuo7Wzr2rHKtoKjQ4DuEE',
        'x-jfuguzwb-f': 'A2oLaAWWAQAAEXT9tPIq3E91AOWsrN13ONjL2wcjlcIlZhqo1DlnNVx2jZJUAWJi6KeuckX5wH8AAEB3AAAAAA==',
        'x-jfuguzwb-z': 'q'
    }
    session.headers.update(headers)
    #
    ## 设置cookie
    #cookies = {
    #    'TLTSID': '516d4317d77f16638b0600e0ed96ae55',
    #    'NSC_u.tt1_443': 'ffffffff2198ff1e45525d5f4f58455e445a4a42378b',
    #    'NSC_uppmt-usvf-ofx': 'ffffffff3b22377f45525d5f4f58455e445a4a4212d3',
    #    'o59a9A4Gx': 'A9aXM_mVAQAABgxLUwUW_-jKBaCAEBgdp7IP_4q2pL7cUXnpLa9nCvHksHVFAWJi6KeuckX5wH8AAEB3AAAAAA|1|1|a51cc4cb5d55569497477996f666a1a3a6f1a329',
    #    'NSC_uppmt-hp': 'ffffffff3b22378745525d5f4f58455e445a4a4212d3',
    #    '_gcl_au': '1.1.1017027227.1743642654',
    #    'mdLogger': 'false',
    #    '_gid': 'GA1.2.1578774596.1743649843',
    #    '_scid': '_wCiSs-TsLDi7Ntu8LqZzuwVbsjfg7W4',
    #    'sm_uuid': '1743819301528',
    #    '_ScCbts': '%5B%5D',
    #    '_pin_unauth': 'dWlkPU1HSmtPVFU1WTJRdE1UVmhPQzAwTUdFM0xUaGhNakV0T1dFeU9HWmlaalppTjJabQ',
    #    '_sctr': '1%7C1743782400000',
    #    'mab_usps': '80',
    #    'tmab_usps': '25',
    #    'ak_bmsc': '65D53A112E8BD5E296E341F3FA871166~000000000000000000000000000000~YAAQtSE1FwFiWv+VAQAAPySgBBv+LNPjzkzeGuav+IhtuMpIwV9SiwpU219Nd/YuoVO1+JzydLfvXtcJD++BMLXmDwsZCj8kzLHGD8u4QzJIcqokOMxTMFSMT8G5DVGzcL760lvFBfosR8A0AHdL3crkBzTdbgORvzR5T7Bfe7Gz/ZRVjEkR8wrTDk0rGnFfKTjjr3IYOsOL3pu9uYgHVFZ5mtAl7sZqm1IG5wjrlr2ta458r2CQKGyQg2OKaE7ZzeGMssbI6EcUY2n66ETs4jS2RiyKxEXAOdzkIWcSsks/qU77Y8Ba2hpvrG3i3RANRVYlayCB0R9hVrnlkCgHt0oPSbevBEZB4QS4uVbvN5Hdlpz487qb7FW1UI1oE1dU2NK+YKjNFgY=',
    #    '_dpm_ses.340b': '*',
    #    'JSESSIONID': '0000td9I5YljcG4pjroJd60QCiK:1e8uh35kb',
    #    '_ga': 'GA1.1.690027187.1743642655',
    #    '_rdt_uuid': '1743818764461.628deda0-1a15-42e4-b393-030569add3ec',
    #    '_scid_r': 'C4CiSs-TsLDi7Ntu8LqZzuwVbsjfg7W4u6KmAA',
    #    '_uetsid': '87baf4c011c211f09042b3d27c4f6887',
    #    '_uetvid': '87bb146011c211f0a39469136f4c3c56',
    #    '_dpm_id.340b': 'e8344e17-8e5a-4278-8e74-77b22693acd5.1743818765.2.1743837975.1743818765.e3204961-1f94-48f6-b242-a0c8cd11de7e',
    #    'reg-entreg': 'ffffffff3b22206a45525d5f4f58455e445a4a4212d3',
    #    '_ga_QM3XHZ2B95': 'GS1.1.1743835156.5.1.1743838014.0.0.0',
    #    'w3IsGuY1': 'AyR52ASWAQAA718HRAbnATHbUeNfKMK0IBQ8ve1vp2XV4gbwWKnFAurSxtwSAWJi6KeuckX5wH8AAEB3AAAAAA==',
    #    '_ga_3NXP3C8S9V': 'GS1.1.1743833190.19.1.1743838029.0.0.0',
    #    '_ga_CSLL4ZEK4L': 'GS1.1.1743833200.19.1.1743838029.0.0.0',
    #    'kampyleUserSession': '1743838029587',
    #    'kampyleUserSessionsCount': '49',
    #    'kampyleSessionPageCounter': '1',
    #    'bm_sv': '631E83B77E50EED74AB2396CEA9B54F3~YAAQtSE1F9wCXP+VAQAAN53fBBt7kgbD1MEbLP2k2km/bkl5FTqB7lPghfkSZ/O8KUco/L2oV3tYJ4tSkfhr9DLdpjx2lgztPLpr0Bc/kuMUWbMMzlqj5HTzyMAmVXmNL0QevBDlailep+SVh28Yzqingdz04b8BEv5M73euWomG7A30cYtgMZAm9INKt7PQzhEMS8xrQh3ZdjVf7D1PbgI7o+mVh+RkiD6PMuFapdUgLwfmzGCiCL1knnmThJU=~1'
    #}
    #session.cookies.update(cookies)
    
    return session

def save_results():
    """保存结果到文件，使用追加模式并确保线程安全"""
    # 生成当天日期的文件名
    today = datetime.now().strftime("%Y%m%d")
    results_filename = f"tracking_results_{today}.json"
    
    # 获取队列中的所有结果
    results = {}
    while not results_queue.empty():
        tracking_number, result = results_queue.get()
        results[tracking_number] = result
    
    if not results:
        return []
    
    # 使用文件锁确保线程安全
    with file_lock:
        # 检查文件是否存在
        file_exists = os.path.exists(results_filename)
        
        # 如果是新文件，写入初始JSON结构
        if not file_exists:
            with open(results_filename, 'w', encoding='utf-8') as f:
                f.write('{\n')
        
        # 追加新数据
        with open(results_filename, 'a', encoding='utf-8') as f:
            # 如果不是第一条记录，添加逗号
            if file_exists and os.path.getsize(results_filename) > 2:
                f.write(',\n')
            
            # 写入新数据
            for i, (tracking_number, result) in enumerate(results.items()):
                if i > 0:
                    f.write(',\n')
                f.write(f'  "{tracking_number}": {json.dumps(result, ensure_ascii=False, indent=2)}')
            
            # 如果是新文件，添加结束括号
            if not file_exists:
                f.write('\n}')
        
        logging.info(f"已保存 {len(results)} 个结果到 {results_filename}")
    
    return list(results.keys())

def request_worker(worker_id: int, total_numbers: int, results_filename: str):
    """请求工作线程"""
    session = setup_session()
    global total_processed
    
    while True:
        try:
            batch = batch_queue.get()
        except Empty:
            break
        
        try:
            # 构建查询URL
            tracking_numbers_str = ",".join(batch)
            url = f"https://tools.usps.com/go/TrackConfirmAction?tLabels={tracking_numbers_str}"
            
            # 合并headers，添加referer
            headers = session.headers.copy()
            headers['referer'] = url
            
            # 发送请求
            response = session.get(url, headers=headers, verify=False)
            time.sleep(random.randint(1, 3))
            
            # 检查是否被重定向到维护页面
            if "anyapp_outage_apology" in response.url:
                logging.error(f"请求者 {worker_id} 被重定向到维护页面")
                time.sleep(random.randint(5,10))
                continue
            
            # 使用BeautifulSoup解析HTML
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 找到对应单号的容器
            containers = soup.find_all(class_="track-bar-container")
            if containers:
                for container in containers:
                    container_data = {
                        'tracking_number': container.find(class_="tracking-number"),
                        'lastest': container.find(class_="banner-content"),
                        'tracks': container.find_all(class_="tb-status"),
                    }
                    parse_queue.put(container_data)
                    
                    
                with progress_counter:
                    total_processed += len(batch)
                    logging.info(f"请求者 {worker_id} 已成功将 {total_processed}/{total_numbers} 个订单推向解析队列")
                    
            else:
                logging.warning(f"未找到任何单号容器")
            
        except Exception as e:
            logging.error(f"请求 {worker_id} 处理批次时出错: {str(e)}")
            continue

def parse_worker(worker_id: int):
    """解析工作线程"""
    print(f"解析工作线程 {worker_id} 已启动")
    while True:
        try:
            container_data = parse_queue.get()
        except Empty:
            time.sleep(2,5)
            continue
        
        try:
            history = []
            for track in  container_data['tracks']:
                if "toggle-history-container" in track.get('class', []):
                    continue
                    
                history_entry = {
                    "date_time": "",
                    "location": "",
                    "status": ""
                }
                
                date_element = track.find(class_="tb-date")
                if date_element:
                    history_entry["date_time"] = date_element.text.strip().replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                
                location_element = track.find(class_="tb-location")
                if location_element:
                    history_entry["location"] = location_element.text.strip().replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                
                status_element = track.find(class_="tb-status-detail")
                if status_element:
                    history_entry["status"] = status_element.text.strip().replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                
                history.append(history_entry)
            
            tracking_number = container_data['tracking_number']
            
            result = {
                "tracking_number" : tracking_number,
                "lastest": container_data['lastest'],
                "history": history
            }
            
            # 将结果放入队列，而不是直接保存
            results_queue.put((tracking_number, result))
            
            # 每处理100个结果保存一次
            if results_queue.qsize() >= 50:
                save_results()
                # 更新处理状态
                state = TrackingState()
                processed_numbers = set(state.get_processed_numbers())
                failed_numbers = set(state.get_failed_numbers())

                # 添加到已处理列表
                processed_numbers.add(tracking_number)
                # 从失败列表中移除（如果存在）
                if tracking_number in failed_numbers:
                    failed_numbers.remove(tracking_number)
                    
                # 保存更新后的状态
                state.save_state(list(processed_numbers), list(failed_numbers))
                
            print(f"解析者 {worker_id} 成功处理单号: {tracking_number}，轨迹数量: {len(history)}，队列剩余: {parse_queue.qsize()}")
                
        except Exception as e:
            logging.error(f"解析者 {worker_id} 处理跟踪结果时出错: {str(e)}")
            continue

def process_tracking_numbers(tracking_numbers: List[str], batch_size: int = 35, num_workers: int = 5):
    """使用多线程处理跟踪号码"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_filename = f"tracking_results_{timestamp}.json"
    
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
    
    total_numbers = len(tracking_numbers)
    logging.info(f"剩余待处理单号: {total_numbers} 个")
    
    # 将跟踪号码分批放入批次队列
    for i in range(0, len(tracking_numbers), batch_size):
        batch = tracking_numbers[i:i + batch_size]
        if batch:
            batch_queue.put(batch)
    
    # 创建并启动请求线程
    request_threads = []
    for i in range(num_workers):
        thread = threading.Thread(target=request_worker, args=(i+1, total_numbers, results_filename))
        thread.start()
        request_threads.append(thread)
    
    # 创建并启动解析线程
    parse_threads = []
    for i in range(num_workers):
        thread = threading.Thread(target=parse_worker, args=(i+1,))
        thread.start()
        parse_threads.append(thread)
    
    # 等待所有线程完成
    for thread in request_threads:
        thread.join()
    
    for thread in parse_threads:
        thread.join()

def main():
    try:
        # 读取Excel文件
        df = pd.read_excel('数据列表.xlsx')
        tracking_numbers = df.iloc[:, 0].astype(str).tolist()
        logging.info(f"成功读取 {len(tracking_numbers)} 个跟踪号码")
        
        # 处理跟踪号码
        process_tracking_numbers(
            tracking_numbers[:300],
            batch_size=35,
            num_workers=1
        )
        
    except Exception as e:
        logging.error(f"程序执行出错: {str(e)}")
    finally:
        logging.info("程序执行完成")

if __name__ == "__main__":
    main() 