#!/usr/bin/env python
import pika
import sys
import json
import time
import subprocess
import csv
import traceback
from utils import *
import uuid
import threading
import os
import re

class Manager:
    def __init__(self, num_keepers=3):
        self.num_keepers = num_keepers
        self.keepers = []
        self.connection = None
        self.channel = None
        self.data_index = {}  # 存储日期到存储器ID的映射

        # 初始化RabbitMQ连接
        self.init_rabbitmq()
        
        # 启动存储器进程
        self.start_keepers()
        
        print(f"Manager started, using {num_keepers} storage devices")

    def init_rabbitmq(self):
        """初始化RabbitMQ连接"""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        
        # 声明管理器队列，用于接收客户端请求
        self.channel.queue_declare(queue=MANAGER_QUEUE)
        
        # 声明客户端队列，用于向客户端发送响应
        self.channel.queue_declare(queue=CLIENT_QUEUE)
        
        # 为每个存储器声明队列
        for i in range(self.num_keepers):
            self.channel.queue_declare(queue=f'keeper_{i}')
        
        # 设置接收客户端消息的回调
        self.channel.basic_consume(
            queue=MANAGER_QUEUE,
            on_message_callback=self.process_client_message,
            auto_ack=True
        )

    def start_keepers(self):
        """启动存储器进程"""
        # 获取当前Python解释器的路径
        python_executable = sys.executable
        print(f"Using Python interpreter: {python_executable}")
        
        for i in range(self.num_keepers):
            keeper_process = subprocess.Popen(
                [python_executable, 'keeper.py', str(i), str(self.num_keepers)]
            )
            self.keepers.append(keeper_process)
            print(f"Starting storage device {i}")
            time.sleep(2)  # 增加等待时间，确保存储器完全启动

    def process_client_message(self, ch, method, properties, body):
        """处理来自客户端的消息"""
        try:
            message = parse_message(body)
            command = message.get('type')
            data = message.get('data')
            
            print(f"Received client command: {command}")
            
            if command == 'LOAD':
                self.handle_load(data)
            elif command == 'GET':
                self.handle_get(data)
            else:
                self.send_to_client(create_message('ERROR', f"Unknown command: {command}"))
        except Exception as e:
            self.send_to_client(create_message('ERROR', f"Error processing command: {str(e)}"))

    def handle_load(self, filename):
        """处理LOAD命令，加载CSV文件并分发数据"""
        try:
            print(f"Loading file: {filename}")
            
            # 尝试在当前目录和子目录中查找文件
            file_found = False
            for root, dirs, files in os.walk(os.getcwd()):  # 从当前工作目录开始查找
                if filename in files:
                    file_found = True
                    full_path = os.path.join(root, filename)
                    break
            
            if not file_found:
                print(f"File does not exist: {filename}")
                self.send_to_client(create_message('LOAD_RESULT', {'success': False, 'error': f"File does not exist: {filename}"}))
                return
            
            # 读取CSV文件
            records = []
            date_column = None
            column_names = []
            
            with open(full_path, 'r', encoding='utf-8') as f:
                # 尝试使用csv模块读取
                try:
                    reader = csv.reader(f)
                    header = next(reader)  # 读取标题行
                    column_names = header  # 保存列名
                    
                    # 尝试识别日期列
                    date_column = self.identify_date_column(header)
                    if date_column is None:
                        print(f"Cannot identify date column: {header}")
                        self.send_to_client(create_message('LOAD_RESULT', 
                            {'success': False, 'error': f"Cannot identify date column: {header}"}))
                        return
                    
                    print(f"Identified date column: {header[date_column]}")
                    
                    # 读取所有记录
                    for row in reader:
                        if len(row) > date_column:
                            records.append(row)
                except Exception as e:
                    print(f"Error reading CSV file: {e}")
                    self.send_to_client(create_message('LOAD_RESULT', 
                        {'success': False, 'error': f"Error reading CSV file: {e}"}))
                    return
            
            print(f"Read {len(records)} records")
            
            # 按日期分组记录
            date_records = {}
            for record in records:
                date_str = record[date_column]
                
                # 标准化日期格式
                try:
                    # 尝试识别日期格式并转换为标准格式 (YYYY-MM-DD)
                    standard_date = self.standardize_date(date_str)
                    if standard_date:
                        # 同时存储原始格式和标准格式
                        if standard_date not in date_records:
                            date_records[standard_date] = {
                                'data': [],
                                'column_names': column_names
                            }
                        date_records[standard_date]['data'].append(record)
                        
                        # 为了支持多种格式查询，也添加其他常见格式的索引
                        alt_formats = self.generate_alternative_date_formats(standard_date)
                        for alt_date in alt_formats:
                            if alt_date not in date_records:
                                date_records[alt_date] = date_records[standard_date]
                except Exception as e:
                    print(f"Error processing date {date_str}: {e}")
                    continue
            
            print(f"Grouped by date, there are {len(date_records)} different dates")
            
            # 分发数据到存储器
            for date, date_data in date_records.items():
                # 使用一致性哈希确定存储器
                keeper_id = get_keeper_id(date, self.num_keepers)
                print(f"Date {date} assigned to storage device {keeper_id}")
                
                # 更新数据索引
                if date not in self.data_index:
                    self.data_index[date] = []
                if keeper_id not in self.data_index[date]:
                    self.data_index[date].append(keeper_id)
                
                # 发送数据到存储器
                self.send_to_keeper(keeper_id, create_message('STORE', {
                    'date': date,
                    'data': date_data['data'],
                    'column_names': date_data['column_names']
                }))
            
            # 发送成功响应给客户端
            self.send_to_client(create_message('LOAD_RESULT', {
                'success': True,
                'filename': full_path,
                'records': len(records),
                'dates': len(date_records)
            }))
            
        except Exception as e:
            print(f"Error processing LOAD command: {e}")
            traceback.print_exc()
            self.send_to_client(create_message('LOAD_RESULT', {'success': False, 'error': str(e)}))

    def identify_date_column(self, header):
        """识别CSV文件中的日期列"""
        date_keywords = ['date', 'time', 'datetime', 'day', 'month', 'year', 'Date']
        
        # 首先尝试精确匹配
        for i, col in enumerate(header):
            if col.lower() in date_keywords:
                return i
        
        # 然后尝试部分匹配
        for i, col in enumerate(header):
            for keyword in date_keywords:
                if keyword.lower() in col.lower():
                    return i
        
        # 如果没有找到，尝试查找第一个看起来像日期的列
        for i, col in enumerate(header):
            if 'date' in col.lower() or 'time' in col.lower() or 'day' in col.lower():
                return i
        
        return 0  # 如果无法确定，默认使用第一列

    def standardize_date(self, date_str):
        """将各种日期格式转换为标准格式 (YYYY-MM-DD)"""
        try:
            # 移除引号和空格
            date_str = date_str.strip('"\'').strip()
            
            # 1. 尝试 YYYY-MM-DD 格式 (seattle-weather.csv, weather.csv)
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                year, month, day = map(int, date_str.split('-'))
                return f"{year:04d}-{month:02d}-{day:02d}"
            
            # 2. 尝试 DD-MM-YYYY 格式 (客户端查询格式)
            elif re.match(r'^\d{2}-\d{2}-\d{4}$', date_str):
                day, month, year = map(int, date_str.split('-'))
                return f"{year:04d}-{month:02d}-{day:02d}"
            
            # 3. 尝试 YYYYMMDD 格式 (weather_prediction_dataset.csv)
            elif re.match(r'^\d{8}$', date_str):
                year = int(date_str[0:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                return f"{year:04d}-{month:02d}-{day:02d}"
            
            # 4. 尝试 YYYYMMDD-HH:MM 格式 (testset.csv)
            elif re.match(r'^\d{8}-\d{2}:\d{2}$', date_str):
                date_part = date_str.split('-')[0]
                year = int(date_part[0:4])
                month = int(date_part[4:6])
                day = int(date_part[6:8])
                return f"{year:04d}-{month:02d}-{day:02d}"
            
            # 5. 尝试 MM/DD/YYYY 格式 (可能的其他格式)
            elif re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                month, day, year = map(int, date_str.split('/'))
                return f"{year:04d}-{month:02d}-{day:02d}"
            
            # 如果无法识别格式，记录并返回None
            print(f"Unrecognized date format: {date_str}")
            return None
        except Exception as e:
            print(f"Error standardizing date {date_str}: {e}")
            return None

    def generate_alternative_date_formats(self, standard_date):
        """生成日期的替代格式，用于支持多种查询格式"""
        try:
            year, month, day = map(int, standard_date.split('-'))
            
            # 生成常见的替代格式
            formats = [
                f"{day:02d}-{month:02d}-{year:04d}",  # DD-MM-YYYY
                f"{year:04d}{month:02d}{day:02d}"     # YYYYMMDD
            ]
            
            return formats
        except Exception as e:
            print(f"Error generating alternative date formats: {e}")
            return []

    def handle_get(self, date_str):
        """处理GET命令，从存储器获取特定日期的数据"""
        try:
            # 标准化日期格式
            standard_date = self.standardize_date(date_str)
            if not standard_date:
                print(f"Invalid date format: {date_str}")
                empty_response = {
                    'date': date_str,
                    'found': False,
                    'data': [],
                    'count': 0
                }
                self.send_to_client(create_message('GET_RESULT', empty_response))
                return
            
            print(f"Standardized date: {date_str}, original format: {date_str}")
            
            # 查找哪个存储器有这个日期的数据
            keeper_ids = []
            
            # 尝试使用标准格式和原始格式查找
            for date_format in [standard_date, date_str]:
                if date_format in self.data_index:
                    keeper_ids = self.data_index[date_format]
                    break
            
            # 尝试使用替代格式查找
            if not keeper_ids:
                alt_formats = self.generate_alternative_date_formats(standard_date)
                for alt_date in alt_formats:
                    if alt_date in self.data_index:
                        keeper_ids = self.data_index[alt_date]
                        break
            
            if not keeper_ids:
                print(f"No data found for date: {date_str}")
                print(f"Dates in data index: {list(self.data_index.keys())[:10]}")
                
                # 创建符合客户端期望格式的响应
                empty_response = {
                    'date': date_str,  # 返回原始格式日期
                    'found': False,
                    'data': [],
                    'count': 0
                }
                self.send_to_client(create_message('GET_RESULT', empty_response))
                return
            
            print(f"Found data for date: {date_str}, stored in keepers: {keeper_ids}")
            
            # 向第一个存储器请求数据
            keeper_id = keeper_ids[0]
            print(f"Getting data from storage device {keeper_id}")
            
            # 创建一个新的连接和通道，专门用于处理这个请求
            temp_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            temp_channel = temp_connection.channel()
            
            # 创建一个临时队列
            result = temp_channel.queue_declare(queue='', exclusive=True)
            temp_queue = result.method.queue
            print(f"Created temporary queue: {temp_queue}")
            
            # 设置响应标志和数据
            response_received = False
            response_data = None
            
            # 定义回调函数
            def on_response(ch, method, props, body):
                nonlocal response_received, response_data
                response_received = True
                response_data = body
                temp_channel.stop_consuming()
            
            # 设置消费者
            temp_channel.basic_consume(
                queue=temp_queue,
                on_message_callback=on_response,
                auto_ack=True
            )
            
            # 发送请求到存储器
            request_data = {
                'date': standard_date,
                'original_date': date_str  # 保留原始日期格式
            }
            
            temp_channel.basic_publish(
                exchange='',
                routing_key=f'keeper_{keeper_id}',
                properties=pika.BasicProperties(
                    reply_to=temp_queue,
                    correlation_id=str(uuid.uuid4())
                ),
                body=create_message('GET', request_data)
            )
            
            print(f"Sent GET request to storage device {keeper_id}, waiting for response...")
            
            # 设置超时事件
            timeout_event = threading.Event()
            
            # 超时线程函数
            def timeout_thread():
                # 等待5秒
                for _ in range(50):  # 5秒 = 50 * 0.1秒
                    if timeout_event.is_set():
                        return
                    time.sleep(0.1)
                
                # 如果超时，停止消费
                if not response_received:
                    print("Request timed out")
                    try:
                        temp_channel.stop_consuming()
                    except:
                        pass
            
            # 启动超时线程
            timer_thread = threading.Thread(target=timeout_thread)
            timer_thread.daemon = True
            timer_thread.start()
            
            # 开始消费，这会阻塞直到收到消息或超时
            print("Starting to wait for response...")
            temp_channel.start_consuming()
            print("Finished waiting for response")
            
            # 设置超时事件，通知超时线程退出
            timeout_event.set()
            
            # 关闭临时连接
            temp_connection.close()
            
            # 检查是否收到响应
            if response_received and response_data:
                print(f"Successfully received response, forwarding to client")
                # 解析响应
                response_message = parse_message(response_data)
                response_type = response_message.get('type')
                response_content = response_message.get('data', {})
                
                # 确保数据格式一致
                if response_type == 'GET_RESULT' and response_content.get('found'):
                    # 确保数据是一个扁平的列表，而不是嵌套列表
                    data = response_content.get('data', [])
                    if data and isinstance(data[0], list) and isinstance(data[0][0], list):
                        # 如果是嵌套列表，将其展平
                        flat_data = []
                        for group in data:
                            flat_data.extend(group)
                        response_content['data'] = flat_data
                
                # 转发响应给客户端
                self.send_to_client(create_message(response_type, response_content))
            else:
                print(f"No response received, sending empty response to client")
                # 发送空响应
                empty_response = {
                    'date': date_str,
                    'found': False,
                    'data': [],
                    'count': 0
                }
                self.send_to_client(create_message('GET_RESULT', empty_response))
            
        except Exception as e:
            print(f"Error processing GET command: {e}")
            traceback.print_exc()
            
            # 发送错误响应给客户端
            error_response = {
                'date': date_str,
                'found': False,
                'data': [],
                'count': 0,
                'error': str(e)
            }
            self.send_to_client(create_message('GET_RESULT', error_response))

    def send_to_keeper(self, keeper_id, message):
        """发送消息到指定的存储器"""
        self.channel.basic_publish(
            exchange='',
            routing_key=f'keeper_{keeper_id}',
            body=message
        )

    def send_to_client(self, message):
        """发送消息到客户端"""
        self.channel.basic_publish(
            exchange='',
            routing_key=CLIENT_QUEUE,
            body=message
        )

    def run(self):
        """运行管理器，开始监听客户端消息"""
        print("Manager is running, waiting for client commands...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("Manager is shutting down...")
            # 关闭所有存储器进程
            for keeper in self.keepers:
                keeper.terminate()
            self.connection.close()

if __name__ == "__main__":
    # 从命令行参数获取存储器数量，默认为3
    num_keepers = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    manager = Manager(num_keepers)
    manager.run()