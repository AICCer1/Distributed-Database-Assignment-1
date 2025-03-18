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
from utils import ConsistentHashRing
import argparse

class Manager:
    def __init__(self, num_keepers=3, verbose=False):
        self.num_keepers = num_keepers
        self.keepers = []
        self.replicas = []  # 添加replicas列表来跟踪replica进程
        self.connection = None
        self.channel = None
        self.data_index = {}
        self.hash_ring = ConsistentHashRing()
        self.keeper_status = {}  # Track keeper status (alive/dead)
        self.health_check_interval = 8  # 将健康检查间隔从30秒减少到8秒
        self.health_check_thread = None
        self.health_check_running = False
        self.verbose = verbose  # 是否启用详细日志输出

        self.init_rabbitmq()
        
        self.start_keepers()
        
        # Start health check thread
        self.start_health_check()
        
        print(f"Manager started, using {num_keepers} storage devices")

    def init_rabbitmq(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        
        self.channel.queue_declare(queue=MANAGER_QUEUE)
        
        self.channel.queue_declare(queue=CLIENT_QUEUE)
        
        for i in range(self.num_keepers):
            self.channel.queue_declare(queue=f'keeper_{i}')
        
        self.channel.basic_consume(
            queue=MANAGER_QUEUE,
            on_message_callback=self.process_client_message,
            auto_ack=True
        )

    def start_keepers(self):
        python_executable = sys.executable
        print(f"Using Python interpreter: {python_executable}")
        
        for i in range(self.num_keepers):
            # 启动keeper进程
            keeper_process = subprocess.Popen(
                [python_executable, 'keeper.py', str(i), str(self.num_keepers), 
                 '--verbose' if self.verbose else '--quiet']
            )
            self.keepers.append(keeper_process)
            self.hash_ring.add_node(f'keeper_{i}')
            print(f"Starting storage device {i}")
            
            # 注意：keeper会自己启动replica，所以这里不需要再启动
            # 但我们仍然需要等待一段时间，确保keeper和replica都启动完成
            time.sleep(2)  # 给进程启动的时间

    def remove_keeper(self, keeper_id):
        self.hash_ring.remove_node(f'keeper_{keeper_id}')
        if self.keepers[keeper_id]:
            self.keepers[keeper_id].terminate()
            self.keepers[keeper_id] = None
        print(f"Removed keeper {keeper_id} from the hash ring")
        
        # 注意：不要终止replica进程，因为我们需要它来恢复数据

    def process_client_message(self, ch, method, properties, body):
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
        try:
            print(f"Loading file: {filename}")
            
            # 首先检查是否有可用的keeper节点
            available_keepers = [i for i in range(self.num_keepers) 
                               if self.keepers[i] is not None and 
                               (i not in self.keeper_status or self.keeper_status[i])]
            
            if not available_keepers:
                print("No available keeper nodes in the system!")
                self.send_to_client(create_message('LOAD_RESULT', {
                    'success': False, 
                    'error': "No available storage nodes in the system. All nodes have failed. Cannot load data."
                }))
                return
            
            file_found = False
            for root, dirs, files in os.walk(os.getcwd()):
                if filename in files:
                    file_found = True
                    full_path = os.path.join(root, filename)
                    break
            
            if not file_found:
                print(f"File does not exist: {filename}")
                self.send_to_client(create_message('LOAD_RESULT', {'success': False, 'error': f"File does not exist: {filename}"}))
                return
            
            records = []
            date_column = None
            column_names = []
            
            # 发送开始加载的通知
            self.send_to_client(create_message('LOAD_RESULT', {
                'success': True,
                'filename': full_path,
                'status': 'started',
                'message': f"Started loading file: {filename}"
            }))
            
            with open(full_path, 'r', encoding='utf-8') as f:
                try:
                    reader = csv.reader(f)
                    header = next(reader)
                    column_names = header
                    
                    date_column = self.identify_date_column(header)
                    if date_column is None:
                        print(f"Cannot identify date column: {header}")
                        self.send_to_client(create_message('LOAD_RESULT', 
                            {'success': False, 'error': f"Cannot identify date column: {header}"}))
                        return
                    
                    print(f"Identified date column: {header[date_column]}")
                    
                    # 读取所有记录
                    record_count = 0
                    progress_interval = 1000  # 每处理1000条记录发送一次进度通知
                    
                    for row in reader:
                        if len(row) > date_column:
                            records.append(row)
                            record_count += 1
                            
                            # 定期发送进度通知
                            if record_count % progress_interval == 0:
                                print(f"Processed {record_count} records so far")
                                self.send_to_client(create_message('LOAD_RESULT', {
                                    'success': True,
                                    'filename': full_path,
                                    'status': 'progress',
                                    'records_processed': record_count,
                                    'message': f"Processing file: {filename}, records processed: {record_count}"
                                }))
                                
                except Exception as e:
                    print(f"Error reading CSV file: {e}")
                    self.send_to_client(create_message('LOAD_RESULT', 
                        {'success': False, 'error': f"Error reading CSV file: {e}"}))
                    return
            
            print(f"Read {len(records)} records")
            
            # 发送记录读取完成的通知
            self.send_to_client(create_message('LOAD_RESULT', {
                'success': True,
                'filename': full_path,
                'status': 'records_read',
                'records': len(records),
                'message': f"Read {len(records)} records from file: {filename}"
            }))
            
            date_records = {}
            for record in records:
                date_str = record[date_column]
                
                try:
                    standard_date = self.standardize_date(date_str)
                    if standard_date:
                        if standard_date not in date_records:
                            date_records[standard_date] = {
                                'data': [],
                                'column_names': column_names,
                                'source_file': filename
                            }
                        date_records[standard_date]['data'].append(record)
                        
                        # 完全禁用替代日期格式的生成
                        # alt_formats = self.generate_alternative_date_formats(standard_date)
                        # for alt_date in alt_formats:
                        #     if alt_date not in date_records:
                        #         date_records[alt_date] = date_records[standard_date]
                except Exception as e:
                    print(f"Error processing date {date_str}: {e}")
                    continue
            
            print(f"Grouped by date, there are {len(date_records)} different dates")
            
            # Count unique calendar dates (without alternative formats)
            unique_calendar_dates = set()
            for date_str in date_records.keys():
                if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                    unique_calendar_dates.add(date_str)
            
            # 计算实际的日期格式条目数（不包括替代格式）
            actual_date_entries = len(unique_calendar_dates)
            total_date_entries = len(date_records)
            alternative_formats_count = total_date_entries - actual_date_entries
            
            print(f"Actual unique dates: {actual_date_entries}")
            print(f"Total date entries including alternative formats: {total_date_entries}")
            print(f"Alternative format entries: {alternative_formats_count}")
            
            # 发送日期分组完成的通知
            self.send_to_client(create_message('LOAD_RESULT', {
                'success': True,
                'filename': full_path,
                'status': 'dates_grouped',
                'records': len(records),
                'dates': actual_date_entries,  # 使用实际日期数
                'message': f"Grouped {len(records)} records into {actual_date_entries} unique dates"
            }))
            
            # 发送数据到keeper
            dates_processed = 0
            total_dates = len(date_records)
            print(f"Starting to send data to keepers, total dates to process: {total_dates}")
            
            for date, date_data in date_records.items():
                keeper_queue = self.hash_ring.get_node(date)
                if keeper_queue:
                    keeper_id = int(keeper_queue.split('_')[1])
                    
                    if date not in self.data_index:
                        self.data_index[date] = []
                    if keeper_id not in self.data_index[date]:
                        self.data_index[date].append(keeper_id)
                    
                    self.send_to_keeper(keeper_id, create_message('STORE', {
                        'date': date,
                        'data': date_data['data'],
                        'column_names': date_data['column_names'],
                        'source_file': filename
                    }))
                    
                    dates_processed += 1
                    # 每处理100个日期发送一次进度通知，但不打印太多信息
                    if dates_processed % 100 == 0:
                        progress_percent = (dates_processed / total_dates) * 100
                        print(f"Progress: {progress_percent:.1f}% - Processed {dates_processed}/{total_dates} dates")
                else:
                    print(f"No available keeper found for date: {date}")
            
            print(f"All data sent to keepers. Processed {dates_processed}/{total_dates} dates")
            
            # 发送最终完成的通知
            print(f"Sending completion notification to client for file: {filename}")
            self.send_to_client(create_message('LOAD_RESULT', {
                'success': True,
                'filename': full_path,
                'status': 'completed',
                'records': len(records),
                'dates': actual_date_entries,  # 使用实际日期数
                'message': f"File loaded successfully: {filename}"
            }))
            print(f"Load operation completed for file: {filename}")
            
        except Exception as e:
            print(f"Error processing LOAD command: {e}")
            traceback.print_exc()
            self.send_to_client(create_message('LOAD_RESULT', {'success': False, 'error': str(e)}))

    def identify_date_column(self, header):
        date_keywords = ['date', 'time', 'datetime', 'day', 'month', 'year', 'Date']
        
        for i, col in enumerate(header):
            if col.lower() in date_keywords:
                return i
        
        for i, col in enumerate(header):
            for keyword in date_keywords:
                if keyword.lower() in col.lower():
                    return i
        
        for i, col in enumerate(header):
            if 'date' in col.lower() or 'time' in col.lower() or 'day' in col.lower():
                return i
        
        return 0

    def standardize_date(self, date_str):
        try:
            date_str = date_str.strip('"\'').strip()
            
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                year, month, day = map(int, date_str.split('-'))
                return f"{year:04d}-{month:02d}-{day:02d}"
            
            elif re.match(r'^\d{2}-\d{2}-\d{4}$', date_str):
                day, month, year = map(int, date_str.split('-'))
                return f"{year:04d}-{month:02d}-{day:02d}"
            
            elif re.match(r'^\d{8}$', date_str):
                year = int(date_str[0:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                return f"{year:04d}-{month:02d}-{day:02d}"
            
            elif re.match(r'^\d{8}-\d{2}:\d{2}$', date_str):
                date_part = date_str.split('-')[0]
                year = int(date_part[0:4])
                month = int(date_part[4:6])
                day = int(date_part[6:8])
                return f"{year:04d}-{month:02d}-{day:02d}"
            
            elif re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                month, day, year = map(int, date_str.split('/'))
                return f"{year:04d}-{month:02d}-{day:02d}"
            
            print(f"Unrecognized date format: {date_str}")
            return None
        except Exception as e:
            print(f"Error standardizing date {date_str}: {e}")
            return None

    def generate_alternative_date_formats(self, standard_date):
        try:
            year, month, day = map(int, standard_date.split('-'))
            
            # 只生成一种替代格式，减少重复计数
            formats = [
                f"{day:02d}-{month:02d}-{year:04d}"
                # 移除YYYYMMDD格式，减少重复计数
            ]
            
            return formats
        except Exception as e:
            print(f"Error generating alternative date formats: {e}")
            return []

    def handle_get(self, date_str):
        try:
            standard_date = self.standardize_date(date_str)
            if not standard_date:
                print(f"Invalid date format: {date_str}")
                empty_response = {
                    'date': date_str,
                    'found': False,
                    'datasets': [],
                    'count': 0,
                    'error': "Invalid date format"
                }
                self.send_to_client(create_message('GET_RESULT', empty_response))
                return
            
            print(f"Standardized date: {standard_date}, original format: {date_str}")
            
            # 检查是否有可用的keeper节点
            available_keepers = [i for i in range(self.num_keepers) 
                               if self.keepers[i] is not None and 
                               (i not in self.keeper_status or self.keeper_status[i])]
            
            if not available_keepers:
                print("No available keeper nodes in the system!")
                error_response = {
                    'date': date_str,
                    'found': False,
                    'datasets': [],
                    'count': 0,
                    'error': "No available storage nodes in the system. All nodes have failed."
                }
                self.send_to_client(create_message('GET_RESULT', error_response))
                return
            
            # 创建一个已尝试过的keeper列表，避免重复尝试
            tried_keepers = []
            # 创建一个列表存储所有找到的数据集
            all_datasets = []
            # 记录所有查询过的keeper节点
            keepers_queried = []
            # 记录是否找到了数据
            data_found = False
            
            # 尝试查询数据的函数
            def try_get_data_from_keeper(keeper_id):
                nonlocal tried_keepers, all_datasets, data_found, keepers_queried
                
                if keeper_id in tried_keepers:
                    print(f"Already tried keeper {keeper_id}, skipping")
                    return None
                
                tried_keepers.append(keeper_id)
                print(f"Getting data from storage device {keeper_id}")
                
                temp_connection = None
                try:
                    temp_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                    temp_channel = temp_connection.channel()
                    
                    result = temp_channel.queue_declare(queue='', exclusive=True)
                    temp_queue = result.method.queue
                    print(f"Created temporary queue: {temp_queue}")
                    
                    response_received = False
                    response_data = None
                    
                    def on_response(ch, method, props, body):
                        nonlocal response_received, response_data
                        response_received = True
                        response_data = body
                        try:
                            temp_channel.stop_consuming()
                        except Exception as e:
                            print(f"Error stopping consumption: {e}")
                    
                    temp_channel.basic_consume(
                        queue=temp_queue,
                        on_message_callback=on_response,
                        auto_ack=True
                    )
                    
                    request_data = {
                        'date': standard_date,
                        'original_date': date_str
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
                    
                    timeout_seconds = 5  # 增加超时时间
                    timeout_event = threading.Event()
                    
                    def timeout_thread():
                        start_time = time.time()
                        while time.time() - start_time < timeout_seconds:
                            if timeout_event.is_set() or response_received:
                                return
                            time.sleep(0.1)
                        
                        if not response_received:
                            print(f"Request timed out after {timeout_seconds} seconds")
                            try:
                                temp_channel.stop_consuming()
                            except:
                                pass
                    
                    timer_thread = threading.Thread(target=timeout_thread)
                    timer_thread.daemon = True
                    timer_thread.start()
                    
                    print("Starting to wait for response...")
                    try:
                        temp_channel.start_consuming()
                    except Exception as e:
                        print(f"Error during consumption: {e}")
                    print("Finished waiting for response")
                    
                    timeout_event.set()
                    
                    if temp_connection:
                        temp_connection.close()
                    
                    if response_received and response_data:
                        print(f"Successfully received response from keeper {keeper_id}")
                        response_message = parse_message(response_data)
                        response_type = response_message.get('type')
                        response_content = response_message.get('data', {})
                        
                        # 检查是否是"数据不存在"的响应
                        if response_type == 'GET_RESULT' and not response_content.get('found', False):
                            print(f"Keeper {keeper_id} reported that data for date {date_str} does not exist")
                            return False  # 表示节点正常，但没有数据
                        
                        if response_type == 'GET_RESULT' and response_content.get('found'):
                            keepers_queried.append(keeper_id)
                            data_found = True
                            
                            if 'datasets' in response_content:
                                print(f"DEBUG - Received response with datasets structure")
                                # 添加keeper_id信息到每个数据集
                                for dataset in response_content['datasets']:
                                    dataset['keeper_id'] = keeper_id
                                # 添加数据集到结果集合
                                all_datasets.extend(response_content['datasets'])
                            else:
                                data = response_content.get('data', [])
                                column_names = response_content.get('column_names', [])
                                
                                if data and isinstance(data[0], list) and isinstance(data[0][0], list):
                                    flat_data = []
                                    for group in data:
                                        flat_data.extend(group)
                                    data = flat_data
                                
                                # 创建新的数据集并添加到结果集合
                                new_dataset = {
                                    'data': data,
                                    'column_names': column_names,
                                    'source_file': response_content.get('source_file', 'unknown'),
                                    'keeper_id': keeper_id
                                }
                                all_datasets.append(new_dataset)
                            
                            # 更新数据索引
                            if standard_date in self.data_index:
                                if isinstance(self.data_index[standard_date], list):
                                    if keeper_id not in self.data_index[standard_date]:
                                        self.data_index[standard_date].append(keeper_id)
                                else:
                                    self.data_index[standard_date] = [keeper_id]
                            else:
                                self.data_index[standard_date] = [keeper_id]
                            
                            print(f"Updated data index for date {standard_date}: {self.data_index[standard_date]}")
                            
                            return True  # 表示找到了数据
                    else:
                        print(f"No response received from keeper {keeper_id}")
                        
                        # 检查是否是因为节点故障
                        is_keeper_down = self.check_keeper_health(keeper_id)
                        
                        if is_keeper_down:
                            print(f"Keeper {keeper_id} is down, marking as unavailable")
                            self.keeper_status[keeper_id] = False
                            # 从数据索引中移除
                            if standard_date in self.data_index:
                                if isinstance(self.data_index[standard_date], list) and keeper_id in self.data_index[standard_date]:
                                    self.data_index[standard_date].remove(keeper_id)
                                    print(f"Removed unresponsive keeper {keeper_id} from data index for date {standard_date}")
                            return None  # 表示节点不可用
                        else:
                            print(f"Keeper {keeper_id} is healthy, just no data for date {date_str}")
                            return False  # 表示节点正常，但没有数据
                
                except Exception as e:
                    print(f"Error getting data from keeper {keeper_id}: {e}")
                    traceback.print_exc()
                    if temp_connection:
                        try:
                            temp_connection.close()
                        except:
                            pass
                    return None  # 表示出错
            
            # 首先尝试从数据索引中获取keeper_ids
            keeper_ids = []
            for date_format in [standard_date, date_str]:
                if date_format in self.data_index:
                    keeper_ids = self.data_index[date_format]
                    print(f"Found date {date_format} in index, stored in keepers: {keeper_ids}")
                    break
            
            # 尝试从索引中的keeper获取数据
            if keeper_ids:
                for keeper_id in keeper_ids:
                    if isinstance(keeper_id, int) and (keeper_id not in self.keeper_status or self.keeper_status[keeper_id]):
                        try_get_data_from_keeper(keeper_id)
                    elif isinstance(keeper_id, str) and keeper_id.startswith('keeper_'):
                        k_id = int(keeper_id.split('_')[1])
                        if k_id not in self.keeper_status or self.keeper_status[k_id]:
                            try_get_data_from_keeper(k_id)
            
            # 如果索引中的keeper都没有数据，使用哈希环找到其他可能的keeper
            if not data_found:
                print(f"No data found in indexed keepers for date {date_str}, trying other keepers using hash ring")
                
                # 获取所有可用的keeper节点
                available_keepers = [i for i in range(self.num_keepers) 
                                   if i not in tried_keepers and 
                                   self.keepers[i] is not None and 
                                   (i not in self.keeper_status or self.keeper_status[i])]
                
                # 使用哈希环找到可能的keeper
                for i in range(min(3, len(available_keepers))):  # 最多尝试3个其他keeper
                    keeper_queue = self.hash_ring.get_node(standard_date)
                    if keeper_queue:
                        keeper_id = int(keeper_queue.split('_')[1])
                        if keeper_id in available_keepers and keeper_id not in tried_keepers:
                            print(f"Trying keeper {keeper_id} from hash ring")
                            try_get_data_from_keeper(keeper_id)
                    
                    # 如果没有找到数据，尝试下一个可能的keeper
                    if available_keepers and not data_found:
                        next_keeper = available_keepers[0]
                        available_keepers.remove(next_keeper)
                        if next_keeper not in tried_keepers:
                            print(f"Trying next available keeper {next_keeper}")
                            try_get_data_from_keeper(next_keeper)
            
            # 处理结果
            if data_found and all_datasets:
                # 计算总记录数
                total_records = sum(len(dataset.get('data', [])) for dataset in all_datasets)
                
                # 构建完整响应
                complete_response = {
                    'date': date_str,
                    'found': True,
                    'datasets': all_datasets,
                    'count': total_records,
                    'keepers_queried': keepers_queried  # 添加查询过的keeper节点列表
                }
                
                # 发送合并后的结果给客户端
                self.send_to_client(create_message('GET_RESULT', complete_response))
                return
            else:
                # 如果所有尝试都失败，返回空响应
                print(f"No data found for date {date_str} in any keeper")
                empty_response = {
                    'date': date_str,
                    'found': False,
                    'datasets': [],
                    'count': 0,
                    'error': "No data found for this date"
                }
                self.send_to_client(create_message('GET_RESULT', empty_response))
            
        except Exception as e:
            print(f"Error processing GET command: {e}")
            traceback.print_exc()
            
            error_response = {
                'date': date_str,
                'found': False,
                'datasets': [],
                'count': 0,
                'error': str(e)
            }
            self.send_to_client(create_message('GET_RESULT', error_response))
            
    def check_keeper_health(self, keeper_id):
        """Check if a keeper is healthy by sending a ping message
        Returns True if keeper is healthy, False if not healthy"""
        # 如果keeper已经被标记为不可用，直接返回False
        if keeper_id in self.keeper_status and not self.keeper_status[keeper_id]:
            return False
            
        try:
            # 创建临时连接和通道
            temp_connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost',
                connection_attempts=3,
                retry_delay=1,
                socket_timeout=5
            ))
            temp_channel = temp_connection.channel()
            
            # 创建临时回调队列
            result = temp_channel.queue_declare(queue='', exclusive=True)
            callback_queue = result.method.queue
            
            # 设置响应标志
            response_received = False
            
            # 定义回调函数
            def on_response(ch, method, props, body):
                nonlocal response_received
                response_received = True
                print(f"Received health check response from keeper {keeper_id}")
            
            # 设置消费者
            temp_channel.basic_consume(
                queue=callback_queue,
                on_message_callback=on_response,
                auto_ack=True
            )
            
            # 发送ping消息
            temp_channel.basic_publish(
                exchange='',
                routing_key=f'keeper_{keeper_id}',
                properties=pika.BasicProperties(
                    reply_to=callback_queue,
                    correlation_id=str(uuid.uuid4())
                ),
                body=json.dumps({
                    'type': 'PING',
                    'timestamp': time.time()
                })
            )
            
            print(f"Sent health check ping to keeper {keeper_id}")
            
            # 等待响应，最多5秒
            start_time = time.time()
            while time.time() - start_time < 5:
                temp_connection.process_data_events(time_limit=0.1)
                if response_received:
                    break
            
            # 清理资源
            try:
                temp_channel.queue_delete(queue=callback_queue)
            except Exception as e:
                print(f"Error deleting queue: {e}")
                traceback.print_exc()
            
            try:
                temp_connection.close()
            except Exception as e:
                print(f"Error closing connection: {e}")
                traceback.print_exc()
            
            # 返回健康状态
            return response_received
            
        except Exception as e:
            print(f"Error checking health of keeper {keeper_id}: {e}")
            traceback.print_exc()
            return False

    def send_to_keeper(self, keeper_id, message):
        self.channel.basic_publish(
            exchange='',
            routing_key=f'keeper_{keeper_id}',
            body=message
        )

    def send_to_client(self, message):
        self.channel.basic_publish(
            exchange='',
            routing_key=CLIENT_QUEUE,
            body=message
        )

    def run(self):
        print("Manager is running, waiting for client commands...")
        
        # 添加一个定期检查所有节点状态的线程
        def check_system_status():
            while True:
                time.sleep(7)  # 每7秒检查一次，从120秒改为7秒
                
                # 检查是否有可用的keeper节点
                available_keepers = [i for i in range(self.num_keepers) 
                                   if self.keepers[i] is not None and 
                                   (i not in self.keeper_status or self.keeper_status[i])]
                
                if not available_keepers:
                    print("\n" + "="*80)
                    print("*** CRITICAL WARNING: No available keeper nodes in the system! ***")
                    print("*** The system cannot serve requests in this state. ***")
                    print("*** Please restart the system using: python manager.py <num_keepers> ***")
                    print("*** All data operations will fail until the system is restarted. ***")
                    print("="*80 + "\n")
                else:
                    print(f"System status: {len(available_keepers)}/{self.num_keepers} keeper nodes available")
        
        status_thread = threading.Thread(target=check_system_status)
        status_thread.daemon = True
        status_thread.start()
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("Manager is shutting down...")
            for keeper in self.keepers:
                if keeper:
                    keeper.terminate()
            self.connection.close()

    def process_query(self, query):
        if query.startswith("LOAD"):
            filename = query.split(" ")[1]
            if self.keeper_alive:
                return self.keeper.load_data(filename)
            else:
                return "Keeper is not available"
        elif query.startswith("GET"):
            date = query.split(" ")[1]
            
            # 打印调试信息
            print(f"Processing GET query for date: {date}")
            
            if self.keeper_alive:
                data = self.keeper.get_data(date)
                print(f"Data from keeper: {data}")
            else:
                # 尝试从可用的副本获取数据
                data = None
                for replica in self.replicas:
                    if replica.is_alive():
                        data = replica.get_data(date)
                        print(f"Data from replica: {data}")
                        if data:
                            break
                else:
                    return "No available replicas or data not found"
            
            # 处理返回的多条记录
            if not data:
                return f"No data found for date {date}"
            else:
                # 格式化多条记录的返回结果
                result = f"Data for {date}:\n"
                for i, record in enumerate(data):
                    result += f"Record {i+1}: {','.join(record)}\n"
                return result
        elif query.startswith("ADD"):
            parts = query.split(" ")
            date = parts[1]
            values = parts[2:]
            if self.keeper_alive:
                return self.keeper.add_data(date, values)
            else:
                return "Keeper is not available"
        else:
            return "Unknown query"

    def start_health_check(self):
        """Start a thread to periodically check keeper health"""
        self.health_check_running = True
        self.health_check_thread = threading.Thread(target=self.health_check_loop)
        self.health_check_thread.daemon = True
        self.health_check_thread.start()
        print(f"Health check monitoring started (interval: {self.health_check_interval}s)")

    def health_check_loop(self):
        """Continuously check keeper health at regular intervals"""
        while self.health_check_running:
            self.check_keepers_health()
            time.sleep(self.health_check_interval)

    def check_keepers_health(self):
        """Check if all keepers are responsive"""
        for i in range(self.num_keepers):
            # Skip if keeper is already known to be dead
            if i in self.keeper_status and not self.keeper_status[i]:
                continue
                
            # Skip if keeper process is None (already removed)
            if self.keepers[i] is None:
                continue
                
            # Send a health check message to the keeper
            try:
                # 使用单独的连接进行健康检查，避免影响主连接
                temp_connection = None
                
                try:
                    temp_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                    temp_channel = temp_connection.channel()
                    
                    # Create a correlation ID for this request
                    corr_id = str(uuid.uuid4())
                    
                    # Create a temporary response queue
                    result = temp_channel.queue_declare(queue='', exclusive=True)
                    callback_queue = result.method.queue
                    
                    # Set up a consumer for the response
                    response_received = False
                    
                    def on_response(ch, method, props, body):
                        nonlocal response_received
                        if props.correlation_id == corr_id:
                            response_received = True
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                    temp_channel.basic_consume(
                        queue=callback_queue,
                        on_message_callback=on_response,
                        auto_ack=False
                    )
                    
                    # Send health check message
                    temp_channel.basic_publish(
                        exchange='',
                        routing_key=f'keeper_{i}',
                        properties=pika.BasicProperties(
                            reply_to=callback_queue,
                            correlation_id=corr_id,
                        ),
                        body=create_message('HEALTH_CHECK', {})
                    )
                    
                    # Wait for response with timeout
                    start_time = time.time()
                    while not response_received and time.time() - start_time < 2:  # 2 second timeout
                        try:
                            temp_connection.process_data_events(time_limit=0.1)
                        except Exception as e:
                            print(f"Error processing events during health check: {e}")
                            break
                    
                    # Clean up the temporary queue
                    try:
                        temp_channel.queue_delete(queue=callback_queue)
                    except Exception as e:
                        print(f"Error deleting queue: {e}")
                        traceback.print_exc()
                    
                    # Close the temporary connection
                    try:
                        temp_connection.close()
                    except Exception as e:
                        print(f"Error closing connection: {e}")
                        traceback.print_exc()
                    
                    # If no response, mark keeper as dead and handle failure
                    if not response_received:
                        print(f"Keeper {i} is not responding, marking as dead")
                        self.keeper_status[i] = False
                        
                        # 在单独的线程中处理故障，避免阻塞健康检查
                        failure_thread = threading.Thread(
                            target=self.handle_keeper_failure,
                            args=(i,)
                        )
                        failure_thread.daemon = True
                        failure_thread.start()
                    else:
                        # Keeper is alive - 不打印正常的健康检查响应
                        self.keeper_status[i] = True
                
                except Exception as e:
                    print(f"Error during health check for keeper {i}: {e}")
                    
                    # 确保清理资源
                    if temp_connection:
                        try:
                            temp_connection.close()
                        except:
                            pass
                    
                    # 如果连接出错，可能是keeper已经故障
                    print(f"Connection error during health check for keeper {i}, marking as potentially dead")
                    # 不立即标记为死亡，等待下一次健康检查确认
                    
            except Exception as e:
                print(f"Critical error checking health of keeper {i}: {str(e)}")
                traceback.print_exc()

    def handle_keeper_failure(self, failed_keeper_id):
        """Handle the failure of a keeper by redistributing its data"""
        print(f"Handling failure of keeper {failed_keeper_id}")
        
        try:
            # 防止重复处理同一个故障
            if failed_keeper_id in self.keeper_status and not self.keeper_status[failed_keeper_id]:
                # 检查是否已经在处理中，如果是则返回
                if hasattr(self, 'handling_failure') and failed_keeper_id in self.handling_failure:
                    print(f"Keeper {failed_keeper_id} failure is already being handled")
                    return
                
            # 标记正在处理此keeper的故障
            if not hasattr(self, 'handling_failure'):
                self.handling_failure = set()
            self.handling_failure.add(failed_keeper_id)
                
            # 标记keeper为故障状态
            self.keeper_status[failed_keeper_id] = False
            
            # 检查是否还有可用的keeper节点
            available_keepers = [i for i in range(self.num_keepers) 
                               if i != failed_keeper_id and 
                               self.keepers[i] is not None and 
                               (i not in self.keeper_status or self.keeper_status[i])]
            
            if not available_keepers:
                print("\n" + "="*80)
                print(f"*** CRITICAL WARNING: No available keeper nodes left in the system after removing keeper {failed_keeper_id}! ***")
                print("*** The system cannot recover from this state. All data is now unavailable. ***")
                print("*** Please restart the system using: python manager.py <num_keepers> ***")
                print("*** All data operations will fail until the system is restarted. ***")
                print("="*80 + "\n")
                # 清理处理标记
                self.handling_failure.remove(failed_keeper_id)
                return
            
            # 确保replica队列存在
            replica_exists = False
            try:
                temp_connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host='localhost',
                    connection_attempts=3,
                    retry_delay=1,
                    socket_timeout=5
                ))
                temp_channel = temp_connection.channel()
                
                # 检查replica队列是否存在
                replica_queue = f'replica_{failed_keeper_id}'
                try:
                    temp_channel.queue_declare(queue=replica_queue, passive=True)
                    print(f"Replica queue {replica_queue} exists")
                    replica_exists = True
                except pika.exceptions.ChannelClosedByBroker:
                    print(f"Replica queue {replica_queue} does not exist")
                    replica_exists = False
                
                temp_connection.close()
                
                if not replica_exists:
                    print(f"No replica available for keeper {failed_keeper_id}, cannot migrate data")
                    # 即使没有replica，也要从数据索引中移除失败的keeper
                    self.remove_from_data_index(failed_keeper_id)
                    self.hash_ring.remove_node(f'keeper_{failed_keeper_id}')
                    print(f"Removed keeper {failed_keeper_id} from the hash ring")
                    
                    # 终止keeper进程
                    if self.keepers[failed_keeper_id]:
                        try:
                            self.keepers[failed_keeper_id].terminate()
                            self.keepers[failed_keeper_id] = None
                            print(f"Terminated keeper {failed_keeper_id} process")
                        except Exception as e:
                            print(f"Error terminating keeper {failed_keeper_id} process: {e}")
                            traceback.print_exc()
                    
                    self.handling_failure.remove(failed_keeper_id)
                    return
                    
            except Exception as e:
                print(f"Error checking replica queue: {e}")
                traceback.print_exc()
                
                # 如果无法检查replica队列，假设它不存在
                replica_exists = False
                
                # 从数据索引中移除失败的keeper
                self.remove_from_data_index(failed_keeper_id)
                self.hash_ring.remove_node(f'keeper_{failed_keeper_id}')
                print(f"Removed keeper {failed_keeper_id} from the hash ring due to error checking replica")
                
                # 终止keeper进程
                if self.keepers[failed_keeper_id]:
                    try:
                        self.keepers[failed_keeper_id].terminate()
                        self.keepers[failed_keeper_id] = None
                        print(f"Terminated keeper {failed_keeper_id} process")
                    except Exception as e:
                        print(f"Error terminating keeper {failed_keeper_id} process: {e}")
                        traceback.print_exc()
                
                self.handling_failure.remove(failed_keeper_id)
                return
            
            # 如果replica存在，找到下一个keeper来接管数据
            
            # Find the next keeper in the hash ring to take over
            keeper_nodes = [f'keeper_{i}' for i in range(self.num_keepers) 
                           if i != failed_keeper_id and 
                           (i not in self.keeper_status or self.keeper_status[i])]
            
            if not keeper_nodes:
                print("No available keepers to handle redistribution!")
                # 即使没有可用的keeper，也要从数据索引中移除失败的keeper
                self.remove_from_data_index(failed_keeper_id)
                self.handling_failure.remove(failed_keeper_id)
                return
                
            # Get the next keeper in the ring (closest clockwise)
            failed_keeper_hash = self.hash_ring._hash(f'keeper_{failed_keeper_id}')
            next_keeper_id = None
            min_distance = float('inf')
            
            for node in keeper_nodes:
                try:
                    node_hash = self.hash_ring._hash(node)
                    # Calculate clockwise distance
                    distance = (node_hash - failed_keeper_hash) % (2**128)
                    if distance < min_distance:
                        min_distance = distance
                        next_keeper_id = int(node.split('_')[1])
                except Exception as e:
                    print(f"Error calculating distance for node {node}: {e}")
                    traceback.print_exc()
                    continue
            
            if next_keeper_id is None:
                print("Could not determine next keeper in the hash ring")
                # 即使找不到下一个keeper，也要从数据索引中移除失败的keeper
                self.remove_from_data_index(failed_keeper_id)
                self.hash_ring.remove_node(f'keeper_{failed_keeper_id}')
                print(f"Removed keeper {failed_keeper_id} from the hash ring")
                self.handling_failure.remove(failed_keeper_id)
                return
                
            print(f"Selected keeper {next_keeper_id} to take over data from failed keeper {failed_keeper_id}")
            
            # 检查选择的keeper是否可用
            if next_keeper_id in self.keeper_status and not self.keeper_status[next_keeper_id]:
                print(f"Selected keeper {next_keeper_id} is also not available, trying another one")
                # 移除这个不可用的keeper，重新尝试
                keeper_nodes.remove(f'keeper_{next_keeper_id}')
                if not keeper_nodes:
                    print("No available keepers left to handle redistribution!")
                    # 即使没有可用的keeper，也要从数据索引中移除失败的keeper
                    self.remove_from_data_index(failed_keeper_id)
                    self.hash_ring.remove_node(f'keeper_{failed_keeper_id}')
                    print(f"Removed keeper {failed_keeper_id} from the hash ring")
                    self.handling_failure.remove(failed_keeper_id)
                    return
                    
                # 重新选择下一个keeper
                next_keeper_id = None
                min_distance = float('inf')
                
                for node in keeper_nodes:
                    try:
                        node_hash = self.hash_ring._hash(node)
                        distance = (node_hash - failed_keeper_hash) % (2**128)
                        if distance < min_distance:
                            min_distance = distance
                            next_keeper_id = int(node.split('_')[1])
                    except Exception as e:
                        print(f"Error calculating distance for node {node}: {e}")
                        traceback.print_exc()
                        continue
                
                if next_keeper_id is None:
                    print("Could not determine next keeper in the hash ring after retry")
                    # 即使找不到下一个keeper，也要从数据索引中移除失败的keeper
                    self.remove_from_data_index(failed_keeper_id)
                    self.hash_ring.remove_node(f'keeper_{failed_keeper_id}')
                    print(f"Removed keeper {failed_keeper_id} from the hash ring")
                    self.handling_failure.remove(failed_keeper_id)
                    return
                    
                print(f"Re-selected keeper {next_keeper_id} to take over data from failed keeper {failed_keeper_id}")
            
            # 检查目标keeper是否健康
            if not self.check_keeper_health(next_keeper_id):
                print(f"Target keeper {next_keeper_id} is not healthy, cannot migrate data")
                # 从数据索引中移除失败的keeper
                self.remove_from_data_index(failed_keeper_id)
                self.hash_ring.remove_node(f'keeper_{failed_keeper_id}')
                print(f"Removed keeper {failed_keeper_id} from the hash ring")
                self.handling_failure.remove(failed_keeper_id)
                return
            else:
                print(f"Target keeper {next_keeper_id} is healthy, proceeding with migration")
            
            # Initiate data migration from the replica of the failed keeper to the next keeper
            print(f"Starting data migration from replica of keeper {failed_keeper_id} to keeper {next_keeper_id}")
            migration_success = self.migrate_data_from_replica(failed_keeper_id, next_keeper_id)
            
            # 只有在迁移成功后才更新数据索引
            if migration_success:
                self.update_data_index(failed_keeper_id, next_keeper_id)
                print(f"Migration successful, updated data index: keeper {failed_keeper_id} -> keeper {next_keeper_id}")
            else:
                # 如果迁移失败，只从数据索引中移除失败的keeper，而不是替换为新的keeper
                print(f"Migration failed, removing keeper {failed_keeper_id} from data index without replacement")
                self.remove_from_data_index(failed_keeper_id)
            
            # 从哈希环中移除失败的keeper
            self.hash_ring.remove_node(f'keeper_{failed_keeper_id}')
            print(f"Removed keeper {failed_keeper_id} from the hash ring")
            
            # 终止keeper进程
            if self.keepers[failed_keeper_id]:
                try:
                    self.keepers[failed_keeper_id].terminate()
                    self.keepers[failed_keeper_id] = None
                    print(f"Terminated keeper {failed_keeper_id} process")
                except Exception as e:
                    print(f"Error terminating keeper {failed_keeper_id} process: {e}")
                    traceback.print_exc()
            
            if migration_success:
                print(f"Successfully handled failure of keeper {failed_keeper_id}")
            else:
                print(f"Failed to migrate data from keeper {failed_keeper_id}, removed from data index")
            
            # 标记处理完成
            self.handling_failure.remove(failed_keeper_id)
        
        except Exception as e:
            print(f"Error handling keeper failure: {e}")
            traceback.print_exc()
            # 确保清理处理标记
            if hasattr(self, 'handling_failure') and failed_keeper_id in self.handling_failure:
                self.handling_failure.remove(failed_keeper_id)

    def remove_from_data_index(self, keeper_id):
        """Remove a keeper from the data index"""
        removed_dates = []
        for date, keepers in list(self.data_index.items()):
            if keeper_id in keepers:
                keepers.remove(keeper_id)
                print(f"Removed keeper {keeper_id} from data index for date {date}")
                
                # 如果没有keeper存储这个日期的数据，考虑从索引中移除这个日期
                if not keepers:
                    removed_dates.append(date)
        
        # 从索引中移除没有keeper的日期
        for date in removed_dates:
            del self.data_index[date]
            print(f"Removed date {date} from data index as no keepers are storing it")
        
        if removed_dates:
            print(f"WARNING: The following dates no longer have any keepers storing their data: {removed_dates}")
            print("This data is now unavailable unless it can be recovered from other sources.")
        
        return removed_dates

    def update_data_index(self, old_keeper_id, new_keeper_id):
        """Update the data index to replace old_keeper_id with new_keeper_id"""
        for date, keepers in self.data_index.items():
            if old_keeper_id in keepers:
                keepers.remove(old_keeper_id)
                keepers.append(new_keeper_id)
                print(f"Updated data index for date {date}: {old_keeper_id} -> {new_keeper_id}")

    def migrate_data_from_replica(self, source_keeper_id, target_keeper_id):
        """Migrate data from a replica to another keeper"""
        print(f"Migrating data from replica of keeper {source_keeper_id} to keeper {target_keeper_id}")
        
        # 检查目标keeper是否健康
        if not self.check_keeper_health(target_keeper_id):
            print(f"Target keeper {target_keeper_id} is not healthy, cannot migrate data")
            return False
        else:
            print(f"Target keeper {target_keeper_id} is healthy, proceeding with migration")
        
        # 首先获取所有日期列表
        date_list = self.get_date_list_from_replica(source_keeper_id)
        if date_list is None:
            print(f"Failed to get date list from replica of keeper {source_keeper_id}")
            return False
        
        if not date_list:
            print(f"No data to migrate from replica of keeper {source_keeper_id} (empty date list)")
            self.terminate_replica(source_keeper_id)
            return True
        
        print(f"Retrieved {len(date_list)} dates from replica of keeper {source_keeper_id}")
        
        # 分批获取和迁移数据
        batch_size = 50  # 每批处理的日期数
        total_dates = len(date_list)
        dates_migrated = 0
        success = True
        
        for i in range(0, total_dates, batch_size):
            batch_dates = date_list[i:min(i+batch_size, total_dates)]
            print(f"Processing batch {i//batch_size + 1}/{(total_dates+batch_size-1)//batch_size}, {len(batch_dates)} dates")
            
            # 获取这批日期的数据
            batch_data = self.get_data_batch_from_replica(source_keeper_id, batch_dates)
            if batch_data is None:
                print(f"Failed to get data for batch {i//batch_size + 1}")
                success = False
                continue
            
            # 发送数据到目标keeper
            for date, date_data in batch_data.items():
                try:
                    if dates_migrated % 10 == 0:
                        print(f"Migrating data for date {date}")
                    
                    # 检查数据结构
                    if isinstance(date_data, dict) and 'datasets' in date_data:
                        if dates_migrated % 10 == 0:
                            print(f"Date {date} has {len(date_data['datasets'])} datasets")
                    else:
                        if dates_migrated % 10 == 0:
                            print(f"Date {date} has old format data structure")
                    
                    # 直接发送完整的数据结构，保持原始格式
                    self.send_to_keeper(target_keeper_id, create_message('STORE', {
                        'date': date,
                        'data': date_data,
                        'source_file': 'migration'
                    }))
                    
                    dates_migrated += 1
                    
                    if dates_migrated % 10 == 0:
                        print(f"Migrated {dates_migrated}/{total_dates} dates to keeper {target_keeper_id}")
                    
                except Exception as e:
                    print(f"Error sending data for date {date}: {e}")
                    traceback.print_exc()
                    success = False
            
            # 每批处理完后等待一小段时间，避免消息队列过载
            if i + batch_size < total_dates:
                time.sleep(0.5)
        
        print(f"Data migration completed: {dates_migrated}/{total_dates} dates migrated to keeper {target_keeper_id}")
        
        # 终止replica
        self.terminate_replica(source_keeper_id)
        
        return success and dates_migrated > 0

    def get_date_list_from_replica(self, keeper_id):
        """从replica获取所有日期列表"""
        print(f"Getting date list from replica of keeper {keeper_id}")
        
        # 添加重试机制
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            if retry_count > 0:
                print(f"Retry attempt {retry_count} of {max_retries} for getting date list from replica of keeper {keeper_id}")
                time.sleep(1)  # 重试前等待1秒
                
            retry_count += 1
            
            try:
                # 创建新的连接和通道
                temp_connection = None
                
                try:
                    temp_connection = pika.BlockingConnection(pika.ConnectionParameters(
                        host='localhost',
                        connection_attempts=3,
                        retry_delay=1,
                        socket_timeout=5
                    ))
                    temp_channel = temp_connection.channel()
                    
                    # 声明replica队列，确保它存在
                    replica_queue = f'replica_{keeper_id}'
                    try:
                        temp_channel.queue_declare(queue=replica_queue, passive=True)
                        print(f"Successfully connected to replica queue: {replica_queue}")
                    except pika.exceptions.ChannelClosedByBroker as e:
                        print(f"Replica queue {replica_queue} does not exist: {e}")
                        if temp_connection:
                            temp_connection.close()
                        return None
                    
                    # 创建临时队列
                    result = temp_channel.queue_declare(queue='', exclusive=True)
                    callback_queue = result.method.queue
                    
                    # 设置响应变量
                    response = None
                    
                    def on_response(ch, method, props, body):
                        nonlocal response
                        if props.correlation_id == corr_id:
                            try:
                                response = parse_message(body)
                                ch.basic_ack(delivery_tag=method.delivery_tag)
                            except Exception as e:
                                print(f"Error parsing response: {e}")
                                traceback.print_exc()
                    
                    # 设置消费者
                    temp_channel.basic_consume(
                        queue=callback_queue,
                        on_message_callback=on_response,
                        auto_ack=False
                    )
                    
                    # 创建相关ID
                    corr_id = str(uuid.uuid4())
                    
                    # 发送请求获取日期列表
                    request_message = create_message('GET_DATE_LIST', {})
                    print(f"Sending request to get date list from replica queue {replica_queue}")
                    
                    temp_channel.basic_publish(
                        exchange='',
                        routing_key=replica_queue,
                        properties=pika.BasicProperties(
                            reply_to=callback_queue,
                            correlation_id=corr_id,
                        ),
                        body=request_message
                    )
                    
                    # 等待响应，设置超时
                    start_time = time.time()
                    timeout = 10  # 10秒超时
                    
                    while response is None and time.time() - start_time < timeout:
                        try:
                            temp_connection.process_data_events(time_limit=0.1)
                        except Exception as e:
                            print(f"Error processing events: {e}")
                            traceback.print_exc()
                            break
                    
                    # 清理资源
                    try:
                        temp_channel.queue_delete(queue=callback_queue)
                    except Exception as e:
                        print(f"Error deleting queue: {e}")
                    
                    try:
                        temp_connection.close()
                    except Exception as e:
                        print(f"Error closing connection: {e}")
                    
                    # 检查是否收到响应
                    if response is None:
                        print(f"Failed to get date list from replica of keeper {keeper_id} (timeout)")
                        continue  # 重试
                    
                    # 处理响应
                    if response.get('type') == 'GET_DATE_LIST_RESULT':
                        result_data = response.get('data', {})
                        if not result_data.get('success', False):
                            print(f"Error in replica response: {result_data.get('error', 'Unknown error')}")
                            continue  # 重试
                        
                        date_list = result_data.get('dates', [])
                        print(f"Received {len(date_list)} dates from replica of keeper {keeper_id}")
                        return date_list
                    else:
                        print(f"Unexpected response type: {response.get('type')}")
                        continue  # 重试
                    
                except Exception as e:
                    print(f"Error getting date list: {e}")
                    traceback.print_exc()
                    if temp_connection:
                        try:
                            temp_connection.close()
                        except:
                            pass
                    continue  # 重试
                    
            except Exception as e:
                print(f"Critical error getting date list: {e}")
                traceback.print_exc()
                continue  # 重试
        
        print(f"Failed to get date list after {max_retries} attempts")
        return None

    def get_data_batch_from_replica(self, keeper_id, date_list):
        """从replica获取指定日期的数据批次"""
        if not date_list:
            return {}
            
        print(f"Getting data for {len(date_list)} dates from replica of keeper {keeper_id}")
        
        # 添加重试机制
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            if retry_count > 0:
                print(f"Retry attempt {retry_count} of {max_retries} for getting data batch from replica of keeper {keeper_id}")
                time.sleep(1)  # 重试前等待1秒
                
            retry_count += 1
            
            try:
                # 创建新的连接和通道
                temp_connection = None
                
                try:
                    temp_connection = pika.BlockingConnection(pika.ConnectionParameters(
                        host='localhost',
                        connection_attempts=3,
                        retry_delay=1,
                        socket_timeout=10  # 增加超时时间
                    ))
                    temp_channel = temp_connection.channel()
                    
                    # 声明replica队列，确保它存在
                    replica_queue = f'replica_{keeper_id}'
                    try:
                        temp_channel.queue_declare(queue=replica_queue, passive=True)
                    except pika.exceptions.ChannelClosedByBroker as e:
                        print(f"Replica queue {replica_queue} does not exist: {e}")
                        if temp_connection:
                            temp_connection.close()
                        return None
                    
                    # 创建临时队列
                    result = temp_channel.queue_declare(queue='', exclusive=True)
                    callback_queue = result.method.queue
                    
                    # 设置响应变量
                    response = None
                    
                    def on_response(ch, method, props, body):
                        nonlocal response
                        if props.correlation_id == corr_id:
                            try:
                                response = parse_message(body)
                                ch.basic_ack(delivery_tag=method.delivery_tag)
                            except Exception as e:
                                print(f"Error parsing response: {e}")
                                traceback.print_exc()
                    
                    # 设置消费者
                    temp_channel.basic_consume(
                        queue=callback_queue,
                        on_message_callback=on_response,
                        auto_ack=False
                    )
                    
                    # 创建相关ID
                    corr_id = str(uuid.uuid4())
                    
                    # 发送请求获取指定日期的数据
                    request_message = create_message('GET_DATA_BATCH', {
                        'dates': date_list
                    })
                    print(f"Sending request to get data batch from replica queue {replica_queue}")
                    
                    temp_channel.basic_publish(
                        exchange='',
                        routing_key=replica_queue,
                        properties=pika.BasicProperties(
                            reply_to=callback_queue,
                            correlation_id=corr_id,
                        ),
                        body=request_message
                    )
                    
                    # 等待响应，设置超时
                    start_time = time.time()
                    timeout = 20  # 20秒超时
                    
                    while response is None and time.time() - start_time < timeout:
                        try:
                            temp_connection.process_data_events(time_limit=0.1)
                            if (time.time() - start_time) % 5 < 0.1:  # 每5秒打印一次
                                print(f"Still waiting for batch data response... {int(time.time() - start_time)}/{timeout}")
                        except Exception as e:
                            print(f"Error processing events: {e}")
                            traceback.print_exc()
                            break
                    
                    # 清理资源
                    try:
                        temp_channel.queue_delete(queue=callback_queue)
                    except Exception as e:
                        print(f"Error deleting queue: {e}")
                    
                    try:
                        temp_connection.close()
                    except Exception as e:
                        print(f"Error closing connection: {e}")
                    
                    # 检查是否收到响应
                    if response is None:
                        print(f"Failed to get data batch from replica of keeper {keeper_id} (timeout)")
                        continue  # 重试
                    
                    # 处理响应
                    if response.get('type') == 'GET_DATA_BATCH_RESULT':
                        result_data = response.get('data', {})
                        if not result_data.get('success', False):
                            print(f"Error in replica response: {result_data.get('error', 'Unknown error')}")
                            continue  # 重试
                        
                        batch_data = result_data.get('data', {})
                        print(f"Received data for {len(batch_data)} dates from replica of keeper {keeper_id}")
                        return batch_data
                    else:
                        print(f"Unexpected response type: {response.get('type')}")
                        continue  # 重试
                    
                except Exception as e:
                    print(f"Error getting data batch: {e}")
                    traceback.print_exc()
                    if temp_connection:
                        try:
                            temp_connection.close()
                        except:
                            pass
                    continue  # 重试
                    
            except Exception as e:
                print(f"Critical error getting data batch: {e}")
                traceback.print_exc()
                continue  # 重试
        
        print(f"Failed to get data batch after {max_retries} attempts")
        return None

    def terminate_replica(self, keeper_id):
        """Terminate the replica of a keeper"""
        print(f"Terminating replica of keeper {keeper_id}")
        
        # 添加重试机制
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            if retry_count > 0:
                print(f"Retry attempt {retry_count} of {max_retries} for terminating replica of keeper {keeper_id}")
                time.sleep(1)  # 重试前等待1秒
                
            retry_count += 1
            
            try:
                # 创建新的连接和通道，避免影响主连接
                temp_connection = None
                
                try:
                    temp_connection = pika.BlockingConnection(pika.ConnectionParameters(
                        host='localhost',
                        connection_attempts=3,  # 连接尝试次数
                        retry_delay=1,          # 重试延迟
                        socket_timeout=5        # 套接字超时
                    ))
                    temp_channel = temp_connection.channel()
                    
                    # 检查replica队列是否存在
                    replica_queue = f'replica_{keeper_id}'
                    try:
                        temp_channel.queue_declare(queue=replica_queue, passive=True)
                        print(f"Replica queue {replica_queue} exists, sending termination message")
                    except pika.exceptions.ChannelClosedByBroker as e:
                        print(f"Replica queue {replica_queue} does not exist: {e}")
                        if temp_connection:
                            temp_connection.close()
                        return True  # 队列不存在，视为成功终止
                    
                    # 发送终止消息到replica
                    temp_channel.basic_publish(
                        exchange='',
                        routing_key=f'replica_{keeper_id}',
                        body=create_message('TERMINATE', {})
                    )
                    print(f"Sent termination message to replica of keeper {keeper_id}")
                    
                    # 关闭临时连接
                    temp_connection.close()
                    
                    # 等待一段时间，确保replica有时间处理终止消息
                    time.sleep(1)
                    
                    # 再次检查队列是否存在，如果不存在，说明replica已经成功终止
                    try:
                        temp_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                        temp_channel = temp_connection.channel()
                        temp_channel.queue_declare(queue=replica_queue, passive=True)
                        print(f"Replica queue {replica_queue} still exists after termination message")
                        temp_connection.close()
                    except pika.exceptions.ChannelClosedByBroker:
                        print(f"Replica queue {replica_queue} no longer exists, termination successful")
                        return True
                    
                    # 如果队列仍然存在，但我们已经发送了终止消息，视为成功
                    return True
                    
                except Exception as e:
                    print(f"Error sending termination message: {e}")
                    traceback.print_exc()
                    if temp_connection:
                        try:
                            temp_connection.close()
                        except:
                            pass
                    
                    # 如果是连接错误，可能是RabbitMQ服务问题，重试
                    if isinstance(e, pika.exceptions.AMQPConnectionError):
                        continue
                    
                    # 其他错误，视为replica可能已经终止
                    return True
                    
            except Exception as e:
                print(f"Critical error terminating replica of keeper {keeper_id}: {str(e)}")
                traceback.print_exc()
                continue  # 重试
        
        print(f"Failed to terminate replica of keeper {keeper_id} after {max_retries} attempts")
        # 即使终止失败，也返回True，因为这不应该阻止整个故障处理过程
        return True

    def log(self, message, force=False):
        """根据verbose设置打印日志"""
        if self.verbose or force:
            print(message)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start the distributed storage system manager')
    parser.add_argument('num_keepers', type=int, nargs='?', default=3, 
                        help='Number of keeper nodes to start (default: 3)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                        help='Enable verbose logging')
    
    args = parser.parse_args()
    
    manager = Manager(args.num_keepers, args.verbose)
    manager.run()