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
        self.datamart = None    # DataMart process for temperature data analysis
        self.date_file_cache = {}  # 缓存日期-文件对应关系，避免频繁查询
        self.replica_date_cache = {}  # 缓存replica的日期列表，避免重复获取

        self.init_rabbitmq()
        
        self.start_keepers()
        self.start_datamart()  # Start the DataMart process
        
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
            elif command == 'TEMP_RANGE':
                self.handle_temp_range(data, properties)
            elif command == 'TEMP_RANGE_AVG':
                self.handle_temp_range_avg(data, properties)
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
            
            # 发送数据到keeper - 全新设计的高性能数据分配
            dates_processed = 0
            total_dates = len(date_records)
            print(f"Starting to send data to keepers, total dates to process: {total_dates}")
            
            # 创建日期-keeper_id映射，用于高效去重
            date_to_keeper = {}
            skipped_dates = set()  # 用于记录跳过的日期
            
            # 第1步：为每个日期分配keeper，并检查本地缓存中是否有相同文件的数据
            for date in date_records.keys():
                # 首先检查本地缓存，是否已加载此文件的此日期数据
                if date in self.date_file_cache and filename in self.date_file_cache[date]:
                    print(f"Date {date} from file {filename} already loaded (local cache), skipping")
                    skipped_dates.add(date)
                    continue
                    
                # 没有在本地缓存找到，使用hash环确定keeper
                keeper_queue = self.hash_ring.get_node(date)
                if keeper_queue:
                    keeper_id = int(keeper_queue.split('_')[1])
                    date_to_keeper[date] = keeper_id
                    
                    # 更新数据索引
                    if date not in self.data_index:
                        self.data_index[date] = []
                    if keeper_id not in self.data_index[date]:
                        self.data_index[date].append(keeper_id)
                else:
                    print(f"Warning: No keeper available for date {date}")
            
            # 报告跳过的日期数
            if skipped_dates:
                skip_count = len(skipped_dates)
                self.send_to_client(create_message('LOAD_RESULT', {
                    'success': True,
                    'filename': full_path,
                    'status': 'existing_data',
                    'existing_dates': skip_count,
                    'message': f"Skipping {skip_count} dates with existing data from the same file"
                }))
                print(f"Skipping {skip_count} dates with existing data from the same file")
            
            # 第2步：批量发送数据到每个keeper (按keeper分组以提高性能)
            keeper_to_dates = {}
            for date, keeper_id in date_to_keeper.items():
                if date in skipped_dates:
                    continue
                if keeper_id not in keeper_to_dates:
                    keeper_to_dates[keeper_id] = []
                keeper_to_dates[keeper_id].append(date)
            
            # 第3步：对每个keeper批量发送数据
            for keeper_id, dates in keeper_to_dates.items():
                # 检查keeper是否健康
                if keeper_id not in self.keeper_status or self.keeper_status[keeper_id]:
                    batch_size = 300  # 增加每批发送的日期数量
                    for i in range(0, len(dates), batch_size):
                        batch_dates = dates[i:i+batch_size]
                        batch_data = {}
                        
                        # 准备该批次的数据
                        for date in batch_dates:
                            batch_data[date] = date_records[date]
                            # 更新本地缓存
                            self.update_date_file_cache(date, filename)
                            dates_processed += 1
                            
                        # 发送批次数据
                        print(f"Sending batch of {len(batch_dates)} dates to keeper {keeper_id}")
                        
                        # 使用新的批量发送功能
                        message = create_message('STORE_BATCH', {
                            'batch_data': batch_data,
                            'source_file': filename
                        })
                        success = self.send_to_keeper_safe(keeper_id, message, is_migration=False)
                        
                        if not success:
                            print(f"Warning: Failed to send batch to keeper {keeper_id}, will retry with smaller batches")
                            # 如果批量发送失败，尝试更小的批次
                            small_batch_size = 50
                            for j in range(0, len(batch_dates), small_batch_size):
                                small_batch = batch_dates[j:j+small_batch_size]
                                small_batch_data = {}
                                for date in small_batch:
                                    small_batch_data[date] = date_records[date]
                                    
                                print(f"Sending smaller batch of {len(small_batch)} dates to keeper {keeper_id}")
                                small_message = create_message('STORE_BATCH', {
                                    'batch_data': small_batch_data,
                                    'source_file': filename
                                })
                                self.send_to_keeper_safe(keeper_id, small_message, is_migration=False)
                            
                        # 每处理一批发送一次进度通知
                        progress_percent = (dates_processed / (total_dates - len(skipped_dates))) * 100
                        print(f"Progress: {progress_percent:.1f}% - Processed {dates_processed}/{total_dates - len(skipped_dates)} dates")
                else:
                    print(f"Keeper {keeper_id} is not healthy, can't send data")
            
            print(f"All data sent to keepers. Processed {dates_processed}/{total_dates} dates")
            
            # Send data to DataMart for temperature analysis
            try:
                print(f"Sending data to DataMart for temperature analysis...")
                
                # Send each date's data to the DataMart
                for date, date_data in date_records.items():
                    if date not in skipped_dates:  # 只发送未跳过的日期数据
                        # Create data object for the DataMart
                        datamart_data = {
                            'date': date,
                            'data': date_data['data'],
                            'column_names': date_data['column_names'],
                            'source_file': filename
                        }
                        
                        # Send the data to the DataMart
                        self.send_to_datamart(create_message('NEW_DATA', datamart_data))
                
                print(f"All data sent to DataMart for temperature analysis")
            except Exception as e:
                print(f"Error sending data to DataMart: {e}")
                traceback.print_exc()
                # Continue with the load operation even if DataMart update fails
            
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
                                    # 更新本地文件缓存
                                    if 'source_file' in dataset and dataset['source_file'] != 'unknown':
                                        self.update_date_file_cache(standard_date, dataset['source_file'])
                                
                                # 添加数据集到结果集合
                                all_datasets.extend(response_content['datasets'])
                            else:
                                data = response_content.get('data', [])
                                column_names = response_content.get('column_names', [])
                                source_file = response_content.get('source_file', 'unknown')
                                
                                # 更新本地文件缓存
                                if source_file != 'unknown':
                                    self.update_date_file_cache(standard_date, source_file)
                                
                                # 安全检查数据结构以避免KeyError
                                if data and isinstance(data, list) and len(data) > 0:
                                    # 检查是否是嵌套列表结构
                                    if isinstance(data[0], list):
                                        # 再次检查是否是二层嵌套
                                        if len(data[0]) > 0 and isinstance(data[0][0], list):
                                            flat_data = []
                                            for group in data:
                                                flat_data.extend(group)
                                            data = flat_data
                                
                                # 创建新的数据集并添加到结果集合
                                new_dataset = {
                                    'data': data,
                                    'column_names': column_names,
                                    'source_file': source_file,
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
                # 对数据集进行去重，避免返回重复数据
                unique_datasets = []
                seen_source_files = set()
                
                for dataset in all_datasets:
                    # 使用source_file + data长度作为去重标识
                    source_file = dataset.get('source_file', 'unknown')
                    data_len = len(dataset.get('data', []))
                    dataset_key = f"{source_file}:{data_len}"
                    
                    if dataset_key not in seen_source_files:
                        seen_source_files.add(dataset_key)
                        unique_datasets.append(dataset)
                
                # 计算总记录数
                total_records = sum(len(dataset.get('data', [])) for dataset in unique_datasets)
                
                # 打印去重信息
                if len(unique_datasets) < len(all_datasets):
                    print(f"Removed {len(all_datasets) - len(unique_datasets)} duplicate datasets")
                
                # 构建完整响应
                complete_response = {
                    'date': date_str,
                    'found': True,
                    'datasets': unique_datasets,  # 使用去重后的数据集
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

    def send_to_keeper_safe(self, keeper_id, message, is_migration=False):
        """发送消息到keeper的安全版本，适用于大批量数据迁移
        当is_migration=True时使用独立连接，避免主连接崩溃"""
        if not is_migration:
            # 使用主连接发送
            try:
                self.send_to_keeper(keeper_id, message)
                return True
            except Exception as e:
                print(f"Error sending message to keeper {keeper_id}: {e}")
                return False
            
        # 使用独立连接发送迁移数据
        temp_connection = None
        try:
            # 创建新的临时连接
            temp_connection = pika.BlockingConnection(pika.ConnectionParameters(
                'localhost',
                heartbeat=60,  # 较长的心跳间隔
                blocked_connection_timeout=120,  # 较长的阻塞超时
                socket_timeout=60  # 较长的socket超时
            ))
            temp_channel = temp_connection.channel()
            
            # 增加预取数量提高吞吐量
            temp_channel.basic_qos(prefetch_count=10)
            
            # 使用临时连接发送消息，为迁移数据设置非持久化模式
            temp_channel.basic_publish(
                exchange='',
                routing_key=f'keeper_{keeper_id}',
                properties=pika.BasicProperties(
                    delivery_mode=1  # 非持久化模式，提高性能
                ),
                body=message
            )
            
            # 快速关闭临时连接
            temp_connection.close()
            return True
        except Exception as e:
            print(f"Error sending migration data to keeper {keeper_id}: {e}")
            if temp_connection and temp_connection.is_open:
                try:
                    temp_connection.close()
                except:
                    pass
            return False
            
    def send_batch_to_keeper_safe(self, keeper_id, date_messages, is_migration=True):
        """批量发送多个日期数据到keeper，提高性能"""
        if not date_messages:
            return True
            
        # 使用独立连接批量发送数据
        temp_connection = None
        try:
            # 创建新的临时连接
            temp_connection = pika.BlockingConnection(pika.ConnectionParameters(
                'localhost',
                heartbeat=60,
                blocked_connection_timeout=120
            ))
            temp_channel = temp_connection.channel()
            
            # 设置发送确认模式提高可靠性
            temp_channel.confirm_delivery()
            
            # 批量发送所有消息
            success_count = 0
            for date, message in date_messages.items():
                try:
                    temp_channel.basic_publish(
                        exchange='',
                        routing_key=f'keeper_{keeper_id}',
                        properties=pika.BasicProperties(
                            delivery_mode=1  # 非持久化模式提高性能
                        ),
                        body=message
                    )
                    success_count += 1
                except Exception as e:
                    print(f"Error sending data for date {date}: {e}")
            
            # 关闭连接
            temp_connection.close()
            
            # 如果全部成功则返回True，否则返回部分成功
            if success_count == len(date_messages):
                return True
            else:
                print(f"Partially successful batch: {success_count}/{len(date_messages)} messages sent")
                return success_count > 0
                
        except Exception as e:
            print(f"Error in batch sending to keeper {keeper_id}: {e}")
            if temp_connection and temp_connection.is_open:
                try:
                    temp_connection.close()
                except:
                    pass
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
        
        running = True
        connection_errors = 0
        max_connection_errors = 5
        
        while running and connection_errors < max_connection_errors:
            try:
                self.channel.start_consuming()
            except KeyboardInterrupt:
                print("Manager is shutting down...")
                running = False
            except (pika.exceptions.ConnectionClosedByBroker, 
                   pika.exceptions.AMQPConnectionError,
                   pika.exceptions.StreamLostError) as e:
                connection_errors += 1
                print(f"Connection error ({connection_errors}/{max_connection_errors}): {str(e)}")
                
                # 尝试恢复连接
                recovery_success = self.handle_connection_error(str(e))
                
                # 如果恢复失败并达到最大错误次数，终止程序
                if not recovery_success and connection_errors >= max_connection_errors:
                    print("Too many connection errors, shutting down...")
                    running = False
                
                # 成功恢复后给一点时间让系统稳定
                if recovery_success:
                    time.sleep(2)
                    connection_errors = 0  # 重置错误计数
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                traceback.print_exc()
                connection_errors += 1
                time.sleep(1)  # 添加短暂延迟避免快速失败循环
        
        # 清理资源
        for keeper in self.keepers:
            if keeper:
                try:
                    keeper.terminate()
                except:
                    pass
                    
        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
            except:
                pass
                
        print("Manager has shutdown")

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
                
                # 标记正在处理
                if not hasattr(self, 'handling_failure'):
                    self.handling_failure = []
                self.handling_failure.append(failed_keeper_id)
                
                # 更新环上的节点移除故障节点
                self.hash_ring.remove_node(f'keeper_{failed_keeper_id}')
                self.keeper_status[failed_keeper_id] = False
                self.keepers[failed_keeper_id] = None
                
                # 尝试确定哪些日期需要迁移
                needs_migration = True
                affected_dates = []
                
                # 1. 从数据索引中查找
                for date, keeper_list in self.data_index.items():
                    if failed_keeper_id in keeper_list:
                        affected_dates.append(date)
                
                if not affected_dates:
                    print(f"No dates found in data index for failed keeper {failed_keeper_id}")
                    
                    # 2. 如果数据索引没有记录，尝试从副本获取数据
                    print(f"Attempting to get date list from replica of keeper {failed_keeper_id}")
                    affected_dates = self.get_date_list_from_replica(failed_keeper_id)
                    
                    if not affected_dates:
                        print(f"No data found in replica of keeper {failed_keeper_id}")
                        needs_migration = False
                
                if needs_migration:
                    print(f"Found {len(affected_dates)} dates that need to be migrated from keeper {failed_keeper_id}")
                    
                    # 决定目标keeper - 优先考虑有空间的节点
                    available_keepers = []
                    for i in range(self.num_keepers):
                        if i != failed_keeper_id and self.keepers[i] is not None and (i not in self.keeper_status or self.keeper_status[i]):
                            available_keepers.append(i)
                    
                    if not available_keepers:
                        print("No available keepers to migrate data to")
                        # 清理标记
                        self.handling_failure.remove(failed_keeper_id)
                        return
                    
                    # 均匀分配日期数据到其他可用的keeper
                    target_keepers = []
                    for date in affected_dates:
                        # 根据当前哈希环重新计算日期应该存储在哪个keeper上
                        key = self.get_key_for_date(date)
                        new_keeper_id = self.get_keeper_id_for_key(key)
                        
                        # 确保目标keeper可用
                        if new_keeper_id not in available_keepers:
                            # 如果计算出的keeper不可用，选择下一个可用的
                            new_keeper_id = available_keepers[0]
                        
                        target_keepers.append(new_keeper_id)
                    
                    # 按照目标keeper分组
                    migration_groups = {}
                    for i, date in enumerate(affected_dates):
                        target_id = target_keepers[i]
                        if target_id not in migration_groups:
                            migration_groups[target_id] = []
                        migration_groups[target_id].append(date)
                    
                    # 处理每个迁移组
                    migration_success = True
                    for target_id, dates in migration_groups.items():
                        print(f"Migrating {len(dates)} dates from keeper {failed_keeper_id} to keeper {target_id}")
                        
                        # 分批迁移数据，增加批次大小提高性能
                        batch_size = 300  # 增加批处理大小提高性能
                        for i in range(0, len(dates), batch_size):
                            batch = dates[i:i+batch_size]
                            print(f"Processing migration batch {i//batch_size + 1}/{(len(dates)+batch_size-1)//batch_size}")
                            
                            # 获取这批日期的数据
                            batch_data = self.get_data_batch_from_replica(failed_keeper_id, batch)
                            if not batch_data:
                                print(f"Failed to get data for batch {i//batch_size + 1}")
                                migration_success = False
                                continue
                            
                            # 使用批量发送提高性能
                            print(f"Preparing to migrate {len(batch_data)} dates in one batch")
                            
                            try:
                                # 准备批量消息
                                message_batch = {}
                                for date, date_data in batch_data.items():
                                    # 为每个日期创建消息
                                    message = create_message('STORE', {
                                        'date': date,
                                        'data': date_data,
                                        'source_file': 'migration'
                                    })
                                    message_batch[date] = message
                                
                                # 使用批量发送功能
                                batch_success = self.send_batch_to_keeper_safe(target_id, message_batch)
                                
                                if batch_success:
                                    # 更新数据索引
                                    for date in batch_data.keys():
                                        if date in self.data_index:
                                            if failed_keeper_id in self.data_index[date]:
                                                self.data_index[date].remove(failed_keeper_id)
                                            if target_id not in self.data_index[date]:
                                                self.data_index[date].append(target_id)
                                    
                                    print(f"Successfully migrated batch of {len(batch_data)} dates to keeper {target_id}")
                                else:
                                    print(f"Batch migration to keeper {target_id} failed or partially succeeded")
                                    migration_success = False
                                    
                                    # 如果批量发送失败，尝试单个发送
                                    print("Falling back to individual message sending...")
                                    
                                    # 单个发送模式
                                    migrated_count = 0
                                    total_items = len(batch_data)
                                    start_time = time.time()
                                    
                                    for date, date_data in batch_data.items():
                                        try:
                                            # 标记为迁移数据
                                            success = self.send_to_keeper_safe(target_id, create_message('STORE', {
                                                'date': date,
                                                'data': date_data,
                                                'source_file': 'migration'
                                            }), is_migration=True)
                                            
                                            if success:
                                                # 更新数据索引
                                                if date in self.data_index:
                                                    if failed_keeper_id in self.data_index[date]:
                                                        self.data_index[date].remove(failed_keeper_id)
                                                    if target_id not in self.data_index[date]:
                                                        self.data_index[date].append(target_id)
                                                
                                                migrated_count += 1
                                                # 只在关键进度点打印，减少日志输出量
                                                if migrated_count % 20 == 0 or migrated_count == total_items:
                                                    elapsed = time.time() - start_time
                                                    rate = migrated_count / elapsed if elapsed > 0 else 0
                                                    print(f"Migrated {migrated_count}/{total_items} dates in batch ({rate:.1f} dates/sec)")
                                            else:
                                                print(f"Failed to send data for date {date}")
                                                migration_success = False
                                            
                                            # 每10个日期添加一次微小延迟，避免连接过载但不显著影响性能
                                            if migrated_count % 10 == 0:
                                                time.sleep(0.01)  # 减少延迟时间
                                                
                                        except Exception as e:
                                            print(f"Error sending data for date {date}: {e}")
                                            traceback.print_exc()
                                            migration_success = False
                                
                            except Exception as e:
                                print(f"Error in batch migration: {e}")
                                traceback.print_exc()
                                migration_success = False
                            
                            # 批次之间几乎不添加延迟，最大化性能
                            time.sleep(0.001)
                    
                    # 数据迁移完成后，终止副本
                    if migration_success:
                        print(f"Successfully migrated data from keeper {failed_keeper_id}")
                        self.terminate_replica(failed_keeper_id)
                    else:
                        print(f"Some errors occurred during migration from keeper {failed_keeper_id}")
                
                # 从处理列表中移除
                self.handling_failure.remove(failed_keeper_id)
                print(f"Keeper {failed_keeper_id} failure handling completed")
            
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
                # 确保不重复添加keeper_id
                if new_keeper_id not in keepers:
                    keepers.append(new_keeper_id)
                print(f"Updated data index for date {date}: {old_keeper_id} -> {new_keeper_id}")

    def migrate_data_from_replica(self, source_keeper_id, target_keeper_id):
        """从replica迁移数据到新的keeper"""
        try:
            print(f"Starting data migration from keeper {source_keeper_id} to keeper {target_keeper_id}")
            
            # 获取源keeper的日期列表
            date_list = self.get_date_list_from_replica(source_keeper_id)
            if not date_list:
                print(f"No dates found in keeper {source_keeper_id} replica")
                return
                
            print(f"Found {len(date_list)} dates in keeper {source_keeper_id} replica")
            
            # 设置较小的批处理大小，避免消息过大
            batch_size = 50  # 减小批处理大小
            
            # 计算批次数量
            num_batches = (len(date_list) + batch_size - 1) // batch_size
            
            # 处理每个批次
            migrated_count = 0
            
            for i in range(num_batches):
                # 获取当前批次的日期
                start_idx = i * batch_size
                end_idx = min((i+1) * batch_size, len(date_list))
                batch_dates = date_list[start_idx:end_idx]
                
                print(f"Processing batch {i+1}/{num_batches}, {len(batch_dates)} dates")
                
                # 从源replica获取数据
                batch_data = self.get_data_batch_from_replica(source_keeper_id, batch_dates)
                if not batch_data:
                    print(f"Failed to get data for batch {i+1}, continuing with next batch")
                    continue
                
                # 迁移每个日期的数据
                for date, date_data in batch_data.items():
                    try:
                        # 将数据发送到目标keeper
                        print(f"Migrating data for date {date}")
                        
                        # 打印数据集数量
                        if isinstance(date_data, dict) and 'datasets' in date_data:
                            print(f"Date {date} has {len(date_data['datasets'])} datasets")
                        
                        # 以"migration"作为来源标识，告诉keeper这是迁移数据
                        success = self.send_to_keeper_safe(target_keeper_id, create_message('STORE', {
                            'date': date,
                            'data': date_data,  # 发送完整的数据结构
                            'source_file': 'migration'  # 标记为迁移数据
                        }), is_migration=True)
                        
                        # 如果成功发送数据，则增加计数
                        if success:
                            migrated_count += 1
                            if migrated_count % 10 == 0:
                                print(f"Migrated {migrated_count}/{len(date_list)} dates to keeper {target_keeper_id}")
                        else:
                            print(f"Failed to migrate data for date {date}")
                            
                        # 每迁移2个日期添加一个短暂延迟，避免消息堆积
                        if migrated_count % 2 == 0:
                            time.sleep(0.1)  # 增加延迟时间
                        
                    except Exception as e:
                        print(f"Error migrating data for date {date}: {e}")
                        traceback.print_exc()
                
                # 批次之间添加延迟，避免消息堆积过多
                print(f"Completed batch {i+1}/{num_batches}, {migrated_count} dates migrated so far")
                time.sleep(0.5)  # 批次之间添加较长延迟
            
            print(f"Migration complete. Migrated {migrated_count}/{len(date_list)} dates from keeper {source_keeper_id} to keeper {target_keeper_id}")
            
        except Exception as e:
            print(f"Error in migrate_data_from_replica: {e}")
            traceback.print_exc()

    def get_date_list_from_replica(self, keeper_id):
        """从replica获取日期列表"""
        try:
            print(f"Getting date list from replica of keeper {keeper_id}")
            
            # 如果之前已经获取过日期列表，直接使用本地缓存
            if hasattr(self, 'replica_date_cache') and keeper_id in self.replica_date_cache:
                print(f"Using cached date list for keeper {keeper_id} with {len(self.replica_date_cache[keeper_id])} dates")
                return self.replica_date_cache[keeper_id]
            
            # 创建临时连接
            temp_connection = pika.BlockingConnection(pika.ConnectionParameters(
                'localhost',
                heartbeat=10,  # 较短的心跳间隔
                blocked_connection_timeout=30  # 较短的阻塞超时
            ))
            temp_channel = temp_connection.channel()
            
            # 创建临时队列接收响应
            result = temp_channel.queue_declare(queue='', exclusive=True)
            temp_queue = result.method.queue
            
            # 生成一个唯一的correlation_id
            correlation_id = str(uuid.uuid4())
            
            # 创建请求消息
            request_message = create_message('GET_DATE_LIST', {})
            
            # 设置接收响应标志
            response_received = False
            date_list = None
            
            # 定义回调函数
            def on_response(ch, method, props, body):
                nonlocal response_received, date_list
                if props.correlation_id == correlation_id:
                    try:
                        message = parse_message(body)
                        if message.get('type') == 'GET_DATE_LIST_RESULT':
                            result_data = message.get('data', {})
                            if result_data.get('success'):
                                date_list = result_data.get('dates', [])
                                print(f"Received date list with {len(date_list)} dates from replica of keeper {keeper_id}")
                            else:
                                print(f"Error in replica response: {result_data.get('error', 'Unknown error')}")
                        else:
                            print(f"Unexpected response type: {message.get('type')}")
                    except Exception as e:
                        print(f"Error parsing response: {e}")
                        traceback.print_exc()
                    
                    response_received = True
            
            # 设置回调
            temp_channel.basic_consume(
                queue=temp_queue,
                on_message_callback=on_response,
                auto_ack=True
            )
            
            # 发送请求
            print(f"Sending date list request to replica queue replica_{keeper_id}")
            temp_channel.basic_publish(
                exchange='',
                routing_key=f'replica_{keeper_id}',
                properties=pika.BasicProperties(
                    reply_to=temp_queue,
                    correlation_id=correlation_id
                ),
                body=request_message
            )
            
            # 等待响应或超时 (使用更短的超时时间)
            timeout = 15  # 缩短超时时间至15秒
            start_time = time.time()
            
            while not response_received and time.time() - start_time < timeout:
                try:
                    # 每次只处理短时间的事件，避免长时间阻塞
                    temp_connection.process_data_events(time_limit=0.5)
                    
                    # 每3秒输出等待状态
                    if int(time.time() - start_time) % 3 == 0 and int(time.time() - start_time) > 0:
                        print(f"Still waiting for date list response... {int(time.time() - start_time)}/{timeout}s")
                except Exception as e:
                    print(f"Error processing events: {e}")
                    # 如果是连接错误，中断等待
                    if isinstance(e, (pika.exceptions.ConnectionClosed, 
                                     pika.exceptions.AMQPConnectionError,
                                     pika.exceptions.ConnectionClosedByBroker)):
                        print("Connection error, terminating wait loop")
                        break
            
            # 关闭临时连接
            try:
                temp_connection.close()
            except Exception as e:
                print(f"Error closing temporary connection: {e}")
            
            if not response_received:
                print(f"Timeout getting date list from replica of keeper {keeper_id}")
                return []
            
            # 缓存获取到的日期列表
            if date_list:
                if not hasattr(self, 'replica_date_cache'):
                    self.replica_date_cache = {}
                self.replica_date_cache[keeper_id] = date_list
            
            return date_list
            
        except Exception as e:
            print(f"Error getting date list from replica: {e}")
            traceback.print_exc()
            return []

    def get_data_batch_from_replica(self, keeper_id, date_list):
        """从replica获取批量日期的数据"""
        try:
            # 为避免消息过大，分批处理，但增加每批量大小
            MAX_DATES_PER_BATCH = 100  # 增加每批次的日期数量提高性能
            
            if len(date_list) > MAX_DATES_PER_BATCH:
                # 如果日期太多，分批获取
                all_data = {}
                batch_count = (len(date_list) + MAX_DATES_PER_BATCH - 1) // MAX_DATES_PER_BATCH
                
                print(f"Splitting {len(date_list)} dates into {batch_count} batches of {MAX_DATES_PER_BATCH}")
                
                for i in range(0, len(date_list), MAX_DATES_PER_BATCH):
                    batch = date_list[i:i+MAX_DATES_PER_BATCH]
                    self.log(f"Getting batch {i//MAX_DATES_PER_BATCH + 1}/{batch_count} with {len(batch)} dates")
                    batch_data = self.get_data_batch_from_replica(keeper_id, batch)
                    if batch_data:
                        all_data.update(batch_data)
                    # 减少批次间延迟，提高性能
                    time.sleep(0.05)
                return all_data
            
            self.log(f"Getting data for {len(date_list)} dates from replica of keeper {keeper_id}")
            
            # 创建临时队列接收响应
            temp_connection = None
            try:
                temp_connection = pika.BlockingConnection(pika.ConnectionParameters(
                    'localhost',
                    heartbeat=60,  # 增加心跳间隔以支持更大的消息
                    blocked_connection_timeout=120,  # 增加超时时间
                    socket_timeout=60  # 添加socket超时
                ))
                temp_channel = temp_connection.channel()
                
                # 增加预取数量，提高吞吐量
                temp_channel.basic_qos(prefetch_count=10)
                
                result = temp_channel.queue_declare(queue='', exclusive=True)
                temp_queue = result.method.queue
                
                # 生成一个唯一的correlation_id
                correlation_id = str(uuid.uuid4())
                
                # 发送获取数据批量请求
                request = {
                    'dates': date_list
                }
                
                # 发送请求
                self.log(f"Sending request to get data batch from replica queue replica_{keeper_id}")
                temp_channel.basic_publish(
                    exchange='',
                    routing_key=f'replica_{keeper_id}',
                    properties=pika.BasicProperties(
                        reply_to=temp_queue,
                        correlation_id=correlation_id,
                        delivery_mode=1  # 使用非持久化模式提高性能
                    ),
                    body=create_message('GET_DATA_BATCH', request)
                )
                
                # 设置接收超时时间
                timeout = 60  # 增加超时时间以支持更大批量
                
                # 等待响应
                response_received = False
                batch_data = None
                
                def on_response(ch, method, props, body):
                    nonlocal response_received, batch_data
                    
                    if props.correlation_id == correlation_id:
                        try:
                            message = parse_message(body)
                            if message.get('type') == 'GET_DATA_BATCH_RESULT':
                                response_data = message.get('data', {})
                                if response_data.get('success'):
                                    batch_data = response_data.get('data', {})
                                    item_count = len(batch_data) if batch_data else 0
                                    self.log(f"Received data for {item_count} dates from replica")
                                else:
                                    self.log(f"Failed to get data batch: {response_data.get('error')}")
                            else:
                                self.log(f"Received unexpected message type: {message.get('type')}")
                            
                            response_received = True
                        except Exception as e:
                            self.log(f"Error parsing response: {e}")
                            traceback.print_exc()
                            response_received = True  # 防止无限等待
                
                # 设置回调
                temp_channel.basic_consume(
                    queue=temp_queue,
                    on_message_callback=on_response,
                    auto_ack=True
                )
                
                # 等待响应或超时，减少日志输出以提高性能
                start_time = time.time()
                
                while not response_received and time.time() - start_time < timeout:
                    # 使用更安全的方式处理消息
                    try:
                        temp_connection.process_data_events(time_limit=1.0)  # 增加单次处理时间以提高效率
                    except Exception as e:
                        self.log(f"Error processing events: {e}")
                        # 如果是严重错误就中断等待
                        if "ConnectionClosedByBroker" in str(e):
                            self.log("Connection closed by broker, terminating wait loop")
                            break
                    
                    # 每10秒输出一次等待状态，减少日志输出
                    elapsed = int(time.time() - start_time)
                    if elapsed % 10 == 0 and elapsed > 0:
                        self.log(f"Still waiting for batch data response... {elapsed}/{timeout}s")
                
                if not response_received:
                    self.log(f"Timeout waiting for data batch")
                
                return batch_data
                
            finally:
                # 确保关闭临时连接
                if temp_connection and temp_connection.is_open:
                    try:
                        temp_connection.close()
                    except Exception as e:
                        self.log(f"Error closing temporary connection: {e}")
                        
        except Exception as e:
            self.log(f"Error getting data batch from replica: {e}")
            traceback.print_exc()
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

    def handle_temp_range(self, data, properties):
        """Handle temperature range query from client"""
        try:
            if not isinstance(data, dict) or 'start_date' not in data or 'end_date' not in data:
                self.send_to_client(create_message('TEMP_RANGE_RESULT', {
                    'success': False,
                    'error': 'Invalid request format. Expected: {"start_date": "DD-MM-YYYY", "end_date": "DD-MM-YYYY"}'
                }))
                return
                
            # 使用标准化日期
            start_date = data.get('start_date')
            end_date = data.get('end_date')
            
            try:
                # 验证日期格式
                std_start = self.standardize_date(start_date)
                std_end = self.standardize_date(end_date)
                
                if not std_start or not std_end:
                    self.send_to_client(create_message('TEMP_RANGE_RESULT', {
                        'success': False,
                        'error': f'Invalid date format: {start_date} or {end_date}'
                    }))
                    return
            except Exception as e:
                self.send_to_client(create_message('TEMP_RANGE_RESULT', {
                    'success': False,
                    'error': f'Date parsing error: {str(e)}'
                }))
                return
                
            # Forward the request to DataMart - 直接发送，不需要回复属性
            self.log(f"Forwarding temperature range request to DataMart: {start_date} to {end_date}")
            self.send_to_datamart(create_message('TEMP_RANGE', data))
            
        except Exception as e:
            self.log(f"Error handling temperature range request: {str(e)}")
            self.send_to_client(create_message('TEMP_RANGE_RESULT', {
                'success': False,
                'error': f"Failed to process temperature range request: {str(e)}"
            }))
            
    def handle_temp_range_avg(self, data, properties):
        """Handle average temperature for a date range query from client"""
        try:
            if not isinstance(data, dict) or 'start_date' not in data or 'end_date' not in data:
                self.send_to_client(create_message('TEMP_RANGE_AVG_RESULT', {
                    'success': False,
                    'error': 'Invalid request format. Expected: {"start_date": "DD-MM-YYYY", "end_date": "DD-MM-YYYY"}'
                }))
                return
                
            # 使用标准化日期
            start_date = data.get('start_date')
            end_date = data.get('end_date')
            
            try:
                # 验证日期格式
                std_start = self.standardize_date(start_date)
                std_end = self.standardize_date(end_date)
                
                if not std_start or not std_end:
                    self.send_to_client(create_message('TEMP_RANGE_AVG_RESULT', {
                        'success': False,
                        'error': f'Invalid date format: {start_date} or {end_date}'
                    }))
                    return
            except Exception as e:
                self.send_to_client(create_message('TEMP_RANGE_AVG_RESULT', {
                    'success': False,
                    'error': f'Date parsing error: {str(e)}'
                }))
                return
                
            # Forward the request to DataMart - 直接发送，不需要回复属性
            self.log(f"Forwarding temperature range average request to DataMart: {start_date} to {end_date}")
            self.send_to_datamart(create_message('TEMP_RANGE_AVG', data))
            
        except Exception as e:
            self.log(f"Error handling temperature range average request: {str(e)}")
            self.send_to_client(create_message('TEMP_RANGE_AVG_RESULT', {
                'success': False,
                'error': f"Failed to process temperature range average request: {str(e)}"
            }))
            
    def send_to_datamart(self, message, properties=None):
        """Send message to the DataMart"""
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=DATAMART_QUEUE,
                body=message
            )
            self.log(f"Sent message to DataMart")
        except Exception as e:
            self.log(f"Error sending message to DataMart: {str(e)}")
            raise

    def start_datamart(self):
        """Start the DataMart process for temperature data analysis"""
        python_executable = sys.executable
        print(f"Starting DataMart process using Python interpreter: {python_executable}")
        
        # Start the datamart process with the same verbosity setting as the manager
        datamart_process = subprocess.Popen(
            [python_executable, 'datamart.py', 
             '--verbose' if self.verbose else '--quiet']
        )
        self.datamart = datamart_process
        print(f"DataMart process started")
        
        # Give the DataMart time to initialize
        time.sleep(2)

    def get_source_files_for_date(self, date, keeper_id):
        """查询指定日期在指定keeper上的源文件列表"""
        try:
            # print(f"Checking source files for date {date} in keeper {keeper_id}")
            
            temp_connection = None
            try:
                temp_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                temp_channel = temp_connection.channel()
                
                result = temp_channel.queue_declare(queue='', exclusive=True)
                temp_queue = result.method.queue
                
                response_received = False
                source_files = []
                
                def on_response(ch, method, props, body):
                    nonlocal response_received, source_files
                    response_received = True
                    try:
                        response = parse_message(body)
                        response_type = response.get('type')
                        response_data = response.get('data', {})
                        
                        if response_type == 'SOURCE_FILES_RESULT' and response_data.get('success', False):
                            source_files = response_data.get('source_files', [])
                    except Exception as e:
                        print(f"Error parsing source files response: {e}")
                    
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
                    'date': date
                }
                
                temp_channel.basic_publish(
                    exchange='',
                    routing_key=f'keeper_{keeper_id}',
                    properties=pika.BasicProperties(
                        reply_to=temp_queue,
                        correlation_id=str(uuid.uuid4())
                    ),
                    body=create_message('GET_SOURCE_FILES', request_data)
                )
                
                # 设置超时时间 - 减少超时时间以加快处理速度
                timeout_seconds = 1
                timeout_event = threading.Event()
                
                def timeout_thread():
                    start_time = time.time()
                    while time.time() - start_time < timeout_seconds:
                        if timeout_event.is_set() or response_received:
                            return
                        time.sleep(0.1)
                    
                    if not response_received:
                        try:
                            temp_channel.stop_consuming()
                        except:
                            pass
                
                timer_thread = threading.Thread(target=timeout_thread)
                timer_thread.daemon = True
                timer_thread.start()
                
                try:
                    temp_channel.start_consuming()
                except Exception as e:
                    print(f"Error during consumption for source files: {e}")
                
                timeout_event.set()
                
                if temp_connection:
                    temp_connection.close()
                
                return source_files
            
            except Exception as e:
                print(f"Error getting source files: {e}")
                if temp_connection:
                    try:
                        temp_connection.close()
                    except:
                        pass
                return []
        
        except Exception as e:
            print(f"Error in get_source_files_for_date: {e}")
            return []

    def update_date_file_cache(self, date, filename):
        """更新日期-文件缓存，记录哪些文件包含特定日期的数据"""
        if date not in self.date_file_cache:
            self.date_file_cache[date] = []
        if filename not in self.date_file_cache[date]:
            self.date_file_cache[date].append(filename)
            
    def get_key_for_date(self, date):
        """根据日期生成用于一致性哈希的键"""
        # 直接使用日期作为键
        return date
        
    def get_keeper_id_for_key(self, key):
        """根据键计算应该存储在哪个keeper上"""
        # 使用一致性哈希环获取节点
        node = self.hash_ring.get_node(key)
        if node:
            # 从"keeper_X"中提取keeper ID
            return int(node.split('_')[1])
        else:
            # 如果没有可用节点，返回默认值
            available_keepers = [i for i in range(self.num_keepers) 
                               if self.keepers[i] is not None and 
                               (i not in self.keeper_status or self.keeper_status[i])]
            if available_keepers:
                return available_keepers[0]
            return None

    def handle_connection_error(self, error_msg=""):
        """处理RabbitMQ连接错误，尝试重新连接"""
        try:
            print(f"RabbitMQ连接错误{': ' + error_msg if error_msg else ''}，尝试重新连接...")
            
            # 关闭旧连接
            if self.connection and self.connection.is_open:
                try:
                    self.connection.close()
                except:
                    pass
            
            # 重新建立连接
            self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            self.channel = self.connection.channel()
            
            # 重新声明必要的队列
            self.channel.queue_declare(queue=MANAGER_QUEUE)
            self.channel.queue_declare(queue=CLIENT_QUEUE)
            
            for i in range(self.num_keepers):
                if i not in self.keeper_status or self.keeper_status[i]:
                    self.channel.queue_declare(queue=f'keeper_{i}')
            
            # 重新设置消费者
            self.channel.basic_consume(
                queue=MANAGER_QUEUE,
                on_message_callback=self.process_client_message,
                auto_ack=True
            )
            
            print("RabbitMQ连接已恢复")
            return True
        except Exception as e:
            print(f"重新连接失败: {e}")
            return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start the distributed storage system manager')
    parser.add_argument('num_keepers', type=int, nargs='?', default=3, 
                        help='Number of keeper nodes to start (default: 3)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                        help='Enable verbose logging')
    
    args = parser.parse_args()
    
    manager = Manager(args.num_keepers, args.verbose)
    manager.run()