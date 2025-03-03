#!/usr/bin/env python
import pika
import sys
import json
import time
import subprocess
import traceback
from utils import *
import os
import csv
import socket

class Keeper:
    def __init__(self, keeper_id, num_keepers):
        self.keeper_id = keeper_id
        self.num_keepers = num_keepers
        self.connection = None
        self.channel = None
        self.data = {}
        self.replica = None
        self.init_rabbitmq()
        self.start_replica()
        print(f"Storage device {keeper_id} has started")

    def init_rabbitmq(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        queue_name = f'keeper_{self.keeper_id}'
        self.channel.queue_declare(queue=queue_name)
        print(f"Storage device {self.keeper_id} declared queue: {queue_name}")
        self.channel.queue_declare(queue=f'replica_{self.keeper_id}')
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.process_message,
            auto_ack=True
        )
        print(f"Storage device {self.keeper_id} started listening to queue: {queue_name}")

    def start_replica(self):
        try:
            self.replica = subprocess.Popen(
                [sys.executable, 'replica.py', str(self.keeper_id)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            print(f"Storage device {self.keeper_id} started replica")
        except Exception as e:
            print(f"Storage device {self.keeper_id} failed to start replica: {str(e)}")
            traceback.print_exc()

    def process_message(self, ch, method, properties, body):
        try:
            message = parse_message(body)
            msg_type = message.get('type')
            data = message.get('data')
            print(f"Storage device {self.keeper_id} received message: {msg_type}, data: {data}")
            if msg_type == 'STORE':
                self.handle_store(data)
                self.send_to_replica(body)
            elif msg_type == 'GET':
                if isinstance(data, dict) and 'response_queue' in data:
                    self.handle_get_with_queue(data)
                else:
                    self.handle_get(data, properties)
            elif msg_type == 'GET_DIRECT':
                self.handle_get_direct(data)
            else:
                print(f"Storage device {self.keeper_id} received unknown type message: {msg_type}")
        except Exception as e:
            print(f"Error processing message: {str(e)}")
            traceback.print_exc()

    def handle_store(self, data):
        date = data.get('date')
        records = data.get('data', [])
        column_names = data.get('column_names', [])
        source_file = data.get('source_file', 'unknown')  # 添加文件来源信息
        
        print(f"Storing data for date: {date}")
        print(f"DEBUG - Received column names: {column_names}")
        print(f"DEBUG - Source file: {source_file}")
        
        # Check if data for this date already exists
        if date in self.data:
            # 获取现有数据
            date_data = self.data[date]
            
            # 如果是旧格式（列表），转换为新格式
            if isinstance(date_data, list):
                date_data = {
                    'datasets': [
                        {
                            'data': date_data,
                            'column_names': [],
                            'source_file': 'unknown'
                        }
                    ]
                }
            
            # 如果是旧的字典格式（没有datasets字段），转换为新格式
            elif isinstance(date_data, dict) and 'datasets' not in date_data:
                date_data = {
                    'datasets': [
                        {
                            'data': date_data.get('data', []),
                            'column_names': date_data.get('column_names', []),
                            'source_file': 'unknown'
                        }
                    ]
                }
            
            # 检查是否已存在相同来源的数据集
            found_existing_dataset = False
            for dataset in date_data['datasets']:
                if dataset.get('source_file') == source_file:
                    # 已存在相同来源的数据集，更新或添加记录
                    existing_records = dataset['data']
                    
                    # 添加不存在的记录
                    for record in records:
                        if record not in existing_records:
                            existing_records.append(record)
                    
                    dataset['data'] = existing_records
                    found_existing_dataset = True
                    print(f"Updated existing dataset from {source_file} for date {date}. Total records: {len(existing_records)}")
                    break
            
            # 如果没有找到相同来源的数据集，添加新的数据集
            if not found_existing_dataset:
                date_data['datasets'].append({
                    'data': records,
                    'column_names': column_names,
                    'source_file': source_file
                })
                print(f"Added new dataset from {source_file} for date {date} with {len(records)} records")
            
            self.data[date] = date_data
            
        else:
            # Create new entry if date doesn't exist
            self.data[date] = {
                'datasets': [
                    {
                        'data': records,
                        'column_names': column_names,
                        'source_file': source_file
                    }
                ]
            }
            print(f"Created new entry for date {date} with {len(records)} records from {source_file}")
        
        # 打印调试信息
        print(f"DEBUG - Final data structure for date {date}: {self.data[date]}")
            
        self.send_to_replica(create_message('REPLICATE', {
            'date': date,
            'data': self.data[date],
            'source_file': source_file
        }))

    def handle_get(self, date_data, properties):
        try:
            if isinstance(date_data, dict):
                date = date_data.get('date', '')
                original_date = date_data.get('original_date', date)
            else:
                date = date_data
                original_date = date
            print(f"Query date: {date}")
            if date in self.data:
                date_info = self.data[date]
                
                # 检查数据结构
                if isinstance(date_info, list):
                    # 旧格式（列表），转换为新格式
                    datasets = [{
                        'data': date_info,
                        'column_names': [],
                        'source_file': 'unknown'
                    }]
                elif isinstance(date_info, dict) and 'datasets' in date_info:
                    # 新格式（包含datasets字段）
                    datasets = date_info['datasets']
                elif isinstance(date_info, dict) and 'data' in date_info:
                    # 中间格式（包含data字段但没有datasets字段）
                    datasets = [{
                        'data': date_info.get('data', []),
                        'column_names': date_info.get('column_names', []),
                        'source_file': 'unknown'
                    }]
                else:
                    # 未知格式
                    datasets = []
                
                total_records = sum(len(dataset.get('data', [])) for dataset in datasets)
                print(f"Found {total_records} records in {len(datasets)} datasets")
                
                # 构建响应，包含所有数据集
                response = {
                    'date': original_date,
                    'found': True,
                    'datasets': datasets,
                    'count': total_records
                }
            else:
                print(f"Date {date} data not found, trying to get from replica...")
                replica_data = self.get_data_from_replica(date)
                if replica_data:
                    return replica_data
                else:
                    return {
                        'date': original_date,
                        'found': False,
                        'datasets': [],
                        'count': 0
                    }
            reply_to = properties.reply_to if properties else None
            correlation_id = properties.correlation_id if properties else None
            if not reply_to:
                print(f"Storage device {self.keeper_id} did not receive reply_to attribute, cannot reply")
                return
            print(f"Storage device {self.keeper_id} will reply to queue: {reply_to}, correlation_id: {correlation_id}")
            print(f"Storage device {self.keeper_id} current data: {list(self.data.keys())[:5]}...")
            print(f"Storage device {self.keeper_id} sending response to queue: {reply_to}")
            response_message = create_message('GET_RESULT', response)
            print(f"Storage device {self.keeper_id} sending response content: {response}")
            self.channel.basic_publish(
                exchange='',
                routing_key=reply_to,
                properties=pika.BasicProperties(
                    correlation_id=correlation_id
                ),
                body=response_message
            )
            print(f"Storage device {self.keeper_id} sent response to queue {reply_to}")
        except Exception as e:
            print(f"Error processing GET request: {e}")
            traceback.print_exc()
            response = {
                'date': date_data.get('date', ''),
                'found': False,
                'datasets': [],
                'count': 0,
                'error': str(e)
            }
            self.channel.basic_publish(
                exchange='',
                routing_key=reply_to,
                body=create_message('GET_RESULT', response)
            )
            print(f"Storage device {self.keeper_id} sent response to queue {reply_to}")

    def handle_get_direct(self, data):
        try:
            date = data.get('date')
            original_date = data.get('original_date', date)
            print(f"Query date: {date}")
            if date in self.data:
                date_info = self.data[date]
                
                # 检查数据结构
                if isinstance(date_info, list):
                    # 旧格式（列表），转换为新格式
                    datasets = [{
                        'data': date_info,
                        'column_names': [],
                        'source_file': 'unknown'
                    }]
                elif isinstance(date_info, dict) and 'datasets' in date_info:
                    # 新格式（包含datasets字段）
                    datasets = date_info['datasets']
                elif isinstance(date_info, dict) and 'data' in date_info:
                    # 中间格式（包含data字段但没有datasets字段）
                    datasets = [{
                        'data': date_info.get('data', []),
                        'column_names': date_info.get('column_names', []),
                        'source_file': 'unknown'
                    }]
                else:
                    # 未知格式
                    datasets = []
                
                total_records = sum(len(dataset.get('data', [])) for dataset in datasets)
                print(f"Found {total_records} records in {len(datasets)} datasets")
                
                # 构建响应，包含所有数据集
                response = {
                    'date': original_date,
                    'found': True,
                    'datasets': datasets,
                    'count': total_records
                }
            else:
                print(f"Date {date} data not found")
                response = {
                    'date': original_date,
                    'found': False,
                    'datasets': [],
                    'count': 0
                }
            print(f"Storage device {self.keeper_id} directly sending response to client")
            response_message = create_message('GET_RESULT', response)
            print(f"Storage device {self.keeper_id} sending response content: {response}")
            self.channel.basic_publish(
                exchange='',
                routing_key=CLIENT_QUEUE,
                body=response_message
            )
            print(f"Storage device {self.keeper_id} sent directly response to client")
        except Exception as e:
            print(f"Error processing direct GET request: {e}")
            traceback.print_exc()
            response = {
                'date': data.get('date', ''),
                'found': False,
                'datasets': [],
                'count': 0,
                'error': str(e)
            }
            self.channel.basic_publish(
                exchange='',
                routing_key=CLIENT_QUEUE,
                body=create_message('GET_RESULT', response)
            )
            print(f"Storage device {self.keeper_id} sent directly response to client")

    def handle_get_with_queue(self, data):
        try:
            date = data.get('date')
            original_date = data.get('original_date', date)
            response_queue = data.get('response_queue')
            print(f"Query date: {date}")
            if date in self.data:
                date_info = self.data[date]
                
                # 检查数据结构
                if isinstance(date_info, list):
                    # 旧格式（列表），转换为新格式
                    datasets = [{
                        'data': date_info,
                        'column_names': [],
                        'source_file': 'unknown'
                    }]
                elif isinstance(date_info, dict) and 'datasets' in date_info:
                    # 新格式（包含datasets字段）
                    datasets = date_info['datasets']
                elif isinstance(date_info, dict) and 'data' in date_info:
                    # 中间格式（包含data字段但没有datasets字段）
                    datasets = [{
                        'data': date_info.get('data', []),
                        'column_names': date_info.get('column_names', []),
                        'source_file': 'unknown'
                    }]
                else:
                    # 未知格式
                    datasets = []
                
                total_records = sum(len(dataset.get('data', [])) for dataset in datasets)
                print(f"Found {total_records} records in {len(datasets)} datasets")
                
                # 构建响应，包含所有数据集
                response = {
                    'date': original_date,
                    'found': True,
                    'datasets': datasets,
                    'count': total_records
                }
            else:
                print(f"Date {date} data not found")
                response = {
                    'date': original_date,
                    'found': False,
                    'datasets': [],
                    'count': 0
                }
            print(f"Storage device {self.keeper_id} sending response to queue: {response_queue}")
            response_message = create_message('GET_RESULT', response)
            print(f"Storage device {self.keeper_id} sending response content: {response}")
            self.channel.basic_publish(
                exchange='',
                routing_key=response_queue,
                body=response_message
            )
            print(f"Storage device {self.keeper_id} sent response to queue {response_queue}")
        except Exception as e:
            print(f"Error processing GET request: {e}")
            traceback.print_exc()
            response = {
                'date': data.get('date', ''),
                'found': False,
                'datasets': [],
                'count': 0,
                'error': str(e)
            }
            self.channel.basic_publish(
                exchange='',
                routing_key=response_queue,
                body=create_message('GET_RESULT', response)
            )
            print(f"Storage device {self.keeper_id} sent response to queue {response_queue}")

    def send_to_replica(self, message):
        self.channel.basic_publish(
            exchange='',
            routing_key=f'replica_{self.keeper_id}',
            body=message
        )

    def get_data_from_replica(self, date):
        try:
            print(f"Preparing to send request to replica to get date {date} data")
            response = self.send_request_to_replica(date)
            if response and response.get('found'):
                print(f"Successfully got data from replica: {response}")
                return response
            else:
                print(f"Failed to get data from replica")
                return None
        except Exception as e:
            print(f"Error getting data from replica: {e}")
            return None

    def send_request_to_replica(self, date):
        replica_queue = f'replica_{self.keeper_id}'
        request_message = {'type': 'GET', 'date': date}
        print(f"Sending request to replica queue {replica_queue}: {request_message}")
        self.channel.basic_publish(exchange='', routing_key=replica_queue, body=json.dumps(request_message))
        response = self.wait_for_response(replica_queue)
        if response:
            print(f"Received response from replica: {response}")
        else:
            print("Did not receive response from replica")
        return response

    def wait_for_response(self, queue_name):
        method_frame, header_frame, body = self.channel.basic_get(queue=queue_name)
        if method_frame:
            return json.loads(body)
        return None

    def run(self):
        print(f"Storage device {self.keeper_id} running...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print(f"Storage device {self.keeper_id} shutting down...")
            if self.replica:
                self.replica.terminate()
            self.connection.close()

    def load_data(self, filename):
        try:
            with open(os.path.join(self.data_dir, filename), 'r') as f:
                reader = csv.reader(f)
                header = next(reader)  # Read the header row
                date_column = header.index('date')
                
                # 打印调试信息
                print(f"Before loading {filename}, data structure: {type(self.data)}")
                print(f"CSV Header: {header}")
                if '2015-01-01' in self.data:
                    print(f"Before loading, data for 2015-01-01: {self.data['2015-01-01']}")
                
                # 确保 self.data 是字典
                if not isinstance(self.data, dict):
                    self.data = {}
                
                # 加载数据
                for row in reader:
                    date = row[date_column]
                    
                    # 将行转换为列表，确保一致性
                    row_list = list(row)
                    
                    # 检查日期是否已存在
                    if date not in self.data:
                        # 创建新的日期条目，包含数据和列名
                        self.data[date] = {
                            'data': [row_list],
                            'column_names': header
                        }
                    else:
                        # 如果日期已存在，检查数据结构
                        if isinstance(self.data[date], list):
                            # 旧格式，转换为新格式
                            existing_rows = self.data[date]
                            self.data[date] = {
                                'data': existing_rows + [row_list],
                                'column_names': header
                            }
                        else:
                            # 新格式，检查记录是否已存在
                            existing_records = self.data[date]['data']
                            row_exists = False
                            for existing_row in existing_records:
                                if existing_row == row_list:
                                    row_exists = True
                                    break
                            
                            # 只添加不存在的记录
                            if not row_exists:
                                existing_records.append(row_list)
                
                # 打印调试信息
                if '2015-01-01' in self.data:
                    print(f"After loading {filename}, data for 2015-01-01: {self.data['2015-01-01']}")
                
                # 更新版本号并通知副本
                self.version += 1
                self.notify_replicas()
                return f"Loaded {filename} successfully"
        except Exception as e:
            import traceback
            traceback.print_exc()
            return f"Error loading {filename}: {str(e)}"

    def notify_replicas(self):
        for replica_port in self.replicas:
            try:
                replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                replica_socket.connect(('localhost', replica_port))
                
                # 确保数据可以被正确序列化
                serializable_data = {}
                for date, date_data in self.data.items():
                    if isinstance(date_data, list):
                        # 旧格式
                        serializable_data[date] = {
                            'data': [list(row) for row in date_data],
                            'column_names': []  # 没有列名
                        }
                    else:
                        # 新格式
                        serializable_data[date] = {
                            'data': [list(row) for row in date_data['data']],
                            'column_names': date_data['column_names']
                        }
                
                # 打印调试信息
                print(f"Sending data to replica: {serializable_data.keys()}")
                if '2015-01-01' in serializable_data:
                    print(f"Sending data for 2015-01-01: {serializable_data['2015-01-01']}")
                
                message = json.dumps({"type": "update", "data": serializable_data, "version": self.version})
                send_message(replica_socket, message)
                response = receive_message(replica_socket)
                replica_socket.close()
                print(f"Notified replica on port {replica_port}: {response}")
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"Failed to notify replica on port {replica_port}: {str(e)}")

    def get_data(self, date):
        # 打印调试信息
        print(f"get_data called for date: {date}")
        print(f"data structure: {type(self.data)}")
        if date in self.data:
            date_data = self.data[date]
            print(f"data for {date}: {date_data}")
            
            # 检查数据结构
            if isinstance(date_data, list):
                # 旧格式，转换为新格式
                return {
                    'data': date_data,
                    'column_names': []  # 没有列名
                }
            else:
                # 新格式，直接返回
                return date_data
        else:
            print(f"No data found for date: {date}")
            return None

    def get_all_data(self):
        # 确保数据可以被正确序列化
        serializable_data = {}
        for date, date_data in self.data.items():
            if isinstance(date_data, list):
                # 旧格式
                serializable_data[date] = {
                    'data': [list(row) for row in date_data],
                    'column_names': []  # 没有列名
                }
            else:
                # 新格式
                serializable_data[date] = {
                    'data': [list(row) for row in date_data['data']],
                    'column_names': date_data['column_names']
                }
        
        return json.dumps({"data": serializable_data, "version": self.version})

if __name__ == "__main__":
    print(f"Starting storage device process: {os.getpid()}")
    if len(sys.argv) < 3:
        print("Usage: python keeper.py <keeper_id> <num_keepers>")
        sys.exit(1)
    keeper_id = int(sys.argv[1])
    num_keepers = int(sys.argv[2])
    keeper = Keeper(keeper_id, num_keepers)
    keeper.run()