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
import argparse

class Keeper:
    def __init__(self, keeper_id, num_keepers, verbose=False):
        self.keeper_id = keeper_id
        self.num_keepers = num_keepers
        self.connection = None
        self.channel = None
        self.data = {}
        self.replica = None
        self.verbose = verbose  # 是否启用详细日志输出
        
        self.init_rabbitmq()
        self.start_replica()
        self.log(f"Storage device {keeper_id} has started", True)

    def log(self, message, force=False):
        """根据verbose设置打印日志"""
        if self.verbose or force:
            print(message)

    def init_rabbitmq(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        queue_name = f'keeper_{self.keeper_id}'
        self.channel.queue_declare(queue=queue_name)
        self.log(f"Storage device {self.keeper_id} declared queue: {queue_name}")
        self.channel.queue_declare(queue=f'replica_{self.keeper_id}')
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.process_message,
            auto_ack=True
        )
        self.log(f"Storage device {self.keeper_id} started listening to queue: {queue_name}")

    def start_replica(self):
        try:
            # 传递verbose参数给replica进程，不再重定向输出
            self.replica = subprocess.Popen(
                [sys.executable, 'replica.py', str(self.keeper_id), 
                 '--verbose' if self.verbose else '--quiet']
            )
            self.log(f"Storage device {self.keeper_id} started replica", True)
        except Exception as e:
            self.log(f"Storage device {self.keeper_id} failed to start replica: {str(e)}", True)
            traceback.print_exc()

    def process_message(self, ch, method, properties, body):
        try:
            message = parse_message(body)
            msg_type = message.get('type')
            data = message.get('data')
            
            # 不打印健康检查消息，减少日志输出
            if msg_type != 'HEALTH_CHECK':
                self.log(f"Storage device {self.keeper_id} received message: {msg_type}, data: {data}")
                
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
            elif msg_type == 'HEALTH_CHECK':
                self.handle_health_check(properties)
            else:
                self.log(f"Storage device {self.keeper_id} received unknown type message: {msg_type}", True)
        except Exception as e:
            self.log(f"Error processing message: {str(e)}", True)
            traceback.print_exc()

    def handle_store(self, data):
        date = data.get('date')
        records = data.get('data', [])
        column_names = data.get('column_names', [])
        source_file = data.get('source_file', 'unknown')  # 添加文件来源信息
        
        self.log(f"Storing data for date: {date}")
        self.log(f"DEBUG - Received column names: {column_names}")
        self.log(f"DEBUG - Source file: {source_file}")
        
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
                    self.log(f"Updated existing dataset from {source_file} for date {date}. Total records: {len(existing_records)}")
                    break
            
            # 如果没有找到相同来源的数据集，添加新的数据集
            if not found_existing_dataset:
                date_data['datasets'].append({
                    'data': records,
                    'column_names': column_names,
                    'source_file': source_file
                })
                self.log(f"Added new dataset from {source_file} for date {date} with {len(records)} records")
            
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
            self.log(f"Created new entry for date {date} with {len(records)} records from {source_file}")
        
        # 打印调试信息
        self.log(f"DEBUG - Final data structure for date {date}: {self.data[date]}")
            
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
            self.log(f"Query date: {date}")
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
                self.log(f"Found {total_records} records in {len(datasets)} datasets")
                
                # 构建响应，包含所有数据集
                response = {
                    'date': original_date,
                    'found': True,
                    'datasets': datasets,
                    'count': total_records
                }
            else:
                self.log(f"Date {date} data not found, trying to get from replica...")
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
                self.log(f"Storage device {self.keeper_id} did not receive reply_to attribute, cannot reply")
                return
            self.log(f"Storage device {self.keeper_id} will reply to queue: {reply_to}, correlation_id: {correlation_id}")
            self.log(f"Storage device {self.keeper_id} current data: {list(self.data.keys())[:5]}...")
            self.log(f"Storage device {self.keeper_id} sending response to queue: {reply_to}")
            response_message = create_message('GET_RESULT', response)
            self.log(f"Storage device {self.keeper_id} sending response content: {response}")
            self.channel.basic_publish(
                exchange='',
                routing_key=reply_to,
                properties=pika.BasicProperties(
                    correlation_id=correlation_id
                ),
                body=response_message
            )
            self.log(f"Storage device {self.keeper_id} sent response to queue {reply_to}")
        except Exception as e:
            self.log(f"Error processing GET request: {e}", True)
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
            self.log(f"Storage device {self.keeper_id} sent response to queue {reply_to}")

    def handle_get_direct(self, data):
        try:
            date = data.get('date')
            original_date = data.get('original_date', date)
            self.log(f"Query date: {date}")
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
                self.log(f"Found {total_records} records in {len(datasets)} datasets")
                
                # 构建响应，包含所有数据集
                response = {
                    'date': original_date,
                    'found': True,
                    'datasets': datasets,
                    'count': total_records
                }
            else:
                self.log(f"Date {date} data not found")
                response = {
                    'date': original_date,
                    'found': False,
                    'datasets': [],
                    'count': 0
                }
            self.log(f"Storage device {self.keeper_id} directly sending response to client")
            response_message = create_message('GET_RESULT', response)
            self.log(f"Storage device {self.keeper_id} sending response content: {response}")
            self.channel.basic_publish(
                exchange='',
                routing_key=CLIENT_QUEUE,
                body=response_message
            )
            self.log(f"Storage device {self.keeper_id} sent directly response to client")
        except Exception as e:
            self.log(f"Error processing direct GET request: {e}", True)
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
            self.log(f"Storage device {self.keeper_id} sent directly response to client")

    def handle_get_with_queue(self, data):
        try:
            date = data.get('date')
            original_date = data.get('original_date', date)
            response_queue = data.get('response_queue')
            self.log(f"Query date: {date}")
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
                self.log(f"Found {total_records} records in {len(datasets)} datasets")
                
                # 构建响应，包含所有数据集
                response = {
                    'date': original_date,
                    'found': True,
                    'datasets': datasets,
                    'count': total_records
                }
            else:
                self.log(f"Date {date} data not found")
                response = {
                    'date': original_date,
                    'found': False,
                    'datasets': [],
                    'count': 0
                }
            self.log(f"Storage device {self.keeper_id} sending response to queue: {response_queue}")
            response_message = create_message('GET_RESULT', response)
            self.log(f"Storage device {self.keeper_id} sending response content: {response}")
            self.channel.basic_publish(
                exchange='',
                routing_key=response_queue,
                body=response_message
            )
            self.log(f"Storage device {self.keeper_id} sent response to queue {response_queue}")
        except Exception as e:
            self.log(f"Error processing GET request: {e}", True)
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
            self.log(f"Storage device {self.keeper_id} sent response to queue {response_queue}")

    def send_to_replica(self, message):
        self.channel.basic_publish(
            exchange='',
            routing_key=f'replica_{self.keeper_id}',
            body=message
        )

    def get_data_from_replica(self, date):
        try:
            self.log(f"Preparing to send request to replica to get date {date} data")
            response = self.send_request_to_replica(date)
            if response and response.get('found'):
                self.log(f"Successfully got data from replica: {response}")
                return response
            else:
                self.log(f"Failed to get data from replica")
                return None
        except Exception as e:
            self.log(f"Error getting data from replica: {e}", True)
            return None

    def send_request_to_replica(self, date):
        replica_queue = f'replica_{self.keeper_id}'
        request_message = {'type': 'GET', 'date': date}
        self.log(f"Sending request to replica queue {replica_queue}: {request_message}")
        self.channel.basic_publish(exchange='', routing_key=replica_queue, body=json.dumps(request_message))
        response = self.wait_for_response(replica_queue)
        if response:
            self.log(f"Received response from replica: {response}")
        else:
            self.log("Did not receive response from replica")
        return response

    def wait_for_response(self, queue_name):
        method_frame, header_frame, body = self.channel.basic_get(queue=queue_name)
        if method_frame:
            return json.loads(body)
        return None

    def run(self):
        self.log(f"Storage device {self.keeper_id} running...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.log(f"Storage device {self.keeper_id} shutting down...")
            if self.replica:
                self.replica.terminate()
            self.connection.close()

    def get_data(self, date):
        # 打印调试信息
        self.log(f"get_data called for date: {date}")
        self.log(f"data structure: {type(self.data)}")
        if date in self.data:
            date_data = self.data[date]
            self.log(f"data for {date}: {date_data}")
            
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
            self.log(f"No data found for date: {date}")
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

    def handle_health_check(self, properties):
        """Handle health check request from manager"""
        # 不打印健康检查接收信息，减少日志输出
        # self.log(f"Storage device {self.keeper_id} received health check")
        
        # If there's a reply_to queue, send a response
        if properties and properties.reply_to:
            try:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id
                    ),
                    body=create_message('HEALTH_RESPONSE', {'status': 'alive', 'keeper_id': self.keeper_id})
                )
                # 不打印健康检查响应信息，减少日志输出
                # self.log(f"Storage device {self.keeper_id} sent health check response")
            except Exception as e:
                self.log(f"Error sending health check response: {str(e)}", True)
                traceback.print_exc()

if __name__ == "__main__":
    print(f"Starting storage device process: {os.getpid()}")
    
    parser = argparse.ArgumentParser(description='Start a keeper node')
    parser.add_argument('keeper_id', type=int, help='ID of this keeper')
    parser.add_argument('num_keepers', type=int, help='Total number of keepers')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--quiet', '-q', action='store_true', help='Disable verbose logging')
    
    args = parser.parse_args()
    
    verbose = args.verbose and not args.quiet
    
    keeper = Keeper(args.keeper_id, args.num_keepers, verbose)
    keeper.run()