#!/usr/bin/env python
import pika
import sys
import json
from utils import *
import traceback
import argparse

class Replica:
    def __init__(self, keeper_id, verbose=False):
        self.keeper_id = keeper_id
        self.connection = None
        self.channel = None
        self.data = {}
        self.version = 0
        self.verbose = verbose  # 是否启用详细日志输出

        self.init_rabbitmq()

        self.log(f"Replica {keeper_id} has started", True)

    def log(self, message, force=False):
        """根据verbose设置打印日志"""
        if self.verbose or force:
            print(message)

    def init_rabbitmq(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        queue_name = f'replica_{self.keeper_id}'
        self.channel.queue_declare(queue=queue_name)

        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.process_message,
            auto_ack=True
        )

    def process_message(self, ch, method, properties, body):
        try:
            message = json.loads(body)
            msg_type = message.get('type')
            data = message.get('data')

            # 不打印健康检查相关消息
            if msg_type not in ['HEALTH_CHECK', 'HEALTH_RESPONSE']:
                self.log(f"Replica received message: {msg_type}, data: {message}")

            if msg_type == 'STORE':
                self.handle_store(data)
            elif msg_type == 'GET':
                response = self.handle_get(data)
                self.channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    body=json.dumps(response))
            elif msg_type == 'REPLICATE':
                self.handle_replicate(data)
            elif msg_type == 'GET_ALL_DATA':
                self.handle_get_all_data(properties)
            elif msg_type == 'TERMINATE':
                self.handle_terminate()
            else:
                self.log(f"Replica {self.keeper_id} received unknown type message: {msg_type}", True)
        except Exception as e:
            self.log(f"Error processing message in replica {self.keeper_id}: {str(e)}", True)
            traceback.print_exc()

    def handle_store(self, data):
        date = data.get('date')
        record = data.get('data')

        if date not in self.data:
            # 初始化为字典结构，与keeper保持一致
            self.data[date] = {
                'data': [],
                'column_names': data.get('column_names', []),
                'source_file': data.get('source_file', 'unknown')
            }
        
        # 如果record是列表，将其添加到data列表中
        if isinstance(record, list):
            # 确保self.data[date]['data']是一个列表
            if 'data' not in self.data[date]:
                self.data[date]['data'] = []
            elif not isinstance(self.data[date]['data'], list):
                # 如果data不是列表，将其转换为列表
                self.log(f"Converting data for date {date} from {type(self.data[date]['data'])} to list")
                self.data[date]['data'] = []
            
            # 添加每条记录
            for r in record:
                # 使用字符串表示来检查记录是否已存在
                # 这样可以避免unhashable type: 'list'错误
                r_str = str(r)
                record_exists = False
                for existing_r in self.data[date]['data']:
                    if str(existing_r) == r_str:
                        record_exists = True
                        break
                
                if not record_exists:
                    self.data[date]['data'].append(r)
        
        self.log(f"Replica {self.keeper_id} stored data for date {date}")

    def handle_get(self, data):
        date = data.get('date')
        self.log(f"Replica querying date: {date}")

        if date in self.data:
            # 确保返回正确的数据结构
            if isinstance(self.data[date], dict) and 'data' in self.data[date]:
                records = self.data[date]['data']
            else:
                records = self.data[date]  # 兼容旧格式
            
            self.log(f"Found {len(records)} records")
            return {'found': True, 'data': records}
        else:
            self.log(f"Data for date {date} not found")
            return {'found': False, 'data': []}

    def handle_replicate(self, data):
        try:
            date = data.get('date')
            source_file = data.get('source_file', 'unknown')
            
            # 检查数据结构
            if isinstance(data.get('data'), dict) and 'datasets' in data.get('data'):
                # 新格式（包含datasets字段）
                datasets = data.get('data').get('datasets', [])
                self.log(f"Replicating data with datasets structure for date {date}")
                
                # 初始化数据结构
                if date not in self.data:
                    self.data[date] = {
                        'data': [],
                        'column_names': [],
                        'source_file': source_file
                    }
                
                # 合并所有数据集的记录
                for dataset in datasets:
                    records = dataset.get('data', [])
                    column_names = dataset.get('column_names', [])
                    
                    # 如果当前数据集有列名但replica没有，使用这个列名
                    if not self.data[date].get('column_names') and column_names:
                        self.data[date]['column_names'] = column_names
                    
                    # 添加记录
                    for record in records:
                        # 使用字符串表示来检查记录是否已存在
                        record_str = str(record)
                        record_exists = False
                        
                        for existing_record in self.data[date]['data']:
                            if str(existing_record) == record_str:
                                record_exists = True
                                break
                        
                        if not record_exists:
                            self.data[date]['data'].append(record)
                
                self.log(f"Replica storage complete, date: {date}, total record count: {len(self.data[date]['data'])}")
            else:
                # 旧格式
                records = data.get('data', [])
                column_names = data.get('column_names', [])
                
                self.log(f"Replicating {len(records)} records for date {date}")
                
                self.data[date] = {
                    'data': records,
                    'column_names': column_names,
                    'source_file': source_file
                }
                
                self.log(f"Replica storage complete, date: {date}, record count: {len(records)}")
            
            return {'success': True}
        except Exception as e:
            self.log(f"Error processing REPLICATE request: {e}", True)
            traceback.print_exc()
            return {'success': False, 'error': str(e)}

    def handle_get_all_data(self, properties):
        """Handle request to get all data for migration"""
        self.log(f"Replica {self.keeper_id} received request for all data")
        
        try:
            # 确保数据是可序列化的
            serializable_data = {}
            for date, date_data in self.data.items():
                if isinstance(date_data, dict):
                    # 复制数据，确保不修改原始数据
                    serializable_data[date] = {
                        'data': date_data.get('data', []),
                        'column_names': date_data.get('column_names', []),
                        'source_file': date_data.get('source_file', 'unknown')
                    }
                else:
                    # 如果是旧格式（列表），转换为新格式
                    serializable_data[date] = {
                        'data': date_data,
                        'column_names': [],
                        'source_file': 'unknown'
                    }
            
            # 准备响应
            response = {
                'success': True,
                'data': serializable_data
            }
            
            # 发送响应
            if properties and properties.reply_to:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id
                    ),
                    body=create_message('GET_ALL_DATA_RESULT', response)
                )
                self.log(f"Replica {self.keeper_id} sent all data ({len(serializable_data)} dates)")
            else:
                self.log(f"No reply_to queue provided for GET_ALL_DATA request", True)
        except Exception as e:
            self.log(f"Error handling GET_ALL_DATA request: {str(e)}", True)
            traceback.print_exc()
            
            # 尝试发送错误响应
            if properties and properties.reply_to:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id
                    ),
                    body=create_message('GET_ALL_DATA_RESULT', {'success': False, 'error': str(e)})
                )

    def handle_terminate(self):
        """Handle termination request from manager"""
        self.log(f"Replica {self.keeper_id} received termination request", True)
        
        try:
            # Clean up resources
            if self.connection:
                self.connection.close()
                
            self.log(f"Replica {self.keeper_id} is shutting down", True)
            # Exit the process
            sys.exit(0)
        except Exception as e:
            self.log(f"Error during replica termination: {str(e)}", True)
            traceback.print_exc()
            sys.exit(1)

    def run(self):
        self.log(f"Replica {self.keeper_id} is running...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.log(f"Replica {self.keeper_id} is shutting down...")
            self.connection.close()

    def update_data(self, data, version):
        if version > self.version:
            # 打印调试信息
            self.log(f"Updating data, new version: {version}")
            self.log(f"Received data structure: {type(data)}")
            if '2015-01-01' in data:
                self.log(f"Received data for 2015-01-01: {data['2015-01-01']}")
            
            # 确保 self.data 是字典
            if not isinstance(self.data, dict):
                self.data = {}
            
            # 合并数据，而不是替换
            for date, date_data in data.items():
                if date not in self.data:
                    # 使用新的数据结构格式
                    self.data[date] = {
                        'data': [],
                        'column_names': [],
                        'source_file': 'unknown'
                    }
                
                # 确保self.data[date]是字典格式
                if not isinstance(self.data[date], dict):
                    self.data[date] = {
                        'data': self.data[date] if isinstance(self.data[date], list) else [],
                        'column_names': [],
                        'source_file': 'unknown'
                    }
                
                # 确保data字段存在且是列表
                if 'data' not in self.data[date]:
                    self.data[date]['data'] = []
                elif not isinstance(self.data[date]['data'], list):
                    self.data[date]['data'] = []
                
                # 处理不同格式的输入数据
                rows = []
                if isinstance(date_data, dict) and 'data' in date_data:
                    rows = date_data['data']
                elif isinstance(date_data, list):
                    rows = date_data
                
                for row in rows:
                    # 将行转换为列表，确保一致性
                    row_list = list(row) if not isinstance(row, str) else row
                    
                    # 使用字符串表示来检查记录是否已存在
                    row_str = str(row_list)
                    record_exists = False
                    for existing_row in self.data[date]['data']:
                        if str(existing_row) == row_str:
                            record_exists = True
                            break
                    
                    # 只添加不存在的记录
                    if not record_exists:
                        self.data[date]['data'].append(row_list)
            
            # 打印调试信息
            if '2015-01-01' in self.data:
                self.log(f"After update, data for 2015-01-01: {self.data['2015-01-01']}")
            
            self.version = version
            return "Data updated successfully"
        else:
            return "Data is already up to date"

    def get_data(self, date):
        # 打印调试信息
        self.log(f"Replica get_data called for date: {date}")
        self.log(f"Replica data structure: {type(self.data)}")
        
        if date in self.data:
            # 检查数据结构并返回正确的格式
            if isinstance(self.data[date], dict):
                if 'data' in self.data[date]:
                    self.log(f"Found {len(self.data[date]['data'])} records for date {date}")
                    return self.data[date]['data']
                else:
                    self.log(f"Data for date {date} exists but has no 'data' field")
                    return []
            else:
                # 兼容旧格式
                self.log(f"Found data in old format for date {date}")
                return self.data[date]
        else:
            self.log(f"Replica: No data found for date: {date}")
            return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start a replica node')
    parser.add_argument('keeper_id', type=int, help='ID of the associated keeper')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--quiet', '-q', action='store_true', help='Disable verbose logging')
    
    args = parser.parse_args()
    
    verbose = args.verbose and not args.quiet
    
    replica = Replica(args.keeper_id, verbose)
    replica.run()