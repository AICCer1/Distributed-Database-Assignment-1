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
                if properties and properties.reply_to:
                    try:
                        self.channel.basic_publish(
                            exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(
                                correlation_id=properties.correlation_id if properties.correlation_id else None
                            ),
                            body=json.dumps(response))
                        self.log(f"Replica {self.keeper_id} sent response to {properties.reply_to}")
                    except Exception as e:
                        self.log(f"Error sending response to {properties.reply_to}: {e}", True)
                        traceback.print_exc()
                else:
                    self.log(f"Replica {self.keeper_id} cannot reply to GET request: no reply_to queue provided", True)
            elif msg_type == 'REPLICATE':
                self.handle_replicate(data)
            elif msg_type == 'REPLICATE_BATCH':
                self.handle_replicate_batch(data)
            elif msg_type == 'GET_ALL_DATA':
                self.handle_get_all_data(properties)
            elif msg_type == 'GET_DATE_LIST':
                self.handle_get_date_list(properties)
            elif msg_type == 'GET_DATA_BATCH':
                self.handle_get_data_batch(data, properties)
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
        # 添加对data参数的检查
        if data is None:
            self.log(f"Replica {self.keeper_id} received GET request with None data")
            return {'found': False, 'error': 'Invalid request format'}
            
        # 检查data是否是字典或字符串
        if isinstance(data, str):
            date = data
        elif isinstance(data, dict):
            date = data.get('date')
        else:
            self.log(f"Replica {self.keeper_id} received GET request with invalid data type: {type(data)}")
            return {'found': False, 'error': f'Invalid data type: {type(data)}'}
            
        if not date:
            self.log(f"Replica {self.keeper_id} received GET request without date")
            return {'found': False, 'error': 'No date provided'}
            
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
            
            # 检查是否是从迁移过来的数据，可能包含完整的数据结构
            if source_file == 'migration' and isinstance(data.get('data'), dict):
                self.log(f"Received migrated data with complete structure for date {date}")
                
                # 直接使用迁移过来的完整数据结构，不做任何修改
                if 'datasets' in data.get('data'):
                    self.log(f"Using complete datasets structure with {len(data.get('data')['datasets'])} datasets")
                    self.data[date] = data.get('data')
                else:
                    # 如果没有datasets字段，但有其他数据，创建一个datasets结构
                    self.log(f"Creating datasets structure for migrated data")
                    self.data[date] = {
                        'datasets': []
                    }
                    
                    # 如果有data字段，添加为一个数据集
                    if 'data' in data.get('data'):
                        self.data[date]['datasets'].append({
                            'data': data.get('data')['data'],
                            'column_names': data.get('data').get('column_names', []),
                            'source_file': data.get('data').get('source_file', source_file)
                        })
                        self.log(f"Added dataset with {len(data.get('data')['data'])} records")
                
                self.log(f"Replica storage complete, date: {date}, using complete structure")
                return {'success': True}
            
            # 检查数据结构
            if isinstance(data.get('data'), dict) and 'datasets' in data.get('data'):
                # 新格式（包含datasets字段）
                datasets = data.get('data').get('datasets', [])
                self.log(f"Replicating data with datasets structure for date {date}")
                
                # 直接使用完整的数据结构
                self.data[date] = data.get('data')
                self.log(f"Replica storage complete, date: {date}, with {len(datasets)} datasets")
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

    def handle_replicate_batch(self, data):
        """批量处理多个日期的复制请求"""
        try:
            batch_data = data.get('batch_data', {})
            source_file = data.get('source_file', 'unknown')
            
            if not batch_data:
                self.log(f"Received empty batch data for replication", True)
                return {'success': False, 'error': 'Empty batch data'}
                
            self.log(f"Replicating batch of {len(batch_data)} dates from {source_file}")
            
            # 批量处理每个日期的数据
            for date, date_data in batch_data.items():
                records = date_data.get('data', [])
                column_names = date_data.get('column_names', [])
                
                if not records:
                    continue
                    
                # 使用datasets格式存储
                if date not in self.data:
                    self.data[date] = {
                        'datasets': [{
                            'data': records,
                            'column_names': column_names,
                            'source_file': source_file
                        }]
                    }
                else:
                    # 日期已存在，需要检查是否有相同来源的数据集
                    existing_data = self.data[date]
                    
                    # 如果是列表或旧格式，转换为新格式
                    if isinstance(existing_data, list):
                        existing_data = {
                            'datasets': [{
                                'data': existing_data,
                                'column_names': [],
                                'source_file': 'unknown'
                            }]
                        }
                    elif isinstance(existing_data, dict) and 'datasets' not in existing_data:
                        existing_data = {
                            'datasets': [{
                                'data': existing_data.get('data', []),
                                'column_names': existing_data.get('column_names', []),
                                'source_file': existing_data.get('source_file', 'unknown')
                            }]
                        }
                    
                    # 检查是否已存在来自相同文件的数据集
                    found_existing = False
                    for dataset in existing_data['datasets']:
                        if dataset.get('source_file') == source_file:
                            # 已存在该来源的数据集，跳过
                            found_existing = True
                            break
                    
                    # 如果没有找到相同来源的数据集，添加新数据集
                    if not found_existing:
                        existing_data['datasets'].append({
                            'data': records,
                            'column_names': column_names,
                            'source_file': source_file
                        })
                    
                    self.data[date] = existing_data
            
            self.log(f"Batch replication completed for {len(batch_data)} dates")
            return {'success': True}
            
        except Exception as e:
            self.log(f"Error processing REPLICATE_BATCH message: {e}", True)
            traceback.print_exc()
            return {'success': False, 'error': str(e)}

    def handle_get_all_data(self, properties):
        """Handle request to get all data for migration"""
        self.log(f"Replica {self.keeper_id} received request for all data")
        
        try:
            # 确保数据是可序列化的，同时保持完整的数据结构
            serializable_data = {}
            for date, date_data in self.data.items():
                # 打印调试信息
                self.log(f"Processing date {date}, data type: {type(date_data)}")
                
                if isinstance(date_data, dict):
                    if 'datasets' in date_data:
                        # 如果已经是新格式（包含datasets字段），直接使用
                        self.log(f"Date {date} already has datasets structure with {len(date_data['datasets'])} datasets")
                        serializable_data[date] = date_data
                    elif 'data' in date_data:
                        # 如果是中间格式（包含data字段但没有datasets字段），转换为新格式
                        self.log(f"Converting date {date} from intermediate format to datasets format")
                        serializable_data[date] = {
                            'datasets': [{
                                'data': date_data.get('data', []),
                                'column_names': date_data.get('column_names', []),
                                'source_file': date_data.get('source_file', 'unknown')
                            }]
                        }
                    else:
                        # 其他字典格式，尝试转换
                        self.log(f"Converting date {date} from other dict format to datasets format")
                        serializable_data[date] = {
                            'datasets': [{
                                'data': [],
                                'column_names': [],
                                'source_file': 'unknown'
                            }]
                        }
                        for key, value in date_data.items():
                            if key not in ['datasets']:
                                serializable_data[date]['datasets'][0][key] = value
                else:
                    # 如果是旧格式（列表），转换为新格式
                    self.log(f"Converting date {date} from list format to datasets format")
                    serializable_data[date] = {
                        'datasets': [{
                            'data': date_data if isinstance(date_data, list) else [],
                            'column_names': [],
                            'source_file': 'unknown'
                        }]
                    }
            
            # 打印最终数据结构的摘要
            for date, date_data in serializable_data.items():
                if 'datasets' in date_data:
                    self.log(f"Final structure for date {date}: {len(date_data['datasets'])} datasets")
                    for i, dataset in enumerate(date_data['datasets']):
                        self.log(f"  Dataset {i+1}: {len(dataset.get('data', []))} records, source: {dataset.get('source_file', 'unknown')}")
            
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

    def handle_get_date_list(self, properties):
        """Handle request to get list of all dates"""
        self.log(f"Replica {self.keeper_id} received request for date list")
        
        try:
            # 获取所有日期列表
            date_list = list(self.data.keys())
            self.log(f"Replica has {len(date_list)} dates")
            
            # 准备响应
            response = {
                'success': True,
                'dates': date_list
            }
            
            # 发送响应
            if properties and properties.reply_to:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id
                    ),
                    body=create_message('GET_DATE_LIST_RESULT', response)
                )
                self.log(f"Replica {self.keeper_id} sent date list ({len(date_list)} dates)")
            else:
                self.log(f"No reply_to queue provided for GET_DATE_LIST request", True)
        except Exception as e:
            self.log(f"Error handling GET_DATE_LIST request: {str(e)}", True)
            traceback.print_exc()
            
            # 尝试发送错误响应
            if properties and properties.reply_to:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id
                    ),
                    body=create_message('GET_DATE_LIST_RESULT', {'success': False, 'error': str(e)})
                )

    def handle_get_data_batch(self, data, properties):
        """Handle request to get data for a batch of dates"""
        date_list = data.get('dates', [])
        self.log(f"Replica {self.keeper_id} received request for data batch with {len(date_list)} dates")
        
        try:
            # 获取指定日期的数据
            batch_data = {}
            for date in date_list:
                if date in self.data:
                    # 确保数据是可序列化的，同时保持完整的数据结构
                    date_data = self.data[date]
                    
                    if isinstance(date_data, dict):
                        if 'datasets' in date_data:
                            # 如果已经是新格式（包含datasets字段），直接使用
                            batch_data[date] = date_data
                        elif 'data' in date_data:
                            # 如果是中间格式（包含data字段但没有datasets字段），转换为新格式
                            batch_data[date] = {
                                'datasets': [{
                                    'data': date_data.get('data', []),
                                    'column_names': date_data.get('column_names', []),
                                    'source_file': date_data.get('source_file', 'unknown')
                                }]
                            }
                        else:
                            # 其他字典格式，尝试转换
                            batch_data[date] = {
                                'datasets': [{
                                    'data': [],
                                    'column_names': [],
                                    'source_file': 'unknown'
                                }]
                            }
                            for key, value in date_data.items():
                                if key not in ['datasets']:
                                    batch_data[date]['datasets'][0][key] = value
                    else:
                        # 如果是旧格式（列表），转换为新格式
                        batch_data[date] = {
                            'datasets': [{
                                'data': date_data if isinstance(date_data, list) else [],
                                'column_names': [],
                                'source_file': 'unknown'
                            }]
                        }
            
            self.log(f"Prepared data for {len(batch_data)}/{len(date_list)} dates")
            
            # 准备响应
            response = {
                'success': True,
                'data': batch_data
            }
            
            # 发送响应
            if properties and properties.reply_to:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id
                    ),
                    body=create_message('GET_DATA_BATCH_RESULT', response)
                )
                self.log(f"Replica {self.keeper_id} sent data batch ({len(batch_data)} dates)")
            else:
                self.log(f"No reply_to queue provided for GET_DATA_BATCH request", True)
        except Exception as e:
            self.log(f"Error handling GET_DATA_BATCH request: {str(e)}", True)
            traceback.print_exc()
            
            # 尝试发送错误响应
            if properties and properties.reply_to:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id
                    ),
                    body=create_message('GET_DATA_BATCH_RESULT', {'success': False, 'error': str(e)})
                )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start a replica node')
    parser.add_argument('keeper_id', type=int, help='ID of the associated keeper')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--quiet', '-q', action='store_true', help='Disable verbose logging')
    
    args = parser.parse_args()
    
    verbose = args.verbose and not args.quiet
    
    replica = Replica(args.keeper_id, verbose)
    replica.run()