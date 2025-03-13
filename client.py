#!/usr/bin/env python
import pika
import sys
import json
import cmd
from utils import *
import re
import datetime
import traceback
import time

def is_valid_date(year, month, day):
    try:
        datetime.date(year, month, day)
        return True
    except ValueError:
        return False

class Client(cmd.Cmd):
    prompt = '> '
    intro = 'Welcome to the data storage system. Type help or ? to see available commands.'

    def __init__(self):
        super().__init__()
        self.connection = None
        self.channel = None
        self.last_response = None
        
        self.init_rabbitmq()

    def init_rabbitmq(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        
        self.channel.queue_declare(queue=CLIENT_QUEUE)
        self.channel.queue_declare(queue=MANAGER_QUEUE)
        
        self.channel.basic_consume(
            queue=CLIENT_QUEUE,
            on_message_callback=self.process_message,
            auto_ack=False
        )

    def process_message(self, ch, method, properties, body):
        try:
            message = parse_message(body)
            msg_type = message.get('type')
            data = message.get('data')
            
            # 保存最后收到的响应
            self.last_response = message
            
            if msg_type == 'GET_RESULT':
                print(f"\n===== GET results for date: {data.get('date', '')} =====")
                if data.get('found'):
                    date = data.get('date', '')
                    count = data.get('count', 0)
                    keeper_id = data.get('keeper_id', 'unknown')
                    
                    # 显示数据来自哪个keeper节点
                    print(f"Data from keeper node: {keeper_id}")
                    
                    # 检查是否使用新的数据结构（包含datasets字段）
                    if 'datasets' in data:
                        datasets = data.get('datasets', [])
                        print(f"Found {count} records in {len(datasets)} datasets for the date: {date}\n")
                        
                        # 遍历每个数据集
                        for dataset_idx, dataset in enumerate(datasets):
                            records = dataset.get('data', [])
                            column_names = dataset.get('column_names', [])
                            source_file = dataset.get('source_file', 'unknown')
                            
                            print(f"\n----- Dataset #{dataset_idx + 1} from {source_file} -----")
                            print(f"Records count: {len(records)}")
                            
                            if column_names and len(column_names) > 0:
                                print("\nHeader:")
                                print("-" * 100)
                                for i in range(0, len(column_names), 5):
                                    chunk = column_names[i:i+5]
                                    print(", ".join(chunk))
                                print("-" * 100)
                            
                            # 显示所有记录，不省略
                            for record_idx, record in enumerate(records):
                                print(f"\nRecord #{record_idx + 1}:")
                                print("-" * 50)
                                
                                if isinstance(record, list):
                                    # 使用实际的列名
                                    if column_names and len(column_names) > 0:
                                        for i, value in enumerate(record):
                                            if i < len(column_names):
                                                column_name = column_names[i]
                                            else:
                                                column_name = f"Column {i+1}"
                                            
                                            if i > 0 and i % 5 == 0:
                                                print()
                                            print(f"{column_name}: {value}", end="  ")
                                        print("\n")
                                    else:
                                        # 如果没有列名，使用索引
                                        for i in range(0, len(record), 5):
                                            chunk = record[i:i+5]
                                            formatted = ", ".join(f"[{i+j}]: {v}" for j, v in enumerate(chunk))
                                            print(formatted)
                                        print()
                                else:
                                    print(record)
                                print("-" * 50)
                            
                            print(f"----- End of Dataset #{dataset_idx + 1} -----\n")
                    else:
                        # 兼容旧的数据结构
                        records = data.get('data', [])
                        column_names = data.get('column_names', [])
                        
                        print(f"Found {count} records for the date: {date}\n")
                        
                        if column_names and len(column_names) > 0:
                            print("Header:")
                            print("-" * 100)
                            for i in range(0, len(column_names), 5):
                                chunk = column_names[i:i+5]
                                print(", ".join(chunk))
                            print("-" * 100)
                            print()
                        
                        try:
                            for record_idx, record in enumerate(records):
                                print(f"Record #{record_idx + 1}:")
                                print("-" * 50)
                                
                                if isinstance(record, list):
                                    # 使用实际的列名
                                    if column_names and len(column_names) > 0:
                                        for i, value in enumerate(record):
                                            if i < len(column_names):
                                                column_name = column_names[i]
                                            else:
                                                column_name = f"Column {i+1}"
                                            
                                            if i > 0 and i % 5 == 0:
                                                print()
                                            print(f"{column_name}: {value}", end="  ")
                                        print("\n")
                                    else:
                                        # 如果没有列名，使用索引
                                        for i in range(0, len(record), 5):
                                            chunk = record[i:i+5]
                                            formatted = ", ".join(f"[{i+j}]: {v}" for j, v in enumerate(chunk))
                                            print(formatted)
                                        print()
                                else:
                                    print(record)
                                print("-" * 50)
                        except Exception as e:
                            print(f"Error processing response: {e}")
                            traceback.print_exc()
                else:
                    date = data.get('date', '')
                    error_msg = data.get('error', 'No data found')
                    print(f"No data found for the date: {date}")
                    if 'error' in data:
                        print(f"Error: {error_msg}")
            
            elif msg_type == 'LOAD_RESULT':
                # 不显示处理LOAD_RESULT的调试信息，只显示实际的结果
                if data.get('success'):
                    filename = data.get('filename', '')
                    status = data.get('status', 'completed')
                    
                    # 简化进度显示，只显示开始和完成信息
                    if status == 'started':
                        print(f"Started loading file: {filename}")
                    elif status == 'completed':
                        records = data.get('records', 0)
                        dates = data.get('dates', 0)
                        unique_dates = data.get('unique_dates', 0)
                        
                        print(f"\n===== File loading completed: {filename} =====")
                        print(f"File loaded successfully: {filename}")
                        print(f"Total records: {records}")
                        print(f"Unique dates: {dates}")
                else:
                    error = data.get('error', 'Unknown error')
                    print(f"Failed to load file: {error}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error processing message: {e}")
            traceback.print_exc()

    def send_to_manager(self, message):
        try:
            if self.connection is None or self.connection.is_closed:
                print("Connection closed, trying to reconnect...")
                self.init_rabbitmq()
            
            self.channel.basic_publish(
                exchange='',
                routing_key=MANAGER_QUEUE,
                body=message
            )
        except pika.exceptions.AMQPError as e:
            print(f"Error sending message to manager: {e}")
            try:
                self.init_rabbitmq()
                self.channel.basic_publish(
                    exchange='',
                    routing_key=MANAGER_QUEUE,
                    body=message
                )
            except Exception as conn_err:
                print(f"Failed to reconnect and send message: {conn_err}")
        except Exception as e:
            print(f"Error sending message: {e}")

    def do_LOAD(self, arg):
        if not arg:
            print("Please provide filename")
            return
        
        print(f"Loading file {arg}...")
        self.last_response = None
        self.send_to_manager(create_message('LOAD', arg))
        
        print("Waiting for file to load completely...")
        # 无限等待，直到收到LOAD_RESULT且状态为completed的响应
        completed_received = False
        while True:
            try:
                self.connection.process_data_events(time_limit=0.5)
                
                # 检查是否收到了最终的LOAD_RESULT响应
                if (self.last_response and 
                    self.last_response.get('type') == 'LOAD_RESULT' and 
                    self.last_response.get('data', {}).get('status') == 'completed'):
                    completed_received = True
                    # 收到完成消息后，再等待1秒确保所有消息都已处理完毕
                    time.sleep(1)
                    break
                    
                # 如果收到了错误响应，也退出
                if (self.last_response and 
                    self.last_response.get('type') == 'LOAD_RESULT' and 
                    not self.last_response.get('data', {}).get('success', True)):
                    break
                    
            except pika.exceptions.AMQPError as e:
                print(f"Error processing event: {e}")
                try:
                    self.init_rabbitmq()
                except Exception as conn_err:
                    print(f"Failed to reconnect: {conn_err}")
                    break
        
        if completed_received:
            print("All processing completed.")

    def do_GET(self, arg):
        if not arg:
            print("Please provide date, supported formats: DD-MM-YYYY, YYYY-MM-DD, YYYYMMDD")
            return
        
        parsed_date = None
        try:
            if re.match(r'^\d{2}-\d{2}-\d{4}$', arg):
                day, month, year = map(int, arg.split('-'))
                parsed_date = datetime.date(year, month, day)
            elif re.match(r'^\d{4}-\d{2}-\d{2}$', arg):
                year, month, day = map(int, arg.split('-'))
                parsed_date = datetime.date(year, month, day)
            elif re.match(r'^\d{8}$', arg):
                year = int(arg[0:4])
                month = int(arg[4:6])
                day = int(arg[6:8])
                parsed_date = datetime.date(year, month, day)
            else:
                print("Unrecognized date format, supported formats: DD-MM-YYYY, YYYY-MM-DD, YYYYMMDD")
                return
                
            if not is_valid_date(year, month, day):
                print(f"Invalid date: {arg}, for example, there is no 31st in February")
                return
                
        except ValueError as e:
            print(f"Date parsing error: {e}")
            return
            
        print(f"\n===== Getting data for date: {arg} =====")
        self.last_response = None
        
        try:
            if self.connection is None or self.connection.is_closed:
                print("Connection closed, trying to reconnect...")
                self.init_rabbitmq()
                
            self.send_to_manager(create_message('GET', arg))
            
            print("Waiting for response...")
            for i in range(20):
                if i % 5 == 0 and i > 0:
                    print(f"Still waiting for response... {i}/20")
                    
                try:
                    self.connection.process_data_events(time_limit=0.5)
                    if self.last_response and self.last_response.get('type') == 'GET_RESULT':
                        break
                except pika.exceptions.AMQPError as e:
                    print(f"Error processing event: {e}")
                    try:
                        self.init_rabbitmq()
                    except Exception as conn_err:
                        print(f"Failed to reconnect: {conn_err}")
                        break
            
            if not self.last_response or self.last_response.get('type') != 'GET_RESULT':
                print("\n===== No response or incorrect response type received =====")
        except pika.exceptions.AMQPError as e:
            print(f"RabbitMQ error: {e}")
            print("Trying to reconnect...")
            try:
                self.init_rabbitmq()
                print("Reconnected successfully")
            except Exception as conn_err:
                print(f"Failed to reconnect: {conn_err}")
        except Exception as e:
            print(f"Error sending GET request: {e}")
            traceback.print_exc()
        
        print("\n===== GET request completed =====")

    def do_exit(self, arg):
        print("Exiting the program...")
        self.connection.close()
        return True

    def do_quit(self, arg):
        return self.do_exit(arg)

    def default(self, line):
        parts = line.split()
        if parts and parts[0].upper() == 'LOAD':
            return self.do_LOAD(' '.join(parts[1:]))
        elif parts and parts[0].upper() == 'GET':
            return self.do_GET(' '.join(parts[1:]))
        else:
            print(f"Unknown command: {line}")
            print("Available commands: LOAD [filename], GET [date], exit")

    def emptyline(self):
        pass

    def get_data(self, date):
        print(f"Getting data for date: {date}...")
        response = self.send_request(date)
        if response:
            print(f"Received response: {response}")
        else:
            print("No response received")

if __name__ == "__main__":
    client = Client()
    client.cmdloop()