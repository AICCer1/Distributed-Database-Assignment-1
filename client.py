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
                    keepers_queried = data.get('keepers_queried', [])
                    
                    # 显示数据来自哪个keeper节点
                    if 'keepers_queried' in data:
                        print(f"Data queried from keeper nodes: {keepers_queried}")
                    else:
                        print(f"Data from keeper node: {keeper_id}")
                    
                    # 检查是否使用新的数据结构（包含datasets字段）
                    if 'datasets' in data:
                        datasets = data.get('datasets', [])
                        print(f"Found {count} records in {len(datasets)} datasets for the date: {date}\n")
                        
                        # 数据源摘要
                        sources = {}
                        for ds in datasets:
                            source = ds.get('source_file', 'unknown')
                            records = len(ds.get('data', []))
                            if source in sources:
                                sources[source] += records
                            else:
                                sources[source] = records
                        
                        print("Data sources summary:")
                        for source, count in sources.items():
                            print(f"  - {source}: {count} records")
                        print()
                        
                        # 遍历每个数据集
                        for dataset_idx, dataset in enumerate(datasets):
                            records = dataset.get('data', [])
                            column_names = dataset.get('column_names', [])
                            source_file = dataset.get('source_file', 'unknown')
                            ds_keeper_id = dataset.get('keeper_id', keeper_id)
                            
                            print(f"\n----- Dataset #{dataset_idx + 1} from {source_file} (keeper {ds_keeper_id}) -----")
                            print(f"Records count: {len(records)}")
                            
                            if column_names and len(column_names) > 0:
                                print("\nHeader:")
                                print("-" * 100)
                                for i in range(0, len(column_names), 5):
                                    chunk = column_names[i:i+5]
                                    print(", ".join(chunk))
                                print("-" * 100)
                            
                            # 对于大数据集，限制显示的记录数
                            max_records_to_show = 10
                            records_to_display = records[:max_records_to_show] if len(records) > max_records_to_show else records
                            
                            for record_idx, record in enumerate(records_to_display):
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
                            
                            # 显示记录被截断的提示
                            if len(records) > max_records_to_show:
                                print(f"\n... and {len(records) - max_records_to_show} more records (showing first {max_records_to_show} only)")
                            
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
                    print(f"\n===== Error loading file =====")
                    print(f"Failed to load file: {error}")
                    print("===== End of error message =====\n")
            
            elif msg_type == 'TEMP_RANGE_RESULT':
                # Let display_temp_range_result handle the formatting
                # No need to duplicate output here as it's already handled by the callback
                pass
                
            elif msg_type == 'TEMP_RANGE_AVG_RESULT':
                # Let display_temp_range_avg_result handle the formatting
                # No need to duplicate output here as it's already handled by the callback
                pass
            
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
        elif parts and parts[0].upper() == 'TEMP_RANGE':
            return self.do_temp_range(' '.join(parts[1:]))
        elif parts and parts[0].upper() == 'TEMP_RANGE_AVG':
            return self.do_temp_range_avg(' '.join(parts[1:]))
        else:
            print(f"Unknown command: {line}")
            print("Available commands: LOAD [filename], GET [date], TEMP_RANGE [date1] [date2], TEMP_RANGE_AVG [date1] [date2], exit")

    def emptyline(self):
        pass

    def do_temp_range(self, arg):
        """Display temperature data for a date range: TEMP_RANGE start_date end_date"""
        parts = arg.split()
        if len(parts) != 2:
            print("Please provide start_date and end_date. Format: TEMP_RANGE DD-MM-YYYY DD-MM-YYYY")
            return
            
        start_date, end_date = parts
        
        # Validate date formats
        try:
            self.validate_date_format(start_date)
            self.validate_date_format(end_date)
        except ValueError as e:
            print(f"Date format error: {e}")
            return
            
        print(f"\n===== Getting temperature data for range: {start_date} to {end_date} =====")
        self.last_response = None
        
        try:
            if self.connection is None or self.connection.is_closed:
                print("Connection closed, trying to reconnect...")
                self.init_rabbitmq()
                
            # Send TEMP_RANGE request to manager
            self.send_to_manager(create_message('TEMP_RANGE', {
                'start_date': start_date,
                'end_date': end_date
            }))
            
            print("Waiting for response...")
            for i in range(30):
                if i % 5 == 0 and i > 0:
                    print(f"Still waiting for response... {i}/30")
                    
                try:
                    self.connection.process_data_events(time_limit=0.5)
                    if self.last_response and self.last_response.get('type') == 'TEMP_RANGE_RESULT':
                        self.display_temp_range_result(self.last_response.get('data', {}))
                        break
                except pika.exceptions.AMQPError as e:
                    print(f"Error processing event: {e}")
                    try:
                        self.init_rabbitmq()
                    except Exception as conn_err:
                        print(f"Failed to reconnect: {conn_err}")
                        break
            
            if not self.last_response or self.last_response.get('type') != 'TEMP_RANGE_RESULT':
                print("\n===== No response or incorrect response type received =====")
                
        except Exception as e:
            print(f"Error processing temp_range request: {e}")
            traceback.print_exc()
            
        print("\n===== TEMP_RANGE request completed =====")
        
    def do_temp_range_avg(self, arg):
        """Display average temperature for a date range: TEMP_RANGE_AVG start_date end_date"""
        parts = arg.split()
        if len(parts) != 2:
            print("Please provide start_date and end_date. Format: TEMP_RANGE_AVG DD-MM-YYYY DD-MM-YYYY")
            return
            
        start_date, end_date = parts
        
        # Validate date formats
        try:
            self.validate_date_format(start_date)
            self.validate_date_format(end_date)
        except ValueError as e:
            print(f"Date format error: {e}")
            return
            
        print(f"\n===== Getting average temperature for range: {start_date} to {end_date} =====")
        self.last_response = None
        
        try:
            if self.connection is None or self.connection.is_closed:
                print("Connection closed, trying to reconnect...")
                self.init_rabbitmq()
                
            # Send TEMP_RANGE_AVG request to manager
            self.send_to_manager(create_message('TEMP_RANGE_AVG', {
                'start_date': start_date,
                'end_date': end_date
            }))
            
            print("Waiting for response...")
            for i in range(30):
                if i % 5 == 0 and i > 0:
                    print(f"Still waiting for response... {i}/30")
                    
                try:
                    self.connection.process_data_events(time_limit=0.5)
                    if self.last_response and self.last_response.get('type') == 'TEMP_RANGE_AVG_RESULT':
                        self.display_temp_range_avg_result(self.last_response.get('data', {}))
                        break
                except pika.exceptions.AMQPError as e:
                    print(f"Error processing event: {e}")
                    try:
                        self.init_rabbitmq()
                    except Exception as conn_err:
                        print(f"Failed to reconnect: {conn_err}")
                        break
            
            if not self.last_response or self.last_response.get('type') != 'TEMP_RANGE_AVG_RESULT':
                print("\n===== No response or incorrect response type received =====")
                
        except Exception as e:
            print(f"Error processing temp_range_avg request: {e}")
            traceback.print_exc()
            
        print("\n===== TEMP_RANGE_AVG request completed =====")
    
    def validate_date_format(self, date_str):
        """Validate date format - accepts DD-MM-YYYY, YYYY-MM-DD, YYYYMMDD"""
        if re.match(r'^\d{2}-\d{2}-\d{4}$', date_str):
            day, month, year = map(int, date_str.split('-'))
            if not is_valid_date(year, month, day):
                raise ValueError(f"Invalid date: {date_str}")
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            year, month, day = map(int, date_str.split('-'))
            if not is_valid_date(year, month, day):
                raise ValueError(f"Invalid date: {date_str}")
        elif re.match(r'^\d{8}$', date_str):
            year = int(date_str[0:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            if not is_valid_date(year, month, day):
                raise ValueError(f"Invalid date: {date_str}")
        else:
            raise ValueError("Unrecognized date format, supported formats: DD-MM-YYYY, YYYY-MM-DD, YYYYMMDD")
    
    def display_temp_range_result(self, data):
        """Display temperature range results in a formatted way"""
        if not data.get('success', False):
            error = data.get('error', 'Unknown error')
            print(f"\nError retrieving temperature range data: {error}")
            return
            
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        daily_temps = data.get('daily_data', {})
        min_temp = data.get('min')
        max_temp = data.get('max')
        count = data.get('count', 0)
        
        print(f"\n===== Temperature data from {start_date} to {end_date} =====")
        print(f"Days with temperature data: {count}")
        
        if count > 0:
            print(f"Temperature range: {min_temp:.2f}°C to {max_temp:.2f}°C")
            
            print("\nDaily temperatures:")
            print("-" * 50)
            print(f"{'Date':<12} | {'Temperature (°C)':<15}")
            print("-" * 50)
            
            for date, temp in sorted(daily_temps.items()):
                print(f"{date:<12} | {temp:.2f}°C")
                
            print("-" * 50)
        else:
            print("No temperature data available for this date range.")
    
    def display_temp_range_avg_result(self, data):
        """Display temperature range average results in a formatted way"""
        if not data.get('success', False):
            error = data.get('error', 'Unknown error')
            print(f"\nError retrieving average temperature: {error}")
            return
            
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        avg_temp = data.get('avg_temperature')
        days_with_data = data.get('days_with_data', 0)
        total_days = data.get('total_days', 0)
        
        print(f"\n===== Average Temperature from {start_date} to {end_date} =====")
        print(f"Days with data: {days_with_data} out of {total_days} days")
        
        if days_with_data > 0:
            print(f"Average temperature: {avg_temp:.2f}°C")
        else:
            print("No temperature data available for this date range.")

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