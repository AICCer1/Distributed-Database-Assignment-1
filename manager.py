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

class Manager:
    def __init__(self, num_keepers=3):
        self.num_keepers = num_keepers
        self.keepers = []
        self.connection = None
        self.channel = None
        self.data_index = {}
        self.hash_ring = ConsistentHashRing()

        self.init_rabbitmq()
        
        self.start_keepers()
        
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
            keeper_process = subprocess.Popen(
                [python_executable, 'keeper.py', str(i), str(self.num_keepers)]
            )
            self.keepers.append(keeper_process)
            self.hash_ring.add_node(f'keeper_{i}')
            print(f"Starting storage device {i}")
            time.sleep(2)

    def remove_keeper(self, keeper_id):
        self.hash_ring.remove_node(f'keeper_{keeper_id}')
        if self.keepers[keeper_id]:
            self.keepers[keeper_id].terminate()
            self.keepers[keeper_id] = None
        print(f"Removed keeper {keeper_id} from the hash ring")

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
                    
                    for row in reader:
                        if len(row) > date_column:
                            records.append(row)
                except Exception as e:
                    print(f"Error reading CSV file: {e}")
                    self.send_to_client(create_message('LOAD_RESULT', 
                        {'success': False, 'error': f"Error reading CSV file: {e}"}))
                    return
            
            print(f"Read {len(records)} records")
            
            date_records = {}
            for record in records:
                date_str = record[date_column]
                
                try:
                    standard_date = self.standardize_date(date_str)
                    if standard_date:
                        if standard_date not in date_records:
                            date_records[standard_date] = {
                                'data': [],
                                'column_names': column_names
                            }
                        date_records[standard_date]['data'].append(record)
                        
                        alt_formats = self.generate_alternative_date_formats(standard_date)
                        for alt_date in alt_formats:
                            if alt_date not in date_records:
                                date_records[alt_date] = date_records[standard_date]
                except Exception as e:
                    print(f"Error processing date {date_str}: {e}")
                    continue
            
            print(f"Grouped by date, there are {len(date_records)} different dates")
            
            for date, date_data in date_records.items():
                keeper_id = get_keeper_id(date, self.num_keepers)
                print(f"Date {date} assigned to storage device {keeper_id}")
                
                if date not in self.data_index:
                    self.data_index[date] = []
                if keeper_id not in self.data_index[date]:
                    self.data_index[date].append(keeper_id)
                
                self.send_to_keeper(keeper_id, create_message('STORE', {
                    'date': date,
                    'data': date_data['data'],
                    'column_names': date_data['column_names']
                }))
            
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
            
            formats = [
                f"{day:02d}-{month:02d}-{year:04d}",
                f"{year:04d}{month:02d}{day:02d}"
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
                    'data': [],
                    'count': 0
                }
                self.send_to_client(create_message('GET_RESULT', empty_response))
                return
            
            print(f"Standardized date: {date_str}, original format: {date_str}")
            
            keeper_ids = []
            
            for date_format in [standard_date, date_str]:
                if date_format in self.data_index:
                    keeper_ids = self.data_index[date_format]
                    break
            
            if not keeper_ids:
                alt_formats = self.generate_alternative_date_formats(standard_date)
                for alt_date in alt_formats:
                    if alt_date in self.data_index:
                        keeper_ids = self.data_index[alt_date]
                        break
            
            if not keeper_ids:
                print(f"No data found for date: {date_str}")
                print(f"Dates in data index: {list(self.data_index.keys())[:10]}")
                
                empty_response = {
                    'date': date_str,
                    'found': False,
                    'data': [],
                    'count': 0
                }
                self.send_to_client(create_message('GET_RESULT', empty_response))
                return
            
            print(f"Found data for date: {date_str}, stored in keepers: {keeper_ids}")
            
            keeper_id = keeper_ids[0]
            print(f"Getting data from storage device {keeper_id}")
            
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
                temp_channel.stop_consuming()
            
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
            
            timeout_event = threading.Event()
            
            def timeout_thread():
                for _ in range(50):
                    if timeout_event.is_set():
                        return
                    time.sleep(0.1)
                
                if not response_received:
                    print("Request timed out")
                    try:
                        temp_channel.stop_consuming()
                    except:
                        pass
            
            timer_thread = threading.Thread(target=timeout_thread)
            timer_thread.daemon = True
            timer_thread.start()
            
            print("Starting to wait for response...")
            temp_channel.start_consuming()
            print("Finished waiting for response")
            
            timeout_event.set()
            
            temp_connection.close()
            
            if response_received and response_data:
                print(f"Successfully received response, forwarding to client")
                response_message = parse_message(response_data)
                response_type = response_message.get('type')
                response_content = response_message.get('data', {})
                
                if response_type == 'GET_RESULT' and response_content.get('found'):
                    data = response_content.get('data', [])
                    if data and isinstance(data[0], list) and isinstance(data[0][0], list):
                        flat_data = []
                        for group in data:
                            flat_data.extend(group)
                        response_content['data'] = flat_data
                
                self.send_to_client(create_message(response_type, response_content))
            else:
                print(f"No response received, sending empty response to client")
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
            
            error_response = {
                'date': date_str,
                'found': False,
                'data': [],
                'count': 0,
                'error': str(e)
            }
            self.send_to_client(create_message('GET_RESULT', error_response))

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
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("Manager is shutting down...")
            for keeper in self.keepers:
                if keeper:
                    keeper.terminate()
            self.connection.close()

if __name__ == "__main__":
    num_keepers = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    manager = Manager(num_keepers)
    manager.run()