#!/usr/bin/env python
import pika
import sys
import json
import cmd
from utils import *
import re
import datetime
import traceback

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
            
            print(f"\nReceived response: {msg_type}\n")
            
            self.last_response = message
            
            if msg_type == 'GET_RESULT':
                if data.get('found'):
                    records = data.get('data', [])
                    count = data.get('count', 0)
                    date = data.get('date', '')
                    column_names = data.get('column_names', [])
                    
                    print(f"Found {count} records for the date: {date}\n")
                    
                    if column_names:
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
                                if column_names and len(column_names) >= len(record):
                                    for i, (name, value) in enumerate(zip(column_names, record)):
                                        if i > 0 and i % 5 == 0:
                                            print()
                                        print(f"{name}: {value}", end="  ")
                                    print("\n")
                                else:
                                    for i in range(0, len(record), 5):
                                        chunk = record[i:i+5]
                                        formatted = ", ".join(f"[{i+j}]:{v}" for j, v in enumerate(chunk))
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
                    print(f"No data found for the date: {date}")
            
            elif msg_type == 'LOAD_RESULT':
                if data.get('success'):
                    filename = data.get('filename', '')
                    records = data.get('records', 0)
                    dates = data.get('dates', 0)
                    print(f"File loaded successfully: {filename}")
                    print(f"Total records: {records}")
                    # print(f"Different dates count: {dates}")
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
        self.send_to_manager(create_message('LOAD', arg))
        
        self.connection.process_data_events(time_limit=5)

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
            
        print(f"Getting data for date: {arg}...")
        self.last_response = None
        
        try:
            if self.connection is None or self.connection.is_closed:
                print("Connection closed, trying to reconnect...")
                self.init_rabbitmq()
                
            self.send_to_manager(create_message('GET', arg))
            
            print("Waiting for response...")
            for i in range(20):
                if i % 5 == 0:
                    print(f"Waiting for response... {i+1}/20")
                    
                try:
                    self.connection.process_data_events(time_limit=0.5)
                    if self.last_response and self.last_response.get('type') == 'GET_RESULT':
                        print(f"Received response: {self.last_response}")
                        break
                except pika.exceptions.AMQPError as e:
                    print(f"Error processing event: {e}")
                    try:
                        self.init_rabbitmq()
                    except Exception as conn_err:
                        print(f"Failed to reconnect: {conn_err}")
                        break
            
            if not self.last_response or self.last_response.get('type') != 'GET_RESULT':
                print("No response or incorrect response type received")
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