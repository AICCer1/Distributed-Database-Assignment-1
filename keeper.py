#!/usr/bin/env python
import pika
import sys
import json
import time
import subprocess
import traceback
from utils import *
import os

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
        
        print(f"Storing data for date: {date}")
        self.data[date] = {
            'data': records,
            'column_names': column_names
        }
        self.send_to_replica(create_message('REPLICATE', {
            'date': date,
            'data': records,
            'column_names': column_names
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
                records = self.data[date].get('data', [])
                column_names = self.data[date].get('column_names', [])
                print(f"Found {len(records)} records")
                response = {
                    'date': original_date,
                    'found': True,
                    'data': records,
                    'count': len(records),
                    'column_names': column_names
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
                        'data': [],
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
                'data': [],
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
                records = self.data[date].get('data', [])
                column_names = self.data[date].get('column_names', [])
                print(f"Found {len(records)} records")
                response = {
                    'date': original_date,
                    'found': True,
                    'data': records,
                    'count': len(records),
                    'column_names': column_names
                }
            else:
                print(f"Date {date} data not found")
                response = {
                    'date': original_date,
                    'found': False,
                    'data': [],
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
                'data': [],
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
                records = self.data[date].get('data', [])
                column_names = self.data[date].get('column_names', [])
                print(f"Found {len(records)} records")
                response = {
                    'date': original_date,
                    'found': True,
                    'data': records,
                    'count': len(records),
                    'column_names': column_names
                }
            else:
                print(f"Date {date} data not found")
                response = {
                    'date': original_date,
                    'found': False,
                    'data': [],
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
                'data': [],
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

if __name__ == "__main__":
    print(f"Starting storage device process: {os.getpid()}")
    if len(sys.argv) < 3:
        print("Usage: python keeper.py <keeper_id> <num_keepers>")
        sys.exit(1)
    keeper_id = int(sys.argv[1])
    num_keepers = int(sys.argv[2])
    keeper = Keeper(keeper_id, num_keepers)
    keeper.run()