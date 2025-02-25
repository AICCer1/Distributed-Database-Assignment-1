#!/usr/bin/env python
import pika
import sys
import json
from utils import *
import traceback

class Replica:
    def __init__(self, keeper_id):
        self.keeper_id = keeper_id
        self.connection = None
        self.channel = None
        self.data = {}

        self.init_rabbitmq()

        print(f"Replica {keeper_id} has started")

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

            print(f"Replica received message: {msg_type}, data: {message}")

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
            else:
                print(f"Replica {self.keeper_id} received unknown type message: {msg_type}")
        except Exception as e:
            print(f"Error processing message in replica {self.keeper_id}: {str(e)}")

    def handle_store(self, data):
        date = data.get('date')
        record = data.get('data')

        if date not in self.data:
            self.data[date] = []
        self.data[date].append(record)

        print(f"Replica {self.keeper_id} stored data for date {date}")

    def handle_get(self, data):
        date = data.get('date')
        print(f"Replica querying date: {date}")

        if date in self.data:
            records = self.data[date].get('data', [])
            print(f"Found {len(records)} records")
            return {'found': True, 'data': records}
        else:
            print(f"Data for date {date} not found")
            return {'found': False, 'data': []}

    def handle_replicate(self, data):
        try:
            date = data.get('date')
            records = data.get('data', [])
            column_names = data.get('column_names', [])

            print(f"Replicating {len(records)} records for date {date}")

            self.data[date] = {
                'data': records,
                'column_names': column_names
            }

            print(f"Replica storage complete, date: {date}, record count: {len(records)}")

            return {'success': True}
        except Exception as e:
            print(f"Error processing REPLICATE request: {e}")
            return {'success': False, 'error': str(e)}

    def run(self):
        print(f"Replica {self.keeper_id} is running...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print(f"Replica {self.keeper_id} is shutting down...")
            self.connection.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python replica.py <keeper_id>")
        sys.exit(1)

    keeper_id = int(sys.argv[1])
    replica = Replica(keeper_id)
    replica.run()