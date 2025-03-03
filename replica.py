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
        self.version = 0

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

    def update_data(self, data, version):
        if version > self.version:
            # 打印调试信息
            print(f"Updating data, new version: {version}")
            print(f"Received data structure: {type(data)}")
            if '2015-01-01' in data:
                print(f"Received data for 2015-01-01: {data['2015-01-01']}")
            
            # 确保 self.data 是字典
            if not isinstance(self.data, dict):
                self.data = {}
            
            # 合并数据，而不是替换
            for date, rows in data.items():
                if date not in self.data:
                    self.data[date] = []
                
                for row in rows:
                    # 将行转换为列表，确保一致性
                    row_list = list(row)
                    
                    # 检查是否已存在相同记录
                    row_exists = False
                    for existing_row in self.data[date]:
                        if existing_row == row_list:
                            row_exists = True
                            break
                    
                    # 只添加不存在的记录
                    if not row_exists:
                        self.data[date].append(row_list)
            
            # 打印调试信息
            if '2015-01-01' in self.data:
                print(f"After update, data for 2015-01-01: {self.data['2015-01-01']}")
            
            self.version = version
            return "Data updated successfully"
        else:
            return "Data is already up to date"

    def get_data(self, date):
        # 打印调试信息
        print(f"Replica get_data called for date: {date}")
        print(f"Replica data structure: {type(self.data)}")
        if date in self.data:
            print(f"Replica data for {date}: {self.data[date]}")
            return self.data[date]
        else:
            print(f"Replica: No data found for date: {date}")
            return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python replica.py <keeper_id>")
        sys.exit(1)

    keeper_id = int(sys.argv[1])
    replica = Replica(keeper_id)
    replica.run()