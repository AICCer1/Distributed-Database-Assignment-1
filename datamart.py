#!/usr/bin/env python
import pika
import sys
import json
import re
import datetime
import traceback
import argparse
from utils import *

class DataMart:
    def __init__(self, verbose=False):
        self.connection = None
        self.channel = None
        self.temperature_data = {}  # 格式: {日期: 平均温度}
        self.verbose = verbose
        
        self.init_rabbitmq()
        self.log(f"Data Mart has started", True)
        
    def log(self, message, force=False):
        """根据verbose设置打印日志"""
        if self.verbose or force:
            print(message)
            
    def init_rabbitmq(self):
        """初始化RabbitMQ连接和队列"""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        
        # 声明数据集市队列
        self.channel.queue_declare(queue=DATAMART_QUEUE)
        
        # 监听来自Manager的消息
        self.channel.basic_consume(
            queue=DATAMART_QUEUE,
            on_message_callback=self.process_message,
            auto_ack=True
        )
        
        self.log("DataMart initialized RabbitMQ connection")
        
    def process_message(self, ch, method, properties, body):
        """处理接收到的消息"""
        try:
            message = parse_message(body)
            msg_type = message.get('type')
            data = message.get('data')
            
            self.log(f"DataMart received message: {msg_type}")
            
            if msg_type == 'NEW_DATA':
                self.process_new_data(data)
            elif msg_type == 'TEMP_RANGE':
                self.handle_temp_range(data, None)
            elif msg_type == 'TEMP_RANGE_AVG':
                self.handle_temp_range_avg(data, None)
            else:
                self.log(f"Unknown message type: {msg_type}")
                
        except Exception as e:
            self.log(f"Error processing message: {str(e)}", True)
            traceback.print_exc()
            
    def standardize_date(self, date_str):
        """将各种格式的日期字符串转换为标准格式 YYYY-MM-DD"""
        try:
            # DD-MM-YYYY
            if re.match(r'^\d{2}-\d{2}-\d{4}$', date_str):
                day, month, year = map(int, date_str.split('-'))
                return f"{year:04d}-{month:02d}-{day:02d}"
                
            # YYYY-MM-DD
            elif re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                return date_str  # 已经是标准格式
                
            # YYYYMMDD
            elif re.match(r'^\d{8}$', date_str):
                year = int(date_str[0:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                return f"{year:04d}-{month:02d}-{day:02d}"
                
            # YYYYMMDD-HH:MM (处理带时间的日期)
            elif re.match(r'^\d{8}-\d{2}:\d{2}$', date_str):
                date_part = date_str.split('-')[0]
                year = int(date_part[0:4])
                month = int(date_part[4:6])
                day = int(date_part[6:8])
                return f"{year:04d}-{month:02d}-{day:02d}"
                
            else:
                raise ValueError(f"Unrecognized date format: {date_str}")
                
        except (ValueError, IndexError) as e:
            raise ValueError(f"Error standardizing date {date_str}: {str(e)}")
    
    def format_date_display(self, date_str):
        """将YYYY-MM-DD格式的日期转换为DD-MM-YYYY格式用于显示"""
        try:
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                year, month, day = map(int, date_str.split('-'))
                return f"{day:02d}-{month:02d}-{year:04d}"
            return date_str
        except Exception as e:
            self.log(f"Error formatting date for display: {str(e)}")
            return date_str
            
    def extract_temperature(self, record, column_names, file_format=None):
        """从不同格式的数据记录中提取温度信息"""
        # 如果没有列名，无法提取温度
        if not column_names:
            return None
            
        # 尝试识别温度字段
        temp_value = None
        
        # 根据不同的文件格式提取温度
        if file_format == "weather.csv":
            # 第一种格式: 使用 "Data.Temperature.Avg Temp" 字段
            avg_temp_idx = None
            for i, col in enumerate(column_names):
                if col == "Data.Temperature.Avg Temp":
                    avg_temp_idx = i
                    break
                    
            if avg_temp_idx is not None and avg_temp_idx < len(record):
                try:
                    temp_value = float(record[avg_temp_idx])
                except (ValueError, TypeError):
                    pass
                    
        elif "_tempm" in column_names:
            # 第二种格式: 使用 "_tempm" 字段 (testset.csv格式)
            tempm_idx = column_names.index("_tempm")
            if tempm_idx < len(record):
                try:
                    temp_value = float(record[tempm_idx])
                except (ValueError, TypeError):
                    pass
                    
        else:
            # 通用方法: 尝试查找包含温度关键词的列
            # 优先查找Data.Temperature.Avg Temp (美国气象站数据格式)
            data_temp_avg_idx = None
            for i, col in enumerate(column_names):
                if col == "Data.Temperature.Avg Temp":
                    data_temp_avg_idx = i
                    break
                
            if data_temp_avg_idx is not None and data_temp_avg_idx < len(record):
                try:
                    temp_value = float(record[data_temp_avg_idx])
                    return temp_value  # 找到后直接返回
                except (ValueError, TypeError):
                    pass
            
            # 如果没找到特定字段，使用通用方法
            temp_columns = []
            avg_temp_columns = []
            min_temp_columns = []
            max_temp_columns = []
            
            # 查找温度相关列
            for i, col in enumerate(column_names):
                col_lower = col.lower()
                
                # 优先查找平均温度
                if 'temp_mean' in col_lower or 'avg temp' in col_lower or 'avg_temp' in col_lower:
                    avg_temp_columns.append(i)
                # 查找最低温度
                elif 'temp_min' in col_lower or 'min temp' in col_lower or 'min_temp' in col_lower:
                    min_temp_columns.append(i)
                # 查找最高温度
                elif 'temp_max' in col_lower or 'max temp' in col_lower or 'max_temp' in col_lower:
                    max_temp_columns.append(i)
                # 查找一般温度字段
                elif 'temp' in col_lower and 'precipitation' not in col_lower:
                    temp_columns.append(i)
                    
            # 优先使用平均温度
            if avg_temp_columns:
                temps = []
                for idx in avg_temp_columns:
                    if idx < len(record):
                        try:
                            temps.append(float(record[idx]))
                        except (ValueError, TypeError):
                            pass
                if temps:
                    temp_value = sum(temps) / len(temps)
                    
            # 如果没有平均温度，计算最高和最低温度的平均值
            elif min_temp_columns and max_temp_columns:
                min_temps = []
                max_temps = []
                
                for idx in min_temp_columns:
                    if idx < len(record):
                        try:
                            min_temps.append(float(record[idx]))
                        except (ValueError, TypeError):
                            pass
                            
                for idx in max_temp_columns:
                    if idx < len(record):
                        try:
                            max_temps.append(float(record[idx]))
                        except (ValueError, TypeError):
                            pass
                            
                if min_temps and max_temps:
                    min_avg = sum(min_temps) / len(min_temps)
                    max_avg = sum(max_temps) / len(max_temps)
                    temp_value = (min_avg + max_avg) / 2
                    
            # 如果上述方法都失败，尝试使用一般温度字段
            elif temp_columns:
                temps = []
                for idx in temp_columns:
                    if idx < len(record):
                        try:
                            temps.append(float(record[idx]))
                        except (ValueError, TypeError):
                            pass
                if temps:
                    temp_value = sum(temps) / len(temps)
                    
        return temp_value
        
    def process_new_data(self, data):
        """处理新加载的数据，提取温度信息并更新数据集市"""
        try:
            date = data.get('date')
            records = data.get('data', [])
            column_names = data.get('column_names', [])
            source_file = data.get('source_file', 'unknown')
            
            if not date or not records:
                self.log(f"Invalid data format: missing date or records")
                return
                
            # 标准化日期格式
            try:
                standard_date = self.standardize_date(date)
            except ValueError as e:
                self.log(f"Error standardizing date: {e}")
                return
                
            self.log(f"Processing new data for date: {standard_date} from {source_file}")
            
            # 提取温度数据
            temperatures = []
            
            # 检查是否是数据集格式
            if isinstance(records, dict) and 'datasets' in records:
                datasets = records['datasets']
                for dataset in datasets:
                    dataset_records = dataset.get('data', [])
                    dataset_columns = dataset.get('column_names', [])
                    dataset_source = dataset.get('source_file', source_file)
                    
                    for record in dataset_records:
                        temp = self.extract_temperature(record, dataset_columns, dataset_source)
                        if temp is not None:
                            temperatures.append(temp)
            else:
                # 单一数据集
                for record in records:
                    temp = self.extract_temperature(record, column_names, source_file)
                    if temp is not None:
                        temperatures.append(temp)
                        
            # 计算平均温度
            if temperatures:
                avg_temp = sum(temperatures) / len(temperatures)
                
                # 更新数据集市
                if standard_date in self.temperature_data:
                    # 如果日期已存在，更新为新旧数据的平均值
                    old_temp = self.temperature_data[standard_date]
                    old_count = self.temperature_data.get(f"{standard_date}_count", 1)
                    new_count = old_count + len(temperatures)
                    
                    # 加权平均
                    weighted_avg = (old_temp * old_count + avg_temp * len(temperatures)) / new_count
                    
                    self.temperature_data[standard_date] = weighted_avg
                    self.temperature_data[f"{standard_date}_count"] = new_count
                else:
                    # 如果是新日期，直接添加
                    self.temperature_data[standard_date] = avg_temp
                    self.temperature_data[f"{standard_date}_count"] = len(temperatures)
                    
                self.log(f"Updated temperature for {standard_date}: {self.temperature_data[standard_date]:.2f}°C (from {len(temperatures)} records)")
            else:
                self.log(f"No temperature data found for date: {standard_date}")
                
        except Exception as e:
            self.log(f"Error processing new data: {str(e)}", True)
            traceback.print_exc()
            
    def get_date_range(self, start_date_str, end_date_str):
        """获取两个日期之间的所有日期列表"""
        try:
            # 标准化日期格式
            start_date_std = self.standardize_date(start_date_str)
            end_date_std = self.standardize_date(end_date_str)
            
            # 转换为datetime对象
            start_date = datetime.datetime.strptime(start_date_std, "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime(end_date_std, "%Y-%m-%d").date()
            
            # 确保开始日期不晚于结束日期
            if start_date > end_date:
                start_date, end_date = end_date, start_date
                
            # 生成日期范围
            date_range = []
            current_date = start_date
            while current_date <= end_date:
                date_range.append(current_date.strftime("%Y-%m-%d"))
                current_date += datetime.timedelta(days=1)
                
            return date_range
            
        except ValueError as e:
            raise ValueError(f"Error processing date range: {str(e)}")
            
    def handle_temp_range(self, data, properties):
        """处理温度范围查询请求"""
        try:
            start_date = data.get('start_date')
            end_date = data.get('end_date')
            
            if not start_date or not end_date:
                response = {
                    'success': False,
                    'error': 'Missing start_date or end_date'
                }
            else:
                date_range = self.get_date_range(start_date, end_date)
                
                # 收集日期范围内的温度数据
                daily_temps = {}
                temp_values = []
                for date in date_range:
                    if date in self.temperature_data:
                        # 使用标准格式的日期作为键
                        display_date = self.format_date_display(date)
                        daily_temps[display_date] = self.temperature_data[date]
                        temp_values.append(self.temperature_data[date])
                
                # 计算统计数据（最低、最高温度）
                min_temp = None
                max_temp = None
                if temp_values:
                    min_temp = min(temp_values)
                    max_temp = max(temp_values)
                
                # 准备响应数据，包含每天的温度和统计数据
                response = {
                    'success': True,
                    'start_date': self.format_date_display(self.standardize_date(start_date)),
                    'end_date': self.format_date_display(self.standardize_date(end_date)),
                    'daily_data': daily_temps,  # 每天的温度数据
                    'min': min_temp,            # 保留最低温度，方便客户端展示
                    'max': max_temp,            # 保留最高温度，方便客户端展示
                    'count': len(temp_values)   # 有温度数据的天数
                }
                
                self.log(f"Temperature data for {start_date} to {end_date}: {len(daily_temps)} days, min={min_temp}, max={max_temp}")
                
            # 发送响应 - 直接发送到客户端队列，因为Manager只是转发请求
            self.channel.basic_publish(
                exchange='',
                routing_key=CLIENT_QUEUE,
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id if properties and properties.correlation_id else None
                ),
                body=create_message('TEMP_RANGE_RESULT', response)
            )
            self.log(f"Sent temperature range response to client")
                
        except Exception as e:
            self.log(f"Error handling temp_range request: {str(e)}", True)
            traceback.print_exc()
            
            # 发送错误响应
            self.channel.basic_publish(
                exchange='',
                routing_key=CLIENT_QUEUE,
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id if properties and properties.correlation_id else None
                ),
                body=create_message('TEMP_RANGE_RESULT', {
                    'success': False,
                    'error': str(e)
                })
            )
                
    def handle_temp_range_avg(self, data, properties):
        """处理温度范围平均值查询请求"""
        try:
            start_date = data.get('start_date')
            end_date = data.get('end_date')
            
            if not start_date or not end_date:
                response = {
                    'success': False,
                    'error': 'Missing start_date or end_date'
                }
            else:
                date_range = self.get_date_range(start_date, end_date)
                
                # 收集日期范围内的温度数据
                temperatures = []
                for date in date_range:
                    if date in self.temperature_data:
                        temperatures.append(self.temperature_data[date])
                        
                if temperatures:
                    avg_temp = sum(temperatures) / len(temperatures)
                    response = {
                        'success': True,
                        'start_date': self.format_date_display(self.standardize_date(start_date)),
                        'end_date': self.format_date_display(self.standardize_date(end_date)),
                        'avg_temperature': avg_temp,
                        'days_with_data': len(temperatures),
                        'total_days': len(date_range)
                    }
                else:
                    response = {
                        'success': False,
                        'error': f'No temperature data available for the date range {start_date} to {end_date}'
                    }
                    
            # 发送响应 - 直接发送到客户端队列，因为Manager只是转发请求
            self.channel.basic_publish(
                exchange='',
                routing_key=CLIENT_QUEUE,
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id if properties and properties.correlation_id else None
                ),
                body=create_message('TEMP_RANGE_AVG_RESULT', response)
            )
            self.log(f"Sent temperature range average response to client")
                
        except Exception as e:
            self.log(f"Error handling temp_range_avg request: {str(e)}", True)
            traceback.print_exc()
            
            # 发送错误响应
            self.channel.basic_publish(
                exchange='',
                routing_key=CLIENT_QUEUE,
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id if properties and properties.correlation_id else None
                ),
                body=create_message('TEMP_RANGE_AVG_RESULT', {
                    'success': False,
                    'error': str(e)
                })
            )
                
    def start(self):
        """启动数据集市服务"""
        print("DataMart is running. Press CTRL+C to exit.")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("DataMart shutting down...")
            self.connection.close()
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start the data mart service')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--quiet', '-q', action='store_true', help='Disable verbose logging')
    
    args = parser.parse_args()
    verbose = args.verbose and not args.quiet
    
    datamart = DataMart(verbose=verbose)
    datamart.start()
