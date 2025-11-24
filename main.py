from abc import ABC, abstractmethod
from typing import List, Dict, Any
import asyncio
import aiohttp
from oss2 import Bucket
import pika
import json
from datetime import datetime


class ImageParser(ABC):
    """图片解析器抽象类"""

    @abstractmethod
    async def search_images(self, keyword: str, max_count: int) -> List[str]:
        """搜索图片并返回图片URL列表"""
        pass

    @abstractmethod
    def get_source_site(self) -> str:
        """返回图片网站标识"""
        pass


class ImageDownloader:
    """图片下载器"""

    def __init__(self, oss_bucket: Bucket, db_connection, max_concurrent: int = 10):
        self.oss_bucket = oss_bucket
        self.db = db_connection
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.parsers: Dict[str, ImageParser] = {}

    def register_parser(self, parser: ImageParser):
        """注册图片解析器"""
        self.parsers[parser.get_source_site()] = parser

    async def download_task(self, task_message: Dict) -> Dict:
        """处理下载任务"""
        task_id = task_message['task_id']
        keyword = task_message['keyword']
        source_site = task_message['source_site']
        max_images = task_message.get('max_images', 100)

        # 更新任务状态为下载中
        self.update_task_status(task_id, 'downloading')

        try:
            # 获取对应的解析器
            parser = self.parsers.get(source_site)
            if not parser:
                raise ValueError(f"Unsupported source site: {source_site}")

            # 搜索图片
            image_urls = await parser.search_images(keyword, max_images)

            # 并发下载图片
            download_tasks = []
            for image_url in image_urls:
                task = self.download_single_image(task_id, image_url)
                download_tasks.append(task)

            results = await asyncio.gather(*download_tasks, return_exceptions=True)

            # 统计结果
            success_count = sum(1 for r in results if isinstance(r, dict) and r['status'] == 'success')
            failed_count = len(results) - success_count

            # 更新任务状态
            self.update_task_status(task_id, 'completed', success_count, failed_count)

            return {
                'task_id': task_id,
                'status': 'completed',
                'total_images': len(image_urls),
                'success_count': success_count,
                'failed_count': failed_count,
                'image_results': [r for r in results if isinstance(r, dict)]
            }

        except Exception as e:
            self.update_task_status(task_id, 'failed', error_message=str(e))
            raise

    async def download_single_image(self, task_id: str, image_url: str) -> Dict:
        """下载单张图片"""
        async with self.semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(image_url) as response:
                        if response.status == 200:
                            image_data = await response.read()

                            # 上传到OSS
                            file_name = self.generate_filename(image_url)
                            oss_path = f"images/{task_id}/{file_name}"

                            result = self.oss_bucket.put_object(oss_path, image_data)

                            if result.status == 200:
                                # 记录到数据库
                                image_info = await self.get_image_info(image_data)
                                self.save_image_record(
                                    task_id, image_url, oss_path,
                                    len(image_data), image_info
                                )

                                return {
                                    'image_url': image_url,
                                    'oss_url': oss_path,
                                    'file_size': len(image_data),
                                    'status': 'success'
                                }

                return {
                    'image_url': image_url,
                    'status': 'failed',
                    'error_message': f"HTTP {response.status}"
                }

            except Exception as e:
                return {
                    'image_url': image_url,
                    'status': 'failed',
                    'error_message': str(e)
                }

    def generate_filename(self, image_url: str) -> str:
        """生成文件名"""
        import hashlib
        import os

        ext = os.path.splitext(image_url)[1] or '.jpg'
        hash_name = hashlib.md5(image_url.encode()).hexdigest()
        return f"{hash_name}{ext}"

    async def get_image_info(self, image_data: bytes) -> Dict:
        """获取图片信息"""
        from PIL import Image
        import io

        try:
            image = Image.open(io.BytesIO(image_data))
            return {
                'width': image.width,
                'height': image.height,
                'format': image.format
            }
        except:
            return {'width': 0, 'height': 0, 'format': 'unknown'}

    def update_task_status(self, task_id: str, status: str,
                           success_count: int = 0, failed_count: int = 0,
                           error_message: str = ""):
        """更新任务状态到数据库"""
        # 实现数据库更新逻辑
        pass

    def save_image_record(self, task_id: str, image_url: str, oss_url: str,
                          file_size: int, image_info: Dict):
        """保存图片记录到数据库"""
        # 实现数据库插入逻辑
        pass


class TaskManager:
    """任务管理器"""

    def __init__(self, mq_host: str, downloader: ImageDownloader):
        self.mq_host = mq_host
        self.downloader = downloader
        self.task_queue = "image_download_tasks"
        self.result_queue = "task_results"

    def start_consuming(self):
        """开始消费任务"""
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.mq_host)
        )
        channel = connection.channel()

        # 声明队列
        channel.queue_declare(queue=self.task_queue, durable=True)
        channel.queue_declare(queue=self.result_queue, durable=True)

        def callback(ch, method, properties, body):
            try:
                task_message = json.loads(body)
                # 异步处理任务
                result = asyncio.run(self.downloader.download_task(task_message))

                # 发送结果回传
                channel.basic_publish(
                    exchange='',
                    routing_key=self.result_queue,
                    body=json.dumps(result),
                    properties=pika.BasicProperties(delivery_mode=2)
                )

                ch.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as e:
                # 处理失败，可以发送到死信队列
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        channel.basic_consume(queue=self.task_queue, on_message_callback=callback)
        channel.start_consuming()

    def submit_task(self, task_data: Dict) -> str:
        """提交新任务"""
        task_id = self.generate_task_id()
        task_data['task_id'] = task_id

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.mq_host)
        )
        channel = connection.channel()

        channel.basic_publish(
            exchange='',
            routing_key=self.task_queue,
            body=json.dumps(task_data),
            properties=pika.BasicProperties(delivery_mode=2)  # 持久化消息
        )
        connection.close()

        # 在数据库中创建任务记录
        self.create_task_record(task_data)

        return task_id

    def generate_task_id(self) -> str:
        """生成任务ID"""
        import uuid
        return str(uuid.uuid4())

    def create_task_record(self, task_data: Dict):
        """在数据库中创建任务记录"""
        # 实现数据库插入逻辑
        pass