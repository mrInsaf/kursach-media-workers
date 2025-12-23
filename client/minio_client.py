# utils/minio_client.py
from minio import Minio
from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
import os


class MinioClient:
    def __init__(self):
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        # Создаем бакет если не существует
        if not self.client.bucket_exists(MINIO_BUCKET):
            self.client.make_bucket(MINIO_BUCKET)

    def download_file(self, object_name, local_path=None):
        """Скачать файл из MinIO"""
        if local_path is None:
            local_path = f"/tmp/{os.path.basename(object_name)}"
        self.client.fget_object(MINIO_BUCKET, object_name, local_path)
        return local_path

    def upload_file(self, local_path, object_name=None):
        """Загрузить файл в MinIO"""
        if object_name is None:
            object_name = os.path.basename(local_path)
        self.client.fput_object(MINIO_BUCKET, object_name, local_path)
        # Возвращаем публичный URL (для внутренней сети)
        return f"http://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{object_name}"