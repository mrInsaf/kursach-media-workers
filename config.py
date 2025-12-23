# config.py
import os

# Настройки Redis
REDIS_HOST = "10.0.1.4"
REDIS_PORT = 6379
REDIS_PASSWORD = "4982a1bba0b00a6f0bea0ca36a0febe8"
REDIS_QUEUE_NAME = "media_tasks"

# Настройки MinIO
MINIO_ENDPOINT = "10.0.1.3:9000"
MINIO_ACCESS_KEY = "7f3fc78338969edd"
MINIO_SECRET_KEY = "0d8ab1eb0a01ea0321586ab431bf66cc"
MINIO_BUCKET = "media-processing"

MAX_PROCESSING_TIME = 1800  # 30 минут максимум на задачу
