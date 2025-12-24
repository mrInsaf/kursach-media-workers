# utils/redis_client.py
import json

import redis
from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_QUEUE_NAME


class RedisClient:
    def __init__(self):
        self.redis = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True
        )

    def get_next_task(self):
        """Получить следующую задачу из очереди"""
        # Использует BLPOP для блокирующего ожидания задачи
        task_data = self.redis.blpop(REDIS_QUEUE_NAME, timeout=1)
        return json.loads(task_data[1]) if task_data else None

    def update_task_status(self, task_id, status, result_data=None):
        """Обновить статус задачи"""
        update_data = {"status": status}
        if result_data:
            update_data.update(result_data)
        self.redis.hset(f"task:{task_id}", mapping=update_data)