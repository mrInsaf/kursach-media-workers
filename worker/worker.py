#!/usr/bin/env python3
"""
Media Worker - основное приложение для обработки видео задач
Получает задачи из Redis, обрабатывает видео через FFmpeg и MediaInfo,
сохраняет результаты в MinIO и обновляет статус в Redis
"""

import json
import logging
import os
import signal
import sys
import time

from client.minio_client import MinioClient
from client.redis_client import RedisClient
# Импорт конфигурации и компонентов
from config import (
    REDIS_HOST, REDIS_PORT, MINIO_ENDPOINT, MINIO_BUCKET,
    MAX_PROCESSING_TIME
)
from logger.logger import setup_logger
from processors.ffmpeg_processor import FFmpegProcessor
from processors.mediainfo_processor import MediaInfoProcessor


class MediaWorker:
    """Основной класс воркера для обработки медиа задач"""

    def __init__(self):
        """Инициализация всех компонентов приложения"""
        # Настройка логгера
        self.logger = setup_logger()
        self.logger.info("🚀 Запуск Media Worker")
        self.logger.info(f"🔧 Версия: 1.0 (стабильная)")
        self.logger.info(f"🌐 Redis: {REDIS_HOST}:{REDIS_PORT}")
        self.logger.info(f"☁️ MinIO: {MINIO_ENDPOINT}/{MINIO_BUCKET}")

        # Инициализация клиентов
        try:
            self.redis_client = RedisClient()
            self.minio_client = MinioClient()
            self.logger.info("✅ Все клиенты успешно инициализированы")
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка инициализации: {str(e)}")
            sys.exit(1)

        # Инициализация процессоров
        self.ffmpeg_processor = FFmpegProcessor()
        self.mediainfo_processor = MediaInfoProcessor()
        self.logger.info("✅ Процессоры инициализированы")

        # Флаг для graceful shutdown
        self.running = True
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

        # Временная директория для обработки
        self.temp_dir = "/var/lib/media-worker/tmp"
        os.makedirs(self.temp_dir, exist_ok=True)
        self.logger.info(f"📁 Временная директория: {self.temp_dir}")

    def handle_shutdown(self, signum, frame):
        """Обработка сигналов завершения для graceful shutdown"""
        self.logger.info("🛑 Получен сигнал завершения. Выполняется graceful shutdown...")
        self.running = False

    def cleanup_temp_files(self, file_paths):
        """Очистка временных файлов"""
        for path in file_paths:
            try:
                if os.path.exists(path):
                    os.remove(path)
                    self.logger.debug(f"🧹 Удален временный файл: {path}")
            except Exception as e:
                self.logger.warning(f"⚠️ Не удалось удалить {path}: {str(e)}")

    def process_task(self, task):
        """
        Обработка одной задачи

        Args:
            task (dict): Задача из Redis в формате:
                {
                    "id": "task_123",
                    "source_path": "videos/input.mp4",
                    "thumbnail_time": "00:00:05",
                    "quality": "720p",
                    "output_prefix": "processed_123"
                }
        """
        task_id = task.get('id', 'unknown')
        self.logger.info(f"📋 Начало обработки задачи: {task_id}")

        # Список для очистки временных файлов
        temp_files = []

        try:
            start_time = time.time()

            # 1. Скачиваем исходное видео из MinIO
            self.logger.info(f"⬇️ Скачивание файла: {task['source_path']}")
            video_path = self.minio_client.download_file(
                object_name=task['source_path'],
                local_path=os.path.join(self.temp_dir, f"input_{task_id}.mp4")
            )
            temp_files.append(video_path)
            self.logger.info(f"✅ Файл скачан: {video_path}")

            # 2. Генерация превью
            self.logger.info(f"📸 Генерация превью для времени: {task['thumbnail_time']}")
            thumbnail_result = self.ffmpeg_processor.generate_thumbnail(
                video_path=video_path,
                timestamp=task['thumbnail_time'],
                width=640
            )

            if 'error' in thumbnail_result:
                raise RuntimeError(f"Ошибка генерации превью: {thumbnail_result['error']}")

            thumbnail_path = thumbnail_result['path']
            temp_files.append(thumbnail_path)
            self.logger.info(f"✅ Превью создано: {thumbnail_path}")
            self.logger.info(f"   Размер: {thumbnail_result['size_mb']} MB")

            # 3. Транскодирование в HLS
            self.logger.info(f"🌐 Транскодирование в HLS ({task['quality']})")
            hls_result = self.ffmpeg_processor.transcode_to_hls(
                video_path=video_path,
                quality=task['quality']
            )

            if 'error' in hls_result:
                raise RuntimeError(f"Ошибка транскодирования: {hls_result['error']}")

            master_playlist = hls_result['master_playlist']
            hls_dir = os.path.dirname(master_playlist)

            # Собираем все сегменты HLS для очистки
            for f in os.listdir(hls_dir):
                if f.endswith('.ts') or f.endswith('.m3u8'):
                    temp_files.append(os.path.join(hls_dir, f))

            self.logger.info(f"✅ HLS создан: {master_playlist}")
            self.logger.info(f"   Сегментов: {hls_result['segments_count']}")
            self.logger.info(f"   Общий размер: {hls_result['total_size_mb']} MB")

            # 4. Анализ метаданных
            self.logger.info("📊 Анализ метаданных видео")
            metadata = self.mediainfo_processor.analyze_video(video_path)
            self.logger.info(f"✅ Метаданные получены: {metadata['filename']}")

            # 5. Загрузка результатов в MinIO
            self.logger.info("⬆️ Загрузка результатов в MinIO")

            # Загружаем превью
            thumbnail_name = f"{task['output_prefix']}_thumbnail.jpg"
            thumbnail_url = self.minio_client.upload_file(
                local_path=thumbnail_path,
                object_name=thumbnail_name
            )
            self.logger.info(f"✅ Превью загружено: {thumbnail_url}")

            # Загружаем HLS сегменты и плейлист
            hls_files = []
            for f in os.listdir(hls_dir):
                if f.endswith('.ts') or f.endswith('.m3u8'):
                    local_path = os.path.join(hls_dir, f)
                    object_name = f"{task['output_prefix']}_hls/{f}"
                    hls_files.append({
                        'local_path': local_path,
                        'object_name': object_name
                    })

            hls_urls = []
            for file_info in hls_files:
                url = self.minio_client.upload_file(
                    local_path=file_info['local_path'],
                    object_name=file_info['object_name']
                )
                hls_urls.append(url)
                self.logger.debug(f"✅ HLS файл загружен: {file_info['object_name']}")

            master_playlist_url = next((url for url in hls_urls if 'master.m3u8' in url), None)

            # 6. Формируем результаты для обновления статуса
            processing_time = time.time() - start_time
            result_data = {
                "status": "completed",
                "thumbnail_url": thumbnail_url,
                "master_playlist_url": master_playlist_url,
                "hls_segments_count": hls_result['segments_count'],
                "total_size_mb": hls_result['total_size_mb'] + thumbnail_result['size_mb'],
                "metadata": metadata,
                "processing_time_sec": round(processing_time, 2),
                "completed_at": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            # 7. Обновляем статус в Redis
            self.redis_client.update_task_status(task_id, "completed", result_data)
            self.logger.info(f"✅ Задача {task_id} успешно завершена")
            self.logger.info(f"   Время обработки: {processing_time:.2f} сек")
            self.logger.info(f"   Результаты: {json.dumps(result_data, indent=2)}")

            return True

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"❌ Ошибка обработки задачи {task_id}: {error_msg}")

            # Пытаемся обновить статус даже при ошибке
            try:
                error_data = {
                    "status": "failed",
                    "error": error_msg,
                    "error_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "attempted_files": temp_files
                }
                self.redis_client.update_task_status(task_id, "failed", error_data)
                self.logger.info(f"✅ Статус ошибки обновлен для задачи {task_id}")
            except Exception as update_error:
                self.logger.error(f"❌ Не удалось обновить статус: {str(update_error)}")

            return False

        finally:
            # Всегда очищаем временные файлы
            self.logger.info("🧹 Очистка временных файлов")
            self.cleanup_temp_files(temp_files)

    def run(self):
        """Основной цикл работы воркера"""
        self.logger.info("🔄 Media Worker запущен и готов принимать задачи")
        self.logger.info(f"⏱️ Максимальное время обработки: {MAX_PROCESSING_TIME} сек")
        self.logger.info("🔍 Ожидание задач из очереди...")

        last_activity = time.time()

        while self.running:
            try:
                # Проверяем активность (для логирования)
                current_time = time.time()
                if current_time - last_activity > 60:  # Каждую минуту
                    self.logger.info(f"💤 Ожидаю задачу... (активность: {time.strftime('%H:%M:%S')})")
                    last_activity = current_time

                # Получаем задачу из Redis
                task = self.redis_client.get_next_task()

                if task:
                    last_activity = time.time()
                    self.logger.info(f"🎯 Получена задача: {task.get('id', 'unknown')}")

                    # Обрабатываем задачу
                    success = self.process_task(task)

                    if success:
                        self.logger.info("✅ Задача обработана успешно")
                    else:
                        self.logger.warning("⚠️ Задача завершилась с ошибкой")
                else:
                    # Небольшая пауза при отсутствии задач
                    time.sleep(1)

            except Exception as e:
                self.logger.error(f"❌ Критическая ошибка в основном цикле: {str(e)}")
                time.sleep(5)  # Пауза перед повторной попыткой

            # Короткая пауза для снижения нагрузки CPU
            time.sleep(0.1)

        self.logger.info("🏁 Media Worker завершает работу")
        self.logger.info("✅ Все задачи обработаны, ресурсы очищены")


if __name__ == "__main__":
    try:
        # Проверка существования временной директории
        temp_dir = "/var/lib/media-worker/tmp"
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir, exist_ok=True)
            os.chmod(temp_dir, 0o777)  # Даем полные права для временных файлов

        # Запуск воркера
        worker = MediaWorker()
        worker.run()

    except Exception as e:
        logging.error(f"❌ Фатальная ошибка при запуске: {str(e)}")
        sys.exit(1)