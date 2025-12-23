import os
import subprocess
import time
from pathlib import Path


class FFmpegProcessor:
    """Процессор для работы с FFmpeg (транскодирование и превью)"""

    @staticmethod
    def generate_thumbnail(video_path, timestamp, output_path=None, width=320):
        """
        Генерация превью через FFmpeg (реальный production-метод)

        Args:
            video_path (str): Путь к видеофайлу
            timestamp (str): Время в формате "ЧЧ:ММ:СС"
            output_path (str, optional): Путь для сохранения превью
            width (int): Ширина превью (высота рассчитается автоматически)

        Returns:
            dict: Метаданные сгенерированного превью
        """
        try:
            # Проверяем существование видеофайла
            if not os.path.exists(video_path):
                raise FileNotFoundError(f"Видео файл не найден: {video_path}")

            # Формируем имя выходного файла если не указано
            if output_path is None:
                video_dir = Path(video_path).parent
                video_name = Path(video_path).stem
                timestamp_clean = timestamp.replace(':', '-')
                output_path = str(video_dir / f"{video_name}_thumb_{timestamp_clean}.jpg")

            # Формируем команду FFmpeg
            cmd = [
                'ffmpeg',
                '-hide_banner',
                '-ss', timestamp,  # Позиция в видео
                '-i', video_path,  # Входной файл
                '-vframes', '1',  # Только 1 кадр
                '-vf', f'scale={width}:-2',  # Масштабирование с сохранением пропорций
                '-q:v', '2',  # Качество JPEG (2 = высокое)
                '-y',  # Перезаписать если файл существует
                output_path  # Выходной файл
            ]

            # Выполняем команду
            start_time = time.time()
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            processing_time = time.time() - start_time

            # Проверяем результат
            if result.returncode != 0:
                error_msg = result.stderr.strip().split('\n')[-1]
                raise RuntimeError(f"FFmpeg ошибка: {error_msg}")

            if not os.path.exists(output_path):
                raise RuntimeError("FFmpeg выполнился успешно, но файл не создан")

            # Собираем метаданные
            file_size = os.path.getsize(output_path)
            return {
                "path": output_path,
                "size_bytes": file_size,
                "size_mb": round(file_size / (1024 * 1024), 2),
                "width": width,
                "timestamp": timestamp,
                "processing_time_sec": round(processing_time, 2),
                "format": "jpg"
            }

        except Exception as e:
            error_data = {
                "error": str(e),
                "input_file": video_path,
                "timestamp": timestamp,
                "attempted_path": output_path or "auto-generated"
            }
            print(f"❌ Ошибка при генерации превью: {str(e)}")
            return error_data

    @staticmethod
    def transcode_to_hls(video_path, output_dir=None, quality="720p"):
        """
        Транскодирование видео в HLS формат

        Args:
            video_path (str): Путь к исходному видео
            output_dir (str, optional): Директория для сохранения HLS
            quality (str): Качество транскодирования ("720p", "1080p", "480p")

        Returns:
            dict: Метаданные HLS-стрима
        """
        try:
            # Проверяем существование видеофайла
            if not os.path.exists(video_path):
                raise FileNotFoundError(f"Видео файл не найден: {video_path}")

            # Определяем параметры качества
            quality_settings = {
                "480p": {"resolution": "854x480", "video_bitrate": "1000k", "audio_bitrate": "128k"},
                "720p": {"resolution": "1280x720", "video_bitrate": "2500k", "audio_bitrate": "128k"},
                "1080p": {"resolution": "1920x1080", "video_bitrate": "5000k", "audio_bitrate": "192k"}
            }

            if quality not in quality_settings:
                raise ValueError(f"Неподдерживаемое качество: {quality}. Доступно: {list(quality_settings.keys())}")

            settings = quality_settings[quality]

            # Определяем директории
            video_dir = Path(video_path).parent
            video_name = Path(video_path).stem

            if output_dir is None:
                output_dir = str(video_dir / f"{video_name}_hls_{quality}")

            os.makedirs(output_dir, exist_ok=True)
            master_playlist = str(Path(output_dir) / "master.m3u8")

            # Команда FFmpeg для HLS
            cmd = [
                'ffmpeg',
                '-hide_banner',
                '-i', video_path,
                '-profile:v', 'baseline',
                '-level', '3.0',
                '-s', settings["resolution"],
                '-start_number', '0',
                '-hls_time', '10',
                '-hls_list_size', '0',
                '-f', 'hls',
                '-c:v', 'libx264',
                '-b:v', settings["video_bitrate"],
                '-c:a', 'aac',
                '-b:a', settings["audio_bitrate"],
                '-max_muxing_queue_size', '9999',
                '-y',
                master_playlist
            ]

            # Выполняем транскодирование
            start_time = time.time()
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            processing_time = time.time() - start_time

            # Проверяем результат
            if result.returncode != 0:
                error_msg = result.stderr.strip().split('\n')[-1]
                raise RuntimeError(f"FFmpeg ошибка: {error_msg}")

            if not os.path.exists(master_playlist):
                raise RuntimeError("HLS плейлист не создан")

            # Анализируем результаты
            segment_files = [f for f in os.listdir(output_dir) if f.endswith('.ts')]
            total_size = sum(os.path.getsize(os.path.join(output_dir, f)) for f in segment_files)

            return {
                "master_playlist": master_playlist,
                "segments_count": len(segment_files),
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "quality": quality,
                "resolution": settings["resolution"],
                "processing_time_sec": round(processing_time, 2),
                "bitrate": {
                    "video": settings["video_bitrate"],
                    "audio": settings["audio_bitrate"]
                }
            }

        except Exception as e:
            error_data = {
                "error": str(e),
                "input_file": video_path,
                "quality": quality,
                "output_dir": output_dir
            }
            print(f"❌ Ошибка при транскодировании в HLS: {str(e)}")
            return error_data
