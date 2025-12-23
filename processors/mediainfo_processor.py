import json
import subprocess
import os
from pathlib import Path


class MediaInfoProcessor:
    """Процессор для анализа метаданных видео через MediaInfo"""

    @staticmethod
    def analyze_video(video_path):
        """
        Анализ видеофайла через MediaInfo

        Args:
            video_path (str): Путь к видеофайлу

        Returns:
            dict: Структурированные метаданные
        """
        try:
            if not os.path.exists(video_path):
                raise FileNotFoundError(f"Видео файл не найден: {video_path}")

            # Запускаем mediainfo с выводом в JSON
            cmd = [
                'mediainfo',
                '--Output=JSON',
                video_path
            ]

            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )

            # Парсим JSON
            media_data = json.loads(result.stdout)

            # Получаем все треки
            tracks = media_data['media']['track']

            # Найдем видео и аудио треки
            general_track = None
            video_track = None
            audio_track = None

            for track in tracks:
                track_type = track.get('@type', '').lower()
                if track_type == 'general':
                    general_track = track
                elif track_type == 'video':
                    video_track = track
                elif track_type == 'audio':
                    audio_track = track

            # Если не нашли видео трек - ошибка
            if video_track is None:
                raise RuntimeError("Видео трек не найден в файле")

            # Размер файла из General трека или из пути
            if general_track and 'FileSize' in general_track:
                filesize_bytes = int(general_track['FileSize'])
            else:
                filesize_bytes = os.path.getsize(video_path)

            # Длительность из видео трека
            duration = float(video_track.get('Duration', 0))
            if duration == 0 and general_track:
                duration = float(general_track.get('Duration', 0))

            metadata = {
                "filename": os.path.basename(video_path),
                "filesize_bytes": filesize_bytes,
                "duration_sec": duration,
                "video": {
                    "codec": video_track.get('CodecID', video_track.get('Format', 'unknown')),
                    "width": int(video_track.get('Width', 0)),
                    "height": int(video_track.get('Height', 0)),
                    "frame_rate": float(video_track.get('FrameRate', 0)),
                    "bit_rate": int(video_track.get('BitRate', 0)) if video_track.get('BitRate') else 0
                }
            }

            # Добавляем аудио если есть
            if audio_track:
                metadata["audio"] = {
                    "codec": audio_track.get('CodecID', audio_track.get('Format', 'unknown')),
                    "channels": audio_track.get('Channel(s)', 'unknown'),
                    "sample_rate": audio_track.get('SamplingRate', 'unknown'),
                    "bit_rate": int(audio_track.get('BitRate', 0)) if audio_track.get('BitRate') else 0
                }

            print(f"✅ Успешно проанализировано: {metadata['filename']}")
            print(f"   Длительность: {metadata['duration_sec']:.2f} сек")
            print(f"   Разрешение: {metadata['video']['width']}x{metadata['video']['height']}")

            return metadata

        except subprocess.CalledProcessError as e:
            print(f"❌ Ошибка MediaInfo: {e.stderr.strip()}")
            raise RuntimeError(f"MediaInfo ошибка: {e.stderr.strip()}")
        except json.JSONDecodeError as e:
            print(f"❌ Ошибка парсинга JSON от MediaInfo: {str(e)}")
            raise RuntimeError("Неверный формат вывода MediaInfo")
        except Exception as e:
            print(f"❌ Общая ошибка при анализе: {str(e)}")
            raise
