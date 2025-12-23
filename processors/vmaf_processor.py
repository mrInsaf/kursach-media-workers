import json
import os
import subprocess
from pathlib import Path


class VMAFProcessor:
    """Процессор для оценки качества видео через VMAF"""

    @staticmethod
    def calculate_vmaf(original_path, compressed_path, output_json=None):
        """
        Расчет VMAF между оригинальным и сжатым видео

        Args:
            original_path (str): Путь к оригинальному видео
            compressed_path (str): Путь к сжатому видео
            output_json (str, optional): Путь для сохранения результатов в JSON

        Returns:
            dict: Результаты VMAF анализа
        """
        try:
            # Проверяем существование файлов
            if not os.path.exists(original_path):
                raise FileNotFoundError(f"Оригинальное видео не найдено: {original_path}")
            if not os.path.exists(compressed_path):
                raise FileNotFoundError(f"Сжатое видео не найдено: {compressed_path}")

            # Определяем параметры
            model_path = "/usr/local/share/model/vmaf_v0.6.1.json"  # Стандартный путь

            # Если output_json не указан - генерируем временный
            if output_json is None:
                temp_dir = Path("/tmp/vmaf_results")
                temp_dir.mkdir(exist_ok=True)
                output_json = str(temp_dir / f"vmaf_{os.path.basename(original_path)}.json")

            # Команда FFmpeg с VMAF
            cmd = [
                'ffmpeg',
                '-hide_banner',
                '-i', compressed_path,
                '-i', original_path,
                '-lavfi', f"[0:v]scale=1920:1080:flags=bicubic[distorted];"
                          f"[1:v]scale=1920:1080:flags=bicubic[ref];"
                          f"[distorted][ref]libvmaf="
                          f"model_path={model_path}:"
                          f"log_path={output_json}:"
                          f"log_fmt=json:"
                          f"phone_model=1:"
                          f"psnr=1:"
                          f"ssim=1:"
                          f"ms_ssim=1",
                '-f', 'null',
                '-'
            ]

            # Выполняем команду
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Проверяем результат
            if result.returncode != 0:
                error_msg = result.stderr.strip().split('\n')[-1]
                raise RuntimeError(f"VMAF ошибка: {error_msg}")

            if not os.path.exists(output_json):
                raise RuntimeError("Файл с результатами VMAF не создан")

            # Читаем результаты
            with open(output_json, 'r') as f:
                vmaf_data = json.load(f)

            # Формируем удобный результат
            vmaf_score = vmaf_data['pooled_metrics']['vmaf']['mean']
            psnr_score = vmaf_data['pooled_metrics']['psnr']['mean']
            ssim_score = vmaf_data['pooled_metrics']['ssim']['mean']

            result_data = {
                "vmaf_score": round(vmaf_score, 2),
                "psnr_score": round(psnr_score, 2),
                "ssim_score": round(ssim_score, 2),
                "details": {
                    "model": os.path.basename(model_path),
                    "frames_processed": len(vmaf_data['frames']),
                    "original_file": original_path,
                    "compressed_file": compressed_path
                },
                "json_report": output_json
            }

            print(f"⭐ VMAF анализ завершен:")
            print(f"   VMAF: {vmaf_score:.2f}/100")
            print(f"   PSNR: {psnr_score:.2f} dB")
            print(f"   SSIM: {ssim_score:.4f}")

            return result_data

        except Exception as e:
            print(f"❌ Ошибка при расчете VMAF: {str(e)}")
            return {
                "error": str(e),
                "original_file": original_path,
                "compressed_file": compressed_path
            }
