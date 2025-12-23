# utils/logger.py
import logging
import sys


def setup_logger():
    """Настройка логгера для приложения"""
    logger = logging.getLogger('media_worker')
    logger.setLevel(logging.INFO)

    # Формат для systemd
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(message)s'
    )

    # Вывод в stdout (systemd забирает оттуда логи)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
