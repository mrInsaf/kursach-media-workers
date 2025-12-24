import json

from processors.mediainfo_processor import MediaInfoProcessor

if __name__ == "__main__":
    import os

    print("📊 Тестирование MediaInfoProcessor...")
    print("=" * 50)

    # Автоматическое определение пути для Windows
    test_video = None
    possible_paths = [
        os.path.join(os.getcwd(), "WIN_20251223_23_20_51_Pro.mp4"),
        os.path.join(os.getcwd(), "sample.mp4"),
        "C:\\test_video.mp4",
        "D:\\test_video.mp4"
    ]

    for path in possible_paths:
        if os.path.exists(path):
            test_video = path
            break

    if test_video is None:
        print("❌ Тестовое видео не найдено!")
        print("💡 Создайте файл test_video.mp4 в текущей директории или укажите путь ниже:")
        print(f"   Текущая директория: {os.getcwd()}")
        print("   Примеры рабочих путей для Windows:")
        print("     - C:\\Users\\username\\Desktop\\test_video.mp4")
        print("     - D:\\projects\\media_worker\\sample.mp4")

        # Предлагаем пользователю ввести путь
        user_path = input("\n📁 Введите полный путь к тестовому видео (или нажмите Enter для выхода): ").strip()
        if user_path and os.path.exists(user_path):
            test_video = user_path
        else:
            exit(1)

    print(f"🎬 Анализ видео: {test_video}")
    print(f"   Размер файла: {os.path.getsize(test_video) / (1024 * 1024):.2f} MB")

    try:
        # Анализируем видео
        metadata = MediaInfoProcessor.analyze_video(test_video)

        print("\n✅ Успешно получены метаданные:")
        print(f"   📝 Имя файла: {metadata.get('filename', 'N/A')}")
        print(f"   ⏱️ Длительность: {metadata.get('duration_sec', 0):.2f} сек")

        if 'video' in metadata:
            video = metadata['video']
            print(f"   🖥️ Разрешение: {video.get('width', 0)}x{video.get('height', 0)}")
            print(f"   📡 Битрейт видео: {video.get('bit_rate', 0) // 1000} kbps")
            print(f"   🎞️ Кодек: {video.get('codec', 'N/A')}")

        if 'audio' in metadata:
            audio = metadata['audio']
            print(f"   🔊 Аудио: {audio.get('codec', 'N/A')}, {audio.get('channels', 'N/A')} каналов")
            print(f"   🔊 Битрейт аудио: {audio.get('bit_rate', 0) // 1000} kbps")

        # Сохраняем результаты в файл для отчета
        output_file = os.path.join(os.getcwd(), "mediainfo_results.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        print(f"\n💾 Результаты сохранены в: {output_file}")
        print("📊 Тест MediaInfoProcessor завершен успешно!")

    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
        print("💡 Советы для Windows:")
        print("   1. Убедитесь, что MediaInfo установлен: https://mediaarea.net/ru/MediaInfo/Download/Windows")
        print(
            "   2. Добавьте MediaInfo в PATH: Панель управления -> Система -> Дополнительные параметры системы -> Переменные среды")
        print("   3. Перезапустите командную строку после установки")