"""
Модуль для асинхронного извлечения и предобработки кадров из видео

Этот модуль отвечает за:
1. Извлечение кадров из видео (каждый 5-й кадр)
2. Предобработку каждого кадра (Gaussian blur + корректировка контраста)
3. Кодирование кадров в JPEG для передачи через Kafka

ВАЖНО: Все операции выполняются асинхронно через ProcessPoolExecutor
для обеспечения неблокирующей обработки видео.
"""
import cv2
import numpy as np
import tempfile
import os
import asyncio
from concurrent.futures import ProcessPoolExecutor
from typing import List, Tuple

# Глобальный executor для CPU-bound операций с видео
executor_frames = ProcessPoolExecutor(max_workers=os.cpu_count())


def extract_and_preprocess_frames_sync(
    video_bytes: bytes, 
    every_n_frames: int = 5,
    blur_ksize: int = 3, 
    alpha: float = 1.3, 
    beta: int = 5
) -> List[Tuple[int, bytes]]:
    """
    Синхронная функция для извлечения и предобработки кадров из видео.
    Выполняется в отдельном процессе через executor для неблокирующей работы.
    
    Извлекает каждый N-й кадр (по умолчанию каждый 5-й) и применяет предобработку:
    - Gaussian Blur (ksize=3) для шумоподавления
    - Корректировка контраста (alpha=1.3) и яркости (beta=5)
    
    Args:
        video_bytes: байты входного видео
        every_n_frames: извлекать каждый N-й кадр (по умолчанию 5)
        blur_ksize: размер ядра blur (по умолчанию 3)
        alpha: множитель контраста (по умолчанию 1.3)
        beta: смещение яркости (по умолчанию 5)
    
    Returns:
        Список кортежей (frame_id, frame_bytes), где:
        - frame_id: номер кадра в исходном видео (0, 5, 10, 15, ...)
        - frame_bytes: байты предобработанного кадра в формате JPEG
    
    Пример:
        [(0, b'...'), (5, b'...'), (10, b'...'), ...]
    """
    # Создаём временный файл для видео
    temp_in = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
    try:
        temp_in.write(video_bytes)
        temp_in.flush()
        temp_in.close()
        
        # Открываем видео через OpenCV
        cap = cv2.VideoCapture(temp_in.name)
        if not cap.isOpened():
            raise ValueError("Не удалось открыть видео файл")
        
        frames = []
        frame_id = 0
        
        # Читаем кадры из видео
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            
            # Извлекаем только каждый N-й кадр
            if frame_id % every_n_frames == 0:
                # Применяем предобработку: Gaussian Blur
                blurred = cv2.GaussianBlur(frame, (blur_ksize, blur_ksize), 0)
                
                # Применяем корректировку контраста и яркости
                processed = cv2.convertScaleAbs(blurred, alpha=alpha, beta=beta)
                
                # Кодируем кадр в JPEG для передачи через Kafka
                # JPEG выбран для уменьшения размера данных
                _, encoded = cv2.imencode(".jpg", processed, [cv2.IMWRITE_JPEG_QUALITY, 95])
                frame_bytes = encoded.tobytes()
                
                # Сохраняем пару (номер кадра, байты кадра)
                frames.append((frame_id, frame_bytes))
            
            frame_id += 1
        
        cap.release()
        return frames
    
    finally:
        # Удаляем временный файл
        if os.path.exists(temp_in.name):
            os.unlink(temp_in.name)


async def extract_and_preprocess_frames_async(
    video_bytes: bytes,
    every_n_frames: int = 5,
    blur_ksize: int = 3,
    alpha: float = 1.3,
    beta: int = 5
) -> List[Tuple[int, bytes]]:
    """
    Асинхронная обёртка для извлечения и предобработки кадров из видео.
    
    Выполняет CPU-bound операцию в отдельном процессе через ProcessPoolExecutor,
    что позволяет не блокировать event loop во время обработки видео.
    
    Args:
        video_bytes: байты входного видео
        every_n_frames: извлекать каждый N-й кадр (по умолчанию 5)
        blur_ksize: размер ядра blur (по умолчанию 3)
        alpha: множитель контраста (по умолчанию 1.3)
        beta: смещение яркости (по умолчанию 5)
    
    Returns:
        Список кортежей (frame_id, frame_bytes) с предобработанными кадрами
    
    Пример использования:
        frames = await extract_and_preprocess_frames_async(video_bytes, every_n_frames=5)
        for frame_id, frame_bytes in frames:
            # Отправить frame_bytes в inference через Kafka
            await send_frame_to_inference(event_id, frame_id, frame_bytes)
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        executor_frames,
        extract_and_preprocess_frames_sync,
        video_bytes,
        every_n_frames,
        blur_ksize,
        alpha,
        beta
    )
