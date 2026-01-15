"""
Модуль для асинхронного извлечения и предобработки кадров из видео

Этот модуль отвечает за:
1. Извлечение кадров из видео (каждый 5-й кадр)
2. Предобработку каждого кадра (Gaussian blur + корректировка контраста)

Важно: Все операции выполняются асинхронно через ProcessPoolExecutor
для обеспечения неблокирующей обработки видео.
"""
import asyncio
import os
import tempfile
from concurrent.futures import ProcessPoolExecutor
from typing import List, Tuple
import cv2

executor_frames = ProcessPoolExecutor(max_workers=os.cpu_count())


def extract_and_preprocess_frames_sync(
    video_bytes: bytes,
    every_n_frames: int = 5,
    blur_ksize: int = 3,
    alpha: float = 1.3,
    beta: int = 5
) -> List[Tuple[int, bytes]]:
    temp_in = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
    try:
        temp_in.write(video_bytes)
        temp_in.flush()
        temp_in.close()

        cap = cv2.VideoCapture(temp_in.name)
        if not cap.isOpened():
            raise ValueError("Не удалось открыть видео файл")

        frames = []
        frame_id = 0

        while True:
            ret, frame = cap.read()
            if not ret:
                break
            if frame_id % every_n_frames == 0:
                blurred = cv2.GaussianBlur(frame, (blur_ksize, blur_ksize), 0)
                processed = cv2.convertScaleAbs(blurred, alpha=alpha, beta=beta)
                _, encoded = cv2.imencode(
                    ".jpg", processed, [cv2.IMWRITE_JPEG_QUALITY, 95]
                )
                frame_bytes = encoded.tobytes()
                frames.append((frame_id, frame_bytes))

            frame_id += 1

        cap.release()
        return frames

    finally:
        if os.path.exists(temp_in.name):
            os.unlink(temp_in.name)


async def extract_and_preprocess_frames_async(
    video_bytes: bytes,
    every_n_frames: int = 5,
    blur_ksize: int = 3,
    alpha: float = 1.3,
    beta: int = 5
) -> List[Tuple[int, bytes]]:
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
