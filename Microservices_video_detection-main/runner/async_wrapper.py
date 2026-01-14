"""
Async Wrapper — асинхронные обёртки для CPU-bound операций

Этот модуль предоставляет асинхронные обёртки для операций,
которые блокируют GIL (Global Interpreter Lock) в Python.

Использует ProcessPoolExecutor для выполнения CPU-интенсивных задач
в отдельных процессах, что позволяет избежать блокировки event loop
и обеспечивает истинный параллелизм для вычислительных операций.

Основная задача: вынести предобработку изображений в отдельный процесс
чтобы не блокировать асинхронную обработку в runner сервисе.
"""
import asyncio
import os
from concurrent.futures import ProcessPoolExecutor
from image_cpu import preprocess_image

# Глобальный ProcessPoolExecutor для CPU-bound операций
# max_workers=os.cpu_count() использует все доступные ядра CPU
executor = ProcessPoolExecutor(max_workers=os.cpu_count())

async def preprocess_image_async(img_bytes: bytes) -> bytes:
    """
    Асинхронная обёртка для предобработки изображений.
    
    Выполняет синхронную функцию preprocess_image в отдельном процессе
    через ProcessPoolExecutor, что позволяет не блокировать event loop
    во время CPU-интенсивной обработки изображения.
    
    Args:
        img_bytes: байты входного изображения в формате JPEG
    
    Returns:
        bytes: байты предобработанного изображения
    
    Raises:
        Exception: при ошибках обработки изображения
    
    Пример использования:
        processed_bytes = await preprocess_image_async(image_bytes)
        # event loop не заблокирован во время обработки
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        executor,
        preprocess_image,
        img_bytes
    )

