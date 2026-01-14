"""
Image CPU — CPU-bound операции предобработки изображений

Этот модуль содержит синхронные функции для предобработки изображений,
которые требуют интенсивного использования CPU и блокируют GIL.

Функции предназначены для вызова через ProcessPoolExecutor из async_wrapper.py,
что обеспечивает истинный параллелизм и не блокирует event loop.

Предобработка включает:
1. Gaussian Blur — шумоподавление
2. Корректировка контрастности и яркости — улучшение качества детекции

Параметры предобработки оптимизированы для улучшения детекции объектов
YOLO моделью на видео с животными.
"""
import cv2
import numpy as np

def preprocess_image(
    img_bytes: bytes,
    blur_ksize: int = 5,
    alpha: float = 1.5,  # множитель контраста
    beta: int = 0        # смещение яркости
) -> bytes:
    """
    Выполняет CPU-bound предобработку изображения.
    
    Применяет два этапа предобработки для улучшения качества детекции:
    1. Gaussian Blur для шумоподавления
    2. Корректировка контрастности и яркости
    
    Args:
        img_bytes: байты входного изображения в формате JPEG
        blur_ksize: размер ядра для Gaussian Blur (нечётное число, по умолчанию 5)
        alpha: множитель контраста (>1 увеличивает контраст, по умолчанию 1.5)
        beta: смещение яркости (>0 увеличивает яркость, по умолчанию 0)
    
    Returns:
        bytes: байты предобработанного изображения в формате JPEG
    
    Raises:
        ValueError: если не удалось декодировать изображение
    
    Процесс обработки:
        1. Декодирование JPEG в numpy array
        2. Применение Gaussian Blur для шумоподавления
        3. Корректировка контраста и яркости
        4. Кодирование обратно в JPEG
    
    Пример:
        processed = preprocess_image(image_bytes, blur_ksize=3, alpha=1.3, beta=5)
    """
    # Декодируем байты JPEG в numpy array
    np_img = np.frombuffer(img_bytes, dtype=np.uint8)
    img = cv2.imdecode(np_img, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError("Не удалось декодировать изображение")
    
    # 1. Gaussian Blur для шумоподавления
    # Используем нечётное ядро (blur_ksize x blur_ksize)
    img = cv2.GaussianBlur(img, (blur_ksize, blur_ksize), 0)
    
    # 2. Корректировка контрастности и яркости
    # Формула: new_pixel = alpha * old_pixel + beta
    img = cv2.convertScaleAbs(img, alpha=alpha, beta=beta)
    
    # Кодируем обратно в байты JPEG
    _, encoded = cv2.imencode(".jpg", img)
    return encoded.tobytes()

