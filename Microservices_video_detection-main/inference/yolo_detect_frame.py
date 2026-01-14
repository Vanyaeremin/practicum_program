"""
YOLO Frame Detection — получение детекций для одного кадра в JSON

Этот модуль обрабатывает отдельные кадры (не видео целиком).
Кадр приходит в формате JPEG (base64-кодированный), декодируется,
обрабатывается моделью YOLO и возвращается JSON с детекциями.

НАСТРОЙКА МОДЕЛИ:
- yolo_path='best.pt' — путь к весам модели
- conf=0.25 — порог уверенности (снижен для лучшего распознавания животных)
- iou=0.45 — порог IoU для NMS (более строгий для фильтрации дубликатов)

Функция принимает байты кадра (JPEG), запускает YOLO и возвращает JSON
с детекциями для этого кадра.
"""
import cv2
import numpy as np
from ultralytics import YOLO
import base64
from typing import Dict, List, Any

# Global model cache - load YOLO model once
_global_model = None
_model_path = None

def get_yolo_model(yolo_path: str = 'best.pt') -> YOLO:
    """Get or create cached YOLO model instance"""
    global _global_model, _model_path
    
    if _global_model is None or _model_path != yolo_path:
        print(f"[yolo] Loading model from {yolo_path} (first time or path changed)", flush=True)
        _global_model = YOLO(yolo_path)
        _model_path = yolo_path
        print("[yolo] Model loaded and cached", flush=True)
    
    return _global_model


def get_yolo_predictions_for_frame(
    frame_bytes: bytes,
    frame_id: int,
    yolo_path: str = 'best.pt',
    conf: float = 0.25,
    iou: float = 0.45
) -> Dict[str, Any]:
    """
    Запустить YOLO на одном кадре и вернуть результат в виде JSON.
    
    Кадр должен быть в формате JPEG (байты).
    Функция декодирует кадр, запускает YOLO модель и возвращает JSON
    с детекциями для этого кадра.
    
    Args:
        frame_bytes: байты кадра в формате JPEG
        frame_id: номер кадра в исходном видео (для правильной сборки финального JSON)
        yolo_path: путь к модели (по умолчанию 'best.pt')
        conf: порог уверенности (0.25 - снижен для лучшего распознавания животных)
        iou: порог IoU для NMS (0.45 - более строгий для фильтрации дубликатов)
    
    Returns:
        Словарь с детекциями для кадра:
        {
            "frame": frame_id,
            "objects": [
                {"class": "person", "box": [x1, y1, x2, y2], "conf": 0.95},
                ...
            ]
        }
    
    Пример:
        result = get_yolo_predictions_for_frame(frame_bytes, frame_id=0)
        # result = {
        #     "frame": 0,
        #     "objects": [
        #         {"class": "cat", "box": [100, 200, 300, 400], "conf": 0.87},
        #         {"class": "dog", "box": [150, 250, 350, 450], "conf": 0.92}
        #     ]
        # }
    """
    # Декодируем JPEG кадр в numpy array
    np_img = np.frombuffer(frame_bytes, dtype=np.uint8)
    frame = cv2.imdecode(np_img, cv2.IMREAD_COLOR)
    
    if frame is None:
        raise ValueError("Failed to decode frame from JPEG")
    
    # Загружаем модель YOLO (кэшированная, загружается только один раз)
    model = get_yolo_model(yolo_path)
    
    # Запускаем YOLO на кадре
    yolo_results = model.predict(source=frame, conf=conf, iou=iou, verbose=False)
    
    # Извлекаем детекции из результатов
    frame_objects = []
    for r in yolo_results:
        for box in r.boxes:
            cls = int(box.cls[0])
            label = model.names[cls]  # Название класса (например, "cat", "dog")
            x1, y1, x2, y2 = map(int, box.xyxy[0])  # Координаты bounding box
            conf_val = float(box.conf[0])  # Уверенность детекции
            
            frame_objects.append({
                "class": label,
                "box": [x1, y1, x2, y2],
                "conf": conf_val
            })
    
    # Формируем результат в формате, совместимом со старой версией
    result = {
        "frame": frame_id,
        "objects": frame_objects
    }
    
    return result


def get_yolo_predictions_for_frame_base64(
    frame_base64: str,
    frame_id: int,
    yolo_path: str = 'best.pt',
    conf: float = 0.25,
    iou: float = 0.45
) -> Dict[str, Any]:
    """
    Запустить YOLO на кадре, переданном в формате base64.
    
    Удобная обёртка для работы с base64-кодированными кадрами,
    которые передаются через Kafka в JSON.
    
    Args:
        frame_base64: base64-кодированная строка с JPEG кадром
        frame_id: номер кадра в исходном видео
        yolo_path: путь к модели (по умолчанию 'best.pt')
        conf: порог уверенности (0.25)
        iou: порог IoU для NMS (0.45)
    
    Returns:
        Словарь с детекциями для кадра (см. get_yolo_predictions_for_frame)
    """
    # Декодируем base64 в байты
    frame_bytes = base64.b64decode(frame_base64)
    
    # Вызываем основную функцию
    return get_yolo_predictions_for_frame(frame_bytes, frame_id, yolo_path, conf, iou)
