"""
YOLO Frame Detection — получение детекций для одного кадра в JSON

Этот модуль обрабатывает отдельные кадры (не видео целиком).
Кадр приходит в формате JPEG (байты), декодируется, обрабатывается моделью YOLO
и возвращается JSON с детекциями.

Настройка модели:
- yolo_path='best.pt' — путь к весам модели
- conf=0.25 — порог уверенности (снижен для лучшего распознавания животных)
- iou=0.45 — порог IoU для NMS (более строгий для фильтрации дубликатов)

Функция принимает байты кадра (JPEG), запускает YOLO и возвращает JSON
с детекциями для этого кадра.
"""
import base64
from typing import Any, Dict
import cv2
import numpy as np
from ultralytics import YOLO

_global_model = None
_model_path = None


def get_yolo_model(yolo_path: str = 'best.pt') -> YOLO:

    global _global_model, _model_path

    if _global_model is None or _model_path != yolo_path:
        print(
            f"[yolo] Loading model from {yolo_path} "
            f"(first time or path changed)",
            flush=True
        )
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

    np_img = np.frombuffer(frame_bytes, dtype=np.uint8)
    frame = cv2.imdecode(np_img, cv2.IMREAD_COLOR)

    if frame is None:
        raise ValueError("Failed to decode frame from JPEG")

    model = get_yolo_model(yolo_path)

    yolo_results = model.predict(
        source=frame, conf=conf, iou=iou, verbose=False
    )

    frame_objects = []
    for r in yolo_results:
        for box in r.boxes:
            cls = int(box.cls[0])
            label = model.names[cls]  
            x1, y1, x2, y2 = map(int, box.xyxy[0])  
            conf_val = float(box.conf[0]) 

            frame_objects.append({
                "class": label,
                "box": [x1, y1, x2, y2],
                "conf": conf_val
            })

    result = {
        "frame": frame_id,
        "objects": frame_objects
    }

    return result