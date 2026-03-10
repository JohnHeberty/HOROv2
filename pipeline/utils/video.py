"""
pipeline/utils/video.py
=======================
Geração de vídeo MP4 e GIF a partir de frames JPEG.

Extraído de Functions.py (`CreateVideo`) e RUN_HORO.ipynb (trecho moviepy).
"""

from __future__ import annotations

import os
from glob import glob
from typing import Optional

import cv2 as cv
import numpy as np

from pipeline.core.logger import get_logger

log = get_logger("utils.video")


def create_video(
    frames_folder: str,
    output_path: str,
    width: int = 1920,
    height: int = 1080,
    fps: int = 10,
) -> str:
    """
    Gera um vídeo MP4 a partir dos frames JPEG em *frames_folder*.

    Os frames são ordenados numericamente pelo número presente no nome do arquivo.

    Args:
        frames_folder: Pasta com os frames .jpg
        output_path:   Caminho de saída do .mp4
        width:         Largura do vídeo
        height:        Altura do vídeo
        fps:           Frames por segundo

    Returns:
        Caminho absoluto do vídeo gerado.
    """
    images = sorted(
        glob(os.path.join(frames_folder, "*.jpg")),
        key=lambda x: int("".join(c for c in os.path.basename(x) if c.isdigit()) or "0"),
    )

    if not images:
        raise FileNotFoundError(
            f"Nenhum frame .jpg encontrado em '{frames_folder}'"
        )

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    codec = cv.VideoWriter_fourcc(*"XVID")
    writer = cv.VideoWriter(output_path, codec, fps, (width, height))

    for img_path in images:
        # cv.imread falha silenciosamente em caminhos Windows com caracteres não-ASCII
        # (ex.: "Área de Trabalho"). np.fromfile + imdecode é o contorno correto.
        try:
            raw = np.fromfile(img_path, dtype=np.uint8)
            frame = cv.imdecode(raw, cv.IMREAD_COLOR)
        except Exception:
            frame = None
        if frame is None:
            log.warning("Frame ilegível, pulando", path=img_path)
            continue
        frame = cv.resize(frame, (width, height))
        writer.write(frame)

    writer.release()
    log.info(
        "Vídeo gerado",
        path=output_path,
        frames=len(images),
        fps=fps,
    )
    return output_path


def create_gif(
    video_path: str,
    output_gif: Optional[str] = None,
    speed_multiplier: int = 4,
) -> str:
    """
    Converte um vídeo MP4 em GIF animado usando moviepy.

    Compatível com moviepy 1.x e 2.x.

    Args:
        video_path:         Caminho do .mp4 de entrada.
        output_gif:         Caminho do .gif de saída. Se None, usa mesmo nome do vídeo.
        speed_multiplier:   Fator de aceleração (ex.: 4 = 4× mais rápido).

    Returns:
        Caminho absoluto do GIF gerado.
    """
    if output_gif is None:
        output_gif = os.path.splitext(video_path)[0] + ".gif"

    os.makedirs(os.path.dirname(os.path.abspath(output_gif)), exist_ok=True)

    try:
        # moviepy >= 2.0
        from moviepy import VideoFileClip  # type: ignore
    except ImportError:
        # moviepy 1.x
        from moviepy.editor import VideoFileClip  # type: ignore

    clip = VideoFileClip(video_path)

    if speed_multiplier != 1:
        clip = clip.with_speed_scaled(speed_multiplier) if hasattr(clip, "with_speed_scaled") else clip.speedx(speed_multiplier)

    clip.write_gif(output_gif, fps=clip.fps)
    clip.close()

    log.info("GIF gerado", path=output_gif, speed=f"{speed_multiplier}x")
    return output_gif
