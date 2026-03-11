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


def create_gif_from_frames(
    frames_folder: str,
    output_gif: str,
    fps: int = 10,
    speed_multiplier: int = 4,
    max_width: int = 640,
) -> str:
    """
    Cria GIF animado diretamente de frames JPG usando Pillow.

    Evita MoviePy e seus problemas de crash no Fortran runtime (forrtl error 200).

    Args:
        frames_folder:      Pasta com frames .jpg (ordenados numericamente).
        output_gif:         Caminho de saída do .gif.
        fps:                Frames por segundo do vídeo original.
        speed_multiplier:   Aceleração do GIF (ex.: 4 = 4× mais rápido).
        max_width:          Largura máxima para redimensionar os frames.

    Returns:
        Caminho absoluto do GIF gerado.
    """
    from PIL import Image  # type: ignore

    jpg_files = sorted(
        glob(os.path.join(frames_folder, "*.jpg")),
        key=lambda x: int("".join(c for c in os.path.basename(x) if c.isdigit()) or "0"),
    )
    if not jpg_files:
        raise FileNotFoundError(f"Nenhum frame .jpg encontrado em: {frames_folder}")

    duration_ms = max(20, int(1000 / (fps * speed_multiplier)))

    pil_frames: list = []
    for path in jpg_files:
        try:
            img = Image.open(path).convert("RGB")
        except Exception as exc:
            log.warning("Frame ilegível no GIF, pulando", path=path, error=str(exc))
            continue
        if img.width > max_width:
            ratio = max_width / img.width
            img = img.resize((max_width, int(img.height * ratio)), Image.LANCZOS)
        pil_frames.append(img.convert("P", palette=Image.ADAPTIVE, colors=256))

    if not pil_frames:
        raise ValueError(f"Nenhum frame válido para criar GIF em: {frames_folder}")

    os.makedirs(os.path.dirname(os.path.abspath(output_gif)), exist_ok=True)
    pil_frames[0].save(
        output_gif,
        save_all=True,
        append_images=pil_frames[1:],
        optimize=False,
        duration=duration_ms,
        loop=0,
    )

    log.info(
        "GIF gerado (Pillow)",
        path=output_gif,
        frames=len(pil_frames),
        duration_ms=duration_ms,
        speed=f"{speed_multiplier}x",
    )
    return output_gif


def create_gif(
    video_path: str,
    output_gif: Optional[str] = None,
    speed_multiplier: int = 4,
    max_width: int = 640,
) -> str:
    """
    Converte um vídeo MP4 em GIF animado usando Pillow (via frames OpenCV).

    Substitui a implementação anterior baseada em MoviePy que causava
    crash no Fortran runtime (forrtl error 200) em Windows.

    Args:
        video_path:         Caminho do .mp4 de entrada.
        output_gif:         Caminho do .gif de saída (None = mesmo nome do vídeo).
        speed_multiplier:   Fator de aceleração (ex.: 4 = 4× mais rápido).
        max_width:          Largura máxima para redimensionar os frames.

    Returns:
        Caminho absoluto do GIF gerado.
    """
    from PIL import Image  # type: ignore

    if output_gif is None:
        output_gif = os.path.splitext(video_path)[0] + ".gif"

    os.makedirs(os.path.dirname(os.path.abspath(output_gif)), exist_ok=True)

    cap = cv.VideoCapture(video_path)
    if not cap.isOpened():
        raise FileNotFoundError(f"Não foi possível abrir o vídeo: {video_path}")

    fps = cap.get(cv.CAP_PROP_FPS) or 10.0
    duration_ms = max(20, int(1000 / (fps * speed_multiplier)))

    pil_frames: list = []
    while True:
        ok, frame = cap.read()
        if not ok:
            break
        # BGR → RGB
        rgb = cv.cvtColor(frame, cv.COLOR_BGR2RGB)
        img = Image.fromarray(rgb)
        if img.width > max_width:
            ratio = max_width / img.width
            img = img.resize((max_width, int(img.height * ratio)), Image.LANCZOS)
        pil_frames.append(img.convert("P", palette=Image.ADAPTIVE, colors=256))
    cap.release()

    if not pil_frames:
        raise ValueError(f"Nenhum frame extraído do vídeo: {video_path}")

    pil_frames[0].save(
        output_gif,
        save_all=True,
        append_images=pil_frames[1:],
        optimize=False,
        duration=duration_ms,
        loop=0,
    )

    log.info(
        "GIF gerado (Pillow/vídeo)",
        path=output_gif,
        frames=len(pil_frames),
        duration_ms=duration_ms,
        speed=f"{speed_multiplier}x",
    )
    return output_gif
