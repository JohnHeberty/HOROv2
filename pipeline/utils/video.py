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
    gif_width: int = 960,
) -> str:
    """
    Converte MP4 em GIF via ffmpeg com palette adaptativa (2-pass em filtergraph).

    Técnica:
      - `palettegen` constrói a paleta ótima de 256 cores por diff de cena
      - `paletteuse` aplica dithering Bayer para suavizar a quantização
      - `fps` é elevado pelo speed_multiplier para acelerar sem interpolação
      - `scale` usa Lanczos para redimensionar com máxima nitidez

    Args:
        video_path:       Caminho do .mp4 de entrada.
        output_gif:       Caminho do .gif de saída (None = mesmo nome do vídeo).
        speed_multiplier: Fator de aceleração (ex.: 4 = 4× mais rápido).
        gif_width:        Largura de saída px (-2 = altura automática proporcional).

    Returns:
        Caminho absoluto do GIF gerado.
    """
    import shutil
    import subprocess

    if output_gif is None:
        output_gif = os.path.splitext(video_path)[0] + ".gif"

    os.makedirs(os.path.dirname(os.path.abspath(output_gif)), exist_ok=True)

    ff = shutil.which("ffmpeg")
    if not ff:
        raise FileNotFoundError(
            "ffmpeg não encontrado no PATH — necessário para gerar GIF."
        )

    # FPS de saída do GIF: aceleração via frequência maior de frames
    cap = cv.VideoCapture(video_path)
    src_fps = cap.get(cv.CAP_PROP_FPS) or 10.0
    total_frames = int(cap.get(cv.CAP_PROP_FRAME_COUNT))
    cap.release()
    gif_fps = max(10, int(src_fps * speed_multiplier))  # mínimo 10 fps

    # filtergraph: escala Lanczos → split → palettegen (diff) + paletteuse (Bayer)
    scale_filter = f"scale={gif_width}:-2:flags=lanczos"
    vf = (
        f"fps={gif_fps},{scale_filter},"
        f"split[s0][s1];"
        f"[s0]palettegen=stats_mode=diff[p];"
        f"[s1][p]paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle"
    )

    cmd = [
        ff, "-y",
        "-i", video_path,
        "-vf", vf,
        "-loop", "0",
        output_gif,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg GIF falhou:\n{result.stderr[-800:]}")

    size_mb = os.path.getsize(output_gif) / 1024 / 1024
    log.info(
        "GIF gerado (ffmpeg/palette)",
        path=output_gif,
        frames=total_frames,
        gif_fps=gif_fps,
        width=gif_width,
        speed=f"{speed_multiplier}x",
        size_mb=f"{size_mb:.1f} MB",
    )
    return output_gif


def create_gif_from_frames(
    frames_folder: str,
    output_gif: str,
    fps: int = 10,
    speed_multiplier: int = 4,
    gif_width: int = 960,
    width: int = 1920,
    height: int = 1080,
) -> str:
    """
    Cria GIF a partir de frames JPG: monta MP4 temporário → ffmpeg palette GIF.

    Fluxo: frames/ → MP4 temp → ffmpeg palettegen/paletteuse → .gif
    O MP4 temporário é removido automaticamente após a conversão.

    Args:
        frames_folder:    Pasta com frames .jpg ordenados numericamente.
        output_gif:       Caminho do .gif de saída.
        fps:              FPS do vídeo temporário.
        speed_multiplier: Aceleração do GIF.
        gif_width:        Largura de saída do GIF em pixels.
        width:            Largura do vídeo temporário (deve bater com os frames).
        height:           Altura do vídeo temporário.

    Returns:
        Caminho absoluto do GIF gerado.
    """
    tmp_mp4 = os.path.splitext(output_gif)[0] + "_tmp.mp4"
    create_video(
        frames_folder=frames_folder,
        output_path=tmp_mp4,
        width=width,
        height=height,
        fps=fps,
    )
    try:
        create_gif(
            video_path=tmp_mp4,
            output_gif=output_gif,
            speed_multiplier=speed_multiplier,
            gif_width=gif_width,
        )
    finally:
        try:
            os.remove(tmp_mp4)
        except OSError:
            pass
    return output_gif
