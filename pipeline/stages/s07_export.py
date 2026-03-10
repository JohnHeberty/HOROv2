"""
pipeline/stages/s07_export.py
================================
Stage 7 — EXPORT: Gold → Output final (vídeo, GIF, relatório)

Responsabilidades:
  - Gera vídeo MP4 a partir dos frames de cada estação
  - Converte vídeo em GIF animado acelerado
  - Serializa relatório FinalResult.json consolidando todos os aeródromos

Entrada:  context.results, frames em data/gold/exports/{station}/frames/
Saída:    data/gold/exports/{station}/RunwayOrientation-{years}.mp4
          data/gold/exports/{station}/RunwayOrientation-{years}.gif
          data/gold/exports/FinalResult.json
"""

from __future__ import annotations

import json
import os
from typing import Dict

from pipeline.core.config import PipelineConfig, cfg
from pipeline.core.exceptions import ExportError
from pipeline.core.logger import get_logger
from pipeline.core.models import PipelineContext
from pipeline.utils.video import create_gif, create_video

log = get_logger("s07_export")


def _build_final_report(context: PipelineContext) -> Dict:
    """Consolida todos os resultados em um dicionário serializável."""
    report: Dict = {}
    for station, by_years in context.results.items():
        report[station] = {}
        silver = context.silver.get(station)
        if silver:
            report[station]["metadata"] = {
                "latitude":  silver.metadata.latitude,
                "longitude": silver.metadata.longitude,
                "altitude":  silver.metadata.altitude,
            }
        for years, res in by_years.items():
            report[station][f"{years}y"] = {
                "runway":         res.runway_designation,
                "fo_pct":         res.fo_pct,
                "crosswind_pct":  res.crosswind_pct,
                "calm_pct":       res.calm_pct,
                "heading_deg":    res.best_heading_deg,
                "declination":    res.magnetic_declination,
            }
    return report


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------

def run(context: PipelineContext, config: PipelineConfig = cfg) -> PipelineContext:
    """Gera vídeos, GIFs e relatório final."""
    log.info("=== STAGE 7 — EXPORT (Gold → Output) ===")

    rc = config.render
    exports_root = os.path.join(config.output.data_gold, "exports")
    os.makedirs(exports_root, exist_ok=True)

    for station, by_years in context.results.items():
        for years, result in by_years.items():
            frames_folder = config.output.folder_images.format(station)

            if not os.path.isdir(frames_folder):
                log.warning("Pasta de frames não encontrada", station=station, years=years)
                continue

            station_dir = os.path.join(exports_root, station)
            os.makedirs(station_dir, exist_ok=True)

            video_path = config.output.video_path_template.format(station, years)

            # --- Vídeo ---
            if config.output.make_video:
                try:
                    create_video(
                        frames_folder=frames_folder,
                        output_path=video_path,
                        width=rc.image_width,
                        height=rc.image_height,
                        fps=rc.fps_video,
                    )
                    log.info("Vídeo gerado", station=station, years=years, path=video_path)
                except Exception as exc:
                    log.error("Falha ao gerar vídeo", station=station, error=str(exc))
                    raise ExportError(f"[{station}/{years}y vídeo] {exc}") from exc

                # --- GIF ---
                try:
                    gif_path = os.path.splitext(video_path)[0] + ".gif"
                    create_gif(
                        video_path=video_path,
                        output_gif=gif_path,
                        speed_multiplier=rc.gif_speed_multiplier,
                    )
                    log.info("GIF gerado", station=station, years=years, path=gif_path)
                except Exception as exc:
                    log.warning("Falha ao gerar GIF (não crítico)", station=station, error=str(exc))

    # --- Relatório consolidado ---
    if config.output.save_final_result:
        report = _build_final_report(context)
        report_path = os.path.join(exports_root, "FinalResult.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "pipeline_run_id": context.run_id,
                    "generated_at": context.started_at.isoformat() if context.started_at else "",
                    "results": report,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        log.info("Relatório final salvo", path=report_path, stations=len(report))

    context.stages_executed.append("s07_export")
    log.info("Stage 7 finalizado — pipeline completo!")
    return context
