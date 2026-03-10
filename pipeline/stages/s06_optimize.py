"""
pipeline/stages/s06_optimize.py
=================================
Stage 6 — OPTIMIZE: Silver → Gold (Resultados)

Responsabilidades:
  - Para cada estação e cada janela temporal:
    * Aplica declinação magnética à direção do vento
    * Varre todos os ângulos de 0 a 179° calculando o FO matematicamente
    * Identifica a melhor orientação de pista
    * Gera os frames de simulação visual:
        - Rosa dos ventos colorida (bandas de velocidade) preenchendo o frame
        - Retângulo de pista rotacionado (branco = atual, verde = melhor)
        - Painel esquerdo (verde): BEST DIRECTION
        - Painel direito (branco): DIRECTION NOW
        - Info do aeroporto na base
  - Salva JSON de resultados em data/gold/

Entrada:  context.silver
Saída:    data/gold/FinalResult.json
          data/gold/exports/{station}/frames/*.jpg
          context.results[station][years] = RunwayOptimizationResult
"""

from __future__ import annotations

import json
import os
from typing import Dict, List, Optional, Tuple

import cv2 as cv
import numpy as np
import pandas as pd

from pipeline.core.config import PipelineConfig, cfg
from pipeline.core.logger import get_logger
from pipeline.core.models import PipelineContext, RunwayOptimizationResult
from pipeline.services.drawing import draw_reference_point
from pipeline.services.runway import calcular_fo, headboard_runway, otimizar_orientacao
from pipeline.services.wind import (
    angulos_rosa,
    calcular_setores,
    calcular_tabela_ventos,
    get_column_titles,
    pistas_possiveis,
)
from pipeline.utils.geo import latlon_to_grau_minuto

log = get_logger("s06_optimize")

# Janelas temporais serão calculadas dinamicamente baseado nos dados disponíveis

# Cores carregadas de config_runway.json via config.render.video_band_colors_bgr


# ---------------------------------------------------------------------------
# Construção da imagem base (executado UMA vez por estação × janela)
# ---------------------------------------------------------------------------
def _build_base_image(
    df_slice: pd.DataFrame,
    config: PipelineConfig,
) -> Tuple[np.ndarray, int, int, Tuple[int, int]]:
    """
    Desenha a rosa dos ventos colorida sobre um fundo preto.

    Cada célula (setor × banda de velocidade) é preenchida com a cor da banda
    modulada pela frequência relativa do vento naquela célula.

    Retorna:
        (base_image, comprimento_px, crosswind_radius_px, center)
    """
    rc = config.render
    wc = config.wind
    W, H = rc.image_width, rc.image_height
    cx, cy = W // 2, H // 2
    center: Tuple[int, int] = (cx, cy)

    limits    = wc.limits_kts                      # [3, 13, 20, 25, 40]
    n_sec     = wc.sectors                          # 16
    sec_names = wc.sector_names[n_sec]
    proporcao = rc.wind_rose_proportion / max(limits)          # 0.005
    comprimento  = int(W * max(limits) * proporcao)            # ≈ 384 px
    crosswind_r  = int(W * wc.crosswind_limit_kts * proporcao) # ≈ 192 px

    # ---- Tabela de ventos (setor × banda) em % ----
    wind_table = calcular_tabela_ventos(
        df_slice["direction_mag"],
        df_slice["speed_kts"],
        sec_names,
        limits,
    )
    col_titles = get_column_titles(limits)   # ['[0-3]', '[3-13]', ..., '[25-40]', '[40-*]']
    max_pct    = float(wind_table.values.max()) or 1.0

    # ---- Fundo cinza escuro ----
    image = np.full((H, W, 3), rc.background_gray, dtype=np.uint8)
    
    # ---- Círculo de fundo da rosa dos ventos (cinza mais claro) ----
    cv.circle(image, center, comprimento, rc.windrose_background, -1)

    # ---- Grades de coordenadas polares ----
    Yg, Xg = np.ogrid[:H, :W]
    dx = Xg.astype(float) - cx
    dy = Yg.astype(float) - cy
    R  = np.sqrt(dx ** 2 + dy ** 2)
    # Ângulo a partir do Norte, sentido horário (0°=N, 90°=E, 180°=S, 270°=W)
    ANGLE = (np.degrees(np.arctan2(dx, -dy)) + 360.0) % 360.0

    # ---- Preenche cada célula (setor × banda) ----
    setores = calcular_setores(n_sec, sec_names)

    for b_idx in range(len(limits)):
        col_name = col_titles[b_idx]
        r_outer  = int(W * limits[b_idx]                  * proporcao)
        r_inner  = int(W * limits[b_idx - 1]              * proporcao) if b_idx > 0 else 0
        palette  = config.render.video_band_colors_bgr
        base_bgr = palette[min(b_idx, len(palette) - 1)]
        r_mask   = (R >= r_inner) & (R < r_outer)

        for s_name, (s_start, s_end) in setores.items():
            if s_start > s_end:   # setor cruza 0°/360°
                ang_mask = (ANGLE >= s_start) | (ANGLE < s_end)
            else:
                ang_mask = (ANGLE >= s_start) & (ANGLE < s_end)

            cell_mask = r_mask & ang_mask
            if not cell_mask.any():
                continue

            try:
                pct = float(wind_table.loc[s_name, col_name])
            except KeyError:
                pct = 0.0

            if pct <= 0.0:
                continue

            # Degradê suave: cores variam de 35% (pouco vento) até 100% (muito vento) dentro da banda
            brightness = 0.35 + 0.65 * (pct / max_pct)
            color = tuple(min(255, int(c * brightness)) for c in base_bgr)
            image[cell_mask] = color

    # ---- Bordas dos círculos concêntricos (branco) ----
    for i, limit in enumerate(limits):
        r = int(W * limit * proporcao)
        cv.circle(image, center, r, (255, 255, 255), 2)

    # ---- Círculo do limite de crosswind (branco espesso) — sempre desenhado ----
    # mesmo que crosswind_limit_kts não esteja em limits_kts
    cv.circle(image, center, crosswind_r, (255, 255, 255), 4)

    cv.circle(image, center, comprimento, (255, 255, 255), 3)   # borda externa mais grossa

    # ---- Divisores de setor (linhas radiais brancas) ----
    for _s_name, (s_start, _s_end) in setores.items():
        rad = np.radians(-s_start - 180)
        p2  = (int(cx + comprimento * np.sin(rad)),
               int(cy + comprimento * np.cos(rad)))
        cv.line(image, center, p2, (255, 255, 255), 2)

    # ---- Labels cardeais e colaterais (N, NE, E, SE, S, SW, W, NW) ----
    cardinal = {
        0: "N", 45: "NE", 90: "E", 135: "SE", 
        180: "S", 225: "SW", 270: "W", 315: "NW"
    }
    for angle_deg, label in cardinal.items():
        rad    = np.radians(-angle_deg - 180)
        offset = comprimento + 35
        px     = int(cx + offset * np.sin(rad))
        py     = int(cy + offset * np.cos(rad))
        (tw, th), _ = cv.getTextSize(label, rc.font, 0.8, 2)
        cv.putText(image, label, (px - tw // 2, py + th // 2),
                   rc.font, 0.8, (240, 240, 240), 2, cv.LINE_AA)

    return image, comprimento, crosswind_r, center


# ---------------------------------------------------------------------------
# Utilitário: retângulo de pista rotacionado
# ---------------------------------------------------------------------------
def _draw_runway_rect(
    image: np.ndarray,
    center: Tuple[int, int],
    half_width: int,
    half_height: int,
    heading_deg: float,
    color: Tuple[int, int, int],
    thickness: int = 2,
) -> None:
    """
    Desenha um retângulo representando a largura e comprimento da pista,
    rotacionado pelo ângulo *heading_deg* (Norte=0°, sentido horário).

    half_width  = raio do limite de vento cruzado (crosswind_r)
    half_height = raio total da rosa (comprimento)
    """
    cx, cy = center
    corners = np.array([
        [-half_width, -half_height],
        [ half_width, -half_height],
        [ half_width,  half_height],
        [-half_width,  half_height],
    ], dtype=float)

    theta = np.radians(heading_deg)
    cos_t, sin_t = np.cos(theta), np.sin(theta)
    rot = np.array([[cos_t, -sin_t], [sin_t, cos_t]])
    rotated = corners @ rot.T + np.array([cx, cy])
    pts = rotated.astype(np.int32)
    cv.polylines(image, [pts], True, color, thickness)


# ---------------------------------------------------------------------------
# Renderização de frame individual
# ---------------------------------------------------------------------------
def _render_frame(
    base_image: np.ndarray,
    comprimento: int,
    crosswind_r: int,
    center: Tuple[int, int],
    best_heading: float,
    best_fo: float,
    heading_deg: float,
    fo_pct: float,
    station_name: str,
    lat: float,
    lon: float,
    declination: float,
    years: int,
    frame_idx: int,
    frames_folder: str,
    config: PipelineConfig,
) -> None:
    """Renderiza um único frame e salva como JPEG."""
    rc  = config.render
    img = base_image.copy()

    # ---- Helper: retorna o ângulo do extremo que aponta para o Norte (parte superior da tela) ----
    def _north_h(h: float) -> float:
        """De um par de cabeceiras, retorna a que fica no semi-plano norte (y < cy)."""
        h_n = h % 360.0
        return h_n if (h_n <= 90.0 or h_n >= 270.0) else (h_n + 180.0) % 360.0

    # ---- Retângulo da MELHOR pista (verde) — na melhor posição encontrada até agora ----
    _draw_runway_rect(img, center, crosswind_r, comprimento,
                      best_heading, rc.color_best_runway, 2)
    # Bolinha apenas no extremo norte da pista verde
    best_north_h = _north_h(best_heading)
    draw_reference_point(img, center, comprimento, best_north_h,
                         (0, 210, 0), rc.point_ref_size)

    # ---- Numeração de pista (verde) próxima à bolinha norte ----
    cx, cy = center
    rwy_text = headboard_runway(best_heading).replace("-", "/")
    rwy_label = f"RWY {rwy_text}"
    _rad_rwy = np.radians(-best_north_h - 180)
    _offs = comprimento + 55
    _tx = int(cx + _offs * np.sin(_rad_rwy))
    _ty = int(cy + _offs * np.cos(_rad_rwy))
    (_tw, _th), _ = cv.getTextSize(rwy_label, rc.font, rc.font_size * 0.85, rc.font_thickness)
    cv.putText(img, rwy_label, (_tx - _tw // 2, _ty + _th // 2),
               rc.font, rc.font_size * 0.85, rc.color_best_runway, rc.font_thickness + 1, cv.LINE_AA)

    # ---- Retângulo da pista ATUAL (branco, mais grosso) ----
    _draw_runway_rect(img, center, crosswind_r, comprimento,
                      heading_deg, rc.color_runway, 3)
    # Bolinha apenas no extremo norte da pista branca (sem texto)
    draw_reference_point(img, center, comprimento, _north_h(heading_deg),
                         rc.color_point_ref, rc.point_ref_size)

    # ---- Parâmetros de texto ----
    font   = rc.font
    fsize  = rc.font_size
    fthick = rc.font_thickness
    lspace = rc.legend_y_spacing
    cross_now  = round(100.0 - fo_pct,  2)
    cross_best = round(100.0 - best_fo, 2)

    # ---- Painel DIREITO — pista atual (branco) ----
    right_lines = [
        "DIRECTION NOW",
        f"FO: {fo_pct:.2f}%",
        f"RUMO: {int(heading_deg):03d}",
        f"MAGNETIC DECLINATION: {declination:.1f}",
        f"RUNWAY ORIENTATION: {headboard_runway(heading_deg)}",
        f"CROSS WIND: {cross_now:.2f}%",
    ]
    rx  = rc.legend_x_right
    ry0 = lspace * 2
    for i, line in enumerate(right_lines):
        cv.putText(img, line, (rx, ry0 + i * lspace),
                   font, fsize, (255, 255, 255), fthick, cv.LINE_AA)

    # ---- Painel ESQUERDO — melhor pista (verde) — sempre visível ----
    lx  = rc.legend_x_left
    ly0 = lspace * 2
    left_lines = [
        "BEST DIRECTION",
        f"FO: {best_fo:.2f}%",
        f"RUMO: {int(best_heading):03d}",
        f"MAGNETIC DECLINATION: {declination:.1f}",
        f"RUNWAY ORIENTATION: {headboard_runway(best_heading)}",
        f"CROSS WIND: {cross_best:.2f}%",
    ]
    for i, line in enumerate(left_lines):
        cv.putText(img, line, (lx, ly0 + i * lspace),
                   font, fsize, (0, 255, 0), fthick, cv.LINE_AA)

    # ---- Info do aeroporto (base) ----
    lat_dms, lat_dir, lon_dms, lon_dir = latlon_to_grau_minuto(lat, lon)
    bottom_lines = [
        f"Station: {station_name}  |  {years}-year window",
        f"Lat: {lat_dms} {lat_dir}  /  Lon: {lon_dms} {lon_dir}",
    ]
    by0 = rc.image_height - lspace * (len(bottom_lines) + 1)
    for i, line in enumerate(bottom_lines):
        cv.putText(img, line, (lx, by0 + i * lspace),
                   font, fsize, (210, 210, 210), fthick, cv.LINE_AA)

    # ---- Salva frame (contorno para caminhos não-ASCII no Windows) ----
    os.makedirs(frames_folder, exist_ok=True)
    jpg_path = os.path.join(frames_folder, f"{frame_idx:04d}.jpg")
    ok, encoded = cv.imencode(".jpg", img, [cv.IMWRITE_JPEG_QUALITY, 90])
    if ok:
        with open(jpg_path, "wb") as fh:
            fh.write(encoded.tobytes())


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------
def run(context: PipelineContext, config: PipelineConfig = cfg) -> PipelineContext:
    """Otimiza orientação de pista para cada estação e janela temporal."""
    log.info("=== STAGE 6 — OPTIMIZE (Silver → Gold) ===")

    os.makedirs(config.output.data_gold, exist_ok=True)

    for station, silver in context.silver.items():
        df_all      = silver.df
        declination = silver.magnetic_declination or 0.0
        lat         = silver.metadata.latitude
        lon         = silver.metadata.longitude
        context.results[station] = {}

        # Calcula janelas temporais baseado nos dados disponíveis
        if df_all.empty:
            log.warning("DataFrame vazio", station=station)
            continue
        
        max_date = df_all["timestamp"].max()
        min_date = df_all["timestamp"].min()
        anos_disponiveis = (max_date - min_date).days / 365.25
        
        # Define janelas possíveis (somente as que temos dados)
        possible_windows = [5, 10, 15, 20]
        year_windows = [y for y in possible_windows if y <= anos_disponiveis]
        
        # Se temos menos de 1 ano, usa todos os dados disponíveis como janela única
        if not year_windows:
            year_windows = [int(anos_disponiveis) if anos_disponiveis >= 1 else 1]
        
        log.info("Otimizando", station=station, declination=declination, 
                 anos_disponiveis=f"{anos_disponiveis:.1f}", janelas=year_windows)

        for years in year_windows:
            from dateutil.relativedelta import relativedelta  # type: ignore

            max_date = df_all["timestamp"].max()
            cutoff   = max_date - relativedelta(years=years)
            df_slice = df_all[df_all["timestamp"] >= cutoff].copy()

            if len(df_slice) < 10:
                continue

            # Aplica declinação magnética à direção
            df_slice["direction_mag"] = (df_slice["direction"] + declination) % 360

            try:
                fo_map = otimizar_orientacao(
                    direcao=df_slice["direction_mag"],
                    magnitude=df_slice["speed_kts"],
                    crosswind_limit_kts=config.wind.crosswind_limit_kts,
                    keep_calms=config.data.keep_calms,
                )

                best_heading_final = max(fo_map, key=fo_map.get)  # type: ignore[arg-type]
                fo_best, cross_pct, calm_pct = calcular_fo(
                    df_slice["direction_mag"],
                    df_slice["speed_kts"],
                    heading_deg=best_heading_final,
                    crosswind_limit_kts=config.wind.crosswind_limit_kts,
                    keep_calms=config.data.keep_calms,
                )

                designation = headboard_runway(best_heading_final)

                result = RunwayOptimizationResult(
                    station=station,
                    period_years=years,
                    best_heading_deg=best_heading_final,
                    runway_designation=designation,
                    fo_pct=fo_best,
                    crosswind_pct=cross_pct,
                    calm_pct=calm_pct,
                    magnetic_declination=declination,
                    fo_by_heading=fo_map,
                )
                context.results[station][years] = result

                log.info(
                    "Resultado",
                    station=station,
                    years=years,
                    runway=designation,
                    fo=f"{fo_best:.1f}%",
                    crosswind=f"{cross_pct:.1f}%",
                )

                # ---- Constrói imagem base UMA VEZ por estação × janela ----
                # Cria pasta separada para cada janela temporal
                frames_folder = os.path.join(
                    config.output.data_gold, "exports", station, f"{years}y", "frames"
                )
                os.makedirs(frames_folder, exist_ok=True)

                base_image, comprimento, crosswind_r, rose_center = \
                    _build_base_image(df_slice, config)

                # ---- Gera frames 0–179°: verde atualiza quando encontra novo máximo ----
                best_h_so_far:  float = 0.0
                best_fo_so_far: float = 0.0

                for frame_idx in range(config.render.max_spin_deg):
                    heading = float(frame_idx)
                    fo_now  = fo_map.get(heading, fo_map.get(int(heading), 0.0))

                    # Atualiza melhor posição sempre que FO supera o máximo anterior
                    if fo_now > best_fo_so_far:
                        best_fo_so_far = fo_now
                        best_h_so_far  = heading

                    _render_frame(
                        base_image=base_image,
                        comprimento=comprimento,
                        crosswind_r=crosswind_r,
                        center=rose_center,
                        best_heading=best_h_so_far,
                        best_fo=best_fo_so_far,
                        heading_deg=heading,
                        fo_pct=fo_now,
                        station_name=station,
                        lat=lat,
                        lon=lon,
                        declination=declination,
                        years=years,
                        frame_idx=frame_idx,
                        frames_folder=frames_folder,
                        config=config,
                    )
                
                # ---- Gera frames finais FIXOS na melhor posição ----
                n_final_frames = 30
                for i in range(n_final_frames):
                    _render_frame(
                        base_image=base_image,
                        comprimento=comprimento,
                        crosswind_r=crosswind_r,
                        center=rose_center,
                        best_heading=best_heading_final,
                        best_fo=fo_best,
                        heading_deg=best_heading_final,
                        fo_pct=fo_best,
                        station_name=station,
                        lat=lat,
                        lon=lon,
                        declination=declination,
                        years=years,
                        frame_idx=config.render.max_spin_deg + i,
                        frames_folder=frames_folder,
                        config=config,
                    )

                log.info(
                    "Frames gerados",
                    station=station,
                    years=years,
                    n_frames=config.render.max_spin_deg + n_final_frames,
                )

                # ---- GIF frames: spin 0→359° + 30 fixos (GIF 360°) — bloco isolado ----
                try:
                    gif_frames_folder = os.path.join(
                        config.output.data_gold, "exports", station, f"{years}y", "gif_frames"
                    )
                    os.makedirs(gif_frames_folder, exist_ok=True)

                    _best_h_gif: float  = 0.0
                    _best_fo_gif: float = 0.0

                    for gif_idx in range(config.render.gif_spin_deg):
                        heading_gif = float(gif_idx)
                        # fo_map cobre 0–179; para ângulos além disso usa o espelho
                        _key_int = int(heading_gif) % config.render.max_spin_deg
                        fo_gif   = fo_map.get(float(_key_int), fo_map.get(_key_int, 0.0))

                        # Verde só é atualizado na primeira meia-volta (0 até max_spin_deg)
                        if gif_idx < config.render.max_spin_deg and fo_gif > _best_fo_gif:
                            _best_fo_gif = fo_gif
                            _best_h_gif  = heading_gif

                        _render_frame(
                            base_image=base_image,
                            comprimento=comprimento,
                            crosswind_r=crosswind_r,
                            center=rose_center,
                            best_heading=_best_h_gif,
                            best_fo=_best_fo_gif,
                            heading_deg=heading_gif,
                            fo_pct=fo_gif,
                            station_name=station,
                            lat=lat,
                            lon=lon,
                            declination=declination,
                            years=years,
                            frame_idx=gif_idx,
                            frames_folder=gif_frames_folder,
                            config=config,
                        )

                    # Frames finais fixos para o GIF
                    for i in range(n_final_frames):
                        _render_frame(
                            base_image=base_image,
                            comprimento=comprimento,
                            crosswind_r=crosswind_r,
                            center=rose_center,
                            best_heading=best_heading_final,
                            best_fo=fo_best,
                            heading_deg=best_heading_final,
                            fo_pct=fo_best,
                            station_name=station,
                            lat=lat,
                            lon=lon,
                            declination=declination,
                            years=years,
                            frame_idx=config.render.gif_spin_deg + i,
                            frames_folder=gif_frames_folder,
                            config=config,
                        )

                    log.info(
                        "GIF frames gerados (360°)",
                        station=station,
                        years=years,
                        n_frames=config.render.gif_spin_deg + n_final_frames,
                    )
                except Exception as exc_gif:
                    log.warning(
                        "Falha nos GIF frames (nao critico)",
                        station=station,
                        years=years,
                        error=str(exc_gif),
                    )

            except Exception as exc:
                log.error(
                    "Falha na otimizacao",
                    station=station,
                    years=years,
                    error=str(exc),
                )

    # ---- Serializa resultados ----
    results_json: dict = {}
    for station, by_years in context.results.items():
        results_json[station] = {}
        for years_key, res in by_years.items():
            results_json[station][str(years_key)] = {
                "best_heading_deg":    res.best_heading_deg,
                "runway_designation":  res.runway_designation,
                "fo_pct":              res.fo_pct,
                "crosswind_pct":       res.crosswind_pct,
                "calm_pct":            res.calm_pct,
                "magnetic_declination": res.magnetic_declination,
            }

    json_path = os.path.join(config.output.data_gold, "FinalResult.json")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(results_json, fh, ensure_ascii=False, indent=2)

    log.info("Resultados salvos", path=json_path)
    return context