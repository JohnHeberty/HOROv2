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
from pipeline.utils.windrose_mpl import WindRosePlotter

log = get_logger("s06_optimize")

# Janelas temporais serão calculadas dinamicamente baseado nos dados disponíveis

# Cores carregadas de config_runway.json via config.render.video_band_colors_bgr

# Instância do plotter de windrose
_windrose_plotter = WindRosePlotter()


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

            # Cor sólida da legenda — sem degrêê
            image[cell_mask] = base_bgr

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
# Utilitário: legenda de cores (bandas de velocidade)
# ---------------------------------------------------------------------------
def _draw_color_legend(
    image: np.ndarray,
    config: PipelineConfig,
    start_x: int,
    start_y: int,
) -> None:
    """
    Desenha uma legenda de cores mostrando as bandas de velocidade do vento.
    Cada banda é representada por um retângulo colorido + label.
    
    Args:
        image: Imagem onde desenhar
        config: Configuração do pipeline
        start_x: Coordenada X inicial (canto superior esquerdo)
        start_y: Coordenada Y inicial
    """
    rc = config.render
    wc = config.wind
    
    # Dimensões de cada retângulo de cor (maior para melhor visualização)
    box_width = 70
    box_height = 40
    text_offset_x = box_width + 15
    line_spacing = box_height + 12
    
    # Monta as labels das bandas
    limits = wc.limits_kts
    band_labels = [
        f"[0-{limits[0]:.0f} kt]",
        f"[{limits[0]:.0f}-{limits[1]:.0f} kt]",
        f"[{limits[1]:.0f}-{limits[2]:.0f} kt]",
        f"[{limits[2]:.0f}-{limits[3]:.0f} kt]",
        f"[{limits[3]:.0f}-{limits[4]:.0f} kt]",
        f"[{limits[4]:.0f}+ kt]",
    ]
    
    # Título da legenda (padronizado com os outros textos)
    cv.putText(image, "WIND SPEED:", (start_x, start_y - 15),
               rc.font, rc.font_size, (255, 255, 255), rc.font_thickness, cv.LINE_AA)
    
    # Desenha cada banda
    for i, (color_bgr, label) in enumerate(zip(rc.video_band_colors_bgr, band_labels)):
        y_pos = start_y + i * line_spacing
        
        # Retângulo colorido
        cv.rectangle(image, 
                    (start_x, y_pos),
                    (start_x + box_width, y_pos + box_height),
                    color_bgr, -1)  # -1 = preenchido
        
        # Borda branca
        cv.rectangle(image,
                    (start_x, y_pos),
                    (start_x + box_width, y_pos + box_height),
                    (255, 255, 255), 2)
        
        # Label da banda (padronizado com os outros textos)
        cv.putText(image, label,
                  (start_x + text_offset_x, y_pos + box_height - 10),
                  rc.font, rc.font_size, (255, 255, 255), rc.font_thickness, cv.LINE_AA)


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
    windrose_img: Optional[np.ndarray] = None,
) -> None:
    """Renderiza um único frame e salva como JPEG."""
    rc  = config.render
    img = base_image.copy()

    # ---- Retângulo da MELHOR pista (verde) — na melhor posição encontrada até agora ----
    _draw_runway_rect(img, center, crosswind_r, comprimento,
                      best_heading, rc.color_best_runway, 2)
    # Bolinha sempre no extremo best_heading (onde a varredura parou)
    draw_reference_point(img, center, comprimento, best_heading,
                         (0, 210, 0), rc.point_ref_size)

    # ---- Numeração de pista (verde): cabeceira em cada extremo ----
    cx, cy = center
    _head_a, _head_b = headboard_runway(best_heading).split("-")  # ex: "11", "29"
    # _head_a sempre corresponde ao ângulo best_heading; _head_b ao extremo oposto
    _best_dot_h = best_heading % 360.0
    _far_dot_h  = (_best_dot_h + 180.0) % 360.0
    _dot_label  = _head_a   # cabeceira no lado da bolinha (best_heading)
    _far_label  = _head_b   # cabeceira no extremo oposto

    # Padronização de fonte para cabeceiras (mesmo tamanho que paineis)
    _fsize_rwy = rc.font_size
    _fthick_rwy = rc.font_thickness
    _offs = comprimento + 55

    for _lbl, _ang in ((_dot_label, _best_dot_h), (_far_label, _far_dot_h)):
        _rad = np.radians(-_ang - 180)
        _lx = int(cx + _offs * np.sin(_rad))
        _ly = int(cy + _offs * np.cos(_rad))
        (_lw, _lh), _ = cv.getTextSize(_lbl, rc.font, _fsize_rwy, _fthick_rwy)
        cv.putText(img, _lbl, (_lx - _lw // 2, _ly + _lh // 2),
                   rc.font, _fsize_rwy, rc.color_best_runway, _fthick_rwy, cv.LINE_AA)

    # ---- Retângulo da pista ATUAL (branco, mais grosso) ----
    _draw_runway_rect(img, center, crosswind_r, comprimento,
                      heading_deg, rc.color_runway, 3)
    # Sem bolinha na pista branca — o retângulo já indica a direção e evita
    # confusão quando heading_deg ≈ best_heading + 180° (dois extremos opostos)

    # ---- Parâmetros de texto (padronizados) ----
    font   = rc.font
    fsize  = rc.font_size
    fthick = rc.font_thickness
    lspace = rc.legend_y_spacing
    cross_now  = round(100.0 - fo_pct,  2)
    cross_best = round(100.0 - best_fo, 2)

    # ----- Centralização vertical dos painéis esquerdos -----
    # Calcula altura total dos painéis + legenda
    green_panel_lines = 5  # BEST DIRECTION + 4 linhas
    white_panel_lines = 5  # DIRECTION NOW + 4 linhas
    legend_lines = 7       # Wind Speed: + 6 bandas
    
    # Altura total dos painéis + espaçamentos
    green_height = green_panel_lines * lspace
    white_height = white_panel_lines * lspace
    legend_height = legend_lines * 52  # line_spacing da legenda
    
    spacing_between = int(lspace * 2)  # Espaço entre painéis
    total_height = green_height + spacing_between + white_height + spacing_between + legend_height
    
    # Centraliza verticalmente
    start_y = (rc.image_height - total_height) // 2
    
    # ---- Painel ESQUERDO SUPERIOR — melhor pista (verde) ----
    lx  = rc.legend_x_left
    ly0 = start_y
    left_lines = [
        "BEST DIRECTION",
        f"FO: {best_fo:.2f}%",
        f"RUMO: {int(best_heading):03d}",
        f"ORIENTATION: {headboard_runway(best_heading)}",
        f"CROSS WIND: {cross_best:.2f}%",
    ]
    for i, line in enumerate(left_lines):
        cv.putText(img, line.upper(), (lx, ly0 + i * lspace),
                   font, fsize, (0, 255, 0), fthick, cv.LINE_AA)

    # ---- Painel ESQUERDO MEIO — pista atual (branco) ----
    ly_white = ly0 + green_height + spacing_between
    white_lines = [
        "DIRECTION NOW",
        f"FO: {fo_pct:.2f}%",
        f"RUMO: {int(heading_deg):03d}",
        f"ORIENTATION: {headboard_runway(heading_deg)}",
        f"CROSS WIND: {cross_now:.2f}%",
    ]
    for i, line in enumerate(white_lines):
        cv.putText(img, line.upper(), (lx, ly_white + i * lspace),
                   font, fsize, (255, 255, 255), fthick, cv.LINE_AA)
    
    # ---- Legenda de cores (ESQUERDO INFERIOR, equidistante) ----
    legend_y_start = ly_white + white_height + spacing_between
    _draw_color_legend(img, config, lx, legend_y_start)
    
    # ---- Info do aeroporto (lado DIREITO superior) ----
    lat_dms, lat_dir, lon_dms, lon_dir = latlon_to_grau_minuto(lat, lon)
    rx = rc.legend_x_right
    ry0_info = lspace * 2
    info_lines = [
        f"STATION: {station_name.upper()}",
        f"WINDOW: {years} YEARS",
        f"LAT: {lat_dms} {lat_dir}",
        f"LON: {lon_dms} {lon_dir}",
    ]
    for i, line in enumerate(info_lines):
        cv.putText(img, line, (rx, ry0_info + i * lspace),
                   font, fsize, (210, 210, 210), fthick, cv.LINE_AA)
    
    # ---- Declinação magnética (lado DIREITO, abaixo da info) ----
    # Usa " DEG" (ASCII) pois fontes OpenCV HERSHEY não suportam símbolo de grau
    decl_y = ry0_info + len(info_lines) * lspace + int(lspace * 1.5)
    cv.putText(img, f"MAGNETIC DECLINATION: {declination:.1f} DEG", (rx, decl_y),
               font, fsize, (255, 200, 100), fthick, cv.LINE_AA)
    
    # ---- Rosa dos ventos NOAA (lado DIREITO, alinhada) ----
    if windrose_img is not None:
        try:
            # Redimensiona windrose (+20% em relação a 360px base)
            wr_height, wr_width = windrose_img.shape[:2]
            target_width = 432  # 360 * 1.20 = 432 (+20%)
            scale = target_width / wr_width
            target_height = int(wr_height * scale)

            windrose_resized = cv.resize(windrose_img, (target_width, target_height),
                                        interpolation=cv.INTER_LANCZOS4)

            # Alinha à direita com margem de 10px (desloca ~20% para direita vs posição anterior)
            wr_x_start = rc.image_width - target_width - 10
            # Y: mesma altura da legenda; clampado para não sair da imagem pela base
            wr_y_start = min(legend_y_start, rc.image_height - target_height - 10)

            # Certifica que não ultrapassa os limites
            if wr_y_start >= 0 and wr_x_start >= 0:
                # Insere a windrose na imagem
                img[wr_y_start:wr_y_start + target_height,
                    wr_x_start:wr_x_start + target_width] = windrose_resized
        except Exception as e:
            # Se falhar, apenas loga mas não quebra o frame
            log.warning("Erro ao inserir windrose no frame", error=str(e))

    # ---- Salva frame (contorno para caminhos não-ASCII no Windows) ----
    os.makedirs(frames_folder, exist_ok=True)
    jpg_path = os.path.join(frames_folder, f"{frame_idx:04d}.jpg")
    ok, encoded = cv.imencode(".jpg", img, [cv.IMWRITE_JPEG_QUALITY, 70])
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

                # ---- Carrega imagem da rosa dos ventos do NOAA (declinação magnética) ----
                windrose_img = None
                try:
                    # Primeiro tenta carregar a rosa dos ventos extraída do mapa NOAA
                    noaa_windrose_path = os.path.join(config.output.data_silver, "noaa_windrose.png")
                    noaa_screenshot_path = os.path.join(config.output.data_silver, "noaa_after_calc.png")
                    
                    # Prioriza a rosa dos ventos extraída
                    noaa_path = noaa_windrose_path if os.path.exists(noaa_windrose_path) else noaa_screenshot_path
                    
                    if os.path.exists(noaa_path):
                        # Carrega imagem do NOAA com np.fromfile para suportar caminhos Unicode
                        file_bytes = np.fromfile(noaa_path, dtype=np.uint8)
                        windrose_img = cv.imdecode(file_bytes, cv.IMREAD_COLOR)
                        
                        if windrose_img is not None:
                            log.info("Windrose NOAA carregada", station=station, 
                                    years=years, shape=windrose_img.shape, source=noaa_path)
                        else:
                            log.warning("Falha ao decodificar windrose NOAA", station=station, 
                                       years=years, path=noaa_path)
                    else:
                        # Sem fallback: se NOAA não disponível, não mostra imagem
                        log.warning("Imagem NOAA não encontrada, vídeo será gerado sem rosa dos ventos", 
                                   station=station, paths=[noaa_windrose_path, noaa_screenshot_path])
                except Exception as exc:
                    log.warning("Erro ao carregar windrose (não crítico)", 
                               station=station, years=years, error=str(exc))

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
                        windrose_img=windrose_img,
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
                        windrose_img=windrose_img,
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
                            windrose_img=windrose_img,
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
                            windrose_img=windrose_img,
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