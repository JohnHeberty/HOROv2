"""
pipeline/services/runway.py
============================
Lógica de domínio de pista: cabeceiras, geometria e Fator de Operação.

Migrado de Functions.py (HeadboardRunway) e RUN_HORO.ipynb.
Usa método geométrico (área dentro do PPD) para calcular FO.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from pipeline.core.logger import get_logger

log = get_logger("services.runway")


def headboard_runway(pista_graus: float) -> str:
    """
    Converte um ângulo em graus para o par de cabeceiras de pista.

    Exemplos:
        87°  → "09-27"
        180° → "18-36"
        0°   → "36-18"   (0° normalizado para 360° → cabeceira 36)

    Args:
        pista_graus: Orientação magnética da pista em graus.

    Returns:
        String no formato "HH-HH" (ex.: "09-27").
    """
    graus      = float(pista_graus) % 360
    headboard  = int(round(graus / 10))
    if headboard == 0:
        headboard = 36
    opposite = headboard + 18 if headboard <= 18 else headboard - 18
    return f"{headboard:02d}-{opposite:02d}"


def calcular_fo(
    direcao: pd.Series,
    magnitude: pd.Series,
    heading_deg: float,
    crosswind_limit_kts: float = 20.0,
    keep_calms: bool = True,
) -> Tuple[float, float, float]:
    """
    Calcula o Fator de Operação (FO) pelo método direto ICAO/RBAC154.

    Para cada observação de vento, calcula o componente cruzado real:
        crosswind = |speed × sin(wind_dir - runway_heading)|

    Se crosswind ≤ crosswind_limit_kts → vento está dentro do envelope → contribui ao FO.
    Ventos calmos (speed == 0) sempre contribuem ao FO (ou são ignorados se keep_calms=False).

    Args:
        direcao:             Série de direções do vento (graus).
        magnitude:           Série de velocidades do vento (nós).
        heading_deg:         Orientação da pista em graus verdadeiros.
        crosswind_limit_kts: Limite de vento cruzado (kt).
        keep_calms:          Se True, ventos calmos contam como dentro do envelope.

    Returns:
        (fo_pct, crosswind_pct, calm_pct)
    """
    df = pd.DataFrame({"dir": direcao, "spd": magnitude}).dropna()
    if df.empty:
        return 0.0, 0.0, 0.0

    total = len(df)
    calms_mask = df["spd"] == 0
    calm_pct = calms_mask.mean() * 100.0

    non_calms = df[~calms_mask].copy()

    if len(non_calms) > 0:
        # Ângulo relativo entre a direção do vento e o eixo da pista
        delta_rad = np.radians(non_calms["dir"].values - heading_deg)
        # Componente cruzada = magnitude × |sin(delta)|
        crosswind = np.abs(non_calms["spd"].values * np.sin(delta_rad))
        inside_mask = crosswind <= crosswind_limit_kts
        n_inside = inside_mask.sum()
    else:
        n_inside = 0

    calms_inside = int(calms_mask.sum()) if keep_calms else 0
    fo_count = n_inside + calms_inside
    fo_pct = fo_count / total * 100.0

    fo_pct       = min(100.0, fo_pct)
    crosswind_pct = 100.0 - fo_pct

    return round(fo_pct, 3), round(crosswind_pct, 3), round(calm_pct, 3)


def otimizar_orientacao(
    direcao: pd.Series,
    magnitude: pd.Series,
    crosswind_limit_kts: float = 20.0,
    keep_calms: bool = True,
    step_deg: int = 1,
) -> Dict[float, float]:
    """
    Varre todas as orientações de pista de 0 a 179° em passos de *step_deg*
    e calcula o FO para cada uma.

    Returns:
        Dict { heading_deg: fo_pct } para todos os ângulos testados.
    """
    results: Dict[float, float] = {}
    for heading in range(0, 180, step_deg):
        fo, _, _ = calcular_fo(
            direcao, magnitude,
            heading_deg=float(heading),
            crosswind_limit_kts=crosswind_limit_kts,
            keep_calms=keep_calms,
        )
        results[float(heading)] = fo

    log.debug(
        "Otimização concluída",
        melhor_rumo=max(results, key=results.get),  # type: ignore[arg-type]
        fo_max=f"{max(results.values()):.2f}%",
    )
    return results
