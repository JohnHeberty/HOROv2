"""
pipeline/utils/windrose_plot.py
=================================
Geração de anemograma (rosa dos ventos) usando matplotlib/windrose.

Cria visualização estilo publicação científica com as mesmas bandas
de velocidade configuradas no sistema.
"""

from __future__ import annotations

import os
from typing import List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from windrose import WindroseAxes

from pipeline.core.logger import get_logger

log = get_logger("utils.windrose_plot")


def create_windrose_plot(
    df: pd.DataFrame,
    output_path: str,
    title: str = "Wind Rose",
    speed_bins: Optional[List[float]] = None,
    width: float = 10.0,
    height: float = 10.0,
    dpi: int = 150,
) -> None:
    """
    Gera anemograma (rosa dos ventos) usando matplotlib/windrose.

    Args:
        df: DataFrame com colunas 'direction_mag' (direção) e 'speed_kts' (velocidade em nós)
        output_path: Caminho para salvar a imagem PNG
        title: Título do gráfico
        speed_bins: Limites das bandas de velocidade [3, 13, 20, 25, 40]
        width: Largura da figura em polegadas
        height: Altura da figura em polegadas
        dpi: Resolução da imagem
    """
    if df.empty or "direction_mag" not in df.columns or "speed_kts" not in df.columns:
        log.warning("DataFrame vazio ou sem colunas necessárias", path=output_path)
        return

    # Limites padrão conforme RBAC154
    if speed_bins is None:
        speed_bins = [3, 13, 20, 25, 40]

    # Remove valores nulos
    df_clean = df[["direction_mag", "speed_kts"]].dropna()
    if len(df_clean) < 3:
        log.warning("Dados insuficientes para gerar windrose", path=output_path)
        return

    direction = df_clean["direction_mag"].values
    speed = df_clean["speed_kts"].values

    # Adiciona 0 no início para criar banda [0-3]
    bins = [0] + speed_bins

    # Cria figura
    fig = plt.figure(figsize=(width, height), dpi=dpi, facecolor="white")
    ax = WindroseAxes.from_ax(fig=fig)

    # Plota rosa dos ventos
    ax.bar(
        direction,
        speed,
        normed=True,
        opening=0.9,
        edgecolor="white",
        bins=bins,
        cmap=plt.cm.viridis,
        nsector=16,
    )

    # Configurações
    ax.set_legend(
        title="Wind Speed (kt)",
        loc="lower left",
        bbox_to_anchor=(-0.15, -0.15),
        fontsize=9,
    )
    ax.set_title(title, fontsize=14, fontweight="bold", pad=20)

    # Ajusta layout
    plt.tight_layout()

    # Salva
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)

    log.info("Windrose plot gerado", path=output_path, n_samples=len(df_clean))
