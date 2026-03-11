"""
pipeline/utils/windrose_mpl.py
================================
Módulo independente de geração de rosa dos ventos com matplotlib.

Lê os limites de velocidade e comprimento de pista de `config_runway.json`
(ou usa valores padrão RBAC154) e gera a rosa dos ventos com as mesmas
bandas de cor usadas no vídeo:

    Bandas (kt):  [0-3], [3-13], [13-20], [20-25], [25-40], [40+]
    Cores:        cinza, azul, verde, amarelo, laranja, vermelho

Uso standalone:
    from pipeline.utils.windrose_mpl import WindRosePlotter
    import pandas as pd

    df = pd.read_parquet("data/silver/SBSP.parquet")
    plotter = WindRosePlotter()
    plotter.plot(df["direction_mag"], df["speed_kts"], output_path="output/windrose.png")

Uso integrado (pipeline):
    plotter.plot_from_config(df, station="SBSP", years=5, output_dir="data/gold/exports/SBSP")
"""

from __future__ import annotations

import json
import os
from typing import List, Optional, Tuple

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Paleta padrão — pode ser sobrescrita por config_runway.json na instância
_BAND_COLORS_RGB_DEFAULT: List[Tuple[float, float, float]] = [
    (0.706, 0.706, 0.706),   # [0-3]   cinza claro
    (0.118, 0.392, 1.000),   # [3-13]  azul forte
    (0.157, 1.000, 0.157),   # [13-20] verde vibrante
    (1.000, 1.000, 0.000),   # [20-25] amarelo brilhante
    (1.000, 0.588, 0.000),   # [25-40] laranja forte
    (1.000, 0.118, 0.000),   # [40+]   vermelho intenso
]


def _load_config(config_path: Optional[str] = None) -> dict:
    """Carrega config_runway.json da raiz do projeto."""
    if config_path is None:
        # Vai dois níveis acima: pipeline/utils/ → pipeline/ → raiz
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(root, "config_runway.json")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _crosswind_limit(runway_length_m: float) -> float:
    """Retorna limite de vento cruzado conforme RBAC154."""
    if runway_length_m >= 1500:
        return 20.0
    elif runway_length_m >= 1200:
        return 13.0
    return 10.0


class WindRosePlotter:
    """
    Rosa dos ventos em matplotlib com as mesmas bandas do vídeo HOROv1.

    Args:
        config_path: Caminho para config_runway.json.
                     Se None, busca automaticamente na raiz do projeto.
    """

    def __init__(self, config_path: Optional[str] = None) -> None:
        cfg = _load_config(config_path)
        _rosa  = cfg.get("rosa_dos_ventos", {})
        _pista = cfg.get("pista", {})

        # Bandas de velocidade — lidas de rosa_dos_ventos.wind_speed_bands_kts
        self.bands: List[float] = _rosa.get("wind_speed_bands_kts", [3, 13, 20, 25, 40])

        # Comprimento de pista e limite de crosswind — sempre via RBAC154
        self.runway_length_m: float = float(_pista.get("runway_length_m", 1500))
        self.crosswind_limit: float = _crosswind_limit(self.runway_length_m)

        # Número de setores (sempre 16 conforme RBAC154)
        self.n_sectors: int = 16

        # Paleta de cores — lida de rosa_dos_ventos.cores_rgb no config_runway.json
        _rosa = cfg.get("rosa_dos_ventos", {})
        raw = _rosa.get("cores_rgb")
        if isinstance(raw, list) and len(raw) >= 6:
            self.band_colors: List[Tuple[float, float, float]] = [
                (c[0] / 255.0, c[1] / 255.0, c[2] / 255.0) for c in raw
            ]
        else:
            self.band_colors = list(_BAND_COLORS_RGB_DEFAULT)

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def plot(
        self,
        direction: pd.Series,
        speed: pd.Series,
        output_path: str,
        title: str = "Wind Rose",
        figsize: Tuple[float, float] = (12, 10),
        dpi: int = 150,
    ) -> None:
        """
        Gera e salva a rosa dos ventos.

        Args:
            direction:   Série com direções do vento em graus (0-360).
            speed:       Série com velocidades do vento em nós.
            output_path: Caminho completo para salvar o PNG.
            title:       Título do gráfico.
            figsize:     Tamanho da figura (largura, altura) em polegadas.
            dpi:         Resolução da imagem.
        """
        df = pd.DataFrame({"dir": direction, "spd": speed}).dropna()
        if df.empty:
            return

        # Separa calmos
        calms = df[df["spd"] == 0]
        non_calms = df[df["spd"] > 0].copy()

        total = len(df)
        calm_pct = len(calms) / total * 100 if total > 0 else 0.0

        # Constrói bins de velocidade: [0] + [3, 13, 20, 25, 40] + [inf]
        bins = self.bands + [999.0]
        n_bands = len(bins)  # número de bandas de velocidade não-calmas

        # Divide em setores
        setor_deg = 360.0 / self.n_sectors
        sector_edges = np.arange(0, 360 + setor_deg, setor_deg) - setor_deg / 2
        sector_edges = sector_edges % 360

        # Matriz de frequência [n_sectors × n_bands]
        freq = np.zeros((self.n_sectors, n_bands))

        for i in range(self.n_sectors):
            s_start = sector_edges[i]
            s_end   = sector_edges[i + 1] if i < self.n_sectors - 1 else 360.0

            # Máscara de setor (lida com o setor que cruza 0°)
            if s_start > s_end:
                mask_dir = (non_calms["dir"] >= s_start) | (non_calms["dir"] < s_end)
            else:
                mask_dir = (non_calms["dir"] >= s_start) & (non_calms["dir"] < s_end)

            sector_data = non_calms.loc[mask_dir, "spd"]

            for b in range(n_bands):
                lo = self.bands[b - 1] if b > 0 else 0.0
                hi = bins[b]
                freq[i, b] = ((sector_data >= lo) & (sector_data < hi)).sum()

        # Normaliza para porcentagem do total (incluindo calmos)
        freq_pct = freq / total * 100  # shape: (n_sectors, n_bands)

        # ------------------------------------------------------------------
        # Desenha figura
        # ------------------------------------------------------------------
        fig = plt.figure(figsize=figsize, facecolor="white")
        # Margem esquerda suficiente para a legenda no canto sup. esq. sem sobrepor a rosa
        fig.subplots_adjust(left=0.28, right=0.97, bottom=0.09, top=0.93)
        ax = fig.add_subplot(111, polar=True)

        # Orientação: Norte no topo, sentido horário
        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)

        # Ângulos de cada setor (centro)
        angles_deg = np.arange(0, 360, setor_deg)
        angles_rad = np.radians(angles_deg)
        bar_width  = np.radians(setor_deg) * 0.9  # leve espaço entre barras

        # Plota bandas acumuladas (menor velocidade primeiro, embaixo)
        bottom = np.zeros(self.n_sectors)
        legend_patches = []

        for b in range(n_bands):
            heights = freq_pct[:, b]
            color_idx = min(b, len(self.band_colors) - 1)
            color = self.band_colors[color_idx]

            ax.bar(
                angles_rad,
                heights,
                width=bar_width,
                bottom=bottom,
                color=color,
                edgecolor="white",
                linewidth=0.5,
                align="center",
            )
            bottom += heights

            # Label da banda para legenda
            lo_label = f"{self.bands[b - 1]:.0f}" if b > 0 else "0"
            hi_label = f"{bins[b]:.0f}" if bins[b] < 999 else "+"
            label = f"{lo_label} – {hi_label} kt"
            legend_patches.append(mpatches.Patch(facecolor=color, edgecolor="white", label=label))

        # ------------------------------------------------------------------
        # Círculos de referência radial proporcionais aos nós reais
        # ------------------------------------------------------------------
        r_max = bottom.max() if bottom.max() > 0 else 1.0
        max_band_kt = float(self.bands[-1])  # e.g. 40 kt = raio total de referência

        # Posição de cada limite de banda no eixo radial (proporcional ao kt)
        r_ticks = [r_max * (kt / max_band_kt) for kt in self.bands]
        r_labels = [f"{int(kt)} kt" for kt in self.bands]

        ax.set_rticks(r_ticks)
        ax.set_yticklabels(r_labels)

        # Linhas de grade radiais — desativa o auto e usa só as customizadas
        ax.set_rlabel_position(22.5)
        ax.tick_params(axis="y", labelsize=13, labelcolor="black", pad=4)
        ax.yaxis.grid(True, linestyle="--", linewidth=0.5, alpha=0.6, color="black")
        ax.xaxis.grid(True, linestyle="-", linewidth=0.4, alpha=0.5, color="black")

        # Labels cardeais
        ax.set_xticks(np.radians([0, 45, 90, 135, 180, 225, 270, 315]))
        ax.set_xticklabels(
            ["N", "NE", "E", "SE", "S", "SW", "W", "NW"],
            fontsize=11, fontweight="bold", color="black",
        )

        # Fundo cinza claro
        ax.set_facecolor("#e8e8e8")
        fig.patch.set_facecolor("white")

        # Legenda no canto superior esquerdo da figura (coordenadas da figura)
        fig.legend(
            handles=legend_patches,
            loc="upper left",
            bbox_to_anchor=(0.01, 0.97),
            bbox_transform=fig.transFigure,
            fontsize=18,
            framealpha=0.9,
            title="Velocidade do vento",
            title_fontsize=20,
        )

        # Informações de ventos calmos — centralizado sob a rosa dos ventos
        calm_text = f"Ventos calmos: {calm_pct:.1f}%  |  Total: {total:,} observações".replace(",", ".")
        fig.text(
            0.625, 0.02, calm_text,
            ha="center", va="bottom", fontsize=18, color="gray",
        )

        # Título
        ax.set_title(title, fontsize=14, fontweight="bold", pad=20, color="black")

        # Salva
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        plt.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
        plt.close(fig)

    def plot_from_config(
        self,
        df: pd.DataFrame,
        station: str,
        years: int,
        output_dir: str,
        declination: float = 0.0,
        dpi: int = 150,
    ) -> str:
        """
        Atalho para uso dentro do pipeline: aplica declinação e nomeia arquivo.

        Args:
            df:          DataFrame com colunas 'direction' e 'speed_kts'.
            station:     Nome da estação (para título e nome de arquivo).
            years:       Janela temporal em anos.
            output_dir:  Pasta de saída.
            declination: Declinação magnética em graus.
            dpi:         Resolução da imagem.

        Returns:
            Caminho do arquivo PNG gerado.
        """
        df = df.copy()
        df["direction_mag"] = (df["direction"] + declination) % 360

        output_path = os.path.join(output_dir, f"Windrose-{years}y.png")

        self.plot(
            direction=df["direction_mag"],
            speed=df["speed_kts"],
            output_path=output_path,
            title=f"Rosa dos Ventos — {station}  ({years} ano{'s' if years > 1 else ''})",
            dpi=dpi,
        )
        return output_path
