"""
pipeline/core/config.py
=======================
Configuração centralizada do pipeline HOROv1.

Todas as constantes e paths do projeto são definidas aqui.
Substitui Default.py. Permite override via variáveis de ambiente.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from glob import glob
from typing import Dict, List

import cv2 as cv

# Raiz do PROJETO (dois níveis acima deste arquivo: pipeline/core/ → pipeline/ → raiz)
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# Configurações de dados
# ---------------------------------------------------------------------------
@dataclass
class DataConfig:
    """Parâmetros de leitura e processamento dos dados meteorológicos."""

    # Padrões para detecção de velocidade do vento (case-insensitive, testados em ordem)
    # Suporta formato antigo BDMEP ("VENTO. VELO") e novo portal INMET ("Vel. Vento")
    wind_patterns: List[str] = field(default_factory=lambda: [
        "Vel. Vento",
        "VENTO. VELO",
        "VEL. VENTO",
        "VELOCIDADE DO VENTO",
    ])
    # Padrões para detecção de direção do vento
    direction_patterns: List[str] = field(default_factory=lambda: [
        "Dir. Vento",
        "VENTO. DIRE",
        "DIR. VENTO",
        "DIRECAO DO VENTO",
    ])
    # Padrões para EXCLUSÃO — colunas de rajada que colidem com velocidade
    gust_patterns: List[str] = field(default_factory=lambda: [
        "Raj. Vento",
        "RAJADA",
        "RAJ.",
    ])
    # Casas decimais para arredondamentos
    decimal_places: int = 3
    # Fator de conversão m/s → nós  (1 m/s = 1.944 kt)
    m_to_knots: float = 1.944
    # Separador dos CSVs de entrada
    csv_sep: str = ";"
    # Manter ventos calmos (velocidade == 0) no dataset
    keep_calms: bool = True
    # Salvar análise em disco após ETL
    save_analysis: bool = False


# ---------------------------------------------------------------------------
# Configurações da rosa dos ventos
# ---------------------------------------------------------------------------
@dataclass
class WindRoseConfig:
    """Parâmetros da rosa dos ventos RBAC154."""

    # Número de setores da rosa (padrão RBAC154: 16)
    sectors: int = 16
    # Limites de velocidade [nós] que definem as bandas de cores
    limits_kts: List[float] = field(default_factory=lambda: [3, 13, 20, 25, 40])
    # Limites dentro da PPD (pista)
    limits_in_ppd: List[float] = field(default_factory=lambda: [3, 13, 20])
    # Limites fora da PPD (vento de través)
    limits_out_ppd: List[float] = field(default_factory=lambda: [20, 25, 40])
    # Vento cruzado máximo permitido dentro da pista
    crosswind_limit_kts: float = 20.0
    # Nomes dos setores por quantidade
    sector_names: Dict[int, List[str]] = field(default_factory=lambda: {
        4:  ["N",   "W",   "S",   "E"],
        8:  ["N",   "NW",  "W",   "SW",  "S",   "SE",  "E",   "NE"],
        16: ["N",   "NNW", "NW",  "WNW", "W",   "WSW", "SW",  "SSW",
             "S",   "SSE", "SE",  "ESE", "E",   "ENE", "NE",  "NNE"],
    })


# ---------------------------------------------------------------------------
# Configurações de renderização visual
# ---------------------------------------------------------------------------
@dataclass
class RenderConfig:
    """Parâmetros visuais para geração dos frames e vídeo."""

    image_width: int = 1920
    image_height: int = 1080
    font: int = cv.FONT_HERSHEY_SIMPLEX
    font_size: float = 0.90
    font_thickness: int = 1
    gif_speed_multiplier: int = 4
    fps_video: int = 10
    wind_rose_proportion: float = 0.28  # Raio da rosa em relação à imagem (0.28 para visualização ideal)
    color_runway: tuple = (255, 255, 255)
    color_best_runway: tuple = (0, 255, 0)
    color_point_ref: tuple = (255, 165, 0)
    color_legend: tuple = (255, 255, 255)
    point_ref_size: int = 25
    legend_x_right: int = field(init=False)
    legend_x_left: int = 40
    legend_y_spacing: int = 40
    max_spin_deg: int = 180

    def __post_init__(self) -> None:
        self.legend_x_right = self.image_width - 510


# ---------------------------------------------------------------------------
# Configurações de saída e cache
# ---------------------------------------------------------------------------
@dataclass
class OutputConfig:
    """Caminhos e flags de controle das saídas do pipeline."""

    # Raiz do projeto
    repo_root: str = field(default_factory=lambda: _REPO_ROOT)

    # Camadas de dados (Medallion)
    data_raw: str = field(init=False)
    data_bronze: str = field(init=False)
    data_silver: str = field(init=False)
    data_gold: str = field(init=False)
    data_cache: str = field(init=False)

    # Caminhos de saída legacy (ainda referenciados no stage 07)
    folder_images: str = field(init=False)
    video_path_template: str = field(init=False)  # .format(station, years)

    # Flags
    make_video: bool = True
    save_final_result: bool = True

    def __post_init__(self) -> None:
        d = self.repo_root
        self.data_raw    = os.path.join(d, "data", "raw")
        self.data_bronze = os.path.join(d, "data", "bronze")
        self.data_silver = os.path.join(d, "data", "silver")
        self.data_gold   = os.path.join(d, "data", "gold")
        self.data_cache  = os.path.join(d, "data", ".cache")
        self.folder_images      = os.path.join(d, "data", "gold", "exports", "{}", "frames")
        self.video_path_template = os.path.join(d, "data", "gold", "exports", "{}", "RunwayOrientation-{}.mp4")

    def input_csvs(self) -> List[str]:
        """Retorna todos os CSVs encontrados em data/raw/ (case-insensitive)."""
        files = glob(os.path.join(self.data_raw, "*.csv"))
        if not files:
            files = glob(os.path.join(self.data_raw, "*.CSV"))
        return files


# ---------------------------------------------------------------------------
# Configuração do browser (declinação magnética)
# ---------------------------------------------------------------------------
@dataclass
class BrowserConfig:
    """Parâmetros do browser Chromium para consulta à NOAA."""

    url_magnetic_declination: str = "https://ngdc.noaa.gov/geomag/calculators/magcalc.shtml"
    timeout_load: int = 60
    headless: bool = True


# ---------------------------------------------------------------------------
# PipelineConfig — agrega tudo em um único objeto
# ---------------------------------------------------------------------------
@dataclass
class PipelineConfig:
    """Configuração completa do pipeline. Ponto único de acesso às settings."""

    data:    DataConfig    = field(default_factory=DataConfig)
    wind:    WindRoseConfig = field(default_factory=WindRoseConfig)
    render:  RenderConfig  = field(default_factory=RenderConfig)
    output:  OutputConfig  = field(default_factory=OutputConfig)
    browser: BrowserConfig = field(default_factory=BrowserConfig)

    def ensure_dirs(self) -> None:
        """Cria diretórios de dados caso não existam."""
        for path in [
            self.output.data_raw,
            self.output.data_bronze,
            os.path.join(self.output.data_bronze, "rejected"),
            self.output.data_silver,
            self.output.data_gold,
            os.path.join(self.output.data_gold, "exports"),
            self.output.data_cache,
        ]:
            os.makedirs(path, exist_ok=True)


# Instância global — reutilize em todo o pipeline
cfg = PipelineConfig()
