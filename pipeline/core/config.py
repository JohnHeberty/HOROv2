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
from typing import Dict, List, Optional, Tuple

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
    
    # Comprimento de pista de referência (m) - usado como fallback RBAC154
    # >= 1500m → 20kt  |  1200-1500m → 13kt  |  < 1200m → 10kt
    runway_length_m: float = 1500.0

    # Limite de vento cruzado em nós — lido diretamente de config_runway.json.
    # Se não estiver no JSON, calculado automaticamente via regra RBAC154.
    crosswind_limit_kts: float = 20.0
    # Overrides opcionais de coordenadas e declinação magnética.
    # Se definidos, substituem os valores lidos do CSV / cache NOAA.
    latitude_override:  Optional[float] = None
    longitude_override: Optional[float] = None
    # Quando informado, pula a consulta à NOAA por completo.
    magnetic_declination_override: Optional[float] = None

    # Nomes dos setores por quantidade
    sector_names: Dict[int, List[str]] = field(default_factory=lambda: {
        4:  ["N",   "W",   "S",   "E"],
        8:  ["N",   "NW",  "W",   "SW",  "S",   "SE",  "E",   "NE"],
        16: ["N",   "NNW", "NW",  "WNW", "W",   "WSW", "SW",  "SSW",
             "S",   "SSE", "SE",  "ESE", "E",   "ENE", "NE",  "NNE"],
    })

    # Paleta de cores da rosa PNG — carregada de config_runway.json
    # Formato interno: normalized RGB float (matplotlib). Índices 0–5 = bandas de velocidade.
    windrose_band_colors_rgb: List[Tuple[float, float, float]] = field(default_factory=lambda: [
        (0.706, 0.706, 0.706),  # [0-3]   cinza claro
        (0.118, 0.392, 1.000),  # [3-13]  azul forte
        (0.157, 1.000, 0.157),  # [13-20] verde vibrante
        (1.000, 1.000, 0.000),  # [20-25] amarelo brilhante
        (1.000, 0.588, 0.000),  # [25-40] laranja forte
        (1.000, 0.118, 0.000),  # [40+]   vermelho intenso
    ])


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
    wind_rose_proportion: float = 0.207  # Raio da rosa (aumentado 15% para melhor visualização)
    background_gray: tuple = (40, 40, 40)  # Fundo cinza escuro da imagem (BGR)
    windrose_background: tuple = (90, 90, 90)  # Fundo cinza claro da rosa dos ventos (BGR)
    color_runway: tuple = (255, 255, 255)
    color_best_runway: tuple = (0, 255, 0)
    color_point_ref: tuple = (255, 165, 0)
    color_legend: tuple = (255, 255, 255)
    point_ref_size: int = 25
    legend_x_right: int = field(init=False)
    legend_x_left: int = 40
    legend_y_spacing: int = 40
    max_spin_deg: int = 180   # graus de rotação do vídeo MP4
    gif_spin_deg: int  = 360   # graus de rotação do GIF (separado)

    # Paleta de cores do vídeo (BGR, OpenCV) — carregada de config_runway.json
    # Índices 0–5 = bandas de velocidade.
    video_band_colors_bgr: List[Tuple[int, int, int]] = field(default_factory=lambda: [
        (180, 180, 180),  # [0-3]   cinza claro
        (255, 100,  30),  # [3-13]  azul forte  (BGR: B=255)
        ( 40, 255,  40),  # [13-20] verde vibrante
        (  0, 255, 255),  # [20-25] amarelo brilhante
        (  0, 150, 255),  # [25-40] laranja forte
        (  0,  30, 255),  # [40+]   vermelho intenso
    ])

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

    def load_runway_config(self) -> None:
        """Carrega configuração de pista do arquivo config_runway.json se existir."""
        config_path = os.path.join(_REPO_ROOT, "config_runway.json")
        if not os.path.exists(config_path):
            return
        try:
            import json
            with open(config_path, "r", encoding="utf-8") as f:
                d = json.load(f)

            # ----------------------------------------------------------------
            # Pista (nested: d["pista"][...])
            # ----------------------------------------------------------------
            pista = d.get("pista", {})
            self.wind.runway_length_m = float(pista.get("runway_length_m", self.wind.runway_length_m))
            rl = self.wind.runway_length_m
            self.wind.crosswind_limit_kts = (
                20.0 if rl >= 1500 else 13.0 if rl >= 1200 else 10.0
            )

            # ----------------------------------------------------------------
            # Rosa dos ventos (nested: d["rosa_dos_ventos"][...])
            # ----------------------------------------------------------------
            rosa = d.get("rosa_dos_ventos", {})
            _bands = rosa.get("wind_speed_bands_kts")
            if isinstance(_bands, list) and len(_bands) >= 3:
                self.wind.limits_kts = _bands
            _cores_rosa = rosa.get("cores_rgb")
            if isinstance(_cores_rosa, list) and len(_cores_rosa) >= 6:
                self.wind.windrose_band_colors_rgb = [
                    (c[0] / 255.0, c[1] / 255.0, c[2] / 255.0) for c in _cores_rosa
                ]

            # ----------------------------------------------------------------
            # Vídeo (nested: d["video"][...])
            # ----------------------------------------------------------------
            video = d.get("video", {})
            _vspin = video.get("video_spin_deg")
            if isinstance(_vspin, int) and _vspin > 0:
                self.render.max_spin_deg = _vspin
            _gspin = video.get("gif_spin_deg")
            if isinstance(_gspin, int) and _gspin > 0:
                self.render.gif_spin_deg = _gspin
            _cores_video = video.get("cores_rgb")
            if isinstance(_cores_video, list) and len(_cores_video) >= 6:
                self.render.video_band_colors_bgr = [
                    (int(c[2]), int(c[1]), int(c[0])) for c in _cores_video
                ]

            # ----------------------------------------------------------------
            # Localização (nested: d["localizacao"][...])
            # ----------------------------------------------------------------
            loc = d.get("localizacao", {})
            if loc.get("latitude") is not None:
                self.wind.latitude_override = float(loc["latitude"])
            if loc.get("longitude") is not None:
                self.wind.longitude_override = float(loc["longitude"])

            # ----------------------------------------------------------------
            # Declinação magnética (nested: d["declinacao_magnetica"]["valor"])
            # ----------------------------------------------------------------
            decl = d.get("declinacao_magnetica", {})
            if decl.get("valor") is not None:
                self.wind.magnetic_declination_override = float(decl["valor"])

        except Exception:
            pass  # Se falhar, mantém os valores padrão


# Instância global — reutilize em todo o pipeline
cfg = PipelineConfig()
cfg.load_runway_config()  # Carrega configuração de pista
