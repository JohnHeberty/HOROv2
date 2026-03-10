"""
pipeline/core/models.py
=======================
Contratos de dados (dataclasses) que transitam entre os estágios do pipeline.

Cada estágio consome e/ou produz um ou mais destes modelos.
Garante que não passemos dicionários ou DataFrames brutos sem tipagem.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Bronze layer
# ---------------------------------------------------------------------------
@dataclass
class StationMetadata:
    """
    Metadados de uma estação meteorológica extraídos no Stage 1 (Ingest).

    Presente no cabeçalho dos arquivos INMET / REDEMET.
    """

    name: str
    latitude: float
    longitude: float
    altitude: str              # Ex.: "1160,96" — mantido como string original
    file_path: str             # Caminho absoluto do CSV raw
    encoding_used: str         # Encoding com que o arquivo foi lido
    ingested_at: datetime = field(default_factory=datetime.utcnow)

    # Calculados após ingestão
    parquet_path: Optional[str] = None
    meta_json_path: Optional[str] = None


@dataclass
class BronzeRecord:
    """
    Record de uma estação na camada Bronze: metadados + DataFrame bruto.

    O DataFrame ainda pode conter nulos, unidades originais e tipos mistos.
    """

    metadata: StationMetadata
    df: pd.DataFrame
    row_count: int = 0
    rejected: bool = False
    rejection_reason: Optional[str] = None

    def __post_init__(self) -> None:
        if self.row_count == 0 and self.df is not None:
            self.row_count = len(self.df)


# ---------------------------------------------------------------------------
# Silver layer
# ---------------------------------------------------------------------------
@dataclass
class SilverRecord:
    """
    Record de uma estação na camada Silver: dados limpos, validados e
    com unidades padronizadas (direção em graus 0–360, velocidade em nós).

    Colunas garantidas:
      - timestamp  : datetime (UTC)
      - direction  : float  0–360
      - speed_kts  : float  ≥ 0
    """

    metadata: StationMetadata
    df: pd.DataFrame            # colunas: timestamp, direction, speed_kts
    null_pct_speed: float = 0.0
    null_pct_direction: float = 0.0
    years_available: List[int] = field(default_factory=list)
    magnetic_declination: Optional[float] = None   # preenchido no Stage 5


# ---------------------------------------------------------------------------
# Gold layer
# ---------------------------------------------------------------------------
@dataclass
class WindTable:
    """
    Tabela percentual de vento por setor e banda de velocidade.

    Gerada pelo Stage 4 (Analyze).
    Linhas = setores (N, NNW, NW …)
    Colunas = bandas de velocidade ([0-3], [3-13] …)
    """

    station: str
    period_years: int
    sector_names: List[str]
    limit_bins: List[float]
    pct_table: pd.DataFrame      # shape (n_sectors, n_bins)
    total_records: int = 0
    calm_pct: float = 0.0        # % de ventos calmos (speed == 0)


@dataclass
class RunwayOptimizationResult:
    """
    Resultado da otimização de orientação de pista para N anos de dados.
    Gerado pelo Stage 6 (Optimize).
    """

    station: str
    period_years: int
    best_heading_deg: float           # Ângulo verdadeiro da melhor orientação (0–180)
    runway_designation: str           # Ex.: "09-27"
    fo_pct: float                     # Fator de Operação (%)
    crosswind_pct: float              # % de ventos de través (> limite)
    calm_pct: float                   # % de ventos calmos
    magnetic_declination: float       # Declinação magnética aplicada
    fo_by_heading: Dict[float, float] = field(default_factory=dict)  # {heading: fo_pct}


@dataclass
class PipelineContext:
    """
    Objeto de contexto que transita entre os estágios do orquestrador.

    Cada estágio lê o que precisa e escreve seus resultados aqui.
    """

    # Entradas
    input_files: List[str] = field(default_factory=list)

    # Resultados por estágio
    bronze: Dict[str, BronzeRecord] = field(default_factory=dict)
    silver: Dict[str, SilverRecord] = field(default_factory=dict)
    wind_tables: Dict[str, Dict[int, WindTable]] = field(default_factory=dict)
    results: Dict[str, Dict[int, RunwayOptimizationResult]] = field(default_factory=dict)

    # Metadados de execução
    run_id: str = ""
    started_at: Optional[datetime] = None
    stages_executed: List[str] = field(default_factory=list)
    stages_failed: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        import uuid
        if not self.run_id:
            self.run_id = str(uuid.uuid4())[:8]
        if self.started_at is None:
            self.started_at = datetime.utcnow()
