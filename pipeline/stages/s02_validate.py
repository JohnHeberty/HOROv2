"""
pipeline/stages/s02_validate.py
================================
Stage 2 — VALIDATE: Quality gate na camada Bronze

Responsabilidades:
  - Verifica colunas obrigatórias (DATA, direction_raw, speed_raw)
  - Verifica tipos e ranges (dir 0–360, spd ≥ 0)
  - Calcula percentual de nulos por coluna
  - Descarta estações que excedam os limites de qualidade
  - Registra warnings sem abortar para colunas com nulos aceitáveis

Entrada:  context.bronze
Saída:    context.bronze (BronzeRecord.rejected=True para falhas graves)
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd

from pipeline.core.config import PipelineConfig, cfg
from pipeline.core.exceptions import DataQualityError, SchemaError
from pipeline.core.logger import get_logger
from pipeline.core.models import PipelineContext

log = get_logger("s02_validate")

# ---------------------------------------------------------------------------
# Limites de qualidade configuráveis
# ---------------------------------------------------------------------------
NULL_PCT_HARD_LIMIT  = 0.50   # >= 50% de nulos → rejeitar estação
NULL_PCT_WARN_LIMIT  = 0.10   # >= 10% de nulos → emitir aviso
REQUIRED_COLS        = ["DATA", "direction_raw", "speed_raw"]
DIR_RANGE            = (0.0, 360.0)
SPD_MIN              = 0.0


def _check_schema(station: str, df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise SchemaError(station, missing)


def _check_nulls(station: str, df: pd.DataFrame) -> None:
    for col in REQUIRED_COLS[1:]:  # DATA já foi validada no stage 1
        null_pct = df[col].isna().mean()
        if null_pct >= NULL_PCT_HARD_LIMIT:
            raise DataQualityError(station, col, null_pct, NULL_PCT_HARD_LIMIT)
        if null_pct >= NULL_PCT_WARN_LIMIT:
            log.warning(
                "Alta taxa de nulos",
                station=station,
                col=col,
                null_pct=f"{null_pct:.1%}",
            )


def _check_ranges(station: str, df: pd.DataFrame) -> None:
    """Emite warnings para valores fora do domínio físico (não rejeita)."""
    dir_col = "direction_raw"
    spd_col = "speed_raw"

    if dir_col in df.columns:
        oob_dir = df[dir_col].dropna()
        oob_dir = ((oob_dir < DIR_RANGE[0]) | (oob_dir > DIR_RANGE[1])).sum()
        if oob_dir > 0:
            log.warning(
                "Valores de direção fora de 0–360",
                station=station,
                count=int(oob_dir),
            )

    if spd_col in df.columns:
        oob_spd = (df[spd_col].dropna() < SPD_MIN).sum()
        if oob_spd > 0:
            log.warning(
                "Velocidades negativas encontradas",
                station=station,
                count=int(oob_spd),
            )


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------

def run(context: PipelineContext, config: PipelineConfig = cfg) -> PipelineContext:
    """
    Executa validações em todos os BronzeRecords não rejeitados.
    Rejeita estações com falhas graves; emite warnings para problemas menores.
    """
    log.info("=== STAGE 2 — VALIDATE (Bronze quality gate) ===")

    for station, record in context.bronze.items():
        if record.rejected:
            log.info("Estação já rejeitada no Stage 1, pulando", station=station)
            continue

        df = record.df
        if df is None or df.empty:
            record.rejected = True
            record.rejection_reason = "DataFrame vazio após ingestão"
            log.error("DataFrame vazio", station=station)
            continue

        try:
            _check_schema(station, df)
            _check_nulls(station, df)
            _check_ranges(station, df)
            log.info(
                "Validação OK",
                station=station,
                rows=len(df),
                dir_nulls=f"{df['direction_raw'].isna().mean():.1%}",
                spd_nulls=f"{df['speed_raw'].isna().mean():.1%}",
            )
        except (SchemaError, DataQualityError) as exc:
            record.rejected = True
            record.rejection_reason = str(exc)
            log.error("Estação rejeitada na validação", station=station, reason=str(exc))

    passed  = sum(1 for r in context.bronze.values() if not r.rejected)
    rejected = sum(1 for r in context.bronze.values() if r.rejected)
    context.stages_executed.append("s02_validate")
    log.info("Stage 2 finalizado", passed=passed, rejected=rejected)
    return context
