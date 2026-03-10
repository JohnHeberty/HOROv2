"""
pipeline/stages/s03_transform.py
=================================
Stage 3 — TRANSFORM: Bronze → Silver

Responsabilidades:
  - Remove duplicatas de timestamp
  - Renomeia colunas para o contrato Silver (timestamp, direction, speed_kts)
  - Converte velocidade m/s → nós
  - Remove/mantém calmarias conforme config
  - Ordena por timestamp
  - Salva Parquet em data/silver/
  - Popula context.silver

Entrada:  context.bronze (somente registros não rejeitados)
Saída:    data/silver/{station}.parquet + context.silver
"""

from __future__ import annotations

import os
from typing import List

import numpy as np
import pandas as pd

from pipeline.core.config import PipelineConfig, cfg
from pipeline.core.exceptions import TransformError
from pipeline.core.logger import get_logger
from pipeline.core.models import PipelineContext, SilverRecord

log = get_logger("s03_transform")


def _transform(
    df: pd.DataFrame,
    m_to_knots: float,
    decimal_places: int,
    keep_calms: bool,
) -> pd.DataFrame:
    """
    Aplica transformações ao DataFrame Bronze e retorna o Silver.

    Resultado: colunas [timestamp, direction, speed_kts]
    """
    df = df.copy()

    # Renomeia para contrato Silver
    rename = {"DATA": "timestamp", "direction_raw": "direction", "speed_raw": "speed_kts"}
    df = df.rename(columns=rename)

    # Remove duplicatas de timestamp (mantém o primeiro)
    before = len(df)
    df = df.drop_duplicates(subset="timestamp").reset_index(drop=True)
    dupes = before - len(df)
    if dupes > 0:
        log.warning("Duplicatas de timestamp removidas", count=dupes)

    # Converte m/s → nós
    df["speed_kts"] = (
        pd.to_numeric(
            df["speed_kts"].astype(str).str.replace(",", ".", regex=False),
            errors="coerce",
        ) * m_to_knots
    ).round(decimal_places)

    # Dropa nulos residuais
    df = df.dropna(subset=["direction", "speed_kts"]).reset_index(drop=True)

    # Remove calmarias se solicitado
    if not keep_calms:
        before_calms = len(df)
        df = df[df["speed_kts"] > 0].reset_index(drop=True)
        removed = before_calms - len(df)
        if removed > 0:
            log.info("Calmarias removidas", count=removed)

    # Garante tipos finais
    df["direction"] = pd.to_numeric(df["direction"], errors="coerce")
    df["speed_kts"] = pd.to_numeric(df["speed_kts"], errors="coerce")
    df = df.dropna(subset=["direction", "speed_kts"]).reset_index(drop=True)

    # Ordena cronologicamente
    df = df.sort_values("timestamp").reset_index(drop=True)

    return df


def _available_years(df: pd.DataFrame) -> List[int]:
    """Retorna lista dos anos com dados disponíveis."""
    if "timestamp" not in df.columns or df.empty:
        return []
    return sorted(df["timestamp"].dt.year.dropna().unique().astype(int).tolist())


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------

def run(context: PipelineContext, config: PipelineConfig = cfg) -> PipelineContext:
    """Transforma dados Bronze em Silver para todas as estações válidas."""
    log.info("=== STAGE 3 — TRANSFORM (Bronze → Silver) ===")

    os.makedirs(config.output.data_silver, exist_ok=True)

    for station, record in context.bronze.items():
        if record.rejected:
            log.info("Pulando estação rejeitada", station=station)
            continue

        log.info("Transformando", station=station, rows_bronze=len(record.df))

        try:
            df_silver = _transform(
                record.df,
                m_to_knots=config.data.m_to_knots,
                decimal_places=config.data.decimal_places,
                keep_calms=config.data.keep_calms,
            )

            # Estatísticas de qualidade
            null_dir = df_silver["direction"].isna().mean()
            null_spd = df_silver["speed_kts"].isna().mean()
            years    = _available_years(df_silver)

            # Salva Parquet Silver
            parquet_out = os.path.join(config.output.data_silver, f"{station}.parquet")
            df_silver.to_parquet(parquet_out, index=False)

            context.silver[station] = SilverRecord(
                metadata=record.metadata,
                df=df_silver,
                null_pct_speed=null_spd,
                null_pct_direction=null_dir,
                years_available=years,
            )

            log.info(
                "Transformação concluída",
                station=station,
                rows_silver=len(df_silver),
                years=f"{min(years) if years else 'N/A'}–{max(years) if years else 'N/A'}",
                calms_pct=f"{(df_silver['speed_kts'] == 0).mean():.1%}",
            )

        except Exception as exc:
            log.error("Falha na transformação", station=station, error=str(exc))
            raise TransformError(f"[{station}] {exc}") from exc

    context.stages_executed.append("s03_transform")
    log.info("Stage 3 finalizado", stations_ok=len(context.silver))
    return context
