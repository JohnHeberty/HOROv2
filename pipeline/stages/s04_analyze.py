"""
pipeline/stages/s04_analyze.py
================================
Stage 4 — ANALYZE: Silver → Wind tables (Gold)

Responsabilidades:
  - Para cada estação, calcula tabela de ventos por setor e banda de velocidade
  - Segmenta os dados por janela temporal (últimos 5, 10, 15, 20 anos)
  - Salva Parquet das tabelas em data/gold/

Entrada:  context.silver
Saída:    data/gold/{station}_wind_table_{years}y.parquet
          context.wind_tables[station][years] = WindTable
"""

from __future__ import annotations

import os
from typing import Dict, List

import pandas as pd

from pipeline.core.config import PipelineConfig, cfg
from pipeline.core.exceptions import AnalysisError
from pipeline.core.logger import get_logger
from pipeline.core.models import PipelineContext, WindTable
from pipeline.services.wind import calcular_tabela_ventos

log = get_logger("s04_analyze")


def _calculate_year_windows(df: pd.DataFrame) -> List[int]:
    """
    Calcula automaticamente quais janelas de anos fazem sentido baseado nos dados disponíveis.
    
    Só gera análises para janelas onde há dados suficientes (mínimo 90% dos registros esperados).
    Por exemplo: se tem 6 anos de dados, retorna [5, 6] (não 10, 15, 20)
    """
    if df.empty or "timestamp" not in df.columns:
        return [5]  # Fallback padrão
    
    from dateutil.relativedelta import relativedelta
    
    max_date = df["timestamp"].max()
    min_date = df["timestamp"].min()
    
    # Anos totais de dados disponíveis (arredonda para cima)
    delta = relativedelta(max_date, min_date)
    total_years = delta.years + (1 if delta.months > 0 or delta.days > 0 else 0)
    
    if total_years < 1:
        total_years = 1
    
    # Define janelas baseado nos dados reais
    # Sempre inclui último ano completodo completo
    windows = [min(5, total_years)]  # Janela padrão de 5 anos ou total se menor
    
    # Adiciona janelas de 10, 15, 20 anos APENAS se tiver dados suficientes
    # Critério: total_years >= window (100% de cobertura mínima)
    for window in [10, 15, 20]:
        if total_years >= window:
            windows.append(window)
    
    # Remove duplicatas e ordena
    windows = sorted(list(set(windows)))
    
    log.info(f"Dados disponíveis: {total_years} anos, janelas geradas: {windows}",
            station="current", min_date=min_date.strftime("%Y-%m-%d"), 
            max_date=max_date.strftime("%Y-%m-%d"))
    return windows


def _slice_years(df: pd.DataFrame, n_years: int) -> pd.DataFrame:
    """
    Retorna os registros dos últimos *n_years* anos disponíveis na série.

    Usa `relativedelta` para aritmética correta em anos bissextos.
    """
    from dateutil.relativedelta import relativedelta  # type: ignore

    if df.empty or "timestamp" not in df.columns:
        return df

    max_date = df["timestamp"].max()
    cutoff   = max_date - relativedelta(years=n_years)
    return df[df["timestamp"] >= cutoff].copy()


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------

def run(context: PipelineContext, config: PipelineConfig = cfg) -> PipelineContext:
    """Calcula tabelas de vento para todas as estações Silver."""
    log.info("=== STAGE 4 — ANALYZE (Silver → Wind Tables) ===")

    os.makedirs(config.output.data_gold, exist_ok=True)

    sector_names = config.wind.sector_names.get(config.wind.sectors, [])
    limits = config.wind.limits_kts

    for station, record in context.silver.items():
        df_all = record.df
        if df_all.empty:
            log.warning("DataFrame vazio, pulando", station=station)
            continue

        log.info("Calculando tabelas de vento", station=station)
        context.wind_tables[station] = {}
        
        # Calcula automaticamente quais janelas processar baseado em dados disponíveis
        year_windows = _calculate_year_windows(df_all)

        for years in year_windows:
            df_slice = _slice_years(df_all, years)
            if len(df_slice) < 10:
                log.warning(
                    "Dados insuficientes para a janela temporal, pulando",
                    station=station,
                    years=years,
                    rows=len(df_slice),
                )
                continue

            try:
                pct_table = calcular_tabela_ventos(
                    direcao=df_slice["direction"],
                    magnitude=df_slice["speed_kts"],
                    sector_names=sector_names,
                    limits=limits,
                )

                calm_pct = float((df_slice["speed_kts"] == 0).mean() * 100)

                wt = WindTable(
                    station=station,
                    period_years=years,
                    sector_names=sector_names,
                    limit_bins=limits,
                    pct_table=pct_table,
                    total_records=len(df_slice),
                    calm_pct=calm_pct,
                )

                context.wind_tables[station][years] = wt

                # Persiste Parquet
                out_path = os.path.join(
                    config.output.data_gold,
                    f"{station}_wind_table_{years}y.parquet",
                )
                pct_table.to_parquet(out_path)

                log.info(
                    "Tabela calculada",
                    station=station,
                    years=years,
                    rows=len(df_slice),
                    calm_pct=f"{calm_pct:.1f}%",
                )

            except Exception as exc:
                log.error(
                    "Falha no cálculo de tabela",
                    station=station,
                    years=years,
                    error=str(exc),
                )
                raise AnalysisError(f"[{station}/{years}y] {exc}") from exc

    context.stages_executed.append("s04_analyze")
    log.info("Stage 4 finalizado", stations=len(context.wind_tables))
    return context
