"""
pipeline/stages/s05_enrich.py
===============================
Stage 5 — ENRICH: Silver → Silver (Declinação magnética)

Responsabilidades:
  - Calcula a declinação magnética localmente via modelo WMM (pacote geomag)
  - Usa cache local (data/silver/declinations.json) para evitar recálculos
  - Aplica declinação magnética ao SilverRecord

Entrada:  context.silver
Saída:    context.silver[station].magnetic_declination preenchido
          data/silver/declinations.json (cache)
"""

from __future__ import annotations

import json
import os
from typing import Dict, Optional

from pipeline.core.config import PipelineConfig, cfg
from pipeline.core.logger import get_logger
from pipeline.core.models import PipelineContext

log = get_logger("s05_enrich")

DECLINATIONS_CACHE_FILE = "declinations.json"


def _load_cache(cache_path: str) -> Dict[str, float]:
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_cache(cache_path: str, data: Dict[str, float]) -> None:
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _fetch_declination(lat: float, lon: float) -> float:
    """
    Calcula a declinação magnética localmente via modelo WMM (pacote geomag).

    Sem internet, sem API key. Retorna graus decimais:
      positivo = Leste, negativo = Oeste.
    """
    import geomag

    declination = geomag.declination(lat, lon)
    log.debug("Declinação calculada localmente (WMM)", lat=lat, lon=lon, declination=declination)
    return float(declination)


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------

def run(context: PipelineContext, config: PipelineConfig = cfg) -> PipelineContext:
    """
    Obtém a declinação magnética para cada estação Silver.
    Usa cache para evitar requisições desnecessárias.

    Overrides de config_runway.json (todos opcionais):
      - latitude / longitude → substituem as coordenadas lidas do CSV antes da consulta NOAA
      - magnetic_declination → pula a NOAA por completo e usa o valor informado diretamente
    """
    log.info("=== STAGE 5 — ENRICH (Declinação magnética) ===")

    # ------------------------------------------------------------------
    # Override: aplica lat/lon de config_runway.json a todas as estações
    # ------------------------------------------------------------------
    if config.wind.latitude_override is not None or config.wind.longitude_override is not None:
        for station, record in context.silver.items():
            if config.wind.latitude_override is not None:
                record.metadata.latitude = config.wind.latitude_override
            if config.wind.longitude_override is not None:
                record.metadata.longitude = config.wind.longitude_override
        log.info(
            "Coordenadas sobrescritas pelo config_runway.json",
            lat=config.wind.latitude_override,
            lon=config.wind.longitude_override,
        )

    # ------------------------------------------------------------------
    # Override: declinação magnética direta — pula a consulta NOAA
    # ------------------------------------------------------------------
    override_declination = config.wind.magnetic_declination_override
    if override_declination is not None:
        log.info(
            "Declinação magnética sobrescrita pelo config_runway.json — consulta NOAA ignorada",
            declination=override_declination,
        )

    cache_path = os.path.join(config.output.data_silver, DECLINATIONS_CACHE_FILE)
    cache = _load_cache(cache_path)
    updated = False

    # Estações sem coordenadas reais (fallback 0,0) — skip da NOAA, declinação = 0.0
    for station, record in context.silver.items():
        if record.metadata.latitude == 0.0 and record.metadata.longitude == 0.0:
            if station not in cache:
                log.warning(
                    "Coordenadas desconhecidas (0,0) — declinação magnética definida como 0.0."
                    " Renomeie o arquivo para {CODIGO_INMET}.csv ou adicione LATITUDE/LONGITUDE"
                    " ao cabeçalho para habilitar a consulta NOAA.",
                    station=station,
                )
                cache[station] = 0.0
                updated = True

    # Estações que precisam de consulta real à NOAA (sem cache e com override desativado)
    need_fetch = [
        (station, record)
        for station, record in context.silver.items()
        if station not in cache
        and override_declination is None
        and not (record.metadata.latitude == 0.0 and record.metadata.longitude == 0.0)
    ]

    if not need_fetch:
        log.info("Todas as declinações já estão em cache")
    else:
        log.info("Calculando declinação magnética (WMM local)", stations=len(need_fetch))
        for station, record in need_fetch:
            lat = record.metadata.latitude
            lon = record.metadata.longitude
            log.info("Calculando (WMM)", station=station, lat=lat, lon=lon)
            try:
                dec = _fetch_declination(lat, lon)
                cache[station] = dec
                updated = True
                log.info("Declinação calculada", station=station, declination=dec)
            except Exception as exc:
                log.warning(
                    "Cálculo de declinação falhou — usando 0.0 (fallback neutro)",
                    station=station,
                    error=str(exc),
                )
                cache[station] = 0.0
                updated = True

    if updated:
        _save_cache(cache_path, cache)

    # Aplica ao contexto Silver (usa override se definido, senão usa cache)
    for station, record in context.silver.items():
        if override_declination is not None:
            record.magnetic_declination = override_declination
            log.info(
                "Declinação aplicada (override)",
                station=station,
                declination=record.magnetic_declination,
            )
        else:
            record.magnetic_declination = cache.get(station, 0.0)
            log.info(
                "Declinação aplicada",
                station=station,
                declination=record.magnetic_declination,
            )

    context.stages_executed.append("s05_enrich")
    log.info("Stage 5 finalizado")
    return context
