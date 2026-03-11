"""
pipeline/stages/s05_enrich.py
===============================
Stage 5 — ENRICH: Silver → Silver (Declinação magnética)

Responsabilidades:
  - Consulta a NOAA para cada estação via Selenium
  - Usa cache local (data/silver/declinations.json) para evitar requisições repetidas
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


def _fetch_declination(lat: float, lon: float, timeout: int = 30) -> float:
    """
    Consulta a NOAA via API HTTP para obter a declinação magnética.

    Endpoint: https://www.ngdc.noaa.gov/geomag-web/calculators/calculateDeclination
    Sem browser — usa apenas urllib (stdlib). Retorna graus decimais:
      positivo = Leste, negativo = Oeste.
    """
    import json
    import urllib.parse
    import urllib.request
    from datetime import date

    today = date.today()
    params = urllib.parse.urlencode({
        "lat1":        lat,
        "lon1":        lon,
        "model":       "WMM",
        "startYear":   today.year,
        "startMonth":  today.month,
        "startDay":    today.day,
        "resultFormat": "json",
    })
    url = f"https://www.ngdc.noaa.gov/geomag-web/calculators/calculateDeclination?{params}"
    log.debug("Requisição NOAA API", url=url)

    req = urllib.request.Request(url, headers={"User-Agent": "HOROv2/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read().decode())

    declination = float(data["result"][0]["declination"])
    log.debug("NOAA API respondeu", raw_declination=declination)
    return declination


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
        log.info("Consultando NOAA via API HTTP", stations=len(need_fetch))
        for station, record in need_fetch:
            lat = record.metadata.latitude
            lon = record.metadata.longitude
            log.info("Consultando NOAA", station=station, lat=lat, lon=lon)
            try:
                dec = _fetch_declination(lat, lon, timeout=30)
                cache[station] = dec
                updated = True
                log.info("Declinação obtida", station=station, declination=dec)
            except Exception as exc:
                log.warning(
                    "API NOAA falhou — usando declinação 0.0 (fallback neutro)",
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
