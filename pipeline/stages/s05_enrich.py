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
import time
from datetime import datetime
from typing import Dict, Optional

from pipeline.core.config import PipelineConfig, cfg
from pipeline.core.exceptions import MagneticDeclinationError
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


def _fetch_declination(lat: float, lon: float, driver, timeout: int = 60) -> float:
    """
    Consulta a NOAA para obter a declinação magnética.
    Migrado de Functions.py (GetMagneticDeclination).
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    from pipeline.utils.geo import latlon_to_grau_minuto, dms_string_to_decimal

    Declination = ""
    lat_dms, lat_dir, lon_dms, lon_dir = latlon_to_grau_minuto(lat, lon)

    def wait_click(css):
        try:
            el = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, css))
            )
            el.click()
            return el
        except Exception:
            return None

    wait_click("#declinationIGRF")

    # Latitude
    el = wait_click("#declinationLat1")
    if el:
        el.clear()
        el.send_keys(lat_dms)
        time.sleep(1)
        try:
            driver.find_element(By.CSS_SELECTOR, f"[value='{lat_dir}']").click()
        except Exception:
            pass

    # Longitude
    el = wait_click("#declinationLon1")
    if el:
        el.clear()
        el.send_keys(lon_dms)
        time.sleep(1)
        try:
            driver.find_element(By.CSS_SELECTOR, f"[value='{lon_dir}']").click()
        except Exception:
            pass

    wait_click("#declinationHTML")
    
    # DEBUG: salva screenshot antes de clicar Calculate
    try:
        driver.save_screenshot("data/silver/noaa_before_calc.png")
        log.debug("Screenshot salvo", path="data/silver/noaa_before_calc.png")
    except Exception:
        pass
    
    wait_click("#calcbutton")
    time.sleep(8)  # Espera mais longa para o cálculo completar no servidor NOAA
    
    # DEBUG: salva screenshot depois do cálculo
    try:
        driver.save_screenshot("data/silver/noaa_after_calc.png")
        log.debug("Screenshot salvo", path="data/silver/noaa_after_calc.png")
    except Exception:
        pass

    try:
        data_find = datetime.now().strftime("%Y-%m-%d")
        log.debug("Buscando resultado", date_pattern=data_find)
        el = WebDriverWait(driver, timeout * 2).until(  # timeout dobrado para aguardar resultado
            EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(), '{data_find}')]"))
        )
        parent = el.find_element(By.XPATH, "./..")
        children = parent.find_elements(By.XPATH, ".//*")
        Declination = children[1].text.split("changing")[0].strip().upper()
        log.debug("Resultado encontrado", declination_raw=Declination)
    except Exception as exc:
        # Salva o HTML da página para debug
        try:
            html_path = "data/silver/noaa_page_source.html"
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            log.error("Erro ao extrair resultado — HTML salvo", error=str(exc)[:500], html_path=html_path)
        except Exception:
            log.error("Erro ao extrair resultado", error=str(exc)[:500])
        pass

    driver.delete_all_cookies()
    driver.refresh()

    if not Declination.strip():
        raise MagneticDeclinationError(lat, lon)

    direction  = Declination[-1:].strip()
    dms_str    = Declination[:-1].strip()
    angle      = dms_string_to_decimal(dms_str, direction)

    time.sleep(5)
    return angle


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
    # Override: declina\u00e7\u00e3o magnética direta — pula NOAA completamente
    # ------------------------------------------------------------------
    if config.wind.magnetic_declination_override is not None:
        dec = config.wind.magnetic_declination_override
        log.info(
            "Declinação magnética sobrescrita pelo config_runway.json — consulta NOAA ignorada",
            declination=dec,
        )
        for station, record in context.silver.items():
            record.magnetic_declination = dec
            log.info("Declinação aplicada (override)", station=station, declination=dec)
        context.stages_executed.append("s05_enrich")
        log.info("Stage 5 finalizado")
        return context

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

    # Verifica quantas estações precisam de consulta real à NOAA
    need_fetch = [
        (station, record)
        for station, record in context.silver.items()
        if station not in cache
        and not (record.metadata.latitude == 0.0 and record.metadata.longitude == 0.0)
    ]

    if not need_fetch:
        log.info("Todas as declinações já estão em cache")
    else:
        log.info("Consultando NOAA via browser", stations=len(need_fetch))
        from pipeline.services.browser import CBrowser

        with CBrowser() as driver:
            for station, record in need_fetch:
                lat = record.metadata.latitude
                lon = record.metadata.longitude
                log.info("Consultando NOAA", station=station, lat=lat, lon=lon)
                try:
                    dec = _fetch_declination(lat, lon, driver, timeout=config.browser.timeout_load)
                    cache[station] = dec
                    updated = True
                    log.info("Declinação obtida", station=station, declination=dec)
                except MagneticDeclinationError as exc:
                    log.error("Falha ao obter declinação", station=station, error=str(exc))
                    cache[station] = 0.0  # fallback neutro
                    updated = True

    if updated:
        _save_cache(cache_path, cache)

    # Aplica ao contexto Silver
    for station, record in context.silver.items():
        record.magnetic_declination = cache.get(station, 0.0)
        log.info(
            "Declinação aplicada",
            station=station,
            declination=record.magnetic_declination,
        )

    context.stages_executed.append("s05_enrich")
    log.info("Stage 5 finalizado")
    return context
