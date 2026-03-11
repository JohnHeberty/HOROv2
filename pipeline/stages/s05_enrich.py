"""
pipeline/stages/s05_enrich.py
===============================
Stage 5 — ENRICH: Silver → Silver (Declinação magnética)

Responsabilidades:
  - Local  → consulta o site NOAA via Selenium/Chrome (alta precisão)
  - Colab  → calcula localmente via modelo WMM (pacote geomag, sem browser)
  - Usa cache local (data/silver/declinations.json) para evitar repetições
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


def _in_colab() -> bool:
    """Retorna True se o código estiver rodando no Google Colab."""
    try:
        import google.colab  # noqa: F401
        return True
    except ImportError:
        return False


def _load_cache(cache_path: str) -> Dict[str, float]:
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_cache(cache_path: str, data: Dict[str, float]) -> None:
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Estratégia A — NOAA via Selenium (local: Windows/Mac/Linux com Chrome)
# ---------------------------------------------------------------------------

def _fetch_declination_noaa(lat: float, lon: float, driver, timeout: int = 60) -> float:
    """
    Consulta o site NOAA via Selenium para obter a declinação magnética.
    Alta precisão — usa o modelo WMM2025 do servidor NOAA.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    from pipeline.utils.geo import dms_string_to_decimal

    Declination = ""

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

    el = wait_click("#declinationLat1")
    if el:
        el.clear()
        el.send_keys(str(abs(lat)))
        time.sleep(1)
        lat_dir = 'S' if lat < 0 else 'N'
        try:
            driver.find_element(By.CSS_SELECTOR, f"[value='{lat_dir}']").click()
        except Exception:
            pass

    el = wait_click("#declinationLon1")
    if el:
        el.clear()
        el.send_keys(str(abs(lon)))
        time.sleep(1)
        lon_dir = 'W' if lon < 0 else 'E'
        try:
            driver.find_element(By.CSS_SELECTOR, f"[value='{lon_dir}']").click()
        except Exception:
            pass

    wait_click("#declinationHTML")
    wait_click("#calcbutton")
    time.sleep(8)

    try:
        data_find = datetime.now().strftime("%Y-%m-%d")
        el = WebDriverWait(driver, timeout * 2).until(
            EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(), '{data_find}')]"))
        )
        parent = el.find_element(By.XPATH, "./..")
        children = parent.find_elements(By.XPATH, ".//*")
        Declination = children[1].text.split("changing")[0].strip().upper()
        log.debug("Resultado NOAA encontrado", declination_raw=Declination)
    except Exception as exc:
        try:
            html_path = "data/silver/noaa_page_source.html"
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            log.error("Erro ao extrair resultado NOAA — HTML salvo", error=str(exc)[:500], html_path=html_path)
        except Exception:
            log.error("Erro ao extrair resultado NOAA", error=str(exc)[:500])

    driver.delete_all_cookies()
    driver.refresh()

    if not Declination.strip():
        raise MagneticDeclinationError(lat, lon)

    direction = Declination[-1:].strip()
    value_str = Declination[:-1].strip().replace('°', '').strip()

    if '.' in value_str and "'" not in value_str and '"' not in value_str:
        angle = float(value_str)
    else:
        angle = dms_string_to_decimal(value_str, direction)

    if direction in ('W', 'S'):
        angle = -abs(angle)
    else:
        angle = abs(angle)

    time.sleep(5)
    return angle


# ---------------------------------------------------------------------------
# Estratégia B — WMM local via geomag (Colab: sem browser, sem internet)
# ---------------------------------------------------------------------------

def _fetch_declination_wmm(lat: float, lon: float) -> float:
    """
    Calcula a declinação magnética localmente via modelo WMM (pacote geomag).
    Usado no Colab onde Chrome não está disponível.
    Retorna graus decimais: positivo = Leste, negativo = Oeste.
    """
    import geomag

    declination = geomag.declination(lat, lon)
    log.debug("Declinação calculada (WMM local)", lat=lat, lon=lon, declination=declination)
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
        colab = _in_colab()
        if colab:
            # ── Colab: calcula localmente (sem Chrome) ─────────────────────────
            log.info("Colab detectado — calculando declinação via WMM local", stations=len(need_fetch))
            for station, record in need_fetch:
                lat = record.metadata.latitude
                lon = record.metadata.longitude
                log.info("Calculando (WMM)", station=station, lat=lat, lon=lon)
                try:
                    dec = _fetch_declination_wmm(lat, lon)
                    cache[station] = dec
                    updated = True
                    log.info("Declinação calculada (WMM)", station=station, declination=dec)
                except Exception as exc:
                    log.warning(
                        "WMM falhou — usando 0.0 (fallback neutro)",
                        station=station, error=str(exc),
                    )
                    cache[station] = 0.0
                    updated = True
        else:
            # ── Local: consulta NOAA via Selenium (alta precisão) ──────────────
            log.info("Ambiente local — consultando NOAA via browser", stations=len(need_fetch))
            from pipeline.services.browser import CBrowser
            try:
                with CBrowser() as driver:
                    for station, record in need_fetch:
                        lat = record.metadata.latitude
                        lon = record.metadata.longitude
                        log.info("Consultando NOAA", station=station, lat=lat, lon=lon)
                        try:
                            dec = _fetch_declination_noaa(lat, lon, driver, timeout=config.browser.timeout_load)
                            cache[station] = dec
                            updated = True
                            log.info("Declinação NOAA obtida", station=station, declination=dec)
                        except MagneticDeclinationError as exc:
                            log.error("Falha ao obter declinação NOAA", station=station, error=str(exc))
                            cache[station] = 0.0
                            updated = True
            except Exception as browser_exc:
                log.error(
                    "Falha ao iniciar o browser — usando WMM local como fallback",
                    error=str(browser_exc),
                )
                for station, record in need_fetch:
                    if station not in cache:
                        lat = record.metadata.latitude
                        lon = record.metadata.longitude
                        try:
                            cache[station] = _fetch_declination_wmm(lat, lon)
                            log.warning(
                                "Fallback WMM aplicado (browser falhou)",
                                station=station, declination=cache[station],
                            )
                        except Exception:
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
