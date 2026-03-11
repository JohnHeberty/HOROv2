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

    # Latitude (formato decimal, ex: -23.428375)
    el = wait_click("#declinationLat1")
    if el:
        el.clear()
        el.send_keys(str(abs(lat)))  # Envia valor absoluto
        time.sleep(1)
        # Seleciona hemisfério N/S
        lat_dir = 'S' if lat < 0 else 'N'
        try:
            driver.find_element(By.CSS_SELECTOR, f"[value='{lat_dir}']").click()
        except Exception:
            pass

    # Longitude (formato decimal, ex: -46.467887)
    el = wait_click("#declinationLon1")
    if el:
        el.clear()
        el.send_keys(str(abs(lon)))  # Envia valor absoluto
        time.sleep(1)
        # Seleciona hemisfério E/W
        lon_dir = 'W' if lon < 0 else 'E'
        try:
            driver.find_element(By.CSS_SELECTOR, f"[value='{lon_dir}']").click()
        except Exception:
            pass
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
    
    # Extrai a imagem da rosa dos ventos do popup "Declination"
    try:
        # Aguarda o popup carregar
        time.sleep(3)
        
        # Busca o popup/modal "Declination" que aparece após o cálculo
        popup_selectors = [
            '.modal-content',  # Bootstrap modal
            '.modal-dialog',
            '[role="dialog"]',
            '#declinationModal',
            '.ui-dialog',  # jQuery UI dialog
            '.popup',
            'div[style*="position: absolute"]',  # Popup posicionado
        ]
        
        popup_element = None
        for selector in popup_selectors:
            try:
                popup_element = driver.find_element(By.CSS_SELECTOR, selector)
                if popup_element.is_displayed():
                    log.debug(f"Popup encontrado com seletor: {selector}")
                    break
            except Exception:
                continue
        
        if popup_element:
            # Tenta encontrar o conteúdo interno do popup (sem o header/título)
            content_selectors = [
                '.modal-body',      # Bootstrap modal body
                '.ui-dialog-content',  # jQuery UI content
                'div[class*="content"]',  # Qualquer div com "content"
            ]
            
            content_element = None
            for content_selector in content_selectors:
                try:
                    content_element = popup_element.find_element(By.CSS_SELECTOR, content_selector)
                    if content_element.is_displayed():
                        log.debug(f"Conteúdo do popup encontrado: {content_selector}")
                        break
                except Exception:
                    continue
            
            # Se encontrou o conteúdo interno, captura só ele; senão captura o popup todo
            element_to_capture = content_element if content_element else popup_element
            windrose_path = "data/silver/noaa_windrose.png"
            element_to_capture.screenshot(windrose_path)
            log.info("Rosa dos ventos NOAA extraída (popup)", path=windrose_path, 
                    captured="content" if content_element else "full_popup")
        else:
            # Fallback: tenta encontrar qualquer div com title="Declination" ou texto "Declination"
            try:
                # Procura pelo título do popup
                title_element = driver.find_element(By.XPATH, "//*[contains(text(), 'Declination')]")
                # Sobe na hierarquia até encontrar o container do popup
                popup_element = title_element.find_element(By.XPATH, "./ancestor::div[contains(@class, 'modal') or contains(@class, 'dialog') or contains(@class, 'popup')]")
                
                windrose_path = "data/silver/noaa_windrose.png"
                popup_element.screenshot(windrose_path)
                log.info("Rosa dos ventos NOAA extraída (popup via XPath)", path=windrose_path)
            except Exception as e:
                # Último fallback: screenshot da página inteira
                log.warning("Popup não encontrado, usando screenshot completo", error=str(e))
                driver.save_screenshot("data/silver/noaa_after_calc.png")
                log.info("Screenshot NOAA salvo (fallback página completa)", path="data/silver/noaa_after_calc.png")
            
    except Exception as e:
        log.warning("Erro ao salvar imagem NOAA", error=str(e))

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

    # Parse do resultado (pode vir em formato decimal ou DMS)
    direction  = Declination[-1:].strip()  # W ou E
    value_str  = Declination[:-1].strip()  # Remove direção
    
    # Remove símbolo de grau se presente
    value_str = value_str.replace('°', '').strip()
    
    # Detecta se é decimal (contém ponto) ou DMS (contém espaços/aspas)
    if '.' in value_str and "'" not in value_str and '"' not in value_str:
        # Formato decimal ex: "21.95"
        angle = float(value_str)
    else:
        # Formato DMS ex: "21 57 00" ou "21° 57' 00\""
        angle = dms_string_to_decimal(value_str, direction)
    
    # Aplica sinal baseado na direção (W e S são negativos)
    if direction in ('W', 'S'):
        angle = -abs(angle)
    else:
        angle = abs(angle)

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
    # Override: declinação magnética direta — MAS ainda faz consulta NOAA para gerar imagem
    # ------------------------------------------------------------------
    override_declination = config.wind.magnetic_declination_override
    if override_declination is not None:
        log.info(
            "Declinação magnética sobrescrita pelo config_runway.json — mas consultando NOAA para gerar imagem da rosa dos ventos",
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

    # Verifica quantas estações precisam de consulta real à NOAA
    # Força consulta para gerar imagem se override está ativo e imagem não existe
    noaa_image_path = os.path.join(config.output.data_silver, "noaa_after_calc.png")
    force_fetch_for_image = (
        override_declination is not None 
        and not os.path.exists(noaa_image_path)
    )
    
    need_fetch = [
        (station, record)
        for station, record in context.silver.items()
        if ((station not in cache) or force_fetch_for_image)
        and not (record.metadata.latitude == 0.0 and record.metadata.longitude == 0.0)
    ]
    
    if force_fetch_for_image and need_fetch:
        log.info("Forçando consulta NOAA para gerar imagem da rosa dos ventos", 
                override_declination=override_declination)

    if not need_fetch:
        log.info("Todas as declinações já estão em cache")
    else:
        log.info("Consultando NOAA via browser", stations=len(need_fetch))
        from pipeline.services.browser import CBrowser

        try:
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
        except Exception as browser_exc:
            # Chrome/ChromeDriver falhou ao iniciar (ex: ambiente sem display, Colab com paths
            # incorretos, versão incompatível).  Registra o erro, aplica fallback 0.0 e
            # CONTINUA o pipeline — o vídeo/GIF ainda será gerado sem correção magnética.
            log.error(
                "Falha ao iniciar o browser — declinação será 0.0 para todas as estações. "
                "No Colab, verifique se a Seção 0 instalou o chromium-chromedriver corretamente.",
                error=str(browser_exc),
            )
            for station, _ in need_fetch:
                if station not in cache:
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
