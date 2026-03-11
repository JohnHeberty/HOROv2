"""
pipeline/services/browser.py
============================
Gerenciamento do navegador Chromium headless para consultas à NOAA.

Migrado de Modulos/BROWSER/Engine.py.

Estratégia de detecção de ambiente:
  1. Google Colab / Linux com Chromium de sistema  →  usa paths fixos do sistema
     (/usr/lib/chromium-browser/chromedriver + /usr/bin/chromium-browser)
  2. Windows / Mac / Linux sem Chromium de sistema →  usa webdriver-manager
     para baixar/gerenciar o ChromeDriver automaticamente
"""

from __future__ import annotations

import os
import platform
import shutil
from typing import Optional

from pipeline.core.logger import get_logger

log = get_logger("services.browser")

# Caminhos conhecidos do Chromium no Ubuntu/Debian (Colab, CI, etc.)
_COLAB_CHROMEDRIVER_PATHS = [
    "/usr/lib/chromium-browser/chromedriver",
    "/usr/bin/chromedriver",
    "/usr/local/bin/chromedriver",
]
_COLAB_CHROME_BINARY_PATHS = [
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",
    "/snap/bin/chromium",
]


def _find_system_chromium():
    """
    Procura pelo Chromium instalado no sistema (Linux/Colab).
    Retorna (chrome_binary, chromedriver_path) ou (None, None).
    """
    # Procura pelo chromedriver no PATH primeiro
    driver_in_path = shutil.which("chromedriver")
    if driver_in_path:
        for binary in _COLAB_CHROME_BINARY_PATHS:
            if os.path.isfile(binary):
                return binary, driver_in_path

    # Procura por paths fixos conhecidos
    for driver_path in _COLAB_CHROMEDRIVER_PATHS:
        if os.path.isfile(driver_path):
            for binary in _COLAB_CHROME_BINARY_PATHS:
                if os.path.isfile(binary):
                    return binary, driver_path

    return None, None


class CBrowser:
    """
    Controla uma instância headless do Chrome para scraping da NOAA.

    • No Colab / Linux com Chromium de sistema: usa os paths do sistema operacional
      sem precisar baixar nada da internet.
    • No Windows / Mac: usa webdriver-manager para gerenciar o ChromeDriver.

    Uso:
        with CBrowser() as driver:
            ...
    """

    def __init__(self) -> None:
        from pipeline.core.config import cfg

        self.base_url     = cfg.browser.url_magnetic_declination
        self.timeout_load = cfg.browser.timeout_load
        self.headless     = cfg.browser.headless
        self.system       = platform.system()
        self.driver       = None

    # ------------------------------------------------------------------
    # Abertura do browser
    # ------------------------------------------------------------------
    def open(self):
        """
        Abre o Chrome headless.
        Detecta automaticamente se usa o Chromium do sistema
        (Colab / Linux) ou webdriver-manager (Windows / Mac).
        """
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service

        options = webdriver.ChromeOptions()
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--remote-debugging-port=9222")
        if self.headless:
            options.add_argument("--headless=new")

        # ── Detecção: Colab / Linux com Chromium de sistema ────────────────
        chrome_binary, chromedriver_path = _find_system_chromium()

        if chrome_binary and chromedriver_path:
            log.info(
                "Chromium de sistema detectado",
                binary=chrome_binary,
                driver=chromedriver_path,
            )
            options.binary_location = chrome_binary
            service = Service(executable_path=chromedriver_path)

        else:
            # ── Fallback: webdriver-manager (Windows / Mac) ────────────────
            log.info("Usando webdriver-manager para baixar ChromeDriver", os=self.system)
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(executable_path=ChromeDriverManager().install())

        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.set_page_load_timeout(self.timeout_load * 5)
        self.driver.implicitly_wait(self.timeout_load)
        self.driver.get(self.base_url)
        log.info("Browser aberto", url=self.base_url, os=self.system)
        return self.driver

    def close(self) -> None:
        """Fecha o browser graciosamente."""
        if self.driver:
            try:
                self.driver.quit()
                log.debug("Browser fechado")
            except Exception as exc:
                log.warning("Erro ao fechar browser", error=str(exc))
            finally:
                self.driver = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------
    def __enter__(self):
        return self.open()

    def __exit__(self, *_) -> None:
        self.close()
