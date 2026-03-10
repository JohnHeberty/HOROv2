"""
pipeline/services/browser.py
============================
Gerenciamento do navegador Chromium headless para consultas à NOAA.

Migrado de Modulos/BROWSER/Engine.py.
"""

from __future__ import annotations

import platform
from typing import Optional

from pipeline.core.logger import get_logger

log = get_logger("services.browser")


class CBrowser:
    """
    Controla uma instância headless do Chrome para scraping da NOAA.

    Usa webdriver-manager para baixar o ChromeDriver automaticamente em
    qualquer sistema operacional — sem dependência de chrome-win.zip.

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
        """Abre o Chrome headless com ChromeDriver gerenciado automaticamente."""
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager

        options = webdriver.ChromeOptions()
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-software-rasterizer")
        if self.headless:
            options.add_argument("--headless=new")

        driver_path = ChromeDriverManager().install()
        service = Service(executable_path=driver_path)

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
