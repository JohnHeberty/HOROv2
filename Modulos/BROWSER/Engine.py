"""
Modulos/BROWSER/Engine.py
=========================
Gerenciamento do navegador Chromium headless para consultas à NOAA.

Restaurado de .trash/Modulos/BROWSER/Engine.py e atualizado para:
  - Suportar Windows (chrome-win.zip local no mesmo diretório) e Linux/macOS
  - Usar webdriver-manager como fallback quando o zip local não estiver presente
  - Implementar context manager (__enter__ / __exit__) para uso com 'with'

Uso:
    from Modulos.BROWSER.Engine import CBrowser

    with CBrowser() as driver:
        driver.get("https://ngdc.noaa.gov/geomag/calculators/magcalc.shtml")
        ...
"""

from __future__ import annotations

import os
import platform
import shutil
import zipfile
from typing import Optional

# ---------------------------------------------------------------------------
# Caminhos internos do módulo
# ---------------------------------------------------------------------------
_MODULE_DIR    = os.path.dirname(os.path.abspath(__file__))
_ZIP_PATH      = os.path.join(_MODULE_DIR, "chrome-win.zip")
_CHROME_DIR    = os.path.join(_MODULE_DIR, "chrome-win")
_CHROME_EXE    = os.path.join(_CHROME_DIR, "chrome.exe")
_DRIVER_EXE    = os.path.join(_CHROME_DIR, "chromedriver.exe")

_BASE_URL      = "https://ngdc.noaa.gov/geomag/calculators/magcalc.shtml"
_TIMEOUT       = 60


class CBrowser:
    """
    Controla uma instância headless do Chrome para scraping da NOAA.

    Em Windows: tenta usar o binário local extraído de chrome-win.zip (sem
    necessidade de instalação).  Se o zip não estiver presente, cai para o
    webdriver-manager automático.

    Em Linux / macOS: usa webdriver-manager diretamente.

    Uso como context manager:
        with CBrowser() as driver:
            driver.get(url)
    """

    BASE_URL = _BASE_URL

    def __init__(
        self,
        base_url: str = _BASE_URL,
        timeout_load: int = _TIMEOUT,
        headless: bool = True,
    ) -> None:
        self.base_url     = base_url
        self.timeout_load = timeout_load
        self.headless     = headless
        self.system       = platform.system()
        self.driver       = None

        # No Windows, extrai o zip local se ainda não extraído
        if self.system == "Windows" and os.path.exists(_ZIP_PATH):
            if not os.path.isdir(_CHROME_DIR):
                self._extract_zip()

    # ------------------------------------------------------------------
    # Setup interno
    # ------------------------------------------------------------------

    def _extract_zip(self) -> None:
        """Extrai chrome-win.zip para a pasta do módulo."""
        try:
            if os.path.exists(_CHROME_DIR):
                shutil.rmtree(_CHROME_DIR, ignore_errors=True)
            with zipfile.ZipFile(_ZIP_PATH, "r") as zf:
                zf.extractall(_MODULE_DIR)
        except Exception as exc:
            print(f"[CBrowser] Aviso: não foi possível extrair chrome-win.zip — {exc}")

    # ------------------------------------------------------------------
    # Abertura do browser
    # ------------------------------------------------------------------

    def open(self):
        """Abre o Chrome headless e retorna o driver Selenium."""
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service

        options = webdriver.ChromeOptions()
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-software-rasterizer")
        if self.headless:
            options.add_argument("--headless=new")

        if self.system == "Windows" and os.path.isfile(_CHROME_EXE) and os.path.isfile(_DRIVER_EXE):
            # Usa binário local extraído do zip
            options.binary_location = _CHROME_EXE
            service = Service(executable_path=_DRIVER_EXE)
        else:
            # Fallback: webdriver-manager baixa o ChromeDriver automaticamente
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                service = Service(executable_path=ChromeDriverManager().install())
            except ImportError:
                raise RuntimeError(
                    "webdriver-manager não instalado e chrome-win.zip não encontrado. "
                    "Execute: pip install webdriver-manager"
                )

        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.set_page_load_timeout(self.timeout_load * 5)
        self.driver.implicitly_wait(self.timeout_load)
        self.driver.get(self.base_url)
        return self.driver

    # ------------------------------------------------------------------
    # Encerramento
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Fecha o browser graciosamente."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as exc:
                print(f"[CBrowser] Erro ao fechar browser: {exc}")
            finally:
                self.driver = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self):
        return self.open()

    def __exit__(self, *_) -> None:
        self.close()
