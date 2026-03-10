"""
pipeline/core/logger.py
=======================
Logging estruturado para o pipeline HOROv1.

Uso:
    from pipeline.core.logger import get_logger
    log = get_logger("s01_ingest")
    log.info("Arquivo lido", station="BRASILIA", rows=87600)
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Any


# ---------------------------------------------------------------------------
# Formatter que injeta contexto extra na mensagem
# ---------------------------------------------------------------------------
class _ContextFormatter(logging.Formatter):
    """Inclui pares chave=valor extras passados via 'extra' no log record."""

    FMT = "[{asctime}] {levelname:<5} [{name}] {message}{context}"
    DATE_FMT = "%Y-%m-%d %H:%M:%S"

    def __init__(self) -> None:
        super().__init__(fmt=self.FMT, datefmt=self.DATE_FMT, style="{")

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        ctx = getattr(record, "_ctx", {})
        if ctx:
            record.context = "  " + "  ".join(f"{k}={v}" for k, v in ctx.items())
        else:
            record.context = ""
        return super().format(record)


# ---------------------------------------------------------------------------
# Logger wrapper com contexto estruturado
# ---------------------------------------------------------------------------
class StructuredLogger:
    """
    Wrapper em torno de logging.Logger que suporta kwargs estruturados.

    Uso:
        log = StructuredLogger("s03_transform")
        log.info("Dado limpo", station="BRASILIA", rows=87600)
    """

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)

    def _log(self, level: int, msg: str, **kwargs: Any) -> None:
        extra = {"_ctx": kwargs}
        self._logger.log(level, msg, extra=extra, stacklevel=3)

    def debug(self, msg: str, **kwargs: Any) -> None:
        self._log(logging.DEBUG, msg, **kwargs)

    def info(self, msg: str, **kwargs: Any) -> None:
        self._log(logging.INFO, msg, **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:
        self._log(logging.WARNING, msg, **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:
        self._log(logging.ERROR, msg, **kwargs)

    def critical(self, msg: str, **kwargs: Any) -> None:
        self._log(logging.CRITICAL, msg, **kwargs)

    def exception(self, msg: str, **kwargs: Any) -> None:
        self._logger.exception(msg, extra={"_ctx": kwargs}, stacklevel=2)


# ---------------------------------------------------------------------------
# Configuração global do sistema de logging
# ---------------------------------------------------------------------------
_configured = False


def configure_logging(
    level: str = "INFO",
    log_file: str | None = None,
) -> None:
    """
    Configura o handler raiz.  Deve ser chamado UMA vez no início do pipeline.

    Args:
        level:    Nível mínimo de log ('DEBUG', 'INFO', 'WARNING', 'ERROR').
        log_file: Caminho opcional para gravar o log em arquivo.
    """
    global _configured
    if _configured:
        return

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    fmt = _ContextFormatter()

    # Console
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    # Arquivo opcional
    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)

    _configured = True


def get_logger(name: str) -> StructuredLogger:
    """
    Retorna um StructuredLogger com o nome fornecido.
    Se o logging ainda não foi configurado, chama configure_logging() com defaults.
    """
    if not _configured:
        configure_logging()
    return StructuredLogger(name)
