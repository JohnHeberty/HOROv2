"""
pipeline/core/logger.py
=======================
Logging estruturado para o pipeline HOROv1.

Boas práticas de auditoria:
  - Todo run grava em   logs/pipeline_run.log      (sobrescreve — log do último run)
  - Cópia timestampada em logs/archive/pipeline_YYYYMMDD_HHMMSS.log
  - logs/ NÃO fica em bronze (camada de dados). Bronze = dados brutos ingeridos.
  - logs/ está no .gitignore

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
from pathlib import Path
from typing import Any

# Diretório de logs na raiz do projeto
_REPO_ROOT   = Path(__file__).resolve().parents[2]
LOGS_DIR     = _REPO_ROOT / "logs"
ARCHIVE_DIR  = LOGS_DIR / "archive"
LOG_LATEST   = LOGS_DIR / "pipeline_run.log"   # sempre o último run


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
    log_file: str | None = "AUTO",
) -> None:
    """
    Configura o handler raiz.  Deve ser chamado UMA vez no início do pipeline.

    Estratégia de auditoria (camada correta = logs/, NÃO bronze):
      • logs/pipeline_run.log          — sobrescrito a cada run (fácil de ler o último)
      • logs/archive/pipeline_<ts>.log — cópia imutável por timestamp

    Args:
        level:    Nível mínimo de log ('DEBUG', 'INFO', 'WARNING', 'ERROR').
        log_file: Caminho do arquivo de log.
                  'AUTO' (padrão) → usa logs/pipeline_run.log + arquivo em logs/archive/.
                  None            → somente console (sem arquivo).
                  <path>          → arquivo no caminho informado.
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

    # ── Arquivo de log ────────────────────────────────────────────────────────
    resolved_file: str | None = None

    if log_file == "AUTO":
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        resolved_file = str(LOG_LATEST)           # pipeline_run.log (ow)
        # Cópia com timestamp para auditoria
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = ARCHIVE_DIR / f"pipeline_{ts}.log"
        _add_file_handler(root, fmt, str(archive_path), mode="w")
    elif log_file is not None:
        resolved_file = log_file

    if resolved_file:
        _add_file_handler(root, fmt, resolved_file, mode="w")

    _configured = True


def _add_file_handler(
    logger: logging.Logger,
    fmt: logging.Formatter,
    path: str,
    mode: str = "a",
) -> None:
    """Adiciona um FileHandler ao logger raiz."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fh = logging.FileHandler(path, encoding="utf-8", mode=mode)
    fh.setFormatter(fmt)
    logger.addHandler(fh)


def get_logger(name: str) -> StructuredLogger:
    """
    Retorna um StructuredLogger com o nome fornecido.
    Se o logging ainda não foi configurado, chama configure_logging() com defaults.
    """
    if not _configured:
        configure_logging()
    return StructuredLogger(name)
