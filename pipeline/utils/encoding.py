"""
pipeline/utils/encoding.py
==========================
Detecção automática e fallback de encoding para arquivos de órgãos
governamentais brasileiros (INMET, REDEMET, ANAC, DECEA, etc.).

Extraído de Modulos/DADOS/Engine.py e promovido a utilitário reutilizável.
"""

from __future__ import annotations

import os
from typing import List, Optional, Tuple

from pipeline.core.logger import get_logger

log = get_logger("utils.encoding")

# ---------------------------------------------------------------------------
# Encodings testados em ordem de prioridade
# ---------------------------------------------------------------------------
_ENCODINGS_FALLBACK: List[str] = [
    "utf-8-sig",   # UTF-8 com BOM — exports do Excel / INMET modernos
    "utf-8",
    "ISO-8859-1",  # Latin-1 — padrão histórico INMET / BDMEP
    "ISO-8859-2",  # Latin-2 — sistemas legados regionais
    "cp1252",      # Windows-1252 — muito comum em sistemas gov Windows
    "cp850",       # DOS Latin-1
    "latin-1",     # alias de ISO-8859-1, aceito por mais parsers
]


def detect_encoding(file_path: str) -> Optional[str]:
    """
    Tenta detectar o encoding via chardet (confiança mínima 0.65).

    Retorna o encoding detectado ou None se chardet não estiver instalado
    ou a confiança for baixa.
    """
    try:
        import chardet
        with open(file_path, "rb") as f:
            raw = f.read()
        result = chardet.detect(raw)
        encoding: Optional[str] = result.get("encoding")
        confidence: float = result.get("confidence", 0.0)
        if encoding and confidence >= 0.65:
            log.debug(
                "chardet detectou encoding",
                file=os.path.basename(file_path),
                encoding=encoding,
                confidence=f"{confidence:.0%}",
            )
            return encoding
    except ImportError:
        log.debug("chardet não instalado; usando apenas fallbacks")
    return None


def read_lines_with_fallback(
    file_path: str,
    preferred_encoding: Optional[str] = None,
) -> Tuple[List[str], str]:
    """
    Lê todas as linhas do arquivo tentando múltiplos encodings em ordem:
      1. Encoding detectado pelo chardet
      2. Encoding preferido (parâmetro do usuário)
      3. Lista _ENCODINGS_FALLBACK
      4. UTF-8 com substituição de caracteres inválidos (último recurso)

    Returns:
        (linhas, encoding_utilizado)
    """
    detected = detect_encoding(file_path)

    candidates: List[str] = []
    if detected:
        candidates.append(detected)
    if preferred_encoding and preferred_encoding.lower() not in [
        c.lower() for c in candidates
    ]:
        candidates.append(preferred_encoding)
    for enc in _ENCODINGS_FALLBACK:
        if enc.lower() not in [c.lower() for c in candidates]:
            candidates.append(enc)

    for encoding in candidates:
        try:
            with open(file_path, "r", encoding=encoding, errors="strict") as f:
                lines = f.readlines()
            log.info(
                "Arquivo lido com sucesso",
                file=os.path.basename(file_path),
                encoding=encoding,
            )
            return lines, encoding
        except (UnicodeDecodeError, LookupError):
            continue

    # Último recurso
    log.warning(
        "Nenhum encoding funcionou perfeitamente; usando UTF-8 com replace",
        file=os.path.basename(file_path),
    )
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    return lines, "utf-8 (replace)"
