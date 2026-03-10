"""
pipeline/stages/s01_ingest.py
==============================
Stage 1 — INGEST: Raw → Bronze

Responsabilidades:
  - Detecta encoding do CSV (chardet + fallbacks)
  - Extrai metadados do cabeçalho (nome, lat, lon, altitude)
  - Constrói DataFrame bruto com DATA, DIRECAO, VENTO
  - Salva Parquet + JSON sidecar em data/bronze/
  - Registra resultado em PipelineContext.bronze

Entrada:  data/raw/*.csv
Saída:    data/bronze/{station}.parquet + {station}_meta.json
"""

from __future__ import annotations

import json
import os
import re
import warnings
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from unidecode import unidecode

from pipeline.core.config import PipelineConfig, cfg
from pipeline.core.exceptions import HeaderNotFoundError, MetadataMissingError
from pipeline.core.logger import get_logger
from pipeline.core.models import BronzeRecord, PipelineContext, StationMetadata
from pipeline.utils.encoding import read_lines_with_fallback

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

log = get_logger("s01_ingest")


# ---------------------------------------------------------------------------
# Helpers de parsing
# ---------------------------------------------------------------------------

def _extract_header_value(lines: List[str], label: str, sep: str = ";") -> str:
    """Extrai valor associado a *label* nas linhas de cabeçalho do arquivo."""
    for raw in lines:
        normalized = (
            unidecode(raw.strip())
            .upper()
            .replace(sep, " ")
            .replace("  ", " ")
            .replace(",", ".")
        )
        if label in normalized:
            parts = [p.strip() for p in normalized.split(label) if p.strip()]
            return parts[-1] if parts else "NÃO LOCALIZADO"
    return "NÃO LOCALIZADO"


def _normalize_hour(hora_str: str) -> str:
    """
    Normaliza strings de hora para HH:MM.

    Padrões encontrados nos arquivos gov. brasileiros:
      "0" → "00:00" | "1200" → "12:00" | "100" → "01:00" | "UTC 0000" → "00:00"
    """
    hora = hora_str.replace("(UTC)", "").replace("UTC", "").replace("UTM", "").strip()
    if re.match(r"^\d{1,2}:\d{2}$", hora): return hora
    if re.match(r"^0{1,2}$", hora):        return "00:00"
    if re.match(r"^\d{4}$", hora):         return f"{hora[:2]}:{hora[2:]}"
    if re.match(r"^\d{3}$", hora):         return f"0{hora[0]}:{hora[1:]}"
    return hora


def _parse_dates(df: pd.DataFrame) -> pd.Series:
    """
    Combina colunas de Data e Hora em uma série de datetime.
    Testa 8 formatos comuns + inferência do pandas como fallback.
    """
    FORMATS = [
        "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M", "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H%M",  "%d/%m/%Y %H%M",  "%d-%m-%Y %H%M",  "%Y/%m/%d %H%M",
    ]

    col_data = next((c for c in df.columns if "DATA" in c.upper()), None)
    col_hora = next((c for c in df.columns if "HORA" in c.upper()), None)
    if col_data is None:
        raise ValueError(f"Coluna de data não encontrada. Colunas: {list(df.columns)}")

    df = df.copy()
    if col_hora:
        df[col_hora] = df[col_hora].astype(str).apply(_normalize_hour)
        combined = df[col_data].astype(str).str.strip() + " " + df[col_hora]
    else:
        combined = df[col_data].astype(str).str.strip()

    for fmt in FORMATS:
        try:
            return pd.to_datetime(combined, format=fmt, errors="raise")
        except (ValueError, TypeError):
            continue

    log.warning("Usando inferência automática de datas (pode ser lenta)")
    return pd.to_datetime(combined, infer_datetime_format=True, errors="coerce")


def _build_dataframe(
    lines: List[str],
    sep: str,
    wind_patterns: List[str],
    dir_patterns: List[str],
    gust_patterns: List[str],
    decimal_places: int,
) -> pd.DataFrame:
    """Constrói DataFrame bruto a partir das linhas do arquivo."""
    # Localiza linha de cabeçalho (a primeira com >= 3 colunas distintas)
    header_idx = None
    for i, line in enumerate(lines):
        cols = [c for c in line.split(sep) if c.strip()]
        if sep in line and len(pd.Series(cols).unique()) > 2:
            header_idx = i
            break

    if header_idx is None:
        raise HeaderNotFoundError("<arquivo desconhecido>")

    def _clean(s: str) -> str:
        # strip surrounding quotes → handles "Data";"Vel. Vento (m/s)"... style CSVs
        return unidecode(s.strip()).strip('"').strip("'").replace("  ", " ").replace(",", ".")

    title = [_clean(c) for c in lines[header_idx].split(sep)]
    data  = [
        [_clean(v) for v in line.strip().split(sep)]
        for line in lines[header_idx + 1:]
        if line.strip()
    ]
    df = pd.DataFrame(data, columns=title)
    df["DATA"] = _parse_dates(df)
    df = df.dropna(subset=["DATA"]).reset_index(drop=True)

    # Seleciona DATA + direção + velocidade
    invalid_tokens = {"None", "null", "none", "nan", "NaN", "N/A", "-", "--", "---", "////", "VRB", "vrb", ""}

    def get_col(patterns: List[str], exclude_patterns: Optional[List[str]] = None) -> List[str]:
        """Busca case-insensitive com lista de padrões. Retorna primeiros matches."""
        seen: set = set()
        result: List[str] = []
        for pattern in patterns:
            p_up = pattern.upper()
            for c in df.columns:
                c_up = c.upper()
                if p_up not in c_up:
                    continue
                if exclude_patterns and any(ex.upper() in c_up for ex in exclude_patterns):
                    continue
                if c not in seen:
                    seen.add(c)
                    result.append(c)
            if result:
                break  # para no primeiro padrão que retornar matches
        return result

    dir_cols = get_col(dir_patterns)
    vel_cols = get_col(wind_patterns, exclude_patterns=gust_patterns)

    if not dir_cols or not vel_cols:
        raise ValueError(
            f"Colunas de direção {dir_patterns} ou velocidade {wind_patterns} não localizadas. "
            f"Colunas disponíveis: {list(df.columns)}"
        )

    keep = ["DATA", dir_cols[0], vel_cols[0]]
    df   = df[keep].copy()

    for col in keep[1:]:
        df[col] = df[col].astype(str).str.strip().replace(list(invalid_tokens), np.nan)
        df[col] = pd.to_numeric(
            df[col].str.replace(",", ".", regex=False), errors="coerce"
        ).round(decimal_places)

    df = df.dropna(subset=keep[1:], how="all").reset_index(drop=True)
    df.columns = ["DATA", "direction_raw", "speed_raw"]
    return df


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------

def run(context: PipelineContext, config: PipelineConfig = cfg) -> PipelineContext:
    """
    Executa o Stage 1 para todos os arquivos em context.input_files.

    Popula context.bronze com BronzeRecord por estação.
    """
    log.info("=== STAGE 1 — INGEST (Raw → Bronze) ===", files=len(context.input_files))

    os.makedirs(config.output.data_bronze, exist_ok=True)
    os.makedirs(os.path.join(config.output.data_bronze, "rejected"), exist_ok=True)

    for file_path in context.input_files:
        station = os.path.splitext(os.path.basename(file_path))[0]
        log.info("Ingerindo arquivo", station=station, file=os.path.basename(file_path))

        # --- Verificação de idempotência ---
        parquet_out = os.path.join(config.output.data_bronze, f"{station}.parquet")
        meta_out    = os.path.join(config.output.data_bronze, f"{station}_meta.json")

        try:
            lines, enc = read_lines_with_fallback(file_path, config.data.csv_sep)

            # Metadados
            name      = _extract_header_value(lines, "ESTACAO:",   config.data.csv_sep) or station
            lat_raw   = _extract_header_value(lines, "LATITUDE:",  config.data.csv_sep)
            lon_raw   = _extract_header_value(lines, "LONGITUDE:", config.data.csv_sep)
            alt_raw   = _extract_header_value(lines, "ALTITUDE:",  config.data.csv_sep)

            try:
                lat = float(lat_raw.replace(",", "."))
                lon = float(lon_raw.replace(",", "."))
            except ValueError:
                log.warning(
                    "Coordenadas não encontradas no arquivo — usando lat=0.0 lon=0.0."
                    " Renomeie o arquivo para {CODIGO_INMET}.csv para identificar a estação.",
                    station=station,
                )
                lat, lon = 0.0, 0.0

            meta = StationMetadata(
                name=name,
                latitude=lat,
                longitude=lon,
                altitude=alt_raw,
                file_path=file_path,
                encoding_used=enc,
                ingested_at=datetime.utcnow(),
                parquet_path=parquet_out,
                meta_json_path=meta_out,
            )

            # DataFrame bruto
            df = _build_dataframe(
                lines,
                sep=config.data.csv_sep,
                wind_patterns=config.data.wind_patterns,
                dir_patterns=config.data.direction_patterns,
                gust_patterns=config.data.gust_patterns,
                decimal_places=config.data.decimal_places,
            )

            # Salva Parquet
            df.to_parquet(parquet_out, index=False)

            # Salva sidecar JSON
            with open(meta_out, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "name": meta.name,
                        "latitude": meta.latitude,
                        "longitude": meta.longitude,
                        "altitude": meta.altitude,
                        "file_path": meta.file_path,
                        "encoding_used": meta.encoding_used,
                        "ingested_at": meta.ingested_at.isoformat(),
                        "rows": len(df),
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

            context.bronze[station] = BronzeRecord(metadata=meta, df=df)
            log.info(
                "Ingestão concluída",
                station=station,
                rows=len(df),
                encoding=enc,
            )

        except Exception as exc:
            log.error("Falha na ingestão", station=station, error=str(exc))
            # Move para rejected
            rejected_path = os.path.join(
                config.output.data_bronze, "rejected", os.path.basename(file_path)
            )
            try:
                import shutil
                shutil.copy2(file_path, rejected_path)
            except Exception:
                pass
            context.bronze[station] = BronzeRecord(
                metadata=StationMetadata(
                    name=station, latitude=0.0, longitude=0.0,
                    altitude="", file_path=file_path, encoding_used="",
                ),
                df=pd.DataFrame(),
                rejected=True,
                rejection_reason=str(exc),
            )

    context.stages_executed.append("s01_ingest")
    log.info(
        "Stage 1 finalizado",
        ok=sum(1 for r in context.bronze.values() if not r.rejected),
        rejected=sum(1 for r in context.bronze.values() if r.rejected),
    )
    return context
