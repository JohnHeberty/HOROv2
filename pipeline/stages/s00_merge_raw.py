"""
pipeline/stages/s00_merge_raw.py
=================================
Stage 0 — MERGE RAW

Mescla múltiplos CSVs da mesma estação em data/raw/ em um único arquivo
e remove linhas duplicadas (por timestamp). Os arquivos originais são
deletados após a fusão, deixando apenas o arquivo consolidado.

Regra de agrupamento:
  • CSVs com exatamente as mesmas colunas de cabeçalho são considerados
    da mesma fonte/estação e fundidos em um único arquivo.

Saída:  um arquivo <nome_base>.csv por grupo em data/raw/  (os demais deletados)
"""

from __future__ import annotations

import os
from collections import defaultdict
from typing import List, Tuple

import pandas as pd

from pipeline.core.config import PipelineConfig, cfg
from pipeline.core.logger import get_logger
from pipeline.utils.encoding import read_lines_with_fallback

log = get_logger("s00_merge_raw")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_header(file_path: str, sep: str) -> Tuple[List[str], str]:
    """
    Lê o arquivo e retorna (header_columns, encoding).
    Identifica a linha de cabeçalho como a primeira com >= 2 colunas.
    """
    lines, enc = read_lines_with_fallback(file_path, sep)
    for line in lines:
        cols = [c.strip().strip('"').strip("'") for c in line.split(sep)]
        if len(cols) >= 2:
            return cols, enc
    return [], ""


def _read_csv(file_path: str, sep: str) -> pd.DataFrame:
    """Lê um CSV individual de data/raw/ como DataFrame."""
    lines, enc = read_lines_with_fallback(file_path, sep)

    # Encontra linha de cabeçalho (primeira com >= 2 colunas)
    header_idx = None
    for i, line in enumerate(lines):
        cols = [c.strip() for c in line.split(sep)]
        if len(cols) >= 2:
            header_idx = i
            break

    if header_idx is None:
        raise ValueError(f"Cabeçalho não encontrado em {file_path}")

    # Limpa aspas do cabeçalho
    raw_header = lines[header_idx].split(sep)
    columns = [c.strip().strip('"').strip("'") for c in raw_header]

    rows = []
    for line in lines[header_idx + 1:]:
        if not line.strip():
            continue
        parts = line.strip().split(sep)
        # Garante mesmo número de colunas
        while len(parts) < len(columns):
            parts.append("")
        parts = parts[: len(columns)]
        rows.append([v.strip().strip('"').strip("'") for v in parts])

    return pd.DataFrame(rows, columns=columns)


def _dedup_key_cols(df: pd.DataFrame) -> List[str]:
    """
    Retorna as colunas usadas para deduplicação (Data + Hora, se existirem).
    Fallback: todas as colunas.
    """
    cols_upper = {c.upper(): c for c in df.columns}
    date_col = next((cols_upper[k] for k in cols_upper if "DATA" in k or "DATE" in k), None)
    hour_col = next((cols_upper[k] for k in cols_upper if "HORA" in k or "HOUR" in k or "TIME" in k), None)

    if date_col and hour_col:
        return [date_col, hour_col]
    if date_col:
        return [date_col]
    return list(df.columns)


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------

def run(raw_dir: str | None = None, config: PipelineConfig = cfg) -> List[str]:
    """
    Mescla todos os arquivos CSV de data/raw/ com o mesmo cabeçalho em um
    único arquivo consolidado e remove os originais.

    Args:
        raw_dir: Pasta com arquivos raw. Padrão: config.output.data_raw
        config:  Configuração do pipeline.

    Returns:
        Lista de caminhos dos arquivos consolidados gerados.
    """
    raw_dir = raw_dir or config.output.data_raw
    sep     = config.data.csv_sep

    csv_files = sorted([
        os.path.join(raw_dir, f)
        for f in os.listdir(raw_dir)
        if f.lower().endswith(".csv")
    ])

    if not csv_files:
        log.info("Nenhum CSV encontrado em data/raw/ — merge ignorado")
        return []

    if len(csv_files) == 1:
        log.info("Apenas 1 CSV encontrado — merge desnecessário", file=os.path.basename(csv_files[0]))
        return csv_files

    log.info("=== STAGE 0 — MERGE RAW ===", n_files=len(csv_files))

    # -----------------------------------------------------------------------
    # 1ª passagem: lê cabeçalho de cada arquivo e agrupa por colunas
    # -----------------------------------------------------------------------
    # Chave do grupo = tupla de colunas (frozen set para normalização)
    groups: dict[tuple, dict] = defaultdict(lambda: {"files": [], "columns": None})

    for fp in csv_files:
        try:
            cols, _ = _read_header(fp, sep)
            if not cols:
                log.warning("Cabeçalho vazio ou não detectado — ignorando", file=os.path.basename(fp))
                continue
            key = tuple(cols)  # preserva ordem exata
            groups[key]["files"].append(fp)
            groups[key]["columns"] = cols
        except Exception as exc:
            log.error("Falha ao ler cabeçalho", file=os.path.basename(fp), error=str(exc))

    # -----------------------------------------------------------------------
    # 2ª passagem: mescla, deduplica e salva
    # -----------------------------------------------------------------------
    output_files: List[str] = []

    for key, entry in groups.items():
        files: List[str] = sorted(entry["files"])

        if len(files) == 1:
            log.info("Grupo com 1 arquivo — nenhuma fusão necessária", file=os.path.basename(files[0]))
            output_files.append(files[0])
            continue

        log.info(
            "Fundindo arquivos",
            n_files=len(files),
            files=[os.path.basename(f) for f in files],
        )

        dfs = []
        for fp in files:
            try:
                df = _read_csv(fp, sep)
                dfs.append(df)
                log.info("  Lido", file=os.path.basename(fp), rows=len(df))
            except Exception as exc:
                log.error("Falha ao ler arquivo", file=os.path.basename(fp), error=str(exc))

        if not dfs:
            continue

        # Concatena todos
        df_merged = pd.concat(dfs, ignore_index=True)
        rows_before = len(df_merged)

        # Deduplica por timestamp (Data + Hora)
        dedup_cols = _dedup_key_cols(df_merged)
        df_merged = df_merged.drop_duplicates(subset=dedup_cols, keep="first")
        df_merged = df_merged.reset_index(drop=True)
        rows_after = len(df_merged)

        log.info(
            "Merge concluído",
            rows_total=rows_before,
            rows_after_dedup=rows_after,
            duplicatas_removidas=rows_before - rows_after,
            dedup_cols=dedup_cols,
        )

        # Nome de saída: primeiro arquivo em ordem alfabética (mais simples/limpo)
        output_path = files[0]

        # Salva com separador original e aspas para manter compatibilidade
        df_merged.to_csv(
            output_path,
            sep=sep,
            index=False,
            quoting=1,   # csv.QUOTE_ALL → mantém formato original
            encoding="utf-8-sig",
        )
        log.info("Arquivo consolidado salvo", path=output_path, rows=rows_after)

        # Deleta os demais arquivos do grupo
        for fp in files[1:]:
            try:
                os.remove(fp)
                log.info("Arquivo original deletado", file=os.path.basename(fp))
            except Exception as exc:
                log.warning("Não foi possível deletar", file=os.path.basename(fp), error=str(exc))

        output_files.append(output_path)

    log.info("Stage 0 finalizado", arquivos_consolidados=len(output_files))
    return output_files
