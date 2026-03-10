"""
orchestrator.py
===============
Ponto de entrada CLI do pipeline HOROv1.

Uso:
    python orchestrator.py --all
    python orchestrator.py --stage ingest
    python orchestrator.py --all --force
    python orchestrator.py --all --station BRASILIA
    python orchestrator.py --all --dry-run

Desenvolvido por John Heberty de Freitas — john.7heberty@gmail.com
"""

from __future__ import annotations

import argparse
import sys

from pipeline.core.config import cfg
from pipeline.core.logger import configure_logging, get_logger
from pipeline.core.models import PipelineContext

# ---------------------------------------------------------------------------
# Importa estágios
# ---------------------------------------------------------------------------
from pipeline.stages import (
    s01_ingest,
    s02_validate,
    s03_transform,
    s04_analyze,
    s05_enrich,
    s06_optimize,
    s07_export,
)

log = get_logger("orchestrator")

# Mapa de estágios na ordem correta de execução
STAGE_MAP = {
    "ingest":    s01_ingest.run,
    "validate":  s02_validate.run,
    "transform": s03_transform.run,
    "analyze":   s04_analyze.run,
    "enrich":    s05_enrich.run,
    "optimize":  s06_optimize.run,
    "export":    s07_export.run,
}
STAGE_NAMES = list(STAGE_MAP.keys())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_context(station_filter: str | None = None) -> PipelineContext:
    """
    Cria o PipelineContext inicial com os arquivos de entrada.

    Args:
        station_filter: Se fornecido, filtra somente a estação com esse nome.

    Returns:
        PipelineContext com input_files preenchido.
    """
    cfg.ensure_dirs()
    files = cfg.output.input_csvs()
    if not files:
        log.error(
            "Nenhum CSV encontrado em data/raw/",
            path=cfg.output.data_raw,
        )
        sys.exit(1)

    if station_filter:
        files = [f for f in files if station_filter.upper() in f.upper()]
        if not files:
            log.error(
                "Nenhum arquivo corresponde ao filtro de estação",
                station=station_filter,
            )
            sys.exit(1)

    log.info("Arquivos de entrada", count=len(files))
    return PipelineContext(input_files=files)


def run_pipeline(
    stages: list[str],
    station_filter: str | None = None,
    dry_run: bool = False,
) -> PipelineContext:
    """
    Executa os estágios especificados na ordem correta.

    Args:
        stages:         Lista de nomes de estágios a executar.
        station_filter: Filtra arquivos por nome de estação.
        dry_run:        Apenas lista o que seria executado.

    Returns:
        PipelineContext preenchido ao final da execução.
    """
    context = build_context(station_filter)

    for stage_name in STAGE_NAMES:
        if stage_name not in stages:
            continue

        if dry_run:
            log.info(f"[DRY-RUN] Executaria: {stage_name}")
            continue

        log.info(f"Iniciando estágio: {stage_name}")
        fn = STAGE_MAP[stage_name]
        context = fn(context, cfg)

    if not dry_run:
        log.info(
            "Pipeline concluído",
            stages=context.stages_executed,
            failed=context.stages_failed,
        )

    return context


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="orchestrator",
        description="Pipeline ETL HOROv1 — Análise de Vento para Aeródromos.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all",
        action="store_true",
        help="Executa todos os 7 estágios.",
    )
    group.add_argument(
        "--stage",
        choices=STAGE_NAMES,
        help="Executa somente o estágio especificado.",
    )

    parser.add_argument(
        "--station",
        default=None,
        metavar="NOME",
        help="Processa apenas a estação cujo nome de arquivo contém NOME (case-insensitive).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignora cache e força re-processamento.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Lista o que seria executado sem executar.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nível de log (padrão: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    configure_logging(level=args.log_level)

    stages = STAGE_NAMES if args.all else [args.stage]

    run_pipeline(
        stages=stages,
        station_filter=args.station,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
