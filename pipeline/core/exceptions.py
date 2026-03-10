"""
pipeline/core/exceptions.py
===========================
Hierarquia de exceções customizadas do pipeline HOROv1.

Princípio: nenhum estágio usa `except Exception: pass`.
Toda exceção tem contexto suficiente para diagnóstico.
"""


# ---------------------------------------------------------------------------
# Raiz
# ---------------------------------------------------------------------------
class HOROError(Exception):
    """Exceção base para todos os erros do pipeline HOROv1."""


# ---------------------------------------------------------------------------
# Stage 1 — Ingestão
# ---------------------------------------------------------------------------
class IngestError(HOROError):
    """Erro durante a ingestão de dados (leitura de arquivo raw)."""


class EncodingError(IngestError):
    """Nenhum encoding funcionou para decodificar o arquivo."""

    def __init__(self, file_path: str) -> None:
        super().__init__(
            f"Não foi possível ler '{file_path}' com nenhum encoding conhecido. "
            f"Verifique a integridade do arquivo."
        )
        self.file_path = file_path


class MetadataMissingError(IngestError):
    """Metadados obrigatórios (nome, lat, lon, alt) não encontrados no arquivo."""

    def __init__(self, file_path: str, fields: list) -> None:
        super().__init__(
            f"Metadados ausentes em '{file_path}': {fields}. "
            f"O arquivo segue o formato INMET/REDEMET?"
        )
        self.file_path = file_path
        self.fields = fields


class HeaderNotFoundError(IngestError):
    """Cabeçalho de dados não encontrado após varredura das primeiras linhas."""

    def __init__(self, file_path: str) -> None:
        super().__init__(
            f"Cabeçalho de dados não encontrado em '{file_path}'. "
            f"Verifique se o arquivo contém colunas de data/hora e vento."
        )
        self.file_path = file_path


# ---------------------------------------------------------------------------
# Stage 2 — Validação
# ---------------------------------------------------------------------------
class ValidationError(HOROError):
    """Erro de qualidade de dados na camada Bronze."""


class SchemaError(ValidationError):
    """Colunas obrigatórias ausentes no DataFrame."""

    def __init__(self, station: str, missing: list) -> None:
        super().__init__(
            f"Estação '{station}' não possui as colunas obrigatórias: {missing}"
        )
        self.station = station
        self.missing = missing


class DataQualityError(ValidationError):
    """Percentual de dados inválidos excede o limite tolerado."""

    def __init__(self, station: str, col: str, null_pct: float, limit: float) -> None:
        super().__init__(
            f"Estação '{station}': coluna '{col}' tem {null_pct:.1%} de nulos "
            f"(limite: {limit:.1%})."
        )
        self.station = station
        self.col = col
        self.null_pct = null_pct


# ---------------------------------------------------------------------------
# Stage 3 — Transformação
# ---------------------------------------------------------------------------
class TransformError(HOROError):
    """Erro durante normalização / conversão de unidades."""


# ---------------------------------------------------------------------------
# Stage 4 — Análise
# ---------------------------------------------------------------------------
class AnalysisError(HOROError):
    """Erro no cálculo da tabela de ventos ou setores."""


# ---------------------------------------------------------------------------
# Stage 5 — Enriquecimento (Declinação magnética)
# ---------------------------------------------------------------------------
class EnrichmentError(HOROError):
    """Erro durante enriquecimento externo."""


class MagneticDeclinationError(EnrichmentError):
    """Falha ao obter declinação magnética da NOAA."""

    def __init__(self, lat: float, lon: float) -> None:
        super().__init__(
            f"Não foi possível obter a declinação magnética para "
            f"Lat={lat}, Lon={lon}. "
            f"Verifique a conexão com o site e tente novamente."
        )
        self.lat = lat
        self.lon = lon


# ---------------------------------------------------------------------------
# Stage 6 — Otimização
# ---------------------------------------------------------------------------
class OptimizationError(HOROError):
    """Erro no cálculo de orientação ótima de pista."""


# ---------------------------------------------------------------------------
# Stage 7 — Exportação
# ---------------------------------------------------------------------------
class ExportError(HOROError):
    """Erro na geração de vídeo, GIF ou relatório final."""


# ---------------------------------------------------------------------------
# Orquestrador
# ---------------------------------------------------------------------------
class PipelineAbortError(HOROError):
    """Erro crítico que interrompe toda a execução do pipeline."""

    def __init__(self, stage: str, reason: str) -> None:
        super().__init__(f"Pipeline abortado no estágio '{stage}': {reason}")
        self.stage = stage
        self.reason = reason
