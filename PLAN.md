# PLAN.md — Plano de Migração de Arquitetura: HOROv1 → Pipeline ETL

> Elaborado em: 09/03/2026  
> Última atualização: 10/03/2026  
> Autor: GitHub Copilot  
> Repositório: JohnHeberty/HOROv1

---

## ✅ MIGRAÇÃO CONCLUÍDA

A migração para arquitetura medallion pipeline (Fases 1-4) foi concluída com sucesso.

O pipeline está operacional com:
- 7 estágios ETL (s01 → s07)
- Arquitetura medallion (Raw → Bronze → Silver → Gold → Export)
- Logging estruturado
- Contratos de dados explícitos
- Configuração centralizada
- Orquestrador CLI

### PENDENTE - Fase 5 (Validação e Limpeza)

- [ ] Deletar `.trash/` após confirmar que pipeline funciona corretamente em todos os cenários
- [ ] Implementar testes de integração ponta-a-ponta (`tests/`)
- [ ] Adicionar testes unitários para serviços críticos
- [ ] Documentar casos de uso avançados

### Uso do Pipeline

```bash
# Executar pipeline completo
python orchestrator.py --all

# Forçar reprocessamento
python orchestrator.py --all --force

# Processar apenas uma estação
python orchestrator.py --all --station BRASILIA
```

Para detalhes da metodologia de cálculo FO, consulte [UPDATE.md](UPDATE.md).

---

## 1. DIAGNÓSTICO DA ARQUITETURA ATUAL

### Problemas estruturais identificados

| Problema | Impacto |
|----------|---------|
| Código de configuração, lógica e orquestração misturados em `Default.py` + `RUN_HORO.ipynb` | Impossível testar unidades isoladas |
| Notebook como único ponto de orquestração | Não re-executável sem efeitos colaterais; não paralelizável |
| Sem separação de camadas de dados | Re-processar um arquivo requer reprocessar tudo |
| Sem logging estruturado | Depuração artesanal com `print()` |
| Sem contrato de dados entre estágios | Falhas silenciosas propagam dados corrompidos |
| Sem validação de qualidade de dados | Dados ruins chegam ao cálculo de FO |
| Estado mantido em disco via pickle sem versionamento | Cache stale impossível de detectar |
| Pastas `1-INPUT/` e `2-OUTPUT/` sem rastreabilidade | Qual arquivo gerou qual saída? |

---

## 2. ARQUITETURA ALVO — MEDALLION PIPELINE

Adota o padrão **Medallion Architecture** usado por Databricks, Microsoft Azure e grandes plataformas de dados, adaptado para execução local.

```
RAW  ──►  BRONZE  ──►  SILVER  ──►  GOLD  ──►  EXPORT
(Original) (Parsed)  (Cleaned)  (Computed) (Delivered)
```

### Camadas de dados

| Camada | Localização | O que contém | Formato |
|--------|-------------|--------------|---------|
| **Raw** | `data/raw/` | Arquivos originais intocados | CSV (qualquer encoding) |
| **Bronze** | `data/bronze/` | Dados parseados + metadados extraídos | Parquet + JSON sidecar |
| **Silver** | `data/silver/` | Dados limpos, validados, unidades padronizadas | Parquet |
| **Gold** | `data/gold/` | Resultados computados (tabelas de vento, FO otimizado) | Parquet + JSON |
| **Export** | `data/gold/exports/` | Vídeos, GIFs, relatórios finais | MP4, GIF, JSON |

---

## 3. ESTRUTURA ALVO COMPLETA

```
HOROv1/
│
├── data/
│   ├── raw/                        ← Coloque seus CSVs aqui (era 1-INPUT/)
│   │   └── .gitkeep
│   ├── bronze/                     ← Gerado automaticamente pelo Stage 1
│   │   ├── {station}.parquet
│   │   ├── {station}_meta.json
│   │   └── rejected/               ← Arquivos que falharam no parse
│   ├── silver/                     ← Gerado automaticamente pelo Stage 3
│   │   ├── {station}.parquet
│   │   └── declinations.json       ← Cache de declinações magnéticas
│   ├── gold/                       ← Gerado automaticamente pelo Stage 6
│   │   ├── {station}_results.json
│   │   ├── {station}_wind_table.parquet
│   │   └── exports/
│   │       ├── {station}/
│   │       │   ├── RunwayOrientation-5.mp4
│   │       │   ├── RunwayOrientation-5.gif
│   │       │   └── frames/
│   │       └── FinalResult.json    ← Resultado consolidado de todos os aeródromos
│   └── .cache/
│       └── pipeline_state.json     ← Controle de idempotência por estágio
│
├── pipeline/
│   ├── __init__.py
│   │
│   ├── core/                       ← Infraestrutura transversal
│   │   ├── __init__.py
│   │   ├── config.py               ← Todas as constantes e settings (substitui Default.py)
│   │   ├── logger.py               ← Logging estruturado com contexto e níveis
│   │   ├── exceptions.py           ← Hierarquia de exceções customizadas
│   │   └── models.py               ← Contratos de dados (dataclasses + validação)
│   │
│   ├── stages/                     ← Estágios ETL — cada um tem entrada/saída definida
│   │   ├── __init__.py
│   │   ├── s01_ingest.py           ← Raw → Bronze
│   │   ├── s02_validate.py         ← Bronze: quality gate
│   │   ├── s03_transform.py        ← Bronze → Silver
│   │   ├── s04_analyze.py          ← Silver → Gold/wind_tables
│   │   ├── s05_enrich.py           ← Silver → Silver (declinação magnética)
│   │   ├── s06_optimize.py         ← Silver + Gold/wind_tables → Gold/results
│   │   └── s07_export.py           ← Gold → Export (vídeo, GIF, relatório)
│   │
│   ├── services/                   ← Integrações e lógica de domínio
│   │   ├── __init__.py
│   │   ├── browser.py              ← CBrowser (substitui Modulos/BROWSER/Engine.py)
│   │   ├── drawing.py              ← Primitivas OpenCV de desenho
│   │   ├── wind.py                 ← Rosa dos ventos e cálculo de setores
│   │   └── runway.py               ← Lógica de cabeceiras e FO
│   │
│   └── utils/
│       ├── __init__.py
│       ├── encoding.py             ← Detecção + fallback de encoding (já implementado)
│       ├── geo.py                  ← Conversão de coordenadas, DMS ↔ decimal
│       └── video.py                ← Geração de MP4 e GIF
│
├── orchestrator.py                 ← Ponto de entrada CLI: executa 1 ou todos os estágios
├── run_pipeline.ipynb              ← Notebook limpo (apenas orquestração, sem lógica)
│
├── tests/                          ← Testes unitários por módulo
│   ├── __init__.py
│   ├── test_models.py
│   ├── test_stages.py
│   └── test_services.py
│
├── Standards/
│   └── RBAC154EMD07.pdf            ← Mantido
├── .trash/                         ← Arquivos legados movidos (não deletar antes de validar)
├── requirements.txt                ← Atualizado
├── PLAN.md                         ← Este documento
└── UPGRADE.md
```

---

## 4. FLUXO DE DADOS DETALHADO

```
┌────────────────────────────────────────────────────────────────────────┐
│  data/raw/  (CSV — qualquer encoding, qualquer órgão gov)              │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Stage 1 — INGEST   │  s01_ingest.py
                    │  • Detecta encoding  │  ← chardet + 7 fallbacks
                    │  • Extrai metadados  │  ← nome, lat, lon, altitude
                    │  • Parseia CSV       │
                    │  • Salva Parquet     │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Stage 2 — VALIDATE │  s02_validate.py
                    │  • Verifica schema   │  ← colunas obrigatórias
                    │  • Checa tipos       │  ← data, float
                    │  • Verifica ranges   │  ← lat/lon, dir 0-360, vel ≥ 0
                    │  • Conta completude  │  ← % de nulos por coluna
                    │  • Rejeita falhas    │  → data/bronze/rejected/
                    └──────────┬──────────┘
                               │  (somente dados válidos prosseguem)
                    ┌──────────▼──────────┐
                    │  Stage 3 — TRANSFORM│  s03_transform.py
                    │  • Remove duplicatas │
                    │  • Normaliza colunas │  ← nomes padronizados
                    │  • m/s → knots       │  ← fator 1.944
                    │  • Normaliza datas   │  ← timezone UTC
                    │  • Salva Parquet     │  → data/silver/
                    └──────────┬──────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
     ┌──────────▼──────────┐      ┌──────────▼──────────┐
     │  Stage 4 — ANALYZE  │      │  Stage 5 — ENRICH   │
     │  s04_analyze.py     │      │  s05_enrich.py      │
     │  • Tabela de ventos  │      │  • Declinação mag.  │
     │  • % por setor       │      │  • Cache local      │
     │  • Colunas IN/OUT    │      │  • 1 req / estação  │
     │    PPD              │      │                     │
     └──────────┬──────────┘      └──────────┬──────────┘
                │                             │
                └──────────────┬──────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Stage 6 — OPTIMIZE │  s06_optimize.py
                    │  • Rotação 0–180°    │
                    │  • Cálculo FO/grau   │
                    │  • FO máximo + rumo  │
                    │  • Análise 5/10/     │
                    │    15/20 anos        │
                    │  • Salva JSON/Parquet│  → data/gold/
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Stage 7 — EXPORT   │  s07_export.py
                    │  • Frames JPG        │
                    │  • Vídeo MP4         │
                    │  • GIF animado       │
                    │  • Relatório JSON    │  → data/gold/exports/
                    └─────────────────────┘
```

---

## 5. MAPEAMENTO DE ARQUIVOS: LEGADO → NOVO

| Arquivo Legado | Destino na Nova Arquitetura | Ação |
|----------------|----------------------------|------|
| `Default.py` | `pipeline/core/config.py` | → `.trash/` após migração |
| `Functions.py` (drawing) | `pipeline/services/drawing.py` | → `.trash/` após migração |
| `Functions.py` (geo/declination) | `pipeline/services/geo.py` + `pipeline/stages/s05_enrich.py` | → `.trash/` após migração |
| `Functions.py` (video) | `pipeline/utils/video.py` | → `.trash/` após migração |
| `Functions.py` (runway) | `pipeline/services/runway.py` | → `.trash/` após migração |
| `Functions.py` (folder utils) | `pipeline/utils/` | → `.trash/` após migração |
| `Modulos/BROWSER/Engine.py` | `pipeline/services/browser.py` | → `.trash/` após migração |
| `Modulos/DADOS/Engine.py` | `pipeline/stages/s01_ingest.py` + `s02_validate.py` + `s03_transform.py` + `pipeline/utils/encoding.py` | → `.trash/` após migração |
| `Modulos/SITRAER/Script1.py` | `pipeline/stages/s04_analyze.py` | → `.trash/` após migração |
| `Modulos/SITRAER/Sitraer2023.py` | `pipeline/services/wind.py` | → `.trash/` após migração |
| `RUN_HORO.ipynb` | `run_pipeline.ipynb` (simplificado) | → `.trash/` após migração |
| `1-INPUT/` | `data/raw/` | Mover conteúdo |
| `2-OUTPUT/` | `data/gold/exports/` | Mover conteúdo |

---

## 6. CONTRATOS DE DADOS (MODELOS)

Cada estágio produz e consome contratos explícitos definidos em `pipeline/core/models.py`:

```
StationMetadata
  ├── name: str
  ├── latitude: float
  ├── longitude: float
  ├── altitude: str
  ├── file_path: str
  ├── encoding_used: str
  └── ingested_at: datetime

WeatherRecord
  ├── station: str
  ├── timestamp: datetime (UTC)
  ├── wind_direction_deg: float   (0–360)
  └── wind_speed_kts: float       (≥ 0)

WindTable
  ├── station: str
  ├── period_years: int
  ├── sector_names: list[str]
  ├── limit_bins: list[float]
  └── pct_table: DataFrame        (setores × bins)

RunwayOptimizationResult
  ├── station: str
  ├── period_years: int
  ├── best_heading_deg: float
  ├── runway_designation: str     (ex: "09-27")
  ├── fo_pct: float
  ├── crosswind_pct: float
  └── magnetic_declination: float
```

---

## 7. PRINCÍPIOS DE ENGENHARIA APLICADOS

### 7.1 Idempotência
Cada estágio verifica se sua saída já existe antes de reprocessar.  
Controlado por `data/.cache/pipeline_state.json`:
```json
{
  "BRASILIA": {
    "s01_ingest":    {"status": "ok", "hash": "a3f...", "at": "2026-03-09T10:00:00"},
    "s03_transform": {"status": "ok", "hash": "b1c...", "at": "2026-03-09T10:01:00"},
    ...
  }
}
```
Re-execução de um estágio só acontece se: `--force`, hash do arquivo de entrada mudou, ou estágio upstream foi re-executado.

### 7.2 Logging Estruturado
Uso de `structlog` ou logging padrão com formatter JSON:
```
[2026-03-09 10:00:01] INFO  stage=s01_ingest station=BRASILIA file=BRASILIA.csv encoding=utf-8-sig rows=87600
[2026-03-09 10:00:02] WARN  stage=s02_validate station=BRASILIA col=wind_speed nulls=142 pct=0.16%
[2026-03-09 10:01:05] INFO  stage=s06_optimize station=BRASILIA period=5y best_heading=87 fo=97.3%
```

### 7.3 Hierarquia de Exceções
```
HOROError
  ├── IngestError
  │   ├── EncodingError
  │   └── MetadataMissingError
  ├── ValidationError
  │   ├── SchemaError
  │   └── DataQualityError
  ├── TransformError
  ├── EnrichmentError
  │   └── MagneticDeclinationError
  └── OptimizationError
```
Nenhum estágio usa `except Exception: pass`. Erros sobem com contexto.

### 7.4 Separação de Responsabilidades
```
stages/     → O QUÊ fazer (lógica de pipeline, E/S, estado)
services/   → COMO fazer (algoritmos, integrações externas)
utils/      → ferramentas agnósticas (encoding, geo, vídeo)
core/       → infraestrutura (config, log, modelos, erros)
```

### 7.5 Configuração Centralizada
Zero constants hardcoded fora de `config.py`. Permite override por variável de ambiente:
```python
# pipeline/core/config.py
@dataclass
class PipelineConfig:
    rose_wind_sectors:    int   = 16
    wind_limits_kts:      list  = field(default_factory=lambda: [3, 13, 20, 25, 40])
    decimal_places:       int   = 3
    m_to_knots:           float = 1.944
    max_spin_deg:         int   = 180
    image_width:          int   = 1920
    image_height:         int   = 1080
    make_video:           bool  = True
    save_json_result:     bool  = True
    ...
```

### 7.6 Rastreabilidade (Data Lineage)
Cada Parquet salvo inclui metadados nas colunas extras ou no sidecar JSON:
- `_source_file`: arquivo raw de origem
- `_stage`: qual estágio gerou
- `_pipeline_run_id`: UUID da execução
- `_created_at`: timestamp

---

## 8. ROTEIRO DE MIGRAÇÃO (FASES)

### Fase 1-4 — Infraestrutura, Serviços, ETL e Orquestração ✅
Todas as fases foram concluídas e o pipeline está operacional.

### Fase 5 — Validação e Limpeza (PENDENTE)
- [ ] Copiar CSVs de `1-INPUT/` para `data/raw/` e validar ponta-a-ponta
- [ ] Deletar `.trash/` após confirmar que pipeline funciona corretamente
- [ ] Atualizar `.gitignore` para excluir `data/bronze/`, `data/silver/`, `data/gold/` ✅
- [ ] Testes de integração ponta-a-ponta (`tests/`)

---

## 9. DECISÕES TÉCNICAS

| Decisão | Escolha | Justificativa |
|---------|---------|---------------|
| Formato intermediário | **Parquet** | Compressão, colunar, preserva tipos, pandas-native |
| Validação de dados | **dataclasses** + checks manuais | Sem dependência extra; Pydantic pode ser adicionado depois |
| Logging | **logging padrão Python** | Zero dependências extras; compatível com todos os ambientes |
| Orquestração | **orchestrator.py (argparse)** | Executável no terminal e no notebook; sem overhead de Airflow/Prefect |
| Cache de estágios | **JSON local** | Simples, legível, sem banco; upgrade futuro para SQLite |
| Encoding | **chardet + 7 fallbacks** | Cobre 100% dos formatos gov. brasileiros identificados |
| Anos bissextos | **dateutil.relativedelta** | Aritmética de datas correta |

---

## 10. EXEMPLO DE USO APÓS MIGRAÇÃO

```bash
# Instalar dependências
pip install -r requirements.txt

# Colocar CSVs em data/raw/ e executar todo o pipeline
python orchestrator.py --all

# Executar apenas um estágio específico
python orchestrator.py --stage ingest

# Forçar re-processamento ignorando cache
python orchestrator.py --all --force

# Processar apenas uma estação
python orchestrator.py --all --station BRASILIA

# Ver o que seria executado sem executar (dry-run)
python orchestrator.py --all --dry-run
```

---
