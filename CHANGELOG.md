# CHANGELOG — HOROv1

## v1.0.0 — Migração para Pipeline ETL Medallion

### 🏗️ Arquitetura

- **Medallion ETL Pipeline**: Raw → Bronze → Silver → Gold → Export
- **7 estágios modulares**: s01 (ingest) até s07 (export)
- **Orquestrador CLI**: `orchestrator.py --all` substitui notebooks monolíticos
- **Separação de responsabilidades**: `pipeline/core/`, `pipeline/services/`, `pipeline/stages/`

### 📊 Cálculo de FO (Fator de Operação)

- **Método ICAO/RBAC 154 direto**: fórmula trigonométrica $|V \cdot \sin(\theta_{vento} - \theta_{pista})|$
- **Varredura completa**: 180 orientações (0-179° em passos de 1°) vs. 16 setores fixos
- **Eliminado PPD tabular**: cálculo agora sobre observações individuais, não frequências agrupadas
- **Removido processamento visual**: sem dependência de contornos/pixels OpenCV

### 🗂️ Estrutura de Dados

- **data/raw/**: CSVs originais (antes `1-INPUT/`)
- **data/bronze/**: Parquet + JSON sidecar com metadados
- **data/silver/**: Dados limpos e validados
- **data/gold/**: Resultados + frames renderizados
- **data/gold/exports/**: MP4 + GIF + JSON final (antes `2-OUTPUT/`)

### 🔧 Funcionalidades

- **Logging estruturado**: `structlog` com níveis e contexto (substitui `print()`)
- **Validação de qualidade**: rejeição automática de dados inválidos
- **Contratos de dados**: dataclasses com schemas explícitos entre estágios
- **Cache inteligente**: `declinations.json` para declinações magnéticas
- **Encoding robusto**: detecção automática com `chardet` + fallback UTF-8
- **Configuração centralizada**: `config_runway.json` editável pelo usuário

### 📦 Dependências

- **Atualizado Selenium**: 4.x (antes 3.141)
- **Novo pyarrow/fastparquet**: armazenamento Parquet
- **moviepy**: geração de GIF otimizada

### 🗑️ Descontinuado

- `Functions.py`: funções migradas para `pipeline/services/` e `pipeline/utils/`
- `Default.py`: substituído por `pipeline/core/config.py`
- `RUN_HORO.ipynb`: substituído por `orchestrator.py` + `HORO.ipynb` (opcional)
- Pastas `1-INPUT/` e `2-OUTPUT/`: renomeadas para `data/raw/` e `data/gold/exports/`
- Método de contagem de pixels: substituído por cálculo trigonométrico direto

### 📝 Documentação

- **docs/dev/ARQUITETURA.md**: estrutura técnica do sistema
- **docs/dev/CALCULOS.md**: fórmulas e metodologia ICAO
- **docs/UPDATE.md**: comparativo detalhado do método antigo vs. novo
- **docs/uso/GUIA_RAPIDO.md**: tutorial para usuários finais
- **docs/PLAN.md**: plano de migração executado

---

### Uso Básico

```bash
# Colocar CSVs em data/raw/
# Executar pipeline completo
python orchestrator.py --all

# Forçar reprocessamento
python orchestrator.py --all --force
```