# HORO v1 — Análise de Orientação de Pistas (RBAC 154)

Pipeline ETL para análise estatística do vento e otimização da orientação de pistas de aeródromos, conforme requisitos da **RBAC 154** (ANAC/Brasil).

---

## O que é

Dado um histórico de observações horárias do INMET, o HORO calcula:

- **Fator de Operação (FO)** — percentual do tempo em que o vento cruzado está abaixo do limite operacional (20 kt).
- **Orientação ótima de pista** — qual ângulo (0–179°) maximiza o FO.
- **Declinação magnética** — corrigida via NOAA para cada estação.
- **Rosa dos ventos** — visualização por janela temporal (5 / 10 / 15 / 20 anos).
- **Vídeo e GIF animados** — varredura de todos os ângulos de orientação de pista.

---

## Arquitetura — Medallion ETL

```
data/raw/        ← CSVs do INMET (entrada manual)
     │
     ▼ Stage 1 — Ingest
data/bronze/     ← Parquet + JSON sidecar (dados brutos normalizados)
     │
     ▼ Stage 2 — Validate (quality gate)
     ▼ Stage 3 — Transform
data/silver/     ← Parquet colunas [DATA, direction_deg, speed_kts]
     │
     ▼ Stage 4 — Analyze (tabelas de vento por janela temporal)
     ▼ Stage 5 — Enrich  (declinação magnética via NOAA)
     ▼ Stage 6 — Optimize (varredura 0–179° e melhor FO + frames)
data/gold/       ← JSON de resultados + frames JPG
     │
     ▼ Stage 7 — Export
data/gold/exports/  ← Vídeo MP4 + GIF animado + FinalResult.json
```

---

## Como obter dados do INMET

O INMET disponibiliza dados históricos horários de todas as estações automáticas do Brasil.

### 1. Encontrar o código da sua estação

1. Acesse o mapa de estações: **https://mapas.inmet.gov.br/**
2. Localize o aeródromo ou cidade mais próxima
3. Clique na estação para ver o código (ex: `A511` = Brasília, `A601` = São Paulo/Congonhas)

### 2. Baixar os dados

1. Acesse a tabela de dados: **https://tempo.inmet.gov.br/TabelaEstacoes/{CODIGO}**
   - Exemplo: `https://tempo.inmet.gov.br/TabelaEstacoes/A511`
2. Selecione o intervalo de datas (recomendado: últimos 5–20 anos)
3. Clique em **"Exportar CSV"** — o site usa CAPTCHA, portanto o download é manual
4. O arquivo será salvo como `generatedBy_react-csv.csv`

### 3. Colocar em data/raw/

```bash
# Recomendado: renomeie para o código da estação
mv generatedBy_react-csv.csv data/raw/A511.csv

# Alternativa: mantenha o nome original (funciona, mas lat/lon ficará como 0,0)
cp generatedBy_react-csv.csv data/raw/
```

> **Nota:** O novo portal do INMET não inclui metadados de localização no CSV.
> As coordenadas são usadas apenas para a consulta NOAA de declinação magnética (Stage 5).
> Para cálculo de FO e orientação de pista, as coordenadas não são necessárias.

---

## Instalação

```bash
# Clone o repositório
git clone <repo-url>
cd HOROv1

# Instale as dependências
pip install -r requirements.txt
```

### Dependências principais

| Pacote | Uso |
|--------|-----|
| `pandas` | Processamento de séries temporais |
| `numpy` | Cálculos vetoriais |
| `windrose` | Geração da rosa dos ventos |
| `matplotlib` | Renderização de gráficos |
| `opencv-python` | Composição de frames e vídeo |
| `moviepy` | Geração de GIF animado |
| `selenium` + `webdriver-manager` | Consulta à NOAA (declinação magnética) |
| `chardet` | Detecção automática de encoding |
| `pyarrow` / `fastparquet` | Armazenamento em Parquet |
| `Unidecode` | Normalização de acentos |

---

## Como executar

### Pipeline completo (recomendado)

```bash
python orchestrator.py --all
```

### Filtrar por estação

```bash
python orchestrator.py --all --station A511
```

### Dry-run (sem executar)

```bash
python orchestrator.py --all --dry-run
```

### Via Jupyter Notebook

Abra `run_pipeline.ipynb` para execução interativa com saídas inline.

---

## Formatos de CSV suportados

O Stage 1 detecta automaticamente o formato do arquivo:

| Campo | Formato antigo BDMEP | Novo portal INMET (2024+) |
|-------|---------------------|--------------------------|
| Separador | `;` | `;` |
| Decimal | `,` | `,` |
| Cabeçalho de metadados | `ESTACAO:`, `LATITUDE:`, `LONGITUDE:` | Ausente |
| Coluna de velocidade | `VENTO. VELO. (M/S)` | `Vel. Vento (m/s)` |
| Coluna de direção | `VENTO. DIRE. (GR)` | `Dir. Vento (m/s)` |
| Encoding | Latin-1 / UTF-8 | UTF-8-SIG |

> **Atenção:** A coluna `Dir. Vento (m/s)` do novo portal tem rótulo incorreto no cabeçalho
> (deveria ser `°`). Os valores são graus (0–360). O pipeline interpreta corretamente.

---

## Saídas

```
data/gold/
├── {station}_results.json          ← Resultado por janela temporal
├── exports/
│   ├── {station}/
│   │   ├── frames/                 ← 180 frames JPG (um por ângulo de orientação)
│   │   ├── RunwayOrientation-5.mp4
│   │   ├── RunwayOrientation-5.gif
│   │   └── ... (10, 15, 20 anos)
│   └── FinalResult.json            ← Consolidado de todas as estações
```

### Exemplo de FinalResult.json

```json
{
  "A511": {
    "metadata": { "latitude": -15.789, "longitude": -47.925, "altitude": "1160.96" },
    "5y":  { "runway": "36-18", "fo_pct": 98.7, "crosswind_pct": 1.3, "calm_pct": 4.2, "heading_deg": 5.0, "declination": -22.5 },
    "10y": { "runway": "36-18", "fo_pct": 97.8, ... },
    "15y": { ... },
    "20y": { ... }
  }
}
```

---

## Estrutura do projeto

```
HOROv1/
├── pipeline/
│   ├── core/
│   │   ├── config.py        ← Configuração centralizada
│   │   ├── logger.py        ← Logger estruturado
│   │   ├── exceptions.py    ← Hierarquia de erros HOROError
│   │   └── models.py        ← Dataclasses: BronzeRecord, SilverRecord, etc.
│   ├── utils/
│   │   ├── encoding.py      ← Detecção de encoding (chardet + 7 fallbacks)
│   │   ├── geo.py           ← Conversões DMS / decimal
│   │   └── video.py         ← Geração de MP4 e GIF
│   ├── services/
│   │   ├── browser.py       ← Chrome headless via webdriver-manager
│   │   ├── drawing.py       ← Primitivos OpenCV
│   │   ├── wind.py          ← Rosa dos ventos e cálculo de setores
│   │   └── runway.py        ← Fator de Operação e otimização de orientação
│   └── stages/
│       ├── s01_ingest.py    ← Raw CSV → Bronze Parquet
│       ├── s02_validate.py  ← Quality gate
│       ├── s03_transform.py ← Bronze → Silver (m/s → kt, normalização)
│       ├── s04_analyze.py   ← Silver → Wind Tables (5/10/15/20 anos)
│       ├── s05_enrich.py    ← Declinação magnética via NOAA
│       ├── s06_optimize.py  ← Varredura 0–179° → best FO + frames
│       └── s07_export.py    ← Vídeo, GIF, FinalResult.json
├── data/
│   ├── raw/                 ← Coloque os CSVs do INMET aqui
│   ├── bronze/              ← Gerado automaticamente
│   ├── silver/              ← Gerado automaticamente
│   └── gold/                ← Resultados finais
├── tests/                   ← Testes unitários (pytest)
├── Standards/               ← RBAC154EMD07.pdf
├── orchestrator.py          ← Ponto de entrada CLI
├── run_pipeline.ipynb       ← Notebook interativo
└── requirements.txt
```

---

## Notas técnicas

### Quantidade mínima de dados
O pipeline aceita qualquer quantidade de dados, mas para análise estatisticamente válida conforme RBAC 154 recomenda-se **mínimo de 5 anos** de observações horárias completas (≈ 43.800 registros). Com menos dados, o FO será calculado sobre uma amostra não representativa — os resultados serão exibidos com um aviso.

### Declinação magnética
O Stage 5 consulta o calculador da NOAA via Selenium (Chrome headless, gerenciado automaticamente). Quando as coordenadas são desconhecidas (lat=0, lon=0), o estágio pula a consulta e usa declinação = 0° com um aviso no log.

### Performance do Stage 6
A varredura percorre 180 ângulos por janela temporal (720 total). Com dados completos de 5 anos (~43.800 linhas), leva ~6 min. Com o arquivo de exemplo de 7 dias (~161 linhas), leva ~4 min (gargalo: renderização dos 720 frames matplotlib).

---

## Referências

- [RBAC 154 EMD 07 — ANAC](https://www.anac.gov.br/assuntos/legislacao/legislacao-1/rbha-e-rbac/rbac/rbac-154)
- [INMET — Mapa de Estações](https://mapas.inmet.gov.br/)
- [INMET — Portal de Tabelas](https://tempo.inmet.gov.br/TabelaEstacoes/)
- [NOAA — Calculadora de Declinação Magnética](https://ngdc.noaa.gov/geomag/calculators/magcalc.shtml)

---

## Autor

**John Heberty de Freitas** — john.7heberty@gmail.com
