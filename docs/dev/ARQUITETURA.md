# HOROv1 — Arquitetura do Sistema

> Documentação técnica para desenvolvedores. Descreve a estrutura do código, fluxo dos dados e responsabilidades de cada módulo.

---

## 1. Visão Geral

O HOROv1 é um pipeline **ETL Medallion** que:
1. Ingere séries históricas de vento de aeródromos (CSV SITRAER/REDEMET)
2. Limpa, valida e transforma os dados em camadas Bronze → Silver → Gold
3. Calcula a melhor orientação de pista pelo método ICAO (Fator de Operação)
4. Gera saídas visuais: vídeo MP4, GIF animado e rosa dos ventos PNG

```
data/raw/          ← CSVs brutos colocados pelo usuário
data/bronze/       ← Dados ingeridos (rejeitados separados)
data/silver/       ← Dados limpos, horários padronizados, speed em nós (Parquet)
data/gold/         ← Resultados de análise + frames + FinalResult.json
data/gold/exports/ ← Saídas finais: MP4, GIF, PNG por estação
data/.cache/       ← Cache de browser/screenshots NOAA
```

---

## 2. Estrutura de Arquivos

```
HOROv1/
├── orchestrator.py            # Ponto de entrada CLI (--all, --stage, --force)
├── config_runway.json         # Configuração editável pelo usuário
├── run_pipeline.ipynb         # Notebook interativo (editor do JSON + runner)
│
├── pipeline/
│   ├── core/
│   │   ├── config.py          # Dataclasses de configuração (PipelineConfig)
│   │   ├── logger.py          # Logger estruturado (structlog)
│   │   ├── models.py          # Dataclasses de contexto (PipelineContext, etc.)
│   │   └── exceptions.py      # Exceções específicas do domínio
│   │
│   ├── stages/
│   │   ├── s00_merge_raw.py   # Merge de múltiplos CSVs (pré-ingest)
│   │   ├── s01_ingest.py      # Raw → Bronze (parse, detecção de encoding)
│   │   ├── s02_validate.py    # Valida schema e regras de negócio
│   │   ├── s03_transform.py   # Bronze → Silver (m/s→kt, dedup, sort)
│   │   ├── s04_analyze.py     # Tabelas de vento por setor × banda
│   │   ├── s05_enrich.py      # Declinação magnética (consulta NOAA/EMAG2)
│   │   ├── s06_optimize.py    # FO varredura + renderização de frames
│   │   └── s07_export.py      # MP4 + GIF + PNG windrose + JSON
│   │
│   ├── services/
│   │   ├── runway.py          # calcular_fo(), otimizar_orientacao(), headboard_runway()
│   │   ├── wind.py            # calcular_tabela_ventos(), angulos_rosa(), setores
│   │   └── drawing.py         # draw_reference_point() e helpers OpenCV
│   │
│   └── utils/
│       ├── video.py           # create_video(), create_gif() via ffmpeg palette
│       ├── windrose_mpl.py    # WindRosePlotter (Matplotlib → PNG)
│       └── geo.py             # latlon_to_grau_minuto()
│
└── docs/
    ├── dev/                   # Documentação técnica (você está aqui)
    └── uso/                   # Guia de uso passo a passo
```

---

## 3. Fluxo de Dados — Dia a Dia do Pipeline

```
CSVs em data/raw/
        │
        ▼  s00_merge_raw   (opcional — mescla múltiplos CSVs da mesma estação)
        │
        ▼  s01_ingest      Raw → Bronze
        │  - Detecta encoding (chardet)
        │  - Parse das colunas DATA/HORA + DIRECAO + VELOCIDADE
        │  - Separa registros rejeitados
        │
        ▼  s02_validate    Valida regras de negócio
        │  - direction ∈ [0, 360]
        │  - speed ≥ 0
        │  - Mínimo de registros por estação
        │
        ▼  s03_transform   Bronze → Silver
        │  - Remove duplicatas de timestamp
        │  - Converte m/s → nós  (÷ 0.514444)
        │  - Ordena cronologicamente
        │  - Salva Parquet em data/silver/
        │
        ▼  s04_analyze     Silver → Tabelas de Vento
        │  - Para cada janela temporal (5/10/15/20 anos disponíveis)
        │  - Calcula wind_table: setor × banda de velocidade em %
        │  - Agrupamento em 16 setores de 22.5° cada
        │
        ▼  s05_enrich      Declinação Magnética
        │  - Consulta NOAA EMAG2 via Selenium (Chrome headless)
        │  - Ou usa valor fixo de config_runway.json
        │  - Aplica à direção do vento: direction_mag = direction_true + declination
        │
        ▼  s06_optimize    Melhor Orientação de Pista
        │  - Varre 0–179° (1° por 1°, 180 candidatos)
        │  - Para cada heading: calcular_fo() retorna FO%
        │  - Seleciona heading com maior FO
        │  - Renderiza 210 frames (video) + 390 frames (GIF)
        │
        ▼  s07_export      Gold → Saídas Finais
           - MP4  via OpenCV + ffmpeg
           - GIF  via ffmpeg palettegen/paletteuse
           - PNG  via Matplotlib windrose
           - JSON FinalResult
```

---

## 4. Modelo de Contexto (`PipelineContext`)

O estado do pipeline trafega em um único objeto `PipelineContext` imutável entre estágios:

```python
@dataclass
class PipelineContext:
    run_id: str                              # UUID único da execução
    started_at: datetime
    input_files: List[str]                   # Caminhos dos CSVs de entrada

    bronze:      Dict[str, BronzeRecord]     # station → registro Bronze
    silver:      Dict[str, SilverRecord]     # station → série limpa (DataFrame)
    wind_tables: Dict[str, Dict[int, WindTable]]  # station × years → tabela
    results:     Dict[str, Dict[int, RunwayOptimizationResult]]  # resultados FO
    stages_executed: List[str]
```

Cada estágio recebe `(context, config)`, muda o contexto e retorna o contexto atualizado.

---

## 5. Configuração (`PipelineConfig`)

Toda configuração vive em `pipeline/core/config.py` via dataclasses:

| Dataclass | Responsabilidade |
|-----------|-----------------|
| `DataConfig` | Caminhos de dados, decimais, m_to_knots |
| `WindRoseConfig` | Setores, bandas de velocidade, limites cross-wind |
| `RenderConfig` | Resolução, fps, cores, multiplicador GIF, spin_deg |
| `OutputConfig` | Caminhos de saída do pipeline |
| `BrowserConfig` | URL NOAA, timeout Chrome headless |

Os campos de `RenderConfig` e `WindRoseConfig` são sobrescritos ao carregar `config_runway.json` via `config.load_runway_config()`.

---

## 6. Camada de Renderização (Stage 6)

### 6.1 Imagem Base

A `_build_base_image()` executa **uma única vez** por estação × janela. Gera:
- Fundo cinza escuro `(40, 40, 40)` em 1920×1080
- 16 pétalas da rosa dos ventos (setores × bandas), preenchidas com cor da banda modulada pela frequência relativa
- Círculo de vento cruzado (crosswind envelope) em vermelho
- Imagem NOAA windrose no canto inferior direito (432px)
- Painel de legenda de cores no canto superior esquerdo (PNG via Matplotlib)

### 6.2 Loop de Frames

A `_render_frame()` é chamada para cada ângulo de rotação:

```
Para video:   0° → 179°  (180 frames) + 30 frames finais parados = 210 total
Para GIF:     0° → 359°  (360 frames) + 30 frames parados = 390 total
```

Cada frame adiciona à base:
1. Retângulo verde (melhor pista encontrada até o frame atual)
2. Rótulos das cabeceiras no retângulo verde
3. Retângulo branco (pista no heading atual)
4. Painéis de texto: BEST DIRECTION + DIRECTION NOW + legenda de velocidade

---

## 7. GIF via ffmpeg — Detalhe Técnico

A geração do GIF usa um filtergraph em 2 passos dentro do mesmo comando:

```
setpts=PTS/{speed_multiplier}    → acelera timestamps (não duplica frames)
fps={src_fps}                    → reamostrar ao fps da fonte
scale={gif_width}:-2:flags=lanczos
split[s0][s1]
[s0]palettegen=stats_mode=diff[p]    → paleta ótima por diferença de cena
[s1][p]paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle
```

**Por que `setpts` em vez de `fps` elevado?**  
Usar `fps={fps × speed_multiplier}` duplica cada frame N vezes → 3900 frames → crash de memória.  
`setpts=PTS/N` comprime os timestamps → ffmpeg amostra naturalmente em `fps` sem duplicação.

---

## 8. Declinação Magnética

O Stage 5 (`s05_enrich`) consulta o **NOAA World Magnetic Model** via:
1. Selenium (Chrome headless) → navega para `ngdc.noaa.gov/geomag/calculators/magcalc.shtml`
2. Preenche latitude, longitude e data atual
3. Extrai o valor de declinação do popup de resultados

A declinação é aplicada aos dados Silver:
```python
direction_mag = (direction_true - declination) % 360
```

Se `declinacao_magnetica.valor` estiver definido no JSON, a consulta é ignorada.

---

## 9. Janelas Temporais Inteligentes

O Stage 4 calcula automaticamente quais janelas processar:

```python
windows = [min(5, total_years)]   # sempre inclui o menor período
for window in [10, 15, 20]:
    if total_years >= window:
        windows.append(window)
```

Isso evita gerar análises de "20 anos" com 6 anos de dados reais.

---

## 10. Paleta de Cores — Bandas de Velocidade

| Índice | Banda (kt) | Nome | Cor padrão (RGB) |
|--------|------------|------|-----------------|
| 0 | 0–3 | Calmarias | 180, 180, 180 — cinza claro |
| 1 | 3–13 | Fraco | 30, 100, 255 — azul |
| 2 | 13–20 | Moderado | 40, 255, 40 — verde |
| 3 | 20–25 | Forte | 255, 255, 0 — amarelo |
| 4 | 25–40 | Muito Forte | 255, 150, 0 — laranja |
| 5 | 40+ | Severo | 255, 30, 0 — vermelho |

As cores podem ser customizadas separadamente para a rosa PNG (`rosa_dos_ventos.cores_rgb`) e para o vídeo/GIF (`video.cores_rgb`) no `config_runway.json`.

---

## 11. Dependências Principais

| Biblioteca | Uso |
|-----------|-----|
| `pandas` | Manipulação de DataFrames, Parquet |
| `numpy` | Cálculo vetorizado de crosswind e FO |
| `opencv-python` | Renderização de frames, fontes, formas |
| `matplotlib` / `windrose` | Rosa dos ventos PNG |
| `ffmpeg` (binário externo) | Codificação MP4 e GIF palettegen |
| `selenium` + `webdriver-manager` | Consulta NOAA via Chrome headless |
| `structlog` | Log estruturado com campos chave=valor |
| `chardet` | Detecção automática de encoding dos CSVs |
| `python-dateutil` | Aritmética de anos com relativedelta |

---

## 12. Pontos de Extensão

- **Novo formato de entrada**: Adicionar parser em `s01_ingest.py`
- **Nova fonte de declinação**: Substituir `s05_enrich.py` por uma API REST
- **Novos formatos de saída**: Adicionar exportador em `s07_export.py`
- **Novo algoritmo de FO**: Substituir `calcular_fo()` em `pipeline/services/runway.py`
- **Novos parâmetros configuráveis**: Adicionar campo em `RenderConfig` e ler do JSON em `load_runway_config()`
