# HOROv1 — Guia de Uso

> Passo a passo para usuários que querem executar o pipeline sem precisar entender o código.

---

## 1. Pré-requisitos

### 1.1 Software obrigatório

| Software | Como instalar |
|----------|--------------|
| **Python 3.10+** | [python.org/downloads](https://www.python.org/downloads/) |
| **ffmpeg** | Windows: baixar de [ffmpeg.org](https://ffmpeg.org/download.html), extrair e adicionar `bin/` ao PATH |
| **Google Chrome** | [google.com/chrome](https://www.google.com/chrome/) — necessário para consultar a declinação magnética automaticamente |

### 1.2 Instalação das dependências Python

Abra um terminal na pasta do projeto e execute:

```bash
pip install -r requirements.txt
```

Ou, dentro do notebook `run_pipeline.ipynb`, execute a célula de instalação (célula 1).

---

## 2. Preparar os Dados de Entrada

### 2.1 Formato dos arquivos CSV

Os CSVs devem estar em `data/raw/`. O sistema aceita os formatos exportados pelo **SITRAER** (DECEA) e **REDEMET/METAR**.

Colunas esperadas (o sistema detecta automaticamente):

| Coluna | Conteúdo |
|--------|----------|
| `DATA` ou `DATE` | Data e hora da observação |
| `HORA` | Hora (se separada da data) |
| `DIRECAO` | Direção do vento (graus, 0–360) |
| `VELOCIDADE` | Velocidade do vento (m/s) |

O sistema detecta automaticamente o encoding (UTF-8, Latin-1, CP1252, etc.).

### 2.2 Como colocar os arquivos

1. Copie os arquivos `.csv` para a pasta `data/raw/`
2. Múltiplos arquivos da mesma estação serão mesclados automaticamente pelo Stage 0

---

## 3. Configurar o Projeto

Abra o arquivo **`config_runway.json`** na raiz do projeto (ou use o notebook `run_pipeline.ipynb` para configurar visualmente).

### 3.1 Comprimento da pista

```json
"pista": {
    "runway_length_m": 1199
}
```

- **≥ 1500 m** → limite de vento cruzado de **20 kt**
- **1200–1500 m** → **13 kt**
- **< 1200 m** → **10 kt**

### 3.2 Bandas de velocidade do vento

```json
"rosa_dos_ventos": {
    "wind_speed_bands_kts": [3, 13, 20, 25, 40]
}
```

Define os limiares das faixas coloridas da rosa dos ventos. Esses 5 números criam **6 bandas**:
`[0-3]` `[3-13]` `[13-20]` `[20-25]` `[25-40]` `[40+]`

### 3.3 Cores da rosa dos ventos

```json
"rosa_dos_ventos": {
    "cores_rgb": [
        [180, 180, 180],   ← banda 0: calmarias (cinza)
        [ 30, 100, 255],   ← banda 1: fraco (azul)
        [ 40, 255,  40],   ← banda 2: moderado (verde)
        [255, 255,   0],   ← banda 3: forte (amarelo)
        [255, 150,   0],   ← banda 4: muito forte (laranja)
        [255,  30,   0]    ← banda 5: severo (vermelho)
    ]
}
```

As cores do vídeo são configuradas separadamente em `video.cores_rgb`.

### 3.4 Velocidade e duração do vídeo/GIF

```json
"video": {
    "video_spin_deg": 180,      ← o MP4 gira 180° (uma volta da pista)
    "gif_spin_deg":   360,      ← o GIF gira 360° (volta completa)
    "fps_video":       14,      ← 210 frames ÷ 14 fps = 15 segundos
    "gif_speed_multiplier": 2   ← GIF 2× mais rápido que o vídeo
}
```

**Calculando a duração desejada:**  
- Frames do vídeo = `video_spin_deg` + 30 (hold final) = 210  
- Duração = `(video_spin_deg + 30) / fps_video`  
- Exemplo: 15 s → `fps_video = 210 / 15 = 14`

### 3.5 Localização (opcional)

```json
"localizacao": {
    "latitude":  -23.4283,
    "longitude": -46.4678
}
```

Se não informado, o sistema tenta extrair as coordenadas do próprio CSV.

### 3.6 Declinação magnética (opcional)

```json
"declinacao_magnetica": {
    "valor": null
}
```

- `null` → consulta automática ao NOAA (requer Chrome)
- Número (ex.: `-21.95`) → usa o valor fixo informado, sem consulta web

---

## 4. Executar o Pipeline

### Opção A — Notebook interativo (recomendado)

1. Abra `run_pipeline.ipynb` no Jupyter ou VS Code
2. Na **Seção 2 — Configuração**, ajuste os parâmetros com os controles visuais
3. Clique em **"💾 Salvar config_runway.json"** para gravar
4. Execute a **Seção 5 — Rodar Pipeline Completo** (célula única, executa todos os estágios)
5. Os resultados aparecem automaticamente na **Seção 6 — Resultados**

### Opção B — Terminal (linha de comando)

```bash
# Executa todo o pipeline (limpa cache automaticamente antes)
python orchestrator.py --all

# Executa um estágio específico
python orchestrator.py --stage optimize

# Força re-execução mesmo sem mudanças
python orchestrator.py --all --force

# Filtra por estação específica
python orchestrator.py --all --station "SBSP"
```

---

## 5. Onde Ficam os Resultados

```
data/gold/exports/
└── {nome_da_estação}/
    ├── RunwayOrientation-5.mp4    ← Vídeo MP4 (15 segundos)
    ├── RunwayOrientation-5.gif    ← GIF animado 360° (1280px)
    └── Windrose-5y.png            ← Rosa dos ventos PNG

data/gold/
└── FinalResult.json               ← Resultados numéricos de todas as estações
```

### Exemplo de FinalResult.json

```json
{
  "tabela (1)": {
    "5": {
      "runway": "08-26",
      "fo_pct": 99.3,
      "crosswind_pct": 0.7,
      "best_heading_deg": 80.0,
      "total_observations": 38925,
      "years": 5
    }
  }
}
```

---

## 6. Interpretando os Resultados

### 6.1 Painel verde — BEST DIRECTION

Exibe a **melhor orientação de pista** encontrada na análise:
- **FO** — Fator de Operação: percentagem do tempo que a pista está operável
- **RUMO** — Heading magnético em graus (ex.: 080)
- **ORIENTATION** — Par de cabeceiras (ex.: 08-26)
- **CROSS WIND** — Percentagem do tempo com vento cruzado acima do limite

### 6.2 Painel branco — DIRECTION NOW

Exibe a pista **na orientação atual do frame** durante a animação.

### 6.3 Rosa dos ventos (canto inferior direito)

A rosa do NOAA mostra a **distribuição histórica real** dos ventos no aeródromo, produzida com todos os dados disponíveis.

---

## 7. Problemas Comuns

| Problema | Causa provável | Solução |
|----------|---------------|---------|
| "ffmpeg não encontrado" | ffmpeg não está no PATH | Adicionar o diretório `bin/` do ffmpeg ao PATH do sistema |
| GIF não gerado | Menos de 360 frames no gif_frames/ | Verificar se o pipeline completou o Stage 6 sem erros |
| "Chrome não encontrado" | Chrome não instalado ou webdriver falhou | Instalar Chrome ou definir `declinacao_magnetica.valor` manualmente |
| CSV rejeitado | Colunas não reconhecidas | Verificar formato e codificação do CSV |
| FO muito baixo (< 90%) | Crosswind limit inadequado ou pista subótima | Revisar `runway_length_m` no config |

---

## 8. Estrutura de Pastas de Entrada

```
data/
└── raw/
    ├── metar_SBSP_2019_2024.csv        ← um único arquivo
    ├── sitraer_aeroporto_2020.csv      ← pode ter múltiplos
    └── sitraer_aeroporto_2021.csv      ← serão mesclados automaticamente
```

---

## 9. Dicas de Performance

- **Menos de 3 anos de dados**: o pipeline funcionará, mas os resultados terão baixa representatividade estatística
- **Chrome headless muito lento**: defina a declinação manualmente em `config_runway.json` para evitar a consulta web
- **GIF muito pesado**: reduza `gif_spin_deg` para 270 ou diminua `gif_width` em `pipeline/core/config.py`
- **Vídeo lento demais**: aumente `fps_video` no `config_runway.json` (ex.: 20 para ~10s)
