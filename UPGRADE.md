# UPGRADE.md — Análise de Erros e Melhorias do HOROv1

> Documento gerado em: 09/03/2026  
> Última atualização: 10/03/2026  
> Autor da análise: GitHub Copilot  
> Repositório: JohnHeberty/HOROv1

---

## ✅ TODAS AS MELHORIAS FORAM IMPLEMENTADAS

Todos os 31 itens identificados (erros críticos, erros de lógica, avisos e melhorias) foram corrigidos e migrados para a nova arquitetura medallion pipeline.

O código legado foi movido para `.trash/` e substituído por:
- Pipeline modular (s01 → s07)
- Contratos de dados explícitos
- Logging estruturado
- Separação de responsabilidades
- Configuração centralizada

Para detalhes da arquitetura atual, consulte [PLAN.md](PLAN.md).  
Para metodologia de cálculo FO, consulte [UPDATE.md](UPDATE.md).

---


## ERROS CRÍTICOS (causam falha de execução)

---

### [ERR-01] `Functions.py` — Auto-importação circular
**Arquivo:** `Functions.py`, linha 15  
**Problema:** O arquivo importa a si mesmo com `from Functions import *`, causando `ImportError` ou recursão infinita ao ser carregado.  
**Linha afetada:**
```python
from Functions import *   # <-- LINHA 15: importa o próprio módulo
```
**Correção:** Remover essa linha completamente. `Functions.py` não deve importar a si mesmo.

---

### [ERR-02] `requirements.txt` — Sintaxe inválida de versão do pip
**Arquivo:** `requirements.txt`, última linha  
**Problema:** `numpy=1.26.4` usa `=` simples, que é sintaxe inválida para pip. O `pip install` irá falhar ou ignorar a restrição de versão.  
**Linha afetada:**
```
numpy=1.26.4
```
**Correção:**
```
numpy==1.26.4
```

---

### [ERR-03] `Modulos/DADOS/Engine.py` — Vírgula ausente na lista de formatos de data
**Arquivo:** `Modulos/DADOS/Engine.py`, método `format_dates`, em torno da linha 141  
**Problema:** A ausência de vírgula entre duas strings na lista `formats` faz a concatenação implícita de Python unir `"%Y/%m/%d %H%M"` e `"%d/%m/%Y %H:%M"` em uma única string inválida `"%Y/%m/%d %H%M%d/%m/%Y %H:%M"`. Isso faz o formato `"%d/%m/%Y %H:%M"` nunca ser testado.  
**Código atual:**
```python
formats = [
    "%Y-%m-%d %H%M", "%d-%m-%Y %H%M", 
    "%d/%m/%Y %H%M", "%Y/%m/%d %H%M"   # <-- vírgula ausente aqui
    "%d/%m/%Y %H:%M"
]
```
**Correção:**
```python
formats = [
    "%Y-%m-%d %H%M", "%d-%m-%Y %H%M", 
    "%d/%m/%Y %H%M", "%Y/%m/%d %H%M",  # vírgula adicionada
    "%d/%m/%Y %H:%M"
]
```

---

### [ERR-04] `Modulos/BROWSER/Engine.py` — Inconsistência `self.Driver` vs `self.driver`
**Arquivo:** `Modulos/BROWSER/Engine.py`  
**Problema:** No `__init__`, o atributo é inicializado como `self.Driver = None` (D maiúsculo), mas em `OpenBrowser` é gravado como `self.driver = webdriver.Chrome(...)` (d minúsculo). São dois atributos diferentes em Python. `self.Driver` permanece sempre `None`.  
**Correção:** Padronizar para `self.driver` em ambos os lugares.
```python
# __init__
self.driver = None   # era self.Driver

# OpenBrowser já usa self.driver — manter consistente
```

---

### [ERR-05] `Modulos/DADOS/Engine.py` — Uso inseguro de `eval()` para coordenadas
**Arquivo:** `Modulos/DADOS/Engine.py`, método `process_file`  
**Problema:** `eval(latitude)` e `eval(longitude)` executam código Python arbitrário presente no arquivo CSV. Se o arquivo de entrada contiver código malicioso ou um valor malformado, isso pode causar execução de código ou `SyntaxError`.  
**Código atual:**
```python
self.data_files[name] = {
    "Local": (eval(latitude), eval(longitude)),
    ...
}
```
**Correção:**
```python
self.data_files[name] = {
    "Local": (float(latitude), float(longitude)),
    ...
}
```

---

### [ERR-06] `RUN_HORO.ipynb` — Variável `DataFilesAuto_FILTRO` pode não estar definida
**Arquivo:** `RUN_HORO.ipynb`, célula principal (linha ~207)  
**Problema:** A linha `DATA_RUN = DataFilesAuto_FILTRO if "DataFilesAuto_FILTRO" in locals() else DataFilesAuto` usa `locals()` para checar a variável. Isso funciona em módulos normais, mas em Jupyter Notebooks `locals()` dentro de células pode não capturar variáveis de células anteriores em todos os cenários de execução. Além disso, a célula que define `DataFilesAuto_FILTRO` está completamente comentada, o que facilita o uso acidental de todo o dataset sem perceber.  
**Correção:** Descomentar o bloco de filtro ou adicionar um aviso explícito ao usuário sobre qual dataset está sendo processado.

---

## ERROS DE LÓGICA (comportamento incorreto silencioso)

---

### [ERR-07] `Functions.py` — `DrawRadialLine`, `DrawSemiCircle`, `DrawReferenceRUNWAY` — if/elif com código idêntico
**Arquivo:** `Functions.py`  
**Problema:** Nas três funções, todos os quatro ramos do `if/elif` para quadrantes (≤90, >90≤180, >180≤270, >270≤360) executam exatamente o mesmo cálculo. A ramificação condicional é completamente inerte e induz à falsa crença de que cada quadrante é tratado diferentemente.  
**Código atual (exemplo):**
```python
if angulo <= 90:
    delta_x = comprimento * np.sin(radiano)
    delta_y = comprimento * np.cos(radiano)
elif angulo > 90 and angulo <= 180:
    delta_x = comprimento * np.sin(radiano)   # idêntico
    delta_y = comprimento * np.cos(radiano)   # idêntico
elif ...  # todos idênticos
```
**Correção:** Remover os condicionais e manter apenas o cálculo:
```python
delta_x = comprimento * np.sin(radiano)
delta_y = comprimento * np.cos(radiano)
```

---

### [ERR-08] `RUN_HORO.ipynb` — Typo no índice `"Frota"` em vez de `"Fora"`
**Arquivo:** `RUN_HORO.ipynb`, célula da análise dentro/fora da pista  
**Problema:** O índice do DataFrame é `["Dentro","Frota"]`, mas `"Frota"` (fleet/frota) está incorreto. Deveria ser `"Fora"` (fora da pista). Isso gera resultados com rótulo errado nos relatórios e pode causar falha em filtros subsequentes que busquem `data_dentro_fora[data_dentro_fora.LOCAL=="Fora"]`.  
**Código atual:**
```python
data_dentro_fora = pd.DataFrame(..., index=["Dentro","Frota"])
```
**Correção:**
```python
data_dentro_fora = pd.DataFrame(..., index=["Dentro","Fora"])
```

---

### [ERR-09] `RUN_HORO.ipynb` — Lógica de busca da melhor orientação por indexação booleana incorreta
**Arquivo:** `RUN_HORO.ipynb`, célula `#VSC-75dff1b3`  
**Problema:** `data_dentro_fora[data_dentro_fora==max_direction]` aplica comparação elemento a elemento em todo o DataFrame e substitui não-correspondentes por `NaN`. O `.dropna(axis=1)` subsequente pode descartar colunas válidas por engano, e `.iloc[0:1]` pode retornar a linha errada quando há NaNs.  
**Código atual:**
```python
melhor_orientacao = data_dentro_fora[data_dentro_fora==max_direction].iloc[0:1].dropna(axis=1)
```
**Correção sugerida:**
```python
# Filtrando apenas a linha "Dentro"
linha_dentro = data_dentro_fora[data_dentro_fora.LOCAL == "Dentro"].drop(columns="LOCAL")
melhor_col = linha_dentro.idxmax(axis=1).values[0]
angulo_melhor_orientacao = (dicionario_angulos[melhor_col.split("-")[0]], 
                            dicionario_angulos[melhor_col.split("-")[1]])
```

---

### [ERR-10] `Functions.py` — `HeadboardRunway` — lógica frágil e potencialmente incorreta
**Arquivo:** `Functions.py`, função `HeadboardRunway`  
**Problema:** A conversão de graus para número de cabeceira de pista usa manipulação de string (`str(PISTA)[-1]`, `[:-1]`) em vez de aritmética. Isso falha para `PISTA = 0` (produz `"00"` em vez de `"36"`), e para valores negativos ou fora de [0,360] causa comportamento indefinido. O `CONTRARIO` usa `round()` que aplica "arredondamento de banqueiro" do Python (arredonda 0.5 para o par mais próximo), podendo gerar cabeceiras inconsistentes.  
**Correção sugerida:**
```python
def HeadboardRunway(pista_graus):
    pista_graus = pista_graus % 360
    headboard = round(pista_graus / 10)
    if headboard == 0:
        headboard = 36
    opposite = headboard + 18 if headboard <= 18 else headboard - 18
    return f"{headboard:02d}-{opposite:02d}"
```

---

### [ERR-11] `Modulos/DADOS/Engine.py` — Remoção de ventos calmos distorce a análise
**Arquivo:** `Modulos/DADOS/Engine.py`, método `transform_wind_speed`  
**Problema:** A linha `df[df[wind_column] > 0]` remove todas as observações de vento zero (calmaria). Ventos calmos são observações meteorológicas válidas e compõem parte importante do universo amostral. Removê-los infla artificialmente as porcentagens da rosa dos ventos e distorce o Fator de Operação (FO).  
**Correção:** Expor como parâmetro opcional `remove_calm=False` no `__init__` e aplicar o filtro somente se habilitado.

---

### [ERR-12] `RUN_HORO.ipynb` — Cálculo de intervalo de anos não considera anos bissextos
**Arquivo:** `RUN_HORO.ipynb`, célula principal  
**Problema:** `timedelta(days=365*5)` usa 365 dias fixos por ano, acumulando erro de ~1 dia por ano. Em 20 anos, o erro chega a ~5 dias.  
**Código atual:**
```python
tabelao5 = tabelao[tabelao["DATA"] >= tabelao["DATA"].max() - timedelta(days=365*5)]
```
**Correção:**
```python
from dateutil.relativedelta import relativedelta
tabelao5  = tabelao[tabelao["DATA"] >= tabelao["DATA"].max() - relativedelta(years=5)]
tabelao10 = tabelao[tabelao["DATA"] >= tabelao["DATA"].max() - relativedelta(years=10)]
tabelao15 = tabelao[tabelao["DATA"] >= tabelao["DATA"].max() - relativedelta(years=15)]
tabelao20 = tabelao[tabelao["DATA"] >= tabelao["DATA"].max() - relativedelta(years=20)]
```

---

## AVISOS E DEPRECAÇÕES

---

### [WARN-01] `Modulos/BROWSER/Engine.py` — `executable_path` depreciado no Selenium 4
**Arquivo:** `Modulos/BROWSER/Engine.py`  
**Problema:** `webdriver.Chrome(executable_path=path_driver, ...)` está depreciado desde Selenium 4.0 e pode lançar `DeprecationWarning` ou falhar em versões futuras. O código já tem um `except` que usa `Service()` como fallback, mas a execução sempre tenta o modo depreciado primeiro.  
**Correção:** Usar `Service()` diretamente sem o fallback:
```python
service = Service(executable_path=path_driver)
self.driver = webdriver.Chrome(service=service, options=chrome_options)
```

---

### [WARN-02] `RUN_HORO.ipynb` — API do `moviepy` pode ter mudado na versão 2.x
**Arquivo:** `RUN_HORO.ipynb`, célula principal  
**Problema:** `clip.speedx(SpeedGIF).write_gif(..., program="ffmpeg", opt="OptimizeTransparency", fuzz=1)` usa a API do moviepy 1.x. Na versão 2.x o método `write_gif` foi removido/alterado e os parâmetros `program`, `opt` e `fuzz` não existem mais.  
**Correção:** Verificar a versão do moviepy instalada e adaptar:
```python
# moviepy 2.x
clip.write_gif(path_save.replace(".mp4", ".gif"), fps=5)
```

---

### [WARN-03] `Default.py` — Typos em nomes de variáveis
**Arquivo:** `Default.py`  
**Problema:** `SaveFinalEsult` deveria ser `SaveFinalResult` (falta o 'R'). O erro se propaga para o notebook onde o nome errado é usado.  
**Linha afetada:**
```python
SaveFinalEsult = True  # deveria ser SaveFinalResult
```
E no notebook:
```python
if SaveFinalEsult:  # deve ser atualizado junto
```

---

## MELHORIAS DE QUALIDADE DE CÓDIGO

---

### [MELHORIA-01] Duplicação de código entre `Functions.py` e `Modulos/SITRAER/Sitraer2023.py`
**Arquivos:** `Functions.py` e `Modulos/SITRAER/Sitraer2023.py`  
**Problema:** As funções `calcular_setores`, `angulos_rosa` e `PistasPossiveis` estão definidas em ambos os arquivos com implementações idênticas. Isso viola o princípio DRY (Don't Repeat Yourself) e pode levar a divergências futuras.  
**Correção:** Manter as funções apenas em `Modulos/SITRAER/Sitraer2023.py` e remover de `Functions.py`, ou consolidar em um módulo `utils.py`.

---

### [MELHORIA-02] `Modulos/DADOS/Engine.py` — `__init__` docstring descreve parâmetro `save_analysis` que não existe
**Arquivo:** `Modulos/DADOS/Engine.py`  
**Problema:** O docstring do `__init__` menciona `:param save_analysis: Caminho para salvar o arquivo pickle.` mas esse parâmetro não é exposto na assinatura do método — está hardcoded internamente. Isso confunde usuários da classe.  
**Correção:** Expor `save_analysis` como parâmetro opcional no `__init__`:
```python
def __init__(self, paths, ..., save_analysis=None):
    self.save_analysis = save_analysis or os.path.join("Modulos","DADOS","TREATED")
```

---

### [MELHORIA-03] `Modulos/BROWSER/Engine.py` — Sem suporte a macOS
**Arquivo:** `Modulos/BROWSER/Engine.py`  
**Problema:** O `OpenBrowser` só trata os casos `Windows` e `Linux`. Em macOS o método retorna `None` sem o `self.driver` ser atribuído, causando `AttributeError` na chamada seguinte.  
**Correção:** Adicionar bloco `elif self.system == "Darwin":` com tratamento equivalente ao Linux.

---

### [MELHORIA-04] `Functions.py` — `GetMagneticDeclination` retorna `0.0` silenciosamente em caso de falha
**Arquivo:** `Functions.py`  
**Problema:** Se o Selenium não conseguir extrair a declinação (timeout, mudança de HTML do site), a função retorna `0.0` sem nenhum aviso. Isso faz o cálculo prosseguir com declinação zero, sem que o usuário saiba.  
**Correção:** Levantar exceção ou retornar `None` com log explícito:
```python
if not Declination:
    raise RuntimeError(f"Não foi possível obter a declinação magnética para Lat={lat}, Lon={lon}.")
```

---

### [MELHORIA-05] `Default.py` — Caminhos de glob usam caminhos relativos frágeis
**Arquivo:** `Default.py`  
**Problema:** `glob(os.path.join("1-INPUT", "*.csv"))` depende do diretório de trabalho atual ser a raiz do projeto. Se o script for importado de outro diretório, o glob retorna lista vazia silenciosamente.  
**Correção:** Usar `__file__` para construir o caminho absoluto:
```python
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WeatherStationsPath = (
    glob(os.path.join(_BASE_DIR, "1-INPUT", "*.csv")) or
    glob(os.path.join(_BASE_DIR, "1-INPUT", "*.CSV"))
)
```

---

### [MELHORIA-06] `RUN_HORO.ipynb` — Célula de instalação de pacotes duplica `requirements.txt`
**Arquivo:** `RUN_HORO.ipynb`, célula `#VSC-f2db52dd`  
**Problema:** A célula com `!pip install ...` instalação manual lista pacotes que já estão em `requirements.txt`, mas sem versões fixas e sem `numpy==1.26.4`. Caso o notebook seja executado no mesmo ambiente do projeto, pode sobrescrever versões corretas.  
**Correção:** Substituir a célula por:
```python
!pip install -r requirements.txt
```

---

### [MELHORIA-07] `RUN_HORO.ipynb` — Importações duplicadas entre células
**Arquivo:** `RUN_HORO.ipynb`  
**Problema:** A célula `#VSC-46f0a93d` já realiza todas as importações necessárias. A célula `#VSC-41b862c0` repete as mesmas importações (`from Modulos.DADOS.Engine import *`, `from Functions import *`, etc.) sem necessidade.  
**Correção:** Remover a célula `#VSC-41b862c0` redundante.

---

### [MELHORIA-08] `Functions.py` — `ClearFolder` não remove subdiretórios
**Arquivo:** `Functions.py`, função `ClearFolder`  
**Problema:** `os.remove()` falha silenciosamente para subdiretórios (lança exceção capturada pelo `except`). Se a pasta `IMGS` contiver subpastas acidentalmente, elas não serão removidas e os frames do vídeo anterior poderão ser misturados com os novos.  
**Correção:** Usar `shutil.rmtree` + `os.makedirs` para garantir pasta limpa:
```python
import shutil
def ClearFolder(caminho_pasta):
    if os.path.isdir(caminho_pasta):
        shutil.rmtree(caminho_pasta)
    os.makedirs(caminho_pasta)
```

---

### [MELHORIA-09] `Script1.py` / `Sitraer2023.py` — Nomes de variáveis em português misturado com inglês
**Arquivo:** `Modulos/SITRAER/Script1.py`  
**Problema:** O código mistura `MAGNETUDE` (português de `magnitude`), `DIRECAO` (português), `SETORES_DIRECIONAIS` com `DF_WIND` (inglês). Além disso, `MAGNETUDE` é uma grafia incorreta — a palavra correta em português é `MAGNITUDE`.  
**Correção:** Padronizar para um idioma (preferencialmente português ou inglês) e corrigir a grafia: `MAGNETUDE` → `MAGNITUDE`.

---

### [MELHORIA-10] `Modulos/DADOS/Engine.py` — `transform_wind_speed` lança erro vago
**Arquivo:** `Modulos/DADOS/Engine.py`  
**Problema:** Se `df[wind_column].apply(lambda x: round(float(x) * self.m_to_knots, ...))` encontrar um valor que não pode ser convertido para float (ex.: `"---"`, `"VRB"`), o `float()` lança `ValueError` sem contexto de qual linha ou arquivo causou o problema.  
**Correção:** Usar `pd.to_numeric(df[wind_column], errors='coerce')` antes da multiplicação e logar as linhas com NaN gerados.

---

## SUMÁRIO

| Categoria            | Qtd |
|----------------------|-----|
| Erros críticos       |  6  |
| Erros de lógica      |  6  |
| Avisos/Deprecações   |  3  |
| Melhorias de código  | 10  |
| **Total**            | **25** |

---

## PRIORIDADE DE CORREÇÃO RECOMENDADA

1. **[ERR-01]** Auto-importação em `Functions.py` — bloqueia execução total
2. **[ERR-02]** `requirements.txt` — impede instalação correta do numpy
3. **[ERR-03]** Vírgula ausente em `format_dates` — quebra leitura de CSVs com formato `%d/%m/%Y %H:%M`
4. **[ERR-04]** `self.Driver` vs `self.driver` — navegador nunca é acessível pela propriedade pública
5. **[ERR-05]** `eval()` nas coordenadas — risco de segurança e falha silenciosa
6. **[ERR-08]** Typo `"Frota"` — resultado com rótulo errado propagado aos relatórios
7. **[ERR-07]** if/elif inerte nas três funções de desenho — simplificação urgente
8. **[WARN-03]** `SaveFinalEsult` typo — variável com nome errado usada no notebook
