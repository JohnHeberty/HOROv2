# UPDATE — Metodologia de Cálculo: FO e Vento Cruzado

## Visão Geral

Este documento explica como o **Fator de Operação (FO)** e o **Vento Cruzado (Cross Wind)**
são calculados na implementação atual do HOROv1, e como esse método difere da
abordagem original usada no notebook `.trash/RUN_HORO.ipynb`.

---

## Método Antigo (`.trash/RUN_HORO.ipynb` + `Functions.py`)

### Abordagem: PPD Tabular + Setores da Rosa dos Ventos

O método original operava sobre a **tabela de frequência da rosa dos ventos**,
não diretamente nos dados brutos de cada observação.

**Passo a passo:**

1. Os dados de vento eram divididos em **16 setores angulares** (ex: N, NNE, NE…)
   e **bandas de velocidade** (ex: 0–3 kt, 3–13 kt, 13–20 kt…), gerando uma
   tabela de porcentagens `df_pct_ventos[setor × banda]`.

2. Definia-se um **PPD (Paralelogram Polar Diagram)** — um conjunto de colunas
   da tabela correspondentes às bandas de velocidade **dentro** do limite de
   vento cruzado (`LIMITES_IN_PPD`) e **fora** (`LIMITES_OUT_PPD`).

3. Para cada orientação de pista candidata (combinação de dois setores opostos
   `d1–d2`), somavam-se as frequências dos setores e bandas **dentro do PPD**:

   ```python
   FO_estimado = (df_pct_ventos[Columns_Dentro_PPD].loc[d1]
                + df_pct_ventos[Columns_Dentro_PPD].loc[d2]).sum()
   ```

4. A orientação com **maior soma** era eleita a melhor pista (método rápido /
   não exato, limitado às 16 direções cardeais da rosa).

**Limitação principal:**  
O método avaliava apenas as **16 direções discretas** da rosa dos ventos.
Nunca calculava o componente cruzado real de cada observação — estimava o FO
pela frequência de frequências que caiam em bandas tabeladas, o que é uma
aproximação e pode superestimar/subestimar o FO real dependendo de onde os
ventos caíam dentro de cada setor.

> A menção a "contar pixels" refere-se às funções `Agroup`, `BaricentroArea` e
> `CalculateAzimuth` do `Functions.py`, usadas em versões experimentais para
> identificar visualmente as regiões da rosa dos ventos pintadas na imagem
> OpenCV — extraindo contornos coloridos e computando se o baricentro de cada
> mancha de cor estava dentro do retângulo da pista projetado sobre a imagem.
> Esse caminho foi abandonado por depender da resolução e da renderização gráfica,
> tornando o resultado sensível a parâmetros visuais.

---

## Método Atual (`pipeline/services/runway.py` → `calcular_fo`)

### Abordagem: Fórmula Direta ICAO / RBAC 154

Implementado em `calcular_fo()`. Opera **diretamente sobre cada observação
individual** do dataset bruto (direção + velocidade), sem depender de tabelas
pré-agregadas ou de imagens.

### Fórmula do componente cruzado

Para cada observação de vento $(V, \theta_{vento})$ e uma orientação de pista
$\theta_{pista}$:

$$\text{crosswind} = \left| V \cdot \sin(\theta_{vento} - \theta_{pista}) \right|$$

A observação está **dentro do envelope operacional** (contribui ao FO) se:

$$\text{crosswind} \leq \text{limite\_cruzado (kt)}$$

### Regras especiais

| Condição | Tratamento |
|---|---|
| Vento calmo (`speed == 0`) | Sempre conta como dentro do envelope (ICAO/RBAC154 §3.3) |
| Vento com direção inválida / nulo | Descartado (`dropna`) |

### Cálculo final

$$FO = \frac{N_{dentro} + N_{calmos}}{N_{total}} \times 100\%$$

$$\text{Cross Wind} = 100\% - FO$$

Onde:
- $N_{dentro}$ = observações com `crosswind ≤ limite`
- $N_{calmos}$ = observações com velocidade = 0
- $N_{total}$ = total de observações válidas

### Otimização de orientação

A função `otimizar_orientacao()` varre **todas as orientações de 0° a 179°**
em passos de 1°, calculando o FO para cada ângulo via `calcular_fo()`.  
A pista é **simétrica** (RWY 11 = RWY 29), por isso apenas o semiespaço 0–179°
precisa ser avaliado — o FO de 106° é idêntico ao de 286°.

```python
for heading in range(0, 180, 1):          # 180 orientações discretas
    fo, _, _ = calcular_fo(dir, spd, heading_deg=float(heading))
    results[float(heading)] = fo

best_heading = max(results, key=results.get)
```

---

## Comparação Resumida

| Aspecto | Método Antigo (PPD Tabular) | Método Atual (ICAO Direto) |
|---|---|---|
| Entrada | Tabela de frequências (setor × banda) | Observações individuais (dir, spd) |
| Resolução angular | 16 direções (22,5° por setor) | 1° (180 orientações) |
| Cálculo cruzado | Estimado por bandas de velocidade | `\|V × sin(Δθ)\|` exato por observação |
| Ventos calmos | Incluídos implicitamente no setor | Tratados explicitamente (ICAO) |
| Dependência visual | Sim (pixels / contornos OpenCV) | Não — puramente numérico |
| Conformidade ICAO | Aproximada | Direta (conforme RBAC 154 / ICAO Annex 14) |

---

## Arquivo de referência

- Implementação: [`pipeline/services/runway.py`](pipeline/services/runway.py)
- Consumidor principal: [`pipeline/stages/s06_optimize.py`](pipeline/stages/s06_optimize.py) → `otimizar_orientacao()`
- Configuração do limite cruzado: [`config_runway.json`](config_runway.json) → `pista.crosswind_limit_kts`
- Norma de referência: **RBAC 154 §6.2.3** / **ICAO Annex 14 Vol I §3.1.4**
