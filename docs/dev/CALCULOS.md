# HOROv1 — Explicação dos Cálculos

> Documentação técnica dos algoritmos matemáticos usados no pipeline. Detalha as equações, hipóteses e referências normativas (ICAO/RBAC154).

---

## 1. Fator de Operação (FO) — Definição ICAO

O **Fator de Operação** (em inglês *Wind Coverage* ou *Usability Factor*) mede a fração do tempo em que uma aeronave pode operar em determinada pista sem exceder o limite de vento cruzado.

> **Definição:** A percentagem de observações de vento de superfície para as quais a componente de vento cruzado não excede o valor máximo aceitável. (ICAO Annex 14, §3.1.4)

O RBAC154 brasileiro estabelece os mesmos limiares da ICAO para aeródromos nacionais.

---

## 2. Componente de Vento Cruzado (Crosswind)

Dado um vento com:
- $\vec{v}$ = velocidade (nós)
- $\theta_w$ = direção de onde o vento sopra (graus verdadeiros)
- $\theta_r$ = orientação da pista (heading magnético após aplicar declinação)

A componente cruzada é:

$$C_w = \left| v \cdot \sin(\theta_w - \theta_r) \right|$$

**Implementação** (`pipeline/services/runway.py → calcular_fo`):

```python
delta_rad = np.radians(non_calms["dir"].values - heading_deg)
crosswind = np.abs(non_calms["spd"].values * np.sin(delta_rad))
inside_mask = crosswind <= crosswind_limit_kts
```

Uma observação está **dentro do envelope operacional** se $C_w \leq C_{lim}$.

---

## 3. Cálculo do FO para um Heading

Para uma orientação de pista $\theta_r$ e um conjunto de $N$ observações de vento:

$$FO(\theta_r) = \frac{N_{dentro} + N_{calmo}}{N_{total}} \times 100\%$$

Onde:
- $N_{dentro}$ = número de observações com $C_w \leq C_{lim}$ (vento não nulo)
- $N_{calmo}$ = número de observações com $v = 0$ (ventos calmos sempre somam ao FO)
- $N_{total}$ = total de observações válidas

**Nota sobre simetria:** A pista tem dois sentidos opostos (ex.: 09 e 27). O FO de $\theta_r = 90°$ é idêntico ao de $\theta_r = 270°$ porque $\sin(\alpha) = -\sin(\alpha + 180°)$ e o módulo |·| cancela o sinal. Logo, a varredura cobre apenas 0–179°.

---

## 4. Otimização — Melhor Orientação

O Stage 6 executa uma varredura exaustiva:

```
para heading em {0°, 1°, 2°, ..., 179°}:
    FO[heading] = calcular_fo(vento, heading)

best_heading = argmax(FO)
```

Complexidade: $O(180 \times N)$ onde $N$ é o número de observações. Para 5 anos de dados horários, $N \approx 43\,800$.

---

## 5. Limite de Vento Cruzado por Comprimento de Pista (RBAC154)

| Comprimento ($L$) | Limite $C_{lim}$ |
|-------------------|-----------------|
| $L \geq 1500\,\text{m}$ | **20 kt** (10,3 m/s) |
| $1200 \leq L < 1500\,\text{m}$ | **13 kt** (6,7 m/s) |
| $L < 1200\,\text{m}$ | **10 kt** (5,1 m/s) |

Referência: RBAC n° 154, §154.305 — *Orientação da pista*.

---

## 6. Declinação Magnética

A declinação magnética $\delta$ é o ângulo entre o Norte Verdadeiro (geográfico) e o Norte Magnético num dado ponto e data.

Os CSVs do SITRAER/REDEMET registram a **direção verdadeira** do vento (referência ao Norte Geográfico). A orientação de pista é especificada em graus magnéticos. Para comparar corretamente:

$$\theta_{mag} = (\theta_{true} - \delta) \mod 360°$$

**Convenção:** $\delta > 0$ = declinação para Leste, $\delta < 0$ = para Oeste.

O valor é obtido do **NOAA World Magnetic Model** para as coordenadas do aeródromo na data atual. Fonte: `ngdc.noaa.gov/geomag/calculators/magcalc.shtml`.

---

## 7. Rosa dos Ventos — Setores e Bandas

### 7.1 Divisão em Setores

Os 360° são divididos em $n = 16$ setores iguais de $22,5°$ cada, centrados nos ângulos:

$$\theta_i = 360° - (i-1) \cdot 22,5°, \quad i = 1, 2, \ldots, 16$$

Nomes dos setores (padrão ICAO/METAR): N, NNE, NE, ENE, E, ESE, SE, SSE, S, SSO, SO, OSO, O, ONO, NO, NNO.

### 7.2 Tabela de Frequências (Wind Table)

Para cada par (setor $s_j$, banda de velocidade $b_k$):

$$f_{jk} = \frac{\text{número de obs com } \theta_w \in s_j \text{ e } v \in b_k}{N_{total}} \times 100\%$$

A tabela tem dimensão $16 \times 6$ (para 5 bandas de limite, há 6 intervalos).

### 7.3 Renderização Visual

Cada pétala é desenhada como um setor circular de ângulo $22,5°$ e raio proporcional à frequência somada das bandas:

$$r_{pétala} = W_{img} \cdot \frac{\text{proporção}} {\max(\text{limites})} \cdot v_{banda}$$

A opacidade/intensidade de cada sub-pétala é modulada por:

$$intensity = 0.4 + 0.6 \cdot \frac{f_{jk}}{f_{max}}$$

---

## 8. Cabeceiras de Pista (Runway Designation)

A cabeceira de uma pista é o arredondamento do heading para a dezena mais próxima em unidades de 10°:

$$H = \text{round}\left(\frac{\theta}{10}\right)$$

- $H = 0$ é normalizado para $H = 36$
- A cabeceira oposta é $H_{oposta} = H + 18$ (se $H \leq 18$) ou $H - 18$ (se $H > 18$)

**Exemplos:**

| Heading (°) | Cabeceira | Par |
|-------------|-----------|-----|
| 87 | 09 | 09-27 |
| 180 | 18 | 18-36 |
| 267 | 27 | 09-27 |
| 350 | 35 | 17-35 |

---

## 9. Conversão de Unidades

### 9.1 m/s → Nós

Os CSVs SITRAER registram velocidade em m/s. A conversão usa o fator exato:

$$v_{kt} = \frac{v_{m/s}}{0,514\,444}$$

Equivalências úteis:
- 1 kt = 0,514 444 m/s = 1,852 km/h
- 10 kt = 5,14 m/s = 18,5 km/h
- 20 kt = 10,3 m/s = 37,0 km/h

### 9.2 Coordenadas Decimais → Graus/Minutos

Exibição no painel visual:

$$\text{graus}\,G = \lfloor |\text{lat}| \rfloor, \quad \text{minutos}\,M = (|\text{lat}| - G) \times 60$$

---

## 10. Janelas Temporais — Critério de Seleção

Para uma estação com $T$ anos de dados, as janelas de análise são:

$$W = \{5\} \cup \{w \in \{10, 15, 20\} \mid T \geq w\}$$

Com o mínimo ajustado: se $T < 5$, a janela base é ajustada para $\min(5, T)$.

**Motivação:** Evitar análises estatisticamente inválidas onde a janela de referência supera os dados disponíveis.

---

## 11. Resumo das Equações Principais

| Equação | Variáveis | Uso |
|---------|-----------|-----|
| $C_w = \lvert v \sin(\theta_w - \theta_r) \rvert$ | $v$=speed, $\theta_w$=dir vento, $\theta_r$=heading pista | Componente cruzada por observação |
| $FO = \frac{N_{dentro} + N_{calmo}}{N_{total}} \times 100$ | contagens | Fator de Operação |
| $\theta_{mag} = (\theta_{true} - \delta) \mod 360$ | $\delta$=declinação | Correção magnética |
| $H = \text{round}(\theta / 10)$ | $\theta$=heading em graus | Designação de cabeceira |
| $v_{kt} = v_{m/s} / 0{,}514444$ | — | Conversão de velocidade |
| $f_{jk} = N_{jk} / N_{total} \times 100$ | — | Frequência em célula da rosa |

---

## 12. Referências Normativas

| Documento | Tema |
|-----------|------|
| ICAO Annex 14, Volume I, §3.1.4 | Definição e cálculo do Fator de Operação |
| ICAO Doc 9157 — Aerodrome Design Manual, Part 1 | Usability Factor e wind rose analysis |
| RBAC n° 154 (ANAC Brasil), §154.305 | Orientação de pistas em aeródromos brasileiros |
| NOAA World Magnetic Model | Declinação magnética global |
| SITRAER / REDEMET | Formato dos dados meteorológicos de superfície |
