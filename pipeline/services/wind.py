"""
pipeline/services/wind.py
==========================
Lógica de domínio: setores da rosa dos ventos, ângulos e tabelas de vento.

Migrado de Modulos/SITRAER/Sitraer2023.py e Modulos/SITRAER/Script1.py.
"""

from __future__ import annotations

from typing import Dict, List, Set, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Estrutura de setores
# ---------------------------------------------------------------------------
def calcular_setores(n: int, sector_names: List[str]) -> Dict[str, Tuple[float, float]]:
    """
    Divide os 360° em *n* setores iguais e retorna o intervalo (início, fim)
    de cada setor no sentido horário, começando pelo Norte.

    Args:
        n:             Número de setores (ex.: 16).
        sector_names:  Lista de nomes dos setores (len == n).

    Returns:
        Dict { nome_setor: (inicio_graus, fim_graus) }
    """
    meio_setor = (360 / n) / 2
    inicio = 360.0
    fim    = 0.0
    setores: Dict[str, Tuple[float, float]] = {}

    for i in range(1, n + 1):
        inicio -= meio_setor * (1 if i == 1 else 2)
        fim     = (fim if i == 1 else inicio) + meio_setor * (1 if i == 1 else 2)
        setores[sector_names[i - 1]] = (inicio, fim)

    return setores


def angulos_rosa(n: int, sector_names: List[str]) -> Dict[str, float]:
    """
    Retorna o ângulo central de cada setor da rosa dos ventos.

    Args:
        n:             Número de setores.
        sector_names:  Nomes dos setores.

    Returns:
        Dict { nome_setor: angulo_central }
    """
    passo  = 360 / n
    inicio = 360.0
    angulos: Dict[str, float] = {}

    for i in range(1, n + 1):
        if i != 1:
            inicio -= passo
        angulos[sector_names[i - 1]] = inicio

    return angulos


def pistas_possiveis(directions: Dict[str, float]) -> Set[Tuple[str, str]]:
    """
    Encontra todos os pares de setores opostos (±180°) — representam
    as orientações possíveis de pista.

    Returns:
        Conjunto de tuplas (setor_a, setor_b) ordenadas lexicograficamente.
    """
    opposite_pairs: Set[Tuple[str, str]] = set()
    items = list(directions.items())

    for direction, angle in items:
        target = (angle + 180) % 360
        for other_dir, other_ang in items:
            if other_ang == target:
                pair = tuple(sorted([direction, other_dir]))
                opposite_pairs.add(pair)  # type: ignore[arg-type]

    return opposite_pairs


def get_column_titles(limits: List[float]) -> List[str]:
    """
    Gera os títulos das colunas da tabela de ventos com base nos limites.

    Ex.: limits=[3, 13, 20, 25, 40] →
         ['[0-3]', '[3-13]', '[13-20]', '[20-25]', '[25-40]', '[40-*]']
    """
    titles: List[str] = []
    for i, limit in enumerate(limits):
        if i == 0:
            titles.append(f"[0-{limit}]")
        else:
            titles.append(f"[{limits[i - 1]}-{limit}]")
    titles.append(f"[{limits[-1]}-*]")
    return titles


# ---------------------------------------------------------------------------
# Tabela de ventos (Script1 migrado)
# ---------------------------------------------------------------------------
def calcular_tabela_ventos(
    direcao: pd.Series,
    magnitude: pd.Series,
    sector_names: List[str],
    limits: List[float],
) -> pd.DataFrame:
    """
    Calcula a tabela percentual de vento por setor e banda de velocidade.

    Equivalente ao antigo Script1() porém integrado ao pipeline.

    Args:
        direcao:       Série de direções do vento (graus 0–360).
        magnitude:     Série de velocidades do vento (nós).
        sector_names:  Nomes dos setores (ex.: lista de 16 setores).
        limits:        Limites de velocidade (ex.: [3, 13, 20, 25, 40]).

    Returns:
        DataFrame (setores × bandas) com valores em % do total.
    """
    n = len(sector_names)
    setores = calcular_setores(n, sector_names)

    df_wind = pd.DataFrame({"DIRECAO": direcao, "MAGNITUDE": magnitude})

    # Filtra por banda de velocidade
    filtro_limites: Dict[int, pd.DataFrame] = {}
    for i, limite in enumerate(limits):
        if i == 0:
            filtro_limites[i] = df_wind[df_wind["MAGNITUDE"] <= limite]
        elif i == len(limits) - 1:
            filtro_limites[i]     = df_wind[
                (df_wind["MAGNITUDE"] > limits[i - 1]) & (df_wind["MAGNITUDE"] <= limite)
            ]
            filtro_limites[i + 1] = df_wind[df_wind["MAGNITUDE"] > limite]
        else:
            filtro_limites[i] = df_wind[
                (df_wind["MAGNITUDE"] > limits[i - 1]) & (df_wind["MAGNITUDE"] <= limite)
            ]

    # Conta ocorrências por setor e banda
    resultados: Dict[str, List[int]] = {}
    for setor, (inicio, fim) in setores.items():
        quantidades: Dict[int, int] = {}
        for chave, grupo in filtro_limites.items():
            if inicio < fim:
                filtro = grupo[
                    (grupo["DIRECAO"] >= inicio) & (grupo["DIRECAO"] <= fim)
                ]
            else:  # setor cruza o meridiano de 360°
                f1 = grupo[(grupo["DIRECAO"] >= inicio) & (grupo["DIRECAO"] <= 360)]
                f2 = grupo[(grupo["DIRECAO"] >= 0)      & (grupo["DIRECAO"] <= fim)]
                filtro = pd.concat([f1, f2])
            quantidades[chave] = len(filtro)
        resultados[setor] = list(quantidades.values())

    titulos = get_column_titles(limits)
    df_qtd  = pd.DataFrame(resultados, index=titulos).T
    total   = df_qtd.sum().sum()
    df_pct  = (df_qtd / total * 100) if total > 0 else df_qtd
    return df_pct
