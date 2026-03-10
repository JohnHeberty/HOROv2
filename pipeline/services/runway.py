"""
pipeline/services/runway.py
============================
Lógica de domínio de pista: cabeceiras, geometria e Fator de Operação.

Migrado de Functions.py (HeadboardRunway) e RUN_HORO.ipynb.
Usa método geométrico (área dentro do PPD) para calcular FO.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import cv2 as cv
import numpy as np
import pandas as pd

from pipeline.core.logger import get_logger

log = get_logger("services.runway")


def headboard_runway(pista_graus: float) -> str:
    """
    Converte um ângulo em graus para o par de cabeceiras de pista.

    Exemplos:
        87°  → "09-27"
        180° → "18-36"
        0°   → "36-18"   (0° normalizado para 360° → cabeceira 36)

    Args:
        pista_graus: Orientação magnética da pista em graus.

    Returns:
        String no formato "HH-HH" (ex.: "09-27").
    """
    graus      = float(pista_graus) % 360
    headboard  = int(round(graus / 10))
    if headboard == 0:
        headboard = 36
    opposite = headboard + 18 if headboard <= 18 else headboard - 18
    return f"{headboard:02d}-{opposite:02d}"


def calcular_fo(
    direcao: pd.Series,
    magnitude: pd.Series,
    heading_deg: float,
    crosswind_limit_kts: float = 20.0,
    keep_calms: bool = True,
) -> Tuple[float, float, float]:
    """
    Calcula o Fator de Operação (FO) usando método GEOMÉTRICO.
    
    Para cada setor da rosa dos ventos e banda de velocidade:
    1. Cria polígono do setor (fatia de pizza)
    2. Cria polígono do PPD (paralelepípedo de proteção) rotacionado
    3. Calcula área da intersecção
    4. Multiplica pela frequência de ventos naquele setor
    5. Soma tudo
    
    Este é o método ORIGINAL do código .trash/RUN_HORO.ipynb
    
    Args:
        direcao:             Série de direções do vento (graus).
        magnitude:           Série de velocidades do vento (nós).
        heading_deg:         Orientação da pista em graus verdadeiros.
        crosswind_limit_kts: Limite de vento cruzado para largura PPD (padrão RBAC154: 20 kt).
        keep_calms:          Se True, ventos calmos contam como dentro da PPD.

    Returns:
        (fo_pct, crosswind_pct, calm_pct)
    """
    df = pd.DataFrame({"dir": direcao, "spd": magnitude}).dropna()
    if df.empty:
        return 0.0, 0.0, 0.0

    total = len(df)
    calms = df[df["spd"] == 0]
    calm_pct = len(calms) / total * 100

    # Cria rosa dos ventos: 16 setores × 6 bandas de velocidade
    n_setores = 16
    setor_deg = 360 / n_setores
    bandas = [0, 3, 7, 13, 20, 35, 999]  # kt
    
    # Conta frequência em cada célula (setor, banda)
    freq_matrix = np.zeros((n_setores, len(bandas) - 1))
    
    for _, row in df[df["spd"] > 0].iterrows():
        dir_val = row["dir"] % 360
        spd_val = row["spd"]
        
        # Determina setor (0 = N, indo sentido horário)
        setor_idx = int((dir_val + setor_deg/2) % 360 // setor_deg)
        
        # Determina banda de velocidade
        for b_idx in range(len(bandas) - 1):
            if bandas[b_idx] <= spd_val < bandas[b_idx + 1]:
                freq_matrix[setor_idx, b_idx] += 1
                break
    
    # Normaliza frequências (porcentagem)
    if df[df["spd"] > 0].shape[0] > 0:
        freq_matrix = freq_matrix / df[df["spd"] > 0].shape[0] * 100
    
    # ========== MÉTODO GEOMÉTRICO ==========
    # PPD: paralelepípedo de proteção (retângulo largo ao longo da pista)
    # Largura = 2 × crosswind_limit (40 kt total, 20 kt cada lado)
    # Comprimento = raio máximo da rosa (40 kt × 2 = 80 kt)
    
    raio_max = bandas[-2]  # 35 kt (última banda com dados)
    comprimento_ppd = raio_max * 2
    largura_ppd = crosswind_limit_kts * 2
    
    # Centro da imagem (virtual, resolução 1000×1000 para cálculo geométrico)
    img_size = 1000
    cx, cy = img_size // 2, img_size // 2
    escala = img_size / (raio_max * 2.5)  # Escala para caber na imagem
    
    # PPD rotacionado pelo heading_deg
    theta_rad = np.radians(heading_deg)
    rot_mat = np.array([
        [np.cos(theta_rad), -np.sin(theta_rad)],
        [np.sin(theta_rad),  np.cos(theta_rad)]
    ])
    
    # Retângulo PPD (antes de rotacionar): horizontal, centrado em (0,0)
    ppd_half_w = largura_ppd * escala / 2
    ppd_half_h = comprimento_ppd * escala / 2
    ppd_pts_local = np.array([
        [-ppd_half_w, -ppd_half_h],
        [ ppd_half_w, -ppd_half_h],
        [ ppd_half_w,  ppd_half_h],
        [-ppd_half_w,  ppd_half_h]
    ], dtype=np.float32)
    
    # Rotaciona PPD
    ppd_pts_rot = ppd_pts_local @ rot_mat.T
    ppd_pts_rot[:, 0] += cx
    ppd_pts_rot[:, 1] += cy
    ppd_polygon = ppd_pts_rot.astype(np.int32)
    
    # Calcula FO somando áreas de cada setor × banda que ficam dentro do PPD
    fo_acumulado = 0.0
    
    for setor_idx in range(n_setores):
        for banda_idx in range(len(bandas) - 1):
            freq_pct = freq_matrix[setor_idx, banda_idx]
            if freq_pct == 0:
                continue
            
            # Ângulos do setor (sentido horário a partir do Norte = topo)
            angulo_ini = setor_idx * setor_deg - setor_deg/2
            angulo_fim = angulo_ini + setor_deg
            
            # Raios da banda
            r_inner = bandas[banda_idx] * escala
            r_outer = bandas[banda_idx + 1] * escala
            
            # Cria polígono do setor (fatia de pizza anular)
            setor_pts = []
            n_pontos_arco = 20
            
            # Arco externo
            for i in range(n_pontos_arco + 1):
                ang = np.radians(angulo_ini + i * setor_deg / n_pontos_arco - 90)  # -90 para N=topo
                x = cx + r_outer * np.cos(ang)
                y = cy + r_outer * np.sin(ang)
                setor_pts.append([x, y])
            
            # Arco interno (reverso)
            for i in range(n_pontos_arco + 1):
                ang = np.radians(angulo_fim - i * setor_deg / n_pontos_arco - 90)
                x = cx + r_inner * np.cos(ang)
                y = cy + r_inner * np.sin(ang)
                setor_pts.append([x, y])
            
            setor_polygon = np.array(setor_pts, dtype=np.int32)
            
            # Calcula área total do setor
            area_setor = cv.contourArea(setor_polygon)
            if area_setor == 0:
                continue
            
            # Cria máscara com intersecção setor ∩ PPD
            mask = np.zeros((img_size, img_size), dtype=np.uint8)
            cv.fillPoly(mask, [setor_polygon], 255)
            
            mask_ppd = np.zeros((img_size, img_size), dtype=np.uint8)
            cv.fillPoly(mask_ppd, [ppd_polygon], 255)
            
            mask_inter = cv.bitwise_and(mask, mask_ppd)
            
            # Contorno da intersecção
            contornos, _ = cv.findContours(mask_inter, cv.RETR_EXTERNAL, cv.CHAIN_APPROX_SIMPLE)
            
            if len(contornos) > 0:
                area_dentro = cv.contourArea(contornos[0])
                fracao_dentro = area_dentro / area_setor
            else:
                fracao_dentro = 0.0
            
            # Adiciona ao FO: fração que está dentro × frequência desse setor×banda
            fo_acumulado += fracao_dentro * freq_pct
    
    # Adiciona calmos ao FO (se keep_calms=True)
    if keep_calms:
        fo_acumulado += calm_pct
    
    fo_pct = min(100.0, fo_acumulado)
    crosswind_pct = 100.0 - fo_pct
    
    return round(fo_pct, 3), round(crosswind_pct, 3), round(calm_pct, 3)


def otimizar_orientacao(
    direcao: pd.Series,
    magnitude: pd.Series,
    crosswind_limit_kts: float = 20.0,
    keep_calms: bool = True,
    step_deg: int = 1,
) -> Dict[float, float]:
    """
    Varre todas as orientações de pista de 0 a 179° em passos de *step_deg*
    e calcula o FO para cada uma.

    Returns:
        Dict { heading_deg: fo_pct } para todos os ângulos testados.
    """
    results: Dict[float, float] = {}
    for heading in range(0, 180, step_deg):
        fo, _, _ = calcular_fo(
            direcao, magnitude,
            heading_deg=float(heading),
            crosswind_limit_kts=crosswind_limit_kts,
            keep_calms=keep_calms,
        )
        results[float(heading)] = fo

    log.debug(
        "Otimização concluída",
        melhor_rumo=max(results, key=results.get),  # type: ignore[arg-type]
        fo_max=f"{max(results.values()):.2f}%",
    )
    return results
