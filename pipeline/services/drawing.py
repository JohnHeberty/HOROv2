"""
pipeline/services/drawing.py
=============================
Primitivas de desenho com OpenCV para a simulação de orientação de pista.

Migrado de Functions.py (DrawRadialLine, DrawSemiCircle, DrawReferenceRUNWAY,
Agroup, BaricentroArea, CalculateAzimuth, GenerateUniqueGrayColors).
"""

from __future__ import annotations

import math
from typing import List, Sequence, Tuple

import cv2 as cv
import numpy as np
from sklearn.cluster import KMeans


# ---------------------------------------------------------------------------
# Tipos
# ---------------------------------------------------------------------------
Color  = Tuple[int, int, int]
Point  = Tuple[int, int]


# ---------------------------------------------------------------------------
# Primitivas de linha / círculo
# ---------------------------------------------------------------------------
def draw_radial_line(
    image: np.ndarray,
    center: Point,
    length: float,
    angle_deg: float,
    color: Color,
    thickness: int,
) -> None:
    """
    Desenha uma linha radial a partir de *center* no ângulo *angle_deg*
    (0° = Norte, sentido horário).
    """
    rad = np.radians(-angle_deg - 180)
    dx  = length * np.sin(rad)
    dy  = length * np.cos(rad)
    p1  = (int(center[0]), int(center[1]))
    p2  = (int(center[0] + dx), int(center[1] + dy))
    cv.line(image, p1, p2, color, thickness)


def draw_semi_circle(
    image: np.ndarray,
    center: Point,
    length: float,
    angle_min: float,
    angle_max: float,
    color: Color,
    thickness: int,
) -> None:
    """
    Desenha um arco entre *angle_min* e *angle_max* usando pontos individuais.
    """
    for angle in np.arange(angle_min, angle_max + 1):
        rad = np.radians(-angle - 180)
        dx  = length * np.sin(rad)
        dy  = length * np.cos(rad)
        pt  = (int(center[0] + dx), int(center[1] + dy))
        cv.circle(image, pt, thickness, color, -1)


def draw_reference_point(
    image: np.ndarray,
    center: Point,
    length: float,
    angle_deg: float,
    color: Color,
    size: int,
) -> None:
    """
    Desenha um círculo de referência na extremidade de um radial.
    """
    rad = np.radians(-angle_deg - 180)
    dx  = length * np.sin(rad)
    dy  = length * np.cos(rad)
    pt  = (int(center[0] + dx), int(center[1] + dy))
    cv.circle(image, pt, size, color, -1)


# ---------------------------------------------------------------------------
# Agrupamento e geometria
# ---------------------------------------------------------------------------
def agroup_contours(
    contours: list,
    n_clusters: int = 5,
) -> Tuple[np.ndarray, list, List[int]]:
    """
    Agrupa contornos pelo tamanho de área usando K-Means.

    Returns:
        (labels, grouped_contours, sorted_cluster_centers)
    """
    areas = np.array(
        [cv.contourArea(c) for c in contours]
    ).reshape(-1, 1)
    kmeans = KMeans(n_clusters=n_clusters, n_init="auto")
    kmeans.fit(areas)
    labels = kmeans.labels_

    grouped: list = [[] for _ in range(n_clusters)]
    for i, label in enumerate(labels):
        grouped[label].append(contours[i])

    centers = sorted(
        kmeans.cluster_centers_.reshape(-1).tolist(),
        reverse=True,
    )
    return labels, grouped, [int(c) for c in centers]


def barycenter(area_points: Sequence) -> Point:
    """
    Calcula o baricentro (centroide) de um polígono.

    Args:
        area_points: Lista de pontos [(x, y), …] ou array numpy.

    Returns:
        (x, y) do baricentro.
    """
    pts    = np.array(area_points)
    moments = cv.moments(pts)
    if moments["m00"] == 0:
        return (0, 0)
    x = int(moments["m10"] / moments["m00"])
    y = int(moments["m01"] / moments["m00"])
    return x, y


def calculate_azimuth(p1: Point, p2: Point) -> float | None:
    """
    Calcula o azimute (0–360°) de p1 para p2.

    Returns:
        Ângulo em graus ou None se os pontos forem idênticos.
    """
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]

    if dx == 0:
        if dy > 0:  return 90.0
        if dy < 0:  return 270.0
        return None
    az = math.atan(dy / dx) * (180 / math.pi)
    if dx < 0:   az += 180
    elif dy < 0: az += 360
    return az


def generate_gray_colors(n: int) -> List[Color]:
    """
    Gera *n* tons de cinza uniformemente espaçados.
    """
    step = 255 // (n - 1) if n > 1 else 255
    return [(i * step, i * step, i * step) for i in range(1, n + 1)]
