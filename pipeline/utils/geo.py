"""
pipeline/utils/geo.py
=====================
Utilitários de coordenadas geográficas.

Extraído de Functions.py (`LatLon_to_GrauMinute`).
"""

from __future__ import annotations

from typing import Tuple


def decimal_to_dms(coordinate: float) -> Tuple[int, int, float]:
    """
    Converte uma coordenada decimal para graus, minutos e segundos.

    Args:
        coordinate: Valor decimal (positivo ou negativo).

    Returns:
        (graus, minutos, segundos)
    """
    graus = int(abs(coordinate))
    minutos_float = (abs(coordinate) - graus) * 60
    minutos = int(minutos_float)
    segundos = round((minutos_float - minutos) * 60, 1)
    return graus, minutos, segundos


def latlon_to_grau_minuto(
    latitude: float,
    longitude: float,
) -> Tuple[str, str, str, str]:
    """
    Converte latitude e longitude decimais para strings grau-minuto com direção.

    Usado na consulta à NOAA para declinação magnética.

    Returns:
        (lat_dms_str, lat_dir, lon_dms_str, lon_dir)
        Ex.: ("22° 54' 33.6''", "S", "43° 10' 15.2''", "W")
    """
    lat_dir = "N" if latitude >= 0 else "S"
    lon_dir = "E" if longitude >= 0 else "W"

    lat_g, lat_m, lat_s = decimal_to_dms(abs(latitude))
    lon_g, lon_m, lon_s = decimal_to_dms(abs(longitude))

    # Usa "deg" em vez de símbolo ° (OpenCV não suporta Unicode)
    lat_str = f"{lat_g}deg {lat_m}' {lat_s}''"
    lon_str = f"{lon_g}deg {lon_m}' {lon_s}''"

    return lat_str, lat_dir, lon_str, lon_dir


def dms_string_to_decimal(dms_str: str, direction: str) -> float:
    """
    Converte uma string "G° M' S''" e direção (N/S/E/W) para decimal.

    Args:
        dms_str:   Ex. "22° 54' 33.6''" ou "22° 54'"
        direction: 'N', 'S', 'E' ou 'W'

    Returns:
        Ângulo decimal (negativo para S ou W).
    """
    cleaned = (
        dms_str
        .replace("°", "")
        .replace("''", "")
        .replace("'", "")
        .strip()
    )
    parts = cleaned.split()
    graus    = int(parts[0])    if len(parts) > 0 else 0
    minutos  = int(parts[1])    if len(parts) > 1 else 0
    segundos = float(parts[2])  if len(parts) > 2 else 0.0

    decimal = round(graus + minutos / 60 + segundos / 3600, 4)
    if direction.upper() in ("S", "W"):
        decimal = -decimal
    return decimal
