"""
Testa o cálculo de FO geométrico em diferentes ângulos
"""
import pandas as pd
from pipeline.services.runway import calcular_fo

# Lê dados
df = pd.read_parquet("data/silver/generatedBy_react-csv.parquet")

print("=" * 80)
print(f"Total de registros: {len(df)}")
print(f"Direção: Min={df['direction'].min():.0f}° Max={df['direction'].max():.0f}° Média={df['direction'].mean():.1f}°")
print(f"Velocidade: Min={df['speed_kts'].min():.1f} Max={df['speed_kts'].max():.1f} Média={df['speed_kts'].mean():.1f} kt")
print("=" * 80)

# Testa FO para vários ângulos
angles = [0, 30, 45, 60, 90, 120, 135, 150, 180]

print("\nFO GEOMÉTRICO com crosswind_limit = 3kt (TESTE):")
print("-" * 80)
for angle in angles:
    fo, cross, calm = calcular_fo(
        df["direction"], 
        df["speed_kts"], 
        heading_deg=angle,
        crosswind_limit_kts=3.0,  # TESTE: limite baixo para ver variação
        keep_calms=True
    )
    print(f"Ângulo {angle:3d}° → FO: {fo:5.1f}%  Crosswind: {cross:5.1f}%  Calm: {calm:5.1f}%")

print("\nFO GEOMÉTRICO com crosswind_limit = 20kt (PADRÃO RBAC154):")
print("-" * 80)
for angle in angles:
    fo, cross, calm = calcular_fo(
        df["direction"], 
        df["speed_kts"], 
        heading_deg=angle,
        crosswind_limit_kts=20.0,
        keep_calms=True
    )
    print(f"Ângulo {angle:3d}° → FO: {fo:5.1f}%  Crosswind: {cross:5.1f}%  Calm: {calm:5.1f}%")

print("=" * 80)
