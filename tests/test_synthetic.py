"""
Testa FO geométrico com dados sintéticos (ventos fortes)
"""
import pandas as pd
import numpy as np
from pipeline.services.runway import calcular_fo

# Cria dados sintéticos: ventos de várias direções com velocidades altas
np.random.seed(42)
n = 500

# Direções variadas (0-360°)
direcoes = np.random.uniform(0, 360, n)

# Velocidades: mix de calmos, fracos, moderados e fortes
velocidades = np.concatenate([
    np.zeros(50),  # 50 calmos
    np.random.uniform(5, 15, 200),  # 200 ventos moderados
    np.random.uniform(15, 25, 150),  # 150 ventos fortes
    np.random.uniform(25, 35, 100),  # 100 ventos muito fortes
])
np.random.shuffle(velocidades)

df_synth = pd.DataFrame({
    "direction": direcoes,
    "speed_kts": velocidades
})

print("=" * 80)
print("DADOS SINTÉTICOS (ventos fortes)")
print(f"Total: {len(df_synth)} registros")
print(f"Direção: Min={df_synth['direction'].min():.0f}° Max={df_synth['direction'].max():.0f}°")
print(f"Velocidade: Min={df_synth['speed_kts'].min():.1f} Max={df_synth['speed_kts'].max():.1f} Média={df_synth['speed_kts'].mean():.1f} kt")
print("=" * 80)

# Testa FO para vários ângulos
angles = [0, 30, 45, 60, 90, 120, 135, 150, 180]

print("\nFO GEOMÉTRICO (dados sintéticos, crosswind_limit = 20kt):")
print("-" * 80)
for angle in angles:
    fo, cross, calm = calcular_fo(
        df_synth["direction"], 
        df_synth["speed_kts"], 
        heading_deg=angle,
        crosswind_limit_kts=20.0,
        keep_calms=True
    )
    print(f"Ângulo {angle:3d}° → FO: {fo:5.1f}%  Crosswind: {cross:5.1f}%  Calm: {calm:5.1f}%")

print("\n" + "=" * 80)
print("Se os valores de FO variarem entre ângulos, o método geométrico funciona!")
print("=" * 80)
