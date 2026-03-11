"""Script de teste para analisar FO dos dados"""
import pandas as pd
import sys
sys.path.insert(0, '.')
from pipeline.services.runway import calcular_fo

# Ler dados silver
df = pd.read_parquet('data/silver/generatedBy_react-csv.parquet')

print(f"="*70)
print(f"ANÁLISE DOS DADOS DE VENTO")
print(f"="*70)
print(f"\nTotal de registros: {len(df)}")
print(f"\nDireção do vento:")
print(f"  Min: {df['direction'].min():.0f}°")
print(f"  Max: {df['direction'].max():.0f}°")
print(f"  Média: {df['direction'].mean():.0f}°")
print(f"  Desvio: {df['direction'].std():.0f}°")

print(f"\nVelocidade do vento (kt):")
print(f"  Min: {df['speed_kts'].min():.1f}")
print(f"  Max: {df['speed_kts'].max():.1f}")
print(f"  Média: {df['speed_kts'].mean():.1f}")
print(f"  Desvio: {df['speed_kts'].std():.1f}")

# Distribuição de velocidade
print(f"\nDistribuição de velocidade:")
print(f"  Calmos (0 kt): {(df['speed_kts'] == 0).sum()} ({(df['speed_kts'] == 0).sum()/len(df)*100:.1f}%)")
print(f"  0-3 kt: {((df['speed_kts'] > 0) & (df['speed_kts'] <= 3)).sum()}")
print(f"  3-13 kt: {((df['speed_kts'] > 3) & (df['speed_kts'] <= 13)).sum()}")
print(f"  13-20 kt: {((df['speed_kts'] > 13) & (df['speed_kts'] <= 20)).sum()}")
print(f"  >20 kt: {(df['speed_kts'] > 20).sum()}")

print(f"\n{'-'*70}")
print(f"FO PARA DIFERENTES ÂNGULOS (crosswind limit = 20 kt)")
print(f"{'-'*70}")
print(f"{'Ângulo':<10} {'FO %':<10} {'Crosswind %':<15} {'Calm %':<10}")
print(f"{'-'*70}")

for angle in range(0, 180, 15):
    fo, cross, calm = calcular_fo(
        df['direction'], 
        df['speed_kts'], 
        float(angle), 
        20.0,  # crosswind_limit_kts
        True   # keep_calms
    )
    print(f"{angle:3d}°       {fo:5.1f}%     {cross:5.1f}%          {calm:5.1f}%")

print(f"="*70)
