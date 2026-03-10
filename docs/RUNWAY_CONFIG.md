# Configuração de Comprimento de Pista (RBAC154)

## Como Usar

Edite o arquivo `config_runway.json` na raiz do projeto para definir o comprimento da pista de referência do aeroporto.

## Limites de Vento Cruzado (RBAC154)

O sistema calcula automaticamente o limite de vento cruzado baseado no comprimento da pista:

| Comprimento da Pista | Limite de Vento Cruzado |
|----------------------|-------------------------|
| ≥ 1.500 m            | **20 kt** (37 km/h, 10,2 m/s) |
| 1.200 - 1.500 m      | **13 kt** (24 km/h, 6,6 m/s)  |
| < 1.200 m            | **10 kt** (19 km/h, 5,1 m/s)  |

## Exemplo de Configuração

```json
{
  "runway_length_m": 1500,
  "description": "Comprimento de pista de referência em metros (RBAC154)"
}
```

### Para aeroportos grandes (≥ 1.500m):
```json
{
  "runway_length_m": 2500
}
```
→ Usa limite de 20 kt

### Para aeroportos médios (1.200-1.500m):
```json
{
  "runway_length_m": 1350
}
```
→ Usa limite de 13 kt

### Para aeroportos pequenos (< 1.200m):
```json
{
  "runway_length_m": 900
}
```
→ Usa limite de 10 kt

## Observações

- O arquivo é carregado automaticamente ao iniciar o pipeline
- Se o arquivo não existir, o sistema usa o valor padrão de 1.500m (20 kt)
- Altere apenas o campo `runway_length_m`
