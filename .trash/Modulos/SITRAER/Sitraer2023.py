# CRIANDO OS SETOES
def calcular_setores(n, name_setores):
    meio_setor = (360 / n) / 2
    inicio = 360
    fim = 0
    dicionario_setores = {}
    for i in range(1,n+1):
        inicio = inicio - meio_setor * (1 if i == 1 else 2)
        fim = (fim if i == 1 else inicio) + meio_setor * (1 if i == 1 else 2)
        dicionario_setores[name_setores[i-1]] = (inicio, fim)
    return dicionario_setores

# CRIA O TITULO DO DATAFRAME FINAL CONFORME VARIAÇÃO DOS LIMITES
def GetTitle(limites):
    # CRIANDO O TITULO PERSONALIZAVEL
    columns_end = []
    for i in range(len(limites)):
        if i == 0:
            titulo = f"[0-{limites[i]}]"
            columns_end.append(titulo)
        elif i == len(limites)-1:
            titulo = f"[{limites[i-1]}-{limites[i]}]"
            columns_end.append(titulo)
            titulo = f"[{limites[i]}-*]"
            columns_end.append(titulo)
        else:
            titulo = f"[{limites[i-1]}-{limites[i]}]"
            columns_end.append(titulo)
    return columns_end

# CALCULANDO ANGULOS DA ROSA DOS VENTOS
def angulos_rosa(n, name_setores):
    passo = (360 / n)
    inicio = 360
    dicionario_setores = {}
    for i in range(1,n+1):
        if i != 1:
            inicio = inicio - passo
        dicionario_setores[name_setores[i-1]] = inicio if i == 1 else (inicio)
    return dicionario_setores

# FUNÇÃO QUE ENCONTRA AS POSSIVEIS PISTAS A 180 GRAUS
def PistasPossiveis(directions):
    opposite_directions = set()
    for direction, angle in directions.items():
        opposite_angle = (angle + 180) % 360
        opposite_direction = None
        for dir, ang in directions.items():
            if ang == opposite_angle:
                opposite_direction = dir
                break
        if opposite_direction:
            opposite_directions.add(tuple(sorted([direction, opposite_direction])))
    return opposite_directions

