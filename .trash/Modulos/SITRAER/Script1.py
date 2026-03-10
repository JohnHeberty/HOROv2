import pandas as pd

# CODIGO E SEU PSEUDOCODIGO
def Script1(DIRECAO, MAGNITUDE, NOMESETORES, LIMITES):
    """
    ################################################
    ###################### V1 ######################
    ################################################

    (DIRECAO, MAGNETUDE, NOMESETORES, LIMITES) <- DADOS DE ENTRADA 
    
    # VARIAVEIS INICIAIS PARA O CALCULO DOS SETORES
    N                               <- armazene quantidade de SETORES_DIRECIONAIS com base em NOMESETORES
    FIM                             <- armazene 0
    INICIO                          <- armazene 360
    MEIO_SETOR                      <- calcule ( ( INICIO / N ) / 2 )
    SETORES_DIRECIONAIS             <- declare um dicionario {}
    PARA cada i dentro de range(1,N+1) FAÇA:
        INICIO                      <- calcule  INICIO - MEIO_SETOR * (1 se SE for iqual a 1 SENÃO 2)
        FIM                         <- calcule  (FIM SE i for iqual a 1 SENÃO INICIO) + MEIO_SETOR * (1 SE i for iqual a 1 SENÃO 2)
        SETORES_DIRECIONAIS[NOMESETORES[i-1]] <- armzene (inicio, FIM)
    FIM PARA
    
    # VARIAVEIS INICIAIS PARA EFETUAR OS FILTROS NOS VENTOS RESPECTIVOS SEUS LIMITES 
    DF_WIND                         <- Crie um dataframe com as colunas sendo DIRECAO e MAGNETUDE
    FILTRO_LIMITES                  <- declare um dicionario {}
    PARA cada i, lIMITE dentro de enumerate(LIMITES) FAÇA:
        SE i iqual 0:
            FILTRO_LIMITES[i]       <- filtre os valores de MAGNETUDE dentro de DF_WIND sendo menor ou iqual ao LIMITE
        CASO i iqual ao tamanho de ( LIMITES - 1 ):
            FILTRO_LIMITES[i]       <- filtre os valores de MAGNETUDE dentro de DF_WIND maior que o LIMITES[i - 1] e menor que lIMITE
            FILTRO_LIMITES[i + 1]   <- filtre os valores de MAGNETUDE dentro de DF_WIND maior que o lIMITE
        SENÃO:
            FILTRO_LIMITES[i]       <- filtre os valores de MAGNETUDE dentro de DF_WIND maior que LIMITES[i - 1] e menor ou iqual a lIMITE
    FIM PARA
    
    # CRIANDO UM DICIONARIOS COM OS RESULTADOS DOS VENTOS DENTRO DOS SETORES_DIRECIONAIS
    RESULTADOS                      <- declare um dicionario {}
    PARA cada setor, (COMECO_SETOR, FIM_SETOR) dentro das chaves de SETORES_DIRECIONAIS FAÇA:
        QUANTIDADE_VENTOS           <- declare um dicionario {}
        PARA cada Chave e Valores dentro de FILTRO_LIMITES FAÇA:
            SE COMECO_SETOR < FIM_SETOR:
                FILTRO              <- filtre a quantidade de Ventos dentro de Valores usando a DIRECAO maior ou iqual a COMECO_SETOR e menor ou iqual a FIM_SETOR  
            SE NAO:
                FILTRO1             <- filtre a quantidade de Ventos dentro de Valores usando a DIRECAO maior ou iqual a COMECO_SETOR e menor ou iqual a 360  
                FILTRO2             <- filtre a quantidade de Ventos dentro de Valores usando a DIRECAO maior ou iqual a 0 e menor ou iqual a FIM_SETOR  
                FILTRO              <- Junte os valores do FILTRO1 com o do FILTRO2
            QUANTIDADE_VENTOS       <- armazene na Chave o tamanho de FILTRO
        RESULTADOS                  <- armazene no setor os valores da QUANTIDADE_VENTOS em forma de lista
    FIM PARA
        
    # CRIANDO OS TITULOS PARA A TABELA FINAL DE VENTOS
    TITULOS = []
    PARA cada i, lIMITE dentro de enumerate( LIMITES ):
        SE i for iqual 0:
            adicione f"[0-{lIMITE}]" a lista de TITULOS
        elif i == len(LIMITES) - 1:
            adicione f"[{LIMITES[i - 1]}-{lIMITE}]" a lista de TITULOS
        else:
            adicione f"[{LIMITES[i - 1]}-{lIMITE}]" a lista de TITULOS
    FIM PARA
    adicione f"[{lIMITE}-*]" a lista de TITULOS
    
    # CRIANDO O RESULTADO FINAL DA TABELA DE VENTOS
    DF_QUANTIDADE_VENTOS            <- crie um dataframe transposto com base em RESULTADOS com seu indice sendo sendo TITULOS
    DF_VENTOS_PERCENT               <- calcule um dataframe das porcentagem usando DF_QUANTIDADE_VENTOS em que 100% e a soma de tudo
    retorne DF_VENTOS_PERCENT
    
    ################################################
    ###################### V2 ######################
    ################################################
    
    Receba os dados de direção do vento, sua magnitude, nomes dos setores e os LIMITES de magnitude para cada setor.
    Divida o círculo de direção do vento em setores de igual tamanho.
    Para cada setor, conte a quantidade de ventos que se encaixam nos LIMITES de magnitude especificados.
    Calcule a porcentagem de ventos para cada setor em relação ao total.
    Retorne a tabela de porcentagens de ventos por setor.
    
    """
    
    # VARIAVEIS INICIAIS PARA O CALCULO DOS SETORES
    N = len(NOMESETORES)
    FIM = 0
    INICIO = 360
    MEIO_SETOR = (INICIO / N) / 2
    SETORES_DIRECIONAIS = {}
    for i in range(1, N + 1):
        INICIO -= MEIO_SETOR * (1 if i == 1 else 2)
        FIM = (FIM if i == 1 else INICIO) + MEIO_SETOR * (1 if i == 1 else 2)
        SETORES_DIRECIONAIS[NOMESETORES[i - 1]] = (INICIO, FIM)

    # VARIAVEIS INICIAIS PARA EFETUAR OS FILTROS NOS VENTOS RESPECTIVOS SEUS LIMITES
    DF_WIND = pd.DataFrame({'DIRECAO': DIRECAO, 'MAGNITUDE': MAGNITUDE})
    FILTRO_LIMITES = {}
    for i, limite in enumerate(LIMITES):
        if i == 0:
            FILTRO_LIMITES[i] = DF_WIND[DF_WIND['MAGNITUDE'] <= limite]
        elif i == len(LIMITES) - 1:
            FILTRO_LIMITES[i] = DF_WIND[(DF_WIND['MAGNITUDE'] > LIMITES[i - 1]) & (DF_WIND['MAGNITUDE'] <= limite)]
            FILTRO_LIMITES[i + 1] = DF_WIND[DF_WIND['MAGNITUDE'] > limite]
        else:
            FILTRO_LIMITES[i] = DF_WIND[(DF_WIND['MAGNITUDE'] > LIMITES[i - 1]) & (DF_WIND['MAGNITUDE'] <= limite)]

    # CRIANDO UM DICIONARIOS COM OS RESULTADOS DOS VENTOS DENTRO DOS SETORES_DIRECIONAIS
    RESULTADOS = {}
    for setor, (comeco_setor, fim_setor) in SETORES_DIRECIONAIS.items():
        quantidade_ventos = {}
        for chave, valores in FILTRO_LIMITES.items():
            if comeco_setor < fim_setor:
                filtro = valores[(valores['DIRECAO'] >= comeco_setor) & (valores['DIRECAO'] <= fim_setor)]
            else:
                filtro1 = valores[(valores['DIRECAO'] >= comeco_setor) & (valores['DIRECAO'] <= 360)]
                filtro2 = valores[(valores['DIRECAO'] >= 0) & (valores['DIRECAO'] <= fim_setor)]
                filtro = pd.concat([filtro1, filtro2])
            quantidade_ventos[chave] = len(filtro)
        RESULTADOS[setor] = list(quantidade_ventos.values())

    # CRIANDO OS TITULOS PARA A TABELA FINAL DE VENTOS
    TITULOS = []
    for i, limite in enumerate(LIMITES):
        if i == 0:
            TITULOS.append(f"[0-{limite}]")
        elif i == len(LIMITES) - 1:
            TITULOS.append(f"[{LIMITES[i - 1]}-{limite}]")
        else:
            TITULOS.append(f"[{LIMITES[i - 1]}-{limite}]")
    TITULOS.append(f"[{limite}-*]")

    # CRIANDO O RESULTADO FINAL DA TABELA DE VENTOS
    DF_QUANTIDADE_VENTOS = pd.DataFrame(RESULTADOS, index=TITULOS).T
    DF_VENTOS_PERCENT = DF_QUANTIDADE_VENTOS / DF_QUANTIDADE_VENTOS.sum().sum() * 100

    return DF_VENTOS_PERCENT