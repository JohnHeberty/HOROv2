from glob import glob
import cv2 as cv
import os

# DESENVOLVIDO POR JOHN HEBERTY DE FREITAS
# E-mail: john.7heberty@gmail.com

Url_MagneticDeclination = "https://ngdc.noaa.gov/geomag/calculators/magcalc.shtml"
DirectionName           = "VENTO. DIRE"                 # NOME COMUM ENTRE OS BANCOS PARA O RUMO DO VENTO
WindName                = "VENTO. VELO"              # NOME COMUM ENTRE OS BANCOS PARA O VENTO
DecimalPlaces           = 3                         # ARREDONDADMENTO EM 3 CASAS DESCIMAIS
RoseWind                = 16                        # QUANTIDADE DE BARRAS QUE RODA DOS VENTOS TERA
LIMITES_IN_PPD          = [3,   13, 20]             # Limites dentro da PPD
LIMITES_OUT_PPD         = [20,  25, 40]             # Limites fora da PPD
LIMITES                 = [3,   13, 20, 25, 40]     # Limites para a Rosas dos Ventos Segundo RBAC154 P/ RUNWAY >= 1500m
WindRunwayLimite        = 20                        # VENTOS MENORES QUE ESTE VALOR SERÁ VENTOS DENTRO DA PISTA, CASO SEJA MAIOR E VENTO DE TRAVEZ - COM BASE NA RBAC154
SectorNames             = {                         # Nome dos Possiveis Nomes para setores padrões
     4: ['N', 'W', 'S', 'E'],
     8: ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE'],
    16: ['N', 'NNW', 'NW', 'WNW', 'W', 'WSW', 'SW', 'SSW', 'S', 'SSE', 'SE', 'ESE', 'E', 'ENE', 'NE', 'NNE']
}

# VARIAVEIS PARA OTIMIZAÇÃO DE PISTA
Width_IMG               = 1920#3840                 # LARGURA DA IMAGEM QUE SERA CRIADA A SIMULAÇÃO (Afeta o desempenho)
Height_IMG              = 1080#2160                 # ALTURA DA IMAGEM QUE SERA CRIADA A SIMULAÇÃO (Afeta o desempenho)
SpeedGIF                = 4                         # ACELERA O VIDEO PARA GIF EM X VEZES
ProportionWindRoseImg   = 0.20                      # E COM BASE NO RAIO LOGO OCUPA 2X DA IMAGEM
FonteThickness          = 1                         # ESPESSURA DA FONTE DENTRO DA IMAGEM
FonteSize               = 0.90                      # TAMANHO DA FONTE DENTRO DA IMAGEM
MaxSpinRoseWind         = 180
StartLegendRight        = Width_IMG - 510           # ONDE COMEÇA A ESCREVER A LEGENDA NA PARTE DIRETA
StartLegendLeft         = 40                        # ONDE COMEÇA A ESCREVER A LEGENDA NA PARTE ESQUERDA
HeightEspaceLegend      = 40                        # ESPAÇO ENTRE AS LEGENDAS
Fonte                   = cv.FONT_HERSHEY_SIMPLEX   # FONTE A SER USADA NA SIMULAÇÃO
MakeVideo               = True                      # SE ATIVADO IRÁ PRODUZIR UM VIDEO NO FINAL
SaveFinalResult         = True                      # SE ATIVADO IRÁ SALVAR RESULTADOS EM JSON
ColorRunWay             = (255, 255, 255)           # COR DA PISTA A SER DESENHADA NA ROSA DOS VENTOS
ColorLegend_BottomLeft  = (255, 255, 255)           # COR DA LEGENDA INFERIRO ESQUERDA 
ColorBestRunWay         = (0, 255, 0)               # COR DA MELHOR PISTA A SER DESENHADA NA ROSA DOS VENTOS
ColorPointRef           = (255, 165, 0)             # COR DO PONTO DE REFERENCIA DA PISTA
PointSizeRef            = 25                        # TAMANHO DO PONTO DE REFERENCIA DA PISTA

# DIRETÓRIO RAIZ DO PROJETO (absoluto, independente de onde o script é executado)
_BASE_DIR               = os.path.dirname(os.path.abspath(__file__))

# PATHS
FolderImages            = os.path.join(_BASE_DIR, "2-OUTPUT", "Movies", "IMGS")
caminho_saida_video     = os.path.join(_BASE_DIR, "2-OUTPUT", "Movies", "{}", "RunwayOrientation-{}.mp4")
_csv_inputs             = glob(os.path.join(_BASE_DIR, "1-INPUT", "*.csv"))
WeatherStationsPath     = _csv_inputs if _csv_inputs else glob(os.path.join(_BASE_DIR, "1-INPUT", "*.CSV"))
WeatherStationsPath_OK  = os.path.join(_BASE_DIR, "Modulos", "DADOS", "TREATED")