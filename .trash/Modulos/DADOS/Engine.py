from unidecode import unidecode
import pandas as pd
import numpy as np
import warnings
import pickle
import re
import os

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
# Encodings testados em ordem de prioridade para arquivos de órgãos
# do governo brasileiro (INMET, REDEMET, ANAC, DECEA, etc.)
# ---------------------------------------------------------------------------
_ENCODINGS_FALLBACK = [
    "utf-8-sig",   # UTF-8 com BOM — comum em exports do Excel / INMET
    "utf-8",
    "ISO-8859-1",  # Latin-1 — padrão antigo INMET / BDMEP
    "ISO-8859-2",  # Latin-2 — variante usada em alguns sistemas legados
    "cp1252",      # Windows-1252 — muito comum em sistemas gov Windows
    "cp850",       # DOS Latin-1
    "latin-1",     # alias de ISO-8859-1, aceito por mais parsers
]


def _detect_encoding(file_path: str) -> str | None:
    """
    Tenta detectar o encoding do arquivo usando chardet (se instalado).
    Retorna o encoding detectado com confiança >= 0.65, ou None.
    """
    try:
        import chardet
        with open(file_path, "rb") as f:
            raw = f.read()
        result = chardet.detect(raw)
        encoding = result.get("encoding")
        confidence = result.get("confidence", 0)
        if encoding and confidence >= 0.65:
            return encoding
    except ImportError:
        pass
    return None


def _read_lines_with_fallback(file_path: str, preferred_encoding: str | None = None) -> tuple[list[str], str]:
    """
    Lê todas as linhas de *file_path* tentando múltiplos encodings em ordem:
      1. Encoding detectado automaticamente (chardet)
      2. Encoding preferido fornecido pelo usuário
      3. Lista de fallbacks comuns de órgãos governamentais
      4. UTF-8 com substituição de caracteres inválidos (último recurso)

    Retorna (linhas, encoding_usado).
    """
    detected = _detect_encoding(file_path)

    candidates: list[str] = []
    if detected:
        candidates.append(detected)
    if preferred_encoding and preferred_encoding not in candidates:
        candidates.append(preferred_encoding)
    for enc in _ENCODINGS_FALLBACK:
        if enc.lower() not in [c.lower() for c in candidates]:
            candidates.append(enc)

    for encoding in candidates:
        try:
            with open(file_path, "r", encoding=encoding, errors="strict") as f:
                lines = f.readlines()
            print(f"  [ENCODING] '{os.path.basename(file_path)}' lido com: {encoding}")
            return lines, encoding
        except (UnicodeDecodeError, LookupError):
            continue

    # Último recurso: lê substituindo caracteres ilegíveis
    print(
        f"  [AVISO] Nenhum encoding funcionou perfeitamente para "
        f"'{os.path.basename(file_path)}'. "
        f"Usando UTF-8 com substituição de caracteres inválidos."
    )
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    return lines, "utf-8 (replace)"


class DatasetReader:

    def __init__(
        self,
        paths,
        decimal_places=3,
        m_to_knots=1.944,
        reanalysis=False,
        sep=";",
        vento="VENTO",
        direcao="DIREÇÃO",
        encoding=None,
        save_analysis=None,
        keep_calms=True,
    ):
        """
        Inicializa o DatasetReader com parâmetros fornecidos.

        :param paths:           Lista de caminhos de arquivos CSV.
        :param decimal_places:  Casas decimais para arredondamento.
        :param m_to_knots:      Fator de conversão de m/s para nós (padrão 1.944).
        :param reanalysis:      Se True, força releitura ignorando cache pickle.
        :param sep:             Separador de campos do CSV (padrão ";").
        :param vento:           Substring do nome da coluna de velocidade do vento.
        :param direcao:         Substring do nome da coluna de direção do vento.
        :param encoding:        Encoding preferido. Se None, detecta automaticamente.
        :param save_analysis:   Pasta para salvar o cache pickle. Usa
                                'Modulos/DADOS/TREATED' por padrão.
        :param keep_calms:      Se True (padrão), mantém registros com vento == 0
                                (calmaria). Se False, remove-os.
        """
        self.sep = sep
        self.vento = vento
        self.encoding = encoding          # None = auto-detect por arquivo
        self.direcao = direcao
        self.paths = paths
        self.decimal_places = decimal_places
        self.m_to_knots = m_to_knots
        self.reanalysis = reanalysis
        self.keep_calms = keep_calms
        self.save_analysis = save_analysis or os.path.join("Modulos", "DADOS", "TREATED")
        if not os.path.exists(self.save_analysis):
            os.makedirs(self.save_analysis)
        self.data_files = {}

    # ------------------------------------------------------------------
    # Ponto de entrada principal
    # ------------------------------------------------------------------

    def read_datasets(self):
        """
        Lê os arquivos de dados ou carrega de um pickle existente.
        """
        path_pickle = os.path.join(self.save_analysis, "DataFiles.pickle")

        if not os.path.exists(path_pickle) or self.reanalysis:
            for file_path in self.paths:
                try:
                    self.process_file(file_path)
                except Exception as e:
                    print(f"  [ERRO] Falha ao processar '{file_path}': {e}")
            if self.data_files:
                self.save_to_pickle(path_pickle)
        else:
            self.load_from_pickle(path_pickle)

        return self.data_files

    # ------------------------------------------------------------------
    # Leitura e processamento de um arquivo
    # ------------------------------------------------------------------

    def process_file(self, file_path: str):
        """
        Lê, valida e processa um único arquivo de dados meteorológicos.
        """
        print(f"READING FILE: {file_path}")
        text_lines, used_encoding = _read_lines_with_fallback(file_path, self.encoding)

        name, latitude, longitude, altitude = self.extract_metadata(text_lines)
        print(f"  Estação: {name} | Lat: {latitude} | Lon: {longitude} | Alt: {altitude}")

        # Converter coordenadas com segurança (sem eval)
        try:
            lat_f = float(str(latitude).replace(",", "."))
        except (ValueError, TypeError):
            raise ValueError(
                f"Latitude inválida '{latitude}' no arquivo '{os.path.basename(file_path)}'. "
                f"Verifique o campo LATITUDE: no cabeçalho."
            )
        try:
            lon_f = float(str(longitude).replace(",", "."))
        except (ValueError, TypeError):
            raise ValueError(
                f"Longitude inválida '{longitude}' no arquivo '{os.path.basename(file_path)}'. "
                f"Verifique o campo LONGITUDE: no cabeçalho."
            )

        dataset = self.create_dataframe(text_lines)
        print("  Colunas brutas:", list(dataset.columns))
        dataset = self.clean_data(dataset)
        print("  Colunas após limpeza:", list(dataset.columns))
        dataset = self.transform_wind_speed(dataset)

        if len(dataset.columns) != 3:
            raise ValueError(
                f"Esperado 3 colunas ['DATA', '{self.direcao}', '{self.vento}'] "
                f"mas encontradas {len(dataset.columns)}: {list(dataset.columns)}. "
                f"Verifique os parâmetros 'vento' e 'direcao'."
            )
        dataset.columns = ["DATA", self.direcao, self.vento]

        self.data_files[name] = {
            "Local": (lat_f, lon_f),
            "Altitude": altitude,
            "Dataset": dataset,
            "File Name": os.path.basename(file_path),
            "Path File": file_path,
            "Encoding": used_encoding,
        }

    # ------------------------------------------------------------------
    # Extração de metadados do cabeçalho
    # ------------------------------------------------------------------

    def extract_metadata(self, text_lines):
        """
        Extrai nome, latitude, longitude e altitude do cabeçalho do arquivo.
        """
        name      = self.extract_value(text_lines, "ESTACAO:",  self.sep)
        latitude  = self.extract_value(text_lines, "LATITUDE:", self.sep)
        longitude = self.extract_value(text_lines, "LONGITUDE:",self.sep)
        altitude  = self.extract_value(text_lines, "ALTITUDE:", self.sep)
        return name, latitude, longitude, altitude

    @staticmethod
    def extract_value(text_lines, label: str, sep: str = ";") -> str:
        """
        Localiza *label* nas linhas do cabeçalho e retorna o valor associado.
        Retorna 'NÃO LOCALIZADO' se o rótulo não existir no arquivo.
        """
        found_line = None
        for raw_line in text_lines:
            normalized = (
                unidecode(raw_line.strip())
                .upper()
                .replace(sep, " ")
                .replace("  ", " ")
                .replace(",", ".")
            )
            if label in normalized:
                found_line = normalized
                break

        if found_line is None:
            return "NÃO LOCALIZADO"

        parts = [p.strip() for p in found_line.split(label) if p.strip()]
        return parts[-1] if parts else "NÃO LOCALIZADO"

    # ------------------------------------------------------------------
    # Construção do DataFrame a partir das linhas de dados
    # ------------------------------------------------------------------

    def create_dataframe(self, text_lines):
        """
        Localiza a linha de título (cabeçalho dos dados) e constrói o DataFrame.
        Lança ValueError se nenhuma linha de título for encontrada.
        """
        title_index = None
        for n, line in enumerate(text_lines):
            cols = [c for c in line.split(self.sep) if c.strip()]
            if self.sep in line and len(pd.Series(cols).unique()) > 2:
                title_index = n
                break

        if title_index is None:
            raise ValueError(
                f"Não foi possível localizar a linha de cabeçalho dos dados. "
                f"Verifique se o separador é '{self.sep}' e se o arquivo possui mais de 2 colunas."
            )

        title = [
            unidecode(col.strip()).replace("  ", " ").replace(",", ".")
            for col in text_lines[title_index].split(self.sep)
        ]
        print("  Título:", title)

        data = [
            unidecode(line.strip()).replace("  ", " ").replace(",", ".").split(self.sep)
            for line in text_lines[title_index + 1:]
            if line.strip()
        ]
        df = pd.DataFrame(data, columns=title)
        df["DATA"] = pd.to_datetime(self.format_dates(df), errors="coerce")

        rows_before = len(df)
        df = df.dropna(subset=["DATA"]).reset_index(drop=True)
        dropped = rows_before - len(df)
        if dropped > 0:
            print(f"  [AVISO] {dropped} linhas descartadas por data inválida ou não reconhecida.")

        return df[[col for col in df.columns if col.strip()]]

    # ------------------------------------------------------------------
    # Normalização e parsing de datas
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_hour(hora_str: str) -> str:
        """
        Normaliza strings de hora para o formato HH:MM, tratando os
        diferentes padrões encontrados nos arquivos governamentais:
          - "UTC", "(UTC)", "UTM"  → removidos
          - "0"                    → "00:00"
          - "1200" (HHMM)          → "12:00"
          - "12:00" (já OK)        → "12:00"
          - "100"  (HMM)           → "01:00"
        """
        hora = (
            hora_str
            .replace("(UTC)", "")
            .replace("UTC", "")
            .replace("UTM", "")
            .strip()
        )
        # Já no formato correto HH:MM ou H:MM
        if re.match(r"^\d{1,2}:\d{2}$", hora):
            return hora
        # Meia-noite escrita como "0" ou "00"
        if re.match(r"^0{1,2}$", hora):
            return "00:00"
        # 4 dígitos sem separador: HHMM → HH:MM
        if re.match(r"^\d{4}$", hora):
            return f"{hora[:2]}:{hora[2:]}"
        # 3 dígitos sem separador: HMM → 0H:MM
        if re.match(r"^\d{3}$", hora):
            return f"0{hora[0]}:{hora[1:]}"
        # Qualquer outra coisa: devolve como está
        return hora

    def format_dates(self, df, Ncol_Data="Data", Ncol_Hora="Hora"):
        """
        Constrói uma série de datetime combinando as colunas de data e hora.
        Tenta múltiplos formatos comuns em arquivos de meteorologia brasileiros.
        Retorna a série de strings se nenhum formato reconhecer (será tratado
        com errors='coerce' em create_dataframe).
        """
        # Formatos comuns (ATENÇÃO: inserir em ordem do mais ao menos específico)
        formats = [
            "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M",
            "%d-%m-%Y %H:%M",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d %H%M",
            "%d/%m/%Y %H%M",
            "%d-%m-%Y %H%M",
            "%Y/%m/%d %H%M",
        ]

        # Localiza coluna de data
        col_Data_matches = [c for c in df.columns if Ncol_Data.upper().strip() in c.upper().strip()]
        if not col_Data_matches:
            raise IndexError(
                f"Coluna de data com nome contendo '{Ncol_Data}' não encontrada. "
                f"Colunas disponíveis: {list(df.columns)}"
            )
        col_Data = col_Data_matches[0]

        # Localiza coluna de hora
        col_Hora_matches = [c for c in df.columns if Ncol_Hora.upper().strip() in c.upper().strip()]
        if not col_Hora_matches:
            raise IndexError(
                f"Coluna de hora com nome contendo '{Ncol_Hora}' não encontrada. "
                f"Colunas disponíveis: {list(df.columns)}"
            )
        col_Hora = col_Hora_matches[0]

        # Normaliza a coluna de hora
        df = df.copy()
        df[col_Hora] = df[col_Hora].astype(str).apply(self._normalize_hour)

        # Combina data + hora
        df["DATA"] = df[col_Data].astype(str).str.strip() + " " + df[col_Hora]

        for fmt in formats:
            try:
                parsed = pd.to_datetime(df["DATA"], format=fmt, errors="raise")
                print(f"  [DATA] Formato reconhecido: '{fmt}'")
                return parsed
            except (ValueError, TypeError):
                continue

        # Nenhum formato exato funcionou — deixa pandas inferir
        print(
            "  [AVISO] Nenhum formato de data exato reconhecido. "
            "Usando inferência automática do pandas (pode ser lenta)."
        )
        return pd.to_datetime(df["DATA"], infer_datetime_format=True, errors="coerce")

    # ------------------------------------------------------------------
    # Limpeza e transformação dos dados
    # ------------------------------------------------------------------

    def clean_data(self, df):
        """
        Seleciona as colunas de DATA, direção e velocidade do vento;
        converte para float; descarta linhas completamente nulas.
        """
        invalid_tokens = {"None", "null", "none", "nan", "NaN", "N/A", "-",
                          "--", "---", "////", "VRB", "vrb", ""}

        columns = ["DATA"]
        columns += self.get_columns_by_keyword(df, self.direcao)
        columns += self.get_columns_by_keyword(
            df, self.vento, exclude1=self.direcao, exclude2="RAJADA"
        )

        if len(columns) < 3:
            raise ValueError(
                f"Não foi possível localizar colunas de direção ('{self.direcao}') e/ou "
                f"velocidade ('{self.vento}') no DataFrame. "
                f"Colunas disponíveis: {list(df.columns)}"
            )

        df = df[columns].copy()
        for col in columns[1:]:
            df[col] = df[col].astype(str).str.strip().replace(invalid_tokens, np.nan)
            df[col] = pd.to_numeric(
                df[col].str.replace(",", ".", regex=False),
                errors="coerce"
            ).round(self.decimal_places)

        rows_before = len(df)
        df = df.dropna(subset=columns[1:], how="all").reset_index(drop=True)
        dropped = rows_before - len(df)
        if dropped > 0:
            print(f"  [AVISO] {dropped} linhas com direção e/ou velocidade inválidas descartadas.")

        return df

    def transform_wind_speed(self, df):
        """
        Converte a velocidade do vento de m/s para nós.
        Valores não numéricos residuais são descartados com aviso.
        Calmarias (0 kt) são mantidas por padrão (veja keep_calms).
        """
        wind_cols = [
            c for c in df.columns
            if self.vento.upper().strip() in c.upper().strip()
        ]
        if not wind_cols:
            raise IndexError(
                f"Parâmetro 'vento'='{self.vento}' não localizado nos dados. "
                f"Colunas disponíveis: {list(df.columns)}"
            )
        wind_column = wind_cols[0]

        # Garante float puro (segunda passagem de segurança)
        df[wind_column] = pd.to_numeric(
            df[wind_column].astype(str).str.replace(",", ".", regex=False).str.strip(),
            errors="coerce"
        )

        invalidos = df[wind_column].isna().sum()
        if invalidos > 0:
            print(f"  [AVISO] {invalidos} valores inválidos na coluna '{wind_column}' descartados.")
        df = df.dropna(subset=[wind_column]).reset_index(drop=True)

        # Converte m/s → nós
        df[wind_column] = (df[wind_column] * self.m_to_knots).round(self.decimal_places)

        # Filtra calmarias somente se solicitado
        if not self.keep_calms:
            df = df[df[wind_column] > 0].reset_index(drop=True)

        return df.sort_values("DATA").reset_index(drop=True)

    # ------------------------------------------------------------------
    # Utilitários
    # ------------------------------------------------------------------

    @staticmethod
    def get_columns_by_keyword(df, keyword, exclude1=None, exclude2=None):
        """
        Retorna colunas cujo nome contém *keyword*,
        excluindo as que contêm *exclude1* ou *exclude2*.
        """
        return list(
            pd.Series([
                col for col in df.columns
                if keyword in col
                and (exclude1 is None or exclude1 not in col)
                and (exclude2 is None or exclude2 not in col)
            ]).unique()
        )

    @staticmethod
    def convert_to_float(value):
        """
        Converte um valor para float, retornando np.nan em caso de falha.
        Mantido por compatibilidade; prefira pd.to_numeric com errors='coerce'.
        """
        try:
            return round(float(str(value).replace(",", ".")), 3)
        except (ValueError, TypeError):
            return np.nan

    def save_to_pickle(self, path_pickle: str):
        """
        Serializa self.data_files em disco.
        """
        with open(path_pickle, "wb") as f:
            pickle.dump(self.data_files, f)
        print(f"  [CACHE] Salvo em: {path_pickle}")

    def load_from_pickle(self, path_pickle: str):
        """
        Carrega self.data_files do cache em disco.
        """
        with open(path_pickle, "rb") as f:
            self.data_files = pickle.load(f)
        print(f"  [CACHE] Carregado de: {path_pickle}")
    