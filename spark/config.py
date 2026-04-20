"""
config.py — Configurações centralizadas do pipeline SafeCity SP

Centralizar paths, nomes de colunas e constantes aqui garante que
uma mudança de configuração (ex: novo path de dados) não precise ser
propagada em vários arquivos.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Carregar .env a partir da raiz do projeto (um nível acima de spark/)
# MOTIVO: .env fica na raiz do projeto, não dentro de spark/
_RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(_RAIZ / ".env")


# ── Paths ─────────────────────────────────────────────────────────────────────

# Pasta com os .xlsx brutos da SSP-SP
RAW_DATA_PATH = Path(os.getenv("RAW_DATA_PATH", str(_RAIZ / "raw")))
if not RAW_DATA_PATH.is_absolute():
    RAW_DATA_PATH = _RAIZ / RAW_DATA_PATH

# Pasta onde os checkpoints Parquet serão salvos
PARQUET_PATH = Path(os.getenv("PARQUET_PATH", str(_RAIZ / "parquet")))
if not PARQUET_PATH.is_absolute():
    PARQUET_PATH = _RAIZ / PARQUET_PATH

# Sub-pastas de checkpoint — separar raw de clean de kpis facilita
# reprocessar apenas uma etapa sem refazer tudo desde o início
PARQUET_RAW   = PARQUET_PATH / "raw"
PARQUET_CLEAN = PARQUET_PATH / "clean"
PARQUET_KPIS  = PARQUET_PATH / "kpis"


# ── Spark ─────────────────────────────────────────────────────────────────────

SPARK_APP_NAME     = "safecity-etl"
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")

# 8 partições é suficiente para dados < 5GB em modo local.
# O default do Spark (200) causaria overhead enorme de agendamento
# sem nenhum benefício em uma única máquina.
SPARK_SHUFFLE_PARTITIONS = "8"


# ── Supabase ──────────────────────────────────────────────────────────────────

SUPABASE_URL         = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# String de conexão PostgreSQL direta — usada pelo SQLAlchemy para carga via pandas
SUPABASE_DB_HOST     = os.getenv("SUPABASE_DB_HOST", "")
SUPABASE_DB_PORT     = os.getenv("SUPABASE_DB_PORT", "5432")
SUPABASE_DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
SUPABASE_DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
SUPABASE_DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD", "")

def get_db_url() -> str:
    """Monta a URL de conexão PostgreSQL para o SQLAlchemy."""
    return (
        f"postgresql+psycopg2://{SUPABASE_DB_USER}:{SUPABASE_DB_PASSWORD}"
        f"@{SUPABASE_DB_HOST}:{SUPABASE_DB_PORT}/{SUPABASE_DB_NAME}"
        f"?sslmode=require"
        # sslmode=require: Supabase exige SSL em todas as conexões externas
    )


# ── Colunas dos arquivos SSP-SP ───────────────────────────────────────────────

# Selecionar apenas as colunas necessárias para os KPIs.
# MOTIVO: os xlsx têm 46 colunas; carregar só 15 reduz memória em ~67%
# e acelera a conversão pandas → Spark.
COLUNAS_NECESSARIAS = [
    "ANO_BO",              # Ano do boletim de ocorrência
    "NUM_BO",              # Número do BO (para identificação)
    "NOME_MUNICIPIO_CIRC", # Município onde o crime aconteceu (não onde a DP está)
    "DATA_OCORRENCIA_BO",  # Data do crime
    "HORA_OCORRENCIA",     # Hora do crime (objeto time no Excel)
    "RUBRICA",             # Tipo do crime: "Furto (art. 155)", "Roubo (art. 157)"...
    "FLAG_STATUS",         # CONSUMADO ou TENTADO
    "DESCR_TIPOLOCAL",     # Tipo de local: "Via pública", "Terminal/Estação"...
    "CIDADE",              # Cidade do local do crime
    "BAIRRO",              # Bairro do crime
    "LATITUDE",            # Coordenada geográfica
    "LONGITUDE",           # Coordenada geográfica
    "MARCA_OBJETO",        # Marca do celular subtraído
    "MES",                 # Mês da ocorrência (1–12)
    "ANO",                 # Ano da ocorrência
]

# Mapeamento de normalização de marcas de celular.
# Chave: regex case-insensitive | Valor: nome padronizado
# MOTIVO: "SAMSUNG", "Samsung", "SAMSUNG GALAXY" devem ser uma única marca
# para que o KPI de marcas não fragmente os dados em dezenas de variações
MARCAS_NORMALIZACAO = {
    r"(?i)(apple|iphone|i[\s-]?phone)": "Apple",
    r"(?i)(samsung|galaxy)":            "Samsung",
    r"(?i)(motorola|moto[\s-])":        "Motorola",
    r"(?i)xiaomi":                      "Xiaomi",
    r"(?i)lg\b":                        "LG",
    r"(?i)positivo":                    "Positivo",
    r"(?i)lenovo":                      "Lenovo",
    r"(?i)asus":                        "Asus",
    r"(?i)nokia":                       "Nokia",
    r"(?i)huawei":                      "Huawei",
    r"(?i)tcl":                         "TCL",
    r"(?i)realme":                      "Realme",
    r"(?i)oppo":                        "Oppo",
    r"(?i)multilaser":                  "Multilaser",
}

# Município da capital — usado para filtrar bairros apenas de SP
MUNICIPIO_SP_CAPITAL = "S.PAULO"

# Nomes das tabelas no Supabase — centralizar evita typos espalhados no código
TABELAS_KPI = {
    "annual_summary":  "kpi_annual_summary",
    "monthly":         "kpi_monthly",
    "municipality":    "kpi_by_municipality",
    "bairro":          "kpi_by_bairro",
    "location_type":   "kpi_by_location_type",
    "brand":           "kpi_by_brand",
    "hour":            "kpi_by_hour",
    "period":          "kpi_by_period",
}
