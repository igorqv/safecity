"""
ingest.py — Leitura dos arquivos xlsx da SSP-SP e checkpoint em Parquet

Etapa 1 do pipeline SafeCity.

Por que pandas para leitura do xlsx, e não spark-excel?
  - openpyxl já está instalado e testado no ambiente (Python nativo)
  - spark-excel requer configuração de JAR externo e é mais frágil
  - Cada arquivo tem ~500k linhas com 46 colunas; pandas aguenta na memória
  - Após selecionar apenas as 15 colunas necessárias, a conversão para
    Spark é rápida e o union entre anos é feito no Spark de forma eficiente

Saída: parquet/raw/  (particionado por ANO para leitura seletiva posterior)
"""

import os
import re
import sys
from pathlib import Path
from functools import reduce

import pandas as pd
from tqdm import tqdm

# Declarar PYSPARK_SUBMIT_ARGS antes de importar pyspark — a JVM lê essa variável
# na inicialização e não é possível adicionar pacotes depois que a sessão começa
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "pyspark-shell")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

# Importar configurações centralizadas — evita hardcodar paths e nomes de colunas
sys.path.insert(0, str(Path(__file__).parent))
from config import (
    RAW_DATA_PATH, PARQUET_RAW,
    SPARK_APP_NAME, SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS,
    COLUNAS_NECESSARIAS,
)


# ── Schema explícito do Parquet de saída ──────────────────────────────────────
# Definir schema fixo garante que o union de 10 arquivos (2017–2026) produza
# um DataFrame com tipos consistentes, mesmo que o pandas infira tipos diferentes
# para arquivos de anos diferentes (ex: CEP como int em 2017, string em 2020).
SCHEMA_RAW = StructType([
    StructField("ANO_BO",              StringType(), True),
    StructField("NUM_BO",              StringType(), True),
    StructField("NOME_MUNICIPIO_CIRC", StringType(), True),
    StructField("DATA_OCORRENCIA_BO",  StringType(), True),
    StructField("HORA_OCORRENCIA",     StringType(), True),
    StructField("RUBRICA",             StringType(), True),
    StructField("FLAG_STATUS",         StringType(), True),
    StructField("DESCR_TIPOLOCAL",     StringType(), True),
    StructField("CIDADE",              StringType(), True),
    StructField("BAIRRO",              StringType(), True),
    StructField("LATITUDE",            StringType(), True),
    StructField("LONGITUDE",           StringType(), True),
    StructField("MARCA_OBJETO",        StringType(), True),
    StructField("MES",                 StringType(), True),
    StructField("ANO",                 StringType(), True),
    StructField("ARQUIVO_ORIGEM",      StringType(), True),
])


def criar_spark_session() -> SparkSession:
    """Cria e retorna a SparkSession configurada para o SafeCity."""
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        # Memória do driver: onde vivem os DataFrames em modo local.
        # Em modo local não há workers — tudo roda no processo do driver.
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        # Reduzir shuffle partitions: o default (200) é excessivo para
        # dados < 5GB em uma única máquina e gera overhead de scheduling
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        # Desabilitar UI web para poupar recursos — habilitar se precisar
        # debugar o plano de execução em http://localhost:4040
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    # Suprimir logs INFO/DEBUG do Java que poluem o terminal durante desenvolvimento
    spark.sparkContext.setLogLevel("WARN")
    return spark


def descobrir_aba(caminho: Path) -> str:
    """
    Descobre o nome da aba de dados no arquivo xlsx.

    Os arquivos SSP-SP têm abas como 'CELULAR_2017', 'CELULAR_2018', etc.
    Usamos regex para encontrar a aba correta sem hardcodar o ano,
    garantindo que novos arquivos (2027, 2028...) funcionem automaticamente.
    """
    import openpyxl
    wb = openpyxl.load_workbook(str(caminho), read_only=True)
    abas = wb.sheetnames
    wb.close()

    # Encontrar aba que corresponde ao padrão CELULAR_YYYY
    for aba in abas:
        if re.match(r"^CELULAR_\d{4}$", aba, re.IGNORECASE):
            return aba

    # Fallback: primeira aba (caso o nome mude em futuras versões do arquivo)
    print(f"  AVISO Aba CELULAR_YYYY não encontrada em {caminho.name} — usando '{abas[0]}'")
    return abas[0]


def ler_xlsx_como_pandas(caminho: Path) -> pd.DataFrame:
    """
    Lê um arquivo xlsx da SSP-SP e retorna apenas as colunas necessárias.

    Por que dtype=str?
      Os arquivos da SSP-SP têm colunas mistas (ex: HORA_OCORRENCIA pode ser
      objeto time ou string dependendo do ano). Ler tudo como string e fazer
      cast explícito no PySpark dá controle total sobre os tipos e evita
      surpresas de inferência automática do pandas.
    """
    aba = descobrir_aba(caminho)

    pdf = pd.read_excel(
        str(caminho),
        sheet_name=aba,
        dtype=str,       # tudo como string — cast controlado no clean.py
        engine="openpyxl",
    )

    # Selecionar apenas as colunas necessárias para os KPIs.
    # Colunas ausentes recebem None — isso acontece se o arquivo de um ano
    # específico não tiver uma coluna que outros anos têm.
    colunas_presentes = [c for c in COLUNAS_NECESSARIAS if c in pdf.columns]
    colunas_ausentes  = [c for c in COLUNAS_NECESSARIAS if c not in pdf.columns]

    if colunas_ausentes:
        print(f"  AVISO Colunas ausentes em {caminho.name}: {colunas_ausentes}")

    pdf = pdf[colunas_presentes].copy()

    # Preencher colunas ausentes com None para manter schema uniforme
    for col in colunas_ausentes:
        pdf[col] = None

    # Reordenar para a ordem definida em config.py
    pdf = pdf[COLUNAS_NECESSARIAS]

    # Converter None do pandas (NaN) para string vazia — o Spark trata vazio
    # diferente de None; vamos padronizar depois no clean.py
    pdf = pdf.fillna("")

    # Adicionar coluna de rastreabilidade para debugging de qualidade de dados.
    # Ex: se um bairro aparece errado, sabemos exatamente de qual arquivo veio.
    pdf["ARQUIVO_ORIGEM"] = caminho.name

    return pdf


def pandas_para_spark(
    spark: SparkSession,
    pdf: pd.DataFrame,
    schema: StructType,
) -> DataFrame:
    """
    Converte um DataFrame pandas para Spark com schema explícito.

    Por que schema explícito em vez de inferSchema?
      A inferência lê todos os dados duas vezes (scan + create) e pode
      escolher tipos diferentes para o mesmo campo em arquivos diferentes,
      causando falha no union posterior.
    """
    return spark.createDataFrame(pdf, schema=schema)


def main() -> None:
    print("=" * 60)
    print("SafeCity SP — Etapa 1: Ingestão dos arquivos xlsx")
    print("=" * 60)

    # Verificar se a pasta de dados existe antes de criar a sessão Spark
    # (criar a sessão é lento; falhar rápido se os dados não estiverem disponíveis)
    if not RAW_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Pasta de dados não encontrada: {RAW_DATA_PATH}\n"
            f"Configure RAW_DATA_PATH no arquivo .env"
        )

    arquivos_xlsx = sorted(RAW_DATA_PATH.glob("CelularesSubtraidos_*.xlsx"))
    # Ignorar arquivos temporários do Windows (Excel cria ~$arquivo.xlsx quando aberto)
    arquivos_xlsx = [f for f in arquivos_xlsx if not f.name.startswith("~$")]

    if not arquivos_xlsx:
        raise FileNotFoundError(
            f"Nenhum arquivo CelularesSubtraidos_*.xlsx encontrado em {RAW_DATA_PATH}"
        )

    print(f"\nArquivos encontrados: {len(arquivos_xlsx)}")
    for f in arquivos_xlsx:
        print(f"  - {f.name}")

    spark = criar_spark_session()
    print(f"\nSparkSession criada — versão {spark.version}")
    print(f"Memória do driver: {spark.conf.get('spark.driver.memory')}")

    # ── Leitura e conversão de cada arquivo ───────────────────────────────────
    dataframes: list[DataFrame] = []
    total_linhas = 0

    for caminho in tqdm(arquivos_xlsx, desc="Lendo arquivos xlsx"):
        print(f"\n-> {caminho.name}")

        pdf = ler_xlsx_como_pandas(caminho)
        n_linhas = len(pdf)
        total_linhas += n_linhas
        print(f"  Linhas lidas: {n_linhas:,}")

        sdf = pandas_para_spark(spark, pdf, SCHEMA_RAW)
        dataframes.append(sdf)

    # ── Union de todos os anos ─────────────────────────────────────────────────
    # union() no Spark é lazy — não executa aqui, apenas empilha os planos lógicos.
    # A execução real acontece quando write.parquet() é chamado abaixo.
    print(f"\nUnindo {len(dataframes)} DataFrames ({total_linhas:,} linhas total)...")
    df_unificado = reduce(lambda a, b: a.union(b), dataframes)

    # ── Checkpoint em Parquet ──────────────────────────────────────────────────
    # Salvar em Parquet aqui evita re-ler os xlsx em execuções futuras do pipeline.
    # Se clean.py ou compute_kpis.py precisar ser reexecutado (ex: correção de bug),
    # lê direto do Parquet em segundos em vez de re-processar todos os xlsx (~10min).
    PARQUET_RAW.mkdir(parents=True, exist_ok=True)
    saida = str(PARQUET_RAW)

    print(f"\nSalvando Parquet em: {saida}")
    (
        df_unificado
        .write
        .mode("overwrite")  # overwrite: reprocessamento completo substitui checkpoint anterior
        # Particionar por ANO permite que as etapas posteriores leiam apenas
        # anos específicos sem fazer full scan (ex: reprocessar só 2024)
        .partitionBy("ANO")
        .parquet(saida)
    )

    # Verificação final — contar lendo do Parquet para confirmar que foi gravado corretamente
    df_verificacao = spark.read.parquet(saida)
    total_parquet = df_verificacao.count()
    print(f"\nOK Parquet salvo com sucesso")
    print(f"  Total de linhas gravadas: {total_parquet:,}")
    print(f"  Partições (anos): {[f.name for f in PARQUET_RAW.iterdir() if f.is_dir()]}")

    spark.stop()
    print("\nOK Etapa 1 concluída — execute clean.py para continuar")


if __name__ == "__main__":
    main()
