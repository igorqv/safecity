"""
clean.py — Limpeza, normalização e derivação de colunas

Etapa 2 do pipeline SafeCity.

Lê o Parquet bruto (saída do ingest.py) e aplica:
  1. Cast de tipos (string -> int, double, etc.)
  2. Limpeza de strings (trim, normalização de encoding)
  3. Normalização da RUBRICA -> tipo_crime ('Furto' | 'Roubo')
  4. Normalização de MARCA_OBJETO -> marcas padronizadas
  5. Derivação do PERIODO do dia a partir da HORA_OCORRENCIA
  6. Filtro de FLAG_STATUS = CONSUMADO (remover tentativas dos KPIs principais)

Saída: parquet/clean/ (particionado por ANO)
"""

import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "pyspark-shell")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).parent))
from config import (
    PARQUET_RAW, PARQUET_CLEAN,
    SPARK_APP_NAME, SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS,
    MARCAS_NORMALIZACAO, MUNICIPIO_SP_CAPITAL,
)


def criar_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-clean")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def cast_tipos(df: DataFrame) -> DataFrame:
    """
    Converte colunas de string para os tipos corretos.

    Por que fazer cast manual em vez de inferSchema no ingest?
      O ingest leu tudo como string para garantir schema uniforme entre arquivos.
      Aqui fazemos o cast uma vez, de forma controlada e auditável.
      Valores inválidos (ex: "NULL" onde esperamos um inteiro) viram null — não crasham.
    """
    return (
        df
        .withColumn("ANO_BO",    F.col("ANO_BO").cast("integer"))
        .withColumn("NUM_BO",    F.col("NUM_BO").cast("integer"))
        .withColumn("MES",       F.col("MES").cast("integer"))
        .withColumn("ANO",       F.col("ANO").cast("integer"))
        # LATITUDE e LONGITUDE: valores como "0" ou "" viram null após cast double
        # Isso é correto — coordenada (0,0) no meio do oceano é dado inválido
        .withColumn("LATITUDE",  F.col("LATITUDE").cast("double"))
        .withColumn("LONGITUDE", F.col("LONGITUDE").cast("double"))
        # HORA_OCORRENCIA vem no formato "HH:MM:SS" (representação do objeto time do Excel)
        # Extraímos apenas a parte das horas (0–23) para os KPIs de distribuição horária
        .withColumn(
            "HORA",
            F.when(
                F.col("HORA_OCORRENCIA").rlike(r"^\d{1,2}:\d{2}"),
                F.split(F.col("HORA_OCORRENCIA"), ":").getItem(0).cast("integer")
            ).otherwise(F.lit(None).cast("integer"))
        )
        # DATA_OCORRENCIA_BO pode vir como "2024-01-15 00:00:00" (datetime do pandas)
        # Convertemos para DateType para facilitar extração de ano/mês
        .withColumn(
            "DATA_OCORRENCIA",
            F.coalesce(
                F.to_date(F.col("DATA_OCORRENCIA_BO"), "yyyy-MM-dd HH:mm:ss"),
                F.to_date(F.col("DATA_OCORRENCIA_BO"), "yyyy-MM-dd"),
                F.to_date(F.col("DATA_OCORRENCIA_BO"), "dd/MM/yyyy"),
            )
        )
    )


def limpar_strings(df: DataFrame) -> DataFrame:
    """
    Remove espaços extras e padroniza strings.

    Os arquivos da SSP-SP têm strings com muitos espaços de preenchimento
    (ex: 'S.PAULO                       '). Sem trim, 'S.PAULO' e 'S.PAULO '
    seriam tratados como municípios diferentes no groupBy.
    """
    colunas_texto = [
        "NOME_MUNICIPIO_CIRC", "RUBRICA", "FLAG_STATUS",
        "DESCR_TIPOLOCAL", "CIDADE", "BAIRRO", "MARCA_OBJETO",
    ]
    for col in colunas_texto:
        df = df.withColumn(col, F.trim(F.col(col)))

    # Substituir strings "NULL" (literais do banco de origem) por null real
    # MOTIVO: o banco da SSP exporta campos nulos como a string "NULL"
    for col in colunas_texto:
        df = df.withColumn(
            col,
            F.when(F.upper(F.col(col)).isin("NULL", "N/D", ""), None)
             .otherwise(F.col(col))
        )

    return df


def normalizar_tipo_crime(df: DataFrame) -> DataFrame:
    """
    Deriva a coluna tipo_crime a partir de RUBRICA.

    RUBRICA nos dados originais é verbosa:
      - "Furto (art. 155)"
      - "Furto qualificado (art. 155, §4o.)"
      - "Roubo (art. 157)"
      - "Roubo majorado (art. 157, §2o.)"

    Simplificamos para 'Furto' | 'Roubo' | 'Outros' para facilitar
    filtros e pivots no dashboard. Mantemos RUBRICA original para
    análises futuras mais granulares.
    """
    return df.withColumn(
        "tipo_crime",
        F.when(F.lower(F.col("RUBRICA")).contains("roubo"), "Roubo")
         .when(F.lower(F.col("RUBRICA")).contains("furto"), "Furto")
         .otherwise("Outros")
    )


def normalizar_marca(df: DataFrame) -> DataFrame:
    """
    Normaliza MARCA_OBJETO para marcas padronizadas.

    Por que normalizar marcas?
      O campo MARCA_OBJETO é preenchido manualmente pelos atendentes das DPs.
      Uma mesma marca aparece em dezenas de variações: 'SAMSUNG', 'Samsung',
      'SAMSUNG GALAXY J5', 'Galaxy'. Sem normalização, o KPI de marcas
      ficaria fragmentado em centenas de valores únicos sem sentido analítico.

    Estratégia:
      1. Converter para maiúsculo (facilita matching)
      2. Aplicar regex para as top 14 marcas em ordem de prevalência
      3. O que não casar em nenhum padrão -> "Outras"
    """
    # Primeiro, criar coluna auxiliar em maiúsculo para o matching
    df = df.withColumn("_marca_upper", F.upper(F.trim(F.col("MARCA_OBJETO"))))

    # Aplicar os padrões de normalização definidos em config.py
    # A ordem importa: Apple antes de "outros iPhone", Samsung antes de "Galaxy"
    marca_col = F.lit("Outras")  # valor padrão se nenhum padrão casar
    for padrao, nome_padrao in MARCAS_NORMALIZACAO.items():
        marca_col = F.when(
            F.col("_marca_upper").rlike(padrao),
            nome_padrao
        ).otherwise(marca_col)

    # Inverter a ordem de aplicação: when encadeia como CASE WHEN em SQL,
    # portanto a ÚLTIMA condição aplicada tem MAIOR prioridade.
    # Reconstruir na ordem correta usando reduce com prioridade explícita.
    marca_col = F.lit("Outras")
    for padrao, nome_padrao in reversed(list(MARCAS_NORMALIZACAO.items())):
        marca_col = F.when(
            F.col("_marca_upper").rlike(padrao),
            nome_padrao
        ).otherwise(marca_col)

    df = df.withColumn("marca_normalizada", marca_col)
    df = df.drop("_marca_upper")

    return df


def derivar_periodo(df: DataFrame) -> DataFrame:
    """
    Deriva o período do dia a partir da hora da ocorrência.

    Por que derivar aqui em vez de no compute_kpis?
      Centralizar a lógica de derivação em clean.py garante que todos
      os KPIs usem exatamente a mesma definição de período. Se a regra
      mudar (ex: Tarde passa a ser 12–18h), muda em um único lugar.

    Faixas:
      Madrugada: 00–05h
      Manhã:     06–11h
      Tarde:     12–17h
      Noite:     18–23h
    """
    return df.withColumn(
        "periodo",
        F.when((F.col("HORA") >= 0)  & (F.col("HORA") < 6),  "Madrugada")
         .when((F.col("HORA") >= 6)  & (F.col("HORA") < 12), "Manhã")
         .when((F.col("HORA") >= 12) & (F.col("HORA") < 18), "Tarde")
         .when((F.col("HORA") >= 18) & (F.col("HORA") <= 23),"Noite")
         .otherwise(None)  # hora nula ou inválida -> não classifica
    )


def filtrar_consumados(df: DataFrame) -> DataFrame:
    """
    Remove ocorrências tentadas (FLAG_STATUS = 'TENTADO').

    Por que filtrar apenas CONSUMADO?
      Os KPIs de segurança pública reportam crimes consumados — é o dado
      relevante para vitimização real. Tentativas têm valor para análise
      policial mas distorceriam os KPIs de impacto ao cidadão.
      Mantemos apenas CONSUMADO como padrão do dashboard.
    """
    return df.filter(
        F.upper(F.col("FLAG_STATUS")) == "CONSUMADO"
    )


def selecionar_colunas_finais(df: DataFrame) -> DataFrame:
    """
    Seleciona apenas as colunas que serão usadas nas agregações de KPI.
    Remove colunas intermediárias e renomeia para nomes snake_case consistentes.
    """
    return df.select(
        F.col("ANO_BO").alias("ano_bo"),
        F.col("ANO").alias("ano"),
        F.col("MES").alias("mes"),
        F.col("NOME_MUNICIPIO_CIRC").alias("municipio"),
        F.col("CIDADE").alias("cidade"),
        F.col("BAIRRO").alias("bairro"),
        F.col("tipo_crime"),
        F.col("RUBRICA").alias("rubrica"),
        F.col("DESCR_TIPOLOCAL").alias("tipo_local"),
        F.col("marca_normalizada").alias("marca"),
        F.col("HORA").alias("hora"),
        F.col("periodo"),
        F.col("LATITUDE").alias("latitude"),
        F.col("LONGITUDE").alias("longitude"),
        F.col("DATA_OCORRENCIA").alias("data_ocorrencia"),
        F.col("ARQUIVO_ORIGEM").alias("arquivo_origem"),
    )


def main() -> None:
    print("=" * 60)
    print("SafeCity SP — Etapa 2: Limpeza e Normalização")
    print("=" * 60)

    if not PARQUET_RAW.exists():
        raise FileNotFoundError(
            f"Parquet bruto não encontrado em {PARQUET_RAW}\n"
            f"Execute ingest.py primeiro."
        )

    spark = criar_spark_session()

    print(f"\nLendo Parquet bruto de: {PARQUET_RAW}")
    df = spark.read.parquet(str(PARQUET_RAW))

    total_bruto = df.count()
    print(f"Total de linhas brutas: {total_bruto:,}")

    # Filtrar datas inválidas (antes de 1900) para evitar Spark datetime rebase error
    # ao escrever Parquet. O Spark 3.0+ rejeita datas muito antigas por segurança.
    df = df.filter(F.col("DATA_OCORRENCIA") >= F.lit("1900-01-01"))
    total_valido = df.count()
    print(f"Linhas após filtro de datas válidas: {total_valido:,} (-{total_bruto - total_valido:,} inválidas)")

    # Aplicar transformações em sequência — Spark é lazy,
    # todas as transformações são empilhadas e executadas juntas no write
    print("\nAplicando transformações...")
    df = cast_tipos(df)
    df = limpar_strings(df)
    df = normalizar_tipo_crime(df)
    df = normalizar_marca(df)
    df = derivar_periodo(df)
    df = filtrar_consumados(df)
    df = selecionar_colunas_finais(df)

    # Salvar checkpoint limpo
    PARQUET_CLEAN.mkdir(parents=True, exist_ok=True)
    saida = str(PARQUET_CLEAN)

    print(f"\nSalvando Parquet limpo em: {saida}")
    (
        df
        .write
        .mode("overwrite")
        # Particionar por ano: compute_kpis.py pode filtrar por ano sem full scan
        .partitionBy("ano")
        .parquet(saida)
    )

    # Verificação de qualidade — comparar total antes e depois para detectar
    # perdas excessivas de dados (ex: filtro de CONSUMADO muito agressivo)
    df_limpo = spark.read.parquet(saida)
    total_limpo = df_limpo.count()
    pct_retido = (total_limpo / total_bruto * 100) if total_bruto > 0 else 0

    print(f"\nOK Limpeza concluída")
    print(f"  Linhas brutas:  {total_bruto:,}")
    print(f"  Linhas limpas:  {total_limpo:,}")
    print(f"  Retido:         {pct_retido:.1f}%")

    # Mostrar distribuição de tipo_crime como sanity check
    print("\nDistribuição por tipo_crime:")
    df_limpo.groupBy("tipo_crime").count().orderBy("count", ascending=False).show()

    # Mostrar top 10 marcas normalizadas
    print("Top 10 marcas normalizadas:")
    df_limpo.groupBy("marca").count().orderBy("count", ascending=False).show(10)

    spark.stop()
    print("\nOK Etapa 2 concluída — execute compute_kpis.py para continuar")


if __name__ == "__main__":
    main()
