"""
compute_kpis.py — Cálculo das 8 tabelas de KPI do SafeCity SP

Etapa 3 do pipeline.

Lê o Parquet limpo (saída do clean.py) e computa:
  1. kpi_annual_summary  — total por ano/tipo com variação YoY
  2. kpi_monthly         — total por mês/ano/tipo
  3. kpi_by_municipality — ranking de municípios
  4. kpi_by_bairro       — ranking de bairros (SP capital)
  5. kpi_by_location_type — distribuição por tipo de local
  6. kpi_by_brand        — marcas de celular mais subtraídas
  7. kpi_by_hour         — distribuição horária (0–23h)
  8. kpi_by_period       — distribuição por período do dia

Saída: parquet/kpis/<nome_tabela>/ — um Parquet por tabela
"""

import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "pyspark-shell")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).parent))
from config import (
    PARQUET_CLEAN, PARQUET_KPIS,
    SPARK_APP_NAME, SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS,
    MUNICIPIO_SP_CAPITAL, TABELAS_KPI,
)


def criar_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-kpis")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def kpi_annual_summary(df: DataFrame) -> DataFrame:
    """
    Resumo anual com variação Year-over-Year.

    Por que usar Window function em vez de JOIN para o YoY?
      Window com lag() é mais eficiente: não cria um DataFrame intermediário
      e evita um shuffle extra que um self-JOIN exigiria. O lag() acessa
      o valor do ano anterior dentro da mesma partição (tipo_crime).
    """
    # Calcular totais por ano e tipo de crime
    base = (
        df
        .groupBy("ano", "tipo_crime")
        .agg(F.count("*").alias("total"))
    )

    # Window particionada por tipo_crime para que o lag do Furto
    # não "vazepara o Roubo — cada tipo tem seu próprio histórico anual
    window_yoy = Window.partitionBy("tipo_crime").orderBy("ano")

    return (
        base
        .withColumn(
            "variacao_yoy_pct",
            # Fórmula: (atual - anterior) / anterior * 100
            # null quando não há ano anterior (primeiro ano da série)
            F.round(
                (F.col("total") - F.lag("total", 1).over(window_yoy))
                / F.lag("total", 1).over(window_yoy) * 100,
                2
            )
        )
        .orderBy("tipo_crime", "ano")
    )


def kpi_monthly(df: DataFrame) -> DataFrame:
    """
    Ocorrências por mês, ano e tipo de crime.

    Alimenta o gráfico de sazonalidade no dashboard.
    Separar por tipo_crime permite que o filtro do dashboard
    mostre sazonalidade só de Furtos ou só de Roubos.
    """
    return (
        df
        .groupBy("ano", "mes", "tipo_crime")
        .agg(F.count("*").alias("total"))
        # Filtrar meses inválidos — cast falhou (valor original era vazio)
        .filter(F.col("mes").isNotNull() & (F.col("mes").between(1, 12)))
        .orderBy("ano", "mes")
    )


def kpi_by_municipality(df: DataFrame) -> DataFrame:
    """
    Ocorrências por município, limitado ao top 30 por ano.

    Por que top 30 e não todos?
      SP tem 645 municípios. Carregar todos no dashboard tornaria
      o JSON enorme (~65k linhas) para dados de cauda longa com
      poucas ocorrências. Top 30 cobre >90% das ocorrências.

    Por que rank() com Window em vez de limit()?
      limit() retorna os top N globais — se filtrarmos depois por ano,
      teremos anos com menos de 30 municípios. rank() por Window
      garante top 30 para CADA ano independentemente.
    """
    base = (
        df
        .filter(F.col("municipio").isNotNull())
        .groupBy("ano", "municipio", "tipo_crime")
        .agg(F.count("*").alias("total"))
    )

    # Window para ranking dentro de cada combinação ano + tipo_crime
    window_rank = Window.partitionBy("ano", "tipo_crime").orderBy(F.desc("total"))

    return (
        base
        .withColumn("rank", F.rank().over(window_rank))
        .filter(F.col("rank") <= 30)
        .drop("rank")
        .orderBy("ano", F.desc("total"))
    )


def kpi_by_bairro(df: DataFrame) -> DataFrame:
    """
    Ocorrências por bairro, restrito à São Paulo capital, top 50 por ano.

    Por que apenas SP capital?
      Bairro é dado granular e inconsistente para cidades menores.
      SP capital tem bairros bem definidos e é onde se concentra
      ~60% de todas as ocorrências — o dado é mais confiável e relevante.

    Por que top 50 bairros?
      SP tem ~96 bairros oficiais mas muitos subdistritos são registrados
      separadamente. Top 50 cobre os pontos críticos sem inflar o JSON.
    """
    base = (
        df
        # Filtrar SP capital pelo código usado nos dados da SSP
        .filter(F.col("municipio") == MUNICIPIO_SP_CAPITAL)
        .filter(F.col("bairro").isNotNull())
        # Excluir "NAO INFORMADO" do ranking — distorceria a análise
        .filter(F.upper(F.col("bairro")) != "NAO INFORMADO")
        .groupBy("ano", "bairro", "tipo_crime")
        .agg(F.count("*").alias("total"))
    )

    window_rank = Window.partitionBy("ano", "tipo_crime").orderBy(F.desc("total"))

    return (
        base
        .withColumn("rank", F.rank().over(window_rank))
        .filter(F.col("rank") <= 50)
        .drop("rank")
        .orderBy("ano", F.desc("total"))
    )


def kpi_by_location_type(df: DataFrame) -> DataFrame:
    """
    Ocorrências por tipo de local (Via pública, Terminal/Estação, etc.)

    Responde à pergunta: onde acontecem mais subtrações?
    Útil para políticas públicas de segurança em locais específicos.
    """
    return (
        df
        .filter(F.col("tipo_local").isNotNull())
        .filter(F.upper(F.col("tipo_local")) != "NAO INFORMADO")
        .groupBy("ano", "tipo_local", "tipo_crime")
        .agg(F.count("*").alias("total"))
        .orderBy("ano", F.desc("total"))
    )


def kpi_by_brand(df: DataFrame) -> DataFrame:
    """
    Marcas de celular mais subtraídas por ano.

    Limitado ao top 15 por ano/tipo para evitar proliferação de
    marcas raras ("Outras" já agrupa o restante do pipeline clean.py).
    """
    base = (
        df
        .filter(F.col("marca").isNotNull())
        .filter(F.col("marca") != "Outras")
        .groupBy("ano", "marca", "tipo_crime")
        .agg(F.count("*").alias("total"))
    )

    window_rank = Window.partitionBy("ano", "tipo_crime").orderBy(F.desc("total"))

    return (
        base
        .withColumn("rank", F.rank().over(window_rank))
        .filter(F.col("rank") <= 15)
        .drop("rank")
        .orderBy("ano", F.desc("total"))
    )


def kpi_by_hour(df: DataFrame) -> DataFrame:
    """
    Distribuição das ocorrências pelas 24 horas do dia.

    Preencher horas sem ocorrências com 0 garante que o gráfico
    de barras no dashboard mostre todas as 24 horas, mesmo que
    às 4h da manhã não haja nenhuma ocorrência em algum ano.
    """
    # Criar uma grade completa de horas (0–23) × anos × tipo_crime
    # para garantir que todas as horas apareçam no gráfico
    anos_tipos = (
        df.select("ano", "tipo_crime").distinct()
    )
    horas = df.sparkSession.range(0, 24).withColumnRenamed("id", "hora")

    # Cross join para a grade completa — é pequena (24 × ~20 anos × 2 tipos = 960 linhas)
    grade_completa = anos_tipos.crossJoin(horas)

    base = (
        df
        .filter(F.col("hora").isNotNull())
        .filter(F.col("hora").between(0, 23))
        .groupBy("ano", "tipo_crime", "hora")
        .agg(F.count("*").alias("total"))
    )

    # Left join para preencher horas ausentes com 0
    return (
        grade_completa
        .join(base, on=["ano", "tipo_crime", "hora"], how="left")
        .fillna(0, subset=["total"])
        .orderBy("ano", "hora")
    )


def kpi_by_period(df: DataFrame) -> DataFrame:
    """
    Distribuição por período do dia (Madrugada / Manhã / Tarde / Noite).

    Versão simplificada do kpi_by_hour — mais legível em apresentações
    executivas e em KPI cards do dashboard.
    """
    return (
        df
        .filter(F.col("periodo").isNotNull())
        .groupBy("ano", "periodo", "tipo_crime")
        .agg(F.count("*").alias("total"))
        .orderBy("ano", "periodo")
    )


def salvar_kpi(df: DataFrame, nome_tabela: str) -> None:
    """Salva um KPI como Parquet na pasta de saída."""
    saida = str(PARQUET_KPIS / nome_tabela)
    (
        df
        .write
        .mode("overwrite")
        .parquet(saida)
    )
    n = df.count()
    print(f"  OK {nome_tabela}: {n:,} linhas -> {saida}")


def main() -> None:
    print("=" * 60)
    print("SafeCity SP — Etapa 3: Cálculo dos KPIs")
    print("=" * 60)

    if not PARQUET_CLEAN.exists():
        raise FileNotFoundError(
            f"Parquet limpo não encontrado em {PARQUET_CLEAN}\n"
            f"Execute clean.py primeiro."
        )

    spark = criar_spark_session()

    print(f"\nLendo Parquet limpo de: {PARQUET_CLEAN}")
    df = spark.read.parquet(str(PARQUET_CLEAN))

    # Cache do DataFrame limpo — será lido 8 vezes (uma por KPI).
    # Sem cache, o Spark re-executaria todo o plano de leitura + filtros
    # do Parquet para cada agregação, multiplicando o tempo de execução.
    df.cache()
    total = df.count()  # ação que materializa o cache
    print(f"Total de linhas no dataset limpo: {total:,} (em cache)")

    PARQUET_KPIS.mkdir(parents=True, exist_ok=True)
    print("\nComputando KPIs...")

    kpis = {
        TABELAS_KPI["annual_summary"]:  kpi_annual_summary(df),
        TABELAS_KPI["monthly"]:         kpi_monthly(df),
        TABELAS_KPI["municipality"]:    kpi_by_municipality(df),
        TABELAS_KPI["bairro"]:          kpi_by_bairro(df),
        TABELAS_KPI["location_type"]:   kpi_by_location_type(df),
        TABELAS_KPI["brand"]:           kpi_by_brand(df),
        TABELAS_KPI["hour"]:            kpi_by_hour(df),
        TABELAS_KPI["period"]:          kpi_by_period(df),
    }

    for nome, df_kpi in kpis.items():
        salvar_kpi(df_kpi, nome)

    df.unpersist()  # liberar cache após uso
    spark.stop()
    print("\nOK Etapa 3 concluída — execute load_supabase.py para continuar")


if __name__ == "__main__":
    main()
