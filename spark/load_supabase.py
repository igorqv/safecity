"""
load_supabase.py — Carga das tabelas KPI no Supabase via REST API

Etapa 4 do pipeline SafeCity.

Por que usar o cliente supabase-py em vez de SQLAlchemy/psycopg2?
  - psycopg2-binary não tem wheel pré-compilado para Python 3.14+
  - supabase-py usa httpx internamente (puro Python) — compatível com qualquer versão
  - Para tabelas com < 10k linhas, a REST API é suficiente e mais simples
  - Não precisamos de JDBC JAR nem driver PostgreSQL instalado no sistema
"""

import sys
import math
from pathlib import Path

import pandas as pd
from supabase import create_client, Client

sys.path.insert(0, str(Path(__file__).parent))
from config import (
    PARQUET_KPIS, TABELAS_KPI,
    SUPABASE_URL, SUPABASE_SERVICE_KEY,
)

# Tamanho do batch para cada upsert.
# MOTIVO: a REST API do Supabase tem limite de payload por request (~1MB).
# 500 linhas por batch fica confortavelmente abaixo desse limite.
BATCH_SIZE = 500


def conectar() -> Client:
    """Cria e retorna o cliente Supabase autenticado com a service_role key."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise ValueError(
            "SUPABASE_URL ou SUPABASE_SERVICE_KEY não configurados.\n"
            "Preencha o arquivo .env com as credenciais do Supabase."
        )
    # service_role bypassa o RLS — necessário para DELETE + INSERT em dados públicos
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def truncar_tabela(supabase: Client, tabela: str) -> None:
    """
    Remove todos os registros da tabela antes da carga.

    Por que DELETE em vez de TRUNCATE via REST?
      A REST API do Supabase não expõe TRUNCATE diretamente.
      DELETE com filtro 'id > 0' remove todas as linhas preservando
      o schema, índices e constraints definidos no schema.sql.
    """
    # Deletar onde id > 0 — como todas as linhas têm id SERIAL > 0, remove tudo
    supabase.table(tabela).delete().gt("id", 0).execute()


def carregar_kpi(supabase: Client, nome_tabela: str) -> int:
    """
    Lê o Parquet do KPI e insere na tabela Supabase em batches.

    Retorna o número de linhas inseridas.
    """
    parquet_path = PARQUET_KPIS / nome_tabela

    if not parquet_path.exists():
        print(f"  AVISO Parquet não encontrado: {parquet_path} — pulando")
        return 0

    pdf = pd.read_parquet(str(parquet_path))
    n_linhas = len(pdf)

    if n_linhas == 0:
        print(f"  AVISO {nome_tabela}: Parquet vazio — pulando")
        return 0

    # Converter para lista de dicts — formato esperado pelo supabase-py
    registros = pdf.to_dict(orient="records")

    # Limpar valores NaN/NaT que o JSON não consegue serializar
    # MOTIVO: pandas usa float NaN para nulos numéricos, mas JSON só entende null
    for r in registros:
        for k, v in r.items():
            if isinstance(v, float) and math.isnan(v):
                r[k] = None
            # Converter numpy int/float para tipos Python nativos para serialização JSON
            elif hasattr(v, 'item'):
                r[k] = v.item()

    # Truncar antes de inserir — garante idempotência (rodar duas vezes não duplica)
    truncar_tabela(supabase, nome_tabela)

    # Inserir em batches
    for i in range(0, len(registros), BATCH_SIZE):
        batch = registros[i:i + BATCH_SIZE]
        supabase.table(nome_tabela).insert(batch).execute()

    return n_linhas


def main() -> None:
    print("=" * 60)
    print("SafeCity SP — Etapa 4: Carga no Supabase")
    print("=" * 60)

    if not PARQUET_KPIS.exists():
        raise FileNotFoundError(
            f"Pasta de KPIs não encontrada: {PARQUET_KPIS}\n"
            f"Execute compute_kpis.py primeiro."
        )

    supabase = conectar()
    print(f"\nConexão estabelecida: {SUPABASE_URL}")

    total_linhas = 0
    resultados = []

    for chave, nome_tabela in TABELAS_KPI.items():
        print(f"\n-> {nome_tabela}")
        try:
            n = carregar_kpi(supabase, nome_tabela)
            total_linhas += n
            resultados.append((nome_tabela, n, "OK"))
            print(f"  OK {n:,} linhas inseridas")
        except Exception as e:
            resultados.append((nome_tabela, 0, f"✗ {e}"))
            print(f"  ✗ Erro: {e}")

    # Relatório final
    print("\n" + "=" * 60)
    print("Relatório de carga:")
    print(f"{'Tabela':<30} {'Linhas':>8}  Status")
    print("-" * 60)
    for tabela, n, status in resultados:
        print(f"{tabela:<30} {n:>8,}  {status}")
    print("-" * 60)
    print(f"{'TOTAL':<30} {total_linhas:>8,}")

    erros = [r for r in resultados if "✗" in r[2]]
    if erros:
        print(f"\nAVISO {len(erros)} tabela(s) com erro")
        sys.exit(1)

    print("\nOK Etapa 4 concluída — pipeline completo!")


if __name__ == "__main__":
    main()
