"""
run_pipeline.py — Entry point do pipeline completo SafeCity SP

Executa as 4 etapas em sequência:
  1. ingest.py        -> lê xlsx, salva Parquet bruto
  2. clean.py         -> limpa, normaliza, salva Parquet limpo
  3. compute_kpis.py  -> agrega 8 KPIs, salva Parquet por tabela
  4. load_supabase.py -> carrega KPIs no Supabase

Por que executar como subprocessos em vez de importar as funções?
  Cada etapa cria sua própria SparkSession. O Spark não suporta múltiplas
  sessões simultâneas no mesmo processo JVM. Usar subprocess.run garante
  que cada SparkSession é criada e destruída de forma isolada, evitando
  conflitos de configuração entre etapas.

Uso:
  python spark/run_pipeline.py              # pipeline completo
  python spark/run_pipeline.py --from clean # reiniciar a partir da etapa clean
"""

import sys
import time
import subprocess
from pathlib import Path

# Etapas do pipeline em ordem de execução
ETAPAS = [
    ("ingest",        "ingest.py",         "Leitura dos xlsx -> Parquet bruto"),
    ("clean",         "clean.py",          "Limpeza e normalização -> Parquet limpo"),
    ("compute_kpis",  "compute_kpis.py",   "Cálculo das 8 tabelas KPI -> Parquet KPI"),
    ("load_supabase", "load_supabase.py",  "Carga dos KPIs -> Supabase"),
]

SPARK_DIR = Path(__file__).parent


def executar_etapa(nome: str, script: str, descricao: str) -> bool:
    """
    Executa uma etapa do pipeline como subprocesso.

    Retorna True se a etapa foi bem-sucedida, False se falhou.
    """
    caminho_script = SPARK_DIR / script

    print(f"\n{'='*60}")
    print(f"ETAPA: {nome.upper()}")
    print(f"Descrição: {descricao}")
    print(f"Script: {caminho_script}")
    print("=" * 60)

    inicio = time.time()

    # Usar o mesmo interpretador Python que executou este script.
    # MOTIVO: garante que as mesmas dependências (pyspark, pandas, etc.)
    # instaladas no ambiente atual sejam usadas nas sub-etapas
    resultado = subprocess.run(
        [sys.executable, str(caminho_script)],
        cwd=str(SPARK_DIR),
    )

    duracao = time.time() - inicio

    if resultado.returncode == 0:
        print(f"\nOK Etapa '{nome}' concluída em {duracao:.1f}s")
        return True
    else:
        print(f"\n✗ Etapa '{nome}' falhou (código {resultado.returncode}) após {duracao:.1f}s")
        return False


def main() -> None:
    print("+======================================================╗")
    print("|     SafeCity SP — Pipeline Completo de Dados         |")
    print("+======================================================╝")

    # Suporte a --from <etapa> para reiniciar o pipeline a partir de uma etapa específica.
    # Útil quando ingest.py já rodou (lento ~10min) e só precisamos recomputar KPIs.
    etapa_inicio = None
    if "--from" in sys.argv:
        idx = sys.argv.index("--from")
        if idx + 1 < len(sys.argv):
            etapa_inicio = sys.argv[idx + 1].lower()

    etapas_para_rodar = ETAPAS
    if etapa_inicio:
        nomes = [e[0] for e in ETAPAS]
        if etapa_inicio not in nomes:
            print(f"\n✗ Etapa '{etapa_inicio}' não encontrada.")
            print(f"  Etapas disponíveis: {', '.join(nomes)}")
            sys.exit(1)
        idx_inicio = nomes.index(etapa_inicio)
        etapas_para_rodar = ETAPAS[idx_inicio:]
        print(f"\nIniciando a partir da etapa: {etapa_inicio}")

    print(f"\nEtapas a executar: {[e[0] for e in etapas_para_rodar]}")

    inicio_total = time.time()
    falhou_em = None

    for nome, script, descricao in etapas_para_rodar:
        sucesso = executar_etapa(nome, script, descricao)
        if not sucesso:
            falhou_em = nome
            break

    duracao_total = time.time() - inicio_total

    print(f"\n{'='*60}")
    if falhou_em:
        print(f"✗ Pipeline interrompido na etapa '{falhou_em}'")
        print(f"  Para reiniciar a partir desta etapa: python run_pipeline.py --from {falhou_em}")
        sys.exit(1)
    else:
        print(f"OK Pipeline completo em {duracao_total/60:.1f} minutos")
        print("  Próximo passo: abrir web/index.html ou deploy no Vercel")


if __name__ == "__main__":
    main()
