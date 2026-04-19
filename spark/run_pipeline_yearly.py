"""
run_pipeline_yearly.py — Processamento incremental por ano

Executa o pipeline (clean → compute_kpis → load_supabase) para cada ano,
permitindo validar e fazer upload de dados parciais.

Uso:
  python spark/run_pipeline_yearly.py              # Processa todos os anos
  python spark/run_pipeline_yearly.py 2024         # Processa apenas 2024
  python spark/run_pipeline_yearly.py 2017 2018    # Processa 2017 e 2018
"""

import sys
import time
import subprocess
from pathlib import Path
from typing import List

# Importar config para acessar PARQUET_RAW
sys.path.insert(0, str(Path(__file__).parent))
from config import PARQUET_RAW


def descobrir_anos_disponiveis() -> List[int]:
    """Descobrir quais anos estão disponíveis no parquet/raw/"""
    if not PARQUET_RAW.exists():
        raise FileNotFoundError(f"Pasta {PARQUET_RAW} não encontrada. Execute ingest.py primeiro.")

    # Buscar pastas particionadas por ANO (formato: ANO=2017, ANO=2018, ...)
    anos = []
    for item in PARQUET_RAW.iterdir():
        if item.is_dir() and item.name.startswith("ANO="):
            ano_str = item.name.replace("ANO=", "")
            try:
                ano = int(ano_str)
                anos.append(ano)
            except ValueError:
                pass  # Ignorar pastas que não seguem o padrão

    return sorted(anos)


def executar_etapa(ano: int, nome: str, script: str, descricao: str) -> bool:
    """Executa uma etapa do pipeline para um ano específico."""
    caminho_script = Path(__file__).parent / script

    print(f"\n{'='*60}")
    print(f"ANO {ano} — ETAPA: {nome.upper()}")
    print(f"Descrição: {descricao}")
    print(f"Script: {caminho_script}")
    print("=" * 60)

    inicio = time.time()

    # Passar o ano como variável de ambiente para que os scripts Python
    # possam filtrar por ano se desejarem
    env = {"SAFECITY_YEAR": str(ano)}

    resultado = subprocess.run(
        [sys.executable, str(caminho_script)],
        cwd=str(Path(__file__).parent),
        env={**dict(os.environ), **env} if 'os' in dir() else env,
    )

    duracao = time.time() - inicio

    if resultado.returncode == 0:
        print(f"\n✓ Ano {ano}, Etapa '{nome}' concluída em {duracao:.1f}s")
        return True
    else:
        print(f"\n✗ Ano {ano}, Etapa '{nome}' falhou (código {resultado.returncode}) após {duracao:.1f}s")
        return False


def processar_ano(ano: int) -> bool:
    """Processa todas as etapas (clean, compute_kpis, load_supabase) para um ano."""
    print(f"\n{'#'*60}")
    print(f"# PROCESSANDO ANO {ano}")
    print(f"{'#'*60}")

    etapas = [
        ("clean", "clean.py", "Limpeza e normalização"),
        ("compute_kpis", "compute_kpis.py", "Cálculo de KPIs"),
        ("load_supabase", "load_supabase.py", "Carga no Supabase"),
    ]

    for nome, script, descricao in etapas:
        sucesso = executar_etapa(ano, nome, script, descricao)
        if not sucesso:
            print(f"\n✗ Pipeline interrompido para o ano {ano} na etapa '{nome}'")
            print(f"  Para reexecutar apenas este ano: python run_pipeline_yearly.py {ano}")
            return False

    print(f"\n✓ Ano {ano} processado com sucesso!")
    return True


def main() -> None:
    print("+======================================================╗")
    print("|   SafeCity SP — Pipeline Incremental por Ano        |")
    print("+======================================================╝")

    # Se fornecido como argumento, processar apenas esses anos
    anos_parametrizados = []
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            try:
                anos_parametrizados.append(int(arg))
            except ValueError:
                print(f"Aviso: argumento '{arg}' não é um ano válido. Ignorando.")

    # Descobrir anos disponíveis
    print("\nDescubrindo anos disponíveis em parquet/raw/...")
    anos_disponiveis = descobrir_anos_disponiveis()

    if not anos_disponiveis:
        print("✗ Nenhum ano encontrado em parquet/raw/")
        print("  Execute ingest.py primeiro.")
        sys.exit(1)

    print(f"✓ Anos disponíveis: {anos_disponiveis}")

    # Determinar quais anos processar
    if anos_parametrizados:
        # Filtrar apenas os anos fornecidos que existem
        anos_para_processar = [a for a in anos_parametrizados if a in anos_disponiveis]
        if not anos_para_processar:
            print("✗ Nenhum dos anos fornecidos está disponível.")
            sys.exit(1)
        print(f"✓ Processando anos especificados: {anos_para_processar}")
    else:
        anos_para_processar = anos_disponiveis
        print(f"✓ Processando todos os anos: {anos_para_processar}")

    # Processar cada ano
    inicio_total = time.time()
    anos_sucesso = []
    anos_falha = []

    for ano in anos_para_processar:
        sucesso = processar_ano(ano)
        if sucesso:
            anos_sucesso.append(ano)
        else:
            anos_falha.append(ano)

    duracao_total = time.time() - inicio_total

    # Resumo final
    print(f"\n{'='*60}")
    print("RESUMO FINAL")
    print("=" * 60)
    print(f"✓ Anos processados com sucesso: {anos_sucesso}")
    if anos_falha:
        print(f"✗ Anos com falha: {anos_falha}")
    print(f"  Tempo total: {duracao_total/60:.1f} minutos")

    if anos_falha:
        print(f"\nPróximo passo: corrigir o erro e reexecutar os anos com falha:")
        print(f"  python run_pipeline_yearly.py {' '.join(map(str, anos_falha))}")
        sys.exit(1)
    else:
        print(f"\n✓ Pipeline completo para todos os anos!")
        print("  Próximo passo: testar o dashboard web com os dados no Supabase")
        sys.exit(0)


if __name__ == "__main__":
    import os
    main()
