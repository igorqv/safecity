#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────────
# check-env.sh — Validar setup antes de rodar o pipeline
#
# Uso:
#   bash check-env.sh
# ──────────────────────────────────────────────────────────────────────────────

set -e

echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║         SafeCity SP — Validação de Ambiente (Pre-flight Check)             ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""

# ─── Validar Docker ────────────────────────────────────────────────────────────

echo "🔍 Verificando Docker..."

if ! command -v docker &> /dev/null; then
    echo "❌ Docker não encontrado. Instale em: https://www.docker.com/products/docker-desktop"
    exit 1
fi

docker_version=$(docker --version | awk '{print $3}' | cut -d',' -f1)
echo "   ✓ Docker $docker_version"

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose não encontrado."
    exit 1
fi

compose_version=$(docker-compose version | head -1)
echo "   ✓ $compose_version"

# ─── Validar .env ────────────────────────────────────────────────────────────

echo ""
echo "🔍 Verificando arquivo .env..."

if [ ! -f .env ]; then
    echo "❌ Arquivo .env não existe."
    echo "   Execute: cp .env.example .env"
    exit 1
fi

echo "   ✓ .env existe"

# Função para validar variável
check_var() {
    local var_name=$1
    local value=$(grep "^$var_name=" .env | cut -d'=' -f2- | tr -d ' ')

    if [ -z "$value" ] || [ "$value" = "your-project-ref.supabase.co" ]; then
        echo "   ⚠️  $var_name não configurado (usar valor placeholder)"
        return 1
    fi
    echo "   ✓ $var_name configurado"
    return 0
}

check_var "SUPABASE_URL"
check_var "SUPABASE_SERVICE_KEY"
check_var "SUPABASE_DB_HOST"
check_var "SUPABASE_DB_USER"
check_var "SUPABASE_DB_PASSWORD"

# ─── Validar estrutura de diretórios ──────────────────────────────────────────

echo ""
echo "🔍 Verificando diretórios..."

required_dirs=("spark" "sql" "web")
for dir in "${required_dirs[@]}"; do
    if [ -d "$dir" ]; then
        echo "   ✓ $dir/ existe"
    else
        echo "❌ Diretório $dir/ não encontrado"
        exit 1
    fi
done

# ─── Validar arquivos-chave ───────────────────────────────────────────────────

echo ""
echo "🔍 Verificando arquivos-chave..."

key_files=(
    "spark/Dockerfile"
    "spark/requirements.txt"
    "spark/run_pipeline.py"
    "spark/ingest.py"
    "spark/clean.py"
    "spark/compute_kpis.py"
    "spark/load_supabase.py"
    "sql/schema.sql"
    "docker-compose.yml"
)

for file in "${key_files[@]}"; do
    if [ -f "$file" ]; then
        echo "   ✓ $file"
    else
        echo "❌ Arquivo não encontrado: $file"
        exit 1
    fi
done

# ─── Validar dados brutos (opcional) ───────────────────────────────────────────

echo ""
echo "🔍 Verificando dados brutos..."

if [ -d "raw" ] && [ "$(ls -A raw)" ]; then
    num_files=$(ls raw/*.xlsx 2>/dev/null | wc -l)
    echo "   ✓ Pasta raw/ tem $num_files arquivo(s) .xlsx"
else
    echo "   ⚠️  Pasta raw/ vazia ou não existe"
    echo "      Baixe os arquivos da SSP-SP: https://www.ssp.sp.gov.br/estatistica/consultas"
fi

# ─── Resumo final ────────────────────────────────────────────────────────────

echo ""
echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║                   ✓ Setup Validado com Sucesso                             ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Próximos passos:"
echo "  1. Se ainda não fez, execute: docker-compose build"
echo "  2. Adicione arquivos .xlsx à pasta raw/"
echo "  3. Crie o schema no Supabase (sql/schema.sql)"
echo "  4. Execute: docker-compose up"
echo ""
