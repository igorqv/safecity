# SafeCity SP — Docker Setup para Pipeline PySpark

## Pré-requisitos

Antes de rodar o Docker, certifique-se de ter:

- **Docker**: v20.10+ instalado ([download](https://www.docker.com/products/docker-desktop))
- **Docker Compose**: v2.0+ (vem com Docker Desktop)
- **Arquivo `.env`**: copiar de `.env.example` e preencher credenciais Supabase

### Validar instalação

```bash
docker --version      # Docker 20.10.0 or higher
docker-compose version  # Docker Compose 2.0.0 or higher
```

---

## Setup — Primeira Execução

### 1. Copiar arquivo de variáveis de ambiente

```bash
cp .env.example .env
```

Editar `.env` com as credenciais reais do Supabase:

```bash
SUPABASE_URL=https://your-project-ref.supabase.co
SUPABASE_SERVICE_KEY=eyJ...  # copiar de Supabase > Settings > API
SUPABASE_DB_HOST=db.your-project-ref.supabase.co
SUPABASE_DB_PASSWORD=...     # password do projeto
```

**⚠️ IMPORTANTE:** Nunca commitar `.env` com credenciais reais. Mantém sempre em `.gitignore`.

### 2. Preparar dados brutos

Baixar arquivos Excel da SSP-SP e colocá-los em `raw/`:

```bash
mkdir -p raw
# Copiar CelularesSubtraidos_2017.xlsx até CelularesSubtraidos_2026.xlsx para raw/
ls raw/
# CelularesSubtraidos_2017.xlsx
# CelularesSubtraidos_2018.xlsx
# ... até 2026
```

### 3. Criar schema no Supabase

Executar o DDL SQL no Supabase SQL Editor:

```bash
# Copiar conteúdo de sql/schema.sql
# Abrir: Supabase > SQL Editor > New Query
# Colar e executar
```

Ou via CLI (se tiver supabase-cli instalado):

```bash
supabase db push --db-url "postgresql://postgres:password@db.host:5432/postgres"
```

### 4. Build da imagem Docker

```bash
# Build local — cria imagem 'safecity-pipeline'
docker-compose build
```

Isso vai:
- Baixar Python 3.11-slim como base
- Instalar Java 17 e dependências
- Instalar pacotes Python (pyspark, pandas, etc.)
- Copiar código Spark

Primeira build leva ~5-10 minutos (depende da conexão).

---

## Execução

### Pipeline Completo

```bash
# Executa: ingest → clean → compute_kpis → load_supabase
docker-compose up
```

Outputs esperados:

```
safecity-pipeline  | ======================================================
safecity-pipeline  |     SafeCity SP — Pipeline Completo de Dados
safecity-pipeline  | ======================================================
safecity-pipeline  | Etapas a executar: ['ingest', 'clean', 'compute_kpis', 'load_supabase']
safecity-pipeline  |
safecity-pipeline  | ============================================================
safecity-pipeline  | ETAPA: INGEST
...
safecity-pipeline  | OK Pipeline completo em 12.5 minutos
```

### Executar Uma Etapa Específica

```bash
# Rodar só 'ingest'
docker-compose run safecity-pipeline python ingest.py

# Rodar só 'clean' (assumindo ingest já rodou)
docker-compose run safecity-pipeline python clean.py

# Reiniciar pipeline a partir de uma etapa (usando --from)
docker-compose run safecity-pipeline python run_pipeline.py --from clean
```

### Rodar em Background (detached mode)

```bash
# Iniciar pipeline em background
docker-compose up -d

# Ver logs em tempo real
docker-compose logs -f

# Parar container
docker-compose stop

# Reiniciar container
docker-compose start
```

---

## Debugging

### Ver logs do container

```bash
# Todos os logs
docker-compose logs safecity-pipeline

# Últimas 50 linhas
docker-compose logs --tail 50 safecity-pipeline

# Streaming (follow)
docker-compose logs -f safecity-pipeline
```

### Entrar no container (bash)

```bash
# Interativo
docker-compose run safecity-pipeline /bin/bash

# Comando único
docker-compose run safecity-pipeline ls -la /app/parquet
```

### Limpar volumes (dados processados)

```bash
# Remove containers + volumes (CUIDADO — apaga parquet/)
docker-compose down -v

# Só containers (mantém volumes)
docker-compose down
```

---

## Troubleshooting

### Erro: "Cannot connect to Docker daemon"

```bash
# Verificar se Docker está rodando
docker ps

# Se não, iniciar Docker Desktop
# Windows: procurar "Docker Desktop" no menu Iniciar
# Mac: abrir Applications > Docker.app
```

### Erro: "out of memory" ou "OOM killer"

```bash
# Aumentar memória alocada no .env
SPARK_DRIVER_MEMORY=8g  # ou 16g se tiver RAM suficiente

# Rebuild
docker-compose build --no-cache
```

### Erro: "JDBC connection refused" (não conecta ao Supabase)

```bash
# Validar credenciais no .env
cat .env | grep SUPABASE_DB

# Testar conexão manualmente
docker-compose run safecity-pipeline python -c "
import psycopg2
conn = psycopg2.connect('postgresql://postgres:password@host:5432/postgres')
print('OK')
"
```

### Erro: "No such file or directory: CelularesSubtraidos_2017.xlsx"

```bash
# Verificar se raw/ tem os arquivos
ls -la raw/

# Se vazio, baixar de: https://www.ssp.sp.gov.br/estatistica/consultas
```

---

## Performance & Otimizações

| Métrica | Padrão | Observação |
|---------|--------|-----------|
| **Build time** | ~5-10min | Primeiro build é lento; rebuilds são mais rápidos (cache) |
| **Pipeline runtime** | ~20-30min | Depende de SPARK_DRIVER_MEMORY e velocidade do Supabase |
| **Tamanho da imagem** | ~1.5GB | Java + Python + Spark é pesado; aceitável para desenvolvimento |
| **Volume de parquet/** | ~2-5GB | Checkpoints intermediários; pode limpar após sucesso |

### Otimizações possíveis

```dockerfile
# Para reduzir tempo de build em refits frequentes:
# 1. Usar BUILDKIT (mais rápido)
DOCKER_BUILDKIT=1 docker-compose build

# 2. Aumentar memória Spark se tiver RAM disponível
SPARK_DRIVER_MEMORY=16g docker-compose up

# 3. Diminuir shuffle partitions se dados forem pequenos (< 100GB)
SPARK_CONF_SPARK_SHUFFLE_PARTITIONS=4
```

---

## Próximos Passos

Após pipeline rodar com sucesso:

1. ✅ Verificar dados no Supabase
   ```sql
   SELECT count(*) FROM kpi_annual_summary;
   SELECT * FROM kpi_by_brand LIMIT 10;
   ```

2. ✅ Implementar `web/js/dashboard.js` (carregar dados do Supabase)

3. ✅ Deploy no Vercel
   ```bash
   vercel --cwd web/
   ```

---

## Referências

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [PySpark Docker Best Practices](https://spark.apache.org/docs/latest/index.html)
- [Supabase Connection Guide](https://supabase.com/docs/guides/database/connecting-to-postgres)
