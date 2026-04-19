# SafeCity SP — Pipeline Incremental por Ano

## Visão Geral

O pipeline tradicional (`run_pipeline.py`) processa **todos os 10 anos de uma vez** (2017–2026). Isso é rápido, mas torna difícil validar dados parciais ou recuperar-se de erros.

O **pipeline incremental** (`run_pipeline_yearly.py`) processa um ano por vez, permitindo:
- ✅ Validar dados de cada ano antes de enviar ao Supabase
- ✅ Recuperar-se de erros sem reprocessar tudo
- ✅ Fazer uploads parciais para visualizar progresso
- ✅ Reprocessar anos específicos se necessário

---

## Como Usar

### 1. Ingest Completo (primeira vez)

Não muda — processa todos os 10 anos de uma vez (está em `parquet/raw/ANO=*/`):

```bash
docker-compose run safecity-pipeline python ingest.py
```

**Saída esperada:** `parquet/raw/ANO=2017/`, `parquet/raw/ANO=2018/`, ..., `parquet/raw/ANO=2026/`

### 2. Pipeline Incremental

#### Processar todos os anos sequencialmente:

```bash
docker-compose run safecity-pipeline python run_pipeline_yearly.py
```

Isso vai:
1. Descobrir anos disponíveis: `[2017, 2018, ..., 2026]`
2. Para cada ano, executar: `clean.py` → `compute_kpis.py` → `load_supabase.py`
3. Após cada ano, fazer upload dos KPIs daquele ano ao Supabase

#### Processar apenas anos específicos:

```bash
# Processar apenas 2017 (validação)
docker-compose run safecity-pipeline python run_pipeline_yearly.py 2017

# Processar 2017–2019 (primeiros 3 anos)
docker-compose run safecity-pipeline python run_pipeline_yearly.py 2017 2018 2019

# Processar apenas 2024 (reprocessar um ano com bug fix)
docker-compose run safecity-pipeline python run_pipeline_yearly.py 2024
```

#### Retomar depois de um erro:

Se o pipeline falhar no ano 2020, você pode:

```bash
# Ver qual ano falhou (logs mostram)
# Corrigir o bug em clean.py ou compute_kpis.py
docker-compose build  # Reconstruir imagem se alterou o código

# Reprocessar apenas os anos que falharam
docker-compose run safecity-pipeline python run_pipeline_yearly.py 2020 2021 2022 2023 2024 2025 2026
```

---

## Arquitetura do Pipeline Incremental

```
parquet/raw/              (saída do ingest.py, particionado por ANO)
├── ANO=2017/
├── ANO=2018/
├── ...
└── ANO=2026/

run_pipeline_yearly.py
├─ Loop: for each year in [2017, 2018, ..., 2026]
│  └─ clean.py (filtra ANO, remove datas inválidas)
│     └─ parquet/clean/ANO=2017/, etc.
│  └─ compute_kpis.py (agrega por year)
│     └─ parquet/kpis/kpi_by_brand.parquet, etc.
│  └─ load_supabase.py (upsert ano por ano)
│     └─ Supabase: INSERT/UPDATE das linhas de cada ano
│        (dados de 2017 estão lá, 2018 é adicionado, etc.)
```

---

## Comparação: Completo vs Incremental

| Aspecto | `run_pipeline.py` | `run_pipeline_yearly.py` |
|---------|------------------|------------------------|
| **Tempo** | ~30-40 min | ~3-5 min/ano (3-5 h total) |
| **Failover** | Tudo falha, recomeça do zero | Falha no ano X, retoma do X |
| **Validação** | Só ao final | A cada ano (é possível pausar) |
| **Supabase** | Todos os dados 1 vez | Incremental (2017, depois +2018, etc.) |
| **Uso** | Ideal para primeira carga | Ideal para reprocessamento e debug |

---

## Exemplos de Fluxo de Trabalho

### Cenário 1: Primeira Carga (Validação Graduada)

```bash
# 1. Fazer ingest completo (rápido, ~25 min)
docker-compose run safecity-pipeline python ingest.py

# 2. Processar 2017 (mais antigo, teste)
docker-compose run safecity-pipeline python run_pipeline_yearly.py 2017

# 3. Validar dados de 2017 no Supabase (consultar base)
# → SELECT COUNT(*) FROM kpi_annual_summary WHERE ano = 2017;

# 4. Se OK, processar próximos anos
docker-compose run safecity-pipeline python run_pipeline_yearly.py 2018 2019 2020

# 5. Finalizar
docker-compose run safecity-pipeline python run_pipeline_yearly.py 2021 2022 2023 2024 2025 2026
```

### Cenário 2: Bug Fix em 2024

```bash
# 1. Descobrir que há erro nas datas de 2024
# 2. Corrigir clean.py
# 3. Reconstruir Docker
docker-compose build

# 4. Reprocessar só 2024 (mantém 2017–2023 intactos no Supabase)
docker-compose run safecity-pipeline python run_pipeline_yearly.py 2024

# 5. Continuar anos restantes
docker-compose run safecity-pipeline python run_pipeline_yearly.py 2025 2026
```

### Cenário 3: Paralelo com Dashboard

```bash
# Terminal 1: Pipeline (iniciar com 2017)
docker-compose run safecity-pipeline python run_pipeline_yearly.py 2017

# Terminal 2: Dashboard (assim que 2017 subir para Supabase, já pode testar)
python -m http.server 8000 --directory web
# Abrir http://localhost:8000/index.html (filtra dados de 2017)

# Terminal 1: Continuar com próximos anos
docker-compose run safecity-pipeline python run_pipeline_yearly.py 2018 2019
```

---

## Troubleshooting

### "Nenhum ano encontrado em parquet/raw/"
- Solução: Execute `ingest.py` primeiro

### "Ano 2024 com falha na etapa clean"
- Verificar logs: `docker-compose logs safecity-pipeline | grep 2024`
- Corrigir clean.py se necessário
- Reexecutar: `python run_pipeline_yearly.py 2024`

### Supabase tem dados de 2017–2019, preciso adicionar 2020+
- Executar: `python run_pipeline_yearly.py 2020 2021 2022 2023 2024 2025 2026`
- O upsert adicionará linhas novas sem remover as antigas

---

## Performance

**Tempos esperados (dados reais, 4M linhas totais):**

| Etapa | Tempo/Ano |
|-------|-----------|
| clean.py | ~10-15 sec |
| compute_kpis.py | ~5-10 sec |
| load_supabase.py | ~2-5 sec (rede) |
| **Total/Ano** | **~20-30 sec** |
| **10 Anos** | **~3-5 horas** |

*Nota: Tempos podem variar conforme CPU, RAM, e latência de rede.*

---

## Próximas Melhorias

- [ ] Suporte a `--parallel` para processar múltiplos anos em paralelo
- [ ] Dry-run mode (validar sem fazer upload)
- [ ] Rollback automático se upload falhar
- [ ] Health check de dados (contar linhas, validar KPIs)
