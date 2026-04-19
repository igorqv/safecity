# PRD — SafeCity SP: Dashboard de Roubos de Celulares

**Versão:** 1.0  
**Data:** 2026-04-19  
**Autor:** igorqv  
**Status:** Aprovado para desenvolvimento

---

## 1. Visão Geral do Produto

### 1.1 Contexto de Negócio

O estado de São Paulo registra centenas de milhares de roubos e furtos de celulares por ano. A Secretaria de Segurança Pública (SSP-SP) disponibiliza publicamente os boletins de ocorrência em formato Excel, mas os dados brutos são inacessíveis ao cidadão comum — planilhas gigantes sem visualização, sem tendência, sem contexto geográfico.

**SafeCity SP** transforma essa massa de dados (2017–2026, ~4,08 milhões de registros) em um dashboard público interativo, mostrando onde, quando e como os celulares são subtraídos em SP.

### 1.2 Objetivo

Criar uma pipeline de dados com PySpark que processa os arquivos brutos da SSP-SP, computa KPIs de segurança pública e publica um dashboard leve no Vercel — acessível a qualquer pessoa, sem necessidade de login.

### 1.3 Fonte de Dados

- **URL oficial:** https://www.ssp.sp.gov.br/estatistica/consultas
- **Arquivos:** `CelularesSubtraidos_YYYY.xlsx` (2017–2026)
- **Localização local:** `raw/`
- **Aba de interesse:** `CELULAR_YYYY` (cada arquivo tem 3 abas: dados, dicionário, metodologia)
- **Volume:** ~4,08 milhões de linhas, 46 colunas

---

## 2. Dados de Entrada

### 2.1 Schema das Colunas (CELULAR_YYYY)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ID_DELEGACIA` | int | ID da delegacia registrante |
| `NOME_DEPARTAMENTO` | string | Departamento policial |
| `NOME_SECCIONAL` | string | Seccional policial |
| `NOME_DELEGACIA` | string | Nome da delegacia |
| `NOME_MUNICIPIO` | string | Município da delegacia registrante |
| `ANO_BO` | int | Ano do boletim de ocorrência |
| `NUM_BO` | int | Número do BO |
| `NOME_DEPARTAMENTO_CIRC` | string | Departamento da circunscrição (local do crime) |
| `NOME_SECCIONAL_CIRC` | string | Seccional da circunscrição |
| `NOME_DELEGACIA_CIRC` | string | Delegacia da circunscrição |
| `NOME_MUNICIPIO_CIRC` | string | **Município do crime** (usar este para análise) |
| `DATA_OCORRENCIA_BO` | date | Data da ocorrência |
| `HORA_OCORRENCIA` | time | Hora da ocorrência |
| `DESCRICAO_APRESENTACAO` | string | Como foi registrado o BO |
| `DATAHORA_REGISTRO_BO` | datetime | Quando foi registrado |
| `DATA_COMUNICACAO_BO` | date | Data de comunicação |
| `DATAHORA_IMPRESSAO_BO` | datetime | Data/hora de impressão |
| `DESCR_PERIODO` | string | Período do dia (manhã/tarde/noite) — frequentemente nulo |
| `AUTORIA_BO` | string | Autoria conhecida/desconhecida |
| `FLAG_INTOLERANCIA` | string | Flag de crime de intolerância (S/N) |
| `TIPO_INTOLERANCIA` | string | Tipo de intolerância (se aplicável) |
| `FLAG_FLAGRANTE` | string | Preso em flagrante (S/N) |
| `FLAG_STATUS` | string | Status do BO (CONSUMADO / TENTADO) |
| `DESC_LEI` | string | Título do Código Penal |
| `RUBRICA` | string | **Tipo do crime**: Furto (art. 155) / Roubo (art. 157) |
| `DESCR_CONDUTA` | string | Conduta detalhada (ex: "Furto qualificado") |
| `DESDOBRAMENTO` | string | Desdobramento legal |
| `CIRCUNSTANCIA` | string | Circunstâncias agravantes |
| `DESCR_TIPOLOCAL` | string | **Tipo de local** (Via pública, Terminal/Estação, etc.) |
| `DESCR_SUBTIPOLOCAL` | string | Subtipo do local |
| `CIDADE` | string | Cidade do crime |
| `BAIRRO` | string | **Bairro do crime** |
| `CEP` | int | CEP |
| `LOGRADOURO` | string | Rua/avenida |
| `NUMERO_LOGRADOURO` | int | Número |
| `LATITUDE` | float | **Latitude** (WGS84) |
| `LONGITUDE` | float | **Longitude** (WGS84) |
| `CONT_OBJETO` | int | Contador do objeto no BO |
| `DESCR_MODO_OBJETO` | string | Modo de subtração ("SUBTRAÍDO") |
| `DESCR_TIPO_OBJETO` | string | Tipo de objeto ("Telecomunicação") |
| `DESCR_SUBTIPO_OBJETO` | string | Subtipo ("Telefone celular") |
| `DESCR_UNIDADE` | string | Unidade |
| `QUANTIDADE_OBJETO` | int | Quantidade subtraída |
| `MARCA_OBJETO` | string | **Marca do celular** (Samsung, Apple, Motorola…) |
| `MES` | int | Mês (1–12) |
| `ANO` | int | Ano |

### 2.2 Problemas de Qualidade Identificados

| Problema | Coluna(s) | Tratamento |
|----------|-----------|------------|
| Strings com espaços em branco extras | Todas as strings | `.strip()` no PySpark |
| Encoding latin-1/cp1252 vs UTF-8 | Strings com acentos | Tratar no read com `encoding='latin-1'` |
| Valores "NULL" como string | `CIRCUNSTANCIA`, `DESDOBRAMENTO`, etc. | Converter para `None` |
| CEP inválido (ex: 0 ou 999) | `CEP` | Filtrar CEPs < 1000000 |
| Coordenadas nulas ou zeradas | `LATITUDE`, `LONGITUDE` | Filtrar lat/lon nulos para mapas |
| Marcas de celular inconsistentes | `MARCA_OBJETO` | Normalizar: "APPLE" / "apple" / "Apple" → "Apple" |
| `HORA_OCORRENCIA` como time object | `HORA_OCORRENCIA` | Extrair hora inteira para análise |
| `DESCR_PERIODO` frequentemente vazio | `DESCR_PERIODO` | Derivar da hora: 0-5=Madrugada, 6-11=Manhã, 12-17=Tarde, 18-23=Noite |

---

## 3. KPIs de Segurança Pública

### 3.1 KPIs Estratégicos (nível executivo)

| KPI | Descrição | Frequência |
|-----|-----------|------------|
| Total de Ocorrências | Soma de todos os registros por período | Anual / Mensal |
| Taxa Furto vs Roubo | % de furtos vs roubos no total | Anual |
| Variação YoY | Crescimento/queda % ano a ano | Anual |
| Top 10 Municípios | Municípios com mais ocorrências | Anual |
| Top 10 Bairros (Capital) | Bairros de SP com mais ocorrências | Anual |
| Pico de Horário | Hora do dia com mais crimes | Anual |
| Top 5 Marcas Roubadas | Marcas de celular mais subtraídas | Anual |
| Top 5 Tipos de Local | Onde mais acontece (via pública, metro, etc.) | Anual |

### 3.2 KPIs Táticos (análise operacional)

| KPI | Descrição |
|-----|-----------|
| Sazonalidade Mensal | Ocorrências por mês (jan–dez) agregado multi-ano |
| Distribuição por Período | Madrugada / Manhã / Tarde / Noite |
| Flagrante Rate | % de crimes com autor flagrado |
| Crimes Tentados vs Consumados | Split FLAG_STATUS |
| Distribuição por Delegacia Seccional | Calor por seccional |
| Evolução de Marcas | Ranking de marcas por ano (Apple subindo?) |

---

## 4. Arquitetura do Sistema

```
raw/*.xlsx
    │
    ▼  [PySpark - spark/]
┌─────────────────────────────┐
│  1. ingest.py               │  Lê 10 xlsx → DataFrame unificado
│  2. clean.py                │  Aplica regras de limpeza
│  3. compute_kpis.py         │  Agrega KPI tables
│  4. load_supabase.py        │  Upsert → Supabase (PostgreSQL)
└─────────────────────────────┘
    │
    ▼ [Supabase - PostgreSQL]
┌─────────────────────────────┐
│  kpi_monthly                │  ~1200 linhas
│  kpi_by_municipality        │  ~500 linhas
│  kpi_by_bairro              │  ~3000 linhas (top bairros SP)
│  kpi_by_location_type       │  ~50 linhas
│  kpi_by_brand               │  ~200 linhas
│  kpi_by_hour                │  ~24 linhas
│  kpi_annual_summary         │  ~10 linhas
│  kpi_by_period              │  ~40 linhas (periodo x ano)
└─────────────────────────────┘
    │
    ▼ [Vercel - web/]
┌─────────────────────────────┐
│  index.html                 │  Dashboard principal
│  js/dashboard.js            │  Plotly.js + Supabase REST
│  css/style.css              │  Layout responsivo
└─────────────────────────────┘
         ▲
         │ HTTP GET (REST API / anon key)
    [Browser do usuário]
```

**Decisão arquitetural chave:** O Supabase armazena apenas **tabelas agregadas de KPI** (total ~5000 linhas), NÃO os 4M de registros brutos. Isso garante:
- Dashboard leve e sem timeout no Vercel
- Queries instantâneas no browser
- Custo zero no Supabase free tier

---

## 5. Stack Técnico

### 5.1 Pipeline de Dados

| Componente | Tecnologia | Versão | Justificativa |
|------------|-----------|--------|---------------|
| Processamento | Apache PySpark | 3.5.x | 4M+ linhas exigem processamento distribuído |
| Leitura Excel | com.crealytics:spark-excel | 0.14.x | Lê xlsx diretamente no Spark |
| Orquestração | Script Python simples | 3.11+ | Sem necessidade de Airflow para carga única |
| Output intermédio | Parquet (local) | — | Checkpoint entre etapas |
| Banco destino | Supabase PostgreSQL | — | REST API nativa para frontend |
| Driver JDBC | PostgreSQL JDBC | 42.7.x | Para Spark escrever no Supabase |

### 5.2 Frontend

| Componente | Tecnologia | Justificativa |
|------------|-----------|---------------|
| Hosting | Vercel | Deploy gratuito, CDN global, zero config |
| Página | HTML5 + CSS3 | Sem framework — zero build step, deploy direto |
| Gráficos | Plotly.js (CDN) | Rico em chart types, interativo, sem server |
| Dados | Supabase JS Client (CDN) | REST API com anon key, sem backend |
| Mapa | Leaflet.js + heatmap plugin | Mapa de calor por coordenadas |
| Filtros | Vanilla JS | Sem framework — performance máxima |

### 5.3 Infraestrutura

```
Supabase (free tier):
  - Projeto: safecity-sp
  - Region: South America (sa-east-1)
  - Row Level Security: OFF (dados públicos)
  - Anon key: exposta no frontend (seguro para dados públicos)

Vercel:
  - Repo: github.com/igorqv/safecity
  - Deploy: automático via push main
  - Output: static (sem serverless functions)
  - Domínio: safecity-sp.vercel.app
```

---

## 6. Estrutura de Diretórios

```
safecity/
├── prd.md                          # Este documento
├── .env.example                    # Template de variáveis de ambiente
├── .gitignore                      # Ignorar raw/, .env, __pycache__
│
├── raw/                            # Dados brutos SSP-SP (não commitar)
│   ├── CelularesSubtraidos_2017.xlsx
│   └── ... (2018–2026)
│
├── spark/                          # Pipeline PySpark
│   ├── requirements.txt            # pyspark, supabase-py, python-dotenv
│   ├── config.py                   # Paths e constantes
│   ├── ingest.py                   # Lê xlsx → DataFrame unificado
│   ├── clean.py                    # Limpeza e padronização
│   ├── compute_kpis.py             # Cálculo de todas as KPI tables
│   ├── load_supabase.py            # Upsert das tabelas no Supabase
│   └── run_pipeline.py             # Entry point: executa tudo em ordem
│
├── sql/                            # DDL Supabase
│   └── schema.sql                  # CREATE TABLE para todas as KPI tables
│
└── web/                            # Frontend Vercel
    ├── index.html                  # Dashboard principal
    ├── js/
    │   └── dashboard.js            # Lógica de charts e filtros
    └── css/
        └── style.css               # Estilos
```

---

## 7. Schema SQL (Supabase)

```sql
-- Resumo anual
CREATE TABLE kpi_annual_summary (
    ano INTEGER PRIMARY KEY,
    total_ocorrencias INTEGER,
    total_furtos INTEGER,
    total_roubos INTEGER,
    pct_roubo NUMERIC(5,2),
    variacao_yoy NUMERIC(6,2),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Por mês/ano e tipo de crime
CREATE TABLE kpi_monthly (
    id SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    tipo_crime VARCHAR(50),          -- 'Furto' | 'Roubo'
    total_ocorrencias INTEGER,
    UNIQUE(ano, mes, tipo_crime)
);

-- Por município
CREATE TABLE kpi_by_municipality (
    id SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    municipio VARCHAR(100),
    total_ocorrencias INTEGER,
    total_furtos INTEGER,
    total_roubos INTEGER,
    UNIQUE(ano, municipio)
);

-- Por bairro (apenas São Paulo capital)
CREATE TABLE kpi_by_bairro (
    id SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    bairro VARCHAR(100),
    total_ocorrencias INTEGER,
    UNIQUE(ano, bairro)
);

-- Por tipo de local
CREATE TABLE kpi_by_location_type (
    id SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    tipo_local VARCHAR(150),
    total_ocorrencias INTEGER,
    UNIQUE(ano, tipo_local)
);

-- Por marca do celular
CREATE TABLE kpi_by_brand (
    id SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    marca VARCHAR(100),
    total_ocorrencias INTEGER,
    UNIQUE(ano, marca)
);

-- Por hora do dia
CREATE TABLE kpi_by_hour (
    id SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    hora INTEGER,                    -- 0 a 23
    total_ocorrencias INTEGER,
    UNIQUE(ano, hora)
);

-- Por período do dia
CREATE TABLE kpi_by_period (
    id SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    periodo VARCHAR(20),             -- Madrugada|Manhã|Tarde|Noite
    total_ocorrencias INTEGER,
    UNIQUE(ano, periodo)
);
```

---

## 8. Pipeline PySpark — Especificação

### 8.1 ingest.py

```
Entrada: raw/CelularesSubtraidos_*.xlsx
Saída:   parquet/raw_unified/

Lógica:
1. Listar todos os .xlsx em raw/
2. Para cada arquivo, identificar a aba CELULAR_YYYY via regex
3. Ler com spark-excel (header=true, inferSchema=true)
4. Adicionar coluna source_file
5. Union de todos os DataFrames
6. Salvar como Parquet em parquet/raw_unified/
7. Logar: total de linhas, schema, arquivos processados
```

### 8.2 clean.py

```
Entrada: parquet/raw_unified/
Saída:   parquet/clean/

Transformações:
1. Strip de strings em todas colunas VARCHAR
2. Converter "NULL" (string) → null
3. Padronizar MARCA_OBJETO:
   - upper() → dicionário de normalização
   - "APPLE" / "apple" / "iPhone" → "Apple"
   - "SAMSUNG" / "samsung" → "Samsung"
   - marcas com < 10 ocorrências no ano → "Outras"
4. Derivar coluna PERIODO:
   hora 0-5  → "Madrugada"
   hora 6-11 → "Manhã"
   hora 12-17 → "Tarde"
   hora 18-23 → "Noite"
5. Derivar TIPO_CRIME a partir de RUBRICA:
   contém "Furto" → "Furto"
   contém "Roubo" → "Roubo"
6. Filtrar FLAG_STATUS = 'CONSUMADO' (remover tentativas para KPIs principais)
7. Filtrar NOME_MUNICIPIO_CIRC não nulo
8. Manter apenas colunas necessárias para os KPIs
```

### 8.3 compute_kpis.py

```
Entrada: parquet/clean/
Saída:   parquet/kpis/ (uma pasta por tabela)

Computações:
1. kpi_annual_summary: GROUP BY ANO → count, sum furtos, sum roubos, lag() para YoY
2. kpi_monthly:        GROUP BY ANO, MES, TIPO_CRIME → count
3. kpi_by_municipality: GROUP BY ANO, NOME_MUNICIPIO_CIRC, TIPO_CRIME → count, pivot
4. kpi_by_bairro:      WHERE NOME_MUNICIPIO_CIRC = 'S.PAULO' → GROUP BY ANO, BAIRRO
5. kpi_by_location_type: GROUP BY ANO, DESCR_TIPOLOCAL → count
6. kpi_by_brand:       GROUP BY ANO, MARCA_OBJETO_NORM → count → top 20 por ano
7. kpi_by_hour:        GROUP BY ANO, HORA → count
8. kpi_by_period:      GROUP BY ANO, PERIODO → count
```

### 8.4 load_supabase.py

```
Entrada: parquet/kpis/
Saída:   Supabase PostgreSQL (upsert)

Lógica:
1. Ler cada parquet de KPI com pandas (pequenos, < 5000 linhas)
2. Conectar ao Supabase via supabase-py (SUPABASE_URL + SUPABASE_KEY)
3. Para cada tabela:
   a. DELETE existing rows (truncate ou upsert com ON CONFLICT)
   b. INSERT batch (chunks de 500 linhas)
4. Logar cada carga com contagem de linhas inseridas
```

### 8.5 run_pipeline.py (Entry Point)

```python
# Executa todo o pipeline em sequência:
# python spark/run_pipeline.py

import subprocess, sys

steps = [
    "spark/ingest.py",
    "spark/clean.py",
    "spark/compute_kpis.py",
    "spark/load_supabase.py",
]

for step in steps:
    result = subprocess.run([sys.executable, step], check=True)
```

---

## 9. Dashboard Web — Especificação

### 9.1 Layout da Página

```
┌─────────────────────────────────────────────────────┐
│  SafeCity SP 🔒  Roubos e Furtos de Celulares       │
│  Filtros: [Ano: 2017-2026 ▼] [Tipo: Todos ▼]       │
├────────────┬────────────┬────────────┬──────────────┤
│ Total      │ Roubos     │ Furtos     │ Var YoY      │
│ 412.527    │ 234.123    │ 178.404    │ -8,3% ↓      │
├────────────┴────────────┴────────────┴──────────────┤
│  📈 Evolução Anual (bar chart)                       │
│  [2017...2026 com linha de tendência]                │
├──────────────────────┬──────────────────────────────┤
│  📅 Sazonalidade      │  🕐 Pico por Hora            │
│  [line chart meses]  │  [bar chart 0–23h]           │
├──────────────────────┴──────────────────────────────┤
│  🗺️ Mapa de Calor SP (Leaflet + heatmap)            │
│  [baseado em bairros com mais ocorrências]           │
├──────────────────────┬──────────────────────────────┤
│  📱 Top Marcas        │  📍 Top Locais               │
│  [horizontal bar]    │  [horizontal bar]            │
├──────────────────────┴──────────────────────────────┤
│  🏙️ Ranking Municípios  │  🏘️ Top Bairros (SP)       │
│  [table com sparkline]  │  [table com sparkline]    │
└─────────────────────────────────────────────────────┘
```

### 9.2 Filtros

| Filtro | Tipo | Opções |
|--------|------|--------|
| Ano | Multi-select dropdown | 2017–2026 + "Todos" |
| Tipo de Crime | Radio button | Todos / Furto / Roubo |

**Comportamento:** Filtros no lado do cliente (JS filtra arrays já carregados), sem nova requisição ao Supabase.

### 9.3 Carregamento de Dados

```javascript
// Carregar todos os KPIs uma vez no load da página
// ~5000 linhas total — menos de 500KB de JSON

async function loadAllData() {
  const [annual, monthly, municipality, brand, hour, location] = await Promise.all([
    supabase.from('kpi_annual_summary').select('*'),
    supabase.from('kpi_monthly').select('*'),
    supabase.from('kpi_by_municipality').select('*').order('total_ocorrencias', { ascending: false }),
    supabase.from('kpi_by_brand').select('*'),
    supabase.from('kpi_by_hour').select('*'),
    supabase.from('kpi_by_location_type').select('*'),
  ]);
  // Armazenar em memória → filtros operam localmente
}
```

### 9.4 Performance

- Todos os dados carregados **uma vez** no page load (~500KB JSON)
- Filtros operam em arrays JavaScript em memória (0ms de latência de rede)
- Plotly re-renderiza localmente
- Sem serverless functions — página 100% estática no Vercel

---

## 10. Variáveis de Ambiente

```bash
# .env.example

# Supabase
SUPABASE_URL=https://xxxxxxxx.supabase.co
SUPABASE_KEY=eyJ...                    # service_role key (só no pipeline)
SUPABASE_ANON_KEY=eyJ...              # anon key (exposta no frontend)

# PySpark
SPARK_DRIVER_MEMORY=8g
RAW_DATA_PATH=../raw
PARQUET_PATH=../parquet
```

---

## 11. Critérios de Aceite

### 11.1 Pipeline

- [ ] Pipeline processa todos os 10 arquivos sem erro
- [ ] Total de linhas processadas: ~4,08 milhões
- [ ] Sem duplicatas nas KPI tables (UNIQUE constraint respeitado)
- [ ] Normalização de marcas funciona (Samsung/SAMSUNG → "Samsung")
- [ ] Carga no Supabase: 8 tabelas, < 5000 linhas total

### 11.2 Dashboard

- [ ] Página carrega em < 3 segundos em conexão 4G
- [ ] Filtro de ano atualiza todos os gráficos simultaneamente
- [ ] Filtro de tipo de crime (Furto/Roubo) funciona em todos os charts
- [ ] Mapa de calor renderiza nos bairros de SP
- [ ] Responsivo: funciona em mobile e desktop
- [ ] Sem erros no console do browser

### 11.3 Negócio

- [ ] Dado mais recente disponível: 2026 (parcial)
- [ ] KPI YoY visível para gestores
- [ ] Ranking de municípios com comparativo ano a ano
- [ ] Top 5 marcas mais roubadas por ano

---

## 12. Fora de Escopo

- Autenticação / login
- Dados em tempo real
- API pública de consulta livre
- Processamento incremental (pipeline é batch — rodar manualmente quando SSP atualizar)
- Outros tipos de crime (homicídio, tráfico, etc.) — foco exclusivo em celulares
- Dados de outros estados

---

## 13. Riscos e Mitigações

| Risco | Probabilidade | Impacto | Mitigação |
|-------|--------------|---------|-----------|
| SSP-SP muda formato do Excel | Média | Alto | Schema validation no ingest; alertar se colunas divergirem |
| Lat/Lon nulos (mapas vazios) | Alta | Médio | Mapa usa bairros apenas quando coord disponível; fallback para ranking textual |
| Supabase free tier (500MB) | Baixa | Alto | Apenas agregados (~5k linhas, <10MB) — seguro |
| Vercel timeout (serverless) | N/A | N/A | Sem funções serverless — página 100% estática |
| Encoding de strings acentuadas | Alta | Médio | `encoding='latin-1'` no spark-excel; strip + normalize antes de salvar |

---

## 14. Próximos Passos (Sequência de Desenvolvimento)

1. **Setup** — Criar `.env`, instalar dependências PySpark, criar projeto Supabase
2. **SQL** — Executar `sql/schema.sql` no Supabase
3. **ingest.py** — Ler e unificar os 10 xlsx em Parquet
4. **clean.py** — Aplicar todas as regras de limpeza
5. **compute_kpis.py** — Gerar as 8 tabelas de KPI em Parquet
6. **load_supabase.py** — Carregar KPIs no Supabase
7. **web/index.html** — Dashboard estático com Plotly.js
8. **web/js/dashboard.js** — Lógica de charts e filtros
9. **Deploy Vercel** — Conectar repositório, deploy automático
10. **Validação** — Checar números contra dados brutos, QA do dashboard

---

*Documento gerado em 2026-04-19. Fonte dos dados: SSP-SP — https://www.ssp.sp.gov.br/estatistica/consultas*
