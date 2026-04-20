-- ============================================================
-- SafeCity SP — Schema das Tabelas KPI no Supabase
-- ============================================================
-- Executar no SQL Editor do Supabase antes de rodar o pipeline.
--
-- DECISÃO ARQUITETURAL: Armazenamos apenas agregados (~5k linhas total),
-- não os 4M de registros brutos. Isso garante que o dashboard no Vercel
-- carregue em < 3 segundos sem nenhuma paginação ou cache no servidor.
-- ============================================================

-- ── 1. Resumo anual com variação YoY ──────────────────────────────────────────
-- Tabela mais importante para o card de KPI principal e gráfico de evolução.
-- variacao_yoy_pct: positivo = aumento de crimes, negativo = queda.
CREATE TABLE IF NOT EXISTS kpi_annual_summary (
    id          SERIAL PRIMARY KEY,
    ano         INTEGER NOT NULL,
    tipo_crime  VARCHAR(20) NOT NULL,   -- 'Furto' | 'Roubo'
    total       INTEGER NOT NULL,
    variacao_yoy_pct NUMERIC(6,2),     -- NULL para o primeiro ano (sem referência anterior)
    UNIQUE (ano, tipo_crime)
);

-- ── 2. Ocorrências por mês/ano ─────────────────────────────────────────────────
-- Alimenta o gráfico de sazonalidade (line chart jan–dez).
-- Separado por tipo_crime para permitir filtro Furto/Roubo no dashboard.
CREATE TABLE IF NOT EXISTS kpi_monthly (
    id          SERIAL PRIMARY KEY,
    ano         INTEGER NOT NULL,
    mes         INTEGER NOT NULL,      -- 1 a 12
    tipo_crime  VARCHAR(20) NOT NULL,
    total       INTEGER NOT NULL,
    UNIQUE (ano, mes, tipo_crime)
);

-- ── 3. Ocorrências por município ───────────────────────────────────────────────
-- Ranking de cidades — usa NOME_MUNICIPIO_CIRC (onde o crime aconteceu),
-- não NOME_MUNICIPIO (onde a delegacia está).
CREATE TABLE IF NOT EXISTS kpi_by_municipality (
    id          SERIAL PRIMARY KEY,
    ano         INTEGER NOT NULL,
    municipio   VARCHAR(100) NOT NULL,
    tipo_crime  VARCHAR(20) NOT NULL,
    total       INTEGER NOT NULL,
    UNIQUE (ano, municipio, tipo_crime)
);

-- ── 4. Ocorrências por bairro (São Paulo capital) ─────────────────────────────
-- Filtrado apenas para S.PAULO — ranking de pontos críticos na capital.
-- Bairros são o dado mais granular disponível sem necessidade de geocoding.
CREATE TABLE IF NOT EXISTS kpi_by_bairro (
    id          SERIAL PRIMARY KEY,
    ano         INTEGER NOT NULL,
    bairro      VARCHAR(100) NOT NULL,
    tipo_crime  VARCHAR(20) NOT NULL,
    total       INTEGER NOT NULL,
    UNIQUE (ano, bairro, tipo_crime)
);

-- ── 5. Ocorrências por tipo de local ──────────────────────────────────────────
-- Responde: onde os celulares são mais roubados?
-- (Via pública, Terminal/Estação, Restaurante, etc.)
CREATE TABLE IF NOT EXISTS kpi_by_location_type (
    id          SERIAL PRIMARY KEY,
    ano         INTEGER NOT NULL,
    tipo_local  VARCHAR(150) NOT NULL,
    tipo_crime  VARCHAR(20) NOT NULL,
    total       INTEGER NOT NULL,
    UNIQUE (ano, tipo_local, tipo_crime)
);

-- ── 6. Ocorrências por marca do celular ───────────────────────────────────────
-- Marcas normalizadas no pipeline (Samsung/SAMSUNG/samsung → Samsung).
-- Topo das marcas mais roubadas — relevante para campanhas de prevenção.
CREATE TABLE IF NOT EXISTS kpi_by_brand (
    id          SERIAL PRIMARY KEY,
    ano         INTEGER NOT NULL,
    marca       VARCHAR(100) NOT NULL,
    tipo_crime  VARCHAR(20) NOT NULL,
    total       INTEGER NOT NULL,
    UNIQUE (ano, marca, tipo_crime)
);

-- ── 7. Ocorrências por hora do dia ────────────────────────────────────────────
-- Distribui os crimes pelas 24 horas — pico às 18-20h é típico em SP.
-- Útil para gestores de segurança planejarem patrulhamento.
CREATE TABLE IF NOT EXISTS kpi_by_hour (
    id          SERIAL PRIMARY KEY,
    ano         INTEGER NOT NULL,
    hora        INTEGER NOT NULL,      -- 0 a 23
    tipo_crime  VARCHAR(20) NOT NULL,
    total       INTEGER NOT NULL,
    UNIQUE (ano, hora, tipo_crime)
);

-- ── 8. Ocorrências por período do dia ─────────────────────────────────────────
-- Agrupamento derivado da hora: Madrugada / Manhã / Tarde / Noite.
-- Mais legível que as 24 horas para apresentações executivas.
CREATE TABLE IF NOT EXISTS kpi_by_period (
    id          SERIAL PRIMARY KEY,
    ano         INTEGER NOT NULL,
    periodo     VARCHAR(20) NOT NULL,  -- 'Madrugada' | 'Manhã' | 'Tarde' | 'Noite'
    tipo_crime  VARCHAR(20) NOT NULL,
    total       INTEGER NOT NULL,
    UNIQUE (ano, periodo, tipo_crime)
);

-- ── Permissões de leitura pública ─────────────────────────────────────────────
-- Dados da SSP-SP são públicos por lei — qualquer pessoa pode consultar.
-- Desativar RLS e conceder SELECT ao role anon (usado pelo dashboard via anon key).

ALTER TABLE kpi_annual_summary   DISABLE ROW LEVEL SECURITY;
ALTER TABLE kpi_monthly          DISABLE ROW LEVEL SECURITY;
ALTER TABLE kpi_by_municipality  DISABLE ROW LEVEL SECURITY;
ALTER TABLE kpi_by_bairro        DISABLE ROW LEVEL SECURITY;
ALTER TABLE kpi_by_location_type DISABLE ROW LEVEL SECURITY;
ALTER TABLE kpi_by_brand         DISABLE ROW LEVEL SECURITY;
ALTER TABLE kpi_by_hour          DISABLE ROW LEVEL SECURITY;
ALTER TABLE kpi_by_period        DISABLE ROW LEVEL SECURITY;

GRANT SELECT ON kpi_annual_summary   TO anon;
GRANT SELECT ON kpi_monthly          TO anon;
GRANT SELECT ON kpi_by_municipality  TO anon;
GRANT SELECT ON kpi_by_bairro        TO anon;
GRANT SELECT ON kpi_by_location_type TO anon;
GRANT SELECT ON kpi_by_brand         TO anon;
GRANT SELECT ON kpi_by_hour          TO anon;
GRANT SELECT ON kpi_by_period        TO anon;

-- ── Confirmar criação ──────────────────────────────────────────────────────────
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name LIKE 'kpi_%'
ORDER BY table_name;
