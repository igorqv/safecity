/**
 * dashboard.js — Lógica do SafeCity SP Dashboard
 *
 * Estratégia de dados:
 *   - Carregamos as 8 tabelas KPI do Supabase UMA VEZ no page load (~500KB total)
 *   - Armazenamos em memória (objeto `kpis`)
 *   - Filtros operam localmente sobre os arrays em memória (sem nova requisição)
 *   - Plotly.react atualiza os gráficos in-place (sem recriar DOM, sem flicker)
 *
 * Por que não usar o Supabase JS Client?
 *   Para dados públicos somente-leitura, fetch() nativo com a anon key é suficiente.
 *   Evita dependência extra de ~100KB de JavaScript.
 */

'use strict';

// ── Configuração ───────────────────────────────────────────────────────────────
// Valores injetados pelo index.html via variáveis globais para facilitar deploy no Vercel
// (não hardcodamos aqui — cada ambiente usa sua própria URL/chave)
const SUPABASE_URL = window.SUPABASE_URL || '';
const SUPABASE_KEY = window.SUPABASE_KEY || '';

// ── Paleta de cores — fonte única de verdade ────────────────────────────────────
// Manter aqui garante que todos os gráficos usem as mesmas cores sem duplicação
const COLORS = {
  vermelho:    '#E63946',  // crimes / urgência
  azul:        '#457B9D',  // furtos / comparativo
  verde:       '#2A9D8F',  // métricas positivas (queda de crime)
  amarelo:     '#E9C46A',  // alerta
  cinza:       '#264653',  // série neutra
  laranja:     '#F4A261',  // destaque secundário
};

// Config global do Plotly — aplicado a todos os gráficos
// displayModeBar:false remove a barra de ferramentas do Plotly que polui o dashboard
const PLOTLY_CONFIG = { responsive: true, displayModeBar: false, locale: 'pt-BR' };

// Layout base compartilhado — transparente para o CSS do card controlar o fundo
const BASE_LAYOUT = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor:  'rgba(0,0,0,0)',
  font: { family: 'Inter, system-ui, sans-serif', size: 12, color: '#94a3b8' },
  margin: { l: 55, r: 20, t: 40, b: 45 },
  colorway: Object.values(COLORS),
};

// ── Estado global ──────────────────────────────────────────────────────────────
// Nunca mutamos esses objetos após o carregamento — filtros criam novos arrays
let kpis = {
  annual:    [],   // kpi_annual_summary
  monthly:   [],   // kpi_monthly
  municipality: [], // kpi_by_municipality
  bairro:    [],   // kpi_by_bairro
  location:  [],   // kpi_by_location_type
  brand:     [],   // kpi_by_brand
  hour:      [],   // kpi_by_hour
  period:    [],   // kpi_by_period
};

// Lista de anos disponíveis — populada após o fetch
let anosDisponiveis = [];

// ── Fetch ──────────────────────────────────────────────────────────────────────
/**
 * Busca uma tabela KPI do Supabase.
 * Usa fetch nativo em vez do Supabase JS Client para evitar dependência extra.
 * A anon key é pública (dados de segurança pública são públicos por lei).
 */
async function fetchTabela(nome) {
  const url = `${SUPABASE_URL}/rest/v1/${nome}?select=*&order=ano.asc`;
  const res  = await fetch(url, {
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
    },
  });
  if (!res.ok) throw new Error(`Erro ao buscar ${nome}: HTTP ${res.status}`);
  return res.json();
}

async function carregarDados() {
  // Se dados mock estão disponíveis (test.html), usá-los em vez de fazer fetch real
  if (window.MOCK_DATA) {
    const { annual, monthly, municipality, bairro, location, brand, hour, period } = window.MOCK_DATA;
    kpis = { annual, monthly, municipality, bairro, location, brand, hour, period };
  } else {
    // Buscar todas as tabelas em paralelo — reduz o tempo total de carregamento
    // (sem paralelo: 8 fetches sequenciais × ~200ms = ~1.6s; paralelo: ~200ms)
    const [annual, monthly, municipality, bairro, location, brand, hour, period] =
      await Promise.all([
        fetchTabela('kpi_annual_summary'),
        fetchTabela('kpi_monthly'),
        fetchTabela('kpi_by_municipality'),
        fetchTabela('kpi_by_bairro'),
        fetchTabela('kpi_by_location_type'),
        fetchTabela('kpi_by_brand'),
        fetchTabela('kpi_by_hour'),
        fetchTabela('kpi_by_period'),
      ]);

    kpis = { annual, monthly, municipality, bairro, location, brand, hour, period };
  }

  // Descobrir anos disponíveis a partir dos dados reais
  anosDisponiveis = [...new Set(kpis.annual.map(r => r.ano))].sort();
  return kpis;
}

// ── Filtro ─────────────────────────────────────────────────────────────────────
/**
 * Retorna os valores atuais dos filtros da UI.
 * Centralizado aqui para que todos os update* leiam o mesmo estado.
 */
function lerFiltros() {
  return {
    ano:  document.getElementById('filter-ano').value,   // '' = todos os anos
    tipo: document.getElementById('filter-tipo').value,  // '' = furto + roubo
  };
}

/**
 * Filtra um array de registros KPI pelos filtros ativos.
 * Função pura — não modifica o array original.
 * Cada condição só é aplicada se o filtro não for vazio ('').
 */
function filtrar(registros, { ano, tipo }) {
  return registros.filter(r => {
    if (ano  && String(r.ano) !== ano)   return false;
    if (tipo && r.tipo_crime !== tipo)   return false;
    return true;
  });
}

// ── Formatação ─────────────────────────────────────────────────────────────────
const fmt = n => Number(n).toLocaleString('pt-BR');
const fmtPct = n => n == null ? '—' : `${n > 0 ? '+' : ''}${Number(n).toFixed(1)}%`;

// Normalizar campo de contagem — diferentes tabelas usam nomes diferentes
const getTotal = r => getTotal(r)_ocorrencias || getTotal(r) || 0;

// ── KPI Cards ──────────────────────────────────────────────────────────────────
function atualizarCards(filtros) {
  const dados = filtrar(kpis.annual, filtros);

  const total  = dados.reduce((s, r) => s + (getTotal(r)_ocorrencias || 0), 0);
  const furtos = dados.reduce((s, r) => s + (getTotal(r)_furtos || 0), 0);
  const roubos = dados.reduce((s, r) => s + (getTotal(r)_roubos || 0), 0);

  // Variação YoY: usar o último ano disponível (mais relevante para o usuário)
  const anoReferencia = filtros.ano
    ? Number(filtros.ano)
    : Math.max(...dados.map(r => r.ano).filter(Boolean));

  const dadosUltimoAno = dados.filter(r => r.ano === anoReferencia);
  const yoyMedio = dadosUltimoAno.length
    ? dadosUltimoAno.reduce((s,r) => s + (r.variacao_yoy_pct || 0), 0) / dadosUltimoAno.length
    : null;

  document.getElementById('kpi-total').textContent  = fmt(total);
  document.getElementById('kpi-furto').textContent  = fmt(furtos);
  document.getElementById('kpi-roubo').textContent  = fmt(roubos);
  document.getElementById('kpi-yoy').textContent    = fmtPct(yoyMedio);

  // Colorir o card de YoY: vermelho = aumento de crime; verde = queda
  const yoyCard = document.getElementById('card-yoy');
  yoyCard.classList.remove('up', 'down');
  if (yoyMedio !== null) yoyCard.classList.add(yoyMedio > 0 ? 'up' : 'down');
}

// ── Gráfico 1: Evolução Anual ──────────────────────────────────────────────────
function renderAnual(filtros, modo = 'new') {
  // Agregar por ano (soma furto + roubo, a menos que tipo esteja filtrado)
  const dados = filtrar(kpis.annual, { ...filtros, tipo: '' }); // sem filtro de tipo para mostrar série completa
  const byAno = {};
  dados.forEach(r => {
    if (!byAno[r.ano]) byAno[r.ano] = { Furto: 0, Roubo: 0 };
    byAno[r.ano][r.tipo_crime] = (byAno[r.ano][r.tipo_crime] || 0) + getTotal(r);
  });

  const anos   = Object.keys(byAno).sort();
  const furtos = anos.map(a => byAno[a].Furto || 0);
  const roubos = anos.map(a => byAno[a].Roubo || 0);

  const traceFurto = {
    type: 'bar', name: 'Furto',
    x: anos, y: furtos,
    marker: { color: COLORS.azul },
    hovertemplate: '%{x}: %{y:,}<extra>Furto</extra>',
  };
  const traceRoubo = {
    type: 'bar', name: 'Roubo',
    x: anos, y: roubos,
    marker: { color: COLORS.vermelho },
    hovertemplate: '%{x}: %{y:,}<extra>Roubo</extra>',
  };

  const layout = {
    ...BASE_LAYOUT,
    barmode: 'stack',  // empilhado: mostra total e split ao mesmo tempo
    xaxis: { title: { text: 'Ano' }, type: 'category', color: '#94a3b8' },
    yaxis: { title: { text: 'Ocorrências' }, tickformat: ',', color: '#94a3b8', gridcolor: '#1e293b' },
    legend: { orientation: 'h', y: -0.15, font: { color: '#94a3b8' } },
  };

  const fn = modo === 'new' ? Plotly.newPlot : Plotly.react;
  fn('chart-anual', [traceFurto, traceRoubo], layout, PLOTLY_CONFIG);
}

// ── Gráfico 2: Sazonalidade Mensal ─────────────────────────────────────────────
function renderMensal(filtros, modo = 'new') {
  const dados = filtrar(kpis.monthly, filtros);
  const MESES = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'];
  const byMes = Array(12).fill(0);
  dados.forEach(r => { byMes[(r.mes || 1) - 1] += getTotal(r); });

  const trace = {
    type: 'scatter', mode: 'lines+markers',
    // lines+markers: a linha mostra a tendência, os marcadores destacam cada mês
    x: MESES, y: byMes,
    line: { color: COLORS.vermelho, width: 2.5 },
    marker: { size: 7, color: COLORS.vermelho },
    fill: 'tozeroy',        // área preenchida: destaca sazonalidade visualmente
    fillcolor: 'rgba(230,57,70,0.08)',
    hovertemplate: '%{x}: %{y:,} ocorrências<extra></extra>',
  };

  const layout = {
    ...BASE_LAYOUT,
    xaxis: { color: '#94a3b8', gridcolor: '#1e293b' },
    yaxis: { tickformat: ',', color: '#94a3b8', gridcolor: '#1e293b' },
  };

  const fn = modo === 'new' ? Plotly.newPlot : Plotly.react;
  fn('chart-mensal', [trace], layout, PLOTLY_CONFIG);
}

// ── Gráfico 3: Distribuição Horária ────────────────────────────────────────────
function renderHorario(filtros, modo = 'new') {
  const dados = filtrar(kpis.hour, filtros);
  const byHora = Array(24).fill(0);
  dados.forEach(r => { byHora[r.hora] = (byHora[r.hora] || 0) + getTotal(r); });

  // Colorir barras por período para facilitar a leitura visual
  // Madrugada (0-5): cinza | Manhã (6-11): amarelo | Tarde (12-17): azul | Noite (18-23): vermelho
  const cores = Array.from({ length: 24 }, (_, h) => {
    if (h < 6)  return COLORS.cinza;
    if (h < 12) return COLORS.amarelo;
    if (h < 18) return COLORS.azul;
    return COLORS.vermelho;
  });

  const trace = {
    type: 'bar',
    x: Array.from({ length: 24 }, (_, i) => `${String(i).padStart(2,'0')}h`),
    y: byHora,
    marker: { color: cores },
    hovertemplate: '%{x}: %{y:,} ocorrências<extra></extra>',
  };

  const layout = {
    ...BASE_LAYOUT,
    margin: { ...BASE_LAYOUT.margin, b: 55 },
    xaxis: { tickangle: -45, color: '#94a3b8' },
    yaxis: { tickformat: ',', color: '#94a3b8', gridcolor: '#1e293b' },
  };

  const fn = modo === 'new' ? Plotly.newPlot : Plotly.react;
  fn('chart-horario', [trace], layout, PLOTLY_CONFIG);
}

// ── Gráfico 4: Furto vs Roubo (Donut) ─────────────────────────────────────────
function renderTipoCrime(filtros, modo = 'new') {
  // Para o donut, ignoramos o filtro de tipo — faz sentido sempre mostrar os dois
  const dados = filtrar(kpis.annual, { ...filtros, tipo: '' });
  const furtos = dados.filter(r => r.tipo_crime === 'Furto').reduce((s,r)=>s+getTotal(r),0);
  const roubos = dados.filter(r => r.tipo_crime === 'Roubo').reduce((s,r)=>s+getTotal(r),0);

  const trace = {
    type: 'pie',
    labels: ['Furto', 'Roubo'],
    values: [furtos, roubos],
    hole: 0.55,  // hole > 0.5: donut com espaço central para legenda visual
    marker: { colors: [COLORS.azul, COLORS.vermelho] },
    textinfo: 'percent',
    textfont: { color: '#fff', size: 13 },
    hovertemplate: '%{label}: %{value:,} (%{percent})<extra></extra>',
  };

  const layout = {
    ...BASE_LAYOUT,
    margin: { l: 20, r: 20, t: 40, b: 20 },
    legend: { orientation: 'h', y: -0.05, font: { color: '#94a3b8' } },
    annotations: [{
      text: `${fmt(furtos + roubos)}<br><span style="font-size:10px">total</span>`,
      x: 0.5, y: 0.5, xref: 'paper', yref: 'paper',
      showarrow: false,
      font: { size: 16, color: '#f1f5f9' },
    }],
  };

  const fn = modo === 'new' ? Plotly.newPlot : Plotly.react;
  fn('chart-tipo', [trace], layout, PLOTLY_CONFIG);
}

// ── Gráfico 5: Top Marcas ──────────────────────────────────────────────────────
function renderMarcas(filtros, modo = 'new', topN = 10) {
  const dados = filtrar(kpis.brand, filtros);
  const byMarca = {};
  dados.forEach(r => { byMarca[r.marca] = (byMarca[r.marca] || 0) + getTotal(r); });

  const sorted = Object.entries(byMarca).sort((a,b) => b[1]-a[1]).slice(0, topN);
  const marcas = sorted.map(e => e[0]).reverse();  // reverse: maior fica no topo do horizontal
  const totais = sorted.map(e => e[1]).reverse();

  const trace = {
    type: 'bar',
    orientation: 'h',  // horizontal: nomes de marca longos cabem melhor no eixo Y
    x: totais, y: marcas,
    marker: { color: COLORS.azul },
    hovertemplate: '%{y}: %{x:,}<extra></extra>',
  };

  const layout = {
    ...BASE_LAYOUT,
    margin: { l: 110, r: 20, t: 40, b: 45 },
    xaxis: { tickformat: ',', color: '#94a3b8', gridcolor: '#1e293b' },
    yaxis: { color: '#94a3b8' },
  };

  const fn = modo === 'new' ? Plotly.newPlot : Plotly.react;
  fn('chart-marcas', [trace], layout, PLOTLY_CONFIG);
}

// ── Gráfico 6: Tipos de Local ──────────────────────────────────────────────────
function renderLocais(filtros, modo = 'new', topN = 8) {
  const dados = filtrar(kpis.location, filtros);
  const byLocal = {};
  dados.forEach(r => { byLocal[r.tipo_local] = (byLocal[r.tipo_local] || 0) + getTotal(r); });

  const sorted = Object.entries(byLocal).sort((a,b) => b[1]-a[1]).slice(0, topN);
  const locais = sorted.map(e => e[0]).reverse();
  const totais = sorted.map(e => e[1]).reverse();

  const trace = {
    type: 'bar', orientation: 'h',
    x: totais, y: locais,
    marker: { color: COLORS.cinza },
    hovertemplate: '%{y}: %{x:,}<extra></extra>',
  };

  const layout = {
    ...BASE_LAYOUT,
    margin: { l: 180, r: 20, t: 40, b: 45 },  // margem esquerda maior para nomes de local
    xaxis: { tickformat: ',', color: '#94a3b8', gridcolor: '#1e293b' },
    yaxis: { color: '#94a3b8', tickfont: { size: 11 } },
  };

  const fn = modo === 'new' ? Plotly.newPlot : Plotly.react;
  fn('chart-locais', [trace], layout, PLOTLY_CONFIG);
}

// ── Tabela de Municípios ───────────────────────────────────────────────────────
function renderMunicipios(filtros) {
  const dados = filtrar(kpis.municipality, filtros);
  const byMunicipio = {};
  dados.forEach(r => { byMunicipio[r.municipio] = (byMunicipio[r.municipio]||0) + getTotal(r); });

  const sorted = Object.entries(byMunicipio).sort((a,b)=>b[1]-a[1]).slice(0, 20);
  const maximo = sorted[0]?.[1] || 1;

  const tbody = sorted.map(([mun, total], i) => `
    <tr>
      <td class="rank-num">${i+1}</td>
      <td>${mun}</td>
      <td>
        <div class="table-row-bar">
          <div class="bar-bg"><div class="bar-fill" style="width:${(total/maximo*100).toFixed(1)}%"></div></div>
          <span class="total-val">${fmt(total)}</span>
        </div>
      </td>
    </tr>`).join('');

  document.getElementById('tabela-municipios').innerHTML = tbody;
}

// ── Tabela de Bairros ──────────────────────────────────────────────────────────
function renderBairros(filtros) {
  const dados = filtrar(kpis.bairro, filtros);
  const byBairro = {};
  dados.forEach(r => { byBairro[r.bairro] = (byBairro[r.bairro]||0) + getTotal(r); });

  const sorted = Object.entries(byBairro).sort((a,b)=>b[1]-a[1]).slice(0, 20);
  const maximo = sorted[0]?.[1] || 1;

  const tbody = sorted.map(([bairro, total], i) => `
    <tr>
      <td class="rank-num">${i+1}</td>
      <td>${bairro}</td>
      <td>
        <div class="table-row-bar">
          <div class="bar-bg"><div class="bar-fill" style="width:${(total/maximo*100).toFixed(1)}%"></div></div>
          <span class="total-val">${fmt(total)}</span>
        </div>
      </td>
    </tr>`).join('');

  document.getElementById('tabela-bairros').innerHTML = tbody;
}

// ── Renderização inicial (newPlot) e atualização (react) ──────────────────────
function renderizarTodos(modo = 'new') {
  const filtros = lerFiltros();
  // Plotly.react é mais eficiente que newPlot para updates — não recria os elementos DOM
  atualizarCards(filtros);
  renderAnual(filtros, modo);
  renderMensal(filtros, modo);
  renderHorario(filtros, modo);
  renderTipoCrime(filtros, modo);
  renderMarcas(filtros, modo);
  renderLocais(filtros, modo);
  renderMunicipios(filtros);
  renderBairros(filtros);
}

// ── Inicialização do dropdown de anos ─────────────────────────────────────────
function popularFiltroAnos() {
  const select = document.getElementById('filter-ano');
  // Manter o option "Todos" e inserir os anos em ordem decrescente (mais recente primeiro)
  anosDisponiveis.slice().reverse().forEach(ano => {
    const opt = document.createElement('option');
    opt.value = ano;
    opt.textContent = ano;
    select.appendChild(opt);
  });
}

// ── Event listeners dos filtros ────────────────────────────────────────────────
function configurarFiltros() {
  ['filter-ano', 'filter-tipo'].forEach(id => {
    document.getElementById(id).addEventListener('change', () => {
      // 'react' reutiliza os elementos DOM existentes — evita flicker e é mais rápido
      // que recriar os gráficos com newPlot a cada mudança de filtro
      renderizarTodos('react');
    });
  });
}

// ── Entry point ────────────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', async () => {
  const loading = document.getElementById('loading');
  const errorEl = document.getElementById('error-banner');

  try {
    loading.classList.remove('hidden');

    await carregarDados();

    loading.classList.add('hidden');

    popularFiltroAnos();
    configurarFiltros();

    // Primeira renderização com newPlot — cria os elementos Plotly no DOM
    renderizarTodos('new');

    // Atualizar metadado de "última atualização" na UI
    const ultimoAno = Math.max(...anosDisponiveis);
    const metaEl = document.getElementById('meta-periodo');
    if (metaEl) metaEl.textContent = `Dados: 2017–${ultimoAno} | Fonte: SSP-SP`;

  } catch (err) {
    loading.classList.add('hidden');
    errorEl.style.display = 'block';
    errorEl.textContent = `Erro ao carregar dados: ${err.message}. Verifique a conexão com o Supabase.`;
    console.error('Dashboard error:', err);
  }
});
