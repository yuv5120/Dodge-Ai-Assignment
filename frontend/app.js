/**
 * app.js — DodgeAI O2C Graph Explorer
 * D3.js force-directed graph + streaming chat interface
 */

const API = '';  // Same origin
const NODE_RADIUS = { SalesOrder: 9, Customer: 10, Delivery: 8, BillingDoc: 9, JournalEntry: 7, Payment: 7, Product: 8, Plant: 7, SalesOrderItem: 5, DeliveryItem: 5, BillingItem: 5, Address: 5 };
const TYPE_ORDER = ['SalesOrder','Customer','Delivery','BillingDoc','JournalEntry','Payment','Product','Plant','SalesOrderItem','DeliveryItem','BillingItem'];

let graphData = { nodes: [], edges: [] };
let simulation = null;
let svg, gContainer, linkGroup, nodeGroup;
let selectedNodeId = null;
let highlightedNodeIds = new Set();
let showLabels = true;
let chatHistory = [];
let currentSql = null;
let currentData = null;

// ── Init ───────────────────────────────────────────────────────────────────
async function init() {
  initGraph();
  await loadGraph();
  initChat();
  initSearch();
  initControls();
}

// ── Graph Initialization ───────────────────────────────────────────────────
function initGraph() {
  const container = document.getElementById('graphContainer');
  const width = container.clientWidth;
  const height = container.clientHeight;

  svg = d3.select('#graphContainer')
    .append('svg')
    .attr('width', '100%')
    .attr('height', '100%');

  // Arrow marker
  svg.append('defs').append('marker')
    .attr('id', 'arrow')
    .attr('viewBox', '0 -4 8 8')
    .attr('refX', 16).attr('refY', 0)
    .attr('markerWidth', 6).attr('markerHeight', 6)
    .attr('orient', 'auto')
    .append('path')
    .attr('d', 'M0,-4L8,0L0,4')
    .attr('fill', 'rgba(79,142,247,0.55)');

  gContainer = svg.append('g');

  // Zoom
  const zoom = d3.zoom()
    .scaleExtent([0.05, 4])
    .on('zoom', (e) => gContainer.attr('transform', e.transform));
  svg.call(zoom);

  // Store zoom ref for fit
  svg._zoom = zoom;

  linkGroup = gContainer.append('g').attr('class', 'links');
  nodeGroup = gContainer.append('g').attr('class', 'nodes');

  // Click on background = deselect
  svg.on('click', (e) => {
    if (e.target === svg.node() || e.target.tagName === 'svg') {
      deselectNode();
    }
  });
}

async function loadGraph() {
  try {
    const [graphRes, statsRes] = await Promise.all([
      fetch(`${API}/api/graph?limit=1500`),
      fetch(`${API}/api/graph/stats`)
    ]);
    graphData = await graphRes.json();
    const stats = await statsRes.json();

    document.getElementById('graphLoading').style.display = 'none';
    document.getElementById('statsPill').textContent =
      `${stats.total_nodes.toLocaleString()} nodes · ${stats.total_edges.toLocaleString()} edges`;

    buildLegend(stats.node_types);
    renderGraph();
  } catch (err) {
    document.getElementById('graphLoading').innerHTML =
      `<p style="color:#FF6B6B">Failed to load graph: ${err.message}</p>`;
  }
}

function buildLegend(typeCounts) {
  const legend = document.getElementById('legend');
  const colors = {
    SalesOrder:'#4F8EF7', SalesOrderItem:'#82AAFF',
    Delivery:'#42D392', DeliveryItem:'#7DEFA1',
    BillingDoc:'#FF6B6B', BillingItem:'#FFA07A',
    JournalEntry:'#FFD93D', Payment:'#6BCB77',
    Customer:'#C77DFF', Product:'#FF9F1C',
    Plant:'#2EC4B6', Address:'#AAAAAA'
  };
  const showing = TYPE_ORDER.filter(t => typeCounts[t]);
  legend.innerHTML = showing.map(t => `
    <div class="legend-item">
      <div class="legend-dot" style="background:${colors[t]}"></div>
      <span>${t}</span>
    </div>
  `).join('');
}

function renderGraph() {
  const nodes = graphData.nodes.map(d => ({ ...d }));
  const nodeMap = new Map(nodes.map(n => [n.id, n]));

  const links = graphData.edges
    .filter(e => nodeMap.has(e.source) && nodeMap.has(e.target))
    .map(e => ({ ...e, source: e.source, target: e.target }));

  const container = document.getElementById('graphContainer');
  const W = container.clientWidth;
  const H = container.clientHeight;

  simulation = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id).distance(60).strength(0.3))
    .force('charge', d3.forceManyBody().strength(-120).distanceMax(300))
    .force('center', d3.forceCenter(W / 2, H / 2))
    .force('collision', d3.forceCollide().radius(d => getRadius(d) + 4))
    .alphaDecay(0.02);

  // Links
  const link = linkGroup.selectAll('.link-line')
    .data(links)
    .join('line')
    .attr('class', 'link-line')
    .attr('marker-end', 'url(#arrow)');

  // Nodes
  const nodeG = nodeGroup.selectAll('.node-g')
    .data(nodes, d => d.id)
    .join('g')
    .attr('class', 'node-g')
    .style('cursor', 'pointer')
    .call(d3.drag()
      .on('start', dragStart)
      .on('drag', dragging)
      .on('end', dragEnd))
    .on('click', (e, d) => {
      e.stopPropagation();
      selectNode(d, nodeG, link);
    });

  nodeG.append('circle')
    .attr('class', 'node-circle')
    .attr('r', d => getRadius(d))
    .attr('fill', d => d.color)
    .attr('stroke', d => lighten(d.color))
    .attr('stroke-width', 1.5);

  nodeG.append('text')
    .attr('class', 'node-label')
    .attr('dy', d => getRadius(d) + 11)
    .text(d => truncate(d.label, 16))
    .style('display', showLabels ? 'block' : 'none');

  simulation.on('tick', () => {
    link
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y);

    nodeG.attr('transform', d => `translate(${d.x},${d.y})`);
  });

  // Store references for later updates
  svg._nodeG = nodeG;
  svg._link = link;
}

function getRadius(d) {
  return NODE_RADIUS[d.type] || 6;
}

function truncate(str, max) {
  if (!str) return '';
  return str.length > max ? str.slice(0, max) + '…' : str;
}

function lighten(hex) {
  // Return semi-transparent version as stroke
  return hex + '80';
}

// Drag handlers
function dragStart(event, d) {
  if (!event.active) simulation.alphaTarget(0.3).restart();
  d.fx = d.x; d.fy = d.y;
}
function dragging(event, d) { d.fx = event.x; d.fy = event.y; }
function dragEnd(event, d) {
  if (!event.active) simulation.alphaTarget(0);
  d.fx = null; d.fy = null;
}

// ── Node Selection ─────────────────────────────────────────────────────────
function selectNode(d, nodeG, link) {
  selectedNodeId = d.id;
  showNodeDetail(d.id);

  if (!nodeG) { nodeG = svg._nodeG; link = svg._link; }

  // Highlight connected nodes
  const connected = new Set([d.id]);
  graphData.edges.forEach(e => {
    if (e.source === d.id || e.source.id === d.id) connected.add(typeof e.target === 'object' ? e.target.id : e.target);
    if (e.target === d.id || e.target.id === d.id) connected.add(typeof e.source === 'object' ? e.source.id : e.source);
  });

  nodeG.selectAll('.node-circle')
    .classed('selected', n => n.id === d.id)
    .classed('dimmed', n => !connected.has(n.id));

  link
    .classed('active', l => {
      const s = l.source.id || l.source;
      const t = l.target.id || l.target;
      return s === d.id || t === d.id;
    })
    .classed('dimmed', l => {
      const s = l.source.id || l.source;
      const t = l.target.id || l.target;
      return s !== d.id && t !== d.id;
    });
}

function deselectNode() {
  selectedNodeId = null;
  document.getElementById('nodeDetail').style.display = 'none';
  if (svg._nodeG) {
    svg._nodeG.selectAll('.node-circle').classed('selected dimmed highlighted', false);
    svg._link.classed('active dimmed', false);
  }
}

async function showNodeDetail(nodeId) {
  const detail = document.getElementById('nodeDetail');
  try {
    const res = await fetch(`${API}/api/node/${encodeURIComponent(nodeId)}`);
    const data = await res.json();

    document.getElementById('nodeDetailTitle').textContent = data.label;

    const badge = document.getElementById('nodeDetailType');
    badge.textContent = data.type;
    badge.style.background = data.color + '30';
    badge.style.color = data.color;

    // Props
    const propsEl = document.getElementById('nodeDetailProps');
    const props = data.properties || {};
    const propKeys = Object.keys(props).slice(0, 12);
    propsEl.innerHTML = propKeys.map(k => {
      const val = props[k];
      if (!val || val === 'None' || val === 'null') return '';
      const display = String(val).length > 30 ? String(val).slice(0, 30) + '…' : String(val);
      return `<div class="prop-row"><span class="prop-key">${k}</span><span class="prop-val">${display}</span></div>`;
    }).join('') + (Object.keys(props).length > 12 ? `<div class="prop-row"><span class="prop-val hidden">+ ${Object.keys(props).length - 12} more fields hidden</span></div>` : '');

    // Connections count
    document.getElementById('neighborCount').textContent = data.connections;

    // Neighbors
    const neighborList = document.getElementById('neighborList');
    neighborList.innerHTML = data.neighbors.slice(0, 15).map(n => `
      <div class="neighbor-item" onclick="focusNode('${n.id}')">
        <div class="neighbor-dot" style="background:${n.color}"></div>
        <span class="neighbor-label">${truncate(n.label, 20)}</span>
        <span class="neighbor-dir">${n.direction === 'incoming' ? '←' : '→'}</span>
        <span class="neighbor-edge">${n.edge_label}</span>
      </div>
    `).join('');

    detail.style.display = 'block';
  } catch (err) {
    console.error('Failed to load node detail:', err);
  }
}

function focusNode(nodeId) {
  // Find node in graph data and trigger select
  const nodeData = graphData.nodes.find(n => n.id === nodeId);
  if (nodeData) {
    showNodeDetail(nodeId);
    // Zoom to node
    const simNode = svg._nodeG ? svg._nodeG.data().find(n => n.id === nodeId) : null;
    if (simNode) {
      const W = document.getElementById('graphContainer').clientWidth;
      const H = document.getElementById('graphContainer').clientHeight;
      svg.transition().duration(600).call(
        svg._zoom.transform,
        d3.zoomIdentity.translate(W/2, H/2).scale(1.5).translate(-simNode.x, -simNode.y)
      );
    }
  }
}

// ── Highlight nodes from chat ──────────────────────────────────────────────
function highlightNodes(nodeIds) {
  highlightedNodeIds = new Set(nodeIds);
  if (!svg._nodeG) return;
  svg._nodeG.selectAll('.node-circle')
    .classed('highlighted', d => highlightedNodeIds.has(d.id));

  // Zoom to first highlighted node
  if (nodeIds.length > 0) {
    focusNode(nodeIds[0]);
  }
}

// ── Controls ───────────────────────────────────────────────────────────────
function initControls() {
  // Fit view
  document.getElementById('fitBtn').addEventListener('click', fitView);
  document.getElementById('btnMinimize').addEventListener('click', fitView);

  // Toggle labels
  document.getElementById('btnToggleOverlay').addEventListener('click', () => {
    showLabels = !showLabels;
    d3.selectAll('.node-label').style('display', showLabels ? 'block' : 'none');
    document.getElementById('overlayBtnText').textContent = showLabels ? 'Hide Labels' : 'Show Labels';
  });

  // Close node detail
  document.getElementById('closeNodeDetail').addEventListener('click', deselectNode);

  // SQL modal close
  document.getElementById('closeSqlModal').addEventListener('click', () => {
    document.getElementById('sqlModal').style.display = 'none';
  });
  document.getElementById('sqlModal').addEventListener('click', (e) => {
    if (e.target === document.getElementById('sqlModal')) {
      document.getElementById('sqlModal').style.display = 'none';
    }
  });
}

function fitView() {
  const container = document.getElementById('graphContainer');
  const W = container.clientWidth;
  const H = container.clientHeight;
  svg.transition().duration(500).call(
    svg._zoom.transform,
    d3.zoomIdentity.translate(W/2, H/2).scale(0.5).translate(-W/2, -H/2)
  );
}

// ── Search ─────────────────────────────────────────────────────────────────
function initSearch() {
  const input = document.getElementById('searchInput');
  const results = document.getElementById('searchResults');
  let searchTimeout;

  input.addEventListener('input', () => {
    clearTimeout(searchTimeout);
    const q = input.value.trim();
    if (q.length < 2) { results.classList.remove('open'); return; }
    searchTimeout = setTimeout(() => performSearch(q), 250);
  });

  input.addEventListener('blur', () => {
    setTimeout(() => results.classList.remove('open'), 200);
  });
}

async function performSearch(q) {
  try {
    const res = await fetch(`${API}/api/search?q=${encodeURIComponent(q)}&limit=8`);
    const data = await res.json();
    const results = document.getElementById('searchResults');
    if (!data.results.length) { results.classList.remove('open'); return; }

    results.innerHTML = data.results.map(r => `
      <div class="search-result-item" onclick="focusNode('${r.id}'); document.getElementById('searchResults').classList.remove('open');">
        <div class="search-result-dot" style="background:${r.color}"></div>
        <span class="search-result-label">${truncate(r.label, 30)}</span>
        <span class="search-result-type">${r.type}</span>
      </div>
    `).join('');
    results.classList.add('open');
  } catch (e) { console.error('Search error:', e); }
}

// ── Chat ───────────────────────────────────────────────────────────────────
function initChat() {
  const input = document.getElementById('chatInput');
  const sendBtn = document.getElementById('sendBtn');

  sendBtn.addEventListener('click', sendMessage);
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });
  // Auto-resize textarea
  input.addEventListener('input', () => {
    input.style.height = 'auto';
    input.style.height = Math.min(input.scrollHeight, 100) + 'px';
  });

  // Suggestion chips
  document.querySelectorAll('.suggestion-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      document.getElementById('chatInput').value = chip.dataset.q;
      sendMessage();
    });
  });
}

async function sendMessage() {
  const input = document.getElementById('chatInput');
  const msg = input.value.trim();
  if (!msg) return;

  input.value = '';
  input.style.height = 'auto';

  // Hide suggestions
  document.getElementById('chatSuggestions').style.display = 'none';

  // Add user message
  appendUserMessage(msg);
  chatHistory.push({ role: 'user', content: msg });

  // Show typing indicator
  const typingId = showTyping();
  setStatus('thinking', 'DodgeAI is thinking…');

  try {
    // Use streaming endpoint
    const response = await fetch(`${API}/api/chat/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: msg,
        history: chatHistory.slice(-6)
      })
    });

    removeTyping(typingId);
    const msgEl = appendAssistantMessage('');
    const bubbleEl = msgEl.querySelector('.msg-bubble-text');
    let fullAnswer = '';
    let sqlRef = null;
    let dataRef = null;
    let nodesRef = [];

    const reader = response.body.getReader();
    const decoder = new TextDecoder();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const text = decoder.decode(value);
      const lines = text.split('\n');

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const payload = JSON.parse(line.slice(6));
          if (payload.type === 'token') {
            fullAnswer += payload.content;
            bubbleEl.textContent = fullAnswer;
            scrollChat();
          } else if (payload.type === 'done') {
            sqlRef = payload.sql;
            dataRef = payload.data;
            nodesRef = payload.highlighted_nodes || [];
            // Render markdown-like formatting
            bubbleEl.innerHTML = formatAnswer(fullAnswer);
            // Add SQL button if applicable
            if (sqlRef) {
              const sqlBtn = document.createElement('button');
              sqlBtn.className = 'msg-sql-btn';
              sqlBtn.textContent = '🔍 View SQL & Data';
              sqlBtn.onclick = () => showSqlModal(sqlRef, dataRef);
              msgEl.querySelector('.msg-bubble').appendChild(sqlBtn);
            }
          } else if (payload.type === 'error') {
            bubbleEl.textContent = `Error: ${payload.content}`;
          }
        } catch (e) { /* skip malformed */ }
      }
    }

    chatHistory.push({ role: 'assistant', content: fullAnswer });

    // Highlight referenced nodes
    if (nodesRef.length > 0) {
      highlightNodes(nodesRef);
    }

    setStatus('ready', 'DodgeAI is awaiting instructions');

  } catch (err) {
    removeTyping(typingId);
    appendAssistantMessage(`Sorry, I encountered an error: ${err.message}. Please check that the server is running and your API key is configured.`);
    setStatus('ready', 'DodgeAI is awaiting instructions');
  }
}

function formatAnswer(text) {
  // Simple markdown-like rendering
  return text
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.*?)\*/g, '<em>$1</em>')
    .replace(/`(.*?)`/g, '<code style="background:rgba(79,142,247,0.1);padding:1px 4px;border-radius:3px;font-family:monospace;font-size:11px;color:#2563eb">$1</code>')
    .replace(/\n\n/g, '</p><p>')
    .replace(/\n- (.*)/g, '<li>$1</li>')
    .replace(/(<li>.*<\/li>)+/g, '<ul style="margin:6px 0 0 16px">$&</ul>')
    .replace(/\n/g, '<br>');
}

function appendUserMessage(text) {
  const el = document.createElement('div');
  el.className = 'chat-msg user';
  el.innerHTML = `
    <div class="msg-avatar user-av">You</div>
    <div class="msg-bubble"><p>${escapeHtml(text)}</p></div>
  `;
  document.getElementById('chatMessages').appendChild(el);
  scrollChat();
  return el;
}

function appendAssistantMessage(text) {
  const el = document.createElement('div');
  el.className = 'chat-msg assistant';
  el.innerHTML = `
    <div class="msg-avatar">D</div>
    <div class="msg-bubble">
      <div class="msg-name">DodgeAI <span class="msg-role-tag">Graph Agent</span></div>
      <div class="msg-bubble-text">${text}</div>
    </div>
  `;
  document.getElementById('chatMessages').appendChild(el);
  scrollChat();
  return el;
}

function showTyping() {
  const id = 'typing-' + Date.now();
  const el = document.createElement('div');
  el.className = 'chat-msg assistant';
  el.id = id;
  el.innerHTML = `
    <div class="msg-avatar">D</div>
    <div class="msg-bubble">
      <div class="typing-indicator"><span></span><span></span><span></span></div>
    </div>
  `;
  document.getElementById('chatMessages').appendChild(el);
  scrollChat();
  return id;
}

function removeTyping(id) {
  const el = document.getElementById(id);
  if (el) el.remove();
}

function scrollChat() {
  const el = document.getElementById('chatMessages');
  el.scrollTop = el.scrollHeight;
}

function setStatus(state, text) {
  const dot = document.querySelector('.status-dot');
  const textEl = document.getElementById('statusText');
  dot.className = 'status-dot' + (state === 'thinking' ? ' thinking' : '');
  textEl.textContent = text;
}

function showSqlModal(sql, data) {
  document.getElementById('sqlCode').textContent = sql || '-- No SQL generated';
  const tableWrap = document.getElementById('dataTableWrap');

  if (data && data.rows && data.rows.length > 0) {
    const cols = data.columns;
    tableWrap.innerHTML = `
      <table class="data-table">
        <thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead>
        <tbody>${data.rows.map(r => `
          <tr>${cols.map(c => `<td title="${escapeHtml(String(r[c] ?? ''))}">${escapeHtml(truncate(String(r[c] ?? ''), 30))}</td>`).join('')}</tr>
        `).join('')}</tbody>
      </table>
      <p style="color:var(--text-3);font-size:11px;padding:8px 0">${data.count} rows returned</p>
    `;
  } else if (data && data.error) {
    tableWrap.innerHTML = `<p style="color:#FF6B6B;font-size:12px">Query error: ${escapeHtml(data.error)}</p>`;
  } else {
    tableWrap.innerHTML = '<p style="color:var(--text-3);font-size:12px">No data returned</p>';
  }

  document.getElementById('sqlModal').style.display = 'grid';
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ── Start ──────────────────────────────────────────────────────────────────
window.focusNode = focusNode;
window.showSqlModal = showSqlModal;
init();
