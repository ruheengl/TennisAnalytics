<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
import * as d3 from 'd3'

const API_BASE = '/api'

const players = ref([])
const hoveredPlayerId = ref(null)
const selectedPlayerId = ref(null)
const loading = ref(false)
const loadError = ref('')
const selectedSeries = ref([])
const degradationScores = ref({})
const query = reactive({
  name: '',
  cluster: 'all',
  winRateMin: 35,
  winRateMax: 95,
  metric: 'elo'
})
const appliedQuery = ref({ ...query })
let debounceHandle

const clusterRequestId = ref(null)
const clusterSvg = ref()
const clusterCanvas = ref()
const performanceSvg = ref()
const degradationSvg = ref()
const treeSvg = ref()
const featureSvg = ref()

const collapsedTreeNodes = ref(new Set())
const treeBlueprint = {
  name: 'Start',
  id: 'root',
  children: [
    {
      name: 'Serve Win% > 65',
      id: 'serve-high',
      children: [
        { name: 'Aggressive Baseline', id: 'style-agg' },
        { name: 'All-court', id: 'style-all' }
      ]
    },
    {
      name: 'Serve Win% <= 65',
      id: 'serve-low',
      children: [
        { name: 'Counterpuncher', id: 'style-counter' },
        { name: 'Defensive Grinder', id: 'style-def' }
      ]
    }
  ]
}
const collapsibleNodeIds = new Set(['root', 'serve-high', 'serve-low'])
const useCanvas = computed(() => filteredPlayers.value.length > 1500)

const clusterOptions = computed(() => {
  const labels = [...new Set(players.value.map((p) => p.cluster))]
  return labels.sort((a, b) => d3.ascending(a, b))
})

const filteredPlayers = computed(() => {
  const q = appliedQuery.value
  return players.value.filter((p) => {
    const matchesName = p.name.toLowerCase().includes(q.name.toLowerCase())
    const matchesCluster = q.cluster === 'all' || String(p.cluster) === String(q.cluster)
    const matchesWinRate = p.winRate >= q.winRateMin && p.winRate <= q.winRateMax
    return matchesName && matchesCluster && matchesWinRate
  })
})

const clusterSummary = computed(() => {
  const grouped = d3.rollups(
    filteredPlayers.value,
    (v) => ({
      count: v.length,
      meanWinRate: d3.mean(v, (d) => d.winRate),
      meanElo: d3.mean(v, (d) => d.currentElo)
    }),
    (d) => d.cluster
  )

  return grouped
    .map(([cluster, values]) => ({ cluster, ...values }))
    .sort((a, b) => d3.ascending(a.cluster, b.cluster))
})

const selectedPlayer = computed(() =>
  players.value.find((p) => p.id === (selectedPlayerId.value ?? hoveredPlayerId.value)) ?? filteredPlayers.value[0]
)

const degradationRanking = computed(() => {
  return filteredPlayers.value
    .map((p) => ({
      ...p,
      degradation: degradationScores.value[p.id]
    }))
    .filter((p) => p.degradation !== undefined)
    .sort((a, b) => a.degradation - b.degradation)
    .slice(0, 20)
})

watch(
  () => ({ ...query }),
  () => {
    clearTimeout(debounceHandle)
    debounceHandle = setTimeout(() => {
      appliedQuery.value = { ...query }
    }, 250)
  },
  { deep: true }
)

watch([filteredPlayers, useCanvas], drawClusterPlot)
watch(selectedSeries, () => {
  drawPerformance()
  drawDegradation()
})
watch(selectedPlayerId, () => {
  loadSelectedPlayerSeries()
})
watch(filteredPlayers, () => {
  hydrateDegradationScores()
})

onMounted(async () => {
  drawTree()
  drawFeatureImportance()
  await loadDashboardData()
})

async function apiGet(path, params = {}) {
  const url = new URL(`${API_BASE}${path}`)
  Object.entries(params).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== '') {
      url.searchParams.set(key, value)
    }
  })
  const response = await fetch(url)
  if (!response.ok) {
    const body = await response.text()
    throw new Error(body || `GET ${path} failed (${response.status})`)
  }
  return response.json()
}

async function apiPost(path, payload) {
  const response = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  })
  if (!response.ok) {
    const body = await response.text()
    throw new Error(body || `POST ${path} failed (${response.status})`)
  }
  return response.json()
}

async function loadDashboardData() {
  loading.value = true
  loadError.value = ''
  try {
    const health = await apiGet('/health')
    const clusterPayload = {
      attributes: health.default_attributes,
      k: 6,
      distance_metric: 'euclidean',
      scaling: 'zscore',
      max_iter: 50,
      seed: 42,
      filters: {}
    }
    const clusterResult = await apiPost('/cluster', clusterPayload)
    clusterRequestId.value = clusterResult.cluster_request_id

    const [clusterPlayersResp, playerTableResp] = await Promise.all([
      apiGet(`/clusters/${clusterResult.cluster_request_id}/players`, { page_size: 2000 }),
      apiPost('/players/query', { filters: [], limit: 2000, offset: 0, sort_by: 'career_win_pct', sort_order: 'desc' })
    ])

    const clusterByPlayer = Object.fromEntries(clusterPlayersResp.players.map((p) => [p.player_id, p.cluster_id]))

    players.value = playerTableResp.players
      .filter((row) => row.player_id in clusterByPlayer)
      .map((row, idx) => ({
        id: row.player_id,
        name: row.player_id,
        cluster: clusterByPlayer[row.player_id],
        projectionX: d3.randomNormal(0, 1)(),
        projectionY: d3.randomNormal(0, 1)(),
        currentElo: Number(row.elo_pre ?? 0),
        winRate: Number((row.career_win_pct ?? 0) * 100),
        _sortIdx: idx
      }))

    if (players.value.length > 0) {
      selectedPlayerId.value = players.value[0].id
      await loadSelectedPlayerSeries()
      await hydrateDegradationScores()
    }

    drawClusterPlot()
  } catch (err) {
    loadError.value = err instanceof Error ? err.message : String(err)
  } finally {
    loading.value = false
  }
}

async function loadSelectedPlayerSeries() {
  const player = selectedPlayer.value
  if (!player?.id) return
  try {
    const [eloRes, winRes, aceRes] = await Promise.all([
      apiGet(`/players/${encodeURIComponent(player.id)}/metrics/timeseries`, { metric: 'elo', limit: 400 }),
      apiGet(`/players/${encodeURIComponent(player.id)}/metrics/timeseries`, { metric: 'win_pct', limit: 400 }),
      apiGet(`/players/${encodeURIComponent(player.id)}/metrics/timeseries`, { metric: 'ace_pct', limit: 400 })
    ])

    const byDate = new Map()
    for (const point of eloRes.points) {
      byDate.set(point.match_date, {
        season: Number(String(point.match_date).slice(0, 4)),
        elo: Number(point.value ?? 0),
        winRate: null,
        aceRate: null
      })
    }

    for (const point of winRes.points) {
      const rec = byDate.get(point.match_date) ?? {
        season: Number(String(point.match_date).slice(0, 4)),
        elo: null,
        winRate: null,
        aceRate: null
      }
      rec.winRate = Number(point.value ?? 0) * 100
      byDate.set(point.match_date, rec)
    }

    for (const point of aceRes.points) {
      const rec = byDate.get(point.match_date) ?? {
        season: Number(String(point.match_date).slice(0, 4)),
        elo: null,
        winRate: null,
        aceRate: null
      }
      rec.aceRate = Number(point.value ?? 0) * 100
      byDate.set(point.match_date, rec)
    }

    selectedSeries.value = [...byDate.values()]
      .filter((d) => d.elo !== null)
      .sort((a, b) => d3.ascending(a.season, b.season))
      .map((d, idx, arr) => ({
        season: d.season + idx / Math.max(arr.length, 1),
        elo: d.elo ?? 0,
        winRate: d.winRate ?? 0,
        aceRate: d.aceRate ?? 0
      }))
  } catch {
    selectedSeries.value = []
  }
}

async function hydrateDegradationScores() {
  const subset = filteredPlayers.value.slice(0, 40)
  const missingIds = subset
    .map((p) => p.id)
    .filter((id) => degradationScores.value[id] === undefined)

  if (missingIds.length === 0) return

  const fetched = await Promise.all(
    missingIds.map(async (id) => {
      try {
        const res = await apiGet(`/players/${encodeURIComponent(id)}/metrics/degradation`, { metric: 'elo', limit: 300 })
        return [id, Number(res.degradation?.slope ?? 0)]
      } catch {
        return [id, 0]
      }
    })
  )

  degradationScores.value = {
    ...degradationScores.value,
    ...Object.fromEntries(fetched)
  }
}

function drawClusterPlot() {
  if (!clusterSvg.value) return

  const width = 640
  const height = 340
  const margin = { top: 20, right: 20, bottom: 40, left: 50 }
  const data = filteredPlayers.value

  const x = d3.scaleLinear().domain(d3.extent(players.value, (d) => d.projectionX)).nice().range([margin.left, width - margin.right])

  const y = d3
    .scaleLinear()
    .domain(d3.extent(players.value, (d) => d.projectionY))
    .nice()
    .range([height - margin.bottom, margin.top])

  const color = d3.scaleOrdinal(d3.schemeTableau10).domain(clusterOptions.value)

  if (useCanvas.value && clusterCanvas.value) {
    const ctx = clusterCanvas.value.getContext('2d')
    clusterCanvas.value.width = width
    clusterCanvas.value.height = height
    ctx.clearRect(0, 0, width, height)

    for (const d of data) {
      ctx.beginPath()
      ctx.fillStyle = d.id === (selectedPlayerId.value ?? hoveredPlayerId.value) ? '#111827' : color(d.cluster)
      ctx.globalAlpha = 0.75
      ctx.arc(x(d.projectionX), y(d.projectionY), 3, 0, Math.PI * 2)
      ctx.fill()
    }
  }

  const svg = d3.select(clusterSvg.value).attr('viewBox', `0 0 ${width} ${height}`)
  svg.selectAll('*').remove()

  svg.append('g').attr('transform', `translate(0,${height - margin.bottom})`).call(d3.axisBottom(x))

  svg.append('g').attr('transform', `translate(${margin.left},0)`).call(d3.axisLeft(y))

  if (!useCanvas.value) {
    svg
      .append('g')
      .selectAll('circle')
      .data(data, (d) => d.id)
      .join('circle')
      .attr('cx', (d) => x(d.projectionX))
      .attr('cy', (d) => y(d.projectionY))
      .attr('r', 3)
      .attr('fill', (d) => color(d.cluster))
      .attr('opacity', (d) => (d.id === hoveredPlayerId.value ? 1 : 0.75))
      .on('mouseenter', (_, d) => {
        hoveredPlayerId.value = d.id
      })
      .on('click', (_, d) => {
        selectedPlayerId.value = d.id
      })
  }
}

function drawPerformance() {
  const data = selectedSeries.value
  if (!data?.length || !performanceSvg.value) return

  const metrics = ['elo', 'winRate', 'aceRate']
  const colors = d3.scaleOrdinal().domain(metrics).range(['#1f77b4', '#2ca02c', '#d62728'])
  const width = 640
  const height = 300
  const margin = { top: 16, right: 20, bottom: 42, left: 48 }

  const x = d3
    .scaleLinear()
    .domain(d3.extent(data, (d) => d.season))
    .range([margin.left, width - margin.right])

  const y = d3
    .scaleLinear()
    .domain([0, d3.max(data, (d) => Math.max(d.elo / 20, d.winRate, d.aceRate))])
    .nice()
    .range([height - margin.bottom, margin.top])

  const svg = d3.select(performanceSvg.value).attr('viewBox', `0 0 ${width} ${height}`)
  svg.selectAll('*').remove()

  const g = svg.append('g')
  const axisX = g.append('g').attr('transform', `translate(0,${height - margin.bottom})`).call(d3.axisBottom(x).tickFormat(d3.format('d')))
  g.append('g').attr('transform', `translate(${margin.left},0)`).call(d3.axisLeft(y))

  const lines = [
    { key: 'elo', accessor: (d) => d.elo / 20 },
    { key: 'winRate', accessor: (d) => d.winRate },
    { key: 'aceRate', accessor: (d) => d.aceRate }
  ]

  g.selectAll('.metric-line')
    .data(lines, (d) => d.key)
    .join('path')
    .attr('class', 'metric-line')
    .attr('fill', 'none')
    .attr('stroke-width', 2)
    .attr('stroke', (d) => colors(d.key))
    .attr(
      'd',
      (metric) =>
        d3
          .line()
          .x((d) => x(d.season))
          .y((d) => y(metric.accessor(d)))(data)
    )

  const brush = d3
    .brushX()
    .extent([
      [margin.left, margin.top],
      [width - margin.right, height - margin.bottom]
    ])
    .on('brush end', (event) => {
      if (!event.selection) return
      const [x0, x1] = event.selection
      const newDomain = [x.invert(x0), x.invert(x1)]
      x.domain(newDomain)
      axisX.call(d3.axisBottom(x).tickFormat(d3.format('d')))
      g.selectAll('.metric-line').attr(
        'd',
        (metric) =>
          d3
            .line()
            .x((d) => x(d.season))
            .y((d) => y(metric.accessor(d)))(data)
      )
    })

  g.append('g').call(brush)
}

function drawDegradation() {
  if (!degradationSvg.value || !selectedSeries.value?.length) return

  const width = 640
  const height = 220
  const margin = { top: 20, right: 20, bottom: 36, left: 46 }
  const x = d3
    .scaleLinear()
    .domain(d3.extent(selectedSeries.value, (d) => d.season))
    .range([margin.left, width - margin.right])

  const y = d3
    .scaleLinear()
    .domain(d3.extent(selectedSeries.value, (d) => d.elo))
    .nice()
    .range([height - margin.bottom, margin.top])

  const svg = d3.select(degradationSvg.value).attr('viewBox', `0 0 ${width} ${height}`)
  svg.selectAll('*').remove()
  svg.append('g').attr('transform', `translate(0,${height - margin.bottom})`).call(d3.axisBottom(x).tickFormat(d3.format('d')))
  svg.append('g').attr('transform', `translate(${margin.left},0)`).call(d3.axisLeft(y))

  svg
    .append('path')
    .datum(selectedSeries.value)
    .attr('fill', 'none')
    .attr('stroke', '#7c3aed')
    .attr('stroke-width', 2)
    .attr(
      'd',
      d3
        .line()
        .x((d) => x(d.season))
        .y((d) => y(d.elo))
    )
}

function drawTree() {
  const width = 640
  const height = 380
  const fold = (node) => ({
    ...node,
    children:
      node.children && !collapsedTreeNodes.value.has(node.id)
        ? node.children.map(fold)
        : undefined
  })

  const root = d3.hierarchy(fold(treeBlueprint))
  d3.tree().size([height - 20, width - 80])(root)

  const svg = d3.select(treeSvg.value).attr('viewBox', `0 0 ${width} ${height}`)
  svg.selectAll('*').remove()

  const g = svg.append('g').attr('transform', 'translate(40,10)')

  g.selectAll('path')
    .data(root.links())
    .join('path')
    .attr('fill', 'none')
    .attr('stroke', '#9ca3af')
    .attr('d', d3.linkHorizontal().x((d) => d.y).y((d) => d.x))

  const node = g
    .selectAll('g.node')
    .data(root.descendants())
    .join('g')
    .attr('class', 'node')
    .attr('transform', (d) => `translate(${d.y},${d.x})`)

  node
    .append('circle')
    .attr('r', 5)
    .attr('fill', '#2563eb')
    .style('cursor', 'pointer')
    .on('click', (_, d) => {
      if (!collapsibleNodeIds.has(d.data.id)) return
      if (collapsedTreeNodes.value.has(d.data.id)) {
        collapsedTreeNodes.value.delete(d.data.id)
      } else {
        collapsedTreeNodes.value.add(d.data.id)
      }
      drawTree()
    })

  node
    .append('text')
    .attr('x', 10)
    .attr('dy', 4)
    .text((d) => d.data.name)
}

function drawFeatureImportance() {
  if (!featureSvg.value) return
  const features = [
    { feature: 'Serve Win%', value: 0.28 },
    { feature: 'Return Win%', value: 0.24 },
    { feature: 'Break Saved%', value: 0.18 },
    { feature: 'Rally Len.', value: 0.16 },
    { feature: 'Double Fault%', value: 0.14 }
  ]

  const width = 640
  const height = 220
  const margin = { top: 16, right: 20, bottom: 40, left: 120 }

  const x = d3.scaleLinear().domain([0, d3.max(features, (d) => d.value)]).range([margin.left, width - margin.right])
  const y = d3
    .scaleBand()
    .domain(features.map((d) => d.feature))
    .range([margin.top, height - margin.bottom])
    .padding(0.2)

  const svg = d3.select(featureSvg.value).attr('viewBox', `0 0 ${width} ${height}`)
  svg.selectAll('*').remove()

  svg.append('g').attr('transform', `translate(0,${height - margin.bottom})`).call(d3.axisBottom(x).ticks(5).tickFormat(d3.format('.0%')))

  svg.append('g').attr('transform', `translate(${margin.left},0)`).call(d3.axisLeft(y))

  svg
    .append('g')
    .selectAll('rect')
    .data(features, (d) => d.feature)
    .join('rect')
    .attr('x', margin.left)
    .attr('y', (d) => y(d.feature))
    .attr('height', y.bandwidth())
    .attr('width', (d) => x(d.value) - margin.left)
    .attr('fill', '#0ea5e9')
}
</script>

<template>
  <main>
    <h1>Player Clustering + Degradation Explorer</h1>

    <section class="panel">
      <h2>Cluster Search / Query</h2>
      <p v-if="loading" class="subtle">Loading player data from API…</p>
      <p v-if="loadError" class="error-text">{{ loadError }}</p>
      <div class="filters">
        <label>
          Player name
          <input v-model="query.name" placeholder="Search player" />
        </label>
        <label>
          Cluster
          <select v-model="query.cluster">
            <option value="all">All</option>
            <option v-for="cluster in clusterOptions" :key="cluster" :value="cluster">{{ cluster }}</option>
          </select>
        </label>
        <label>
          Win rate min
          <input type="range" min="0" max="100" step="1" v-model.number="query.winRateMin" />
          {{ query.winRateMin }}%
        </label>
        <label>
          Win rate max
          <input type="range" min="0" max="100" step="1" v-model.number="query.winRateMax" />
          {{ query.winRateMax }}%
        </label>
      </div>
    </section>

    <section class="panel">
      <h2>Cluster Overview</h2>
      <p class="subtle">Clusters and player table are loaded from FastAPI endpoints on localhost:8000.</p>
      <canvas v-show="useCanvas" ref="clusterCanvas" class="chart"></canvas>
      <svg ref="clusterSvg" class="chart"></svg>
      <div class="summary-grid">
        <article v-for="cluster in clusterSummary" :key="cluster.cluster" class="summary-card">
          <h3>Cluster {{ cluster.cluster }}</h3>
          <p>Players: {{ cluster.count }}</p>
          <p>Avg Win Rate: {{ cluster.meanWinRate?.toFixed(1) }}%</p>
          <p>Avg Elo: {{ cluster.meanElo?.toFixed(0) }}</p>
        </article>
      </div>
    </section>

    <section class="panel">
      <h2>Player Performance View</h2>
      <p class="subtle">Trend lines are pulled from `/players/{id}/metrics/timeseries`.</p>
      <p v-if="selectedPlayer">Focused Player: <strong>{{ selectedPlayer.name }}</strong></p>
      <svg ref="performanceSvg" class="chart"></svg>
    </section>

    <section class="panel split">
      <div>
        <h2>Degradation Explorer</h2>
        <ul class="rank-list">
          <li
            v-for="player in degradationRanking"
            :key="player.id"
            :class="{ selected: player.id === selectedPlayerId }"
            @mouseenter="hoveredPlayerId = player.id"
            @click="selectedPlayerId = player.id"
          >
            <span>{{ player.name }}</span>
            <span>{{ player.degradation?.toFixed(3) ?? '—' }}</span>
          </li>
        </ul>
      </div>
      <div>
        <h3>Linked Trend Chart</h3>
        <svg ref="degradationSvg" class="chart compact"></svg>
      </div>
    </section>

    <section class="panel">
      <h2>Decision-Tree Explorer</h2>
      <p class="subtle">Collapsible tree + feature importance bars.</p>
      <svg ref="treeSvg" class="chart"></svg>
      <svg ref="featureSvg" class="chart compact"></svg>
    </section>
  </main>
</template>
