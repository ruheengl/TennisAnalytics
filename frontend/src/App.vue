<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
import * as d3 from 'd3'

const players = ref(buildMockPlayers(2200))
const hoveredPlayerId = ref(null)
const selectedPlayerId = ref(null)
const query = reactive({
  name: '',
  cluster: 'all',
  winRateMin: 35,
  winRateMax: 95,
  metric: 'elo'
})
const appliedQuery = ref({ ...query })
let debounceHandle

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

const filteredPlayers = computed(() => {
  const q = appliedQuery.value
  return players.value.filter((p) => {
    const matchesName = p.name.toLowerCase().includes(q.name.toLowerCase())
    const matchesCluster = q.cluster === 'all' || p.cluster === q.cluster
    const matchesWinRate = p.winRate >= q.winRateMin && p.winRate <= q.winRateMax
    return matchesName && matchesCluster && matchesWinRate
  })
})

const selectedPlayer = computed(() =>
  players.value.find((p) => p.id === (selectedPlayerId.value ?? hoveredPlayerId.value)) ?? filteredPlayers.value[0]
)

const degradationRanking = computed(() => {
  return filteredPlayers.value
    .map((p) => ({
      ...p,
      degradation: computeSlope(p.timeseries)
    }))
    .sort((a, b) => a.degradation - b.degradation)
    .slice(0, 20)
})

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
watch(selectedPlayer, drawPerformance)
watch([degradationRanking, selectedPlayer], drawDegradation)

onMounted(() => {
  drawClusterPlot()
  drawPerformance()
  drawDegradation()
  drawTree()
  drawFeatureImportance()
})


function computeSlope(series) {
  const xMean = d3.mean(series, (d) => d.season)
  const yMean = d3.mean(series, (d) => d.elo)
  let num = 0
  let den = 0
  for (const point of series) {
    num += (point.season - xMean) * (point.elo - yMean)
    den += (point.season - xMean) ** 2
  }
  return den === 0 ? 0 : num / den
}

function drawClusterPlot() {
  const width = 640
  const height = 340
  const margin = { top: 20, right: 20, bottom: 40, left: 50 }
  const data = filteredPlayers.value

  const x = d3
    .scaleLinear()
    .domain(d3.extent(players.value, (d) => d.projectionX))
    .nice()
    .range([margin.left, width - margin.right])

  const y = d3
    .scaleLinear()
    .domain(d3.extent(players.value, (d) => d.projectionY))
    .nice()
    .range([height - margin.bottom, margin.top])

  const color = d3.scaleOrdinal(d3.schemeTableau10).domain(['A', 'B', 'C', 'D', 'E'])

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

  svg
    .append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(d3.axisBottom(x))

  svg
    .append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(d3.axisLeft(y))

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
  const player = selectedPlayer.value
  if (!player || !performanceSvg.value) return

  const metrics = ['elo', 'winRate', 'aceRate']
  const colors = d3.scaleOrdinal().domain(metrics).range(['#1f77b4', '#2ca02c', '#d62728'])
  const width = 640
  const height = 300
  const margin = { top: 16, right: 20, bottom: 42, left: 48 }

  const data = player.timeseries
  const x = d3
    .scaleLinear()
    .domain(d3.extent(data, (d) => d.season))
    .range([margin.left, width - margin.right])

  const y = d3
    .scaleLinear()
    .domain([0, d3.max(data, (d) => Math.max(d.elo / 20, d.winRate, d.aceRate * 2))])
    .nice()
    .range([height - margin.bottom, margin.top])

  const svg = d3.select(performanceSvg.value).attr('viewBox', `0 0 ${width} ${height}`)
  svg.selectAll('*').remove()

  const g = svg.append('g')
  const axisX = g.append('g').attr('transform', `translate(0,${height - margin.bottom})`).call(d3.axisBottom(x))
  g.append('g').attr('transform', `translate(${margin.left},0)`).call(d3.axisLeft(y))

  const lines = [
    { key: 'elo', accessor: (d) => d.elo / 20 },
    { key: 'winRate', accessor: (d) => d.winRate },
    { key: 'aceRate', accessor: (d) => d.aceRate * 2 }
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
      axisX.call(d3.axisBottom(x))
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
  if (!degradationSvg.value) return
  const rankData = degradationRanking.value
  const chosen = selectedPlayer.value ?? rankData[0]
  if (!chosen) return

  const width = 640
  const height = 220
  const margin = { top: 20, right: 20, bottom: 36, left: 46 }
  const x = d3
    .scaleLinear()
    .domain(d3.extent(chosen.timeseries, (d) => d.season))
    .range([margin.left, width - margin.right])

  const y = d3
    .scaleLinear()
    .domain(d3.extent(chosen.timeseries, (d) => d.elo))
    .nice()
    .range([height - margin.bottom, margin.top])

  const svg = d3.select(degradationSvg.value).attr('viewBox', `0 0 ${width} ${height}`)
  svg.selectAll('*').remove()
  svg
    .append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(d3.axisBottom(x).tickFormat(d3.format('d')))
  svg
    .append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(d3.axisLeft(y))

  svg
    .append('path')
    .datum(chosen.timeseries)
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

  svg
    .append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(d3.axisBottom(x).ticks(5).tickFormat(d3.format('.0%')))

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

function buildMockPlayers(count) {
  const clusters = ['A', 'B', 'C', 'D', 'E']
  return Array.from({ length: count }, (_, idx) => {
    const baseElo = 1450 + Math.random() * 550
    const trend = -8 + Math.random() * 14
    const timeseries = d3.range(2016, 2027).map((season) => ({
      season,
      elo: baseElo + trend * (season - 2016) + (Math.random() * 50 - 25),
      winRate: 45 + Math.random() * 45,
      aceRate: 2 + Math.random() * 10
    }))

    return {
      id: idx + 1,
      name: `Player ${idx + 1}`,
      cluster: clusters[idx % clusters.length],
      projectionX: d3.randomNormal(0, 1)(),
      projectionY: d3.randomNormal(0, 1)(),
      currentElo: timeseries[timeseries.length - 1].elo,
      winRate: timeseries[timeseries.length - 1].winRate,
      timeseries
    }
  })
}
</script>

<template>
  <main>
    <h1>Player Clustering + Degradation Explorer</h1>

    <section class="panel">
      <h2>Cluster Search / Query</h2>
      <div class="filters">
        <label>
          Player name
          <input v-model="query.name" placeholder="Search player" />
        </label>
        <label>
          Cluster
          <select v-model="query.cluster">
            <option value="all">All</option>
            <option value="A">A</option>
            <option value="B">B</option>
            <option value="C">C</option>
            <option value="D">D</option>
            <option value="E">E</option>
          </select>
        </label>
        <label>
          Win rate min
          <input type="range" min="30" max="95" step="1" v-model.number="query.winRateMin" />
          {{ query.winRateMin }}%
        </label>
        <label>
          Win rate max
          <input type="range" min="35" max="100" step="1" v-model.number="query.winRateMax" />
          {{ query.winRateMax }}%
        </label>
      </div>
    </section>

    <section class="panel">
      <h2>Cluster Overview</h2>
      <p class="subtle">Scatter projection using D3 joins; canvas fallback enabled for high point counts.</p>
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
      <p class="subtle">Multi-metric trend lines with brush-to-zoom behavior.</p>
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
            <span>{{ player.degradation.toFixed(2) }}</span>
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
