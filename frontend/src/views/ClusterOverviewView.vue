<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import * as d3 from 'd3'

const props = defineProps({
  clusterResult: { type: Object, required: true },
  players: { type: Array, required: true },
  clusterPlayers: { type: Array, required: true },
  clusteringConfig: { type: Object, required: true },
  projectionMetadata: { type: Object, required: true },
  selectedClusterId: { type: String, required: true },
  activeStoryStep: { type: String, default: 'overview' },
  clusterRequestId: { type: String, default: '' }
})
const emit = defineEmits(['update:selectedClusterId', 'update:activeStoryStep'])

const svgRef = ref()
const selectedCluster = computed({
  get: () => props.selectedClusterId,
  set: (value) => emit('update:selectedClusterId', value)
})

const filtered = computed(() => {
  if (selectedCluster.value === 'all') return props.players
  return props.players.filter((p) => p.cluster_id === Number(selectedCluster.value))
})

const clusters = computed(() => {
  const labels = [...new Set(props.players.map((p) => p.cluster_id))]
  return labels.sort((a, b) => a - b)
})

const summaries = computed(() => {
  const grouped = d3.rollups(
    filtered.value,
    (rows) => ({
      count: rows.length,
      avgElo: d3.mean(rows, (r) => Number(r.elo_pre ?? 0)) ?? 0,
      avgWinPct: (d3.mean(rows, (r) => Number(r.career_win_pct ?? 0)) ?? 0) * 100,
      avgService: d3.mean(rows, (r) => Number(r.service_points_won_pct ?? 0)) ?? 0
    }),
    (r) => r.cluster_id
  )

  return grouped.map(([cluster, values]) => ({ cluster, ...values }))
})

function formatLoading(item) {
  const sign = item.loading >= 0 ? '+' : '-'
  return `${sign}${item.attribute}`
}

function componentLabel(componentIndex) {
  const ratio = Number(props.projectionMetadata.explainedVarianceRatio?.[componentIndex] ?? 0) * 100
  const varianceText = `${ratio.toFixed(1)}% variance`
  const loadings = props.projectionMetadata.topLoadings?.[componentIndex] ?? []
  const loadingText = loadings.length ? loadings.map(formatLoading).join(', ') : 'insufficient variance'
  return `PC${componentIndex + 1} (${varianceText}): ${loadingText}`
}

const xAxisLabel = 'Playing style dimension 1'
const yAxisLabel = 'Playing style dimension 2'

const projectionComponents = computed(() => {
  const varianceCount = props.projectionMetadata.explainedVarianceRatio?.length ?? 0
  const loadingCount = props.projectionMetadata.topLoadings?.length ?? 0
  const count = Math.max(varianceCount, loadingCount, 2)
  return Array.from({ length: count }, (_, index) => ({
    index,
    label: componentLabel(index)
  }))
})

function drawScatter() {
  const svg = d3.select(svgRef.value)
  const width = 940
  const height = 430
  const margin = { top: 24, right: 28, bottom: 48, left: 56 }

  svg.selectAll('*').remove()
  svg.attr('viewBox', `0 0 ${width} ${height}`)

  const x = d3
    .scaleLinear()
    .domain(d3.extent(filtered.value, (d) => Number(d.pc1 ?? 0)))
    .nice()
    .range([margin.left, width - margin.right])

  const y = d3
    .scaleLinear()
    .domain(d3.extent(filtered.value, (d) => Number(d.pc2 ?? 0)))
    .nice()
    .range([height - margin.bottom, margin.top])

  const color = d3.scaleOrdinal(d3.schemeTableau10).domain(clusters.value)

  svg
    .append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(d3.axisBottom(x).ticks(8))

  svg
    .append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(d3.axisLeft(y).ticks(7))

  svg
    .append('g')
    .selectAll('circle')
    .data(filtered.value)
    .join('circle')
    .attr('cx', (d) => x(Number(d.pc1 ?? 0)))
    .attr('cy', (d) => y(Number(d.pc2 ?? 0)))
    .attr('r', 4)
    .attr('fill', (d) => color(d.cluster_id))
    .attr('opacity', 0.8)
    .append('title')
    .text((d) => `${d.player_name ?? d.player_id}\nCluster ${d.cluster_id}`)

  svg
    .append('text')
    .attr('x', width / 2)
    .attr('y', height - 10)
    .attr('text-anchor', 'middle')
    .text(xAxisLabel)

  svg
    .append('text')
    .attr('x', -height / 2)
    .attr('y', 18)
    .attr('transform', 'rotate(-90)')
    .attr('text-anchor', 'middle')
    .text(yAxisLabel)
}

onMounted(drawScatter)
watch(filtered, drawScatter)
</script>

<template>
  <section class="panel">
    <h2>Cluster projection overview</h2>
    <p class="subtle">
      Active run uses <strong>{{ clusteringConfig.algorithm }}</strong> with
      {{ clusteringConfig.attributes.length }} attributes.
    </p>

    <label class="inline-filter">
      Cluster
      <select v-model="selectedCluster">
        <option value="all">All</option>
        <option v-for="cluster in clusters" :key="cluster" :value="String(cluster)">Cluster {{ cluster }}</option>
      </select>
    </label>

    <svg ref="svgRef" class="chart"></svg>

    <div class="helper-panel">
      <p>Each dot is a player.</p>
      <p>Nearby dots have similar match patterns.</p>
    </div>

    <details class="advanced-panel">
      <summary>How this chart is computed</summary>
      <p>
        The projection comes from the active run and is based on
        {{ clusteringConfig.attributes.length }} selected attributes.
      </p>
      <ul>
        <li v-for="component in projectionComponents" :key="component.index">
          {{ component.label }}
        </li>
      </ul>
    </details>

    <div class="summary-grid">
      <article v-for="s in summaries" :key="s.cluster" class="summary-card">
        <h3>Cluster {{ s.cluster }}</h3>
        <p>{{ s.count }} players</p>
        <p>Avg Elo: {{ s.avgElo.toFixed(1) }}</p>
        <p>Avg Win%: {{ s.avgWinPct.toFixed(1) }}</p>
        <p>Avg Service%: {{ s.avgService.toFixed(1) }}</p>
      </article>
    </div>
  </section>
</template>
