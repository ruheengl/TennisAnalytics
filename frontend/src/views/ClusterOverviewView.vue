<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import * as d3 from 'd3'

const props = defineProps({
  clusterResult: { type: Object, required: true },
  players: { type: Array, required: true },
  clusterPlayers: { type: Array, required: true }
})

const svgRef = ref()
const selectedCluster = ref('all')

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
      avgService: (d3.mean(rows, (r) => Number(r.service_points_won_pct ?? 0)) ?? 0) * 100
    }),
    (r) => r.cluster_id
  )

  return grouped.map(([cluster, values]) => ({ cluster, ...values }))
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
    .domain(d3.extent(filtered.value, (d) => Number(d.service_points_won_pct ?? 0)))
    .nice()
    .range([margin.left, width - margin.right])

  const y = d3
    .scaleLinear()
    .domain(d3.extent(filtered.value, (d) => Number(d.return_points_won_pct ?? 0)))
    .nice()
    .range([height - margin.bottom, margin.top])

  const color = d3.scaleOrdinal(d3.schemeTableau10).domain(clusters.value)

  svg
    .append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(d3.axisBottom(x).ticks(8).tickFormat((d) => `${Math.round(d * 100)}%`))

  svg
    .append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(d3.axisLeft(y).ticks(7).tickFormat((d) => `${Math.round(d * 100)}%`))

  svg
    .append('g')
    .selectAll('circle')
    .data(filtered.value)
    .join('circle')
    .attr('cx', (d) => x(Number(d.service_points_won_pct ?? 0)))
    .attr('cy', (d) => y(Number(d.return_points_won_pct ?? 0)))
    .attr('r', 4)
    .attr('fill', (d) => color(d.cluster_id))
    .attr('opacity', 0.8)
    .append('title')
    .text((d) => `${d.player_id}\nCluster ${d.cluster_id}`)

  svg
    .append('text')
    .attr('x', width / 2)
    .attr('y', height - 10)
    .attr('text-anchor', 'middle')
    .text('Service points won % (projection X)')

  svg
    .append('text')
    .attr('x', -height / 2)
    .attr('y', 18)
    .attr('transform', 'rotate(-90)')
    .attr('text-anchor', 'middle')
    .text('Return points won % (projection Y)')
}

onMounted(drawScatter)
watch(filtered, drawScatter)
</script>

<template>
  <section class="panel">
    <h2>Cluster projection overview</h2>
    <p class="subtle">Scatter projection by service vs return strength, colored by cluster labels from the API.</p>

    <label class="inline-filter">
      Cluster
      <select v-model="selectedCluster">
        <option value="all">All</option>
        <option v-for="cluster in clusters" :key="cluster" :value="String(cluster)">Cluster {{ cluster }}</option>
      </select>
    </label>

    <svg ref="svgRef" class="chart"></svg>

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
