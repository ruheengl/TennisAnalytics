<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import * as d3 from 'd3'
import { apiGet } from '../services/api'

const props = defineProps({ players: { type: Array, required: true } })

const selectedPlayer = ref('')
const svgRef = ref()
const brushRef = ref()
const error = ref('')
const dataset = ref([])
const domain = ref(null)

const playerOptions = computed(() => props.players.map((p) => p.player_id))

onMounted(() => {
  if (!selectedPlayer.value && playerOptions.value.length > 0) {
    selectedPlayer.value = playerOptions.value[0]
  }
})

watch(selectedPlayer, async () => {
  await loadSeries()
  drawChart()
})

watch(domain, drawChart)

async function loadSeries() {
  if (!selectedPlayer.value) return
  error.value = ''
  try {
    const [elo, ace, win] = await Promise.all([
      apiGet(`/players/${encodeURIComponent(selectedPlayer.value)}/metrics/timeseries`, { metric: 'elo', limit: 500 }),
      apiGet(`/players/${encodeURIComponent(selectedPlayer.value)}/metrics/timeseries`, { metric: 'ace_pct', limit: 500 }),
      apiGet(`/players/${encodeURIComponent(selectedPlayer.value)}/metrics/timeseries`, { metric: 'win_pct', limit: 500 })
    ])

    const map = new Map()
    for (const point of elo.points) {
      map.set(point.match_date, { date: new Date(point.match_date), elo: Number(point.value ?? 0), ace: null, win: null })
    }
    for (const point of ace.points) {
      const rec = map.get(point.match_date) ?? { date: new Date(point.match_date), elo: null, ace: null, win: null }
      rec.ace = Number(point.value ?? 0) * 100
      map.set(point.match_date, rec)
    }
    for (const point of win.points) {
      const rec = map.get(point.match_date) ?? { date: new Date(point.match_date), elo: null, ace: null, win: null }
      rec.win = Number(point.value ?? 0) * 100
      map.set(point.match_date, rec)
    }

    dataset.value = [...map.values()].sort((a, b) => d3.ascending(a.date, b.date))
    domain.value = null
    drawBrush()
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err)
  }
}

function drawChart() {
  const svg = d3.select(svgRef.value)
  svg.selectAll('*').remove()

  if (!dataset.value.length) return

  const width = 940
  const height = 420
  const margin = { top: 24, right: 30, bottom: 42, left: 56 }
  svg.attr('viewBox', `0 0 ${width} ${height}`)

  const xFull = d3.scaleTime().domain(d3.extent(dataset.value, (d) => d.date)).range([margin.left, width - margin.right])
  const xDomain = domain.value ?? xFull.domain()
  const x = d3.scaleTime().domain(xDomain).range([margin.left, width - margin.right])

  const clipped = dataset.value.filter((d) => d.date >= x.domain()[0] && d.date <= x.domain()[1])

  const yLeft = d3.scaleLinear().domain(d3.extent(clipped, (d) => d.elo)).nice().range([height - margin.bottom, margin.top])
  const yRight = d3.scaleLinear().domain([0, 100]).range([height - margin.bottom, margin.top])

  svg.append('g').attr('transform', `translate(0,${height - margin.bottom})`).call(d3.axisBottom(x))
  svg.append('g').attr('transform', `translate(${margin.left},0)`).call(d3.axisLeft(yLeft))
  svg.append('g').attr('transform', `translate(${width - margin.right},0)`).call(d3.axisRight(yRight))

  const line = (key, scale, color) =>
    d3
      .line()
      .defined((d) => d[key] !== null)
      .x((d) => x(d.date))
      .y((d) => scale(d[key]))

  svg.append('path').datum(clipped).attr('fill', 'none').attr('stroke', '#1d4ed8').attr('stroke-width', 2).attr('d', line('elo', yLeft))
  svg.append('path').datum(clipped).attr('fill', 'none').attr('stroke', '#059669').attr('stroke-width', 2).attr('d', line('win', yRight))
  svg.append('path').datum(clipped).attr('fill', 'none').attr('stroke', '#f97316').attr('stroke-width', 2).attr('d', line('ace', yRight))

  svg.append('text').attr('x', margin.left).attr('y', margin.top - 8).text('Elo')
  svg.append('text').attr('x', width - margin.right - 70).attr('y', margin.top - 8).text('Win% / Ace%')
}

function drawBrush() {
  const svg = d3.select(brushRef.value)
  svg.selectAll('*').remove()
  if (!dataset.value.length) return

  const width = 940
  const height = 110
  const margin = { top: 10, right: 30, bottom: 24, left: 56 }
  svg.attr('viewBox', `0 0 ${width} ${height}`)

  const x = d3.scaleTime().domain(d3.extent(dataset.value, (d) => d.date)).range([margin.left, width - margin.right])
  const y = d3.scaleLinear().domain(d3.extent(dataset.value, (d) => d.elo)).nice().range([height - margin.bottom, margin.top])

  svg.append('path').datum(dataset.value).attr('fill', 'none').attr('stroke', '#93c5fd').attr('stroke-width', 1.5)
    .attr('d', d3.line().x((d) => x(d.date)).y((d) => y(d.elo)))

  svg.append('g').attr('transform', `translate(0,${height - margin.bottom})`).call(d3.axisBottom(x).ticks(6))

  const brush = d3.brushX().extent([[margin.left, margin.top], [width - margin.right, height - margin.bottom]])
    .on('brush end', (event) => {
      if (!event.selection) {
        domain.value = null
        return
      }
      const [a, b] = event.selection
      domain.value = [x.invert(a), x.invert(b)]
    })

  svg.append('g').call(brush)
}
</script>

<template>
  <section class="panel">
    <h2>Player performance (multi-metric time series)</h2>
    <div class="filters">
      <label>
        Player
        <select v-model="selectedPlayer">
          <option v-for="id in playerOptions" :key="id" :value="id">{{ id }}</option>
        </select>
      </label>
    </div>

    <p v-if="error" class="error-text">{{ error }}</p>
    <svg ref="svgRef" class="chart"></svg>
    <p class="subtle">Use the brush below to zoom the main chart.</p>
    <svg ref="brushRef" class="chart compact"></svg>
  </section>
</template>
