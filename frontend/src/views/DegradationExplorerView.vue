<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import * as d3 from 'd3'
import { apiGet } from '../services/api'

const props = defineProps({ players: { type: Array, required: true } })

const metric = ref('elo')
const ranking = ref([])
const loading = ref(false)
const error = ref('')
const selectedPlayer = ref('')
const selectedTrend = ref([])
const svgRef = ref()

const topCandidates = computed(() => props.players.slice(0, 40).map((p) => p.player_id))

onMounted(loadRanking)
watch(metric, loadRanking)
watch(selectedPlayer, loadSelectedTrend)
watch(selectedTrend, drawTrend)

async function loadRanking() {
  loading.value = true
  error.value = ''
  try {
    const responses = await Promise.all(
      topCandidates.value.map(async (playerId) => {
        const resp = await apiGet(`/players/${encodeURIComponent(playerId)}/metrics/degradation`, {
          metric: metric.value,
          limit: 300
        })
        return {
          playerId,
          score: Number(resp.degradation?.score ?? 0),
          label: resp.degradation?.label ?? 'unknown'
        }
      })
    )

    ranking.value = responses.sort((a, b) => b.score - a.score)
    selectedPlayer.value = ranking.value[0]?.playerId ?? ''
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err)
  } finally {
    loading.value = false
  }
}

async function loadSelectedTrend() {
  if (!selectedPlayer.value) return
  try {
    const resp = await apiGet(`/players/${encodeURIComponent(selectedPlayer.value)}/metrics/degradation`, {
      metric: metric.value,
      limit: 350
    })
    selectedTrend.value = resp.points.map((p) => ({
      date: new Date(p.match_date),
      value: Number(p.value ?? 0),
      smoothed: Number(p.smoothed_value ?? p.value ?? 0)
    }))
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err)
  }
}

function drawTrend() {
  const svg = d3.select(svgRef.value)
  svg.selectAll('*').remove()
  if (!selectedTrend.value.length) return

  const width = 900
  const height = 280
  const margin = { top: 20, right: 22, bottom: 36, left: 48 }
  svg.attr('viewBox', `0 0 ${width} ${height}`)

  const x = d3.scaleTime().domain(d3.extent(selectedTrend.value, (d) => d.date)).range([margin.left, width - margin.right])
  const y = d3.scaleLinear().domain(d3.extent(selectedTrend.value, (d) => d.smoothed)).nice().range([height - margin.bottom, margin.top])

  svg.append('g').attr('transform', `translate(0,${height - margin.bottom})`).call(d3.axisBottom(x).ticks(6))
  svg.append('g').attr('transform', `translate(${margin.left},0)`).call(d3.axisLeft(y).ticks(6))

  svg.append('path').datum(selectedTrend.value).attr('fill', 'none').attr('stroke', '#94a3b8').attr('stroke-width', 1.2)
    .attr('d', d3.line().x((d) => x(d.date)).y((d) => y(d.value)))

  svg.append('path').datum(selectedTrend.value).attr('fill', 'none').attr('stroke', '#dc2626').attr('stroke-width', 2)
    .attr('d', d3.line().x((d) => x(d.date)).y((d) => y(d.smoothed)))
}
</script>

<template>
  <section class="panel split-two">
    <article class="panel nested">
      <h2>Ranked degradation list</h2>
      <div class="filters">
        <label>
          Metric
          <select v-model="metric">
            <option value="elo">elo</option>
            <option value="ace_pct">ace_pct</option>
            <option value="break_points_won_pct">break_points_won_pct</option>
            <option value="win_pct">win_pct</option>
          </select>
        </label>
      </div>
      <p v-if="loading">Loading degradation scores...</p>
      <p v-if="error" class="error-text">{{ error }}</p>
      <ul class="rank-list">
        <li
          v-for="row in ranking"
          :key="row.playerId"
          :class="{ selected: row.playerId === selectedPlayer }"
          @click="selectedPlayer = row.playerId"
        >
          <span>{{ row.playerId }} ({{ row.label }})</span>
          <strong>{{ row.score.toFixed(3) }}</strong>
        </li>
      </ul>
    </article>

    <article class="panel nested">
      <h2>Linked trend chart</h2>
      <p class="subtle">Selected: {{ selectedPlayer || '—' }}</p>
      <svg ref="svgRef" class="chart"></svg>
    </article>
  </section>
</template>
