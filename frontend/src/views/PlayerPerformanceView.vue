<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import * as d3 from 'd3'
import { apiGet } from '../services/api'

const props = defineProps({
  players: { type: Array, required: true },
  selectedPlayerId: { type: String, default: '' },
  selectedPlayerName: { type: String, default: '' },
  activeStoryStep: { type: String, default: 'overview' },
  clusterRequestId: { type: String, default: '' },
  embedded: { type: Boolean, default: false }
})
const emit = defineEmits(['update:selectedPlayerId', 'update:activeStoryStep', 'select-match-context'])

const selectedPlayer = computed({
  get: () => props.selectedPlayerId,
  set: (value) => emit('update:selectedPlayerId', value)
})
const svgRef = ref()
const brushRef = ref()
const error = ref('')
const dataset = ref([])
const domain = ref(null)

const useBrushWindowForMatches = ref(true)
const selectedMatchKey = ref('')

const matchKeyForRow = (row) => {
  const hasRowId = row?.row_id !== null && row?.row_id !== undefined && row.row_id !== ''
  if (hasRowId) return `row:${row.row_id}`
  if (row?.match_id != null && row.match_id !== '') return `match:${row.match_id}`
  return `composite:${row?.player_id ?? ''}|${row?.opponent_id ?? ''}|${row?.match_date ?? ''}`
}
const selectedPlayerRows = computed(() =>
  props.players
    .filter((row) => row.player_id === selectedPlayer.value)
    .map((row) => ({
      ...row,
      matchDateObj: row.match_date ? new Date(row.match_date) : null
    }))
    .sort((a, b) => d3.descending(a.matchDateObj, b.matchDateObj))
)
const hasBrushWindow = computed(() => Array.isArray(domain.value) && domain.value.length === 2)
const effectiveMatchRows = computed(() => {
  const rows = selectedPlayerRows.value
  if (!rows.length) return []
  if (!useBrushWindowForMatches.value || !hasBrushWindow.value) return rows
  const [start, end] = domain.value
  const filtered = rows.filter((row) => row.matchDateObj && row.matchDateObj >= start && row.matchDateObj <= end)
  return filtered.length ? filtered : rows
})
const matchOptions = computed(() =>
  effectiveMatchRows.value.map((row) => ({
    key: matchKeyForRow(row),
    label: `${row.match_date ?? '-'} · vs ${row.opponent_name ?? row.opponent_id ?? '-'} · ${row.surface ?? '-'} · match_id=${row.match_id ?? '-'}`
  }))
)

const selectedMatchRow = computed(() =>
  effectiveMatchRows.value.find((row) => matchKeyForRow(row) === selectedMatchKey.value) ?? null
)

const normalizeToPercent = (value) => {
  const numeric = Number(value ?? 0)
  if (!Number.isFinite(numeric)) return 0
  return numeric <= 1 ? numeric * 100 : numeric
}

const normalizeToNumber = (value) => {
  const numeric = Number(value ?? 0)
  if (!Number.isFinite(numeric)) return 0
  return numeric
}

const selectedWindowLabel = computed(() => {
  if (!hasBrushWindow.value) return 'full timeline'
  const [start, end] = domain.value
  return `${d3.timeFormat('%Y-%m-%d')(start)} → ${d3.timeFormat('%Y-%m-%d')(end)}`
})

const playerOptions = computed(() =>
  props.players.map((p) => ({
    player_id: p.player_id,
    player_name: p.player_name ?? p.player_id
  }))
)

const selectedPlayerLabel = computed(() => {
  if (props.selectedPlayerName && String(props.selectedPlayerName).trim()) {
    return props.selectedPlayerName
  }
  if (!selectedPlayer.value) return 'No player selected'
  const selectedOption = playerOptions.value.find((option) => option.player_id === selectedPlayer.value)
  return selectedOption?.player_name ?? selectedPlayer.value
})

onMounted(async () => {
  if (!selectedPlayer.value && playerOptions.value.length > 0) {
    selectedPlayer.value = playerOptions.value[0].player_id
    // watch(selectedPlayer) will fire and call loadSeries
  } else if (selectedPlayer.value) {
    // Player already set (v-show keeps component mounted from the start),
    // so the watch won't fire — load data manually.
    await loadSeries()
    drawChart()
  }
})

watch(
  playerOptions,
  (options) => {
    if (!options.length) return
    const optionIds = options.map((option) => option.player_id)
    if (!optionIds.includes(selectedPlayer.value)) {
      selectedPlayer.value = options[0].player_id
    }
  },
  { immediate: true }
)

watch(
  [matchOptions, useBrushWindowForMatches],
  ([options]) => {
    if (!options.length) { selectedMatchKey.value = ''; return }
    if (!options.some((option) => option.key === selectedMatchKey.value)) {
      selectedMatchKey.value = options[0].key
    }
  },
  { immediate: true }
)

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
      apiGet(`/players/${encodeURIComponent(selectedPlayer.value)}/metrics/timeseries`, { metric: 'elo', limit: 20000 }),
      apiGet(`/players/${encodeURIComponent(selectedPlayer.value)}/metrics/timeseries`, { metric: 'ace_pct', limit: 20000 }),
      apiGet(`/players/${encodeURIComponent(selectedPlayer.value)}/metrics/timeseries`, { metric: 'win_pct', limit: 20000 })
    ])

    const map = new Map()
    for (const point of elo.points) {
      map.set(point.match_date, { date: new Date(point.match_date), elo: Number(point.value ?? 0), ace: null, win: null })
    }
    for (const point of ace.points) {
      const rec = map.get(point.match_date) ?? { date: new Date(point.match_date), elo: null, ace: null, win: null }
      rec.ace = normalizeToPercent(point.value)
      map.set(point.match_date, rec)
    }
    for (const point of win.points) {
      const rec = map.get(point.match_date) ?? { date: new Date(point.match_date), elo: null, ace: null, win: null }
      rec.win = normalizeToPercent(point.value)
      map.set(point.match_date, rec)
    }

    dataset.value = [...map.values()].sort((a, b) => d3.ascending(a.date, b.date))
    domain.value = null
    drawBrush()
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err)
  }
}

function continueToMatchExplanation() {
  if (!selectedPlayer.value || !selectedMatchRow.value) return
  emit('select-match-context', {
    player_id: selectedPlayer.value,
    match_key: matchKeyForRow(selectedMatchRow.value)
  })
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

  const legendItems = [
    { label: 'Elo', color: '#1d4ed8' },
    { label: 'Win %', color: '#059669' },
    { label: 'Ace %', color: '#f97316' }
  ]

  const legend = svg.append('g').attr('transform', `translate(${margin.left}, ${height - margin.bottom + 30})`)
  const legendEntry = legend.selectAll('g').data(legendItems).enter().append('g').attr('transform', (_, i) => `translate(${i * 120}, 0)`)
  legendEntry.append('line').attr('x1', 0).attr('x2', 22).attr('y1', 0).attr('y2', 0).attr('stroke-width', 3).attr('stroke', (d) => d.color)
  legendEntry.append('text').attr('x', 30).attr('y', 4).attr('font-size', 12).text((d) => d.label)

  svg.append('text').attr('x', margin.left).attr('y', margin.top - 8).text('Elo')
  svg.append('text').attr('x', width - margin.right - 120).attr('y', margin.top - 8).text('Win% / Ace%')
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
      if (!event.selection) { domain.value = null; return }
      const [a, b] = event.selection
      domain.value = [x.invert(a), x.invert(b)]
    })

  svg.append('g').call(brush)
}
</script>

<template>
  <section class="panel">
    <h2 v-if="!embedded">Player performance (multi-metric time series)</h2>

    <div class="trend-header">
      <h3>Trend context for {{ selectedPlayerLabel }}</h3>
      <button
        v-if="selectedMatchRow && selectedMatchKey"
        type="button"
        class="secondary"
        @click="continueToMatchExplanation"
      >
        View predicted outcomes &amp; explanation →
      </button>
    </div>

    <div class="filters">
      <label v-if="!embedded">
        Player
        <select v-model="selectedPlayer">
          <option v-for="player in playerOptions" :key="player.player_id" :value="player.player_id">
            {{ player.player_name }}
          </option>
        </select>
      </label>

      <label>
        Match in context
        <select v-model="selectedMatchKey" :disabled="!matchOptions.length">
          <option v-for="option in matchOptions" :key="option.key" :value="option.key">
            {{ option.label }}
          </option>
        </select>
      </label>

      <label class="inline-option">
        <input v-model="useBrushWindowForMatches" type="checkbox" :disabled="!hasBrushWindow" />
        Use brush date window ({{ selectedWindowLabel }})
      </label>
    </div>

    <p v-if="error" class="error-text">{{ error }}</p>
    <svg ref="svgRef" class="chart"></svg>
    <p class="subtle">{{ embedded ? 'Brush the mini chart to zoom.' : 'Use the brush below to zoom the main chart.' }}</p>
    <svg ref="brushRef" class="chart compact"></svg>
  </section>
</template>
