<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import * as d3 from 'd3'
import { apiPost } from '../services/api'

const props = defineProps({
  players: { type: Array, required: true },
  featureColumns: { type: Array, required: true }
})

const selectedPlayer = ref('')
const treeSvg = ref()
const barSvg = ref()
const collapsed = ref(new Set())
const explanation = ref(null)
const error = ref('')

const options = computed(() => props.players.map((p) => p.player_id))

onMounted(async () => {
  if (!selectedPlayer.value && options.value.length > 0) {
    selectedPlayer.value = options.value[0]
  }
})
watch(selectedPlayer, loadPrediction)
watch(explanation, () => {
  drawTree()
  drawBars()
})
watch(collapsed, drawTree, { deep: true })

async function loadPrediction() {
  const row = props.players.find((p) => p.player_id === selectedPlayer.value)
  if (!row) return

  error.value = ''
  try {
    const features = {}
    for (const col of props.featureColumns) {
      features[col] = row[col] == null ? null : Number(row[col])
    }

    const resp = await apiPost('/predict', { features, top_k_features: 8 })
    explanation.value = resp.explanation
    collapsed.value = new Set()
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err)
  }
}

function treeData() {
  const rules = explanation.value?.path_summary?.rules ?? []
  let node = { name: 'root', id: 'root', children: [] }
  const root = node
  rules.forEach((rule, idx) => {
    const child = {
      name: `${rule.feature} ${rule.operator} ${rule.threshold.toFixed(3)}`,
      id: `rule-${idx}`,
      children: []
    }
    node.children = [child]
    node = child
  })
  node.children = [{ name: `Leaf ${explanation.value?.path_summary?.leaf_id ?? '-'}`, id: 'leaf', children: [] }]
  return root
}

function drawTree() {
  const svg = d3.select(treeSvg.value)
  svg.selectAll('*').remove()
  if (!explanation.value) return

  const fullRoot = d3.hierarchy(treeData())
  d3.tree().nodeSize([180, 70])(fullRoot)

  const hiddenIds = collapsed.value
  const visibleNodes = fullRoot.descendants().filter(
    (node) => !node.ancestors().slice(0, -1).some((ancestor) => hiddenIds.has(ancestor.data.id))
  )
  const visibleIds = new Set(visibleNodes.map((node) => node.data.id))
  const links = fullRoot
    .links()
    .filter((link) => visibleIds.has(link.source.data.id) && visibleIds.has(link.target.data.id))

  const minX = d3.min(visibleNodes, (node) => node.x) ?? 0
  const maxX = d3.max(visibleNodes, (node) => node.x) ?? 0
  const maxY = d3.max(visibleNodes, (node) => node.y) ?? 0
  const width = Math.max(900, maxX - minX + 180)
  const height = Math.max(320, maxY + 120)
  const offsetX = 90 - minX
  const offsetY = 30

  svg.attr('viewBox', `0 0 ${width} ${height}`)

  const graph = svg.append('g').attr('transform', `translate(${offsetX},${offsetY})`)

  graph
    .append('g')
    .selectAll('path')
    .data(links)
    .join('path')
    .attr('fill', 'none')
    .attr('stroke', '#94a3b8')
    .attr('stroke-width', 1.5)
    .attr('d', (d) => d3.linkVertical()({ source: [d.source.x, d.source.y], target: [d.target.x, d.target.y] }))

  const nodes = graph
    .append('g')
    .selectAll('g')
    .data(visibleNodes)
    .join('g')
    .attr('transform', (d) => `translate(${d.x},${d.y})`)
    .style('cursor', 'pointer')
    .on('click', (_, d) => {
      const next = new Set(collapsed.value)
      if (next.has(d.data.id)) {
        next.delete(d.data.id)
      } else {
        next.add(d.data.id)
      }
      collapsed.value = next
    })

  nodes.append('circle').attr('r', 7).attr('fill', (d) => (collapsed.value.has(d.data.id) ? '#dc2626' : '#2563eb'))
  nodes
    .append('text')
    .attr('dy', -10)
    .attr('text-anchor', 'middle')
    .style('font-size', '11px')
    .text((d) => d.data.name)
}

function drawBars() {
  const svg = d3.select(barSvg.value)
  svg.selectAll('*').remove()
  const rows = explanation.value?.top_contributing_features ?? []
  if (!rows.length) return

  const width = 900
  const height = 280
  const margin = { top: 20, right: 20, bottom: 32, left: 200 }
  svg.attr('viewBox', `0 0 ${width} ${height}`)

  const x = d3.scaleLinear().domain([0, d3.max(rows, (d) => d.score) ?? 1]).nice().range([margin.left, width - margin.right])
  const y = d3.scaleBand().domain(rows.map((d) => d.feature)).range([margin.top, height - margin.bottom]).padding(0.25)

  svg.append('g').attr('transform', `translate(0,${height - margin.bottom})`).call(d3.axisBottom(x))
  svg.append('g').attr('transform', `translate(${margin.left},0)`).call(d3.axisLeft(y))

  svg
    .append('g')
    .selectAll('rect')
    .data(rows)
    .join('rect')
    .attr('x', margin.left)
    .attr('y', (d) => y(d.feature))
    .attr('width', (d) => x(d.score) - margin.left)
    .attr('height', y.bandwidth())
    .attr('fill', '#0ea5e9')
}
</script>

<template>
  <section class="panel">
    <h2>Decision-tree explorer</h2>
    <div class="filters">
      <label>
        Player
        <select v-model="selectedPlayer">
          <option v-for="id in options" :key="id" :value="id">{{ id }}</option>
        </select>
      </label>
    </div>
    <p v-if="error" class="error-text">{{ error }}</p>
    <h3>Collapsible tree (decision path)</h3>
    <svg ref="treeSvg" class="chart"></svg>
    <h3>Feature importance bars</h3>
    <svg ref="barSvg" class="chart"></svg>
  </section>
</template>
