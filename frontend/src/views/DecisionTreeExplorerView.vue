<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import * as d3 from 'd3'
import { apiPost } from '../services/api'

const props = defineProps({
  players: { type: Array, required: true },
  featureColumns: { type: Array, required: true }
})

const selectedPlayer = ref('')
const selectedMatchKey = ref('')
const treeSvg = ref()
const barSvg = ref()
const collapsed = ref(new Set())
const explanation = ref(null)
const predictionResponse = ref(null)
const error = ref('')

const playerNameById = computed(() =>
  Object.fromEntries(props.players.map((p) => [p.player_id, p.player_name ?? p.player_id]))
)
const displayPlayerName = (playerId) => playerNameById.value[playerId] ?? playerId
const playerOptions = computed(() =>
  [...new Set(props.players.map((p) => p.player_id))].map((playerId) => ({
    player_id: playerId,
    player_name: displayPlayerName(playerId)
  }))
)
const matchKeyForRow = (row) => {
  if (row?.match_id != null && row.match_id !== '') return `match:${row.match_id}`
  return `composite:${row?.player_id ?? ''}|${row?.opponent_id ?? ''}|${row?.match_date ?? ''}`
}
const playerMatchRows = computed(() => props.players.filter((p) => p.player_id === selectedPlayer.value))
const matchOptions = computed(() =>
  playerMatchRows.value.map((row) => ({
    key: matchKeyForRow(row),
    label: `${row.match_date ?? '-'} · vs ${displayPlayerName(row.opponent_id)} · ${row.surface ?? '-'} · match_id=${row.match_id ?? '-'}`
  }))
)
const selectedMatchRow = computed(() =>
  props.players.find((row) => matchKeyForRow(row) === selectedMatchKey.value) ?? null
)
const pathSummary = computed(() => explanation.value?.path_summary ?? null)
const contextPanel = computed(() => {
  const row = selectedMatchRow.value
  if (!row) return null

  const usedDefaultMedians = predictionResponse.value?.used_default_medians_for
  return {
    player_id: row.player_id ?? '-',
    opponent_id: row.opponent_id ?? '-',
    match_date: row.match_date ?? '-',
    surface: row.surface ?? '-',
    match_id: row.match_id,
    imputed_count: Array.isArray(usedDefaultMedians) ? usedDefaultMedians.length : null,
    predicted_outcome: predictionResponse.value?.predicted_outcome ?? null,
    win_probability: predictionResponse.value?.win_probability ?? null
  }
})

onMounted(async () => {
  if (!selectedPlayer.value && playerOptions.value.length > 0) {
    selectedPlayer.value = playerOptions.value[0].player_id
  }
})
watch(selectedPlayer, () => {
  const firstMatchKey = playerMatchRows.value[0] ? matchKeyForRow(playerMatchRows.value[0]) : ''
  if (selectedMatchKey.value !== firstMatchKey) {
    selectedMatchKey.value = firstMatchKey
  }
})
watch(selectedMatchKey, loadPrediction)
watch(explanation, () => {
  drawTree()
  drawBars()
})
watch(collapsed, drawTree, { deep: true })

async function loadPrediction() {
  const row = selectedMatchRow.value
  if (!row) return

  error.value = ''
  try {
    const features = {}
    for (const col of props.featureColumns) {
      features[col] = row[col] == null ? null : Number(row[col])
    }

    const resp = await apiPost('/predict', { features, top_k_features: 8 })
    predictionResponse.value = resp
    explanation.value = resp.explanation
    collapsed.value = new Set()
  } catch (err) {
    predictionResponse.value = null
    error.value = err instanceof Error ? err.message : String(err)
  }
}

function treeData() {
  const payload =
    explanation.value?.tree ?? explanation.value?.tree_structure ?? explanation.value?.hierarchical_tree ?? null
  if (!payload || typeof payload !== 'object') return null

  function asNumber(value) {
    const n = Number(value)
    return Number.isFinite(n) ? n : null
  }

  function nodeId(node, fallback) {
    const rawId =
      node.id ?? node.node_id ?? node.nodeId ?? node.tree_id ?? node.treeId ?? node.idx ?? node.index ?? fallback
    return String(rawId)
  }

  function buildNode(node, fallback = 'root') {
    if (!node || typeof node !== 'object') return null

    const id = nodeId(node, fallback)
    const splitFeature = node.split_feature ?? node.feature ?? node.feature_name ?? node.splitFeature ?? null
    const threshold = asNumber(node.threshold ?? node.split_threshold ?? node.splitThreshold)
    const leafMetadata = node.leaf_metadata ?? node.leaf ?? node.leaf_info ?? node.leafInfo ?? null
    const leftRaw = node.left_child ?? node.left ?? node.leftChild ?? null
    const rightRaw = node.right_child ?? node.right ?? node.rightChild ?? null

    const left = buildNode(leftRaw, `${id}-left`)
    const right = buildNode(rightRaw, `${id}-right`)
    const children = [left, right].filter(Boolean)
    const isLeaf = Boolean(node.is_leaf) || (!left && !right)

    const name = isLeaf
      ? `Leaf ${leafMetadata?.leaf_id ?? leafMetadata?.leafId ?? id}`
      : `${splitFeature ?? 'feature'} <= ${(threshold ?? 0).toFixed(3)}`

    return {
      id,
      name,
      splitFeature,
      threshold,
      leafMetadata,
      isLeaf,
      children
    }
  }

  if (Array.isArray(payload)) {
    const nodesById = new Map()
    for (const node of payload) {
      const id = nodeId(node, nodesById.size)
      const splitFeature =
        node.split_feature ??
        node.feature ??
        node.feature_name ??
        node.splitFeature ??
        node.split_feature_name ??
        node.splitFeatureName ??
        null
      const threshold = asNumber(node.threshold ?? node.split_threshold ?? node.splitThreshold)
      const leftId = node.left_child_id ?? node.left_child ?? node.left ?? node.leftChild ?? null
      const rightId = node.right_child_id ?? node.right_child ?? node.right ?? node.rightChild ?? null
      const leafMetadata = node.leaf_metadata ?? node.leaf ?? node.leaf_info ?? node.leafInfo ?? null
      const isLeaf =
        Boolean(node.is_leaf) ||
        (Number(leftId) < 0 && Number(rightId) < 0) ||
        (!Number.isFinite(Number(leftId)) && !Number.isFinite(Number(rightId)))

      nodesById.set(String(id), {
        id: String(id),
        splitFeature,
        threshold,
        leafMetadata,
        isLeaf,
        leftId: Number.isFinite(Number(leftId)) && Number(leftId) >= 0 ? String(leftId) : null,
        rightId: Number.isFinite(Number(rightId)) && Number(rightId) >= 0 ? String(rightId) : null,
        children: []
      })
    }

    for (const node of nodesById.values()) {
      const children = [node.leftId, node.rightId]
        .filter(Boolean)
        .map((childId) => nodesById.get(childId))
        .filter(Boolean)
      node.children = children
      if (children.length > 0) node.isLeaf = false
      node.name = node.isLeaf
        ? `Leaf ${node.leafMetadata?.leaf_id ?? node.leafMetadata?.leafId ?? node.id}`
        : `${node.splitFeature ?? 'feature'} <= ${(node.threshold ?? 0).toFixed(3)}`
    }

    return nodesById.get('0') ?? nodesById.values().next().value ?? null
  }

  return buildNode(payload)
}

function activePathIds(treeRoot) {
  const pathIds = new Set((explanation.value?.path_summary?.node_ids ?? []).map((id) => String(id)))
  if (pathIds.size) return pathIds

  const row = selectedMatchRow.value
  if (!row || !treeRoot) return pathIds

  let node = treeRoot
  while (node) {
    pathIds.add(String(node.id))
    if (node.isLeaf || !node.children?.length) break

    const value = Number(row[node.splitFeature])
    if (!Number.isFinite(value) || !Number.isFinite(node.threshold) || node.children.length < 2) break

    node = value <= node.threshold ? node.children[0] : node.children[1]
  }
  return pathIds
}

function drawTree() {
  const svg = d3.select(treeSvg.value)
  svg.selectAll('*').remove()
  if (!explanation.value) return

  const treeRoot = treeData()
  if (!treeRoot) return

  const fullRoot = d3.hierarchy(treeRoot)
  const highlightedNodeIds = activePathIds(treeRoot)
  d3.tree().nodeSize([180, 70])(fullRoot)

  const hiddenIds = collapsed.value
  const visibleNodes = fullRoot.descendants().filter(
    (node) => !node.ancestors().slice(0, -1).some((ancestor) => hiddenIds.has(ancestor.data.id))
  )
  const visibleIds = new Set(visibleNodes.map((node) => node.data.id))
  const links = fullRoot
    .links()
    .filter((link) => visibleIds.has(link.source.data.id) && visibleIds.has(link.target.data.id))
  const highlightedLinkIds = new Set(
    links
      .filter((link) => highlightedNodeIds.has(String(link.source.data.id)) && highlightedNodeIds.has(String(link.target.data.id)))
      .map((link) => `${link.source.data.id}->${link.target.data.id}`)
  )

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
    .attr('stroke', (d) =>
      highlightedLinkIds.has(`${d.source.data.id}->${d.target.data.id}`) ? '#f97316' : '#94a3b8'
    )
    .attr('stroke-width', (d) =>
      highlightedLinkIds.has(`${d.source.data.id}->${d.target.data.id}`) ? 3 : 1.5
    )
    .attr('d', (d) => d3.linkVertical()({ source: [d.source.x, d.source.y], target: [d.target.x, d.target.y] }))

  const nodes = graph
    .append('g')
    .selectAll('g')
    .data(visibleNodes)
    .join('g')
    .attr('transform', (d) => `translate(${d.x},${d.y})`)
    .style('cursor', 'pointer')
    .on('click', (_, d) => {
      if (!d.children?.length) return
      const next = new Set(collapsed.value)
      if (next.has(d.data.id)) {
        next.delete(d.data.id)
      } else {
        next.add(d.data.id)
      }
      collapsed.value = next
    })

  nodes
    .append('circle')
    .attr('r', 7)
    .attr('fill', (d) => {
      if (highlightedNodeIds.has(String(d.data.id))) return '#fdba74'
      return collapsed.value.has(d.data.id) ? '#dc2626' : '#2563eb'
    })
    .attr('stroke', (d) => (highlightedNodeIds.has(String(d.data.id)) ? '#ea580c' : '#1e293b'))
    .attr('stroke-width', (d) => (highlightedNodeIds.has(String(d.data.id)) ? 2.5 : 1))
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

function formatFixed(value, digits = 3) {
  const numeric = Number(value)
  return Number.isFinite(numeric) ? numeric.toFixed(digits) : '-'
}
</script>

<template>
  <section class="panel">
    <h2>Decision-tree explorer</h2>
    <div class="filters">
      <label>
        Player
        <select v-model="selectedPlayer">
          <option v-for="player in playerOptions" :key="player.player_id" :value="player.player_id">
            {{ player.player_name }}
          </option>
        </select>
      </label>
      <label>
        Match row
        <select v-model="selectedMatchKey">
          <option v-for="row in matchOptions" :key="row.key" :value="row.key">{{ row.label }}</option>
        </select>
      </label>
    </div>
    <p v-if="error" class="error-text">{{ error }}</p>
    <div v-if="contextPanel" class="path-summary">
      <h3>Match prediction context</h3>
      <p>
        <strong>player_id:</strong> {{ contextPanel.player_id }} · <strong>opponent_id:</strong>
        {{ contextPanel.opponent_id }} · <strong>match_date:</strong> {{ contextPanel.match_date }} ·
        <strong>surface:</strong> {{ contextPanel.surface }}
        <span v-if="contextPanel.match_id != null && contextPanel.match_id !== ''">
          · <strong>match_id:</strong> {{ contextPanel.match_id }}
        </span>
        <span v-if="contextPanel.imputed_count != null">
          · <strong>imputed fields:</strong> {{ contextPanel.imputed_count }}
        </span>
      </p>
      <p>
        <strong>predicted_outcome:</strong> {{ contextPanel.predicted_outcome ?? '-' }} ·
        <strong>win_probability:</strong>
        {{ contextPanel.win_probability == null ? '-' : formatFixed(contextPanel.win_probability) }}
      </p>
      <p>Prediction is for this match context (focal player vs opponent), not a general player rating.</p>
    </div>
    <h3>Collapsible tree (full model structure + active path)</h3>
    <svg ref="treeSvg" class="chart"></svg>
    <div v-if="pathSummary" class="path-summary">
      <h3>Selected match path summary</h3>
      <p>
        Leaf {{ pathSummary.leaf_id ?? '-' }} · Samples {{ pathSummary.sample_count ?? '-' }} · Leaf win probability
        {{ pathSummary.leaf_win_probability == null ? '-' : formatFixed(pathSummary.leaf_win_probability) }}
      </p>
      <ol>
        <li v-for="(rule, idx) in pathSummary.rules ?? []" :key="`${rule.feature}-${idx}`">
          {{ rule.feature }} {{ rule.operator }} {{ formatFixed(rule.threshold) }}
          (value: {{ formatFixed(rule.value) }})
        </li>
      </ol>
    </div>
    <h3>Feature importance bars</h3>
    <svg ref="barSvg" class="chart"></svg>
  </section>
</template>
