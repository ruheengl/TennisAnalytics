<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import * as d3 from 'd3'
import { apiPost } from '../services/api'

const props = defineProps({
  players: { type: Array, required: true },
  featureColumns: { type: Array, required: true },
  selectedPlayerId: { type: String, default: '' },
  selectedMatchKey: { type: String, default: '' },
  activeStoryStep: { type: String, default: 'overview' },
  clusterRequestId: { type: String, default: '' }
})
const emit = defineEmits(['update:selectedPlayerId', 'update:selectedMatchKey', 'update:activeStoryStep', 'update:prediction-context'])

const selectedPlayer = computed({
  get: () => props.selectedPlayerId,
  set: (value) => emit('update:selectedPlayerId', value)
})
const selectedMatchKey = computed({
  get: () => props.selectedMatchKey,
  set: (value) => emit('update:selectedMatchKey', value)
})
const treeSvg = ref()
const treeViewport = ref()
const barSvg = ref()
const collapsed = ref(new Set())
const zoomBehavior = ref(null)
const zoomTransform = ref(d3.zoomIdentity)
const defaultTransform = ref(d3.zoomIdentity)
const shouldAutoFitTree = ref(false)
const latestVisibleLayout = ref([])
const highlightedNodeIdsForView = ref(new Set())
const dimNonPathBranches = ref(false)
const maxDepth = ref(8)
const maxDepthLimit = ref(8)
const showActiveContextOnly = ref(true)
const expandAll = ref(false)
const featureSearch = ref('')
const matchedFeatureNodeIds = ref([])
const activeMatchIndex = ref(0)
const focusedNodeId = ref(null)
const largeTreeOverride = ref(false)
const explanation = ref(null)
const predictionResponse = ref(null)
const error = ref('')
const LARGE_TREE_NODE_THRESHOLD = 140
const LARGE_TREE_DEPTH_THRESHOLD = 9
const canRequestPrediction = computed(() => Boolean(selectedMatchRow.value?.row_id))

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
  const hasRowId = row?.row_id !== null && row?.row_id !== undefined && row.row_id !== ''
  if (hasRowId) return `row:${row.row_id}`
  if (row?.match_id != null && row.match_id !== '') return `match:${row.match_id}`
  return `composite:${row?.player_id ?? ''}|${row?.opponent_id ?? ''}|${row?.match_date ?? ''}`
}
const playerMatchRows = computed(() => props.players.filter((p) => p.player_id === selectedPlayer.value))
const matchOptions = computed(() =>
  playerMatchRows.value.map((row) => ({
    key: matchKeyForRow(row),
    label: `${row.match_date ?? '-'} · vs ${row.opponent_name ?? displayPlayerName(row.opponent_id)} · ${row.surface ?? '-'} · match_id=${row.match_id ?? '-'}`
  }))
)
const selectedMatchRow = computed(() =>
  playerMatchRows.value.find((row) => matchKeyForRow(row) === selectedMatchKey.value) ?? null
)
const schemaGuardMessage = computed(() => {
  if (!selectedMatchKey.value) return 'Select a match row to request a prediction.'
  if (!selectedMatchRow.value) return 'The selected match row could not be resolved for the current player.'
  return 'No row_id found for selected row.'
})
const pathSummary = computed(() => explanation.value?.path_summary ?? null)
const treeStats = computed(() => {
  const root = treeData()
  if (!root) return { nodeCount: 0, maxDepth: 0 }
  const hierarchy = d3.hierarchy(root)
  return {
    nodeCount: hierarchy.descendants().length,
    maxDepth: d3.max(hierarchy.descendants(), (node) => node.depth) ?? 0
  }
})
const isLargeTree = computed(
  () =>
    treeStats.value.nodeCount >= LARGE_TREE_NODE_THRESHOLD || treeStats.value.maxDepth >= LARGE_TREE_DEPTH_THRESHOLD
)
const fullExpansionBlocked = computed(() => isLargeTree.value && !largeTreeOverride.value)
const activeMatchNodeId = computed(() =>
  matchedFeatureNodeIds.value.length ? matchedFeatureNodeIds.value[activeMatchIndex.value] : null
)


function emitPredictionContext() {
  const row = selectedMatchRow.value
  if (!row) {
    emit('update:prediction-context', {
      matchLabel: 'No match selected',
      winProbability: null
    })
    return
  }

  const matchLabel = `${row.match_date ?? '-'} vs ${row.opponent_name ?? displayPlayerName(row.opponent_id) ?? '-'}`
  emit('update:prediction-context', {
    matchLabel,
    winProbability: predictionResponse.value?.win_probability ?? null
  })
}
const contextPanel = computed(() => {
  const row = selectedMatchRow.value
  if (!row) return null

  const usedDefaultMedians = predictionResponse.value?.used_default_medians_for
  return {
    player_id: row.player_id ?? '-',
    player_name: row.player_name ?? displayPlayerName(row.player_id) ?? '-',
    opponent_id: row.opponent_id ?? '-',
    opponent_name: row.opponent_name ?? displayPlayerName(row.opponent_id) ?? '-',
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
  const keys = playerMatchRows.value.map((row) => matchKeyForRow(row))
  if (!keys.length) {
    selectedMatchKey.value = ''
    return
  }
  if (!keys.includes(selectedMatchKey.value)) {
    selectedMatchKey.value = keys[0]
  }
})
watch(
  matchOptions,
  (options) => {
    const optionKeys = options.map((option) => option.key)
    if (!optionKeys.length) {
      if (selectedMatchKey.value) selectedMatchKey.value = ''
      return
    }
    if (!optionKeys.includes(selectedMatchKey.value)) {
      selectedMatchKey.value = optionKeys[0]
    }
  },
  { immediate: true }
)
watch(selectedMatchKey, loadPrediction)
watch(
  [selectedMatchRow, predictionResponse],
  () => {
    emitPredictionContext()
  },
  { immediate: true }
)
watch(
  canRequestPrediction,
  (available) => {
    if (available) return
    predictionResponse.value = null
    explanation.value = null
  },
  { immediate: true }
)
watch(explanation, () => {
  const stats = treeStats.value
  maxDepthLimit.value = Math.max(1, stats.maxDepth)
  maxDepth.value = isLargeTree.value ? Math.min(stats.maxDepth, 6) : stats.maxDepth
  showActiveContextOnly.value = isLargeTree.value
  expandAll.value = false
  featureSearch.value = ''
  matchedFeatureNodeIds.value = []
  activeMatchIndex.value = 0
  focusedNodeId.value = null
  largeTreeOverride.value = false
  collapsed.value = new Set()
  drawTree()
  drawBars()
})
watch(collapsed, drawTree, { deep: true })
watch(dimNonPathBranches, drawTree)
watch([maxDepth, showActiveContextOnly, expandAll], ([nextDepth], [prevDepth]) => {
  if (nextDepth !== prevDepth) shouldAutoFitTree.value = true
  if (expandAll.value) collapsed.value = new Set()
  drawTree()
})
watch(expandAll, toggleExpandAll)
watch(featureSearch, updateFeatureMatches)
watch(activeMatchIndex, focusSelectedMatchNode)

async function loadPrediction() {
  if (!canRequestPrediction.value) {
    error.value = 'No row_id found for selected row.'
    return
  }

  const row = selectedMatchRow.value
  if (!row) return

  error.value = ''
  try {
    const resp = await apiPost('/predict', { row_id: Number(row.row_id), top_k_features: 8 })
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

  function inferLeafPrediction(node, leafMetadata) {
    const metadata = leafMetadata && typeof leafMetadata === 'object' ? leafMetadata : {}
    const classDistribution =
      metadata.class_distribution ?? metadata.classDistribution ?? node.class_distribution ?? node.classDistribution ?? null
    if (Array.isArray(classDistribution) && classDistribution.length >= 2) {
      const loseProb = Number(classDistribution[0] ?? 0)
      const winProb = Number(classDistribution[1] ?? 0)
      if (Number.isFinite(loseProb) && Number.isFinite(winProb)) {
        return winProb >= loseProb ? 1 : 0
      }
    }

    const winProbability =
      asNumber(metadata.leaf_win_probability ?? metadata.win_probability ?? node.leaf_win_probability ?? node.win_probability)
    if (winProbability != null) return winProbability >= 0.5 ? 1 : 0

    const explicitPrediction = metadata.predicted_outcome ?? metadata.prediction ?? metadata.class_id ?? metadata.class
    const normalizedPrediction = Number(explicitPrediction)
    if (Number.isFinite(normalizedPrediction) && (normalizedPrediction === 0 || normalizedPrediction === 1)) {
      return normalizedPrediction
    }
    return null
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

    const leafPrediction = inferLeafPrediction(node, leafMetadata)
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
      leafPrediction,
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
      const leafPrediction = inferLeafPrediction(node, leafMetadata)
      const isLeaf =
        Boolean(node.is_leaf) ||
        (Number(leftId) < 0 && Number(rightId) < 0) ||
        (!Number.isFinite(Number(leftId)) && !Number.isFinite(Number(rightId)))

      nodesById.set(String(id), {
        id: String(id),
        splitFeature,
        threshold,
        leafMetadata,
        leafPrediction,
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

function contextualNodeIds(fullRoot, highlightedNodeIds) {
  const includeIds = new Set([...highlightedNodeIds].map(String))
  for (const node of fullRoot.descendants()) {
    const id = String(node.data.id)
    if (!highlightedNodeIds.has(id)) continue
    const parent = node.parent
    if (parent?.children?.length) {
      for (const sibling of parent.children) {
        includeIds.add(String(sibling.data.id))
      }
    }
    if (node.children?.length) {
      for (const child of node.children) {
        includeIds.add(String(child.data.id))
      }
    }
  }
  return includeIds
}

function updateFeatureMatches() {
  const query = featureSearch.value.trim().toLowerCase()
  if (!query || !explanation.value) {
    matchedFeatureNodeIds.value = []
    activeMatchIndex.value = 0
    focusedNodeId.value = null
    return
  }
  const root = treeData()
  if (!root) return
  const nodes = d3.hierarchy(root).descendants()
  const matches = nodes
    .filter((node) => String(node.data.splitFeature ?? '').toLowerCase().includes(query))
    .map((node) => String(node.data.id))
  matchedFeatureNodeIds.value = matches
  activeMatchIndex.value = 0
  focusedNodeId.value = matches[0] ?? null
  drawTree()
}

function cycleFeatureMatch(step = 1) {
  const total = matchedFeatureNodeIds.value.length
  if (!total) return
  activeMatchIndex.value = (activeMatchIndex.value + step + total) % total
}

function focusSelectedMatchNode() {
  focusedNodeId.value = activeMatchNodeId.value
  drawTree()
}

function applyPreset(mode) {
  if (mode === 'context') {
    showActiveContextOnly.value = true
    maxDepth.value = Math.min(maxDepthLimit.value, 6)
    largeTreeOverride.value = false
    return
  }
  if (mode === 'medium') {
    showActiveContextOnly.value = false
    maxDepth.value = Math.min(maxDepthLimit.value, 8)
    return
  }
  if (mode === 'full') {
    largeTreeOverride.value = true
    showActiveContextOnly.value = false
    maxDepth.value = maxDepthLimit.value
  }
}

function toggleExpandAll() {
  if (expandAll.value && fullExpansionBlocked.value && maxDepth.value >= maxDepthLimit.value - 1) {
    expandAll.value = false
    return
  }
  if (expandAll.value) {
    collapsed.value = new Set()
    return
  }
  const root = treeData()
  if (!root) return
  const internalNodeIds = d3
    .hierarchy(root)
    .descendants()
    .filter((node) => node.children?.length)
    .map((node) => node.data.id)
  collapsed.value = new Set(internalNodeIds)
}

function drawTree() {
  const svg = d3.select(treeSvg.value)
  svg.selectAll('*').remove()
  latestVisibleLayout.value = []
  if (!explanation.value) return

  const treeRoot = treeData()
  if (!treeRoot) return

  const fullRoot = d3.hierarchy(treeRoot)
  fullRoot.eachAfter((node) => {
    const children = node.children ?? []
    const leafCount = children.length ? d3.sum(children, (child) => child.data._leafCount ?? 1) : 1
    node.data._leafCount = leafCount
    node.data._labelLength = String(node.data.name ?? '').length
  })
  const highlightedNodeIds = activePathIds(treeRoot)
  const queryMatchSet = new Set(matchedFeatureNodeIds.value.map(String))
  const forcedVisible = new Set()
  if (focusedNodeId.value) {
    const focusedNode = fullRoot.descendants().find((node) => String(node.data.id) === String(focusedNodeId.value))
    if (focusedNode) {
      for (const ancestor of focusedNode.ancestors()) forcedVisible.add(String(ancestor.data.id))
      for (const child of focusedNode.children ?? []) forcedVisible.add(String(child.data.id))
    }
  }
  const contextualIds = contextualNodeIds(fullRoot, highlightedNodeIds)
  const hiddenIds = collapsed.value
  const depthLimit = Math.max(0, Number(maxDepth.value))
  const enforceContext = showActiveContextOnly.value || fullExpansionBlocked.value
  const visibleBaseNodes = fullRoot.descendants().filter((node) => {
    const id = String(node.data.id)
    if (node.depth > depthLimit) return false
    if (enforceContext && !contextualIds.has(id) && !forcedVisible.has(id) && !queryMatchSet.has(id)) return false
    return !node.ancestors().slice(0, -1).some((ancestor) => hiddenIds.has(ancestor.data.id))
  })
  const visibleIds = new Set(visibleBaseNodes.map((node) => String(node.data.id)))
  if (!visibleBaseNodes.length) return
  const buildVisibleSubtree = (node) => {
    const id = String(node.data.id)
    if (!visibleIds.has(id)) return null
    const children = (node.children ?? []).map((child) => buildVisibleSubtree(child)).filter(Boolean)
    return { ...node.data, children }
  }
  const visibleTreeRootData = buildVisibleSubtree(fullRoot)
  if (!visibleTreeRootData) return
  const visibleRoot = d3.hierarchy(visibleTreeRootData)
  visibleRoot.eachAfter((node) => {
    const children = node.children ?? []
    const leafCount = children.length ? d3.sum(children, (child) => child.data._leafCount ?? 1) : 1
    node.data._leafCount = leafCount
    node.data._labelLength = String(node.data.name ?? '').length
  })
  const maxLabelLength = d3.max(visibleRoot.descendants(), (node) => node.data._labelLength ?? 0) ?? 0
  const siblingSpacingBase = 105 + Math.min(90, maxLabelLength * 1.2)
  const depthSpacing = 160
  d3.tree()
    .nodeSize([siblingSpacingBase, depthSpacing])
    .separation((a, b) => {
      const aLeafWeight = Math.min(2.6, Math.sqrt(a.data._leafCount ?? 1))
      const bLeafWeight = Math.min(2.6, Math.sqrt(b.data._leafCount ?? 1))
      const labelFactor = ((a.data._labelLength ?? 0) + (b.data._labelLength ?? 0)) / 50
      const familyWeight = a.parent === b.parent ? 1 : 1.55
      return familyWeight * (0.72 + labelFactor) * ((aLeafWeight + bLeafWeight) / 2)
    })(visibleRoot)

  const visibleNodes = visibleRoot.descendants()
  const links = visibleRoot.links()
  const highlightedLinkIds = new Set(
    links
      .filter((link) => highlightedNodeIds.has(String(link.source.data.id)) && highlightedNodeIds.has(String(link.target.data.id)))
      .map((link) => `${link.source.data.id}->${link.target.data.id}`)
  )

  const minX = d3.min(visibleNodes, (node) => node.x) ?? 0
  const maxX = d3.max(visibleNodes, (node) => node.x) ?? 0
  const maxY = d3.max(visibleNodes, (node) => node.y) ?? 0
  const minY = d3.min(visibleNodes, (node) => node.y) ?? 0
  const contentPaddingX = 120
  const contentPaddingY = 46
  const viewportWidth = treeViewport.value?.clientWidth ?? 980
  const viewportHeight = 560
  const width = Math.max(780, viewportWidth)
  const height = viewportHeight

  svg.attr('viewBox', `0 0 ${width} ${height}`).attr('width', width).attr('height', height)

  const zoomLayer = svg.append('g').attr('class', 'zoom-layer')
  const graph = zoomLayer.append('g')
  const positionedNodes = new Map(
    visibleNodes.map((node) => [
      node.data.id,
      {
        px: node.x - minX + contentPaddingX,
        py: node.y - minY + contentPaddingY
      }
    ])
  )
  const contentWidth = maxX - minX + contentPaddingX * 2
  const contentHeight = maxY - minY + contentPaddingY * 2
  const contentBounds = {
    minX: contentPaddingX,
    maxX: contentPaddingX + (maxX - minX),
    minY: contentPaddingY,
    maxY: contentPaddingY + (maxY - minY),
    width: contentWidth,
    height: contentHeight
  }
  latestVisibleLayout.value = visibleNodes.map((node) => ({
    id: String(node.data.id),
    x: positionedNodes.get(node.data.id).px,
    y: positionedNodes.get(node.data.id).py
  }))
  highlightedNodeIdsForView.value = new Set([...highlightedNodeIds].map((id) => String(id)))

  graph
    .append('g')
    .selectAll('path')
    .data(links)
    .join('path')
    .attr('fill', 'none')
    .attr('stroke', (d) =>
      highlightedLinkIds.has(`${d.source.data.id}->${d.target.data.id}`) ? '#f97316' : '#94a3b8'
    )
    .attr('stroke-opacity', (d) => {
      if (!dimNonPathBranches.value) return 1
      return highlightedLinkIds.has(`${d.source.data.id}->${d.target.data.id}`) ? 1 : 0.14
    })
    .attr('stroke-width', (d) =>
      highlightedLinkIds.has(`${d.source.data.id}->${d.target.data.id}`) ? 3 : 1.5
    )
    .attr('d', (d) =>
      d3.linkVertical()({
        source: [positionedNodes.get(d.source.data.id).px, positionedNodes.get(d.source.data.id).py],
        target: [positionedNodes.get(d.target.data.id).px, positionedNodes.get(d.target.data.id).py]
      })
    )

  const nodes = graph
    .append('g')
    .selectAll('g')
    .data(visibleNodes)
    .join('g')
    .attr('transform', (d) => {
      const pos = positionedNodes.get(d.data.id)
      return `translate(${pos.px},${pos.py})`
    })
    .style('cursor', 'pointer')
    .style('opacity', (d) => {
      if (!dimNonPathBranches.value) return 1
      return highlightedNodeIds.has(String(d.data.id)) ? 1 : 0.16
    })
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
      const depthLimitedLeaf = d.depth >= depthLimit && (d.children?.length ?? 0) > 0
      if ((d.data.isLeaf || depthLimitedLeaf) && d.data.leafPrediction != null) {
        return d.data.leafPrediction === 1 ? '#16a34a' : '#dc2626'
      }
      if (String(d.data.id) === String(focusedNodeId.value)) return '#22c55e'
      if (highlightedNodeIds.has(String(d.data.id))) return '#fdba74'
      return collapsed.value.has(d.data.id) ? '#dc2626' : '#2563eb'
    })
    .attr('stroke', (d) => {
      if (String(d.data.id) === String(focusedNodeId.value)) return '#166534'
      return highlightedNodeIds.has(String(d.data.id)) ? '#ea580c' : '#1e293b'
    })
    .attr('stroke-width', (d) => {
      if (String(d.data.id) === String(focusedNodeId.value)) return 3
      return highlightedNodeIds.has(String(d.data.id)) ? 2.5 : 1
    })

  nodes
    .append('text')
    .attr('dy', -12)
    .attr('text-anchor', 'middle')
    .style('font-size', '12.5px')
    .style('font-weight', (d) => (d.data.isLeaf ? 600 : 500))
    .text((d) => d.data.name)

  const adjacentSplitNodes = visibleNodes.filter((node) => {
    const parentOnPath = node.parent && highlightedNodeIds.has(String(node.parent.data.id))
    const onPath = highlightedNodeIds.has(String(node.data.id))
    return node.children?.length && (parentOnPath || onPath)
  })

  graph
    .append('g')
    .selectAll('g')
    .data(adjacentSplitNodes)
    .join('g')
    .attr('transform', (d) => {
      const pos = positionedNodes.get(d.data.id)
      return `translate(${pos.px + 12},${pos.py - 26})`
    })
    .style('opacity', (d) => {
      if (!dimNonPathBranches.value) return 1
      return highlightedNodeIds.has(String(d.data.id)) ? 1 : 0.35
    })
    .style('cursor', 'pointer')
    .on('click', (event, d) => {
      event.stopPropagation()
      const next = new Set(collapsed.value)
      if (next.has(d.data.id)) {
        next.delete(d.data.id)
      } else {
        next.add(d.data.id)
      }
      collapsed.value = next
    })
    .call((group) => {
      group
        .append('rect')
        .attr('x', -10)
        .attr('y', -9)
        .attr('width', 20)
        .attr('height', 16)
        .attr('rx', 4)
        .attr('fill', '#ffffff')
        .attr('stroke', '#475569')
        .attr('stroke-width', 1)
      group
        .append('text')
        .attr('text-anchor', 'middle')
        .attr('dominant-baseline', 'middle')
        .style('font-size', '12px')
        .style('font-weight', 700)
        .attr('fill', '#0f172a')
        .text((d) => (collapsed.value.has(d.data.id) ? '+' : '−'))
    })

  setupZoom(width, height, contentBounds)
}

function setupZoom(svgWidth, svgHeight, contentBounds) {
  if (!treeSvg.value) return
  const svg = d3.select(treeSvg.value)
  const behavior =
    zoomBehavior.value ??
    d3
      .zoom()
      .scaleExtent([0.35, 4.5])
      .on('zoom', (event) => {
        zoomTransform.value = event.transform
        svg.select('.zoom-layer').attr('transform', event.transform)
      })

  zoomBehavior.value = behavior
  svg.call(behavior).on('dblclick.zoom', null)
  const hasExistingTransform = zoomTransform.value.k !== 1 || zoomTransform.value.x !== 0 || zoomTransform.value.y !== 0

  defaultTransform.value = fitTransform(contentBounds, svgWidth, svgHeight)
  const shouldUseDefault = shouldAutoFitTree.value || !hasExistingTransform
  const initialTransform = shouldUseDefault ? defaultTransform.value : zoomTransform.value
  svg.call(behavior.transform, initialTransform)
  shouldAutoFitTree.value = false
}

function fitTransform(bounds, svgWidth, svgHeight) {
  const width = Math.max(1, bounds.maxX - bounds.minX)
  const height = Math.max(1, bounds.maxY - bounds.minY)
  const centerX = (bounds.minX + bounds.maxX) / 2
  const centerY = (bounds.minY + bounds.maxY) / 2
  const padding = 50
  const scale = Math.max(
    0.35,
    Math.min(4.5, Math.min((svgWidth - padding * 2) / width, (svgHeight - padding * 2) / height))
  )
  return d3.zoomIdentity.translate(svgWidth / 2 - centerX * scale, svgHeight / 2 - centerY * scale).scale(scale)
}

function resetView() {
  dimNonPathBranches.value = false
  if (!treeSvg.value || !zoomBehavior.value) return
  d3.select(treeSvg.value).transition().duration(280).call(zoomBehavior.value.transform, defaultTransform.value)
}

function focusActivePath() {
  if (!treeSvg.value || !zoomBehavior.value || !latestVisibleLayout.value.length) return
  const highlighted = latestVisibleLayout.value.filter((node) => highlightedNodeIdsForView.value.has(node.id))
  if (!highlighted.length) return
  dimNonPathBranches.value = true

  const minX = d3.min(highlighted, (node) => node.x) ?? 0
  const maxX = d3.max(highlighted, (node) => node.x) ?? 0
  const minY = d3.min(highlighted, (node) => node.y) ?? 0
  const maxY = d3.max(highlighted, (node) => node.y) ?? 0
  const svgWidth = treeViewport.value?.clientWidth ?? 980
  const svgHeight = 560
  const focusBounds = { minX: minX - 65, maxX: maxX + 65, minY: minY - 65, maxY: maxY + 65 }
  const transform = fitTransform(focusBounds, svgWidth, svgHeight)
  d3.select(treeSvg.value).transition().duration(320).call(zoomBehavior.value.transform, transform)
}

function drawBars() {
  const svg = d3.select(barSvg.value)
  svg.selectAll('*').remove()
  const rows = explanation.value?.top_contributing_features ?? []
  if (!rows.length) return

  const width = 900
  const height = 280
  const margin = { top: 20, right: 20, bottom: 56, left: 200 }
  svg.attr('viewBox', `0 0 ${width} ${height}`)

  const x = d3.scaleLinear().domain([0, d3.max(rows, (d) => d.score) ?? 1]).nice().range([margin.left, width - margin.right])
  const y = d3.scaleBand().domain(rows.map((d) => d.feature)).range([margin.top, height - margin.bottom]).padding(0.25)

  svg.append('g').attr('transform', `translate(0,${height - margin.bottom})`).call(d3.axisBottom(x))
  svg.append('g').attr('transform', `translate(${margin.left},0)`).call(d3.axisLeft(y))
  svg
    .append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - 16)
    .attr('text-anchor', 'middle')
    .style('font-size', '12px')
    .style('fill', '#334155')
    .text('Cumulative split-margin contribution (Σ|feature value − split threshold| on active path)')

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
    <h2>Decision-tree explorer for selected match context</h2>
    <div class="filters">
      <label>
        Focal player
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
    <p>Choose a focal player, then choose a specific match row to explain.</p>
    <p v-if="!canRequestPrediction" class="error-text">{{ schemaGuardMessage }}</p>
    <p v-if="error" class="error-text">{{ error }}</p>
    <div v-if="contextPanel" class="path-summary">
      <h3>Match prediction context</h3>
      <p>
        <strong>player:</strong> {{ contextPanel.player_name }} ({{ contextPanel.player_id }}) ·
        <strong>opponent:</strong> {{ contextPanel.opponent_name }} ({{ contextPanel.opponent_id }}) ·
        <strong>match_date:</strong> {{ contextPanel.match_date }} ·
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
      <p>Prediction is for the selected match context (focal player vs opponent), not a general player rating.</p>
    </div>
    <h3>Collapsible tree (full model structure + active path)</h3>
    <div class="tree-controls">
      <label>
        Max depth: {{ maxDepth }}
        <input v-model.number="maxDepth" type="range" min="0" :max="maxDepthLimit" step="1" />
      </label>
      <label>
        <input v-model="showActiveContextOnly" type="checkbox" />
        Show only active path + 1-hop siblings
      </label>
      <label>
        <input v-model="expandAll" type="checkbox" />
        Expand all / Collapse all
      </label>
      <label>
        Feature search
        <input
          v-model.trim="featureSearch"
          type="text"
          placeholder="Type feature name (e.g., ace_rate)"
        />
      </label>
      <div class="search-actions">
        <button type="button" :disabled="!matchedFeatureNodeIds.length" @click="cycleFeatureMatch(-1)">Prev</button>
        <button type="button" :disabled="!matchedFeatureNodeIds.length" @click="cycleFeatureMatch(1)">Next</button>
        <span class="subtle">
          {{ matchedFeatureNodeIds.length ? `${activeMatchIndex + 1}/${matchedFeatureNodeIds.length}` : '0 matches' }}
        </span>
      </div>
    </div>
    <div v-if="fullExpansionBlocked" class="expansion-warning">
      Large tree detected ({{ treeStats.nodeCount }} nodes, depth {{ treeStats.maxDepth }}).
      Showing interpretable active-path context by default.
      <div class="tree-actions">
        <button type="button" @click="applyPreset('context')">Quick return: Context</button>
        <button type="button" @click="applyPreset('medium')">Preset: Depth 8</button>
        <button type="button" class="secondary" @click="applyPreset('full')">Show full tree anyway</button>
      </div>
    </div>
    <div class="tree-actions">
      <button type="button" class="secondary" @click="focusActivePath">Focus Active Path</button>
      <button type="button" @click="resetView">Reset view</button>
      <button type="button" @click="applyPreset('context')">Return to context preset</button>
    </div>
    <div ref="treeViewport" class="tree-scroll-wrap">
      <svg ref="treeSvg" class="chart chart-tree"></svg>
    </div>
    <div v-if="pathSummary" class="path-summary">
      <h3>Selected match-context path summary</h3>
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
