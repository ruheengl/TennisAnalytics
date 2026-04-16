<script setup>
import { computed, nextTick, onMounted, ref, watch } from 'vue'
import { apiGet, apiPost } from './services/api'
import ClusterOverviewView from './views/ClusterOverviewView.vue'
import PlayerPerformanceView from './views/PlayerPerformanceView.vue'
import DecisionTreeExplorerView from './views/DecisionTreeExplorerView.vue'

const loading = ref(false)
const error = ref('')

const health = ref(null)
const clusterResult = ref(null)
const clusterPlayers = ref([])
const playerRows = ref([])
const clusterByPlayer = ref({})
const selectedClusterId = ref('all')
const selectedPlayerId = ref('')
const selectedMatchKey = ref('')
const playerTrendsPanelOpen = ref(false)
const predictionPanelOpen = ref(false)
const predictionPanelRef = ref(null)
const activeStoryStep = ref('overview')
const clusterRequestId = ref('')
const explainerContext = ref({ matchLabel: 'No match selected', winProbability: null })

const selectedAttributes = ref([])
const configMode = ref('simple')
const simplePreset = ref('balanced')
const simpleGroupCount = ref(6)
const showAdvancedOptions = ref(false)
const algorithm = ref('kmeans')
const k = ref(6)
const maxIter = ref(50)
const seed = ref(42)
const eps = ref(0.35)
const minSamples = ref(5)
const linkage = ref('average')
const distanceMetric = ref('euclidean')
const scaling = ref('zscore')
const MAX_CLUSTER_PLAYERS_PAGE_SIZE = 4000
const MAX_PLAYER_QUERY_LIMIT = 4000
const clusterPlayerLimit = ref(4000)

const simplePresetOptions = {
  balanced: { label: 'Balanced', algorithm: 'kmeans', distanceMetric: 'euclidean', scaling: 'zscore', params: { maxIter: 75, seed: 42 } },
  detailed: { label: 'Detailed', algorithm: 'gmm', distanceMetric: 'cosine', scaling: 'zscore', params: { maxIter: 150, seed: 42 } },
  fast:     { label: 'Fast',     algorithm: 'kmeans', distanceMetric: 'manhattan', scaling: 'minmax', params: { maxIter: 30, seed: 42 } }
}

const algoDescriptions = {
  kmeans: 'Assigns each player to the nearest centroid. Fast and works well for round, equally-sized groups.',
  gmm: 'Gaussian Mixture Model — like K-Means but allows oval-shaped groups and softer boundaries.',
  dbscan: 'Groups players by density. Can find oddly-shaped clusters and marks outliers automatically.',
  hierarchical: 'Builds a tree of nested groups by merging similar players step-by-step.'
}

const availableAttributes   = computed(() => health.value?.default_attributes ?? [])
const predictorFeatureColumns = computed(() => health.value?.predict_feature_columns ?? [])

const projectionMetadata = computed(() => {
  const projection = clusterResult.value?.projection
  return {
    explainedVarianceRatio: projection?.explained_variance_ratio ?? [0, 0],
    topLoadings: projection?.top_absolute_loadings ?? [[], []],
    hasProjectionAxes: Boolean(projection)
  }
})

const selectedParams = computed(() => {
  if (algorithm.value === 'dbscan')       return { eps: Number(eps.value), min_samples: Number(minSamples.value) }
  if (algorithm.value === 'hierarchical') return { k: Number(k.value), linkage: linkage.value }
  return { k: Number(k.value), max_iter: Number(maxIter.value), seed: Number(seed.value) }
})

const activeClusteringConfig = computed(() => ({
  attributes: [...selectedAttributes.value],
  algorithm: algorithm.value,
  params: selectedParams.value,
  distance_metric: distanceMetric.value,
  scaling: scaling.value
}))

const canRunClustering = computed(() => selectedAttributes.value.length > 0)

function normalizePlayerId(playerId) {
  const normalized = String(playerId ?? '').trim()
  if (/^\d+\.0$/.test(normalized)) return normalized.slice(0, -2)
  return normalized
}

const enrichedPlayers = computed(() => {
  const projectionPoints = clusterResult.value?.projection?.points ?? []
  const projectionByPlayer = Object.fromEntries(projectionPoints.map(p => [normalizePlayerId(p.player_id), p]))
  return playerRows.value.map(row => {
    const nid = normalizePlayerId(row.player_id)
    const clusterId = clusterByPlayer.value[nid]
    if (clusterId === undefined) return null
    return { ...row, player_id: nid, cluster_id: clusterId,
      pc1: Number(projectionByPlayer[nid]?.pc1 ?? 0),
      pc2: Number(projectionByPlayer[nid]?.pc2 ?? 0) }
  }).filter(Boolean)
})

const overviewPlayers = computed(() => {
  const projectionPoints = clusterResult.value?.projection?.points ?? []
  const statsByPlayer = Object.fromEntries(enrichedPlayers.value.map(p => [normalizePlayerId(p.player_id), p]))
  return projectionPoints.map(point => {
    const nid = normalizePlayerId(point.player_id)
    const clusterId = clusterByPlayer.value[nid]
    if (clusterId === undefined) return null
    const stats = statsByPlayer[nid]
    return { ...stats, player_id: nid, player_name: point.player_name ?? stats?.player_name ?? nid,
      cluster_id: clusterId, pc1: Number(point.pc1 ?? 0), pc2: Number(point.pc2 ?? 0) }
  }).filter(Boolean)
})

const statTotalPlayers = computed(() => overviewPlayers.value.length ? overviewPlayers.value.length.toLocaleString() : '—')
const statClusterCount = computed(() => clusterResult.value ? new Set(overviewPlayers.value.map(p => p.cluster_id)).size : '—')
const statAvgWinPct = computed(() => {
  const players = overviewPlayers.value
  if (!players.length) return '—'
  return ((players.reduce((s, p) => s + Number(p.career_win_pct ?? 0), 0) / players.length) * 100).toFixed(1) + '%'
})
const statAlgo = computed(() => algorithm.value)

const selectedPlayerDisplayName = computed(() => {
  if (!selectedPlayerId.value) return 'No player selected'
  return overviewPlayers.value.find(p => p.player_id === selectedPlayerId.value)?.player_name ?? selectedPlayerId.value
})

const playersInSelectedCluster = computed(() => {
  if (selectedClusterId.value === 'all') return enrichedPlayers.value
  return enrichedPlayers.value.filter(p => p.cluster_id === Number(selectedClusterId.value))
})

// Active tab — 'overview' | 'trends' | 'explainer'
const activeTab = ref('overview')
function setTab(tab) { activeTab.value = tab }

onMounted(loadInitialState)

watch(availableAttributes, attrs => {
  if (!attrs.length) return
  if (!selectedAttributes.value.length) selectedAttributes.value = [...attrs]
}, { immediate: true })

watch([configMode, simplePreset, simpleGroupCount], () => {
  if (configMode.value !== 'simple') return
  const preset = simplePresetOptions[simplePreset.value] ?? simplePresetOptions.balanced
  algorithm.value = preset.algorithm
  distanceMetric.value = preset.distanceMetric
  scaling.value = preset.scaling
  k.value = Number(simpleGroupCount.value)
  maxIter.value = Number(preset.params.maxIter)
  seed.value = Number(preset.params.seed)
}, { immediate: true })

watch([selectedClusterId, playersInSelectedCluster], ([, players]) => {
  const ids = players.map(p => String(p.player_id))
  if (!ids.length) {
    if (selectedPlayerId.value) { selectedPlayerId.value = ''; selectedMatchKey.value = '' }
    return
  }
  if (!ids.includes(selectedPlayerId.value)) {
    selectedPlayerId.value = ids[0]
    selectedMatchKey.value = ''
    explainerContext.value = { matchLabel: 'No match selected', winProbability: null }
  }
}, { immediate: true })

watch(selectedPlayerId, id => { if (id) playerTrendsPanelOpen.value = true })

function normalizeParamsForPayload() {
  if (algorithm.value === 'dbscan')       return { eps: Number(eps.value), min_samples: Number(minSamples.value) }
  if (algorithm.value === 'hierarchical') return { k: Number(k.value), linkage: linkage.value }
  return { k: Number(k.value), max_iter: Number(maxIter.value), seed: Number(seed.value) }
}

async function loadInitialState() {
  loading.value = true; error.value = ''
  try {
    health.value = await apiGet('/health')
    if (!selectedAttributes.value.length) selectedAttributes.value = [...(health.value.default_attributes ?? [])]
  } catch (err) { error.value = err instanceof Error ? err.message : String(err) }
  finally { loading.value = false }
}

async function fetchClusterPlayers(reqId, targetLimit = MAX_CLUSTER_PLAYERS_PAGE_SIZE) {
  const normLimit = Math.max(2, Number(targetLimit) || MAX_CLUSTER_PLAYERS_PAGE_SIZE)
  const pageSize = Math.min(normLimit, MAX_CLUSTER_PLAYERS_PAGE_SIZE)
  const firstPage = await apiGet(`/clusters/${reqId}/players`, { page: 1, page_size: pageSize })
  const effectiveTotal = Math.min(firstPage.total, normLimit)
  const totalPages = Math.max(1, Math.ceil(effectiveTotal / pageSize))
  if (totalPages === 1) return firstPage.players.slice(0, normLimit)
  const rest = await Promise.all(
    Array.from({ length: totalPages - 1 }, (_, i) =>
      apiGet(`/clusters/${reqId}/players`, { page: i + 2, page_size: pageSize }))
  )
  return [firstPage.players, ...rest.map(p => p.players)].flat().slice(0, normLimit)
}

async function runClustering() {
  if (!canRunClustering.value) return
  loading.value = true; error.value = ''
  try {
    const payload = {
      attributes: [...selectedAttributes.value], algorithm: algorithm.value,
      params: normalizeParamsForPayload(), distance_metric: distanceMetric.value,
      scaling: scaling.value, filters: {},
      player_limit: Math.max(2, Number(clusterPlayerLimit.value) || MAX_CLUSTER_PLAYERS_PAGE_SIZE)
    }
    clusterResult.value = await apiPost('/cluster', payload)
    const reqId = clusterResult.value.cluster_request_id
    clusterRequestId.value = reqId
    const [cpResp, pResp] = await Promise.all([
      fetchClusterPlayers(reqId, clusterPlayerLimit.value),
      apiPost('/players/query', {
        filters: [], limit: Math.min(MAX_PLAYER_QUERY_LIMIT, Math.max(2, Number(clusterPlayerLimit.value) || MAX_PLAYER_QUERY_LIMIT)),
        offset: 0, sort_by: 'career_win_pct', sort_order: 'desc', cluster_request_id: reqId
      })
    ])
    clusterPlayers.value = cpResp
    clusterByPlayer.value = Object.fromEntries(cpResp.map(p => [normalizePlayerId(p.player_id), p.cluster_id]))
    playerRows.value = pResp.players
    reconcileStoryStateAfterClustering()
  } catch (err) { error.value = err instanceof Error ? err.message : String(err) }
  finally { loading.value = false }
}

function applyExplainerContext(payload) {
  if (!payload || typeof payload !== 'object') return
  explainerContext.value = { matchLabel: payload.matchLabel ?? 'No match selected', winProbability: payload.winProbability ?? null }
}

function applyMatchContextSelection(payload) {
  if (!payload || typeof payload !== 'object') return
  const nextPlayerId = String(payload.player_id ?? '')
  const nextMatchKey = String(payload.match_key ?? '')
  if (nextPlayerId) selectedPlayerId.value = nextPlayerId
  selectedMatchKey.value = nextMatchKey
  activeStoryStep.value = 'tree'
  // Switch tab AFTER updating state so the already-mounted component picks up the new values
  setTab('explainer')
}

function reconcileStoryStateAfterClustering() {
  const validPlayers = enrichedPlayers.value
  const validPlayerIds = validPlayers.map(p => p.player_id)
  const validClusterIds = [...new Set(validPlayers.map(p => p.cluster_id))].sort((a, b) => a - b)
  if (selectedClusterId.value !== 'all' && !validClusterIds.includes(Number(selectedClusterId.value))) {
    if (!validClusterIds.length) {
      selectedClusterId.value = 'all'
    } else {
      const cur = Number(selectedClusterId.value)
      const nearest = validClusterIds.reduce((n, c) => n === null ? c : Math.abs(c - cur) < Math.abs(n - cur) ? c : n, null)
      selectedClusterId.value = String(nearest ?? 'all')
    }
  }
  if (!validPlayerIds.includes(selectedPlayerId.value)) {
    selectedPlayerId.value = validPlayerIds[0] ?? ''
    selectedMatchKey.value = ''
    explainerContext.value = { matchLabel: 'No match selected', winProbability: null }
  }
}
</script>

<template>
  <!-- ── Header ─────────────────────────────────────────────── -->
  <header class="app-header">
    <div class="header-left">
      <div class="app-logo">
        <svg viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
          <circle cx="8" cy="8" r="6.25" stroke-width="1.5"/>
          <path d="M4 8 Q5.5 4.5 8 8 Q10.5 11.5 12 8" stroke-width="1.2" stroke-linecap="round" fill="none"/>
        </svg>
      </div>
      <div class="app-title-block">
        <span class="app-title">Tennis Player Analytics</span>
        <span class="app-sub">ATP match data · CSCE 679 · Cluster → Explore → Explain</span>
      </div>
    </div>
    <div class="header-right">
      <div class="status-pill">
        <span class="status-dot"></span>
        <span v-if="health">API connected</span>
        <span v-else-if="loading">Connecting…</span>
        <span v-else style="color:var(--danger);">API unavailable</span>
      </div>
      <button class="run-btn" @click="runClustering" :disabled="loading || !canRunClustering"
        :title="!canRunClustering ? 'Select at least one attribute first' : 'Run the clustering algorithm with current settings'">
        <span v-if="loading">↺ Running…</span>
        <span v-else>▶ Run clustering</span>
      </button>
    </div>
  </header>

  <!-- ── Body ──────────────────────────────────────────────── -->
  <div class="app-body">

    <!-- ── Sidebar ─────────────────────────────────────────── -->
    <aside class="sidebar">

      <div class="sidebar-section">
        <div class="sidebar-label">How to use</div>
        <div class="sidebar-hint">
          <strong>1.</strong> Configure the settings below.<br>
          <strong>2.</strong> Click <strong>▶ Run clustering</strong> in the header.<br>
          <strong>3.</strong> Explore results in the tabs on the right.
        </div>
      </div>

      <div class="sidebar-section">
        <div class="sidebar-label">
          Clustering algorithm
          <span class="tip" :data-tip="algoDescriptions[algorithm]">?</span>
        </div>
        <select v-model="algorithm">
          <option value="kmeans">K-Means</option>
          <option value="gmm">GMM (Gaussian Mixture)</option>
          <option value="dbscan">DBSCAN</option>
          <option value="hierarchical">Hierarchical</option>
        </select>
        <div class="param-hint" style="margin-top:5px;">{{ algoDescriptions[algorithm] }}</div>
      </div>

      <div class="sidebar-section">
        <div class="sidebar-label">Configuration mode</div>
        <div class="mode-toggle-row" style="margin-bottom:5px;">
          <button class="mode-btn" :class="{ active: configMode === 'simple' }" @click="configMode = 'simple'">Simple</button>
          <button class="mode-btn" :class="{ active: configMode === 'advanced' }" @click="configMode = 'advanced'">Advanced</button>
        </div>
        <div class="param-hint" v-if="configMode === 'simple'">Pre-configured presets — good starting point.</div>
        <div class="param-hint" v-else>Full control over every parameter.</div>
      </div>

      <!-- Simple mode -->
      <template v-if="configMode === 'simple'">
        <div class="sidebar-section">
          <div class="sidebar-label">
            Preset
            <span class="tip" data-tip="Presets bundle algorithm settings optimised for different goals. Balanced is a safe default.">?</span>
          </div>
          <div class="preset-row">
            <button class="preset-btn" :class="{ active: simplePreset === 'balanced' }" @click="simplePreset = 'balanced'">Balanced</button>
            <button class="preset-btn" :class="{ active: simplePreset === 'detailed' }" @click="simplePreset = 'detailed'">Detailed</button>
            <button class="preset-btn" :class="{ active: simplePreset === 'fast' }" @click="simplePreset = 'fast'">Fast</button>
          </div>
          <div class="preset-hint" v-if="simplePreset === 'balanced'">K-Means · euclidean · z-score. Good all-purpose default.</div>
          <div class="preset-hint" v-else-if="simplePreset === 'detailed'">GMM · cosine · z-score. Finds subtler archetypes. Slower.</div>
          <div class="preset-hint" v-else>K-Means · manhattan · min-max. Fastest — great for quick exploration.</div>
        </div>

        <div class="sidebar-section">
          <div class="sidebar-label">
            Player groups (k = {{ simpleGroupCount }})
            <span class="tip" data-tip="How many distinct player archetypes to find. 4–8 works well for ATP data. More groups = finer detail but harder to interpret.">?</span>
          </div>
          <div class="param-row">
            <div class="param-lbl"><span>Groups</span><span class="param-val">{{ simpleGroupCount }}</span></div>
            <input type="range" min="2" max="20" step="1" v-model.number="simpleGroupCount" />
          </div>
          <div class="param-hint">Each group will be one player archetype (e.g. "big server", "baseline grinder").</div>
          <div class="param-row" style="margin-top:8px;">
            <div class="param-lbl"><span>Max iterations</span><span class="param-val">{{ maxIter }}</span></div>
            <input type="range" min="10" max="300" step="5" v-model.number="maxIter" />
          </div>
          <div class="param-hint">How many passes the algorithm makes. More = more accurate, but slower.</div>
        </div>
      </template>

      <!-- Advanced mode -->
      <template v-else>
        <div class="sidebar-section">
          <div class="sidebar-label">Advanced parameters</div>
          <div class="param-row">
            <div class="param-lbl">
              Distance metric
              <span class="tip" data-tip="How similarity between players is measured. Euclidean = straight-line distance. Cosine = angle between vectors (ignores scale). Manhattan = sum of absolute differences.">?</span>
            </div>
            <select v-model="distanceMetric">
              <option value="euclidean">Euclidean</option>
              <option value="manhattan">Manhattan</option>
              <option value="cosine">Cosine</option>
            </select>
          </div>
          <div class="param-row">
            <div class="param-lbl">
              Scaling
              <span class="tip" data-tip="Normalises stats so no single stat dominates. Z-score = mean 0, std 1. Min-max = 0 to 1. None = raw values.">?</span>
            </div>
            <select v-model="scaling">
              <option value="none">None</option>
              <option value="zscore">Z-score (recommended)</option>
              <option value="minmax">Min-max</option>
            </select>
          </div>
          <template v-if="algorithm !== 'dbscan'">
            <div class="param-row">
              <div class="param-lbl">
                Number of clusters (k)
                <span class="tip" data-tip="Target number of groups. Ignored by DBSCAN, which finds k automatically.">?</span>
              </div>
              <div class="param-lbl"><span></span><span class="param-val">{{ k }}</span></div>
              <input type="range" min="2" max="30" step="1" v-model.number="k" />
            </div>
          </template>
          <template v-if="algorithm === 'kmeans' || algorithm === 'gmm'">
            <div class="param-row">
              <div class="param-lbl">Max iterations<span class="param-val" style="margin-left:auto;">{{ maxIter }}</span></div>
              <input type="range" min="10" max="300" step="5" v-model.number="maxIter" />
            </div>
            <div class="param-row">
              <div class="param-lbl">
                Random seed
                <span class="tip" data-tip="Fixes the random starting point so results are reproducible. Change to get a different initialisation.">?</span>
              </div>
              <input type="number" v-model.number="seed" step="1" />
            </div>
          </template>
          <template v-else-if="algorithm === 'dbscan'">
            <div class="param-row">
              <div class="param-lbl">
                eps (neighbourhood radius)
                <span class="tip" data-tip="Maximum distance between two players to be neighbours. Smaller = tighter clusters. Typical range: 0.1–1.0.">?</span>
              </div>
              <input type="number" v-model.number="eps" min="0.0001" step="0.05" />
            </div>
            <div class="param-row">
              <div class="param-lbl">
                min_samples
                <span class="tip" data-tip="Minimum players needed to form a core cluster. Higher = fewer, larger clusters.">?</span>
              </div>
              <input type="number" v-model.number="minSamples" min="1" step="1" />
            </div>
          </template>
          <template v-else-if="algorithm === 'hierarchical'">
            <div class="param-row">
              <div class="param-lbl">
                Linkage
                <span class="tip" data-tip="How clusters merge. Average = mean pairwise distance. Complete = max distance. Single = min distance.">?</span>
              </div>
              <select v-model="linkage">
                <option value="average">Average</option>
                <option value="complete">Complete</option>
                <option value="single">Single</option>
              </select>
            </div>
          </template>
        </div>
      </template>

      <div class="sidebar-section">
        <div class="sidebar-label">
          Player limit
          <span class="tip" data-tip="Maximum number of players to include. Lower this to speed up clustering during exploration.">?</span>
        </div>
        <input type="number" v-model.number="clusterPlayerLimit" min="2" :max="MAX_CLUSTER_PLAYERS_PAGE_SIZE" step="100" />
        <div class="param-hint" style="margin-top:4px;">Max {{ MAX_CLUSTER_PLAYERS_PAGE_SIZE.toLocaleString() }} players. Lower for faster runs.</div>
      </div>

      <div class="sidebar-section">
        <div class="sidebar-label">
          Active attributes
          <span class="tip" data-tip="These stats are used as clustering dimensions. All attributes are selected by default. Use the selector in the Cluster Overview tab to change the selection.">?</span>
        </div>
        <div class="attr-list">
          <div v-for="(attr, i) in availableAttributes" :key="attr" class="attr-chip">
            <div class="chip-dot" :style="{ background: ['#0969da','#1a7f37','#f97316','#d4537e','#9a6700','#7f77dd'][i % 6] }"></div>
            <span>{{ attr }}</span>
          </div>
          <div v-if="!availableAttributes.length" class="param-hint">Attributes load after connecting to the API.</div>
        </div>
        <!-- Hidden multi-select keeps selectedAttributes v-model in sync with the API-driven list -->
        <select v-model="selectedAttributes" multiple style="display:none;">
          <option v-for="attr in availableAttributes" :key="attr" :value="attr">{{ attr }}</option>
        </select>
      </div>

    </aside>

    <!-- ── Main column ──────────────────────────────────────── -->
    <div class="main-col">

      <!-- ── Stat bar ─────────────────────────────────────────── -->
      <div class="stat-bar">
        <div class="stat-card" title="Total players included in the last clustering run">
          <div class="stat-num">{{ statTotalPlayers }}</div>
          <div class="stat-lbl">Players clustered</div>
          <div class="stat-meta" :class="clusterResult ? 'delta-up' : ''" style="color:var(--text-3)">
            {{ clusterResult ? '↑ Run complete' : 'No run yet' }}
          </div>
        </div>
        <div class="stat-card" title="Number of player archetypes found in the last run">
          <div class="stat-num">{{ statClusterCount }}</div>
          <div class="stat-lbl">Player archetypes</div>
          <div class="stat-meta" style="color:var(--text-3)">via {{ statAlgo }}</div>
        </div>
        <div class="stat-card" title="Mean career win percentage across all clustered players">
          <div class="stat-num">{{ statAvgWinPct }}</div>
          <div class="stat-lbl">Avg career win rate</div>
          <div class="stat-meta" style="color:var(--text-3)">{{ clusterResult ? 'All clusters' : '—' }}</div>
        </div>
        <div class="stat-card" title="Number of player stats being used as clustering dimensions">
          <div class="stat-num">{{ selectedAttributes.length }}</div>
          <div class="stat-lbl">Stat dimensions</div>
          <div class="stat-meta" style="color:var(--text-3)">{{ scaling }} scaling</div>
        </div>
      </div>

      <!-- ── Tab strip ────────────────────────────────────────── -->
      <div class="tab-strip">
        <button class="tab-btn" :class="{ active: activeTab === 'overview' }" @click="setTab('overview')"
          title="See all players plotted in 2D space. Each colour is a cluster. Click a dot to select a player.">
          1 · Cluster overview
          <span v-if="clusterResult" class="tab-badge">✓</span>
        </button>
        <button class="tab-btn" :class="{ active: activeTab === 'trends' }" @click="setTab('trends')"
          title="Explore Elo, win%, and ace% trends over time for a selected player.">
          2 · Player trends
          <span v-if="selectedPlayerId" class="tab-badge">✓</span>
        </button>
        <button class="tab-btn" :class="{ active: activeTab === 'explainer' }" @click="setTab('explainer')"
          title="Pick a specific match and see the decision tree path that led to the win/loss prediction.">
          3 · Match explainer
          <span v-if="explainerContext.winProbability != null" class="tab-badge">✓</span>
        </button>
      </div>

      <!-- ── View area ─────────────────────────────────────────── -->
      <div class="view-area">

        <div v-if="loading" class="state-msg">↺ Loading data…</div>
        <div v-else-if="error" class="state-msg error-text">⚠ {{ error }}</div>

        <!-- Onboarding -->
        <div v-else-if="!clusterResult" class="onboarding-card">
          <div style="font-size:32px;margin-bottom:12px;">🎾</div>
          <h3>Welcome to Tennis Player Analytics</h3>
          <p>This tool groups ATP players into archetypes using unsupervised clustering, then lets you explore their performance trends and explain individual match predictions.</p>
          <ul class="onboarding-steps">
            <li><div class="step-num">1</div><span>Choose an algorithm and preset in the <strong>sidebar on the left</strong>.</span></li>
            <li><div class="step-num">2</div><span>Click <strong>▶ Run clustering</strong> in the top-right corner.</span></li>
            <li><div class="step-num">3</div><span>Explore the scatter plot in <strong>Cluster overview</strong>. Click any dot to select a player.</span></li>
            <li><div class="step-num">4</div><span>Switch to <strong>Player trends</strong> to see Elo and win-rate history over time.</span></li>
            <li><div class="step-num">5</div><span>Use <strong>Match explainer</strong> to see why the model predicted a win or loss.</span></li>
          </ul>
          <button class="run-btn" @click="runClustering" :disabled="loading || !canRunClustering" style="margin:0 auto;">
            ▶ Run clustering now
          </button>
        </div>

        <!-- All three views stay mounted via v-show so state (brush window, tree zoom, loaded series) is preserved across tab switches -->
        <template v-else>

          <!-- Cluster overview -->
          <div v-show="activeTab === 'overview'">
            <div class="context-banner">
              <span class="cb-icon">💡</span>
              <span>
                Each dot is a player. <strong>Colour = cluster (archetype).</strong>
                Nearby dots play similarly. <strong>Click any dot</strong> to select that player, then switch to <em>Player trends</em> to dig deeper.
                Use the Cluster filter to isolate one group.
              </span>
            </div>
            <ClusterOverviewView
              :cluster-result="clusterResult" :players="overviewPlayers" :cluster-players="clusterPlayers"
              :clustering-config="activeClusteringConfig" :projection-metadata="projectionMetadata"
              :selected-cluster-id="selectedClusterId" :selected-player-id="selectedPlayerId"
              :active-story-step="activeStoryStep" :cluster-request-id="clusterRequestId"
              @update:selected-cluster-id="selectedClusterId = $event"
              @update:selected-player-id="selectedPlayerId = $event"
              @update:active-story-step="activeStoryStep = $event"
            />
          </div>

          <!-- Player trends -->
          <div v-show="activeTab === 'trends'">
            <div class="context-banner">
              <span class="cb-icon">💡</span>
              <span>
                <strong>Elo</strong> measures overall strength — higher is better.
                <strong>Win %</strong> and <strong>Ace %</strong> are career rolling averages.
                <strong>Brush the mini chart</strong> at the bottom to zoom into a time range.
                Once you pick a match, click <em>View predicted outcomes</em> to explain it.
              </span>
            </div>
            <div v-if="!selectedPlayerId" class="context-banner" style="background:var(--warn-bg);border-color:var(--warn-border);color:var(--warn);">
              <span class="cb-icon">⚠</span>
              <span>No player selected. Go to <strong>Cluster overview</strong> and click a dot first, or pick a player from the dropdown below.</span>
            </div>
            <PlayerPerformanceView
              :players="enrichedPlayers" :selected-player-id="selectedPlayerId"
              :selected-player-name="selectedPlayerDisplayName" :active-story-step="activeStoryStep"
              :cluster-request-id="clusterRequestId" embedded
              @update:selected-player-id="selectedPlayerId = $event"
              @update:active-story-step="activeStoryStep = $event"
              @select-match-context="applyMatchContextSelection"
            />
          </div>

          <!-- Match explainer -->
          <div v-show="activeTab === 'explainer'">
            <div class="context-banner">
              <span class="cb-icon">💡</span>
              <span>
                Select a <strong>focal player</strong> and a <strong>specific match row</strong> — the model traces the decision-tree path it used to predict the outcome.
                <strong>Orange nodes</strong> = active prediction path.
                <strong>Green leaf</strong> = predicted win · <strong>Red leaf</strong> = predicted loss.
                See the feature importance bars below the tree for which stats mattered most.
              </span>
            </div>
            <div v-if="!selectedPlayerId" class="context-banner" style="background:var(--warn-bg);border-color:var(--warn-border);color:var(--warn);">
              <span class="cb-icon">⚠</span>
              <span>No player selected. Start in <strong>Cluster overview</strong>, click a player dot, then come back here.</span>
            </div>
            <DecisionTreeExplorerView
              :players="enrichedPlayers" :feature-columns="predictorFeatureColumns"
              :selected-player-id="selectedPlayerId" :selected-match-key="selectedMatchKey"
              :active-story-step="activeStoryStep" :cluster-request-id="clusterRequestId"
              @update:selected-player-id="selectedPlayerId = $event"
              @update:selected-match-key="selectedMatchKey = $event"
              @update:active-story-step="activeStoryStep = $event"
              @update:prediction-context="applyExplainerContext"
            />
          </div>

        </template>
      </div>

      <!-- ── Footer ──────────────────────────────────────────── -->
      <footer class="app-footer">
        <span>
          <span v-if="clusterResult">
            Run ID: <code style="font-family:var(--font-mono);color:var(--accent-text);font-size:10px;">{{ clusterResult.cluster_request_id }}</code>
            · {{ algorithm }} · {{ distanceMetric }} distance · {{ scaling }} scaling · {{ selectedAttributes.length }} attributes
          </span>
          <span v-else style="color:var(--text-3);">No active run — configure settings in the sidebar and click Run clustering.</span>
        </span>
        <div class="footer-actions">
          <button class="footer-btn" @click="loadInitialState" :disabled="loading" title="Re-check API health and reload default attributes">↺ Refresh</button>
        </div>
      </footer>

    </div>
  </div>
</template>
