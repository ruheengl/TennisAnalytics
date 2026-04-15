<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { apiGet, apiPost } from './services/api'
import ClusterOverviewView from './views/ClusterOverviewView.vue'
import PlayerPerformanceView from './views/PlayerPerformanceView.vue'
import DecisionTreeExplorerView from './views/DecisionTreeExplorerView.vue'
import StoryStepper from './components/StoryStepper.vue'

const tabs = [
  { key: 'overview', label: 'Cluster Overview' },
  { key: 'performance', label: 'Player Performance' },
  { key: 'tree', label: 'Match Outcome Explainer' }
]

const activeTab = ref('overview')
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
const activeStoryStep = ref('overview')
const clusterRequestId = ref('')
const explainerContext = ref({
  matchLabel: 'No match selected',
  winProbability: null
})

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
const MAX_CLUSTER_PLAYERS_PAGE_SIZE = 500
const MAX_PLAYER_QUERY_LIMIT = 2000

const simplePresetOptions = {
  balanced: {
    label: 'Balanced',
    algorithm: 'kmeans',
    distanceMetric: 'euclidean',
    scaling: 'zscore',
    params: {
      maxIter: 75,
      seed: 42
    }
  },
  detailed: {
    label: 'Detailed',
    algorithm: 'gmm',
    distanceMetric: 'cosine',
    scaling: 'zscore',
    params: {
      maxIter: 150,
      seed: 42
    }
  },
  fast: {
    label: 'Fast',
    algorithm: 'kmeans',
    distanceMetric: 'manhattan',
    scaling: 'minmax',
    params: {
      maxIter: 30,
      seed: 42
    }
  }
}

const availableAttributes = computed(() => health.value?.default_attributes ?? [])
const predictorFeatureColumns = computed(() => health.value?.predict_feature_columns ?? [])

const projectionMetadata = computed(() => {
  const projection = clusterResult.value?.projection
  const explainedVarianceRatio = projection?.explained_variance_ratio ?? [0, 0]
  const topLoadings = projection?.top_absolute_loadings ?? [[], []]
  return {
    explainedVarianceRatio,
    topLoadings,
    hasProjectionAxes: Boolean(projection)
  }
})

const selectedParams = computed(() => {
  if (algorithm.value === 'dbscan') {
    return {
      eps: Number(eps.value),
      min_samples: Number(minSamples.value)
    }
  }

  if (algorithm.value === 'hierarchical') {
    return {
      k: Number(k.value),
      linkage: linkage.value
    }
  }

  return {
    k: Number(k.value),
    max_iter: Number(maxIter.value),
    seed: Number(seed.value)
  }
})

const activeClusteringConfig = computed(() => ({
  attributes: [...selectedAttributes.value],
  algorithm: algorithm.value,
  params: selectedParams.value,
  distance_metric: distanceMetric.value,
  scaling: scaling.value
}))

const canRunClustering = computed(() => selectedAttributes.value.length > 0)

const enrichedPlayers = computed(() => {
  const projectionPoints = clusterResult.value?.projection?.points ?? []
  const projectionByPlayer = Object.fromEntries(
    projectionPoints.map((point) => [point.player_id, point])
  )

  return playerRows.value
    .filter((row) => clusterByPlayer.value[row.player_id] !== undefined)
    .map((row) => ({
      ...row,
      cluster_id: clusterByPlayer.value[row.player_id],
      pc1: Number(projectionByPlayer[row.player_id]?.pc1 ?? 0),
      pc2: Number(projectionByPlayer[row.player_id]?.pc2 ?? 0)
    }))
})


const selectedClusterSummary = computed(() => {
  const players = enrichedPlayers.value
  if (!players.length) {
    return {
      clusterLabel: 'All clusters',
      summary: 'Run clustering to generate player archetypes.'
    }
  }

  const selectedId = selectedClusterId.value
  const selectedPlayers =
    selectedId === 'all' ? players : players.filter((player) => player.cluster_id === Number(selectedId))

  const avgWinPct =
    selectedPlayers.length
      ? selectedPlayers.reduce((sum, row) => sum + Number(row.career_win_pct ?? 0), 0) / selectedPlayers.length
      : 0

  return {
    clusterLabel: selectedId === 'all' ? 'All clusters' : `Cluster ${selectedId}`,
    summary: `${selectedPlayers.length} players, avg career win ${(avgWinPct * 100).toFixed(1)}%.`
  }
})

const selectedPlayerTrendSummary = computed(() => {
  const playerRows = enrichedPlayers.value.filter((player) => player.player_id === selectedPlayerId.value)

  if (!playerRows.length) {
    return {
      playerName: 'No player selected',
      trendHints: 'Select a player to inspect Elo, win%, and ace% trends.'
    }
  }

  const playerName = playerRows[0].player_name ?? playerRows[0].player_id
  const chronological = [...playerRows].sort((a, b) => String(a.match_date ?? '').localeCompare(String(b.match_date ?? '')))
  const first = chronological[0]
  const last = chronological[chronological.length - 1]
  const eloDelta = Number(last.elo_pre ?? 0) - Number(first.elo_pre ?? 0)
  const trendWord = eloDelta > 5 ? 'upward' : eloDelta < -5 ? 'downward' : 'stable'
  const recentWinPct = Number(last.career_win_pct ?? 0) * 100

  return {
    playerName,
    trendHints: `Elo ${trendWord} (${eloDelta >= 0 ? '+' : ''}${eloDelta.toFixed(1)}), latest career win ${recentWinPct.toFixed(1)}%.`
  }
})

onMounted(loadInitialState)

watch(activeTab, (nextTab) => {
  activeStoryStep.value = nextTab
})

watch(
  availableAttributes,
  (attrs) => {
    if (!attrs.length) return
    if (!selectedAttributes.value.length) {
      selectedAttributes.value = [...attrs]
    }
  },
  { immediate: true }
)

watch(
  [configMode, simplePreset, simpleGroupCount],
  () => {
    if (configMode.value !== 'simple') return

    const preset = simplePresetOptions[simplePreset.value] ?? simplePresetOptions.balanced

    algorithm.value = preset.algorithm
    distanceMetric.value = preset.distanceMetric
    scaling.value = preset.scaling
    k.value = Number(simpleGroupCount.value)
    maxIter.value = Number(preset.params.maxIter)
    seed.value = Number(preset.params.seed)
  },
  { immediate: true }
)

function normalizeParamsForPayload() {
  if (algorithm.value === 'dbscan') {
    return {
      eps: Number(eps.value),
      min_samples: Number(minSamples.value)
    }
  }

  if (algorithm.value === 'hierarchical') {
    return {
      k: Number(k.value),
      linkage: linkage.value
    }
  }

  return {
    k: Number(k.value),
    max_iter: Number(maxIter.value),
    seed: Number(seed.value)
  }
}

async function loadInitialState() {
  loading.value = true
  error.value = ''

  try {
    health.value = await apiGet('/health')
    if (!selectedAttributes.value.length) {
      selectedAttributes.value = [...(health.value.default_attributes ?? [])]
    }
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err)
  } finally {
    loading.value = false
  }
}

async function fetchClusterPlayers(clusterRequestId) {
  const firstPage = await apiGet(`/clusters/${clusterRequestId}/players`, {
    page: 1,
    page_size: MAX_CLUSTER_PLAYERS_PAGE_SIZE
  })
  const totalPages = Math.max(1, Math.ceil(firstPage.total / MAX_CLUSTER_PLAYERS_PAGE_SIZE))

  if (totalPages === 1) {
    return firstPage.players
  }

  const remainingPages = await Promise.all(
    Array.from({ length: totalPages - 1 }, (_, index) =>
      apiGet(`/clusters/${clusterRequestId}/players`, {
        page: index + 2,
        page_size: MAX_CLUSTER_PLAYERS_PAGE_SIZE
      })
    )
  )

  return [firstPage.players, ...remainingPages.map((page) => page.players)].flat()
}

async function runClustering() {
  if (!canRunClustering.value) return

  loading.value = true
  error.value = ''

  try {
    const payload = {
      attributes: [...selectedAttributes.value],
      algorithm: algorithm.value,
      params: normalizeParamsForPayload(),
      distance_metric: distanceMetric.value,
      scaling: scaling.value,
      filters: {}
    }

    clusterResult.value = await apiPost('/cluster', payload)
    const nextClusterRequestId = clusterResult.value.cluster_request_id
    clusterRequestId.value = nextClusterRequestId

    const [clusterPlayersResp, playersResp] = await Promise.all([
      fetchClusterPlayers(nextClusterRequestId),
      apiPost('/players/query', {
        filters: [],
        limit: MAX_PLAYER_QUERY_LIMIT,
        offset: 0,
        sort_by: 'career_win_pct',
        sort_order: 'desc',
        cluster_request_id: nextClusterRequestId
      })
    ])

    clusterPlayers.value = clusterPlayersResp
    clusterByPlayer.value = Object.fromEntries(clusterPlayersResp.map((p) => [p.player_id, p.cluster_id]))
    playerRows.value = playersResp.players
    reconcileStoryStateAfterClustering()
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err)
  } finally {
    loading.value = false
  }
}




function applyExplainerContext(payload) {
  if (!payload || typeof payload !== 'object') return
  explainerContext.value = {
    matchLabel: payload.matchLabel ?? 'No match selected',
    winProbability: payload.winProbability ?? null
  }
}

function applyMatchContextSelection(payload) {
  if (!payload || typeof payload !== 'object') return

  const nextPlayerId = String(payload.player_id ?? '')
  const nextMatchKey = String(payload.match_key ?? '')

  if (nextPlayerId) selectedPlayerId.value = nextPlayerId
  selectedMatchKey.value = nextMatchKey
  activeStoryStep.value = 'tree'
  activeTab.value = 'tree'
}

function reconcileStoryStateAfterClustering() {
  const validPlayers = enrichedPlayers.value
  const validPlayerIds = validPlayers.map((player) => player.player_id)
  const validClusterIds = [...new Set(validPlayers.map((player) => player.cluster_id))].sort((a, b) => a - b)

  if (
    selectedClusterId.value !== 'all' &&
    !validClusterIds.includes(Number(selectedClusterId.value))
  ) {
    if (!validClusterIds.length) {
      selectedClusterId.value = 'all'
    } else {
      const currentCluster = Number(selectedClusterId.value)
      const nearestCluster = validClusterIds.reduce((nearest, candidate) => {
        if (nearest === null) return candidate
        return Math.abs(candidate - currentCluster) < Math.abs(nearest - currentCluster) ? candidate : nearest
      }, null)
      selectedClusterId.value = String(nearestCluster ?? 'all')
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
  <main>
    <header class="app-header panel">
      <div>
        <h1>Tennis Player Analytics</h1>
        <p class="subtle">D3 + Vue views powered by live API endpoints.</p>
      </div>
      <button class="secondary" @click="loadInitialState" :disabled="loading">Refresh Health</button>
    </header>

    <section class="panel">
      <h2>Clustering configuration</h2>
      <div class="mode-toggle">
        <span class="subtle">Mode</span>
        <div class="mode-toggle-buttons">
          <button
            type="button"
            class="secondary"
            :class="{ active: configMode === 'simple' }"
            @click="configMode = 'simple'"
          >
            Simple
          </button>
          <button
            type="button"
            class="secondary"
            :class="{ active: configMode === 'advanced' }"
            @click="configMode = 'advanced'"
          >
            Advanced
          </button>
        </div>
      </div>

      <div class="filters">
        <label>
          Attributes
          <select v-model="selectedAttributes" multiple size="6">
            <option v-for="attributeName in availableAttributes" :key="attributeName" :value="attributeName">
              {{ attributeName }}
            </option>
          </select>
        </label>

        <template v-if="configMode === 'simple'">
          <label>
            How many player groups?
            <input v-model.number="simpleGroupCount" type="number" min="2" max="30" step="1" />
          </label>

          <label>
            Grouping style
            <select v-model="simplePreset">
              <option value="balanced">{{ simplePresetOptions.balanced.label }}</option>
              <option value="detailed">{{ simplePresetOptions.detailed.label }}</option>
              <option value="fast">{{ simplePresetOptions.fast.label }}</option>
            </select>
          </label>
        </template>

        <template v-else>
          <button type="button" class="secondary" @click="showAdvancedOptions = !showAdvancedOptions">
            {{ showAdvancedOptions ? 'Hide advanced settings' : 'Show advanced settings' }}
          </button>

          <div v-if="showAdvancedOptions" class="filters advanced-fields">
            <label>
              Algorithm
              <select v-model="algorithm">
                <option value="kmeans">kmeans</option>
                <option value="gmm">gmm</option>
                <option value="dbscan">dbscan</option>
                <option value="hierarchical">hierarchical</option>
              </select>
            </label>

            <label>
              Distance metric
              <select v-model="distanceMetric">
                <option value="euclidean">euclidean</option>
                <option value="manhattan">manhattan</option>
                <option value="cosine">cosine</option>
              </select>
            </label>

            <label>
              Scaling
              <select v-model="scaling">
                <option value="none">none</option>
                <option value="zscore">zscore</option>
                <option value="minmax">minmax</option>
              </select>
            </label>

            <label v-if="algorithm !== 'dbscan'">
              k
              <input v-model.number="k" type="number" min="2" max="30" step="1" />
            </label>

            <template v-if="algorithm === 'kmeans' || algorithm === 'gmm'">
              <label>
                max_iter
                <input v-model.number="maxIter" type="number" min="1" step="1" />
              </label>
              <label>
                seed
                <input v-model.number="seed" type="number" step="1" />
              </label>
            </template>

            <template v-else-if="algorithm === 'dbscan'">
              <label>
                eps
                <input v-model.number="eps" type="number" min="0.0001" step="0.01" />
              </label>
              <label>
                min_samples
                <input v-model.number="minSamples" type="number" min="1" step="1" />
              </label>
            </template>

            <label v-else-if="algorithm === 'hierarchical'">
              linkage
              <select v-model="linkage">
                <option value="average">average</option>
                <option value="complete">complete</option>
                <option value="single">single</option>
              </select>
            </label>
          </div>
        </template>
      </div>

      <button @click="runClustering" :disabled="loading || !canRunClustering">Generate player groups</button>
      <p class="subtle" v-if="clusterResult">
        Active cluster request: <strong>{{ clusterResult.cluster_request_id }}</strong>
      </p>
    </section>

    <StoryStepper
      :active-story-step="activeStoryStep"
      :active-tab="activeTab"
      :overview-context="selectedClusterSummary"
      :performance-context="selectedPlayerTrendSummary"
      :explainer-context="explainerContext"
      @update:active-story-step="activeStoryStep = $event"
      @update:active-tab="activeTab = $event"
    />

    <nav class="tabs panel">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="tab"
        :class="{ active: activeTab === tab.key }"
        @click="activeTab = tab.key"
      >
        {{ tab.label }}
      </button>
    </nav>

    <section v-if="loading" class="panel">Loading data from API...</section>
    <section v-else-if="error" class="panel error-text">{{ error }}</section>
    <section v-else-if="!clusterResult" class="panel">Select clustering settings and run clustering to load views.</section>

    <ClusterOverviewView
      v-else-if="activeTab === 'overview'"
      :cluster-result="clusterResult"
      :players="enrichedPlayers"
      :cluster-players="clusterPlayers"
      :clustering-config="activeClusteringConfig"
      :projection-metadata="projectionMetadata"
      :selected-cluster-id="selectedClusterId"
      :active-story-step="activeStoryStep"
      :cluster-request-id="clusterRequestId"
      @update:selected-cluster-id="selectedClusterId = $event"
      @update:active-story-step="activeStoryStep = $event"
    />


    <PlayerPerformanceView
      v-else-if="activeTab === 'performance'"
      :players="enrichedPlayers"
      :selected-player-id="selectedPlayerId"
      :active-story-step="activeStoryStep"
      :cluster-request-id="clusterRequestId"
      @update:selected-player-id="selectedPlayerId = $event"
      @update:active-story-step="activeStoryStep = $event"
      @select-match-context="applyMatchContextSelection"
    />


    <DecisionTreeExplorerView
      v-else
      :players="enrichedPlayers"
      :feature-columns="predictorFeatureColumns"
      :selected-player-id="selectedPlayerId"
      :selected-match-key="selectedMatchKey"
      :active-story-step="activeStoryStep"
      :cluster-request-id="clusterRequestId"
      @update:selected-player-id="selectedPlayerId = $event"
      @update:selected-match-key="selectedMatchKey = $event"
      @update:active-story-step="activeStoryStep = $event"
      @update:prediction-context="applyExplainerContext"
    />
  </main>
</template>
