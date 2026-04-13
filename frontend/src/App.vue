<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { apiGet, apiPost } from './services/api'
import ClusterOverviewView from './views/ClusterOverviewView.vue'
import PlayerPerformanceView from './views/PlayerPerformanceView.vue'
import DecisionTreeExplorerView from './views/DecisionTreeExplorerView.vue'

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

onMounted(loadInitialState)

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
    const clusterRequestId = clusterResult.value.cluster_request_id

    const [clusterPlayersResp, playersResp] = await Promise.all([
      fetchClusterPlayers(clusterRequestId),
      apiPost('/players/query', {
        filters: [],
        limit: MAX_PLAYER_QUERY_LIMIT,
        offset: 0,
        sort_by: 'career_win_pct',
        sort_order: 'desc',
        cluster_request_id: clusterRequestId
      })
    ])

    clusterPlayers.value = clusterPlayersResp
    clusterByPlayer.value = Object.fromEntries(clusterPlayersResp.map((p) => [p.player_id, p.cluster_id]))
    playerRows.value = playersResp.players
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err)
  } finally {
    loading.value = false
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
    />


    <PlayerPerformanceView
      v-else-if="activeTab === 'performance'"
      :players="enrichedPlayers"
    />


    <DecisionTreeExplorerView
      v-else
      :players="enrichedPlayers"
      :feature-columns="predictorFeatureColumns"
    />
  </main>
</template>
