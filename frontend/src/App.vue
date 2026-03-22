<script setup>
import { computed, onMounted, ref } from 'vue'
import { apiGet, apiPost } from './services/api'
import ClusterOverviewView from './views/ClusterOverviewView.vue'
import ClusterSearchView from './views/ClusterSearchView.vue'
import PlayerPerformanceView from './views/PlayerPerformanceView.vue'
import DegradationExplorerView from './views/DegradationExplorerView.vue'
import DecisionTreeExplorerView from './views/DecisionTreeExplorerView.vue'

const tabs = [
  { key: 'overview', label: 'Cluster Overview' },
  { key: 'search', label: 'Cluster Search / Query' },
  { key: 'performance', label: 'Player Performance' },
  { key: 'degradation', label: 'Degradation Explorer' },
  { key: 'tree', label: 'Decision-Tree Explorer' }
]

const activeTab = ref('overview')
const loading = ref(false)
const error = ref('')

const health = ref(null)
const clusterResult = ref(null)
const clusterPlayers = ref([])
const playerRows = ref([])
const clusterByPlayer = ref({})

const enrichedPlayers = computed(() => {
  return playerRows.value
    .filter((row) => clusterByPlayer.value[row.player_id] !== undefined)
    .map((row) => ({
      ...row,
      cluster_id: clusterByPlayer.value[row.player_id]
    }))
})

onMounted(loadBootstrapData)

async function loadBootstrapData() {
  loading.value = true
  error.value = ''

  try {
    health.value = await apiGet('/health')
    const attributes = health.value.default_attributes ?? []

    clusterResult.value = await apiPost('/cluster', {
      attributes,
      k: 6,
      distance_metric: 'euclidean',
      scaling: 'zscore',
      max_iter: 50,
      seed: 42,
      filters: {}
    })

    const [clusterPlayersResp, playersResp] = await Promise.all([
      apiGet(`/clusters/${clusterResult.value.cluster_request_id}/players`, {
        page: 1,
        page_size: 2500
      }),
      apiPost('/players/query', {
        filters: [],
        limit: 2500,
        offset: 0,
        sort_by: 'career_win_pct',
        sort_order: 'desc'
      })
    ])

    clusterPlayers.value = clusterPlayersResp.players
    clusterByPlayer.value = Object.fromEntries(clusterPlayersResp.players.map((p) => [p.player_id, p.cluster_id]))
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
      <button class="secondary" @click="loadBootstrapData" :disabled="loading">Refresh Data</button>
    </header>

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

    <ClusterOverviewView
      v-else-if="activeTab === 'overview'"
      :cluster-result="clusterResult"
      :players="enrichedPlayers"
      :cluster-players="clusterPlayers"
    />

    <ClusterSearchView
      v-else-if="activeTab === 'search'"
      :cluster-result="clusterResult"
      :players="enrichedPlayers"
    />

    <PlayerPerformanceView
      v-else-if="activeTab === 'performance'"
      :players="enrichedPlayers"
    />

    <DegradationExplorerView
      v-else-if="activeTab === 'degradation'"
      :players="enrichedPlayers"
    />

    <DecisionTreeExplorerView
      v-else
      :players="enrichedPlayers"
      :feature-columns="health?.default_attributes ?? []"
    />
  </main>
</template>
