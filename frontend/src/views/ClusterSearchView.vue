<script setup>
import { computed, ref } from 'vue'
import { apiGet, apiPost } from '../services/api'

const props = defineProps({
  clusterResult: { type: Object, required: true },
  players: { type: Array, required: true }
})

const nameQuery = ref('')
const searchResults = ref([])
const searchError = ref('')

const attribute = ref('career_win_pct')
const op = ref('gte')
const minValue = ref('0.55')
const maxValue = ref('0.80')
const clusterFilter = ref('all')
const queryRows = ref([])
const queryTotal = ref(0)
const queryError = ref('')

const clusters = computed(() => [...new Set(props.players.map((p) => p.cluster_id))].sort((a, b) => a - b))

async function runSearch() {
  searchError.value = ''
  try {
    const result = await apiGet('/players/search', {
      q: nameQuery.value,
      cluster_request_id: props.clusterResult.cluster_request_id,
      cluster_id: clusterFilter.value === 'all' ? undefined : Number(clusterFilter.value),
      limit: 30
    })
    searchResults.value = result.players
  } catch (err) {
    searchError.value = err instanceof Error ? err.message : String(err)
  }
}

async function runFilterQuery() {
  queryError.value = ''
  const filter = {
    attribute: attribute.value,
    op: op.value,
    value: op.value === 'between' ? [Number(minValue.value), Number(maxValue.value)] : Number(minValue.value)
  }

  try {
    const result = await apiPost('/players/query', {
      filters: [filter],
      limit: 100,
      offset: 0,
      sort_by: attribute.value,
      sort_order: 'desc',
      cluster_request_id: props.clusterResult.cluster_request_id,
      cluster_label: clusterFilter.value === 'all' ? null : Number(clusterFilter.value)
    })

    queryRows.value = result.players
    queryTotal.value = result.total
  } catch (err) {
    queryError.value = err instanceof Error ? err.message : String(err)
  }
}
</script>

<template>
  <section class="panel split-two">
    <article class="panel nested">
      <h2>Player name search</h2>
      <div class="filters">
        <label>
          Name contains
          <input v-model="nameQuery" placeholder="Djok..." />
        </label>
        <label>
          Cluster
          <select v-model="clusterFilter">
            <option value="all">All</option>
            <option v-for="c in clusters" :key="c" :value="String(c)">Cluster {{ c }}</option>
          </select>
        </label>
      </div>
      <button @click="runSearch" :disabled="!nameQuery.trim()">Search</button>
      <p v-if="searchError" class="error-text">{{ searchError }}</p>
      <ul class="rank-list">
        <li v-for="player in searchResults" :key="player">{{ player }}</li>
      </ul>
    </article>

    <article class="panel nested">
      <h2>Attribute filters</h2>
      <div class="filters">
        <label>
          Attribute
          <select v-model="attribute">
            <option value="career_win_pct">career_win_pct</option>
            <option value="elo_pre">elo_pre</option>
            <option value="service_points_won_pct">service_points_won_pct</option>
            <option value="return_points_won_pct">return_points_won_pct</option>
            <option value="aces_per_service_game">aces_per_service_game</option>
          </select>
        </label>
        <label>
          Operator
          <select v-model="op">
            <option value="gte">&gt;=</option>
            <option value="lte">&lt;=</option>
            <option value="between">between</option>
          </select>
        </label>
        <label>
          Min/value
          <input v-model="minValue" type="number" step="0.01" />
        </label>
        <label v-if="op === 'between'">
          Max
          <input v-model="maxValue" type="number" step="0.01" />
        </label>
      </div>
      <button @click="runFilterQuery">Run Query</button>
      <p v-if="queryError" class="error-text">{{ queryError }}</p>
      <p class="subtle">{{ queryTotal }} matching rows (showing first {{ queryRows.length }}).</p>
      <ul class="rank-list">
        <li v-for="row in queryRows" :key="row.player_id">
          <span>{{ row.player_id }}</span>
          <strong>{{ Number(row[attribute] ?? 0).toFixed(3) }}</strong>
        </li>
      </ul>
    </article>
  </section>
</template>
