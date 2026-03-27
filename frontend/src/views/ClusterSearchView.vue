<script setup>
import { computed, ref, watch } from 'vue'
import { apiGet, apiPost } from '../services/api'

const props = defineProps({
  clusterResult: { type: Object, required: true },
  players: { type: Array, required: true },
  clusteringConfig: { type: Object, required: true },
  projectionMetadata: { type: Object, required: true }
})

const nameQuery = ref('')
const searchResults = ref([])
const searchError = ref('')

const attributeOptions = [
  {
    value: 'career_win_pct',
    apiField: 'career_win_pct',
    label: 'Career win rate (%)',
    unitHint: '%',
    example: 'Example: At least 55%',
    isPercentLike: true
  },
  {
    value: 'elo_pre',
    apiField: 'elo_pre',
    label: 'Skill rating (Elo)',
    unitHint: 'Elo',
    example: 'Example: At least 1800',
    isPercentLike: false
  },
  {
    value: 'service_points_won_pct',
    apiField: 'service_points_won_pct',
    label: 'Service points won (%)',
    unitHint: '%',
    example: 'Example: Between 58% and 66%',
    isPercentLike: true
  },
  {
    value: 'return_points_won_pct',
    apiField: 'return_points_won_pct',
    label: 'Return points won (%)',
    unitHint: '%',
    example: 'Example: At most 45%',
    isPercentLike: true
  },
  {
    value: 'aces_per_service_game',
    apiField: 'aces_per_service_game',
    label: 'Aces per service game',
    unitHint: 'aces/game',
    example: 'Example: Between 0.20 and 0.60',
    isPercentLike: false
  }
]

const operatorOptions = [
  { value: 'gte', label: 'At least' },
  { value: 'lte', label: 'At most' },
  { value: 'between', label: 'Between' }
]

const attribute = ref(attributeOptions[0].value)
const op = ref('gte')
const minValue = ref('55')
const maxValue = ref('80')
const clusterFilter = ref('all')
const queryRows = ref([])
const queryTotal = ref(0)
const queryError = ref('')

const clusters = computed(() => [...new Set(props.players.map((p) => p.cluster_id))].sort((a, b) => a - b))
const projectionSummary = computed(() => {
  const ratios = props.projectionMetadata.explainedVarianceRatio ?? [0, 0]
  return `PC1 ${(Number(ratios[0]) * 100).toFixed(1)}% · PC2 ${(Number(ratios[1]) * 100).toFixed(1)}%`
})
const selectedAttribute = computed(
  () => attributeOptions.find((option) => option.value === attribute.value) ?? attributeOptions[0]
)
const percentInputMax = computed(() => (selectedAttribute.value.isPercentLike ? 100 : undefined))

function clampPercentInput(rawValue) {
  const numeric = Number(rawValue)
  if (Number.isNaN(numeric)) return rawValue
  if (numeric < 0) return '0'
  if (numeric > 100) return '100'
  return String(numeric)
}

function toPayloadValue(rawValue) {
  const numeric = Number(rawValue)
  if (Number.isNaN(numeric)) return Number.NaN
  if (selectedAttribute.value.isPercentLike) {
    const clamped = Math.max(0, Math.min(100, numeric))
    return clamped / 100
  }
  return numeric
}

function formatQueryValue(row) {
  const value = Number(row[selectedAttribute.value.apiField] ?? 0)
  if (selectedAttribute.value.isPercentLike) {
    return `${(value * 100).toFixed(1)}%`
  }
  return value.toFixed(3)
}

watch(
  () => props.clusterResult.cluster_request_id,
  () => {
    clusterFilter.value = 'all'
    searchResults.value = []
    queryRows.value = []
    queryTotal.value = 0
    searchError.value = ''
    queryError.value = ''
  }
)
watch(attribute, () => {
  if (selectedAttribute.value.isPercentLike) {
    minValue.value = clampPercentInput(minValue.value)
    maxValue.value = clampPercentInput(maxValue.value)
  }
})

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
  if (selectedAttribute.value.isPercentLike) {
    minValue.value = clampPercentInput(minValue.value)
    maxValue.value = clampPercentInput(maxValue.value)
  }

  const payloadMinValue = toPayloadValue(minValue.value)
  const payloadMaxValue = toPayloadValue(maxValue.value)
  if (Number.isNaN(payloadMinValue) || (op.value === 'between' && Number.isNaN(payloadMaxValue))) {
    queryError.value = 'Please enter valid numeric values.'
    return
  }

  const filter = {
    attribute: selectedAttribute.value.apiField,
    op: op.value,
    value: op.value === 'between' ? [payloadMinValue, payloadMaxValue] : payloadMinValue
  }

  try {
    const result = await apiPost('/players/query', {
      filters: [filter],
      limit: 100,
      offset: 0,
      sort_by: selectedAttribute.value.apiField,
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
      <p class="subtle">
        Run: {{ clusterResult.cluster_request_id }} · {{ clusteringConfig.algorithm }}
      </p>
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
      <p class="subtle">
        Projection variance: {{ projectionSummary }}
      </p>
      <div class="filters">
        <label>
          Attribute
          <select v-model="attribute">
            <option v-for="option in attributeOptions" :key="option.value" :value="option.value">
              {{ option.label }}
            </option>
          </select>
          <small class="subtle">Units: {{ selectedAttribute.unitHint }}</small>
        </label>
        <label>
          Operator
          <select v-model="op">
            <option v-for="option in operatorOptions" :key="option.value" :value="option.value">
              {{ option.label }}
            </option>
          </select>
        </label>
        <label>
          Min/value
          <input
            v-model="minValue"
            type="number"
            :min="selectedAttribute.isPercentLike ? 0 : undefined"
            :max="percentInputMax"
            step="0.01"
          />
          <small class="subtle">{{ selectedAttribute.example }}</small>
        </label>
        <label v-if="op === 'between'">
          Max
          <input
            v-model="maxValue"
            type="number"
            :min="selectedAttribute.isPercentLike ? 0 : undefined"
            :max="percentInputMax"
            step="0.01"
          />
          <small class="subtle">
            Example: Between
            {{ selectedAttribute.isPercentLike ? '40% and 65%' : 'two numeric bounds' }}.
          </small>
        </label>
      </div>
      <button @click="runFilterQuery">Run Query</button>
      <p v-if="queryError" class="error-text">{{ queryError }}</p>
      <p class="subtle">{{ queryTotal }} matching rows (showing first {{ queryRows.length }}).</p>
      <ul class="rank-list">
        <li v-for="row in queryRows" :key="row.player_id">
          <span>{{ row.player_id }}</span>
          <strong>{{ formatQueryValue(row) }}</strong>
        </li>
      </ul>
    </article>
  </section>
</template>
