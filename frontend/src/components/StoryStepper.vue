<script setup>
import { computed } from 'vue'

const props = defineProps({
  activeStoryStep: { type: String, default: 'overview' },
  activeTab: { type: String, default: 'overview' },
  overviewContext: {
    type: Object,
    default: () => ({
      clusterLabel: 'All clusters',
      summary: 'Generate groups to inspect archetypes.'
    })
  },
  performanceContext: {
    type: Object,
    default: () => ({
      playerName: 'No player selected',
      trendHints: 'Choose a player to inspect timeline behavior.'
    })
  },
  explainerContext: {
    type: Object,
    default: () => ({
      matchLabel: 'No match selected',
      winProbability: null
    })
  }
})

const emit = defineEmits(['update:activeStoryStep', 'update:activeTab'])

const steps = [
  {
    key: 'overview',
    title: 'Step 1',
    subtitle: 'Segment player archetypes (Cluster Overview)'
  },
  {
    key: 'performance',
    title: 'Step 2',
    subtitle: 'Validate trend behavior (Player Performance)'
  },
  {
    key: 'tree',
    title: 'Step 3',
    subtitle: 'Explain a concrete match prediction (Decision-tree Explainer)'
  }
]

const currentStep = computed(() => props.activeStoryStep || props.activeTab || 'overview')

const helperText = computed(() => {
  if (currentStep.value === 'overview') {
    return `${props.overviewContext.clusterLabel}: ${props.overviewContext.summary}`
  }

  if (currentStep.value === 'performance') {
    return `${props.performanceContext.playerName}: ${props.performanceContext.trendHints}`
  }

  const winProb = Number(props.explainerContext.winProbability)
  const probLabel = Number.isFinite(winProb)
    ? `${(winProb * 100).toFixed(1)}% win probability`
    : 'win probability pending'
  return `${props.explainerContext.matchLabel}: ${probLabel}`
})

function activateStep(stepKey) {
  emit('update:activeStoryStep', stepKey)
  emit('update:activeTab', stepKey)
}
</script>

<template>
  <section class="panel story-stepper">
    <div class="stepper-row">
      <button
        v-for="step in steps"
        :key="step.key"
        type="button"
        class="step-chip"
        :class="{ active: currentStep === step.key }"
        @click="activateStep(step.key)"
      >
        <span class="step-title">{{ step.title }}</span>
        <span class="step-subtitle">{{ step.subtitle }}</span>
      </button>
    </div>
    <p class="subtle helper-text">{{ helperText }}</p>
  </section>
</template>

<style scoped>
.story-stepper {
  padding: 0.75rem 1rem;
}

.stepper-row {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.6rem;
}

.step-chip {
  border: 1px solid #cbd5e1;
  border-radius: 0.6rem;
  background: #f8fafc;
  padding: 0.45rem 0.55rem;
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 0.2rem;
  text-align: left;
}

.step-chip.active {
  border-color: #2563eb;
  background: #dbeafe;
}

.step-title {
  font-size: 0.75rem;
  font-weight: 700;
  color: #1e293b;
}

.step-subtitle {
  font-size: 0.73rem;
  line-height: 1.2;
}

.helper-text {
  margin: 0.55rem 0 0;
}
</style>
