import racesJson       from '@/data/races.json'
import predictionsJson  from '@/data/predictions.json'
import summaryJson      from '@/data/summary.json'
import type { RaceEntry, Prediction } from '@/types/race'
import AppShell from '@/components/AppShell'

const races       = racesJson       as RaceEntry[]
const predictions = predictionsJson as Prediction[]
const summary     = summaryJson     as { total_races_in_db: number; overall: Record<string, unknown> }

export default function HomePage() {
  return (
    <AppShell races={races} predictions={predictions} summary={summary} />
  )
}
