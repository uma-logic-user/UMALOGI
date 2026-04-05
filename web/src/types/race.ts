export interface Race {
  race_id: string
  race_name: string
  date: string
  venue: string
  race_number: number
  distance: number
  surface: string
  weather: string
  condition: string
}

export interface RaceResult {
  rank: number | null
  horse_name: string
  horse_id: string | null
  sex_age: string
  weight_carried: number
  jockey: string
  finish_time: string | null
  margin: string | null
  win_odds: number | null
  popularity: number | null
  horse_weight: number | null
  sire: string | null
  dam: string | null
  dam_sire: string | null
}

export interface RaceData {
  race: Race
  results: RaceResult[]
}
