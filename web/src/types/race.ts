export interface Race {
  race_id:         string
  race_name:       string
  year:            string | null
  date:            string
  venue:           string
  race_number:     number
  distance:        number
  surface:         string
  track_direction: string   // "右" / "左" / "右外" / "左外" / "直線" / ""
  weather:         string
  condition:       string
}

export interface RaceResult {
  rank:              number | null
  gate_number:       number | null
  horse_number:      number | null
  horse_name:        string
  horse_id:          string | null
  sex_age:           string
  weight_carried:    number
  jockey:            string
  trainer:           string | null
  finish_time:       string | null
  margin:            string | null
  win_odds:          number | null
  popularity:        number | null
  horse_weight:      number | null
  horse_weight_diff: number | null
  sire:              string | null
  dam:               string | null
  dam_sire:          string | null
  // AI直前予想フィールド（prerace snapshot からマージ）
  honmei_score:      number | null | undefined
  ev_score:          number | null | undefined
  kelly_fraction:    number | null | undefined
  manji_ev:          number | null | undefined
  odds_vs_morning:   number | null | undefined   // 最新オッズ / 朝一オッズ
  odds_velocity:     number | null | undefined   // オッズ下落速度 (倍/分)
  training_eval:     TrainingEval | null | undefined
}

export interface TrainingEval {
  eval_grade: 'A' | 'B' | 'C' | 'D' | string
  eval_text:  string
}

export interface RaceBias {
  today_inner_bias:  number | null   // 内枠率 - 外枠率（プラス = 内枠有利）
  today_front_bias:  number | null   // 前残り率（1番人気勝率）
  today_race_count:  number | null   // 当日先行レース数（サンプル）
}

export interface EvRecommend {
  horse_number:   number
  horse_name:     string
  win_odds:       number | null
  ev_score:       number
  kelly_fraction: number
}

export interface RacePayout {
  bet_type:    string   // "単勝" / "複勝" / "枠連" / "馬連" / "ワイド" / "馬単" / "三連複" / "三連単"
  combination: string   // "14" / "7-14" / "14→7→16"
  payout:      number   // 払戻金（100円あたり）
  popularity:  number | null
}

/** races.json の各エントリ（レース情報 + 結果 + 払戻） */
export interface RaceEntry extends Race {
  results: RaceResult[]
  payouts: RacePayout[]
}

/** races/{race_id}.json の詳細エントリ（RaceEntry + AI予想 + 直前データ） */
export interface RaceDetail extends RaceEntry {
  predictions:    RacePrediction[]
  training_evals: Record<string, TrainingEval>
  prerace?: {
    bias:         RaceBias
    ev_recommend: EvRecommend[]
    generated_at: string
  }
}

/** 後方互換用 */
export interface RaceData {
  race: Race
  results: RaceResult[]
}

export interface PredictionHorse {
  horse_name:    string
  horse_id:      string | null
  predicted_rank: number | null
  model_score:   number | null
  ev_score:      number | null
}

export interface Prediction {
  prediction_id:   number
  race_id:         string
  race_name:       string
  year:            string | null
  date:            string
  venue:           string
  race_number:     number
  surface:         string
  distance:        number
  weather:         string
  condition:       string
  model_type:      string
  bet_type:        string
  confidence:      number | null
  expected_value:  number | null
  recommended_bet: number | null
  combination_json: string | null
  notes:           string | null
  created_at:      string
  is_hit:          number | null   // 0 | 1 | null (未照合)
  payout:          number | null
  profit:          number | null
  roi:             number | null
  horses:          PredictionHorse[]
}

/** races/{race_id}.json 内の予想（Prediction のサブセット） */
export interface RacePrediction {
  prediction_id:   number
  model_type:      string
  bet_type:        string
  confidence:      number | null
  expected_value:  number | null
  recommended_bet: number | null
  combination_json: string | null
  notes:           string | null
  created_at:      string
  is_hit:          number | null
  payout:          number | null
  profit:          number | null
  roi:             number | null
  horses:          PredictionHorse[]
}
