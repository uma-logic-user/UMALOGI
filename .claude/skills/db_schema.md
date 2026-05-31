# DB スキーマリファレンス

このファイルは `v_race_mart` をはじめとする SQLite スキーマの
完全リファレンスです。エージェントはクエリ生成・スキーマ変更時に
必ずこのファイルを参照してください。

---

## 接続情報

```python
# src/database/init_db.py
from src.database.init_db import init_db, query_mart
conn = init_db()                      # PRAGMA 最適化済み接続を返す
rows = query_mart(conn, year="2024")  # v_race_mart への型安全クエリ
```

- DBパス: `data/umalogi.db`（環境変数 `DB_PATH` で上書き可）
- WAL モード・64MB キャッシュ・mmap 256MB 設定済み

---

## テーブル定義

### races — レース基本情報

| 列名 | 型 | 説明 |
|---|---|---|
| `race_id` | TEXT PK | 例: `202401010101` |
| `race_name` | TEXT | レース名 |
| `date` | TEXT | `YYYY/MM/DD` 形式 |
| `venue` | TEXT | 開催場所（東京・中山等） |
| `race_number` | INTEGER | レース番号 |
| `distance` | INTEGER | 距離（m） |
| `surface` | TEXT | 芝 / ダート / 障害 |
| `track_direction` | TEXT | 右 / 左 / 直線 / 右外 / 左外 |
| `weather` | TEXT | 晴 / 曇 / 雨 / 小雨 / 雪 |
| `condition` | TEXT | 良 / 稍重 / 重 / 不良 |

### race_results — 出走・着順結果

| 列名 | 型 | 説明 |
|---|---|---|
| `id` | INTEGER PK | |
| `race_id` | TEXT FK→races | |
| `horse_id` | TEXT FK→horses | NULL = 馬 ID 未解決 |
| `horse_name` | TEXT | |
| `rank` | INTEGER | NULL = 競走中止 |
| `gate_number` | INTEGER | 枠番 |
| `horse_number` | INTEGER | 馬番（払戻照合に使用） |
| `sex_age` | TEXT | 例: `牡3` |
| `weight_carried` | REAL | 斤量 |
| `jockey` | TEXT | 騎手名（コードなし） |
| `trainer` | TEXT | 調教師名（コードなし） |
| `finish_time` | TEXT | |
| `popularity` | INTEGER | 人気順 |
| `win_odds` | REAL | 単勝オッズ |
| `horse_weight` | INTEGER | 馬体重 (kg) |
| `horse_weight_diff` | INTEGER | 前走比 (+2, -4 等) |

**重要**: UNIQUE(race_id, horse_name)。INSERT OR IGNORE を使うこと。

### race_payouts — 確定払戻

| 列名 | 型 | 説明 |
|---|---|---|
| `race_id` | TEXT FK→races | |
| `bet_type` | TEXT | 単勝/複勝/馬連/ワイド/馬単/三連複/三連単/返還 |
| `combination` | TEXT | `"7"` / `"7-14"` / `"7→14→16"` |
| `payout` | INTEGER | 100円あたり払戻額 |
| `popularity` | INTEGER | 人気（複勝・ワイドは複数行） |

**返還エントリ**: `bet_type = '返還'` / `combination = 馬番` / `payout = 100`

### racehorses — 競走馬マスタ (DIFN:UM)

| 列名 | 型 | 説明 |
|---|---|---|
| `horse_id` | TEXT PK | blood_id |
| `father_id` | TEXT | 父の blood_id（breeding_horses との JOIN キー） |
| `father_name` | TEXT | 父名 |
| `grandsire_id` | TEXT | **母父** ID（maternal grandsire） |
| `grandsire_name` | TEXT | 母父名 |
| `east_west` | TEXT | 美浦 / 栗東 |
| `birth_year` | INTEGER | |

### jockeys — 騎手マスタ (DIFN:KS)

| 列名 | 型 | 説明 |
|---|---|---|
| `jockey_code` | TEXT PK | 騎手コード |
| `jockey_name` | TEXT | 騎手名（race_results との JOIN キー） |
| `east_west` | TEXT | 美浦 / 栗東 |
| `license_year` | INTEGER | 免許取得年 |

### trainers — 調教師マスタ (DIFN:CH)

| 列名 | 型 | 説明 |
|---|---|---|
| `trainer_code` | TEXT PK | 調教師コード |
| `trainer_name` | TEXT | 調教師名（race_results との JOIN キー） |
| `stable_name` | TEXT | 厩舎名 |
| `east_west` | TEXT | 美浦 / 栗東 |

### breeding_horses — 繁殖馬マスタ (BLOD:BT)

| 列名 | 型 | 説明 |
|---|---|---|
| `horse_id` | TEXT PK | blood_id |
| `father_id` | TEXT | 父の blood_id（祖父情報） |
| `father_name` | TEXT | 父名 |
| `mother_id` | TEXT | 母の blood_id（BMS系統） |
| `mother_name` | TEXT | 母名 |
| `birth_year` | INTEGER | 生年 |
| `country` | TEXT | 産地 |

### training_times — 調教タイム (WOOD:TC)

| 列名 | 型 | 説明 |
|---|---|---|
| `horse_id` | TEXT | |
| `training_date` | TEXT | `YYYY/MM/DD` |
| `time_4f` / `time_3f` / `time_2f` / `time_1f` | REAL | ハロンタイム（秒） |
| `lap_time` | REAL | ラスト1ハロン |
| `course_type` | TEXT | コース種別 |
| `gear` | TEXT | ギア（馬具） |

UNIQUE(horse_id, training_date, course_type, direction)

---

## v_race_mart — AI 学習用フラットビュー（63列）

`init_db()` 実行時に `_migrate_recreate_mart_view()` で自動再作成される。

### JOIN 構造

```
races (r)
  └─ race_results (rr)               ON rr.race_id = r.race_id
       ├─ race_payouts (rp_tan)       ON race_id + bet_type='単勝'  + CAST(horse_number)
       ├─ race_payouts (rp_fuk)       ON race_id + bet_type='複勝'  + CAST(horse_number)
       ├─ horses (h)                  ON h.horse_id = rr.horse_id
       ├─ racehorses (um)             ON um.horse_id = rr.horse_id
       │    └─ breeding_horses (bt)   ON bt.horse_id = um.father_id
       ├─ jockeys (ks)                ON ks.jockey_name = rr.jockey
       ├─ trainers (ch)               ON ch.trainer_name = rr.trainer
       ├─ training_times (tc)         ON horse_id + MAX(training_date) < r.date
       └─ training_hillwork (hc)      ON horse_id + MAX(training_date) < r.date
```

### 列一覧（グループ別）

| グループ | 列名 |
|---|---|
| races (11) | race_id, date, year, month, venue, race_number, distance, surface, track_direction, condition, weather |
| race_results (15) | result_id, horse_id, horse_number, gate_number, horse_name, sex_age, rank, win_odds, popularity, finish_time, horse_weight, horse_weight_diff, weight_carried, jockey, trainer |
| race_payouts (2) | payout_tansho, payout_fukusho |
| horses (3) | sire, dam, dam_sire |
| racehorses (9) | birth_year, um_sex, coat_color, country, father_id, father_name, grandsire_id, grandsire_name, horse_east_west |
| jockeys (3) | jockey_code, jockey_east_west, jockey_license_year |
| trainers (3) | trainer_code, trainer_east_west, stable_name |
| breeding_horses (6) | father_country, father_birth_year, father_sire_id, father_sire_name, father_dam_id, father_dam_name |
| training_times (6) | last_tc_date, last_tc_4f, last_tc_3f, last_tc_lap, last_tc_course, last_tc_gear |
| training_hillwork (5) | last_hc_date, last_hc_4f, last_hc_3f, last_hc_lap, last_hc_gear |

### クエリ例

```python
# Python: 型安全なクエリヘルパー
rows = query_mart(conn, year="2024", venue="東京", surface="芝")

# SQL: CTE を使った騎手別回収率集計
WITH base AS (
    SELECT
        jockey,
        jockey_code,
        COUNT(*)                          AS races,
        AVG(CASE WHEN rank = 1 THEN 1.0 ELSE 0.0 END) AS win_rate,
        AVG(payout_tansho / 100.0)        AS avg_tansho_return
    FROM v_race_mart
    WHERE year = '2024'
      AND payout_tansho IS NOT NULL
    GROUP BY jockey, jockey_code
)
SELECT * FROM base WHERE races >= 20 ORDER BY avg_tansho_return DESC;
```

---

## 主要インデックス

| インデックス | 用途 |
|---|---|
| `idx_rr_horse_raceid` | 馬の直近N走取得（horse_id + race_id DESC） |
| `idx_rr_jockey_raceid` | 騎手近走成績（jockey + race_id DESC） |
| `idx_rr_trainer_raceid` | 調教師近走成績（trainer + race_id DESC） |
| `idx_rp_race_bet` | 払戻照合（race_id + bet_type） |
| `idx_tc_horse_date` | 直近調教取得（horse_id + training_date DESC） |
| `idx_jockeys_name` | 騎手名結合（v_race_mart JOIN） |
| `idx_trainers_name` | 調教師名結合（v_race_mart JOIN） |
