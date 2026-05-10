# UMALOGI データ設計書（JRA-VAN / netkeiba ハイブリッド）

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-05-10 | 初版作成。ハイブリッド補完ルール・全テーブルスキーマを記述 |

---

## 1. データ取得戦略（ハイブリッド補完ルール）

JRA-VAN（JVLink）を一次ソース、netkeiba を二次（フォールバック）として運用する。

```
【原則】 JVLink が公式の真実。netkeiba は JVLink 失敗時の補完手段。

オッズ:
  JVLink realtime_odds (RTD) → realtime_odds テーブル空 → netkeiba fetch_odds()

エントリー/出走馬:
  JVLink SE レコード → entries テーブル空 → netkeiba fetch_entry_table()

確定結果/払戻:
  JVLink RACE (SE/HR レコード) → 未取得 → netkeiba update_payouts.py

レース基本情報:
  JVLink RA レコード → race_name/distance欠損 → netkeiba repair_race_data.py

調教タイム:
  JVLink WOOD (TC/HC) → 欠損許容 (fillna(-1) でモデルは継続動作)
```

---

## 2. JVLink データ仕様

### 2-1. データスペック

| DATASPEC | 内容 | 取得タイミング |
|---------|------|--------------|
| RACE | 出馬表(RA/SE)・成績(HR)・払戻 | 金曜夜・土日17:30後 |
| WOOD | 調教タイム(TC)・坂路(HC) | 土日07:30 |
| BLOD | 血統(BT)・繁殖馬 | 初期取込のみ |

### 2-2. オプション

| オプション | 意味 |
|-----------|------|
| OPT_NORMAL (1) | 差分取得（ポインタ以降） |
| OPT_STORED (2) | ローカルキャッシュから読込 |
| OPT_SETUP (3) | サーバーから全件強制取得 |

### 2-3. CP932 文字化け対策 (CLAUDE.md §10)

JVLink COM は CP932 バイト列を Latin-1 として返すことがある。

```python
# _to_bytes() で変換（src/scraper/jravan_client.py）
if ord(ch) <= 0xFF:
    byte = ch.encode('latin-1')   # CP932 リードバイトをそのまま保持
else:
    byte = ch.encode('cp932')     # Pattern 2 (正規 Unicode 日本語)

# 保存前スクリーニング（src/utils/text.py）
_GARBLED = re.compile(r'\?[^\s\?]{1,4}\?')  # ?X? パターン検出
sanitize_str(s)  # 制御文字 [\x00-\x08...] を除去
```

---

## 3. DB スキーマ（`data/umalogi.db`）

接続: `src/database/init_db.py` の `init_db()` 経由  
設定: `PRAGMA foreign_keys = ON` / `PRAGMA journal_mode = WAL`

### 3-1. 主要テーブル

#### `races` — レース基本情報

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT PK | 12桁 (YYYY場RR開催日2桁R番号2桁) |
| race_name | TEXT | レース名 (garbled 検査対象) |
| date | TEXT | 開催日 YYYY-MM-DD |
| venue | TEXT | 開催場 |
| race_number | INTEGER | R番号 |
| distance | INTEGER | 距離 (m) |
| surface | TEXT | 芝/ダート/障害 |
| weather | TEXT | 天候 |
| condition | TEXT | 馬場状態 |
| track_direction | TEXT | コース方向 |

#### `race_results` — 出走・着順結果

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT FK | races.race_id |
| horse_id | TEXT | 馬 ID |
| horse_name | TEXT | 馬名 |
| horse_number | INTEGER | 馬番 (NULL = CP932文字化け) |
| rank | INTEGER | 着順 (NULL/0 = 競走中止) |
| finish_time | TEXT | タイム |
| win_odds | REAL | 単勝オッズ |
| popularity | INTEGER | 人気順 |
| horse_weight | INTEGER | 馬体重 |
| horse_weight_diff | INTEGER | 前走比 |

#### `race_payouts` — 確定払戻

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT FK | |
| bet_type | TEXT | 単勝/複勝/馬連/ワイド/馬単/三連複/三連単/返還 |
| combination | TEXT | 馬番組み合わせ (例: "3-7-12") |
| payout | INTEGER | 払戻金額 (100円単位) |

**注意**: `bet_type='返還'` は対象馬券を 100円返還として処理。  
**注意**: 同着 (dead heat) は `rank` が同値の複数行。払戻は分割。

#### `predictions` — 予想バッチ

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT FK | |
| model_type | TEXT | Alpha-Payout(直前)/卍(直前)/本命(直前)/Oracle/HitFocus |
| bet_type | TEXT | 単勝/複勝/馬連/三連複/三連単 |
| confidence | REAL | モデル確信度 |
| expected_value | REAL | 期待値 (EV) |
| recommended_bet | REAL | 推奨投資額 (円) |
| combination_json | TEXT | JSON 配列の馬番組み合わせ |
| notes | TEXT | 補足情報 (EV・Harville確率等) |

#### `realtime_odds` — リアルタイムオッズ

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT FK | |
| horse_number | INTEGER | 馬番 |
| horse_name | TEXT | 馬名 |
| win_odds | REAL | 単勝オッズ |
| place_odds_min | REAL | 複勝オッズ下限 |
| place_odds_max | REAL | 複勝オッズ上限 |
| snapshot_time | TEXT | 取得時刻 |

#### `entries` — エントリー/出走馬

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT FK | |
| horse_number | INTEGER | 馬番 |
| gate_number | INTEGER | 枠番 |
| horse_id | TEXT | 馬 ID |
| horse_name | TEXT | 馬名 |
| jockey_id | TEXT | 騎手 ID |

### 3-2. マスタテーブル

| テーブル | 説明 | JVLink DIFN |
|---------|------|-----------|
| `horses` | 馬マスタ (血統 sire/dam/dam_sire) | UM |
| `racehorses` | 競走馬マスタ | UM |
| `jockeys` | 騎手マスタ | KS |
| `trainers` | 調教師マスタ | CH |
| `breeding_horses` | 繁殖馬マスタ | BLOD:BT |
| `training_times` | 調教タイム | WOOD:TC |
| `training_hillwork` | 坂路調教 | WOOD:HC |

### 3-3. ビュー

| ビュー | 説明 |
|-------|------|
| `v_race_mart` | AI学習用フラットビュー (63列・全テーブル結合済) |
| `v_analytics` | 予想精度分析ビュー |
| `v_prediction_summary` | 予想サマリー (model_type × bet_type 別集計) |
| `v_model_annual_summary` | 年度別モデルパフォーマンスサマリー |

---

## 4. データフロー図

```
JVLink COM (32bit専用)
  │  ← py -3.14-32 _jvlink_force_worker.py
  ├── RACE → races / race_results / race_payouts / entries
  ├── WOOD → training_times / training_hillwork
  └── BLOD → breeding_horses / horses

netkeiba (フォールバック・スクレイピング)
  │  ← src/scraper/netkeiba.py
  │  ← src/scraper/entry_table.py
  ├── レース基本情報 → races (race_name / distance / surface 補完)
  ├── 出走馬 → entries
  ├── オッズ → realtime_odds
  └── 払戻 → race_payouts
```

---

## 5. 文字化け対策 チェックリスト

保存前に必ず通過:
- [ ] `sanitize_str()` で制御文字除去
- [ ] `_GARBLED.search(s)` で `?X?` パターン検出・ワーニング
- [ ] バッチ完了後に `races.race_name LIKE '%?%'` で残留確認
- [ ] `repair_race_data.py --date` で事後修復
