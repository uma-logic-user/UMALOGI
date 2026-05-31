# UMALOGI システム総合監査レポート
**監査日**: 2026-05-05  
**対象ブランチ**: master  
**監査者**: Claude Code (claude-sonnet-4-6)  
**ステータス**: 商用化直前 健全性チェック

---

## エグゼクティブサマリー

3軸の並行監査を実施した結果、**P0（即時対応必須）の致命的バグ** が1件確認された。
記録済み ROI・払戻額がすべて実際の **最大 N 倍**（N＝購入点数）に過大計算されており、
商用化前に必ず修正が必要である。それ以外にも P1〜P2 の問題が複数存在する。

| 優先度 | 件数 | 概要 |
|--------|------|------|
| **P0 — 即時修正** | 2 | ROI過大計算・複数的中sum化漏れ |
| **P1 — 高優先** | 4 | CLAUDE.mdルール違反・複勝頭数バグ・WH名前衝突・rank UNIQUE誤り |
| **P2 — 中優先** | 5 | DDL重複・N+1クエリ・netkeiba存続・馬単ラベル・trainingEvals未定義 |
| **P3 — 低優先** | 6 | formatCombinations重複・型ヒント欠損・BET_ORDER三重定義・その他 |

---

## 1. データ整合性（Data Integrity）

### 1-1. 払戻パーサー ✅ 修正済み

- `_parse_payout` (`jravan_client.py:1021-1067`): かつての "円" 文字列連結バグは完全に排除。払戻金は5バイトASCII整数として `_int()` で読む。
- `amount >= 100` 検証でゴミエントリ自動除外済み。
- UPSERT (`ON CONFLICT DO UPDATE SET`) で重複挿入も安全。

**残存リスク（軽微）**: `cat='2'` 速報払戻が 100 円以上なら DB に書き込まれる。その後 `cat='1'` 確定データで上書きされるはずだが、確定データが未到着の場合に速報値が残留する可能性がある。

---

### 1-2. race_results.rank 汚染 ✅ 修正済み

- DBトリガー `trg_race_results_rank_ins` / `trg_race_results_rank_upd` (`init_db.py:581-621`) で `rank > 18` を DB 層でブロック。
- `_migrate_purge_binary_horse_names` でバイナリゴミ馬名レコード削除済み。

---

### 1-3. **[P1] WH コード名前衝突バグ**

**ファイル**: `jravan_client.py:168, 808-809`

`_PAYOUT_SPECS` に `"WH"` キーが存在する（旧払戻仕様）。`parse_record()` で `if rec_type in _PAYOUT_SPECS` チェックが `elif rec_type == 'WH'`（坂路調教パーサー）より先に評価されるため、**坂路調教 (WH) レコードが来ても `_parse_wh()` に絶対に到達せず、払戻パーサーに誤送信される**。

通常 DATASPEC_WOOD と DATASPEC_RACE は別セッションで JVOpen するため実害は限定的だが、論理的に壊れており、万一同一ストリームに混在した場合は訓練データが汚染される。

**修正方針**: `_PAYOUT_SPECS` から `"WH"` を削除（旧WH払戻は現行JRA-VANでは存在しない）か、`parse_record()` の分岐順序を修正。

---

### 1-4. **[P1] 複勝・ワイドの出走頭数依存バグ**

**ファイル**: `src/evaluation/evaluator.py:32`, `src/ml/models.py:212`

```python
_PLACE_RANKS = {1, 2, 3}  # 固定値
is_placed = (rank <= 3)    # 出走頭数を無視
```

JRA 公式ルールでは出走頭数 **7頭以下は複勝2着まで、8頭以上は3着まで**。7頭立て以下のレースで3着馬を「複勝的中」と誤判定し、過大払戻を記録してしまう。バックテストの信頼性を損なう。

**修正方針**: 評価時に `race_results` の `n_horses`（または `races.n_horses`）を取得し、`place_limit = 2 if n_horses <= 7 else 3` として判定。

---

### 1-5. race_results の UNIQUE 制約誤り

**ファイル**: `schema.py:67`, `jravan_client.py:1512`

`UNIQUE(race_id, horse_name)` が設定されており、同名馬2頭が同一レースに出走する稀なケースで2頭目が1頭目に上書きされる。本来は `UNIQUE(race_id, horse_number)` であるべき。

**修正方針**: マイグレーションで制約を `(race_id, horse_number)` に張り替え（`_save_se` の ON CONFLICT も合わせて変更）。

---

### 1-6. v_race_mart の NULL ホットスポット

| カラム | NULL になる条件 | 現在の対処 |
|--------|----------------|------------|
| `distance`, `surface` | JGプレースホルダーが RA で上書きされなかった場合に `distance=0`, `surface=''` | **フィルタリングなし → モデル汚染リスク** |
| `sire`, `dam`, `dam_sire` | horses テーブル未登録 | `sire_encoded = -1` フォールバック済み |
| `jockey`, `trainer` 関連 | racehorses/jockeys/trainers マスタ未投入 | `code_encoded = 0` 一律（精度低下のみ） |
| 調教特徴量 (`tc.*`, `hc.*`) | WOOD データ未取得 / horse_id 形式不一致 | `fillna(-1)` 処理済み |

**特に重要**: `distance = 0` のレコードが特徴量として流れ込むとモデルを汚染する。`models.py` の学習前フィルタリングで `distance > 0` を追加すべき。

---

### 1-7. combination_json = NULL 旧予想の的中誤記録

**ファイル**: `src/evaluation/evaluator.py:541-559`

旧予想（`combination_json = NULL`）は馬名ベースで馬番を逆引きし的中判定する。この逆引きが失敗すると `is_hit=True / payout=0` が DB に書き込まれ、ROI が 0% として記録される。ログにエラーは出るが DB 修正は行われない。

---

### 1-8. cat='3' 削除レコードのマスタ処理漏れ

**ファイル**: `jravan_client.py:784-786`

削除レコード(`cat='3'`)のスキップは `RA`, `SE`, 払戻のみ対象。`JG`, `WC`, `WH`, `BT`, `HN`, `UM`, `KS`, `CH` マスタレコードは削除チェック対象外で、誤って通常レコードと同様に処理される可能性がある。

---

## 2. ビジネスロジックの正確性（Business Logic）

### 2-1. **[P0 — 致命的] ROI・払戻の n_tickets 倍過大計算**

**ファイル**: `src/evaluation/evaluator.py:561`

```python
# 現在（誤）
actual_payout = (payout_per_100 / 100.0) * invested
# invested = n_tickets × 100円 のため、複数点買いで n 倍過大になる

# 正しい
actual_payout = payout_per_100  # 1点あたり100円賭けた際の払戻額
```

`payout_per_100` は「100円賭けた際の払戻額」を意味するため、1点的中払戻は `payout_per_100` 円そのものである。しかし現在の実装は `× invested（= n_tickets × 100）` を掛けてしまっており、**多点買いで n 倍に過大計算**される。

**実測影響**:
- 馬連5点フォーメーションで1点的中 payout_per_100=1150円 → 実装: 5750円、正解: 1150円
- 記録済み払戻合計: **約5.3倍過大**（推定 ROI 127.4% → 実際 ~26.7%）
- `HitFocus` の roi=1134% は三連単12点買いの誤計算が主因

**修正**: `evaluator.py:561` の `* invested` を `* 100.0` に変更（または `payout_per_100` をそのまま使用）。**修正後は `prediction_results` の再計算も必要。**

---

### 2-2. **[P0 — 致命的] 複数combo的中時に max 使用（sum が正しい）**

**ファイル**: `src/evaluation/evaluator.py:532-539`

```python
# 現在（誤）
payout_per_100 = max(payout_per_100, p)

# 正しい（複数点的中の払戻は合算）
payout_per_100 += p
```

三連複などで複数 combo が同時的中した場合（例: ボックスで2通り的中）、最高払戻のみを採用しており払戻合計を過少計上している。正しくは各的中comboの払戻を合算すべき。

---

### 2-3. **[P1] 返還チェックが馬名ベース（CLAUDE.md最高優先ルール違反）**

**ファイル**: `src/evaluation/evaluator.py:506-524`

```python
horse_names = [h[0] for h in horses]  # 馬名（文字列）を取得
refund = _has_refund(horse_names, horse_numbers, refund_numbers)
```

CLAUDE.md の最高優先事項「**的中判定・結果突合で馬名（文字列）比較は禁止。必ずレースID + 馬番（整数）の組み合わせのみ使用**」に違反。馬名の表記ゆれ（全半角・空白差異）が生じると返還が正しく検出されない。

**修正方針**: `combination_json` から馬番を直接取得して `refund_numbers` と照合する整数ベース処理に変更。

---

### 2-4. n_tickets 計算式の正確性確認

各券種の点数計算は `len(combination_json)` から正しく導出されており、BOX/フォーメーション選択ロジック自体の計算誤りは確認されていない。

**ただし [P2] 馬単BOXラベル誤識別**:

**ファイル**: `web/generate_data.py:150-159`, `web/src/lib/dbHelpers.ts:94-99`

馬連・ワイドの判定式 `n*(n-1)/2` を馬単にも流用しているため、4頭馬単BOX（12点）が「フォーメーション」と表示される。金額計算への影響はなく UI 表示のみの誤り。両ファイルで同じバグが共存。

---

### 2-5. エッジケース対応状況

| ケース | 状態 | 備考 |
|--------|------|------|
| 競走中止 (rank IS NULL / 0) | ✅ 対応済み | L229 `if rank and rank > 0` で除外 |
| 同着1着 馬連・三連複 | ✅ 対応済み | L271-280, L298-303 で分岐 |
| 同着2着 三連単 | ⚠️ 限定対応 | 払戻なしの場合に不確実 |
| 返還 (bet_type='返還') | ✅ 対応済み | `payout=invested, profit=0.0` |
| 同着払戻分割 | ✅ 不要 | JRA規定上分割なし |

---

## 3. コードベースとアーキテクチャ（Code Quality）

### 3-1. **[P1 — CLAUDE.md ルール違反] netkeiba.py が存続・import 継続中**

**ファイル**: `src/ops/data_sync.py:373, 403, 491`

CLAUDE.md 最高優先事項「**netkeiba.com へのアクセスは一切禁止**」に違反して、`src/scraper/netkeiba.py` が存在し、`data_sync.py` から実際に import されて `sync_results_from_netkeiba()` 関数として実行可能な状態にある。

```python
from src.scraper.netkeiba import fetch_race_results, fetch_race_payouts
```

「緊急フォールバック」として意図的に残されていると見られるが、誤って呼び出されるリスクを排除するため、少なくとも関数の先頭で `raise NotImplementedError("netkeiba access is prohibited")` を追加すべき。

---

### 3-2. **[P2] DDL 二重管理（最大の技術的負債）**

**ファイル**: `jravan_client.py:1805-1964`, `schema.py`

| テーブル | schema.py | jravan_client.py |
|----------|-----------|-----------------|
| `training_times` | ✓ | `_TRAINING_DDL` に重複 |
| `training_hillwork` | ✓ | `_TRAINING_DDL` に重複 |
| `breeding_horses` | ✓ | `_MASTER_DDL` に重複 |
| `foals` | ✓ | `_MASTER_DDL` に重複 |
| `racehorses` | ✓ | `_MASTER_DDL` に重複 |
| `jockeys` | ✓ | `_MASTER_DDL` に重複 |
| `trainers` | ✓ | `_MASTER_DDL` に重複 |

`extend_db_schema()` (`jravan_client.py:1966`) が `init_db()` を経由せず独自にテーブルを作成。スキーマ変更時に片方だけ更新して不整合が生じるリスク大。

**修正方針**: `extend_db_schema()` を廃止し、`init_db()` の `DDL_STATEMENTS` に一元化。

---

### 3-3. **[P2] training_evaluations テーブル未定義**

**ファイル**: `web/generate_data.py:170-190`

`_fetch_training_evals()` が `training_evaluations` テーブルを参照しているが、`schema.py` の `DDL_STATEMENTS` にこのテーブルの DDL が存在しない。DB 未作成のためこの関数は常に空を返す（デッドコード状態）。

---

### 3-4. **[P2] generate_data.py の N+1 クエリ**

**ファイル**: `web/generate_data.py:537-546, 812-820`

- `export_predictions()`: 予想ループ内で `prediction_horses` を個別クエリ（予想件数 N 回 SELECT）
- `export_gachi_hits()`: race_id ごとに `horse_name_maps` を個別クエリ

API ルート (`/api/predictions/route.ts`) はバルク IN 句で回避済みだが、静的 JSON エクスポートパスのみ残存している。

---

### 3-5. **[P2] /api/races の SQLite IN句 上限リスク**

**ファイル**: `web/src/app/api/races/route.ts:29`

```typescript
raceIds.map(() => '?').join(',')  // limit=500 時に最大 1000 変数
```

SQLite のバインド変数上限は **999**。`limit=500` で results + payouts = 最大 1000 変数になりうる。`/api/predictions` は `chunkArray(ids, 500)` で回避済みだが `/api/races` は未対応。

---

### 3-6. /api/races/[race_id] の N+1 クエリ

**ファイル**: `web/src/app/api/races/[race_id]/route.ts:100`

`predRows.map()` 内で `getHorses.all(race_id, race_id, pd.prediction_id)` を呼び出し。1レース内の予想数 N に対して N 回 SELECT が発生。

---

### 3-7. **[P3] jravan_client.py — 2295行の神クラス**

単一ファイルに COM クライアント・10種以上のバイナリパーサー・DB書き込みルーティン・高レベルローダー・DDL定義・CLI エントリーポイントが混在。単一責任原則の明確な違反。

**推奨分割**: `parsers.py`, `db_writer.py`, `jvlink_client.py`, `loader.py`

---

### 3-8. **[P3] 重複コード群**

| 問題 | ファイル |
|------|--------|
| `_identify_bet_form()` 二重実装 | `generate_data.py:98-165` / `dbHelpers.ts:64-102` |
| `formatCombinations()` 3コンポーネント重複 | `PredictionsPanel.tsx:59-86` / `HitHistory.tsx:36-60` / `GachiHits.tsx:39-68` |
| `BET_ORDER` 定数三重定義 | `generate_data.py:656`（モジュール） / `generate_data.py:316`（ローカル） / `dbHelpers.ts:6` |

---

### 3-9. **[P3] umanity_uploader.py の init_db() バイパス**

**ファイル**: `src/ops/umanity_uploader.py:77`

```python
conn = sqlite3.connect(str(db_path))  # init_db() を経由しない
```

WAL / cache_size / foreign_keys 等の PRAGMA が設定されない。読み取り専用クエリなので実害は小さいが、規約統一のため `init_db()` 経由に変更すべき。

---

### 3-10. v_race_mart パフォーマンスリスク

**ファイル**: `schema.py:569-588`

- 全 `race_results` 行に対して training_times・training_hillwork の **相関サブクエリ 2 本**が評価される
- `horse_id` フォーマット変換（文字列演算）がインデックス有効活用を制限
- jockeys/trainers の JOIN が馬名文字列マッチ（表記ゆれリスク・インデックスはあり）

AI 学習時の全件取得（`query_mart`）で顕著なスキャンコスト増加の可能性。

---

## 4. 修正優先度マトリクス

| 優先度 | # | 問題 | 修正箇所 | 工数目安 |
|--------|---|------|---------|---------|
| **P0** | 1 | ROI・払戻 n_tickets 倍過大計算 | `evaluator.py:561` `* invested` → `* 100.0` + `prediction_results` 再計算 | 1h |
| **P0** | 2 | 複数combo的中 max → sum | `evaluator.py:539` `max` → `+=` | 30min |
| **P1** | 3 | 複勝・ワイド 出走頭数依存バグ | `evaluator.py:32`, `models.py:212` に `n_horses <= 7` 分岐追加 | 2h |
| **P1** | 4 | netkeiba.py 誤呼び出しリスク | `data_sync.py` の呼び出し元に `raise NotImplementedError` | 30min |
| **P1** | 5 | 返還チェック 馬名→馬番ベースに | `evaluator.py:506-524` combination_json 馬番直接参照 | 2h |
| **P1** | 6 | WH コード名前衝突 | `_PAYOUT_SPECS` の `"WH"` エントリ削除または分岐順序修正 | 30min |
| **P2** | 7 | DDL 二重管理 | `extend_db_schema()` 廃止・`DDL_STATEMENTS` 一元化 | 3h |
| **P2** | 8 | 馬単BOX ラベル誤識別 | `generate_data.py:150-159`, `dbHelpers.ts:94-99` に馬単専用分岐 | 1h |
| **P2** | 9 | training_evaluations DDL 追加 または デッドコード削除 | `schema.py` / `generate_data.py:170-190` | 1h |
| **P2** | 10 | /api/races の IN句上限 | `route.ts:29` に `chunkArray` 適用 | 1h |
| **P3** | 11 | formatCombinations 共通化 | `web/src/lib/` に共通ヘルパー抽出 | 2h |
| **P3** | 12 | BET_ORDER 三重定義 解消 | `generate_data.py:316` ローカル変数削除 | 30min |
| **P3** | 13 | umanity_uploader init_db() バイパス | `sqlite3.connect` → `init_db()` | 30min |
| **P3** | 14 | jravan_client.py 分割 | parsers / db_writer / loader に分割 | 1d |

---

## 5. 知的誠実性に関する総括

本システムの「データの知的誠実さ」という観点では、以下の点を強調する必要がある:

1. **記録済みROIは信頼できない**: P0バグにより `prediction_results` テーブルの払戻額・ROIは全件過大計算されている。商用化の意思決定をこの数値に基づいて行ってはならない。

2. **バックテスト精度の信頼性**: 複勝の7頭以下ルール未対応・combination_json NULL の旧予想誤記録・WH名前衝突により、バックテスト数値に一定の歪みが生じている可能性がある。

3. **良好な点**: 馬番整数ベースの的中判定（CLAUDE.md 遵守）・DBトリガーによるrank汚染防止・払戻パーサーの正確性・同着・返還・競走中止の基本ケース対応は適切に実装されている。

---

*本レポートは 2026-05-05 時点のコードスナップショットに基づく。*
