# SNS用 / 個人ガチ用 モデル完全分離・UI実装 設計書

- 日付: 2026-06-14（日・ライブ運用中／社長特例指令で条項2週末凍結を上書き）
- バージョン: v1.15.1-dev
- 関連弱点: W-091（5月末ポリシー並列シャドー）の本番ライブ配線

## 背景と目的

W-091 で復活させた多券種出力モデル（`{base}_v0525`）は **SNS・マーケティング専用（Hit-Focused）**、
現行の単複ロック高EVモデルは **個人実弾投資用（EV-Focused）** と位置づける。

- 仮想ROIは多券種=赤字（7.4%）であることは実証済み。実弾の単複ロックは**一切変更しない**。
- SNS集客のため「派手な多券種的中シグナル」を公開し続ける運用を、実弾トラックと**完全分離**して両立する。

## 絶対遵守する安全制約

1. **実弾EVパス（`_run_prerace` V1 / Pure_EV_Edge / FukushoElite / 単複ロック）を1行も変更しない。**
2. **ライブ稼働中オートパイロット（PID 16204）を再起動しない。**
   - `_run_prerace` は毎回 `py -m src.main_pipeline prerace` を**新規サブプロセスで spawn** する。
   - よって `src/` のコード変更はサブプロセス側で**即時反映**され、常駐側の再起動は不要。
3. **条項1（予測不変性）:** シャドーは別 `model_type`（`{base}_v0525(再計算)`）。既存 live 予想を構造上 UPDATE しない。
4. シャドー生成は **best-effort**（try/except 完全隔離）。失敗しても実弾予想・通知を絶対に止めない。

## タスク1: モデル分離とライブ配線（DB・パイプライン）

統合点: `src/pipeline/prediction.py:_prerace_pipeline_inner`。
ライブ保存（`_save_predictions`）**直後**に、すでに計算済みの `df / honmei_scores / ev_scores` を
**再利用**して `generate_shadow_bets()` → `save_shadow_bets()` を呼ぶ。

- 新ヘルパー `_maybe_save_shadow(conn, race_id, df, honmei_scores, ev_scores)` を新設。
  - env `SHADOW_SNS_ENABLE`（既定 `1`）で制御。`0` で完全無効化（緊急停止弁）。
  - 全体を try/except で包み、例外は warning ログのみ（再送出しない）。
  - 特徴量の二重生成なし＝**netkeiba 等スクレイピング処理を一切追加しない**（タスク3対策）。
- provisional / 直前 双方で実行。`save_shadow_bets` は INSERT OR REPLACE で自身の前回行のみ更新するため、
  直前が暫定を上書きする（二重計上なし）。

## タスク2: WEB UI（ポート3000）の判別表示

トラック分類は**サーバー側を単一真実源**とする（ハイブリッド・ルール）。

```
classifyTrack(model_type, bet_type):
  model_type に "_v0525" を含む          -> 'sns'   (📱 シャドー多券種)
  bet_type in {単勝, 複勝}               -> 'gachi' (💰 実弾EV単複)
  それ以外(馬連/馬単/ワイド/三連複/三連単) -> 'sns'   (📱 派手な多券種)
```

- `web/src/lib/dbHelpers.ts` に `classifyTrack()` を追加（jest テスト付き）。
- `/api/predictions/route.ts` の出力各行に `track: 'sns' | 'gachi'` を付与。
- `web/src/types/race.ts:Prediction` に `track?: 'sns' | 'gachi'` を追加。
- `PredictionsPanel.tsx` にトラックタブ（全部 / 💰 ガチ / 📱 SNS）＋各行バッジ（📱 SNS / 💰 ガチ）。
  - デスクトップ表: モデル列にバッジ。モバイル: カードヘッダにバッジ。
  - タブで items を `track` フィルタ。締切直前に一瞬で判別できる。

## タスク3: 負荷・タイムアウト対策

- シャドー生成は **DB 内データのみ**（FeatureBuilder + 学習済みモデルの in-memory 計算）。
  netkeiba 等の外部スクレイピングも追加 DB 書き込み負荷も最小（既存スコア再利用）。
- 既存 prerace サブプロセスの 300s タイムアウト内に収まる（買い目生成はミリ秒オーダー）。
- best-effort 隔離により、万一遅延・失敗してもライブ予想本体は無影響。

## 完了条件

- pytest 全 PASS（新規 Python テスト含む）／ jest 全 PASS（新規 classifyTrack テスト）。
- `ruff format` / `ruff check`（変更ファイルのみ）クリーン。
- 本日の直前予想（live）で SNS 多券種と実弾単複が分離表示される。
- `v1.15.1-dev` として master へコミット＆Push（条項6の3点セット）。

## ロールバック

- env `SHADOW_SNS_ENABLE=0` で即座にシャドー生成を停止（コード戻し不要）。
- UI は表示のみの加算変更。リバートしても実弾・通知に影響なし。
