# 設計書: ビジネス拡大フェーズ — UI刷新・Discord完全リアル化・ドキュメント永続化

**日付**: 2026-05-23  
**承認**: 社長（A案方式で承認済み・即時実行指示）  
**実装スコープ**: 5タスク / 7ファイル変更 + 3ファイル新規作成

---

## 概要

UMALOGIのビジネス拡大（NOTE販売・一般開放・地方競馬進出）を見据えた大規模改修。
コアとなる変更は3系統: UIのレース詳細画面4タブ化、Discord通知の完全リアル化、
運用・ビジネスドキュメントの永続化。

---

## タスク1: UI — RaceDetail 4サブタブ化 + SNSコピーボタン

### 背景

現行の `RaceDetail.tsx` は3タブ構成:
- `prerace` (AI直前分析) — BiasPanel + PreraceTable (prerace データがある場合のみ)
- `results` (レース結果) — ResultsTable + PayoutCards
- `predictions` (AI予想) — PredictionsPanel

### 確定設計 (A案)

タブを以下の4枠に再編成する。常に4タブ全表示。データなし時は empty state 表示。

| # | Key | ラベル | コンテンツ |
|---|-----|--------|----------|
| 1 | `race_card` | 出馬表 | 馬番順ソートの出走馬テーブル (horse_number/gate/name/sex_age/weight/jockey/trainer/win_odds/popularity) |
| 2 | `results` | レース結果 | 既存 ResultsTable のみ（PayoutCards を切り離す）|
| 3 | `predictions` | AI予想 | BiasPanel (prerace 存在時) + PreraceTable (prerace 存在時) + PredictionsPanel (predictions 存在時) + SNSコピーボタン |
| 4 | `payouts` | 的中結果 | 既存 PayoutCards セクション |

**デフォルトタブ**: 常に `race_card` (出馬表)

### 出馬表コンポーネント設計

`race.results` を `horse_number` 昇順ソートして表示。

表示列: 枠番 / 馬番 / 馬名 / 性齢 / 斤量 / 騎手 / 厩舎 / 単勝オッズ / 人気

レース前 (rank=null の馬が多い場合) は rank 列を非表示。
既存の `GateBadge` / `OddsCell` サブコンポーネントを再利用。

### SNSコピーボタン設計

AI予想タブの最上部に固定配置。クリックで以下のテキストをクリップボードにコピー。

```
📊 UMALOGI AI予想 | {date} {venue}{race_number}R

🔥【激アツ推奨馬 — EV≥1.0】
  {ev_recommend 各馬をEV順に最大5頭}

【AI買い目 (EV≥1.0 のみ)】
  {predictions を model_type/bet_type/n_tickets でサマリー}

🆓 1レース目は無料公開中
📲 フォロー＆リポストで最新AI予想をチェック
#競馬予想 #AI競馬 #UMALOGI
```

コピー成功後: ボタン表示を「✅ コピーしました!」に1.5秒変更 → 元に戻す。

---

## タスク2: Discord — 的中速報チャンネル分離 + 通知完全リアル化

### 環境変数追加

```
DISCORD_WEBHOOK_HIT_FLASH   : 的中速報専用チャンネル（未設定時 = DISCORD_WEBHOOK_URL へ fallback）
```

### 変更1: `notify_hit_summary()` のチャンネル分離

`DiscordNotifier.__init__()` に `hit_flash_url: str | None = None` パラメータ追加。
環境変数 `DISCORD_WEBHOOK_HIT_FLASH` を読み込み、`self._hit_flash_url` として保持。

`notify_hit_summary()` 内の送信先を `self._url` → `self._hit_flash_url or self._url` に変更。

### 変更2: `notify_prerace_result()` 購入単価×点数明記

各 bet の field_name に購入コスト行を追加:

現行:
```
🔥 三連複  EV=2.13  ¥800
```

改修後:
```
🔥 三連複  EV=2.13 | ¥100×4点=¥400
```

実装:
```python
n_combos = len(getattr(bet, "combinations", []) or [])
unit_price = 100
cost_str = f"¥{unit_price}×{n_combos}点=¥{unit_price * n_combos:,}" if n_combos > 0 else ""
field_name = f"{fire} {bet_type}  EV={ev:.2f} | {cost_str}"
```

### 変更3: `_format_combo_card()` 馬番全表示 + 軸スマート表記

**修正点**:
1. 馬単/三連単: `combos[:4]` の上限を撤廃。文字数が 900 超の場合のみ末尾で打ち切り。
2. 軸流し (axis_set あり): 相手馬 `others[:6]` の上限を撤廃。全員列挙。
3. ボックス: `nums_all[:6]` の上限を撤廃。
4. 軸表記に推奨買い目プレフィックスを追加:
   ```
   【推奨: 馬連流し 軸5 - 相手3,7,9】
   ▶ 軸: 5番 アーバン
     相手: 3番 / 7番 / 9番
     計3点
   ```

**Discord 文字数上限**: field.value = 1024文字。長い場合は末尾 `…(全N点)` で収める。

---

## タスク3: 明日の暫定予想 緊急調査・生成

### 調査フロー

1. DB確認: `SELECT COUNT(*), model_type FROM predictions WHERE date='2026-05-24' GROUP BY model_type`
2. レース存在確認: `SELECT COUNT(*) FROM races WHERE date='2026-05-24'`
3. 予想がゼロ かつ レースが存在 → prediction pipeline を緊急実行
4. UIのJSONデータファイルを再生成

### 実行コマンド (予想ゼロの場合)

```bash
py -3 -m src.pipeline.prediction --date 2026-05-24 --mode provisional
```

---

## タスク4: docs/automation_schedule.md 新規作成

scheduler.py のコメント・ソースから完全なスケジュール表を作成。
以下のメタ指示を末尾に記載:

> 【Claude Codeへの絶対指示】今後スケジュールや自動化スクリプトを改修する際は、
> 必ずこの `docs/automation_schedule.md` も同時に書き換え、
> コードと仕様書の整合性を100%保った状態でコミットすること。

---

## タスク5: docs/roadmap.md 新規作成

ビジネスロードマップ (短期/中期/長期) + 進捗トラッキング表。

短期:
- RaceDetail 4タブ化 + SNSコピー
- Discord 完全リアル化
- NOTE販売半自動化 UI

中期:
- NOTE販売フル自動化 (Selenium 下書き)
- X連動テキスト生成
- 地方競馬データ対応 (JRA共通ラッパー)

長期:
- 一般開放設計 (FastAPI + Next.js public API)
- 地方競馬版AI 創設

---

## 変更ファイル一覧

| ファイル | 種別 | タスク |
|---------|------|--------|
| `web/src/components/RaceDetail.tsx` | 変更 | タスク1 |
| `src/notification/discord_notifier.py` | 変更 | タスク2 |
| `src/notification/router.py` | 変更 (hit_flash対応) | タスク2 |
| `docs/automation_schedule.md` | 新規 | タスク4 |
| `docs/roadmap.md` | 新規 | タスク5 |
| `docs/superpowers/specs/2026-05-23-business-expansion-design.md` | 新規 | この文書 |

---

## 非機能要件

- TypeScript: strict型チェック適合
- Python: PEP8 / 型ヒント完備
- Discord: 全 embed field ≤ 1024文字 / embed 全体 ≤ 6000文字
- SNSコピー: `navigator.clipboard.writeText` (HTTPS 必須) + fallback なし (開発環境はHTTP注意)
