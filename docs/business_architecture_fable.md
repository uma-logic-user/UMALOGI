# UMA-Logic ビジネスアーキテクチャ設計書（business_architecture_fable.md）

**作成**: 2026-06-11 / Claude (claude-fable-5)
**目的**: UMA-Logic を「自動で稼ぎ続けるビジネスシステム」へ昇華させるための
①収益化ロジック精査 ②完全自動運用 ③SNS集客自動化 ④サブスク導線 の統合設計と実装記録。

## 更新履歴

| 日付 | 変更内容 |
|------|---------|
| 2026-06-11 | 初版作成。EVロジック精査・bankroll_manager/sns_generator 新規実装・自動運用設定の出力。影響ファイル: src/ml/bankroll_manager.py, src/marketing/sns_generator.py, config/automation_daily.yaml, scripts/bat/register_daily_tasks.ps1 |

---

## 領域1: 収益プラス化ロジックの精査（金融工学）

### 1.1 現行 EV 算出パイプラインの構造

```
モデルスコア(LightGBM ev_score)
  → Isotonic/Platt 較正 (manji_calibration.calibrate_win_prob)
  → 市場アンカー型ブレンド P_final = w(odds)·P_model + (1−w)·P_market  [v1.7.2]
  → EV = P_final × odds
  → ゲート (TANSHO_EV_MIN=1.05 / TANSHO_ODDS_CEIL=30 / prob_floor=0.06)
  → 1/10 Kelly ステーク (pure_ev_edge.kelly_stake)
```

### 1.2 検出した「数学的・統計的な穴」と対処状況

| # | 穴 | 深刻度 | 状態 |
|---|-----|--------|------|
| H1 | **較正器のオッズ非依存**: ev_score→P が odds を見ず大穴の EV が暴騰（実測 EV=32.5） | 致命 | 🟢 解消済（v1.7.2 市場ブレンド・OOS で単勝ROI 92.7→111.1%・ECE 0.1629→0.0182） |
| H2 | **ブレンド閾値の in-sample 決定**: BLEND_PIVOT=10 等が検証 CSV と同一データで決定 | 高 | 🟡 残課題。本番昇格前に Champion/Challenger OOS 必須（MAINTENANCE_LOG 2026-06-10 注記） |
| H3 | **同時ベットの合成 Kelly 無視**: 1点2%上限でも同日 N 点で Σf が安全域を突破 | 高 | 🟢 **本セッションで解消**（bankroll_manager.allocate_stakes が合計10%超を比例縮約） |
| H4 | **静的バンクロール**: initial_bankroll=10万円固定で複利 Kelly の前提が崩れている | 中 | 🟢 **本セッションで解消**（effective_bankroll が確定損益から現在資金とピークを動的算出） |
| H5 | **破産確率の未定量化**: 「1/10 Kelly だから安全」が感覚論 | 中 | 🟢 **本セッションで解消**（estimate_ruin_probability: ベクトル化モンテカルロ・recommend_kelly_fraction: P(破産)≤目標 の最大分数探索） |
| H6 | **ドローダウン時の無減速**: 連敗中も同率で張り続け破産確率の裾が太い | 中 | 🟢 **本セッションで解消**（drawdown_throttle: -10%で半減/-20%で1/4/-30%で全停止） |
| H7 | **複勝オッズの粗い推定**: `1+(win_odds−1)×0.33` の線形近似に系統誤差 | 中 | 🔴 未着手。JVLink リアルタイム複勝オッズ（realtime_odds 拡張）取得が根治策 |
| H8 | **高 ROI 単勝の分散リスク**: OOS 単勝617%は少数大穴依存＝シャープレシオ劣悪 | 高 | 🟡 認識済・実弾化拒否を継続（[[feedback_ev_precision_safety_first]]）。安定黒字は複勝系102-110%＋聖域{京都,阪神} |
| H9 | **デッドフィーチャー 0 埋め**: x_consensus_score が収集 0 件のまま学習に混入 | 低 | 🟡 W-065（配線済・実収集は次開催実証待ち） |
| H10 | **オッズ取得タイミング乖離**: 直前オッズで EV 判定→確定オッズで払戻の系統ズレ | 中 | 🔴 未着手。卍単勝 OOS<100% の真因候補。オッズ時系列（record_odds_timeseries）での乖離定量化が次手 |

### 1.3 絶対プラス収支への戦略（結論）

JRA の控除率（単勝20%/複勝20%/三連系25%超）の下で長期プラスにする唯一の経路は
**「較正精度で市場の歪みだけを抽出し、破産しないサイズで張り続ける」**こと。

1. **エッジの源泉**: 較正済み確率と市場確率の乖離（ECE 0.018 を維持・劣化したら自動再較正）。
2. **券種の選択**: 実証済みエッジは複勝系（OOS 102-110%）＋聖域会場。高分散単勝は WATCH_ONLY。
3. **サイズの規律**: 1/10 Kelly × 1点2% × 日次10% × ドローダウンスロットル × サーキットブレーカーの5重防御。
4. **検証の規律**: いかなる係数・閾値も OOS 検証なしに本番へ入れない（W-071 の教訓を恒久ルール化）。

### 1.4 新規実装: `src/ml/bankroll_manager.py`

| API | 機能 |
|---|---|
| `full_kelly(p, odds)` | フル Kelly 比率 f*（既存 kelly_stake と数学的同一） |
| `effective_bankroll(conn)` | 確定損益を日次累積した (現在資金, ピーク資金) |
| `drawdown_throttle(cur, peak)` | ドローダウン帯域別の Kelly 減速係数 |
| `allocate_stakes(candidates, bankroll, peak)` | 同時ベット群の一括配分（縮約・丸め・キャップ） |
| `estimate_ruin_probability(p, odds, f)` | モンテカルロ破産確率（5,000パス・対数資産・seed固定可） |
| `recommend_kelly_fraction(p, odds, target_ruin)` | P(破産)≤目標を満たす最大 Kelly 分数 |

**結線ポリシー**: 本番ベットフロー（prediction.py）への結線は
**バックテストで「現行 1/10 Kelly 固定」対「動的バンクロール＋スロットル」の OOS 比較**を行い、
最大ドローダウン・破産確率・最終資産の3指標で優位を確認してから（W-078 として台帳管理）。
未検証ロジックの実弾直結は W-071 事故の再演となるため行わない。

---

## 領域2: 完全自動運用基盤（DevOps）

### 2.1 結論: 3層アーキテクチャ（JVLink 制約により GitHub Actions hosted は不可）

JVLink COM は 32bit Windows ローカル限定のため、クラウド実行は構造的に不可能。
既に本番稼働中の資産を正とし、欠けていた宣言的設定と登録自動化を今回追加した。

```
層1【主経路】 ローカル常駐: today_auto_runner --continuous + watchdog + Streamlit
              └ scripts/bat/start_umalogi.bat（二重起動ガード付き）
層2【補助】   Windows タスクスケジューラ
              ├ UMALOGI_BootStart      … PC起動時に層1を自動起動（無人復電対応）
              └ UMALOGI_DailyMarketing … 毎日21:00 SNSアセット生成（新設）
              └ 登録: scripts/bat/register_daily_tasks.ps1（新規・1回実行）
層3【CI/予備】GitHub Actions self-hosted runner
              ├ ci.yml          … push 毎の pytest
              └ umalogi_auto.yml … cron バッチ（オートパイロット障害時の手動フォールバック）
```

- 宣言的 SSoT: **`config/automation_daily.yaml`**（新規）。日次フロー全体・常駐プロセス・
  バンクロール/マーケ設定を一元記述。
- ⚠️ 排他則: `scheduler.py` と `today_auto_runner --continuous` の同時常駐は厳禁（既存ルール踏襲）。
- 既知ギャップ（台帳管理継続）: watchdog はJVLinkのみ再起動しオートパイロット自体の self-heal は無い
  （2026-06-06 障害の真因）。層2の BootStart 登録で「再起動すれば復旧」までは自動化された。

### 2.2 日次フロー（人間介入ゼロ）

| 時刻 | ステップ | 実行主体 |
|---|---|---|
| 金 20:00 | JVLink 同期→暫定予想→Discord | オートパイロット |
| 土日 発走前 | 直前オッズ→EV 再計算→買い目確定→配信 | オートパイロット |
| 土日 確定後 | 払戻同期→的中判定→Hit Flash | オートパイロット |
| 毎日 21:00 | SNS 無料予想・実績・動画台本生成 | タスクスケジューラ（新設） |
| 日 夜 | 週次 P&L レポート | オートパイロット |
| 毎日 04:00 | DB VACUUM/ANALYZE/バックアップ | 深夜保守ジョブ |

---

## 領域3: SNS 運用・集客の自動化

### 3.1 新規実装: `src/marketing/sns_generator.py`

**「盾と矛」戦略**: 無料公開は的中率の高い本命予想のみ（盾＝信頼獲得）。
収益の源泉である高 EV 穴馬・買い目・推奨ベット額は有料側に温存（矛＝課金価値）。

| 生成物 | 内容 | 出力先 |
|---|---|---|
| `free_pick.md` | 勝率特化（本命系）上位3レースの無料予想ポスト | outputs/marketing/YYYYMMDD/ |
| `hit_report.md` | 前日確定実績（真コスト基準 ROI・最高払戻）。**負けた日も誠実に公開**（長期信頼=LTV） | 同上 |
| `video_script.md` | CapCut 向けシーン分割台本（映像指示/ナレーション/テロップ） | 同上 |

- 実績集計は `pnl_accounting` と同一の真コスト式（実コスト = payout − profit）。捏造・盛りは構造的に不可能。
- CLI: `py -m src.marketing.sns_generator --date YYYYMMDD`（タスクスケジューラから毎日21:00起動）。
- 既存 `scripts/generate_sns_post.py`（レース単位・note誘導）とは併存（日次バッチ vs 単発）。
- **自動投稿は当面オフ**（`auto_post: false`）。X API 規約・凍結リスクの確認後に解禁判断。
  生成物は人間がコピペ投稿（1日1分の作業のみ残る）。

---

## 領域4: サブスクリプション販売への導線設計

### 4.1 ファネル構造

```
X/動画（無料予想＋実績証明）
  → 信頼の蓄積（毎日・自動・誠実な収支公開）
  → キラーフレーズで「期待値投資」へ認知転換
  → note/Discord 有料購読（高EV穴馬・買い目・推奨ベット額・全収支）
```

### 4.2 キラーフレーズ設計（`KILLER_PHRASES`・6種日替わり）

訴求の論理構造を「当てる自慢」から**「期待値で勝ち続ける仕組みの販売」**へ統一:

1. 認知転換型 — 「当たる馬ではなくオッズが実力より高い馬を買う＝期待値」
2. 大数の法則型 — 「1レースの的中は運。100レースの収支は数学」
3. 投資アナロジー型 — 「株のプロと同じことを競馬で。EV>1 だけが投資」
4. リスク管理型 — 「ケリー基準でベット額まで自動算出。1日の負けで退場しない」
5. 限定性型 — 「無料は当てやすい予想。利益の源泉=高EV穴馬は購読者限定」
6. データ権威型 — 「JRA公式データ×機械学習で市場の歪みを狙い撃ち」

- すべての自動生成文（無料予想・実績報告・動画台本 CTA シーン）に日付決定的ローテーションで自動挿入。
- 負け公開時もフレーズ4で「設計された安全性」へ転換し解約・不信を抑制。

### 4.3 残課題（次フェーズ）

- 購読者限定配信の技術実装（Discord ロール別チャンネル or note メンバーシップ連携）は
  既存 NotificationRouter / IS_PREMIUM_NOTE 基盤の拡張で対応（別タスク）。
- 動画の完全自動レンダリング（台本→音声合成→CapCut テンプレ）は台本フォーマット安定後に検討。

---

## 実装サマリ（2026-06-11）

| 種別 | ファイル | テスト |
|---|---|---|
| 新規 | src/ml/bankroll_manager.py | tests/test_bankroll_manager.py（25件） |
| 新規 | src/marketing/sns_generator.py（+ __init__.py） | tests/test_sns_generator.py（15件） |
| 新規 | config/automation_daily.yaml | — |
| 新規 | scripts/bat/register_daily_tasks.ps1 | — |
| 文書 | docs/business_architecture_fable.md（本書） | — |
