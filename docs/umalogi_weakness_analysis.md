# UMALOGI — システム総合弱点分析 & 次世代開発ロードマップ

> 作成日: 2026-05-04  
> 対象ブランチ: master  
> 分析者: Claude Sonnet 4.6（自己診断モード）

---

## エグゼクティブサマリー

UMALOGIは JRA-VAN 統合・LightGBM 予測・自動再学習・SNS通知・Next.js ダッシュボードを
備えた商用水準のアーキテクチャを持つ。しかし「世界最高峰の競馬AI」を目指すには、
以下4領域に本質的な欠陥が存在する。

| 領域 | 深刻度 | 主な問題 |
|---|---|---|
| **予測・投資ロジック** | 🔴 Critical | GroupKFold 時系列リーク / Kelly 未実連動 |
| **インフラ堅牢性** | 🔴 Critical | ローカルPC SPOF / 監視ゼロ |
| **商用化・UI/UX** | 🟡 High | 認証なし API 公開 / SHAP 不可視 |
| **技術的負債** | 🟡 High | netkeiba 依存残存 / テスト不明 |

---

## 1. 予測・投資ロジックの弱点

### 1-1. 過学習・データリークリスク

#### 【Critical】GroupKFold の時系列リーク

```python
# 現状: models.py
gkf = GroupKFold(n_splits=5)
for train_idx, val_idx in gkf.split(X, y, groups=df["race_id"]):
    ...
```

`GroupKFold(groups=race_id)` は「同一レースの馬が train/val に混入しない」ことは保証するが、
**「将来のレースが訓練データに混入しない」ことは一切保証しない**。

実際に起こりうる状況:
- fold-1 の train に 2025年1月のレース、val に 2024年1月のレースが混入
- モデルが「未来の情報を学習した状態」でバックテストを行い、CV AUC が過大評価される
- 結果として本番デプロイ後に性能が急落する「バックテスト詐欺」状態に陥る可能性

**正しいアプローチ**: 日付で厳密に分割する `TimeSeriesSplit` または
カットオフ日による手動分割（例: 直近6ヶ月を val に固定）。

#### 【High】当日バイアス特徴量の前方リーク疑惑

```python
# features.py の当日バイアス特徴量
"today_inner_bias", "today_front_bias", "today_race_count", "today_gate_match"
```

これらは「当日の既終了レースの結果」から算出される。バックテスト再現時に
計算順序が保証されていない場合、同日後半レースのデータが前半レースの訓練に
混入する前方リークが発生する。`simulate_year.py` での計算順序を厳密に検証すること。

#### 【High】LabelEncoder の未見ラベル問題

メモリ記録: 「jockeys/trainers マスタが空のため名前ベース LabelEncode に切替」

- 訓練時に見ていない騎手・調教師（新人・外国人騎手）が本番で出現した場合の処理が不明
- `sklearn.LabelEncoder` は未見ラベルで `ValueError` を送出する
- エンコーダを `data/models/` に pkl 保存して使い回す設計が確認されているが、
  エンコーダの「世代管理」（モデル世代とエンコーダ世代の対応）が保証されているか不明

#### 【High】ALPHA モデルのデータ制約による過学習

メモリ記録: 「win_odds が 2024-01 のみ」、「現状 ROI=59%」

- 1ヶ月（約400〜600レース）のデータでの LightGBM 学習は深刻な過学習リスク
- モデルが 2024年1月の特定の馬場・騎手・季節特性をそのまま記憶している可能性が高い
- ROI=59%（元本割れ）はこのデータ制約が主因と推測される
- ALPHA モデルの実運用は、win_odds データが少なくとも2年分揃うまで保留が妥当

#### 【Medium】Platt Scaling のキャリブレーション品質の無監視

確率キャリブレーション（Platt/Isotonic）は実装済みだが:
- Reliability Diagram（信頼性図）の定期生成がない
- モデル更新後のキャリブレーション劣化を自動検知する仕組みがない
- キャリブレーション不良は Kelly 計算の前提を崩し、過剰/過少投資を招く

---

### 1-2. 資金管理（バンクロールマネジメント）の実連動欠如

#### 【Critical】Kelly 計算がハードコードのバンクロールに依存

```python
# bet_generator.py
@dataclass
class BetConfig:
    bankroll: float = 100_000.0  # ← 固定値。実際のP&Lと無連動
    max_bet_fraction: float = 0.05
    max_bet_per_combo: float = 1_000.0
```

Kelly基準の本質は「資産に応じて賭け金を動的に調整すること」にある。
現状は `prediction_results` テーブルに収支実績が蓄積されているにも関わらず、
その合計損益が `BetConfig.bankroll` に反映されていない。
これにより:

- **連勝時**: 増えた資産に対して賭け金が増えず、複利効果を取り逃がす（機会損失）
- **連敗時**: 減った資産に対して賭け金が減らず、破産リスクが過大になる
- Kelly の「ドローダウン最小化」という最大のメリットが無効化される

**改善案**:

```python
# prediction.py でバンクロールを動的に計算する
def get_current_bankroll(conn: sqlite3.Connection, initial: float = 100_000.0) -> float:
    row = conn.execute(
        "SELECT COALESCE(SUM(payout - bet_amount), 0) FROM prediction_results"
    ).fetchone()
    return initial + (row[0] or 0.0)
```

#### 【Medium】フラットベットがデファクトのデフォルト

`_BASE_BET = 100` が基準として残存。EV > 1.0 の条件を満たしても、
Kelly による可変賭け金が適用されているのは `BetConfig` が明示的に渡された場合のみ。
パイプライン（`prediction.py`）で `BetConfig` がどう渡されているか確認し、
Kelly ベットがデフォルトになるよう変更が必要。

#### 【Medium】EV 計算の信頼性問題

```python
# OddsEstimator: 過去実績の中央値でスケールを推定
scale = median(payout / 100 / win_odds)  # 券種別
EV = harville_prob × win_odds × scale
```

この推定には以下の問題がある:
- スケールが安定するのに必要なサンプル数（MIN_SAMPLES=50）が少ない
- 馬場状態・距離・クラスによってスケールが異なるが、全体を一律集計している
- 「EV 幻覚」防止のキャップ（_EV_MAX）はあるが、その値が経験則で設定されており根拠が弱い

---

## 2. インフラ・システム堅牢性の課題

### 2-1. ローカルPC・Windows スケジューラへの全依存

#### 【Critical】SPOF（シングルポイントオブフェイラー）

```
Windows PC 1台
    └── scheduler.py（scheduler ライブラリ）
            ├── 32bit Python（JVLink COM）
            └── 64bit Python（ML・Web）
```

**リスクシナリオ**:
| 事象 | 影響 | 復旧手段 |
|---|---|---|
| Windows Update 自動再起動 | スケジューラ停止・土日のレースを丸ごと飛ばす | 手動再起動のみ |
| HDD/SSD 故障 | umalogi.db 全消失（バックアップがローカルのみ） | 復旧不能 |
| PC 電源断（停電・過負荷） | 稼働中 WAL の破損リスク | WAL 復旧で対応可能だが不確か |
| `schedule` ライブラリのクラッシュ | プロセス消滅・無音障害 | 気付けない |

**なぜ見えないか**: 監視・ヘルスチェックが一切実装されていない。
金曜のデータ取得が静かに失敗しても、日曜夜まで気付かない可能性がある。

#### 【High】32bit/64bit 分離の負債

- `py -3.14-32` は subprocess で呼び出す設計で、将来的な Python バージョンアップ時に断絶リスク
- Python の 32bit ビルドは Python 3.13 以降でサポート縮小傾向にある
- テスト困難: CI で 32bit Windows Python 環境を再現するのが難しい

**中期的解決策**: JVLink の呼び出しを別マイクロサービス化（32bit専用の小さな FastAPI サーバー）し、
メインプロセスは REST 経由で呼び出す。将来的に JVLink が REST API 化したときのシームレスな移行にもなる。

#### 【High】クラウドバックアップなし

```
現状のバックアップ戦略:
  1. backup.py → ローカルの .zip ファイル
  2. git_ops.py → GitHub（コードのみ、DB は .gitignore）
```

`umalogi.db` がローカルにしか存在しない。PC 故障イコール全データ消失。
`rclone` + Dropbox/Google Drive への自動 DB バックアップが最低限必要。

### 2-2. 監視・可観測性のゼロ状態

#### 【Critical】障害の無音化

以下の監視が一切存在しない:
- **プロセス死活監視**: scheduler.py の停止検知
- **データ品質監視**: 異常なレース数・欠損値率の急増
- **モデルドリフト検知**: 連続 N レースで的中率が閾値を下回った場合のアラート
- **DB 容量監視**: SQLite ファイルサイズの急増（ログテーブルの肥大化など）

**最小限の対策（低コスト）**:
```python
# スケジューラの各ジョブにヘルスチェックを追加
def _heartbeat(job_name: str) -> None:
    # UptimeRobot や Better Uptime の Push URL に HTTP GET
    # または Discord に "✅ {job_name} 完了" を通知
    ...
```

### 2-3. JRA-VAN 仕様変更への耐性

#### 【Medium】COM コンポーネント依存の脆弱性

- JVLink は 30 年以上前のアーキテクチャ（COM）に依存
- JRA が将来的に Web API / MQTT / gRPC 等に移行した場合、`jravan_client.py` の全面書き直しが必要
- データフォーマット（JV-Data コード表）の仕様変更に対するバリデーション層が薄い

**防衛策**: JVLink の呼び出しを `DataProvider` 抽象クラスで隠蔽し、
将来的なバックエンド差し替えを容易にする。

---

## 3. 商用化・UI/UX のボトルネック

### 3-1. API の完全無認証公開

#### 【Critical for Business】Next.js API ルートが認証なし

```typescript
// web/src/app/api/predictions/route.ts
export async function GET(request: NextRequest) {
  // ← 認証チェックが存在しない
  const predictions = getPredictions(raceId);
  return NextResponse.json(predictions);
}
```

競合他社・非会員が無制限に予想データを取得できる状態。
「予想を売る」ビジネスモデルとして致命的な欠陥。

**最小限の対策**: API Key 認証（ヘッダーに `X-API-Key: <secret>` を要求）をミドルウェアで実装。
最終的には NextAuth.js でユーザー管理 + JWT に移行。

### 3-2. モデル根拠の不可視性（コンテンツとしての説得力不足）

#### 【High】SHAP 値の非可視化

現状のUI（PredictionsPanel.tsx）は「スコアと推奨買い目」を表示するだけで、
「なぜこの馬が高スコアなのか」を説明しない。

コンテンツとして競合と差別化するために必要なのは:

```python
# SHAP による特徴量寄与の計算（予測時に追加）
import shap
explainer = shap.TreeExplainer(model.booster_)
shap_values = explainer.shap_values(X_pred)

# 上位3特徴量を自然言語に変換
# 例: "調教タイム（-0.3秒の改善）が最大のプラス要因"
```

この情報が「見解文」として出力されると、予想コンテンツとしての説得力が飛躍的に向上する。

#### 【Medium】narrative_generator.py の活用不足

`src/ml/narrative_generator.py` が存在するにもかかわらず、
SNS通知・UIへの統合状況が不明。このモジュールを以下に接続すべき:

1. Discord 通知の本文（現在は数値羅列のみと推測）
2. ウマニティ投稿の見解欄
3. Web ダッシュボードの「AI見解」セクション

### 3-3. サブスクリプション化の構造的欠如

#### 【High】課金・会員管理インフラがゼロ

商用化に必要なコンポーネントが全て欠損:

| 機能 | 必要ライブラリ | 現状 |
|---|---|---|
| ユーザー認証 | NextAuth.js / Clerk | なし |
| 決済処理 | Stripe | なし |
| サブスクリプション管理 | Stripe Billing | なし |
| note/fanbox 連携 | note API | なし |
| メール通知 | Resend / SendGrid | なし |

**段階的アプローチ**: まず API Key 認証で「招待制」ベータを実装し、
手動で友人・モニターユーザーに配布。その後 Stripe 連携で正式課金化。

### 3-4. データ可視化の深度不足

- **オッズ変動の時系列グラフなし**: 直前予想の根拠となる「オッズ変動トレンド」がUIにない
- **会場別・距離別ドリルダウンが限定的**: BiasPanel はあるが操作性が低い
- **資産推移グラフなし**: FinancialDashboard は月別集計はあるが、日次の資産曲線（Equity Curve）がない
- **勝率と回収率の分解表示なし**: 騎手別・馬主別・血統別のROI分解ができない

---

## 4. その他の技術的負債

### 4-1. CLAUDE.md 禁止規則との矛盾

```
# requirements.txt に残存
beautifulsoup4>=4.12.0   # netkeiba HTML パース用
lxml>=5.1.0              # netkeiba XML/HTML パーサー
```

`CLAUDE.md` で「netkeiba.com へのアクセスは一切禁止」と明記されているにもかかわらず、
netkeiba スクレイピング用ライブラリが依存関係に残存している。
`src/scraper/netkeiba.py` も存在しており、コードが削除されていない。
完全に削除し、関連するコードとテストもクリーンアップすること。

### 4-2. モダンなパッケージ管理への未移行

- `pyproject.toml` が存在せず、旧来の `requirements.txt` のみ
- 依存関係のロックファイル（`requirements.lock`）なし → 環境再現性が低い
- `uv` / `poetry` への移行で再現性・速度が大幅向上

### 4-3. テスト体制の不透明性

- `pytest` は依存関係にあるが、テストファイルの存在・カバレッジが不明
- CI（GitHub Actions）の設定が確認されない
- 特に `evaluator.py`（同着・返還処理）のテストは業務ロジックのコアであり、
  テストなしでの変更は回収率計算の破壊につながる

### 4-4. Docker 化なし

- 環境依存（Windows 11 + 32bit Python）が強く、他PCへの移行が困難
- クラウド移行（AWS/GCP）の障壁になっている

---

## 優先順位付き次世代開発ロードマップ

### Phase 1 — 即時対応（〜1ヶ月）: 「穴を塞げ」

既存システムの根本的な欠陥を最小工数で修正する。収益への直接影響が大きいものを優先。

| 優先度 | タスク | 工数 | 効果 |
|---|---|---|---|
| P0 | GroupKFold → TimeSeriesSplit に変更（バックテスト信頼性の回復） | 2h | バックテスト指標の正確化 |
| P0 | DBのクラウド自動バックアップ実装（rclone + Google Drive） | 3h | 全データ消失リスク排除 |
| P0 | スケジューラの死活監視追加（Discordへの定期ハートビート） | 2h | 無音障害の検知 |
| P1 | バンクロールの prediction_results 動的連動実装 | 4h | Kelly の効果を実現 |
| P1 | Windows タスクスケジューラへの scheduler.py 登録 | 1h | 再起動時自動復帰 |
| P1 | `requirements.txt` から netkeiba 関連ライブラリを削除 | 1h | CLAUDE.md 規則準拠 |
| P2 | 当日バイアス特徴量のリーク検証（simulate_year での計算順序確認） | 4h | 特徴量品質の保証 |

**Phase 1 の完了基準**: バックテストのAUCが「未来情報なし」で再計算され、
DBが毎日クラウドにバックアップされており、ハートビートが Discord に届いている状態。

---

### Phase 2 — 中期改善（1〜3ヶ月）: 「精度と商用化の橋頭堡」

予測精度の向上と、最初の課金ユーザーを獲得するための基盤整備。

| 優先度 | タスク | 工数 | 効果 |
|---|---|---|---|
| P0 | Next.js API への API Key 認証実装（招待制ベータ開始） | 8h | コンテンツ保護・商用化の第一歩 |
| P0 | SHAP 値計算 + UI 表示 + 見解文生成への統合 | 16h | コンテンツ説得力の大幅向上 |
| P1 | narrative_generator.py を Discord/ウマニティ通知に完全統合 | 8h | SNS エンゲージメント向上 |
| P1 | ALPHA モデルの win_odds データ拡充（2022-2025 取得計画） | 40h | ROI の正確な計算基盤 |
| P1 | Equity Curve（日次資産推移）グラフを FinancialDashboard に追加 | 8h | 成果の可視化 |
| P1 | LabelEncoder の未見ラベル対応（OrdinalEncoder + fallback） | 4h | 新人騎手エラー防止 |
| P2 | モデルドリフト検知（連続 N 敗でアラート）の実装 | 8h | 性能劣化の早期発見 |
| P2 | JVLink 呼び出しを DataProvider 抽象クラスで隠蔽 | 12h | 将来の仕様変更耐性 |

**Phase 2 の完了基準**: 10名のベータユーザーが API Key で予想を取得でき、
SHAP ベースの見解文が自動生成され SNS に投稿されている状態。

---

### Phase 3 — 長期戦略（3〜6ヶ月）: 「スケーラブルな競馬 SaaS」

アーキテクチャを商用プロダクトとして成立させる抜本的な再設計。

| 優先度 | タスク | 工数 | 効果 |
|---|---|---|---|
| P0 | Stripe 決済 + NextAuth.js でサブスクリプション課金実装 | 40h | 持続的な収益基盤 |
| P0 | Docker 化（docker-compose でローカル完全再現） | 16h | 環境依存の解消 |
| P1 | AWS / GCP への段階的移行（スケジューラ → Lambda/Cloud Run） | 80h | PC 依存からの脱却 |
| P1 | 32bit JVLink を FastAPI マイクロサービス化（REST ブリッジ） | 24h | Python バージョン依存解消 |
| P1 | note/fanbox 連携 API 実装（プレミアム記事の自動生成・投稿） | 24h | コンテンツ収益化 |
| P2 | PostgreSQL への移行検討（同時接続・レプリケーション対応） | 60h | スケーラビリティ確保 |
| P2 | A/B テスト基盤の実装（Champion-Challenger の UI 可視化） | 16h | モデル改善サイクルの加速 |
| P2 | `pyproject.toml` + `uv` によるパッケージ管理刷新 | 4h | 環境再現性の確保 |

**Phase 3 の完了基準**: サブスクリプション課金が稼働し、
月間 MRR（Monthly Recurring Revenue）が定常的に発生している状態。

---

## 総括

UMALOGIは競馬AIとして「動いている」が、「勝ち続けられる」には至っていない。
最大の問題は技術ではなく、**バックテストの信頼性**（GroupKFold リーク）と
**インフラの脆弱性**（ローカルPC SPOF）という2つの根本欠陥にある。

Phase 1 を徹底することで、既存モデルの「真の実力」が初めて可視化される。
その数値を見てから Phase 2 以降の方向性（精度向上 vs 商用化優先）を判断すること。

> **最優先の一言**: GroupKFold を TimeSeriesSplit に変えてバックテストを再実行せよ。
> その結果が、このプロジェクトの本当の出発点だ。

---

*このドキュメントは自動生成された分析報告です。判断の最終責任はエンジニアにあります。*
