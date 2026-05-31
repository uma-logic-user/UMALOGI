# AIウマスギエンジン 設計仕様書

**作成日**: 2026-05-24  
**ステータス**: 承認済み・実装中  
**分類**: src/umasugi_engine/ — ラッパー型分離

---

## 設計背景

### 問題意識（AIウマスギの設計思想）

現在の競馬トレンドである「穴狙い」は、影響力のある人物に推奨されることで
逆にオッズが下がり、期待値が消失する（大衆心理のジレンマ）。

UMALOGI の既存ロジック（`src/ml/`）は世論分析を `u_score` の 5% ウェイトの
`crowd_bias_ratio` でのみ捕捉しており、「影響者による人気操作」を検知できない。

### 目的

- 30の重要ファクターによる「絶対勝率」算出の精度向上
- 世論分析を **負の相関（EV減算）** として実装し、大衆心理のジレンマを克服
- 既存ロジック（legacy_logic）との並列比較環境を構築

---

## アーキテクチャ

### 分離方式：(B) ラッパー型

```
src/ml/                    # legacy_logic（既存・変更なし）
src/umasugi_engine/        # 新ロジック（差分のみ実装）
  └── legacy からインポートして拡張
```

`src/ml/` は一切変更しない。`umasugi_engine` は既存モジュール
（`FeatureBuilder`, `UScoreEngine`）をインポートして拡張する。

---

## ディレクトリ構造

```
src/umasugi_engine/
├── __init__.py
├── engine.py              # UmasugiEngine クラス（メインエントリポイント）
├── factors/
│   ├── __init__.py
│   ├── crowd_opinion.py   # 世論分析フィルター（crowd_bias 強化版：5% → 15%）
│   ├── track_style.py     # 小回り適性ファクター（新規）
│   └── turf_type.py       # 野芝/洋芝適性ファクター（新規）
├── scorer.py              # 30因子スコアリング（UScoreEngine ラッパー + 拡張）
├── ev_filter.py           # 世論分析による EV 減算フィルター（負の相関）
└── comparator.py          # legacy vs umasugi 並列比較ラッパー
```

---

## 実装仕様

### 1. UmasugiEngine（engine.py）

エントリポイント。以下の順序で処理する：

1. `FeatureBuilder`（legacy）で基本特徴量を生成
2. `UScoreEngine`（legacy）でベース u_score を計算
3. 3つの新規因子を追加計算（track_style, turf_type, crowd_opinion）
4. `ev_filter.py` で世論ペナルティを EV に適用
5. 最終スコアを返却

```python
class UmasugiEngine:
    def predict(self, race_id: str) -> pd.DataFrame:
        """umasugi_ev, legacy_ev, crowd_penalty 列を含む DataFrame を返す"""
```

### 2. 世論分析フィルター（ev_filter.py）

**設計原則**: 世論（SNS人気・オッズ圧縮）は **正の相関ではなく負の相関** として EV に適用する。

```python
# 世論圧力スコア（0〜1）
opinion_pressure = x_consensus_score × odds_compression_ratio
# odds_compression は odds_steam_flag で代替（opening オッズが無い場合）

# EV減算ペナルティ（sigmoid で滑らかに）
OPINION_THRESHOLD = 0.4
MAX_PENALTY = 0.35
crowd_penalty = sigmoid(opinion_pressure - OPINION_THRESHOLD)
ev_final = ev_base × (1.0 - crowd_penalty × MAX_PENALTY)
```

**既存ロジックとの差分**:
- 既存: `crowd_bias_ratio > 1.3 → EV × 1.5`（正の相関）
- 新規: `opinion_pressure 高 → EV × (1 - penalty)`（負の相関）

### 3. 小回り適性ファクター（factors/track_style.py）

`track_direction` カラムが全行空値のため、**会場ベース分類**を採用。

| 分類 | 会場 |
|------|------|
| 確定的小回り | 小倉, 函館, 札幌, 福島 |
| 条件次第小回り | 中山（芝1200-1800m）, 阪神（芝1400m以下） |
| 大回り | 東京, 京都, 中京, 新潟 |

```python
def calc_track_style_score(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    出力列: track_style_score (0〜1)
      1.0 = その馬の小回り適性が高い
      0.0 = その馬の大回り適性が高い（小回り不得意）
    """
```

### 4. 野芝/洋芝適性ファクター（factors/turf_type.py）

`turf_type` カラムが存在しないため、**会場ベース推定**を採用。

| 芝種 | 会場 |
|------|------|
| 洋芝 | 札幌, 函館 |
| 野芝 | 中京, 中山, 京都, 小倉, 新潟, 東京, 福島, 阪神 |

```python
YOSHIBA_VENUES = {"札幌", "函館"}  # 洋芝

def calc_turf_type_score(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    出力列: turf_type_score (0〜1)
      対象レースの芝種での過去 win_rate / 通算 win_rate
      ※ダートレースは 0.5（中立）固定
    """
```

### 5. 並列比較ラッパー（comparator.py）

```python
def compare_predictions(race_id: str, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    戻り値列:
      horse_number, horse_name,
      legacy_ev, umasugi_ev, ev_delta,
      crowd_penalty, track_style_score, turf_type_score,
      recommendation  # "umasugi_better" / "legacy_better" / "agree"
    """
```

将来的に `/api/compare/<race_id>` エンドポイントに接続（今回スコープ外）。

---

## データソース確認結果

| 項目 | カラム | 備考 |
|------|--------|------|
| 会場 | `races.venue` | 10会場確認済み |
| 馬場 | `races.surface` | `芝` / `ダート` / `障害` |
| 小回り判定 | `races.track_direction` | **全行空値** → 会場ベース推定で代替 |
| 野芝/洋芝 | `races.turf_type` | **カラム不存在** → 会場ベース推定で代替 |
| 天気・馬場状態 | `races.weather`, `races.condition` | 存在確認済み |

---

## 実装優先度

| フェーズ | 内容 | ステータス |
|---------|------|-----------|
| Phase 1 | ディレクトリ構造・__init__.py 作成 | 実装中 |
| Phase 1 | `factors/track_style.py` 実装 | 実装中 |
| Phase 1 | `factors/turf_type.py` 実装 | 実装中 |
| Phase 2 | `factors/crowd_opinion.py` 実装 | 未着手 |
| Phase 2 | `ev_filter.py` 実装（EV減算・負の相関） | 未着手 |
| Phase 3 | `engine.py` 統合 | 未着手 |
| Phase 3 | `comparator.py` 並列比較 | 未着手 |
| Phase 4 | `scorer.py` 30因子スコアリング完成 | 未着手 |

---

## 改善効果の見込み

世論分析フィルターは以下のケースで既存 EV 予測を最も改善できる：

1. **SNS で話題の「穴馬」**: `x_consensus_score` が高く `odds_steam_flag = 1` の馬は
   期待値が既に消失しているにもかかわらず既存ロジックでは買いシグナルが出る。
   → umasugi_engine では EV を最大 35% 引き下げ、誤買いを防止。

2. **影響者推奨による短期オッズ圧縮**: 開場直後から急落するオッズは
   `odds_compression_ratio` で捕捉。

3. **小回り・洋芝の得意不得意の精密化**: 会場特性を明示的に因子化することで、
   現状の暗黙的な会場エンコードより直接的な適性スコアを提供。
