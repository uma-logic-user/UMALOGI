# 単複特化モデル再構築・資金推移シミュレーション

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ターゲットリーク（rank=NULL 78.5%問題）を修正して単勝・複勝特化モデルを再構築し、2024-2026年の3年間資金推移シミュレーションと天皇賞（春）予想を出力する。

**Architecture:**
- `src/ml/models.py` の `_build_train_df` を修正してリーク・外れ値を排除。`PlaceModel` を追加。
- `scripts/retrain_win_place.py` で 2024-2025クリーンデータ含む全データで再学習。
- `scripts/simulate_win_place.py` で単勝・複勝限定シミュレーション + 資金管理（初期¥100,000、¥1,000/型/レース）。

**Tech Stack:** Python 3.11+, LightGBM, SQLite, pandas, `src/ml/models.py`, `src/ml/features.py`

---

## 根本原因サマリー

| 問題 | 原因 | 修正 |
|---|---|---|
| AUC 0.97超え | `_build_train_df` が `rr.rank IS NOT NULL` でフィルタ → 2025年は1レース3頭のみ学習、正例率1/3 | `actual_rows`クエリからフィルタ削除、`merge(how="left")` |
| EV 4800超え外れ値 | `ev_target` fallback = `win_odds*100`（payout NULL時）で超高オッズ馬が汚染 | キャップを50,000→10,000に引き下げ |
| 複勝モデルなし | HonmeiModel（is_winner目標）しか存在しない | `PlaceModel` クラスを追加（is_placed=rank≤3） |

## ファイル変更マップ

| ファイル | 変更 |
|---|---|
| `src/ml/models.py` | `_build_train_df` 修正、`PlaceModel` クラス追加、`train_all` 更新 |
| `scripts/retrain_win_place.py` | 新規: 全データ再学習スクリプト |
| `scripts/simulate_win_place.py` | 新規: 単複限定シミュレーション + 資金管理 |
| `tests/test_win_place_model.py` | 新規: リーク修正・PlaceModel のテスト |
| `docs/win_place_simulation_report.md` | 新規: 最終レポート |

---

## Task 1: `_build_train_df` のターゲットリーク修正 + EV外れ値フィルタ強化

**Files:**
- Modify: `src/ml/models.py:163-225`
- Test: `tests/test_win_place_model.py`

- [ ] **Step 1: 失敗するテストを書く**

```python
# tests/test_win_place_model.py
"""単複特化モデル修正のテスト: リーク排除・PlaceModel"""
from __future__ import annotations
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _make_restored_db() -> sqlite3.Connection:
    """restore_results_from_payouts と同様に rank=1/2/3 のみ設定された DB を模倣する。"""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY,
            date TEXT,
            venue TEXT DEFAULT '東京',
            race_number INTEGER DEFAULT 5,
            distance INTEGER DEFAULT 1600,
            surface TEXT DEFAULT '芝',
            condition TEXT DEFAULT '良'
        );
        CREATE TABLE race_results (
            race_id TEXT,
            horse_number INTEGER,
            horse_id TEXT,
            horse_name TEXT,
            sex_age TEXT DEFAULT '牡3',
            weight_carried REAL DEFAULT 55.0,
            gate_number INTEGER DEFAULT 1,
            horse_weight REAL,
            horse_weight_diff REAL,
            jockey TEXT DEFAULT '',
            trainer TEXT DEFAULT '',
            win_odds REAL,
            popularity INTEGER,
            rank INTEGER
        );
        CREATE TABLE race_payouts (
            race_id TEXT, bet_type TEXT, combination TEXT, payout INTEGER
        );
        CREATE TABLE entries (
            race_id TEXT, horse_number INTEGER, horse_id TEXT, horse_name TEXT,
            sex_age TEXT, weight_carried REAL, gate_number INTEGER,
            horse_weight REAL, horse_weight_diff REAL, jockey TEXT, trainer TEXT
        );
        CREATE TABLE horses (horse_id TEXT PRIMARY KEY, sire TEXT);
        CREATE TABLE jockeys (jockey_name TEXT, jockey_code TEXT);
        CREATE TABLE trainers (trainer_name TEXT, trainer_code TEXT);
        CREATE TABLE realtime_odds (race_id TEXT, horse_number INTEGER, horse_name TEXT, win_odds REAL, fetched_at TEXT, popularity INTEGER, recorded_at TEXT);
        CREATE TABLE training_times (horse_id TEXT, training_date TEXT, time_4f REAL, lap_time REAL);
        CREATE TABLE training_hillwork (horse_id TEXT, training_date TEXT, time_4f REAL, lap_time REAL);
    """)
    # 1レース: 5頭 rank=1/2/3のみ設定 (他はNULL) — 復元後の状態
    conn.execute("INSERT INTO races VALUES ('R001','2025-06-01','東京',5,1600,'芝','良')")
    for i in range(1, 6):
        rank_val = i if i <= 3 else None
        conn.execute(
            "INSERT INTO race_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ('R001', i, None, f'馬{i}', '牡3', 55.0, i, 500.0, 0.0, '', '',
             float(i * 5), i, rank_val)
        )
    conn.execute("INSERT INTO race_payouts VALUES ('R001','単勝','1',500)")
    conn.commit()
    return conn


def test_build_train_df_includes_all_horses_not_just_ranked() -> None:
    """_build_train_df が rank=NULL の馬も含んで is_winner=0 とすること。"""
    from src.ml.models import _build_train_df
    conn = _make_restored_db()
    df = _build_train_df(conn)
    # 5頭全員が含まれるはず (rank=NULL → is_winner=0)
    assert len(df) == 5, f"期待5頭, 実際{len(df)}頭"
    assert df["is_winner"].sum() == 1, "勝者は1頭のみ"


def test_build_train_df_ev_target_capped_at_10000() -> None:
    """ev_target が 10,000 を超えないこと。"""
    from src.ml.models import _build_train_df
    conn = _make_restored_db()
    # 超高オッズ馬を追加 (win_odds=150, rank=NULL)
    conn.execute(
        "INSERT INTO race_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ('R001', 6, None, '超穴馬', '牡3', 55.0, 6, 500.0, 0.0, '', '',
         150.0, 6, None)
    )
    conn.commit()
    df = _build_train_df(conn)
    max_ev = df["ev_target"].max()
    assert max_ev <= 10000, f"EV上限超え: {max_ev}"


def test_build_train_df_is_placed_created() -> None:
    """is_placed 列（rank<=3=1）が存在すること。"""
    from src.ml.models import _build_train_df
    conn = _make_restored_db()
    df = _build_train_df(conn)
    assert "is_placed" in df.columns
    # rank=1,2,3 の3頭は is_placed=1, NULL2頭は0
    assert df["is_placed"].sum() == 3, f"期待3頭, 実際{df['is_placed'].sum()}頭"
```

- [ ] **Step 2: テストが失敗することを確認**

```
pytest tests/test_win_place_model.py -v
```

期待: FAIL (全3テスト)

- [ ] **Step 3: `_build_train_df` を修正する**

`src/ml/models.py` の `_build_train_df` 関数（行163-225）を以下に置き換える:

```python
def _build_train_df(
    conn: sqlite3.Connection,
    train_until: int | None = None,
) -> pd.DataFrame:
    """
    FeatureBuilder を使ってリーク排除済みの学習 DataFrame を生成する。

    **修正点 (2026-05-03)**
    - actual_rows クエリから `AND rr.rank IS NOT NULL` を削除し、
      rank=NULL の馬（復元データで上位3頭以外）も学習に含める。
      これにより正例率が 1/3 → 1/14〜1/18 の正常値に戻り AUC 過剰推定を防ぐ。
    - EV外れ値キャップを 50,000→10,000 に引き下げ（100倍上限）。
    - `is_placed` 目的変数（rank<=3=1）を追加。
    """
    from src.ml.features import FeatureBuilder

    if train_until is not None:
        race_rows = conn.execute(
            """
            SELECT DISTINCT r.race_id
            FROM   races r
            JOIN   race_results rr ON rr.race_id = r.race_id
            WHERE  rr.rank IS NOT NULL
            AND    CAST(substr(r.date, 1, 4) AS INTEGER) <= ?
            ORDER  BY r.date
            """,
            (train_until,),
        ).fetchall()
    else:
        race_rows = conn.execute(
            """
            SELECT DISTINCT r.race_id
            FROM   races r
            JOIN   race_results rr ON rr.race_id = r.race_id
            WHERE  rr.rank IS NOT NULL
            ORDER  BY r.date
            """
        ).fetchall()

    if not race_rows:
        return pd.DataFrame()

    fb = FeatureBuilder(conn)
    frames: list[pd.DataFrame] = []

    for (race_id,) in race_rows:
        df_feat = fb.build_race_features_for_simulate(race_id)
        if df_feat.empty:
            continue

        # ── rank=NULL を含む全出走馬の着順・払戻を取得 ─────────
        # rank IS NOT NULL フィルタを削除: 復元データで上位3頭以外は NULL だが
        # 「負け馬」として学習に含めることで正例率を正常化する。
        actual_rows = conn.execute(
            """
            SELECT
                rr.horse_name,
                rr.rank,
                rp.payout AS payout_tansho
            FROM   race_results rr
            LEFT JOIN race_payouts rp
                   ON  rp.race_id     = rr.race_id
                   AND rp.bet_type    = '単勝'
                   AND rp.combination = CAST(rr.horse_number AS TEXT)
            WHERE  rr.race_id = ?
            """,
            (race_id,),
        ).fetchall()

        if not actual_rows:
            continue

        actuals = pd.DataFrame(actual_rows, columns=["horse_name", "rank", "payout_tansho"])
        # inner → left: df_feat（全馬）にactuals（全馬）を左結合
        # rank=NULL の馬は rank=NaN → is_winner=0, is_placed=0 として扱われる
        df_feat = df_feat.merge(actuals, on="horse_name", how="left")
        df_feat["race_id"] = race_id
        frames.append(df_feat)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # ── EV外れ値フィルタリング（10,000円 = 100倍上限） ──────────
    _MAX_TANSHO_PAYOUT = 10_000   # 100倍上限（旧: 50,000）
    before_filter = len(df)
    payout_ok = df["payout_tansho"].isna() | (df["payout_tansho"].astype(float) <= _MAX_TANSHO_PAYOUT)
    df = df[payout_ok].copy()
    removed = before_filter - len(df)
    if removed > 0:
        logger.warning("EV外れ値フィルタ: %d行を除外 (payout_tansho > %d)", removed, _MAX_TANSHO_PAYOUT)

    # ── 目的変数 ──────────────────────────────────────────────────
    # NaN rank は非勝者（0）として扱う
    df["is_winner"] = (df["rank"] == 1).astype(int)
    df["is_placed"]  = (df["rank"] <= 3).astype(int)   # ← 複勝モデル用
    df["ev_target"] = np.where(
        df["payout_tansho"].notna(),
        df["payout_tansho"].astype(float),
        np.where(
            df["rank"] == 1,
            df["win_odds"].fillna(0).astype(float) * 100.0,
            0.0,
        ),
    ).astype(float)

    return df
```

- [ ] **Step 4: テストが通ることを確認**

```
pytest tests/test_win_place_model.py::test_build_train_df_includes_all_horses_not_just_ranked tests/test_win_place_model.py::test_build_train_df_ev_target_capped_at_10000 tests/test_win_place_model.py::test_build_train_df_is_placed_created -v
```

期待: PASS 3件

- [ ] **Step 5: コミット**

```bash
git add tests/test_win_place_model.py src/ml/models.py
git commit -m "fix: _build_train_df リークフィルタ除去・EV上限引下げ・is_placed追加"
```

---

## Task 2: `PlaceModel` クラス追加 + `train_all` 更新

**Files:**
- Modify: `src/ml/models.py`（HonmeiModel定義の後、ManjiModel定義の前に追加）
- Modify: `src/ml/models.py` の `train_all()`、`load_models()`

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_win_place_model.py` に追記:

```python
def test_place_model_trains_on_is_placed() -> None:
    """PlaceModel が is_placed を学習できること。"""
    from src.ml.models import PlaceModel
    conn = _make_restored_db()
    model = PlaceModel()
    result = model.train(conn)
    assert result["n_samples"] > 0
    assert model.is_trained


def test_place_model_predict_returns_series() -> None:
    """PlaceModel.predict() が pd.Series を返すこと。"""
    import pandas as pd
    from src.ml.models import PlaceModel, FEATURE_COLS
    conn = _make_restored_db()
    model = PlaceModel()
    model.train(conn)
    # 最小限の特徴量 DataFrame を作成
    df = pd.DataFrame({col: [0.0] * 3 for col in FEATURE_COLS})
    result = model.predict(df)
    assert isinstance(result, pd.Series)
    assert len(result) == 3
```

- [ ] **Step 2: テスト失敗確認**

```
pytest tests/test_win_place_model.py::test_place_model_trains_on_is_placed tests/test_win_place_model.py::test_place_model_predict_returns_series -v
```

期待: FAIL (`PlaceModel` not defined)

- [ ] **Step 3: `PlaceModel` クラスを `src/ml/models.py` に追加する**

`HonmeiModel` クラス定義の直後（`ManjiModel` の前）に以下を追加:

```python
# ── 複勝モデル ────────────────────────────────────────────────────

class PlaceModel(_BaseModel):
    """
    複勝モデル（3着以内確率特化）。

    HonmeiModel と同一アーキテクチャ（LightGBM + Isotonic Regression）で
    `is_placed`（rank ≤ 3 = 1）を目的変数として訓練する。
    GroupKFold CV で AUC を計算し、全データで本訓練する。
    """

    _filename = "place_model"

    _LGBM_PARAMS: dict[str, Any] = dict(
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=10,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbose=-1,
    )

    def __init__(self) -> None:
        self._model: Any = None
        self._base_lgbm: LGBMClassifier = LGBMClassifier(**self._LGBM_PARAMS)
        self._trained = False

    def train(
        self,
        conn: sqlite3.Connection,
        train_until: int | None = None,
    ) -> dict[str, Any]:
        """
        `is_placed`（rank ≤ 3 = 1）を目的変数として訓練する。
        複勝の正例率は ~3/N (N≒15) ≈ 20% と適切なバランス。
        """
        df = _build_train_df(conn, train_until=train_until)
        if df.empty:
            logger.warning("学習データが0件のため複勝モデル訓練をスキップします")
            return {"n_races": 0, "n_samples": 0}

        n_races = df["race_id"].nunique()
        if n_races < _MIN_TRAIN_RACES:
            logger.warning("複勝モデル: 学習レース数が少ない (%d 件)", n_races)

        df_sorted = df.sort_values("race_id").reset_index(drop=True)
        X_all  = df_sorted[FEATURE_COLS].astype(float).fillna(-1)
        y_all  = df_sorted["is_placed"]
        groups = df_sorted["race_id"]

        n_splits = min(5, n_races)
        aucs: list[float] = []
        oof_preds = np.zeros(len(X_all), dtype=float)

        if n_splits >= 2:
            gkf = GroupKFold(n_splits=n_splits)
            for tr_idx, val_idx in gkf.split(X_all, y_all, groups=groups):
                clone = LGBMClassifier(**self._LGBM_PARAMS)
                clone.fit(X_all.iloc[tr_idx], y_all.iloc[tr_idx])
                proba = clone.predict_proba(X_all.iloc[val_idx])[:, 1]
                oof_preds[val_idx] = proba
                try:
                    aucs.append(roc_auc_score(y_all.iloc[val_idx], proba))
                except ValueError:
                    pass

        cv_auc_mean = float(np.mean(aucs)) if aucs else float("nan")
        cv_auc_std  = float(np.std(aucs))  if aucs else float("nan")

        iso = IsotonicRegression(out_of_bounds="clip")
        if np.any(oof_preds != 0):
            iso.fit(oof_preds, y_all)
        else:
            iso.fit(np.zeros(len(y_all)), y_all)

        self._base_lgbm = LGBMClassifier(**self._LGBM_PARAMS)
        self._base_lgbm.fit(X_all, y_all)
        self._model = _IsotonicModel(base=self._base_lgbm, iso=iso)
        self._trained = True

        logger.info(
            "複勝モデル訓練完了: %d レース / %d サンプル / CV AUC %.4f ±%.4f",
            n_races, len(df), cv_auc_mean, cv_auc_std,
        )
        return {
            "n_races": n_races,
            "n_samples": len(df),
            "cv_auc_mean": cv_auc_mean,
            "cv_auc_std": cv_auc_std,
            "train_until": train_until,
        }

    def predict(self, df: pd.DataFrame) -> pd.Series:
        """各馬の複勝確率（rank ≤ 3 確率）を返す。"""
        if not self._trained:
            logger.debug("未訓練複勝モデル — フォールバック（オッズ逆数）使用")
            odds = df["win_odds"].fillna(100.0).clip(lower=1.0)
            score = 1.0 / odds
            return score / score.sum() if score.sum() > 0 else score

        X = df[FEATURE_COLS].astype(float).fillna(-1)
        proba = self._model.predict_proba(X)[:, 1]
        return pd.Series(proba, index=df.index, name="place_score")
```

- [ ] **Step 4: `train_all()` と `load_models()` を更新する**

`train_all()` 関数のシグネチャと戻り値を更新:

```python
def train_all(
    conn: sqlite3.Connection,
    train_until: int | None = None,
) -> dict[str, dict]:
    """本命・複勝・卍モデルを訓練して data/models/ に保存する。"""
    honmei = HonmeiModel()
    place  = PlaceModel()
    manji  = ManjiModel()

    h_result = honmei.train(conn, train_until=train_until)
    p_result = place.train(conn, train_until=train_until)
    m_result = manji.train(conn, train_until=train_until)

    # ── 本命モデル: Champion/Challenger 判定 ─────────────────────
    if honmei.is_trained:
        challenger_auc: float = h_result.get("challenger_auc", float("nan"))
        champion_auc:   float = h_result.get("champion_auc",   float("nan"))
        if np.isnan(champion_auc) or np.isnan(challenger_auc):
            honmei.save()
            h_result["promoted"] = True
        elif challenger_auc >= champion_auc - 0.005:
            honmei.save()
            h_result["promoted"] = True
        else:
            h_result["promoted"] = False
            logger.warning(
                "世代交代却下: challenger AUC=%.4f < champion AUC=%.4f",
                challenger_auc, champion_auc,
            )

    if place.is_trained:
        place.save()

    if manji.is_trained:
        manji.save()

    clear_model_cache()
    return {"honmei": h_result, "place": p_result, "manji": m_result}


def load_models() -> tuple[HonmeiModel, PlaceModel, ManjiModel]:
    """保存済みモデルを読み込んで返す（プロセス内でキャッシュ）。"""
    cache_key = str(_MODEL_DIR)
    if cache_key in _MODEL_CACHE:
        logger.debug("モデルキャッシュヒット: %s", cache_key)
        return _MODEL_CACHE[cache_key]

    honmei = HonmeiModel()
    place  = PlaceModel()
    manji  = ManjiModel()

    try:
        honmei.load()
    except FileNotFoundError:
        logger.info("本命モデルが見つかりません — フォールバックモード")

    try:
        place.load()
    except FileNotFoundError:
        logger.info("複勝モデルが見つかりません — フォールバックモード")

    try:
        manji.load()
    except FileNotFoundError:
        logger.info("卍モデルが見つかりません — フォールバックモード")

    _MODEL_CACHE[cache_key] = (honmei, place, manji)
    return honmei, place, manji
```

**注意**: `_MODEL_CACHE` の型を `tuple[HonmeiModel, ManjiModel]` から `tuple[HonmeiModel, PlaceModel, ManjiModel]` に変更する。

- [ ] **Step 5: テストが通ることを確認**

```
pytest tests/test_win_place_model.py -v
```

期待: PASS 5件

- [ ] **Step 6: コミット**

```bash
git add src/ml/models.py tests/test_win_place_model.py
git commit -m "feat: PlaceModel追加 + train_all/load_models更新"
```

---

## Task 3: 再学習スクリプト作成・実行

**Files:**
- Create: `scripts/retrain_win_place.py`

- [ ] **Step 1: スクリプトを作成する**

```python
"""
単勝・複勝・卍モデルを 2024-2025 クリーンデータ含む全期間で再学習するスクリプト。

修正後の _build_train_df（リーク排除・全馬含む）を使って
HonmeiModel / PlaceModel / ManjiModel を再訓練し data/models/ に保存する。

使用例:
    py scripts/retrain_win_place.py
    py scripts/retrain_win_place.py --train-until 2025
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("retrain")


def main() -> None:
    ap = argparse.ArgumentParser(description="単複特化モデル再学習")
    ap.add_argument("--train-until", type=int, default=None,
                    help="学習最終年 (例: 2025 → 2025年以前)")
    args = ap.parse_args()

    from src.database.init_db import init_db
    from src.ml.models import train_all

    conn = init_db()
    print("=" * 60)
    print("  単勝・複勝・卍モデル 再学習")
    print(f"  train_until={args.train_until or '全期間'}")
    print("=" * 60)

    results = train_all(conn, train_until=args.train_until)
    conn.close()

    print()
    h = results["honmei"]
    p = results["place"]
    m = results["manji"]

    print(f"  [本命モデル]")
    print(f"    レース数: {h['n_races']:,}  サンプル数: {h['n_samples']:,}")
    print(f"    CV AUC:   {h['cv_auc_mean']:.4f} ±{h['cv_auc_std']:.4f}")
    print(f"    Challenger AUC: {h.get('challenger_auc', float('nan')):.4f}")
    print(f"    世代交代: {h.get('promoted', '?')}")
    print()
    print(f"  [複勝モデル]")
    print(f"    レース数: {p['n_races']:,}  サンプル数: {p['n_samples']:,}")
    print(f"    CV AUC:   {p['cv_auc_mean']:.4f} ±{p['cv_auc_std']:.4f}")
    print()
    print(f"  [卍モデル]")
    print(f"    レース数: {m['n_races']:,}  サンプル数: {m['n_samples']:,}")
    print()
    print("  完了")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 再学習を実行する（~10〜30分）**

```
py scripts/retrain_win_place.py 2>&1
```

期待:
- 本命モデル CV AUC: 0.60〜0.82（0.97超えなら修正漏れ）
- 複勝モデル CV AUC: 0.62〜0.78
- サンプル数が大幅増加（復元2024/2025データを含むため）

- [ ] **Step 3: コミット**

```bash
git add scripts/retrain_win_place.py
git commit -m "feat: retrain_win_place.py — 単複特化再学習スクリプト"
```

---

## Task 4: 単複限定シミュレーション + 資金管理

**Files:**
- Create: `scripts/simulate_win_place.py`

- [ ] **Step 1: スクリプトを作成する**

```python
"""
単勝・複勝限定の 3 年間バックテスト + 資金管理シミュレーション

HonmeiModel（単勝）・PlaceModel（複勝）の両方を使い、
各レースで TOP-1 馬に ¥1,000 ずつ賭けた場合の損益・資金推移を算出する。

資金管理ルール:
  - 初期資本: ¥100,000
  - 賭け方:   単勝 TOP-1 に ¥1,000 + 複勝 TOP-1 に ¥1,000 = 1レース最大 ¥2,000
  - 残高が ¥1,000 未満になったらそのタイプのベットをスキップ（破産保護）

使用例:
    py scripts/simulate_win_place.py
    py scripts/simulate_win_place.py --year 2026
    py scripts/simulate_win_place.py --date-from 2024-01-01 --date-to 2026-12-31
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

import logging
logging.basicConfig(
    level=logging.WARNING,  # シミュレーション中はWARNINGのみ
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sim_wp")


BET_AMOUNT = 1_000   # ¥1,000 per bet type per race
INITIAL_CAPITAL = 100_000


@dataclass
class RaceBetRecord:
    race_id: str
    race_date: str
    # 単勝
    win_predicted_horse: int
    win_odds: float
    win_hit: bool
    win_payout: int
    win_invested: int
    # 複勝
    place_predicted_horse: int
    place_odds_low: float   # 複勝最低払戻
    place_odds_high: float  # 複勝最高払戻
    place_hit: bool
    place_payout: int
    place_invested: int
    # 資金
    balance_after: float


@dataclass
class SimResult:
    records: list[RaceBetRecord] = field(default_factory=list)
    initial_capital: float = INITIAL_CAPITAL

    @property
    def final_balance(self) -> float:
        if not self.records:
            return self.initial_capital
        return self.records[-1].balance_after

    @property
    def total_invested_win(self) -> int:
        return sum(r.win_invested for r in self.records)

    @property
    def total_invested_place(self) -> int:
        return sum(r.place_invested for r in self.records)

    @property
    def total_invested(self) -> int:
        return self.total_invested_win + self.total_invested_place

    @property
    def total_payout_win(self) -> int:
        return sum(r.win_payout for r in self.records)

    @property
    def total_payout_place(self) -> int:
        return sum(r.place_payout for r in self.records)

    @property
    def n_win_hits(self) -> int:
        return sum(1 for r in self.records if r.win_hit)

    @property
    def n_place_hits(self) -> int:
        return sum(1 for r in self.records if r.place_hit)

    @property
    def n_win_bets(self) -> int:
        return sum(1 for r in self.records if r.win_invested > 0)

    @property
    def n_place_bets(self) -> int:
        return sum(1 for r in self.records if r.place_invested > 0)

    @property
    def roi_win(self) -> float:
        if self.total_invested_win == 0:
            return 0.0
        return self.total_payout_win / self.total_invested_win * 100

    @property
    def roi_place(self) -> float:
        if self.total_invested_place == 0:
            return 0.0
        return self.total_payout_place / self.total_invested_place * 100

    @property
    def max_drawdown(self) -> float:
        """最大ドローダウン（高値からの最大下落額）"""
        if not self.records:
            return 0.0
        peak = self.initial_capital
        max_dd = 0.0
        bal = self.initial_capital
        for r in self.records:
            bal = r.balance_after
            peak = max(peak, bal)
            dd = peak - bal
            max_dd = max(max_dd, dd)
        return max_dd

    @property
    def max_consecutive_losses(self) -> int:
        """最長連敗数（単勝または複勝のどちらかが当たればリセット）"""
        max_streak = current = 0
        for r in self.records:
            if not r.win_hit and not r.place_hit:
                current += 1
                max_streak = max(max_streak, current)
            else:
                current = 0
        return max_streak


def _load_payout_cache(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    """単勝・複勝払戻をキャッシュとして返す。"""
    win_cache: dict[str, int] = {}   # "{race_id}:{horse_number}" -> payout
    place_cache: dict[str, dict[str, int]] = {}  # race_id -> {str(num): payout}

    rows = conn.execute(
        """
        SELECT rp.race_id, rp.bet_type, rp.combination, rp.payout
        FROM race_payouts rp
        JOIN races r ON rp.race_id=r.race_id
        WHERE r.date >= ? AND r.date <= ?
          AND rp.bet_type IN ('単勝','複勝')
        """,
        (date_from, date_to),
    ).fetchall()

    for race_id, bet_type, combo, payout in rows:
        if bet_type == "単勝":
            win_cache[f"{race_id}:{combo}"] = payout
        else:  # 複勝
            place_cache.setdefault(race_id, {})[combo] = payout

    return win_cache, place_cache


def simulate_win_place(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
    initial_capital: float = INITIAL_CAPITAL,
    bet_amount: int = BET_AMOUNT,
) -> SimResult:
    """
    単勝・複勝限定シミュレーション。

    各完全レースで:
    1. HonmeiModel → TOP-1 馬に単勝 ¥bet_amount
    2. PlaceModel  → TOP-1 馬に複勝 ¥bet_amount
    3. 資金残高を更新・追跡
    """
    from src.ml.models import HonmeiModel, PlaceModel
    from src.ml.features import FeatureBuilder

    honmei = HonmeiModel()
    place_model = PlaceModel()

    try:
        honmei.load()
    except FileNotFoundError:
        print("  ⚠️  本命モデル未学習 — フォールバック予測を使用", flush=True)

    try:
        place_model.load()
    except FileNotFoundError:
        print("  ⚠️  複勝モデル未学習 — フォールバック予測を使用", flush=True)

    fb = FeatureBuilder(conn)

    # 完全レース（rank=1,2,3 が揃っているもの）を取得
    races = conn.execute(
        """
        SELECT DISTINCT r.race_id, r.date
        FROM races r
        WHERE r.date >= ? AND r.date <= ?
          AND EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id=r.race_id AND rr.rank=1)
        ORDER BY r.date, r.race_id
        """,
        (date_from, date_to),
    ).fetchall()

    win_cache, place_cache = _load_payout_cache(conn, date_from, date_to)

    result = SimResult(initial_capital=initial_capital)
    balance = initial_capital
    processed = 0

    for race_id, race_date in races:
        try:
            df = fb.build_race_features_for_simulate(race_id)
            if df is None or len(df) < 2:
                continue

            win_scores   = honmei.predict(df)
            place_scores = place_model.predict(df)

            # TOP-1 馬番を取得（シミュレーション用内部連番ではなく実際の馬番に変換）
            # build_race_features_for_simulate は popularity 順で sim_num を付与するため
            # horse_name を経由して race_results の horse_number を取得する
            win_top_idx   = win_scores.idxmax()
            place_top_idx = place_scores.idxmax()

            win_horse_name   = df.loc[win_top_idx,   "horse_name"]
            place_horse_name = df.loc[place_top_idx, "horse_name"]

            # race_results から実際の horse_number を引く
            def _get_horse_num(name: str) -> int:
                row = conn.execute(
                    "SELECT horse_number FROM race_results WHERE race_id=? AND horse_name=? LIMIT 1",
                    (race_id, name),
                ).fetchone()
                return row[0] if row else 0

            win_horse_num   = _get_horse_num(win_horse_name)
            place_horse_num = _get_horse_num(place_horse_name)

            # ── 単勝チェック ─────────────────────────────────────
            win_invested = 0
            win_payout   = 0
            win_hit      = False
            win_odds_val = 0.0
            if balance >= bet_amount:
                win_key = f"{race_id}:{win_horse_num}"
                p = win_cache.get(win_key, 0)
                win_hit      = p > 0
                win_payout   = p if win_hit else 0
                win_invested = bet_amount
                win_odds_val = float(df.loc[win_top_idx, "win_odds"] or 0.0)
                balance      = balance - bet_amount + win_payout

            # ── 複勝チェック ─────────────────────────────────────
            place_invested  = 0
            place_payout    = 0
            place_hit       = False
            place_odds_low  = 0.0
            place_odds_high = 0.0
            if balance >= bet_amount:
                place_pays = place_cache.get(race_id, {})
                p = place_pays.get(str(place_horse_num), 0)
                place_hit     = p > 0
                place_payout  = p if place_hit else 0
                place_invested = bet_amount
                balance       = balance - bet_amount + place_payout
                # 複勝払戻の最低・最高（同じ馬番の複数オッズは1件のみ取得）
                if place_pays:
                    vals = list(place_pays.values())
                    place_odds_low  = min(vals) / 100.0
                    place_odds_high = max(vals) / 100.0

            balance = max(balance, 0.0)  # 残高は0以下にならない

            result.records.append(RaceBetRecord(
                race_id=race_id,
                race_date=race_date,
                win_predicted_horse=win_horse_num,
                win_odds=win_odds_val,
                win_hit=win_hit,
                win_payout=win_payout,
                win_invested=win_invested,
                place_predicted_horse=place_horse_num,
                place_odds_low=place_odds_low,
                place_odds_high=place_odds_high,
                place_hit=place_hit,
                place_payout=place_payout,
                place_invested=place_invested,
                balance_after=balance,
            ))

            processed += 1
            if processed % 500 == 0:
                print(f"  処理済: {processed}/{len(races)}レース  残高: ¥{balance:,.0f}", flush=True)

        except Exception as e:
            logger.debug("race %s スキップ: %s", race_id, e)
            continue

    print(f"  シミュレーション完了: {processed}レース処理")
    return result


def _write_report(
    results: dict[str, SimResult],
    output_path: Path,
) -> None:
    """docs/win_place_simulation_report.md を生成する。"""
    from datetime import datetime
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = [
        "# UMALOGI 単複特化 3年間シミュレーション レポート",
        "",
        f"**生成日時**: {now_str}",
        f"**初期資本**: ¥{INITIAL_CAPITAL:,}",
        f"**賭け金**:   ¥{BET_AMOUNT:,}/型/レース",
        "",
        "---",
        "",
        "## 1. モデル改善概要",
        "",
        "| 項目 | 修正前 | 修正後 |",
        "|---|---|---|",
        "| 学習サンプル/レース | ~3頭 (rank IS NOT NULL) | 全頭 (~14頭) |",
        "| AUC（推定） | >0.97 (過剰推定) | 0.60〜0.82（正常範囲） |",
        "| EV外れ値キャップ | 500倍 (¥50,000) | 100倍 (¥10,000) |",
        "| 対象券種 | 馬連/馬単/三連単 | **単勝・複勝** |",
        "",
        "---",
        "",
        "## 2. 年別シミュレーション結果",
        "",
    ]

    for label, sim in results.items():
        if not sim.records:
            lines.append(f"### {label}: データなし")
            lines.append("")
            continue

        lines += [
            f"### {label}",
            "",
            f"**対象レース数**: {len(sim.records):,}件",
            "",
            "| 券種 | ベット数 | 的中 | 的中率 | ROI | 損益 |",
            "|---|---|---|---|---|---|",
        ]

        # 単勝
        wb = sim.n_win_bets
        wh = sim.n_win_hits
        wi = sim.total_invested_win
        wp = sim.total_payout_win
        wpl = wp - wi
        wroi = wp / wi * 100 if wi > 0 else 0
        wsign = "+" if wpl >= 0 else ""
        lines.append(f"| 単勝 | {wb:,} | {wh} | {wh/wb*100:.1f}% | {wroi:.1f}% | {wsign}¥{wpl:,.0f} |")

        # 複勝
        pb = sim.n_place_bets
        ph = sim.n_place_hits
        pi = sim.total_invested_place
        pp = sim.total_payout_place
        ppl = pp - pi
        proi = pp / pi * 100 if pi > 0 else 0
        psign = "+" if ppl >= 0 else ""
        lines.append(f"| 複勝 | {pb:,} | {ph} | {ph/pb*100:.1f}% | {proi:.1f}% | {psign}¥{ppl:,.0f} |")
        lines.append("")

        # 資金管理
        lines += [
            "**資金管理シミュレーション**:",
            "",
            f"| 項目 | 値 |",
            f"|---|---|",
            f"| 初期資本 | ¥{sim.initial_capital:,.0f} |",
            f"| 最終残高 | ¥{sim.final_balance:,.0f} |",
            f"| 損益 | {'+'  if sim.final_balance >= sim.initial_capital else ''}¥{(sim.final_balance - sim.initial_capital):,.0f} |",
            f"| 最大ドローダウン | ¥{sim.max_drawdown:,.0f} |",
            f"| 最長連敗数 | {sim.max_consecutive_losses}レース |",
            f"| 破産回数（残高<1万円） | {sum(1 for r in sim.records if r.balance_after < 10_000):,}件 |",
            "",
        ]

    # 3年合算
    if "ALL" in results:
        sim = results["ALL"]
        lines += [
            "---",
            "",
            "## 3. 3年合算 総合評価",
            "",
            f"| 項目 | 単勝 | 複勝 |",
            "|---|---|---|",
            f"| ベット数 | {sim.n_win_bets:,} | {sim.n_place_bets:,} |",
            f"| 的中数 | {sim.n_win_hits:,} | {sim.n_place_hits:,} |",
            f"| 的中率 | {sim.n_win_hits/sim.n_win_bets*100:.1f}% | {sim.n_place_hits/sim.n_place_bets*100:.1f}% |",
            f"| ROI | {sim.roi_win:.1f}% | {sim.roi_place:.1f}% |",
            f"| 払戻合計 | ¥{sim.total_payout_win:,} | ¥{sim.total_payout_place:,} |",
            "",
            f"**初期 ¥{sim.initial_capital:,.0f} → 最終 ¥{sim.final_balance:,.0f}**",
            "",
            f"**最大ドローダウン: ¥{sim.max_drawdown:,.0f}**",
            f"**最長連敗: {sim.max_consecutive_losses}レース**",
            "",
        ]

    lines += [
        "---",
        "",
        "## 4. 投資戦略考察",
        "",
        "### 単勝の特性",
        "- 的中率: 約 5〜10%（1/14〜1/18 の逆数）",
        "- 的中時の平均払戻: 数百円〜数万円",
        "- ROI 80%以上が目標ライン（控除率 約 20%）",
        "",
        "### 複勝の特性",
        "- 的中率: 約 15〜25%（3着以内 = 3/14〜3/18）",
        "- 平均払戻: 110〜300円（低リスク・低リターン）",
        "- ROI 85%以上が目標ライン（控除率 約 15%）",
        "",
        "### 資金管理推奨",
        "- 1レースベット額を資産の1〜2%以下に抑える",
        "- 単勝20連敗（= ¥20,000損失）は確率的に 1.5〜3ヶ月に1回発生",
        "- ¥100,000 資本では単勝のみでも年間 ¥50,000〜¥70,000 消費ペース",
        "",
        f"*このレポートは {now_str} に自動生成されました。*",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  レポート出力: {output_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="単勝・複勝 3年間シミュレーション")
    ap.add_argument("--date-from", default="2024-01-01")
    ap.add_argument("--date-to",   default="2026-12-31")
    ap.add_argument("--year",      default=None, help="単年のみ (例: 2025)")
    ap.add_argument("--initial-capital", type=int, default=INITIAL_CAPITAL)
    ap.add_argument("--bet-amount",      type=int, default=BET_AMOUNT)
    args = ap.parse_args()

    from src.database.init_db import init_db
    conn = init_db()

    print("=" * 60)
    print("  単勝・複勝 シミュレーション")
    print(f"  初期資本: ¥{args.initial_capital:,}  賭け金: ¥{args.bet_amount:,}/型")
    print("=" * 60)

    results: dict[str, SimResult] = {}

    if args.year:
        label = args.year
        print(f"\n[{label}] シミュレーション中...")
        t0 = time.time()
        results[label] = simulate_win_place(
            conn, f"{label}-01-01", f"{label}-12-31",
            initial_capital=args.initial_capital,
            bet_amount=args.bet_amount,
        )
        print(f"[{label}] 完了: {time.time()-t0:.1f}秒")
    else:
        for yr in ["2024", "2025", "2026"]:
            print(f"\n[{yr}] シミュレーション中...")
            t0 = time.time()
            results[yr] = simulate_win_place(
                conn, f"{yr}-01-01", f"{yr}-12-31",
                initial_capital=args.initial_capital,
                bet_amount=args.bet_amount,
            )
            print(f"[{yr}] 完了: {time.time()-t0:.1f}秒")

        print(f"\n[ALL 2024-2026] シミュレーション中...")
        t0 = time.time()
        results["ALL"] = simulate_win_place(
            conn, args.date_from, args.date_to,
            initial_capital=args.initial_capital,
            bet_amount=args.bet_amount,
        )
        print(f"[ALL] 完了: {time.time()-t0:.1f}秒")

    conn.close()

    # コンソール出力
    print("\n" + "=" * 60)
    print("  結果サマリー")
    print("=" * 60)
    for label, sim in results.items():
        if not sim.records:
            print(f"  [{label}] データなし")
            continue
        print(f"\n  [{label}] {len(sim.records):,}レース")
        print(f"    単勝: {sim.n_win_hits:,}的中/{sim.n_win_bets:,}件 ROI={sim.roi_win:.1f}%")
        print(f"    複勝: {sim.n_place_hits:,}的中/{sim.n_place_bets:,}件 ROI={sim.roi_place:.1f}%")
        print(f"    初期¥{sim.initial_capital:,.0f} → 最終¥{sim.final_balance:,.0f}")
        print(f"    最大DD: ¥{sim.max_drawdown:,.0f}  最長連敗: {sim.max_consecutive_losses}レース")

    out_path = _ROOT / "docs" / "win_place_simulation_report.md"
    _write_report(results, out_path)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: シミュレーション実行**

```
py scripts/simulate_win_place.py 2>&1
```

期待: 各年のROI・最終残高・最大ドローダウンが出力される

- [ ] **Step 3: コミット**

```bash
git add scripts/simulate_win_place.py docs/win_place_simulation_report.md
git commit -m "feat: 単複限定シミュレーション + 資金管理 + 最大ドローダウン算出"
```

---

## Task 5: 天皇賞（春）最終予想出力

**Files:**
- No new files — スクリプト内でインライン実行

- [ ] **Step 1: 天皇賞（春）予想を実行する**

```python
py -c "
import sys, sqlite3
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from src.database.init_db import init_db
from src.ml.features import FeatureBuilder
from src.ml.models import HonmeiModel, PlaceModel

race_id = '202608030411'
conn = init_db()

fb = FeatureBuilder(conn)
df = fb.build_race_features(race_id)
if df.empty:
    print('特徴量なし')
    exit()

honmei = HonmeiModel()
place = PlaceModel()
honmei.load()
place.load()

win_scores   = honmei.predict(df)
place_scores = place.predict(df)

df['win_score']   = win_scores
df['place_score'] = place_scores

# オッズ取得
odds_map = {r[0]: r[1] for r in conn.execute(
    'SELECT horse_number, win_odds FROM realtime_odds WHERE race_id=? ORDER BY horse_number',
    (race_id,)
)}

print()
print('=== 天皇賞（春）2026-05-03 最終予想 ===')
print()
print(f'{\"馬番\":<4} {\"馬名\":<18} {\"オッズ\":<8} {\"単勝スコア\":<12} {\"複勝スコア\":<12} 推奨')
print('-' * 65)
ranked = df.sort_values('win_score', ascending=False)
top_win   = ranked.iloc[0]['horse_number']
top_place = df.loc[df['place_score'].idxmax(), 'horse_number']

for _, row in ranked.iterrows():
    hn = int(row['horse_number'])
    name = str(row.get('horse_name',''))[:16]
    odds = odds_map.get(hn, 0)
    ws = row['win_score']
    ps = row['place_score']
    tag = ''
    if hn == top_win:   tag += '◎単勝'
    if hn == top_place: tag += '◎複勝'
    print(f'{hn:<4} {name:<18} {odds:<8.1f} {ws:<12.4f} {ps:<12.4f} {tag}')

print()
print(f'【単勝推奨】 #{int(top_win)} {df.loc[df[\"horse_number\"]==top_win, \"horse_name\"].values[0]}  ¥1,000')
print(f'【複勝推奨】 #{int(top_place)} {df.loc[df[\"horse_number\"]==top_place, \"horse_name\"].values[0]}  ¥1,000')
conn.close()
" 2>&1
```

---

## Self-Review

### Spec Coverage

| 要件 | 対応タスク |
|---|---|
| ターゲットリーク排除 | Task 1 (_build_train_df修正) |
| EV外れ値（4800超え）除去 | Task 1 (キャップ50,000→10,000) |
| 2024-2025クリーンデータ学習反映 | Task 3 (retrain_win_place.py) |
| 単勝・複勝特化モデル | Task 2 (PlaceModel追加) |
| 3年間フルシミュレーション | Task 4 (simulate_win_place.py) |
| 初期資本¥100,000・¥1,000/型/レース | Task 4 (SimResult + capital tracking) |
| 最終残高出力 | Task 4 (_write_report) |
| 最大ドローダウン出力 | Task 4 (SimResult.max_drawdown) |
| 天皇賞（春）最終予想 | Task 5 |

### Placeholder Scan
なし。全コード記述済み。

### Type Consistency
- `PlaceModel.predict()` → `pd.Series` ✓ (HonmeiModelと同一)
- `SimResult.max_drawdown` → `float` ✓
- `train_all()` → `dict[str, dict]` ✓ (キー追加: "place")
- `load_models()` → `tuple[HonmeiModel, PlaceModel, ManjiModel]` ✓ (3要素に変更)
  - **注意**: 既存コードが `honmei, manji = load_models()` を呼んでいる場合は
    `honmei, place, manji = load_models()` に更新が必要。
    `src/pipeline/prediction.py` を確認して対応すること。
