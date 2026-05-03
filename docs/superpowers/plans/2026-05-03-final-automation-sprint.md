# 単複特化・完全自動化 E2E テスト 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Discord疎通確認・AUCリーク修正・単複モデル再学習・3年バックテスト・本日E2Eテストを完遂する

**Architecture:** 4フェーズ順次実行。Task1(Discord)→Task2(モデル修正)→Task3(再学習+シミュレーション)→Task4(本日E2E)。モデル再学習には15〜20分かかるため Task4 の直前にブロックされる。

**Tech Stack:** Python 3.11, LightGBM, SQLite, Next.js, Discord Webhook, python-dotenv, scikit-learn

---

## 事前確認（コードベース状況）

- `predictions` テーブル: 2026年分のみ 4,495件（2024-2025はバックテスト不可→モデル再実行で対応）
- `race_payouts` テーブル: 99,257件（単勝・複勝の払戻データあり）
- `race_results`: rank=1〜3 のデータが主体（rank=4〜 は少ない: データ不整合の可能性）
- 既存モデル: `honmei_model.pkl` / `manji_model.pkl` 保存済み
- `_get_today_bias()`: `r.race_number < ?` で正しくリーク排除済み
- `_get_horse_stats_bulk()`: `r.date < ?` で同日以前も除外済み（正しい実装）
- AUC 0.97 の原因: 特定の特徴量（`win_rate_all_rank`, `recent_rank_mean_rank`）がレース内順位と高相関の可能性

---

## ファイル変更マップ

| 操作 | ファイル | 内容 |
|------|----------|------|
| Modify | `src/ml/models.py` | ManjiModel.predict()出力クリップ + AUCサニティチェック |
| Modify | `src/ml/models.py` | FukushoModel（is_placed）追加 |
| Modify | `scripts/run_train.py` | FukushoModel学習を追加 |
| Create | `scripts/bankroll_sim_win_place.py` | 所持金推移シミュレーション |
| Modify | `scripts/verify_ui_sync.py` | フロントエンド健全性チェック強化 |

---

## Task 1: Discord疎通確認

**Files:**
- Execute: `scripts/test_notification.py`

- [ ] **Step 1: カスタムメッセージでDiscordに疎通テストを送信する**

```bash
py -c "
import sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from dotenv import load_dotenv
load_dotenv('.env', override=False)
from src.notification.discord_notifier import DiscordNotifier
n = DiscordNotifier()
n.send_text('🔔 UMALOGI システム起動・テスト通信\n─────────────────\n✅ Discord Webhook: 疎通確認済み\n✅ .env 読み込み: 正常\n🤖 完全自動運用モード 開始')
print('送信完了')
"
```

Expected: `送信完了` が表示され、Discord に緑色 embed が届く

- [ ] **Step 2: 結果を確認する**

Discordチャンネルでメッセージ受信を目視確認。失敗時は `DISCORD_WEBHOOK_URL` が `.env` に設定されているか確認。

---

## Task 2: AUC 0.97リーク診断・EV外れ値修正

**Files:**
- Modify: `src/ml/models.py:569-616` (ManjiModel.train, ManjiModel.predict)
- Modify: `src/ml/models.py:392-509` (HonmeiModel.train)

### 2a: ManjiModel.predict() に出力クリップを追加

- [ ] **Step 1: ManjiModel.predict()の最終行を修正してEV上限を設ける**

`src/ml/models.py` の `ManjiModel.predict()` 末尾（`return pd.Series(pred.clip(min=0), ...)`）を:
```python
_MAX_EV_PRED = 50_000.0   # 予測EV上限 (単勝500倍 × 100円)
return pd.Series(pred.clip(min=0, max=_MAX_EV_PRED), index=df.index, name="manji_score")
```
に変更する。

- [ ] **Step 2: AUCサニティチェックをHonmeiModel.train()に追加する**

`src/ml/models.py` の `HonmeiModel.train()` の CV AUC 計算後ブロック（`cv_auc_mean = ...` の直後）に追加:

```python
# AUCサニティチェック: 0.85超えはリーク疑いとして警告
_AUC_SUSPICIOUS = 0.85
if not math.isnan(cv_auc_mean) and cv_auc_mean > _AUC_SUSPICIOUS:
    logger.warning(
        "⚠️ CV AUC=%.4f > %.2f — ターゲットリークの可能性。特徴量を確認してください",
        cv_auc_mean, _AUC_SUSPICIOUS,
    )
    try:
        from sklearn.feature_selection import mutual_info_classif
        mi = mutual_info_classif(X_all, y_all, random_state=42)
        top5 = sorted(zip(FEATURE_COLS, mi), key=lambda t: t[1], reverse=True)[:5]
        logger.warning("【相互情報量 Top5】%s", top5)
    except Exception:
        pass
```

`import math` が models.py 先頭にあることを確認（なければ追加）。

- [ ] **Step 3: テストを実行して構文エラーがないことを確認**

```bash
py -m pytest tests/test_models.py -x -q 2>&1 | head -30
```

Expected: エラーなし（学習テストはスキップまたは通過）

---

## Task 3: FukushoModel追加 + 全モデル再学習

**Files:**
- Modify: `src/ml/models.py` (FukushoModel クラス追加、train_all() 更新)
- Modify: `scripts/run_train.py` (FukushoModel学習追加)

### 3a: FukushoModel（複勝: 3着以内確率）を追加

- [ ] **Step 1: FukushoModel クラスを src/ml/models.py に追加する**

`ManjiModel` クラスの直後（`# ── 学習エントリポイント` の前）に追加:

```python
# ── 複勝モデル ─────────────────────────────────────────────────────

class FukushoModel(_BaseModel):
    """
    複勝モデル（3着以内確率特化）。

    LightGBM 2値分類 + Isotonic Regression（OOF キャリブレーション）で
    各馬の 3着以内確率 P(rank<=3) を推定する。
    複勝馬券の期待値: EV = P(place) × fukusho_odds - 1.0 で判断する。
    """

    _filename = "fukusho_model"

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
        """is_placed (3着以内=1) を目的変数として訓練する。"""
        df = _build_train_df(conn, train_until=train_until)
        if df.empty:
            logger.warning("学習データが0件のため複勝モデル訓練をスキップします")
            return {"n_races": 0, "n_samples": 0, "cv_auc_mean": float("nan")}

        n_races = df["race_id"].nunique()
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
            "複勝モデル訓練完了: %d レース / %d サンプル / CV AUC %.4f",
            n_races, len(df), cv_auc_mean,
        )
        return {"n_races": n_races, "n_samples": len(df), "cv_auc_mean": cv_auc_mean}

    def predict(self, df: pd.DataFrame) -> pd.Series:
        """各馬の 3着以内確率を返す。"""
        if not self._trained:
            logger.debug("未訓練複勝モデル — フォールバック予測を使用")
            return self._fallback_predict(df)
        X = df[FEATURE_COLS].astype(float).fillna(-1)
        proba = self._model.predict_proba(X)[:, 1]
        return pd.Series(proba, index=df.index, name="fukusho_score")
```

- [ ] **Step 2: train_all() に FukushoModel を追加する**

`src/ml/models.py` の `train_all()` 関数を修正して `FukushoModel` の学習・保存を追加:

```python
def train_all(
    conn: sqlite3.Connection,
    train_until: int | None = None,
) -> dict[str, dict]:
    honmei  = HonmeiModel()
    manji   = ManjiModel()
    fukusho = FukushoModel()

    h_result = honmei.train(conn, train_until=train_until)
    m_result = manji.train(conn, train_until=train_until)
    f_result = fukusho.train(conn, train_until=train_until)

    # 本命モデル: Champion/Challenger 判定（変更なし）
    if honmei.is_trained:
        challenger_auc: float = h_result.get("challenger_auc", float("nan"))
        champion_auc:   float = h_result.get("champion_auc",   float("nan"))
        if np.isnan(champion_auc) or np.isnan(challenger_auc):
            honmei.save()
            h_result["promoted"] = True
        elif challenger_auc >= champion_auc - 0.005:
            honmei.save()
            h_result["promoted"] = True
            logger.info("世代交代: challenger AUC=%.4f >= champion AUC=%.4f", challenger_auc, champion_auc)
        else:
            h_result["promoted"] = False
            logger.warning("世代交代却下: challenger=%.4f < champion=%.4f", challenger_auc, champion_auc)

    if manji.is_trained:
        manji.save()

    if fukusho.is_trained:
        fukusho.save()

    clear_model_cache()
    return {"honmei": h_result, "manji": m_result, "fukusho": f_result}
```

- [ ] **Step 3: load_models() に FukushoModel を追加する**

`load_models()` の型と実装を更新:

```python
_MODEL_CACHE: dict[str, "tuple[HonmeiModel, ManjiModel, FukushoModel]"] = {}

def load_models() -> tuple[HonmeiModel, ManjiModel, FukushoModel]:
    cache_key = str(_MODEL_DIR)
    if cache_key in _MODEL_CACHE:
        return _MODEL_CACHE[cache_key]

    honmei  = HonmeiModel()
    manji   = ManjiModel()
    fukusho = FukushoModel()

    try:
        honmei.load()
    except FileNotFoundError:
        logger.info("本命モデルが見つかりません — フォールバックモードで動作します")
    try:
        manji.load()
    except FileNotFoundError:
        logger.info("卍モデルが見つかりません — フォールバックモードで動作します")
    try:
        fukusho.load()
    except FileNotFoundError:
        logger.info("複勝モデルが見つかりません — フォールバックモードで動作します")

    _MODEL_CACHE[cache_key] = (honmei, manji, fukusho)
    return honmei, manji, fukusho
```

- [ ] **Step 4: scripts/run_train.py に FukushoModel 学習を追加する**

`_train_manji()` の後に以下を追加:

```python
def _train_fukusho(
    conn,
    model_dir: Path,
    train_until: int | None = None,
) -> tuple["FukushoModel", dict]:
    from src.ml.models import FukushoModel
    print(f"\n[3/3] 複勝モデル (FukushoModel) を学習中 ...")
    t0 = time.perf_counter()
    fukusho = FukushoModel()
    result  = fukusho.train(conn, train_until=train_until)
    elapsed = time.perf_counter() - t0
    if result["n_races"] == 0:
        print("  [!] 学習データが 0 件のためスキップしました。")
        return fukusho, result
    cv_mean = result.get("cv_auc_mean", float("nan"))
    _kv_table(
        [
            ("学習レース数",   f"{result['n_races']:,}"),
            ("学習サンプル数", f"{result['n_samples']:,}"),
            ("CV AUC (mean)", f"{cv_mean:.4f}" if not math.isnan(cv_mean) else "N/A"),
        ],
        title="複勝モデル学習結果",
    )
    save_path = fukusho.save(model_dir / "fukusho_model.pkl")
    print(f"\n  [OK] 保存完了: {save_path}  ({elapsed:.1f}s)")
    return fukusho, result
```

`main()` 内の卍モデル学習直後に呼び出しを追加:
```python
_, f_result = _train_fukusho(conn, model_dir, train_until=args.train_until)
```

- [ ] **Step 5: 全モデルを再学習する（15〜20分かかる）**

```bash
py scripts/run_train.py 2>&1 | tee logs/retrain_20260503.log
```

Expected:
- 本命モデル CV AUC: 0.60〜0.80 台（0.85超えは警告が出る）
- 複勝モデル CV AUC: 0.70〜0.85 台（複勝は的中率高いため少し高め）
- 卍モデル: 学習完了
- `data/models/honmei_model.pkl`, `manji_model.pkl`, `fukusho_model.pkl` が更新される

- [ ] **Step 6: prediction pipeline の load_models() 呼び出しを 3戻り値に修正する**

`src/pipeline/prediction.py` の `prerace_pipeline()` 内:
```python
# 変更前:
honmei_model, manji_model = load_models()
honmei_scores = honmei_model.predict(df)

# 変更後:
honmei_model, manji_model, fukusho_model = load_models()
honmei_scores    = honmei_model.predict(df)
fukusho_scores   = fukusho_model.predict(df)
honmei_ev_scores = honmei_model.ev_predict(df)
ev_scores        = manji_model.ev_score(df)
```

- [ ] **Step 7: コミット**

```bash
git add src/ml/models.py scripts/run_train.py src/pipeline/prediction.py
git commit -m "feat: FukushoModel追加・AUCサニティチェック・EV上限クリップ"
```

---

## Task 4: 所持金推移シミュレーション（2024-2026年）

**Files:**
- Create: `scripts/bankroll_sim_win_place.py`

- [ ] **Step 1: スクリプトを作成する**

`scripts/bankroll_sim_win_place.py` を以下の内容で作成:

```python
"""
所持金推移シミュレーション（単勝・複勝特化）

初期資金 100,000円 / 1R×1券種 固定 1,000円 賭けで
卍・本命モデルの単勝・複勝に絞った 2024-2026 年バックテストを実行する。

使用例:
    py scripts/bankroll_sim_win_place.py
    py scripts/bankroll_sim_win_place.py --year-from 2024 --year-to 2026
    py scripts/bankroll_sim_win_place.py --ev-threshold 1.1
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

import sqlite3
import numpy as np
import pandas as pd
from src.database.init_db import init_db
from src.ml.features import FeatureBuilder
from src.ml.models import HonmeiModel, ManjiModel, FukushoModel, _MODEL_DIR, FEATURE_COLS

_INITIAL_BANKROLL = 100_000.0
_BET_AMOUNT       = 1_000.0      # 1R×1券種あたり固定賭け金
_EV_THRESHOLD     = 1.1          # 買い推奨の EV 閾値


def _load_payouts(conn: sqlite3.Connection) -> dict[tuple[str, str], float]:
    """race_id × bet_type='単勝' | '複勝' の払戻を辞書で返す。"""
    rows = conn.execute(
        """
        SELECT race_id, bet_type, combination, payout
        FROM race_payouts
        WHERE bet_type IN ('単勝', '複勝')
        """
    ).fetchall()
    result: dict[tuple[str, str, str], float] = {}
    for race_id, bet_type, combination, payout in rows:
        result[(race_id, bet_type, combination)] = float(payout or 0)
    return result


def _simulate(
    conn: sqlite3.Connection,
    honmei: HonmeiModel,
    manji: ManjiModel,
    fukusho: FukushoModel,
    year_from: int,
    year_to: int,
    ev_threshold: float,
) -> dict:
    fb = FeatureBuilder(conn)
    payouts = _load_payouts(conn)

    race_rows = conn.execute(
        """
        SELECT DISTINCT r.race_id, r.date
        FROM races r
        JOIN race_results rr ON rr.race_id = r.race_id
        WHERE rr.rank IS NOT NULL
          AND CAST(substr(r.date, 1, 4) AS INTEGER) BETWEEN ? AND ?
        ORDER BY r.date, r.race_id
        """,
        (year_from, year_to),
    ).fetchall()

    bankroll = _INITIAL_BANKROLL
    peak     = _INITIAL_BANKROLL
    max_dd   = 0.0
    balance_history: list[tuple[str, float]] = []
    n_bets = n_hits = 0
    total_invested = 0.0
    total_payout   = 0.0

    for race_id, race_date in race_rows:
        try:
            df = fb.build_race_features_for_simulate(race_id)
        except Exception:
            continue
        if df.empty:
            continue

        # 本命モデル: 単勝 EV スコア
        h_scores   = honmei.predict(df)
        win_odds   = df["win_odds"].fillna(0.0).astype(float)
        h_ev       = (h_scores * win_odds).rename("honmei_ev")

        # 複勝モデル: 複勝 EV スコア（複勝払戻はオッズ記録がないため暫定 2.0 倍想定）
        f_scores   = fukusho.predict(df)
        # 実データで複勝オッズが race_results に記録されていないためフォールバック
        fuku_est_odds = win_odds.apply(lambda o: max(1.1, min(10.0, o * 0.4)) if o > 0 else 0)
        f_ev       = (f_scores * fuku_est_odds).rename("fukusho_ev")

        # 卍モデル: 単勝 EV
        m_ev = manji.ev_score(df).rename("manji_ev")

        # 当レースの着順マッピング（horse_number→rank）
        actual = conn.execute(
            "SELECT horse_number, rank FROM race_results WHERE race_id = ? AND rank IS NOT NULL",
            (race_id,),
        ).fetchall()
        rank_map = {str(hn): r for hn, r in actual}

        # popularity → horse_number 対応（simulate では sim_num=popularity順の連番）
        pop_order = df.reset_index(drop=True)

        for model_name, ev_series in [
            ("honmei_win", h_ev),
            ("fukusho",    f_ev),
            ("manji_win",  m_ev),
        ]:
            if ev_series.empty:
                continue
            best_idx = ev_series.idxmax()
            ev_val   = ev_series[best_idx]
            if ev_val < ev_threshold:
                continue
            if bankroll < _BET_AMOUNT:
                break  # 資金枯渇

            # 馬番の特定（sim_num = popularity順の1始まり整数インデックス）
            sim_num = int(df.iloc[best_idx]["horse_number"]) if "horse_number" in df.columns else (best_idx + 1)
            combo_key = str(sim_num)

            if "win" in model_name:
                bet_type  = "単勝"
                payout = payouts.get((race_id, "単勝", combo_key), 0.0)
                is_hit = (rank_map.get(combo_key) == 1)
            else:
                bet_type  = "複勝"
                payout = payouts.get((race_id, "複勝", combo_key), 0.0)
                is_hit = (int(rank_map.get(combo_key) or 99) <= 3)

            return_amount = (payout / 100 * _BET_AMOUNT) if is_hit and payout > 0 else 0.0

            bankroll      -= _BET_AMOUNT
            bankroll      += return_amount
            total_invested += _BET_AMOUNT
            total_payout   += return_amount
            n_bets         += 1
            n_hits         += int(is_hit)

        # ドローダウン更新
        peak   = max(peak, bankroll)
        dd     = peak - bankroll
        max_dd = max(max_dd, dd)
        balance_history.append((race_date, bankroll))

    roi = total_payout / total_invested * 100 if total_invested > 0 else 0.0
    hit_rate = n_hits / n_bets * 100 if n_bets > 0 else 0.0

    return {
        "initial_bankroll": _INITIAL_BANKROLL,
        "final_bankroll":   bankroll,
        "profit":           bankroll - _INITIAL_BANKROLL,
        "roi":              roi,
        "max_drawdown":     max_dd,
        "n_bets":           n_bets,
        "n_hits":           n_hits,
        "hit_rate":         hit_rate,
        "total_invested":   total_invested,
        "total_payout":     total_payout,
        "balance_history":  balance_history,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year-from", type=int, default=2024)
    parser.add_argument("--year-to",   type=int, default=2026)
    parser.add_argument("--ev-threshold", type=float, default=_EV_THRESHOLD)
    args = parser.parse_args()

    conn    = init_db()
    honmei  = HonmeiModel();  honmei.load()
    manji   = ManjiModel();   manji.load()
    try:
        fukusho = FukushoModel(); fukusho.load()
    except FileNotFoundError:
        print("⚠️  fukusho_model.pkl が見つかりません — 先に run_train.py を実行してください")
        fukusho = FukushoModel()  # フォールバックで続行

    print(f"\n=== 所持金推移シミュレーション {args.year_from}-{args.year_to} ===")
    print(f"初期資金: ¥{_INITIAL_BANKROLL:,.0f}  固定賭け金: ¥{_BET_AMOUNT:,.0f}  EV閾値: {args.ev_threshold}")
    print("計算中...")

    res = _simulate(conn, honmei, manji, fukusho, args.year_from, args.year_to, args.ev_threshold)
    conn.close()

    print(f"\n── 結果サマリー ──────────────────────────────")
    print(f"最終残高      : ¥{res['final_bankroll']:>12,.0f}  (損益: {res['profit']:+,.0f})")
    print(f"最大ドローダウン: ¥{res['max_drawdown']:>12,.0f}")
    print(f"回収率        : {res['roi']:.1f}%")
    print(f"賭け回数      : {res['n_bets']:,}回  的中: {res['n_hits']:,}回  的中率: {res['hit_rate']:.1f}%")
    print(f"総投資額      : ¥{res['total_invested']:>12,.0f}")
    print(f"総払戻額      : ¥{res['total_payout']:>12,.0f}")

    # 月次残高グラフ（ASCII）
    if res['balance_history']:
        history_df = pd.DataFrame(res['balance_history'], columns=["date", "balance"])
        history_df["ym"] = history_df["date"].str[:7]
        monthly = history_df.groupby("ym")["balance"].last()
        print(f"\n── 月次残高推移 ──")
        _MAX_BAR = 30
        max_b = max(monthly.max(), _INITIAL_BANKROLL)
        for ym, bal in monthly.items():
            bar_len = max(0, int(bal / max_b * _MAX_BAR))
            bar = "#" * bar_len
            mark = "▲" if bal > _INITIAL_BANKROLL else ("▼" if bal < _INITIAL_BANKROLL else "─")
            print(f"  {ym}: {bar:<{_MAX_BAR}} ¥{bal:>10,.0f} {mark}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: スクリプトを実行する（モデル再学習完了後）**

```bash
py scripts/bankroll_sim_win_place.py --year-from 2024 --year-to 2026 2>&1
```

Expected:
- 最終残高・最大ドローダウン・月次残高推移が表示される
- 回収率が 80〜120% 程度（データ質・EV閾値によって変動）

- [ ] **Step 3: コミット**

```bash
git add scripts/bankroll_sim_win_place.py
git commit -m "feat: 単複特化 所持金推移シミュレーション (2024-2026)"
```

---

## Task 5: 本日（5/3）暫定予想生成・Discord通知

**Files:**
- Execute: `scripts/force_provisional_today.py` (または inline スクリプト)

- [ ] **Step 1: 2026-05-03 の全レースに対して暫定予想を実行する**

```bash
py -c "
import sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from dotenv import load_dotenv
load_dotenv('.env', override=False)

from src.pipeline.prediction import provisional_batch
results = provisional_batch('20260503')
print(f'暫定予想完了: {len(results)} レース')
for r in results[:5]:
    print(' -', r)
"
```

Expected: `暫定予想完了: XX レース` (最大36レース)

- [ ] **Step 2: Discord に暫定予想完了通知を送る**

```bash
py -c "
import sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from dotenv import load_dotenv
load_dotenv('.env', override=False)
from src.notification.discord_notifier import DiscordNotifier
n = DiscordNotifier()
n.send_text('📋 [UMALOGI] 2026-05-03 暫定予想 完了\n本日の全レースに対し暫定予想を生成しました。\n(テスト送信 — モデル: 本命+複勝+卍)')
"
```

---

## Task 6: 直前予想生成・Discord通知

- [ ] **Step 1: 本日1レース目を対象に直前予想を実行する**

```bash
py -c "
import sys, sqlite3
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from dotenv import load_dotenv
load_dotenv('.env', override=False)

conn = sqlite3.connect('data/umalogi.db')
race = conn.execute(\"SELECT race_id FROM races WHERE date='2026-05-03' ORDER BY race_id LIMIT 1\").fetchone()
conn.close()

if race:
    race_id = race[0]
    print(f'対象レース: {race_id}')
    from src.pipeline.prediction import prerace_pipeline
    result = prerace_pipeline(race_id, provisional=False)
    print('直前予想:', 'skipped' if result.get('skipped') else 'done')
else:
    print('本日のレースが見つかりません')
"
```

Expected: 直前予想完了 + Discordに自動送信（prerace_pipeline内部で呼び出される）

---

## Task 7: UI JSON更新・フロントエンド健全性確認

**Files:**
- Execute: `web/generate_data.py`
- Check: `web/src/data/races.json`, `web/src/data/predictions.json`, `web/src/data/summary.json`

- [ ] **Step 1: UI JSONを更新する**

```bash
py web/generate_data.py 2>&1 | tail -20
```

Expected: エラーなし、`web/src/data/*.json` が更新される

- [ ] **Step 2: 健全性チェック（文字化け・99バグ確認）**

```bash
py -c "
import json, sys
from pathlib import Path

def check_json(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    print(f'✅ {path.name}: OK')

    # 99バグ: popularity=99 の異常値チェック
    if isinstance(data, list):
        for item in data[:3]:
            pop = item.get('popularity') or (item.get('results') or [{}])[0].get('popularity')
            if pop == 99:
                print(f'  ⚠️ popularity=99 バグ検出: {item.get(\"race_id\")}')
    return True

data_dir = Path('web/src/data')
for f in data_dir.glob('*.json'):
    try:
        check_json(f)
    except Exception as e:
        print(f'❌ {f.name}: {e}')
"
```

Expected: 全JSONファイルが `✅ OK`、99バグなし

- [ ] **Step 3: コミット**

```bash
git add web/src/data/
git commit -m "data: 2026-05-03 UI JSON更新"
```

---

## 実行順序（依存関係）

```
Task 1 (Discord疎通)    → 即時実行可
Task 2 (モデル修正)     → 即時実行可
Task 3 (再学習)         → Task 2 完了後（15〜20分）
Task 4 (バックテスト)   → Task 3 完了後
Task 5 (暫定予想)       → Task 3 完了後
Task 6 (直前予想)       → Task 5 完了後
Task 7 (UI更新)         → Task 5・6 完了後
```

## 成功条件

| チェック | 期待値 |
|---------|--------|
| Discord疎通 | 「🔔 UMALOGI システム起動・テスト通信」がDiscordに届く |
| HonmeiModel CV AUC | 0.60〜0.80（0.85超は要調査） |
| FukushoModel CV AUC | 0.65〜0.85 |
| ManjiModel 最大EV | <= 500（クリップ確認） |
| バックテスト最終残高 | > 0（資金枯渇しない） |
| 暫定予想数 | > 0 レース |
| UI JSON | 文字化けなし・99バグなし |
