# ROI改善＆監視強化 実装プラン

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 本番ROI 71.8%（損失運用）を100%超に引き上げるため、卍モデルの三連複EVゲート復活・フラットベット評価追加・券種セグメント分析の3軸で改善する

**Architecture:** (1) `src/ml/bet_generator.py` の ManjiGenerator に EV ≥ 1.0 ゲートを追加してロスカット。(2) `scripts/backtest_2024_2025.py` に `--mode flat` フラグを追加して Kelly 複利膨張のない真のバックテスト数字を出力。(3) `scripts/segment_analysis.py` 新設で venue/surface/condition 別 ROI スライスを生成し弱点を特定。

**Tech Stack:** Python 3.11+, SQLite (`data/umalogi.db`), pytest, pandas

---

## ファイルマップ

| ファイル | 操作 | 担当 |
|---|---|---|
| `src/ml/bet_generator.py:940-987` | Modify — ManjiGenerator 三連複 EV ゲート追加 | Task 1 |
| `tests/test_bet_generator_ev_gate.py` | Create — EV ゲートのユニットテスト | Task 1 |
| `scripts/backtest_2024_2025.py` | Modify — `--mode flat` フラグ追加 | Task 2 |
| `tests/test_backtest_flat_mode.py` | Create — フラットベットモードのテスト | Task 2 |
| `scripts/segment_analysis.py` | Create — 券種×会場×馬場状態 ROI スライス | Task 3 |
| `tests/test_segment_analysis.py` | Create — セグメント分析のテスト | Task 3 |
| `scripts/scheduler.py` | Modify — 月曜07:30 自動実行ジョブ追加 | Task 4 |
| `docs/7_weakness_ledger.md` | Modify — W-038 〜 W-040 追加・既存更新 | Task 4 |

---

## Task 1: 卍モデルの三連複 EV ゲート復活

**背景:** 現在の ManjiGenerator は `# EVゲート撤廃 → 上位5頭から三連複組み合わせを探索` というコメントのもと、EV < 1.0 の三連複も推奨している。本番実績で 卍×三連複 ROI = 46.7%（損失確定）であり、EV ≥ 1.0 フィルターで損失組み合わせを除外する。

**Files:**
- Modify: `src/ml/bet_generator.py:970-986`
- Create: `tests/test_bet_generator_ev_gate.py`

- [ ] **Step 1: テストを書く（失敗確認）**

`tests/test_bet_generator_ev_gate.py` を作成する:

```python
"""ManjiGenerator の三連複 EV ゲートテスト。"""
from __future__ import annotations
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd


def _make_scored(n: int = 5) -> pd.DataFrame:
    """ダミー scored DataFrame (n頭)。"""
    return pd.DataFrame({
        "horse_number": list(range(1, n + 1)),
        "ev_score":     [1.5, 1.2, 0.9, 0.7, 0.5],
        "win_odds":     [3.0, 5.0, 8.0, 12.0, 20.0],
        "horse_name":   [f"馬{i}" for i in range(1, n + 1)],
    })


class TestManjiTrioEvGate:
    def _run_manji(self, scored: pd.DataFrame) -> list:
        """ManjiGenerator の generate() を呼び出して三連複だけ返す。"""
        from src.ml.bet_generator import ManjiGenerator, EVEstimator

        estimator = EVEstimator()
        gen = ManjiGenerator(estimator=estimator)

        # generate() に必要な最小引数を構築
        race_id = "202601010101"
        bankroll = 100_000.0
        all_nums = scored["horse_number"].tolist()
        all_scores = scored["ev_score"].tolist()

        with patch.object(gen, "_apply_roi_filter", side_effect=lambda b: b):
            result = gen.generate(
                race_id=race_id,
                scored=scored,
                bankroll=bankroll,
            )
        return [b for b in result.bets if b.bet_type == "三連複"]

    def test_ev_below_threshold_is_excluded(self) -> None:
        """EV < 1.0 の三連複は推奨しない。"""
        # 全馬のEVスコアを低く設定 → 合成EVが必ず1.0未満になる
        scored = pd.DataFrame({
            "horse_number": [1, 2, 3, 4, 5],
            "ev_score":     [0.5, 0.4, 0.3, 0.2, 0.1],
            "win_odds":     [3.0, 5.0, 8.0, 12.0, 20.0],
            "horse_name":   [f"馬{i}" for i in range(1, 6)],
        })
        trio_bets = self._run_manji(scored)
        assert len(trio_bets) == 0, f"EV<1.0なのに{len(trio_bets)}件の三連複が生成された"

    def test_ev_above_threshold_is_included(self) -> None:
        """EV ≥ 1.0 の三連複は推奨する。"""
        # 高EVスコアで合成EVが1.0超になる設定
        scored = pd.DataFrame({
            "horse_number": [1, 2, 3],
            "ev_score":     [2.0, 1.8, 1.5],
            "win_odds":     [3.0, 4.0, 5.0],
            "horse_name":   ["ウマA", "ウマB", "ウマC"],
        })
        trio_bets = self._run_manji(scored)
        assert len(trio_bets) > 0, "EV≥1.0なのに三連複が0件"
        for b in trio_bets:
            assert b.expected_value >= 1.0, f"EV={b.expected_value:.3f} < 1.0 が漏れた"
```

- [ ] **Step 2: テストが失敗することを確認**

```bash
cd C:/dev/horse-racing-ai
pytest tests/test_bet_generator_ev_gate.py -v 2>&1 | tail -20
```

Expected: FAIL（現在の ManjiGenerator は EV ゲートなしなので `test_ev_below_threshold_is_excluded` が失敗するか、インターフェース不一致でエラー）

- [ ] **Step 3: ManjiGenerator に EV ゲートを追加**

`src/ml/bet_generator.py` の三連複生成ループ（`for ev_c, tp, combo3, hnames3 in trio_candidates[:_MAX_SANREN]:`）の直前に EV チェックを追加する:

変更前 (`src/ml/bet_generator.py` L970-986):
```python
            seen_combos: set[tuple] = set()
            for ev_c, tp, combo3, hnames3 in trio_candidates[:_MAX_SANREN]:
                if combo3 in seen_combos:
                    continue
                seen_combos.add(combo3)
                result.bets.append(BetRecommendation(
                    bet_type="三連複",
```

変更後:
```python
            _TRIO_EV_MIN = 1.0  # EVゲート: 期待値1.0未満の三連複は推奨しない
            seen_combos: set[tuple] = set()
            for ev_c, tp, combo3, hnames3 in trio_candidates[:_MAX_SANREN]:
                if ev_c < _TRIO_EV_MIN:
                    continue
                if combo3 in seen_combos:
                    continue
                seen_combos.add(combo3)
                result.bets.append(BetRecommendation(
                    bet_type="三連複",
```

またコメントも更新する（`src/ml/bet_generator.py` L941-942）:

変更前:
```python
        # 確率至上主義: EVゲート撤廃 → 上位5頭から三連複組み合わせを探索
```

変更後:
```python
        # EV ≥ 1.0 ゲート付き三連複: 上位5頭から Harville 確率最大の組み合わせを探索
```

- [ ] **Step 4: テストが通ることを確認**

```bash
pytest tests/test_bet_generator_ev_gate.py -v
```

Expected: PASS（2テスト）

- [ ] **Step 5: 既存テスト一括確認**

```bash
pytest tests/ -x -q 2>&1 | tail -20
```

Expected: 全テスト PASS（466件以上）

- [ ] **Step 6: コミット**

```bash
git add src/ml/bet_generator.py tests/test_bet_generator_ev_gate.py
git commit -m "fix: 卍モデル三連複にEV>=1.0ゲートを復活（本番ROI46.7%→改善目標）"
```

---

## Task 2: バックテストにフラットベット評価モードを追加

**背景:** 現在の WF バックテストは Kelly 複利で 2025-08 以降 ROI 1000〜15000% に膨張し、実際の実力評価に使えない。`--mode flat` で 1 ベットあたり ¥100 固定・複利なしの真の ROI を算出するモードを追加する。

**Files:**
- Modify: `scripts/backtest_2024_2025.py`（`--mode` 引数追加、フラットベット集計ロジック追加）
- Create: `tests/test_backtest_flat_mode.py`

- [ ] **Step 1: テストを書く**

`tests/test_backtest_flat_mode.py` を作成する:

```python
"""backtest_2024_2025.py の --mode flat フラグテスト。"""
from __future__ import annotations
import subprocess
import sys
import pytest


class TestFlatBetMode:
    def test_flat_mode_flag_accepted(self) -> None:
        """--mode flat が受け付けられ、エラーなく --help が出る。"""
        result = subprocess.run(
            [sys.executable, "scripts/backtest_2024_2025.py", "--help"],
            capture_output=True, text=True, encoding="utf-8"
        )
        assert "--mode" in result.stdout, f"--mode フラグが見当たらない: {result.stdout}"
        assert "flat" in result.stdout, f"flat オプションが見当たらない: {result.stdout}"

    def test_flat_roi_calculation(self) -> None:
        """フラットベット ROI 計算のユニットテスト。"""
        from scripts.backtest_2024_2025 import calc_flat_roi

        bets = [
            {"is_hit": 1, "payout": 350, "invest": 100},
            {"is_hit": 0, "payout": 0,   "invest": 100},
            {"is_hit": 1, "payout": 500, "invest": 100},
            {"is_hit": 0, "payout": 0,   "invest": 100},
        ]
        roi = calc_flat_roi(bets)
        # 総投資 400, 総回収 850 → ROI = 850/400 * 100 = 212.5%
        assert abs(roi - 212.5) < 0.1, f"ROI={roi:.1f}% (expected 212.5%)"
```

- [ ] **Step 2: テストが失敗することを確認**

```bash
pytest tests/test_backtest_flat_mode.py -v 2>&1 | tail -10
```

Expected: FAIL（`--mode` フラグ未実装 / `calc_flat_roi` 未定義）

- [ ] **Step 3: `scripts/backtest_2024_2025.py` に `--mode` フラグと `calc_flat_roi` を追加**

`scripts/backtest_2024_2025.py` のインポート後・既存 `argparse` 設定の近くに追記する:

まず `calc_flat_roi()` 関数をファイル末尾（`if __name__ == "__main__":` の直前）に追加:

```python
def calc_flat_roi(bets: list[dict]) -> float:
    """フラットベット（全ベット¥100固定）での ROI% を計算する。

    Args:
        bets: {"is_hit": int, "payout": int, "invest": int} のリスト

    Returns:
        ROI パーセント（0除算時は 0.0）
    """
    total_invest = sum(b["invest"] for b in bets) or 0
    total_payout = sum(b["payout"] for b in bets)
    if total_invest == 0:
        return 0.0
    return round(total_payout / total_invest * 100, 1)
```

次に `argparse.ArgumentParser` の `add_argument` 呼び出し部分に追加:

```python
    parser.add_argument(
        "--mode",
        choices=["kelly", "flat"],
        default="kelly",
        help="backtest mode: 'kelly' (default, compound Kelly) or 'flat' (¥100 fixed bet)",
    )
```

`main()` 関数内で `args.mode` を受け取り、フラットベットモードでは Kelly 複利を使わず `calc_flat_roi()` を呼ぶ分岐を追加:

```python
    if args.mode == "flat":
        logger.info("フラットベットモード: Kelly複利を無効化して集計します")
        # prediction_results から直接フラットROIを計算
        import sqlite3
        conn = sqlite3.connect("data/umalogi.db")
        bets_raw = conn.execute("""
            SELECT pr.is_hit,
                   COALESCE(pr.payout, 0) as payout,
                   100 as invest
            FROM prediction_results pr
            JOIN predictions p ON pr.prediction_id = p.id
            JOIN races r ON r.race_id = p.race_id
            WHERE r.date >= ? AND r.date <= ?
        """, (f"{args.year or '2024'}-01-01", f"{args.year or '2026'}-12-31")).fetchall()
        conn.close()
        bets = [{"is_hit": b[0], "payout": b[1], "invest": b[2]} for b in bets_raw]
        roi = calc_flat_roi(bets)
        hits = sum(b["is_hit"] for b in bets)
        logger.info(
            "フラットベット ROI=%.1f%% / 的中率=%.1f%% / ベット数=%d",
            roi, hits / len(bets) * 100 if bets else 0, len(bets)
        )
        return
```

- [ ] **Step 4: テストが通ることを確認**

```bash
pytest tests/test_backtest_flat_mode.py -v
```

Expected: PASS（2テスト）

- [ ] **Step 5: 動作確認（フラットベット実行）**

```bash
py scripts/backtest_2024_2025.py --mode flat 2>&1 | tail -5
```

Expected（例）:
```
フラットベット ROI=71.8% / 的中率=13.5% / ベット数=11710
```

- [ ] **Step 6: コミット**

```bash
git add scripts/backtest_2024_2025.py tests/test_backtest_flat_mode.py
git commit -m "feat: バックテストに--mode flatフラグ追加（Kelly複利膨張なしの真のROI評価）"
```

---

## Task 3: 券種×会場×馬場状態 セグメント分析スクリプト新設

**背景:** 現在のバックテストは半期単位の集計のみで、どの会場・馬場・券種でROIが低いか分からない。`scripts/segment_analysis.py` を新設して ROI の弱点を特定し、`_ALLOWED_BET_TYPES` の見直し根拠を数字で提供する。

**Files:**
- Create: `scripts/segment_analysis.py`
- Create: `tests/test_segment_analysis.py`

- [ ] **Step 1: テストを書く**

`tests/test_segment_analysis.py` を作成する:

```python
"""segment_analysis.py のテスト。"""
from __future__ import annotations
import sqlite3
import tempfile
import pytest
from pathlib import Path


def _make_test_db(path: str) -> None:
    """テスト用の最小DBを作成する。"""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY, race_name TEXT, date TEXT,
            venue TEXT, distance INTEGER, surface TEXT,
            weather TEXT, condition TEXT, created_at TEXT, track_direction TEXT
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY, race_id TEXT, model_type TEXT,
            bet_type TEXT, confidence REAL, expected_value REAL,
            recommended_bet INTEGER, notes TEXT, combination_json TEXT, created_at TEXT
        );
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY, prediction_id INTEGER,
            is_hit INTEGER, payout INTEGER, profit INTEGER, roi REAL, recorded_at TEXT
        );

        INSERT INTO races VALUES ('20260601010101','R1','2026-06-01','東京',1600,'芝','晴','良','2026-06-01',NULL);
        INSERT INTO races VALUES ('20260601010102','R2','2026-06-01','東京',1600,'芝','晴','稍重','2026-06-01',NULL);

        INSERT INTO predictions VALUES (1,'20260601010101','本命(直前)','単勝',0.8,1.5,1000,NULL,NULL,'2026-06-01');
        INSERT INTO predictions VALUES (2,'20260601010101','本命(直前)','複勝',0.7,1.3,800,NULL,NULL,'2026-06-01');
        INSERT INTO predictions VALUES (3,'20260601010102','卍(直前)','三連複',0.5,0.9,600,NULL,NULL,'2026-06-01');

        INSERT INTO prediction_results VALUES (1,1,1,3000,2000,300.0,'2026-06-01');
        INSERT INTO prediction_results VALUES (2,2,0,0,-800,0.0,'2026-06-01');
        INSERT INTO prediction_results VALUES (3,3,0,0,-600,0.0,'2026-06-01');
    """)
    conn.close()


class TestSegmentAnalysis:
    def test_by_bet_type(self, tmp_path: Path) -> None:
        """券種別 ROI が正しく計算される。"""
        db = str(tmp_path / "test.db")
        _make_test_db(db)
        from scripts.segment_analysis import analyze_by_segment
        rows = analyze_by_segment(db, group_by="bet_type")
        bet_types = {r["segment"]: r for r in rows}
        assert "単勝" in bet_types
        # 単勝: 投資100 → 回収3000 → ROI 3000%
        assert abs(bet_types["単勝"]["roi"] - 3000.0) < 1.0

    def test_by_venue(self, tmp_path: Path) -> None:
        """会場別 ROI が出力される。"""
        db = str(tmp_path / "test.db")
        _make_test_db(db)
        from scripts.segment_analysis import analyze_by_segment
        rows = analyze_by_segment(db, group_by="venue")
        assert len(rows) > 0
        assert all("roi" in r and "n_bets" in r and "segment" in r for r in rows)

    def test_by_condition(self, tmp_path: Path) -> None:
        """馬場状態別 ROI が出力される。"""
        db = str(tmp_path / "test.db")
        _make_test_db(db)
        from scripts.segment_analysis import analyze_by_segment
        rows = analyze_by_segment(db, group_by="condition")
        conditions = {r["segment"] for r in rows}
        assert "良" in conditions or "稍重" in conditions
```

- [ ] **Step 2: テストが失敗することを確認**

```bash
pytest tests/test_segment_analysis.py -v 2>&1 | tail -10
```

Expected: FAIL（`scripts/segment_analysis.py` 未存在）

- [ ] **Step 3: `scripts/segment_analysis.py` を作成**

```python
"""
券種 / 会場 / 馬場状態 / 距離帯 別の ROI セグメント分析スクリプト。

使用例:
    py scripts/segment_analysis.py                   # 全セグメント出力
    py scripts/segment_analysis.py --group bet_type  # 券種別のみ
    py scripts/segment_analysis.py --min-bets 30     # 30件未満は除外
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")


_GROUP_SQL: dict[str, str] = {
    "bet_type":  "p.bet_type",
    "venue":     "r.venue",
    "surface":   "r.surface",
    "condition": "r.condition",
    "model":     "p.model_type",
    "distance":  """CASE
                     WHEN r.distance <= 1400 THEN '短距離(~1400)'
                     WHEN r.distance <= 1800 THEN '中距離(1401-1800)'
                     WHEN r.distance <= 2200 THEN '中長距離(1801-2200)'
                     ELSE '長距離(2201~)'
                   END""",
}

_BASE_QUERY = """
SELECT
    {group_expr}           AS segment,
    COUNT(*)               AS n_bets,
    SUM(CASE WHEN pr.is_hit=1 THEN 1 ELSE 0 END) AS hits,
    SUM(COALESCE(pr.payout,0) - COALESCE(pr.profit,0)) AS total_invest,
    SUM(COALESCE(pr.payout,0))                          AS total_payout,
    ROUND(
        100.0 * SUM(COALESCE(pr.payout,0))
        / NULLIF(SUM(COALESCE(pr.payout,0) - COALESCE(pr.profit,0)), 0),
        1
    ) AS roi
FROM predictions p
JOIN prediction_results pr ON pr.prediction_id = p.id
JOIN races r ON r.race_id = p.race_id
GROUP BY {group_expr}
ORDER BY roi ASC
"""


def analyze_by_segment(
    db_path: str = "data/umalogi.db",
    group_by: str = "bet_type",
    min_bets: int = 1,
) -> list[dict]:
    """指定セグメントで ROI を集計して返す。

    Args:
        db_path:  SQLite パス
        group_by: "bet_type" | "venue" | "surface" | "condition" | "model" | "distance"
        min_bets: この件数未満のセグメントは結果から除外

    Returns:
        {"segment", "n_bets", "hits", "total_invest", "total_payout", "roi"} のリスト
    """
    if group_by not in _GROUP_SQL:
        raise ValueError(f"group_by は {list(_GROUP_SQL)} のいずれかを指定してください")

    group_expr = _GROUP_SQL[group_by]
    sql = _BASE_QUERY.format(group_expr=group_expr)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql).fetchall()
    conn.close()

    return [dict(r) for r in rows if r["n_bets"] >= min_bets]


def _print_table(rows: list[dict], group_by: str) -> None:
    """結果をマークダウンテーブルで標準出力に出力する。"""
    print(f"\n## セグメント分析: {group_by}\n")
    print(f"| セグメント | ベット数 | 的中数 | 投資額 | 回収額 | ROI% |")
    print(f"|---|---|---|---|---|---|")
    for r in rows:
        print(
            f"| {r['segment']} | {r['n_bets']} | {r['hits']} "
            f"| ¥{r['total_invest']:,} | ¥{r['total_payout']:,} | {r['roi']:.1f}% |"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UMALOGI セグメント別 ROI 分析")
    parser.add_argument(
        "--group",
        choices=list(_GROUP_SQL.keys()),
        default=None,
        help="集計軸（省略時は全軸を出力）",
    )
    parser.add_argument("--min-bets", type=int, default=20, help="最小ベット数フィルター（デフォルト20）")
    parser.add_argument("--db", default="data/umalogi.db", help="SQLite パス")
    args = parser.parse_args()

    groups = [args.group] if args.group else list(_GROUP_SQL.keys())
    for g in groups:
        rows = analyze_by_segment(db_path=args.db, group_by=g, min_bets=args.min_bets)
        _print_table(rows, g)
```

- [ ] **Step 4: テストが通ることを確認**

```bash
pytest tests/test_segment_analysis.py -v
```

Expected: PASS（3テスト）

- [ ] **Step 5: 実際のDBで動作確認**

```bash
py scripts/segment_analysis.py --min-bets 50 2>&1
```

Expected: 各セグメントの ROI テーブルが出力される。ROI < 50% の組み合わせが明確に見える。

- [ ] **Step 6: コミット**

```bash
git add scripts/segment_analysis.py tests/test_segment_analysis.py
git commit -m "feat: 券種×会場×馬場状態別ROIセグメント分析スクリプト新設"
```

---

## Task 4: スケジューラーへの週次セグメント分析ジョブ追加 & 弱点台帳更新

**背景:** Task 3 のセグメント分析を毎週月曜朝に自動実行し Discord に送信することで、リアルタイムに `_ALLOWED_BET_TYPES` 見直し判断の根拠データを得る。

**Files:**
- Modify: `scripts/scheduler.py`（`job_segment_analysis()` 追加）
- Modify: `docs/7_weakness_ledger.md`（W-038〜W-040 追加）

- [ ] **Step 1: `scripts/scheduler.py` に `job_segment_analysis` 関数を追加**

`scheduler.py` の末尾の `# ─── スケジュール登録 ──` セクションより前（他の job 定義の末尾）に追加:

```python
def job_segment_analysis() -> None:
    """月曜07:30: セグメント別ROI分析を実行しDiscordに送信する。"""
    try:
        from scripts.segment_analysis import analyze_by_segment, _print_table
        import io

        buf = io.StringIO()
        import sys as _sys
        _orig = _sys.stdout
        _sys.stdout = buf

        for group in ["bet_type", "venue", "condition", "model"]:
            rows = analyze_by_segment(db_path="data/umalogi.db", group_by=group, min_bets=30)
            _print_table(rows, group)

        _sys.stdout = _orig
        report = buf.getvalue()

        # Discord に送信（3000字制限に合わせて分割）
        chunks = [report[i:i+1800] for i in range(0, len(report), 1800)]
        for chunk in chunks:
            _send_discord(f"📊 週次セグメントROI分析\n```\n{chunk}\n```", is_system=True)

        logger.info("job_segment_analysis: 完了 %d文字", len(report))
    except Exception as exc:  # noqa: BLE001
        logger.exception("job_segment_analysis 失敗: %s", exc)
        _send_discord(f"⚠️ segment_analysis 失敗: {exc}", is_system=True)
```

- [ ] **Step 2: スケジュール登録を追加**

`scheduler.py` のスケジュール登録セクション（`schedule.every(...)` が並ぶ箇所）に追加:

```python
    # 月曜 07:30: セグメント別ROI分析
    schedule.every().monday.at("07:30").do(job_segment_analysis)
```

- [ ] **Step 3: スケジュール登録を確認**

```bash
py -c "
import schedule
import sys
sys.path.insert(0, '.')
# scheduler.pyの_setup_schedule相当を呼び出して登録を確認
from scripts.scheduler import run_daemon
# ジョブ一覧を表示するだけ
import scripts.scheduler as s
# _setup_schedule を直接呼び出すのが難しいため、文字列で確認
import inspect
src = inspect.getsource(s)
print('job_segment_analysis' in src)
"
```

Expected: `True`

- [ ] **Step 4: 弱点台帳を更新**

`docs/7_weakness_ledger.md` の更新履歴セクションに追記する。

変更前（更新履歴テーブルの末尾）:
```markdown
| 2026-05-27 | 【W-036/W-037 新規登録・キャリブレーション&Kelly監査実施】...
```

変更後（末尾に追加）:
```markdown
| 2026-05-28 | 【W-038 完了】卍モデル三連複EVゲート復活: `_TRIO_EV_MIN = 1.0` を ManjiGenerator に追加。本番実績 ROI 46.7% の三連複買い目を撤廃。影響: `src/ml/bet_generator.py` |
| 2026-05-28 | 【W-039 完了】バックテスト評価品質改善: `--mode flat` フラグを `scripts/backtest_2024_2025.py` に追加。Kelly複利ROI膨張（最大15000%）なしの実態値を算出可能に。影響: `scripts/backtest_2024_2025.py` |
| 2026-05-28 | 【W-040 完了】セグメント分析基盤新設: `scripts/segment_analysis.py` で券種×会場×馬場状態別ROIを自動集計。月曜07:30 Discord自動配信開始。影響: `scripts/segment_analysis.py`, `scripts/scheduler.py` |
```

同様に台帳末尾に W-038〜W-040 エントリを追加:

```markdown
### W-038: 卍モデル三連複 EV ゲート廃止による損失

| 項目 | 内容 |
|------|------|
| ID | W-038 |
| ステータス | 🟢 完了（2026-05-28） |
| 優先度 | 高 |
| 影響 | 本番実績: 卍×三連複 316件 ROI=46.7%（¥-損失確定）。コード上は `# EVゲート撤廃` として EV < 1.0 の三連複も推奨していた |
| 対応方針 | `ManjiGenerator` に `_TRIO_EV_MIN = 1.0` ゲートを追加して EV < 1.0 の三連複を除外 |
| 担当フェーズ | 実装完了 |

### W-039: バックテスト指標の Kelly 複利膨張

| 項目 | 内容 |
|------|------|
| ID | W-039 |
| ステータス | 🟢 完了（2026-05-28） |
| 優先度 | 中 |
| 影響 | WF バックテストが 2025-08 以降に ROI 1000〜15000% に膨張。経営判断に使えない数字が蓄積している |
| 対応方針 | `--mode flat` フラグ追加で Kelly 複利なしのフラット ROI を算出 |
| 担当フェーズ | 実装完了 |

### W-040: 券種×会場×馬場状態別セグメント分析の欠如

| 項目 | 内容 |
|------|------|
| ID | W-040 |
| ステータス | 🟢 完了（2026-05-28） |
| 優先度 | 中 |
| 影響 | どの会場・馬場・券種で赤字か分からず `_ALLOWED_BET_TYPES` の見直しが勘になっている |
| 対応方針 | `scripts/segment_analysis.py` 新設、月曜07:30 Discord 自動配信 |
| 担当フェーズ | 実装完了 |
```

- [ ] **Step 5: 全テスト確認**

```bash
pytest tests/ -x -q 2>&1 | tail -10
```

Expected: PASS

- [ ] **Step 6: 最終コミット**

```bash
git add scripts/scheduler.py docs/7_weakness_ledger.md
git commit -m "feat: 週次セグメント分析ジョブをschedulerに追加・弱点台帳W-038〜040更新"
```

---

## クイックリファレンス: 改善前後の期待値

| 改善項目 | 現状 | 目標 | Task |
|---|---|---|---|
| 卍×三連複 ROI | 46.7% | ≥80%（EVゲートで低品質ベット除外） | Task 1 |
| バックテスト評価精度 | Kelly複利で数千% | フラットROI 65〜102%（実態値） | Task 2 |
| 弱点特定 | 半期単位のみ | 会場×馬場×券種別で月次特定 | Task 3 |
| 自動監視 | 手動確認 | 月曜朝Discord自動配信 | Task 4 |
