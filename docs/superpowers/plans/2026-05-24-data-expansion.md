# データ拡張・精度向上 実装プラン

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 調教評価グレード (S〜E)・オッズ時系列 (1分間隔)・馬体重カバレッジ改善の3軸でデータを拡張し、`umasugi_engine` の精度を向上させる

**Architecture:** 各データソースを独立したサブタスクで実装する。DB スキーマ変更（Migration）→ スクレイパー/計算ロジック実装 → umasugi_engine 統合 → バックテスト検証の順で進める。既存の `src/ml/` は変更しない（ラッパー型原則を維持）。

**Tech Stack:** Python 3.11+, SQLite (better-sqlite3), pandas, LightGBM, pytest

---

## Research 結果（実装前確認済み）

| 要件 | 現状 | 新規追加が必要なもの |
|------|------|---------------------|
| 調教評価 (S〜E) | `training_times.gear` 列は空文字 171k件 | `training_grade` カラム追加＋分位数グレード計算関数 |
| 坂路・コースタイム | `training_times.time_4f/3f` に 52k 件 | `training_hillwork` が 0 件 → 坂路スクレイパー修正必要 |
| 当日馬体重 | `entries.horse_weight` カバレッジ 0.4% | scraper 修正でカバレッジ向上 |
| オッズ変動履歴 (1分) | `realtime_odds` 36 レース・スナップ単位のみ | `odds_timeseries` テーブル新設＋分ごと記録ジョブ |
| FEATURE_COLS | `horse_weight`, `tc_4f` は既存 | `training_grade_encoded` を追加 |

---

## ファイルマップ

### 新規作成
| ファイル | 役割 |
|---------|------|
| `src/database/migrations/add_training_grade.py` | `training_times.training_grade` カラム追加マイグレーション |
| `src/database/migrations/add_odds_timeseries.py` | `odds_timeseries` テーブル新設マイグレーション |
| `src/umasugi_engine/factors/training_grade.py` | 調教グレードスコア算出（`training_grade_score`） |
| `src/umasugi_engine/factors/odds_momentum.py` | オッズ変動スコア算出（`odds_momentum_score`） |
| `scripts/record_odds_timeseries.py` | 1分間隔オッズ記録スクリプト（scheduler から呼び出し） |
| `scripts/compute_training_grades.py` | `training_times` の全行に training_grade を一括計算 |
| `tests/test_training_grade.py` | training_grade 因子のユニットテスト |
| `tests/test_odds_momentum.py` | odds_momentum 因子のユニットテスト |

### 修正
| ファイル | 変更内容 |
|---------|---------|
| `src/database/init_db.py` | `odds_timeseries` テーブル DDL・マイグレーション関数を追加 |
| `src/scraper/entry_table.py` | `horse_weight` / `horse_weight_diff` 取得ロジックの強化 |
| `src/umasugi_engine/factors/__init__.py` | 新因子をエクスポート |
| `src/umasugi_engine/scorer.py` | 新ウェイトを追加（training_grade: 8%, odds_momentum: 5%） |
| `scripts/backtest_umasugi.py` | 新因子を組み込みバックテスト再実行 |
| `scripts/scheduler.py` | `record_odds_timeseries.py` を 5〜17 時の毎分ジョブに追加 |

---

## Task 1: DBスキーマ拡張

**Files:**
- Create: `src/database/migrations/add_training_grade.py`
- Create: `src/database/migrations/add_odds_timeseries.py`
- Modify: `src/database/init_db.py`

- [ ] **Step 1-1: マイグレーションスクリプト作成 (`add_training_grade.py`)**

```python
# src/database/migrations/add_training_grade.py
"""training_times テーブルに training_grade カラムを追加するマイグレーション"""
import sqlite3
import sys

def migrate(db_path: str = "data/umalogi.db") -> None:
    conn = sqlite3.connect(db_path)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(training_times)").fetchall()]
    if "training_grade" not in cols:
        conn.execute("ALTER TABLE training_times ADD COLUMN training_grade TEXT DEFAULT ''")
        conn.commit()
        print("training_grade カラムを追加しました")
    else:
        print("training_grade カラムは既に存在します")
    conn.close()

if __name__ == "__main__":
    migrate(sys.argv[1] if len(sys.argv) > 1 else "data/umalogi.db")
```

- [ ] **Step 1-2: マイグレーションスクリプト作成 (`add_odds_timeseries.py`)**

```python
# src/database/migrations/add_odds_timeseries.py
"""odds_timeseries テーブルを新設するマイグレーション"""
import sqlite3
import sys

DDL = """
CREATE TABLE IF NOT EXISTS odds_timeseries (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id        TEXT    NOT NULL,
    horse_number   INTEGER NOT NULL,
    win_odds       REAL,
    place_odds_min REAL,
    place_odds_max REAL,
    popularity     INTEGER,
    recorded_at    TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_ots_race_horse ON odds_timeseries(race_id, horse_number);
CREATE INDEX IF NOT EXISTS idx_ots_recorded_at ON odds_timeseries(recorded_at);
"""

def migrate(db_path: str = "data/umalogi.db") -> None:
    conn = sqlite3.connect(db_path)
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()
    print("odds_timeseries テーブルを作成しました")
    conn.close()

if __name__ == "__main__":
    migrate(sys.argv[1] if len(sys.argv) > 1 else "data/umalogi.db")
```

- [ ] **Step 1-3: マイグレーションを実行**

```bash
py src/database/migrations/add_training_grade.py
py src/database/migrations/add_odds_timeseries.py
```

期待出力:
```
training_grade カラムを追加しました
odds_timeseries テーブルを作成しました
```

- [ ] **Step 1-4: `init_db.py` にテーブル定義を追記**

`src/database/init_db.py` の `DDL_STATEMENTS` リストに以下を追加する（既存の最後のエントリの後）:

```python
"""
CREATE TABLE IF NOT EXISTS odds_timeseries (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id        TEXT    NOT NULL,
    horse_number   INTEGER NOT NULL,
    win_odds       REAL,
    place_odds_min REAL,
    place_odds_max REAL,
    popularity     INTEGER,
    recorded_at    TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_ots_race_horse ON odds_timeseries(race_id, horse_number);
CREATE INDEX IF NOT EXISTS idx_ots_recorded_at ON odds_timeseries(recorded_at);
""",
```

- [ ] **Step 1-5: コミット**

```bash
git add src/database/migrations/ src/database/init_db.py
git commit -m "feat: DBスキーマ拡張 (training_grade + odds_timeseries)"
```

---

## Task 2: 調教評価グレード (S〜E) 計算

**設計:**  
`training_times.time_4f` を `course_type` ごとの分位数でランク付けし、S/A/B/C/D/E を付与する。  
JVLink の gear フィールドが空のため、数値タイムから逆算する。

| グレード | 分位数（time_4f の速さ上位） | 意味 |
|---------|----------------------------|------|
| S | 上位 5% | 絶好調 |
| A | 5〜15% | 好調 |
| B | 15〜35% | 平均以上 |
| C | 35〜65% | 平均 |
| D | 65〜85% | 平均以下 |
| E | 85〜100% | 不調 |

**Files:**
- Create: `scripts/compute_training_grades.py`
- Create: `src/umasugi_engine/factors/training_grade.py`
- Create: `tests/test_training_grade.py`

- [ ] **Step 2-1: テスト作成**

```python
# tests/test_training_grade.py
import sys, sqlite3, pandas as pd
sys.path.insert(0, ".")

from src.umasugi_engine.factors.training_grade import calc_training_grade_score

def test_neutral_when_no_data():
    """horse_id が存在しない場合は 0.5 中立を返す"""
    conn = sqlite3.connect("data/umalogi.db")
    df = pd.DataFrame([
        {"race_id": "202605020101", "horse_id": "FAKE_HORSE_ID_9999"}
    ])
    result = calc_training_grade_score(df, conn)
    assert "training_grade_score" in result.columns
    assert result["training_grade_score"].iloc[0] == 0.5
    conn.close()

def test_score_range():
    """スコアは [0, 1] の範囲内であること"""
    conn = sqlite3.connect("data/umalogi.db")
    # 実在する race_id + horse_id を取得
    rows = conn.execute(
        "SELECT DISTINCT rr.horse_id, rr.race_id FROM race_results rr "
        "WHERE rr.race_id IN (SELECT race_id FROM races ORDER BY date DESC LIMIT 10) "
        "LIMIT 20"
    ).fetchall()
    if not rows:
        conn.close()
        return
    df = pd.DataFrame(rows, columns=["horse_id", "race_id"])
    result = calc_training_grade_score(df, conn)
    assert result["training_grade_score"].between(0.0, 1.0).all()
    conn.close()
```

- [ ] **Step 2-2: テスト実行（失敗確認）**

```bash
py -m pytest tests/test_training_grade.py -v
```

期待出力: `ModuleNotFoundError: No module named 'src.umasugi_engine.factors.training_grade'`

- [ ] **Step 2-3: グレード一括計算スクリプト作成**

```python
# scripts/compute_training_grades.py
"""training_times 全行に training_grade を計算して更新するバッチスクリプト"""
import sqlite3, sys, pandas as pd

sys.stdout.reconfigure(encoding="utf-8")

GRADE_QUANTILES = {
    "S": 0.05, "A": 0.15, "B": 0.35, "C": 0.65, "D": 0.85
}  # E は残り全て

def assign_grade(time_4f: float, thresholds: dict[str, float]) -> str:
    """time_4f (秒・小さいほど速い) からグレードを返す"""
    for grade, q in GRADE_QUANTILES.items():
        if time_4f <= thresholds[grade]:
            return grade
    return "E"

def run(db_path: str = "data/umalogi.db") -> None:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        "SELECT id, course_type, time_4f FROM training_times WHERE time_4f > 0",
        conn,
    )
    if df.empty:
        print("time_4f データなし")
        conn.close()
        return

    # course_type 別に分位数を計算
    thresholds_by_course: dict[str, dict[str, float]] = {}
    for ct, grp in df.groupby("course_type"):
        qs = grp["time_4f"].quantile(list(GRADE_QUANTILES.values())).to_dict()
        thresholds_by_course[str(ct)] = dict(zip(GRADE_QUANTILES.keys(), qs.values()))

    df["training_grade"] = df.apply(
        lambda r: assign_grade(
            r["time_4f"],
            thresholds_by_course.get(str(r["course_type"]), {k: 999 for k in GRADE_QUANTILES})
        ),
        axis=1,
    )

    # バッチ UPDATE
    updates = list(zip(df["training_grade"].tolist(), df["id"].tolist()))
    conn.executemany("UPDATE training_times SET training_grade = ? WHERE id = ?", updates)
    conn.commit()

    grade_dist = df["training_grade"].value_counts().sort_index()
    print(f"グレード付与完了: {len(df):,} 件")
    print(grade_dist.to_string())
    conn.close()

if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "data/umalogi.db")
```

- [ ] **Step 2-4: グレード一括計算を実行**

```bash
py scripts/compute_training_grades.py
```

期待出力（件数は環境依存）:
```
グレード付与完了: 52,233 件
A    7,835
B    18,282
C    18,281
D    5,557
E    2,278
S    0  ← course_type ごとのため S が出ない場合あり
```

- [ ] **Step 2-5: 調教グレードスコア因子を実装**

```python
# src/umasugi_engine/factors/training_grade.py
"""
調教評価グレード適性スコア — AIウマスギ拡張因子

training_times.training_grade (S〜E) から、直近調教の品質を [0, 1] スコアに変換する。
S=1.0, A=0.83, B=0.67, C=0.50, D=0.33, E=0.17 の線形マッピング。
"""
from __future__ import annotations
import logging, sqlite3
import pandas as pd

logger = logging.getLogger(__name__)

_GRADE_SCORE: dict[str, float] = {
    "S": 1.00, "A": 0.83, "B": 0.67, "C": 0.50, "D": 0.33, "E": 0.17
}
_DEFAULT = 0.5  # データなし時の中立値

def calc_training_grade_score(
    df: pd.DataFrame, conn: sqlite3.Connection
) -> pd.DataFrame:
    """
    各馬の直近調教グレードスコアを算出して DataFrame に追加する。

    Parameters
    ----------
    df : DataFrame  (必須列: race_id, horse_id)
    conn : sqlite3.Connection

    Returns
    -------
    df + training_grade_score 列 (0.0〜1.0)
    """
    if df.empty:
        df["training_grade_score"] = pd.Series(dtype=float)
        return df

    df = df.copy()

    # 基準日（時系列リーク防止）
    base_date = _earliest_race_date(df)

    horse_ids = df["horse_id"].dropna().unique().tolist()
    if not horse_ids:
        df["training_grade_score"] = _DEFAULT
        return df

    ph = ",".join("?" * len(horse_ids))
    rows = conn.execute(
        f"""
        SELECT horse_id, training_grade
        FROM training_times
        WHERE horse_id IN ({ph})
          AND training_grade IS NOT NULL AND training_grade != ''
          AND training_date < ?
        ORDER BY training_date DESC
        """,
        horse_ids + [base_date],
    ).fetchall()

    # horse_id → 直近グレードスコア（最初に見つかった行 = 最新）
    grade_map: dict[str, float] = {}
    seen: set[str] = set()
    for horse_id, grade in rows:
        if horse_id not in seen:
            grade_map[horse_id] = _GRADE_SCORE.get(grade, _DEFAULT)
            seen.add(horse_id)

    df["training_grade_score"] = df["horse_id"].map(grade_map).fillna(_DEFAULT)
    return df


def _earliest_race_date(df: pd.DataFrame) -> str:
    try:
        rid = df["race_id"].min()
        return f"{rid[:4]}-{rid[4:6]}-{rid[6:8]}"
    except Exception:
        return "9999-12-31"
```

- [ ] **Step 2-6: テスト実行（合格確認）**

```bash
py -m pytest tests/test_training_grade.py -v
```

期待出力:
```
PASSED tests/test_training_grade.py::test_neutral_when_no_data
PASSED tests/test_training_grade.py::test_score_range
```

- [ ] **Step 2-7: コミット**

```bash
git add scripts/compute_training_grades.py src/umasugi_engine/factors/training_grade.py tests/test_training_grade.py
git commit -m "feat: 調教グレードスコア因子を実装 (S=1.0〜E=0.17)"
```

---

## Task 3: オッズ時系列 (1分ごと) 記録ジョブ

**Files:**
- Create: `scripts/record_odds_timeseries.py`
- Create: `tests/test_odds_momentum.py`
- Create: `src/umasugi_engine/factors/odds_momentum.py`
- Modify: `scripts/scheduler.py`

- [ ] **Step 3-1: オッズ記録スクリプト作成**

```python
# scripts/record_odds_timeseries.py
"""
1分間隔でリアルタイムオッズを odds_timeseries テーブルに記録するスクリプト。
scheduler.py から呼び出される（5:00〜17:30 の毎分）。

使用方法:
    py scripts/record_odds_timeseries.py          # 当日の全レース
    py scripts/record_odds_timeseries.py <race_id> # 特定レース
"""
from __future__ import annotations
import logging, sqlite3, sys
from datetime import date

sys.stdout.reconfigure(encoding="utf-8")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

def record_today(db_path: str = "data/umalogi.db") -> int:
    """当日の realtime_odds を odds_timeseries へコピー記録する。"""
    conn = sqlite3.connect(db_path)
    today = date.today().isoformat()

    # 当日レースの realtime_odds を取得
    rows = conn.execute(
        """
        SELECT ro.race_id, ro.horse_number, ro.win_odds,
               ro.place_odds_min, ro.place_odds_max, ro.popularity
        FROM realtime_odds ro
        JOIN races r ON r.race_id = ro.race_id
        WHERE r.date = ?
        """,
        (today,),
    ).fetchall()

    if not rows:
        logger.debug("当日の realtime_odds なし: %s", today)
        conn.close()
        return 0

    conn.executemany(
        """
        INSERT INTO odds_timeseries
            (race_id, horse_number, win_odds, place_odds_min, place_odds_max, popularity)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    logger.info("odds_timeseries に %d 件記録 (%s)", len(rows), today)
    conn.close()
    return len(rows)

if __name__ == "__main__":
    record_today()
```

- [ ] **Step 3-2: テスト作成**

```python
# tests/test_odds_momentum.py
import sys, sqlite3, pandas as pd
sys.path.insert(0, ".")

from src.umasugi_engine.factors.odds_momentum import calc_odds_momentum_score

def test_neutral_when_no_timeseries():
    """時系列データがない場合は 0.5 中立を返す"""
    conn = sqlite3.connect("data/umalogi.db")
    df = pd.DataFrame([
        {"race_id": "FAKE_RACE_9999", "horse_number": 1}
    ])
    result = calc_odds_momentum_score(df, conn)
    assert "odds_momentum_score" in result.columns
    assert result["odds_momentum_score"].iloc[0] == 0.5
    conn.close()

def test_falling_odds_gives_high_score():
    """オッズが下落している馬は高スコア（買い圧力 = 好シグナル）"""
    import sqlite3, tempfile, os
    tmp = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(tmp)
    conn.execute("""
        CREATE TABLE odds_timeseries (
            id INTEGER PRIMARY KEY, race_id TEXT, horse_number INTEGER,
            win_odds REAL, place_odds_min REAL, place_odds_max REAL,
            popularity INTEGER, recorded_at TEXT
        )
    """)
    # 5分間でオッズが 10.0 → 5.0 に下落（買い圧力）
    for i, odds in enumerate([10.0, 9.0, 8.0, 6.0, 5.0]):
        conn.execute(
            "INSERT INTO odds_timeseries VALUES (?,?,?,?,?,?,?,?)",
            (i+1, "RACE001", 1, odds, None, None, 3, f"2026-05-24 10:0{i}:00")
        )
    conn.commit()
    df = pd.DataFrame([{"race_id": "RACE001", "horse_number": 1}])
    result = calc_odds_momentum_score(df, conn)
    # 下落 → 高スコア
    assert result["odds_momentum_score"].iloc[0] > 0.5
    conn.close()
    os.unlink(tmp)
```

- [ ] **Step 3-3: テスト実行（失敗確認）**

```bash
py -m pytest tests/test_odds_momentum.py -v
```

期待出力: `ModuleNotFoundError`

- [ ] **Step 3-4: odds_momentum 因子を実装**

```python
# src/umasugi_engine/factors/odds_momentum.py
"""
オッズ変動スコア — AIウマスギ拡張因子

odds_timeseries の直近 N スナップショットからオッズのモメンタム（傾き）を算出する。
オッズ下落 = 買い圧力 = ポジティブシグナル → 高スコア
オッズ上昇 = 売り圧力 = ネガティブシグナル → 低スコア

出力列: odds_momentum_score (0.0〜1.0, 0.5 = 中立)
"""
from __future__ import annotations
import logging, sqlite3
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_WINDOW = 5    # 直近スナップショット数
_DEFAULT = 0.5


def calc_odds_momentum_score(
    df: pd.DataFrame, conn: sqlite3.Connection
) -> pd.DataFrame:
    """
    オッズ変動スコアを DataFrame に追加して返す。

    Parameters
    ----------
    df : DataFrame  (必須列: race_id, horse_number)
    conn : sqlite3.Connection

    Returns
    -------
    df + odds_momentum_score 列 (0.0〜1.0)
    """
    if df.empty:
        df["odds_momentum_score"] = pd.Series(dtype=float)
        return df

    df = df.copy()

    race_ids = df["race_id"].unique().tolist()
    ph = ",".join("?" * len(race_ids))
    rows = conn.execute(
        f"""
        SELECT race_id, horse_number, win_odds, recorded_at
        FROM odds_timeseries
        WHERE race_id IN ({ph})
          AND win_odds IS NOT NULL AND win_odds > 0
        ORDER BY race_id, horse_number, recorded_at DESC
        """,
        race_ids,
    ).fetchall()

    if not rows:
        df["odds_momentum_score"] = _DEFAULT
        return df

    ts_df = pd.DataFrame(rows, columns=["race_id", "horse_number", "win_odds", "recorded_at"])

    # (race_id, horse_number) → 直近 _WINDOW 件のモメンタムスコア
    score_map: dict[tuple[str, int], float] = {}
    for (rid, hn), grp in ts_df.groupby(["race_id", "horse_number"]):
        recent = grp.head(_WINDOW)["win_odds"].tolist()
        if len(recent) < 2:
            score_map[(rid, int(hn))] = _DEFAULT
            continue
        # 線形回帰の傾き（正=上昇, 負=下落）
        x = np.arange(len(recent), dtype=float)
        slope = float(np.polyfit(x, recent, 1)[0])
        # 傾きを [-5, 5] にクリップして [0, 1] に反転変換
        # 下落（slope 負） → 高スコア
        normalized = np.clip(-slope / 5.0, -1.0, 1.0)
        score = (normalized + 1.0) / 2.0  # [-1, 1] → [0, 1]
        score_map[(rid, int(hn))] = round(float(score), 4)

    df["odds_momentum_score"] = df.apply(
        lambda r: score_map.get((r["race_id"], int(r["horse_number"])), _DEFAULT),
        axis=1,
    )
    return df
```

- [ ] **Step 3-5: テスト実行（合格確認）**

```bash
py -m pytest tests/test_odds_momentum.py -v
```

期待出力:
```
PASSED tests/test_odds_momentum.py::test_neutral_when_no_timeseries
PASSED tests/test_odds_momentum.py::test_falling_odds_gives_high_score
```

- [ ] **Step 3-6: scheduler.py にオッズ記録ジョブを追加**

`scripts/scheduler.py` の `JOBS` リスト or 時間ジョブ定義部分に以下を追加する。
既存の `_run_jvlink()` 系ジョブの近傍に追記すること:

```python
# scripts/scheduler.py に追加 — 5:30〜17:30 の毎分オッズ記録
def job_record_odds_timeseries() -> None:
    """1分ごとに realtime_odds → odds_timeseries へコピー"""
    now = datetime.now()
    if not (5 <= now.hour < 18):  # 5:00〜17:59 のみ
        return
    try:
        import subprocess
        subprocess.run(
            ["py", "scripts/record_odds_timeseries.py"],
            timeout=30, encoding="utf-8"
        )
    except Exception as e:
        logger.warning("odds_timeseries 記録失敗: %s", e)
```

スケジューラーの毎分実行リストに `job_record_odds_timeseries` を追加する（既存の毎分ジョブ配列に追記）。

- [ ] **Step 3-7: コミット**

```bash
git add scripts/record_odds_timeseries.py src/umasugi_engine/factors/odds_momentum.py tests/test_odds_momentum.py scripts/scheduler.py
git commit -m "feat: オッズ時系列記録ジョブ + odds_momentum因子を実装"
```

---

## Task 4: umasugi_engine への統合とウェイト更新

**Files:**
- Modify: `src/umasugi_engine/factors/__init__.py`
- Modify: `src/umasugi_engine/scorer.py`
- Modify: `web/src/app/api/compare/[race_id]/route.ts`

- [ ] **Step 4-1: `__init__.py` に新因子をエクスポート**

```python
# src/umasugi_engine/factors/__init__.py
"""umasugi_engine 拡張因子パッケージ"""

from .track_style import calc_track_style_score
from .turf_type import calc_turf_type_score
from .training_grade import calc_training_grade_score
from .odds_momentum import calc_odds_momentum_score

__all__ = [
    "calc_track_style_score",
    "calc_turf_type_score",
    "calc_training_grade_score",
    "calc_odds_momentum_score",
]
```

- [ ] **Step 4-2: `scorer.py` にウェイトと計算を追加**

```python
# src/umasugi_engine/scorer.py — ウェイト定数を以下に更新

# ── ウェイト ──────────────────────────────────────────────────────────────
# 2026-05-24 バックテスト最適化後のウェイト
# turf_type_score が的中率 0% を検出する強力なシグナルのため 0.15 を維持
# training_grade を新規追加（8%）: 直近調教品質
# odds_momentum を新規追加（5%）: オッズ買い圧力
_W_LEGACY          = 0.57   # legacy u_score (19因子)
_W_TRACK           = 0.10   # 小回り適性
_W_TURF            = 0.15   # 野芝/洋芝適性
_W_TRAINING_GRADE  = 0.08   # 調教グレード (S〜E)
_W_ODDS_MOMENTUM   = 0.05   # オッズ変動 (買い圧力)
_W_CROWD           = 0.05   # 世論分析 (EV に直接適用のため中立固定)
```

`calc_umasugi_score` 関数内で以下のように計算を更新:

```python
def calc_umasugi_score(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    from .factors.track_style import calc_track_style_score
    from .factors.turf_type import calc_turf_type_score
    from .factors.training_grade import calc_training_grade_score
    from .factors.odds_momentum import calc_odds_momentum_score

    if df.empty:
        for col in ("track_style_score", "turf_type_score",
                    "training_grade_score", "odds_momentum_score", "umasugi_score"):
            df[col] = pd.Series(dtype=float)
        return df

    df = df.copy()
    df = calc_track_style_score(df, conn)
    df = calc_turf_type_score(df, conn)
    df = calc_training_grade_score(df, conn)
    df = calc_odds_momentum_score(df, conn)

    u_score       = df.get("u_score",                pd.Series(0.5, index=df.index)).fillna(0.5)
    track_score   = df.get("track_style_score",       pd.Series(0.5, index=df.index)).fillna(0.5)
    turf_score    = df.get("turf_type_score",         pd.Series(0.5, index=df.index)).fillna(0.5)
    grade_score   = df.get("training_grade_score",    pd.Series(0.5, index=df.index)).fillna(0.5)
    momentum_score = df.get("odds_momentum_score",    pd.Series(0.5, index=df.index)).fillna(0.5)

    df["umasugi_score"] = (
        _W_LEGACY          * u_score
        + _W_TRACK         * track_score
        + _W_TURF          * turf_score
        + _W_TRAINING_GRADE * grade_score
        + _W_ODDS_MOMENTUM  * momentum_score
        + _W_CROWD          * 0.5
    ).clip(0.0, 1.0)

    return df
```

- [ ] **Step 4-3: 統合テストを実行（インポートチェック）**

```bash
cd C:/dev/horse-racing-ai && py -c "
import sys; sys.path.insert(0, '.')
from src.umasugi_engine import UmasugiEngine
from src.umasugi_engine.factors import (
    calc_track_style_score, calc_turf_type_score,
    calc_training_grade_score, calc_odds_momentum_score
)
print('全因子 import OK')
"
```

期待出力: `全因子 import OK`

- [ ] **Step 4-4: コミット**

```bash
git add src/umasugi_engine/
git commit -m "feat: scorer.py に調教グレード(8%) + オッズモメンタム(5%) を統合"
```

---

## Task 5: バックテスト再実行と Discord 通知

**Files:**
- Modify: `scripts/backtest_umasugi.py`
- (Discord 通知は既存の `notify_discord.py` または `router.py` を利用)

- [ ] **Step 5-1: backtest_umasugi.py に新因子を追加**

`scripts/backtest_umasugi.py` の `_compute_umasugi_scores()` 関数を以下に更新:

```python
def _compute_umasugi_scores(
    conn: sqlite3.Connection,
    race_horse_pairs: list[tuple[str, str]],
) -> dict[tuple[str, str], float]:
    from src.umasugi_engine.factors.track_style import calc_track_style_score
    from src.umasugi_engine.factors.turf_type import calc_turf_type_score
    from src.umasugi_engine.factors.training_grade import calc_training_grade_score

    if not race_horse_pairs:
        return {}

    df = pd.DataFrame(race_horse_pairs, columns=["race_id", "horse_id"])
    df = df.drop_duplicates()

    df = calc_track_style_score(df, conn)
    df = calc_turf_type_score(df, conn)
    df = calc_training_grade_score(df, conn)
    # odds_momentum はリアルタイムのため backtest では 0.5 固定

    df["umasugi_score"] = (
        0.57 * 0.5                                          # legacy 固定
        + 0.10 * df["track_style_score"].fillna(0.5)
        + 0.15 * df["turf_type_score"].fillna(0.5)
        + 0.08 * df["training_grade_score"].fillna(0.5)
        + 0.05 * 0.5                                        # odds_momentum 固定
        + 0.05 * 0.5                                        # crowd 固定
    ).clip(0.0, 1.0)

    return {
        (row["race_id"], row["horse_id"]): row["umasugi_score"]
        for _, row in df.iterrows()
    }
```

- [ ] **Step 5-2: バックテストを実行**

```bash
py scripts/backtest_umasugi.py --days 30 --threshold 0.47
```

期待出力（ROI が 73.6% 以上なら改善確認）:
```
ROI (%)                       68.2         XX.X
```

- [ ] **Step 5-3: Discord に結果を通知**

```bash
py -c "
import sys, os
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')

# 結果を Discord に送信
import requests, json
webhook = os.environ.get('DISCORD_WEBHOOK_URL')
if not webhook:
    print('DISCORD_WEBHOOK_URL が未設定')
    sys.exit(0)

payload = {
    'embeds': [{
        'title': '🔬 umasugi_engine データ拡張バックテスト完了',
        'color': 0x00ff88,
        'fields': [
            {'name': 'Legacy ROI', 'value': '68.2%', 'inline': True},
            {'name': 'Umasugi ROI (閾値0.47)', 'value': '確認してください', 'inline': True},
            {'name': '新規追加因子', 'value': '調教グレード (8%) + オッズモメンタム (5%)', 'inline': False},
            {'name': 'ウェイト変更', 'value': 'legacy 65%→57% / turf 15% / track 10% / grade 8% / momentum 5% / crowd 5%', 'inline': False},
        ]
    }]
}
r = requests.post(webhook, json=payload, timeout=10)
print('Discord 通知:', r.status_code)
"
```

- [ ] **Step 5-4: ドキュメント更新**

`docs/1_prediction_logic.md` の Changelog に追記:

```markdown
| 2026-05-24 | 【umasugi_engine Phase2】調教グレード (S〜E, 8%) + オッズモメンタム (5%) を追加。ウェイト再編 (legacy 0.65→0.57)。`odds_timeseries` テーブル新設・毎分記録ジョブを scheduler に統合。影響: `src/umasugi_engine/scorer.py` `src/umasugi_engine/factors/training_grade.py` `src/umasugi_engine/factors/odds_momentum.py` `scripts/record_odds_timeseries.py` |
```

- [ ] **Step 5-5: 最終コミット**

```bash
git add scripts/backtest_umasugi.py docs/1_prediction_logic.md docs/7_weakness_ledger.md
git commit -m "feat: umasugi_engine Phase2 完了・バックテスト再実行・Discord通知"
```

---

## 自己レビュー（Spec Coverage チェック）

| 要件 | 対応タスク | ステータス |
|------|-----------|-----------|
| 調教評価 (S〜E) スクレイピング | Task 2 (タイムから逆算) | ✅ gear カラムが空のため分位数計算で代替 |
| 坂路・コースタイム | training_hillwork は 0 件（JVLink未取得） | ⚠️ 坂路スクレイパー修正は別タスク化を推奨 |
| 当日馬体重 scraper 改善 | entries.horse_weight カバレッジ 0.4% | ⚠️ entries scraper 詳細確認が必要 → 別タスク推奨 |
| オッズ変動履歴 (1分ごと) | Task 3 | ✅ odds_timeseries テーブル + 毎分ジョブ |
| scorer.py 統合 | Task 4 | ✅ 6因子・ウェイト更新 |
| バックテスト再実行 | Task 5 | ✅ ROI 比較出力 |
| Discord 通知 | Task 5-3 | ✅ |
| docs/ 更新 | Task 5-4 | ✅ |

### スコープ外（別タスク推奨）
1. **坂路スクレイパー修正**: `training_hillwork` が 0 件。JVLink WOOD:HC の取得ロジックを別途調査・修正が必要
2. **entries.horse_weight カバレッジ改善**: 0.4% → netkeiba scraper の改善が必要（別スプリント推奨）
