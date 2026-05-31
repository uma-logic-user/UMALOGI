# 的中報告レポート生成パイプライン 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** レース的中確認時に win_report.txt 生成・Discord 通知・note.com 下書き投稿を自動実行する `src/ops/win_report.py` を新設し、`fetch_race_result.py` から呼び出す。

**Architecture:** `publish_win_report()` を単一エントリーポイントとして、ファイル生成→Discord送信→Note投稿の3ステップを順番に実行する。各ステップは独立した try/except で囲み、失敗が次のステップに波及しない。呼び出し元の `fetch_race_result.py` は防壁関数 `_try_publish_win_report()` を介して呼び出し、的中0件や例外を透過的に無視する。

**Tech Stack:** Python 3.11+、sqlite3（インメモリテスト用）、requests（Discord webhook）、Playwright / note_draft_publisher（Note投稿）、pytest + unittest.mock

---

## ファイルマップ

| ファイル | 変更種別 | 責務 |
|---------|---------|------|
| `src/ops/win_report.py` | 新規作成 | WinReportData dataclass、ファイル生成、X投稿テキスト、Note記事、Discord送信、メインエントリー |
| `tests/test_win_report.py` | 新規作成 | 7件のユニットテスト（全副作用をモック） |
| `scripts/fetch_race_result.py` | 小変更 | `_try_publish_win_report()` 追加 + 2箇所の呼び出し追加（364・442行） |
| `docs/1_prediction_logic.md` | Changelog追記 | 的中レポートパイプライン追加の記録 |
| `docs/6_special_notes.md` | Changelog追記 | win_report.py の役割と出力先の記録 |

---

## Task 1: WinReportData dataclass + ファイル生成 + X投稿テキスト

**Files:**
- Create: `src/ops/win_report.py`
- Create: `tests/test_win_report.py`

- [ ] **Step 1-1: テストファイルを作成して失敗することを確認する**

`tests/test_win_report.py` を以下の内容で作成する:

```python
"""
tests/test_win_report.py — src/ops/win_report.py のユニットテスト

requests.post / save_draft は全てモックし、実際の HTTP / Playwright は呼ばない。
DB は sqlite3 インメモリを使用する。
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import src.ops.win_report as wr
from src.ops.win_report import (
    WinReportData,
    build_note_draft,
    build_x_post,
    generate_win_report_file,
    publish_win_report,
)


# ── テスト用フェイクオブジェクト ──────────────────────────────────────

@dataclass
class _FakeHit:
    prediction_id: int
    bet_type: str
    is_hit: bool
    payout: float
    invested: float
    profit: float
    roi: float
    combination: list[str]
    actual_winners: list[str]
    is_refund: bool = False


@dataclass
class _FakeResult:
    race_id: str
    race_name: str
    date: str
    hits: list[Any]
    total_invested: float
    total_payout: float
    roi: float
    has_manbaken: bool = False
    max_single_roi: float = 0.0
    is_refund_race: bool = False

    @property
    def hit_count(self) -> int:
        return sum(1 for h in self.hits if h.is_hit)


def _make_hit(
    pred_id: int = 1,
    bet_type: str = "複勝",
    payout: float = 1200.0,
    invested: float = 500.0,
) -> _FakeHit:
    return _FakeHit(
        prediction_id=pred_id,
        bet_type=bet_type,
        is_hit=True,
        payout=payout,
        invested=invested,
        profit=payout - invested,
        roi=payout / invested * 100,
        combination=["ロンギングセリーヌ"],
        actual_winners=["ロンギングセリーヌ"],
    )


def _make_data(**kwargs: Any) -> WinReportData:
    defaults: dict[str, Any] = dict(
        race_id="202605021011",
        race_name="優駿牝馬",
        venue="東京",
        race_number=11,
        date_str="2026-05-24",
        hit_items=[_make_hit()],
        total_invested=500.0,
        total_payout=1200.0,
        roi=240.0,
        top_ev=10.44,
        ev_vs_odds=[{"horse_number": 6, "odds": 27.1, "ev": 10.44, "gap": 10.40}],
    )
    defaults.update(kwargs)
    return WinReportData(**defaults)


def _make_mem_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY,
            race_name TEXT,
            date TEXT,
            venue TEXT,
            race_number INTEGER
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY,
            race_id TEXT,
            model_type TEXT,
            expected_value REAL,
            combination_json TEXT
        );
        CREATE TABLE race_results (
            id INTEGER PRIMARY KEY,
            race_id TEXT,
            horse_number INTEGER,
            win_odds REAL
        );
    """)
    return conn


# ── Task 1 テスト ────────────────────────────────────────────────────

def test_generate_win_report_file_creates_correct_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(wr, "_RESULTS_DIR", tmp_path / "results")
    data = _make_data()

    path = generate_win_report_file(data)

    content = path.read_text(encoding="utf-8")
    assert "=== TITLE ===" in content
    assert "=== BODY ===" in content
    assert "=== X_POST ===" in content
    assert "【的中実績】期待値最適化アルゴリズムによる選別成功" in content
    assert "優駿牝馬" in content
    assert "240.0" in content  # ROI


def test_build_x_post_under_280_chars() -> None:
    data = _make_data()
    post = build_x_post(data)
    assert len(post) <= 280


def test_build_x_post_under_280_chars_with_long_race_name() -> None:
    data = _make_data(race_name="非常に長いレース名テスト用サンプルデータABCDEFGHIJKLMNOPQRSTUVWXYZ")
    post = build_x_post(data)
    assert len(post) <= 280


def test_build_x_post_includes_required_hashtags() -> None:
    data = _make_data()
    post = build_x_post(data)
    assert "#競馬予想" in post
    assert "#期待値アルゴリズム" in post
    assert "#的中実績" in post


# ── Task 2 テスト ────────────────────────────────────────────────────

def test_build_note_draft_contains_ev_and_roi() -> None:
    data = _make_data()
    predictions = [{"model_type": "卍", "expected_value": 10.44}]
    title, body = build_note_draft(data, predictions)

    assert "10.44" in body           # EV値
    assert "240.0" in body           # ROI
    assert "【的中実績】" in title
    assert "優駿牝馬" in title


# ── Task 3 テスト ────────────────────────────────────────────────────

def test_ev_vs_odds_table_populated_from_predictions() -> None:
    conn = _make_mem_db()
    conn.execute(
        "INSERT INTO predictions VALUES (?, ?, ?, ?, ?)",
        (1, "202605021011", "卍", 10.44, json.dumps([[6]])),
    )
    conn.execute(
        "INSERT INTO race_results VALUES (?, ?, ?, ?)",
        (1, "202605021011", 6, 27.1),
    )
    conn.commit()

    hit = _make_hit(pred_id=1)
    result = wr._fetch_ev_vs_odds(conn, "202605021011", [hit])

    assert len(result) == 1
    assert result[0]["horse_number"] == 6
    assert result[0]["odds"] == pytest.approx(27.1)
    assert result[0]["ev"] == pytest.approx(10.44)
    assert result[0]["gap"] == pytest.approx(10.44 - 1.0 / 27.1, abs=0.01)


# ── Task 4 テスト ────────────────────────────────────────────────────

def test_publish_win_report_skips_when_no_hits() -> None:
    conn = _make_mem_db()
    result = _FakeResult(
        race_id="202605021011",
        race_name="優駿牝馬",
        date="2026-05-24",
        hits=[],
        total_invested=500.0,
        total_payout=0.0,
        roi=0.0,
    )

    called: list[str] = []
    with patch("src.ops.win_report.generate_win_report_file", side_effect=lambda d: called.append("file")):
        publish_win_report(result, "202605021011", conn)

    assert called == [], "的中なしなのにファイル生成が呼ばれた"


def test_publish_win_report_handles_playwright_failure_gracefully(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(wr, "_RESULTS_DIR", tmp_path / "results")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "")
    monkeypatch.setenv("DISCORD_SYSTEM_WEBHOOK_URL", "")

    conn = _make_mem_db()
    conn.execute(
        "INSERT INTO races VALUES (?, ?, ?, ?, ?)",
        ("202605021011", "優駿牝馬", "2026-05-24", "東京", 11),
    )
    conn.execute(
        "INSERT INTO predictions VALUES (?, ?, ?, ?, ?)",
        (1, "202605021011", "卍", 10.44, json.dumps([[6]])),
    )
    conn.commit()

    hit = _make_hit(pred_id=1)
    result = _FakeResult(
        race_id="202605021011",
        race_name="優駿牝馬",
        date="2026-05-24",
        hits=[hit],
        total_invested=500.0,
        total_payout=1200.0,
        roi=240.0,
    )

    with patch("src.ops.win_report._post_note_draft", side_effect=RuntimeError("Playwright 失敗")):
        # 例外が publish_win_report の外に漏れないことを確認
        publish_win_report(result, "202605021011", conn)  # 例外なし
```

- [ ] **Step 1-2: テストが失敗することを確認する**

```
pytest tests/test_win_report.py -v
```

期待結果: `ModuleNotFoundError: No module named 'src.ops.win_report'`

- [ ] **Step 1-3: `src/ops/win_report.py` を作成する（Task 1 スコープのみ）**

```python
"""
src/ops/win_report.py

的中報告レポート生成パイプライン。

使用例（fetch_race_result.py から）:
    from src.ops.win_report import publish_win_report
    publish_win_report(result, race_id, conn)
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_RESULTS_DIR = _ROOT / "data" / "results"


@dataclass
class WinReportData:
    race_id:        str
    race_name:      str
    venue:          str
    race_number:    int
    date_str:       str           # YYYY-MM-DD
    hit_items:      list[Any]     # BetHitDetail リスト（is_hit=True のもの）
    total_invested: float
    total_payout:   float
    roi:            float
    top_ev:         float         # 的中した買い目の中で最高の expected_value
    ev_vs_odds:     list[dict]    # [{horse_number, odds, ev, gap}, ...]


def build_x_post(data: WinReportData) -> str:
    """280字以内の X 投稿テキストを返す。"""
    h = data.hit_items[0]
    race_name = data.race_name
    hashtags_full  = f"#競馬予想 #期待値アルゴリズム #的中実績 #{race_name}"
    hashtags_short = "#競馬予想 #期待値アルゴリズム #的中実績"

    base = (
        f"🎉【的中】{data.venue}{data.race_number}R {race_name}\n"
        f"EV={data.top_ev:.2f}の歪みを捉えて{h.bet_type}的中\n\n"
        f"投資¥{int(data.total_invested):,} → 払戻¥{int(data.total_payout):,}"
        f"（ROI {data.roi:.0f}%）\n\n"
        "期待値アルゴリズムが市場の非効率を見抜きました📊\n\n"
    )
    for tags in (hashtags_full, hashtags_short):
        candidate = base + tags
        if len(candidate) <= 280:
            return candidate

    short_base = (
        f"🎉【的中】{data.venue}{data.race_number}R {race_name}\n"
        f"投資¥{int(data.total_invested):,} → 払戻¥{int(data.total_payout):,}"
        f"（ROI {data.roi:.0f}%）\n\n"
        "期待値アルゴリズムが的中を導きました📊\n\n"
    )
    return (short_base + hashtags_short)[:280]


def generate_win_report_file(data: WinReportData) -> Path:
    """data/results/YYYYMMDD/{race_id}_win_report.txt を生成して Path を返す。"""
    date_nodash = data.date_str.replace("-", "")
    out_dir = _RESULTS_DIR / date_nodash
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{data.race_id}_win_report.txt"

    hit_lines: list[str] = []
    for h in data.hit_items:
        combo_str = "-".join(str(c) for c in h.combination) if h.combination else "?"
        sign = "+" if h.profit >= 0 else ""
        hit_lines.append(
            f"  {h.bet_type}  {combo_str}  "
            f"¥{int(h.payout):,} 払戻"
            f"（投資¥{int(h.invested):,} / 利益{sign}¥{int(h.profit):,}）"
        )
    hit_text = "\n".join(hit_lines)

    if data.ev_vs_odds:
        odds_lines = [
            f"  馬番{e['horse_number']}  "
            f"市場オッズ {e['odds']:.1f}倍 / AI推奨EV {e['ev']:.2f} / 乖離スコア {e['gap']:+.2f}"
            for e in data.ev_vs_odds
        ]
        odds_text = "\n".join(odds_lines)
    else:
        odds_text = "  (データなし)"

    title = "【的中実績】期待値最適化アルゴリズムによる選別成功"
    body = (
        f"本日、{data.venue}{data.race_number}R「{data.race_name}」において、"
        f"アルゴリズムが市場の歪みを捉え的中を達成しました。\n\n"
        f"推奨根拠：対象馬のEV値は{data.top_ev:.2f}であり、"
        f"ROIフィルター通過後の確実な選別を行いました。\n\n"
        f"■ 的中買い目\n{hit_text}\n\n"
        f"本日の合計ROI：{data.roi:.1f}％\n\n"
        f"■ 市場オッズ vs AIスコア（検証データ）\n{odds_text}"
    )
    x_post = build_x_post(data)

    content = f"=== TITLE ===\n{title}\n\n=== BODY ===\n{body}\n\n=== X_POST ===\n{x_post}\n"
    path.write_text(content, encoding="utf-8")
    return path


# Task 2〜4 で実装する関数のスタブ（テストが import エラーにならないよう）
def build_note_draft(data: WinReportData, predictions: list[dict]) -> tuple[str, str]:
    raise NotImplementedError

def _fetch_ev_vs_odds(conn: sqlite3.Connection, race_id: str, hit_items: list[Any]) -> list[dict]:
    raise NotImplementedError

def publish_win_report(result: Any, race_id: str, conn: sqlite3.Connection) -> None:
    raise NotImplementedError
```

- [ ] **Step 1-4: Task 1 のテストが通ることを確認する**

```
pytest tests/test_win_report.py::test_generate_win_report_file_creates_correct_content tests/test_win_report.py::test_build_x_post_under_280_chars tests/test_win_report.py::test_build_x_post_under_280_chars_with_long_race_name tests/test_win_report.py::test_build_x_post_includes_required_hashtags -v
```

期待結果: 4件 PASSED。他は `NotImplementedError` で FAILED（まだ正常）。

- [ ] **Step 1-5: コミットする**

```
git add src/ops/win_report.py tests/test_win_report.py
git commit -m "feat: win_report WinReportData + generate_win_report_file + build_x_post"
```

---

## Task 2: `build_note_draft()` の実装

**Files:**
- Modify: `src/ops/win_report.py`（`build_note_draft` スタブを置き換え）

- [ ] **Step 2-1: `build_note_draft` テストが失敗することを確認する**

```
pytest tests/test_win_report.py::test_build_note_draft_contains_ev_and_roi -v
```

期待結果: `NotImplementedError` で FAILED。

- [ ] **Step 2-2: `build_note_draft()` スタブを本実装に置き換える**

`src/ops/win_report.py` の `build_note_draft` を以下に置き換える:

```python
def build_note_draft(
    data: WinReportData,
    predictions: list[dict],
) -> tuple[str, str]:
    """(note_title, note_body) の Markdown を返す。"""
    ymd = data.date_str.replace("-", "/")
    title = (
        f"【的中実績】{ymd} {data.venue}{data.race_number}R"
        f"「{data.race_name}」— EV期待値アルゴリズム選別成功"
    )

    h = data.hit_items[0]
    combo_str = "-".join(str(c) for c in h.combination) if h.combination else "?"

    top_model = "AIモデル"
    if predictions:
        best = max(predictions, key=lambda p: p.get("expected_value") or 0.0)
        top_model = best.get("model_type") or "AIモデル"

    if data.ev_vs_odds:
        odds_rows = "\n".join(
            f"| {e['horse_number']} | {e['odds']:.1f}倍 | {e['ev']:.2f} | {e['gap']:+.2f} |"
            for e in data.ev_vs_odds
        )
    else:
        odds_rows = "| — | — | — | — |"

    body = (
        f"# 【的中実績】{ymd} {data.venue}{data.race_number}R"
        f"「{data.race_name}」— EV期待値アルゴリズム選別成功\n\n"
        f"## 的中サマリー\n"
        f"| 項目 | 内容 |\n"
        f"|------|------|\n"
        f"| 買い目 | {h.bet_type} {combo_str} |\n"
        f"| 投資 | ¥{int(data.total_invested):,} |\n"
        f"| 払戻 | ¥{int(data.total_payout):,} |\n"
        f"| ROI | {data.roi:.1f}% |\n\n"
        f"## なぜこのレースを選んだか\n"
        f"本日、{data.venue}{data.race_number}R「{data.race_name}」において、"
        f"アルゴリズムが市場の歪みを捉え的中を達成しました。\n"
        f"推奨根拠：対象馬のEV値は{data.top_ev:.2f}であり、"
        f"ROIフィルター通過後の確実な選別を行いました。\n\n"
        f"## 市場オッズ vs AIスコア（比較データ）\n"
        f"| 馬番 | 市場オッズ | AI推奨EV | 乖離スコア |\n"
        f"|------|----------|---------|----------|\n"
        f"{odds_rows}\n\n"
        f"## フィルター貢献度\n"
        f"- {top_model}: EV={data.top_ev:.2f} で最高スコア\n"
        f"- ROIフィルター: 通過（EV > 1.0）\n"
        f"- ウマスギフィルター: 適用済み\n\n"
        f"## 免責事項\n"
        f"本記事は統計的期待値に基づく投資記録であり、的中を保証するものではありません。\n"
        f"投資は自己責任でお願いします。\n\n"
        f"*UMALOGI — AI 競馬予測プラットフォーム*"
    )
    return title, body
```

- [ ] **Step 2-3: テストが通ることを確認する**

```
pytest tests/test_win_report.py::test_build_note_draft_contains_ev_and_roi -v
```

期待結果: PASSED。

- [ ] **Step 2-4: コミットする**

```
git add src/ops/win_report.py
git commit -m "feat: win_report build_note_draft"
```

---

## Task 3: `_fetch_ev_vs_odds()` の実装

**Files:**
- Modify: `src/ops/win_report.py`（`_fetch_ev_vs_odds` スタブを置き換え）

- [ ] **Step 3-1: テストが `NotImplementedError` で失敗することを確認する**

```
pytest tests/test_win_report.py::test_ev_vs_odds_table_populated_from_predictions -v
```

期待結果: `NotImplementedError` で FAILED。

- [ ] **Step 3-2: `_fetch_ev_vs_odds()` スタブを本実装に置き換える**

```python
def _fetch_ev_vs_odds(
    conn: sqlite3.Connection,
    race_id: str,
    hit_items: list[Any],
) -> list[dict[str, Any]]:
    """的中買い目の馬番ごとに EV vs 市場オッズを構築する。

    predictions.combination_json の形式: [[horse_num, ...], ...]
    race_results.win_odds: 単勝オッズ
    gap = ev - (1.0 / win_odds)  正値 = AI が市場より高く評価
    """
    if not hit_items:
        return []

    pred_ids = [h.prediction_id for h in hit_items]
    placeholders = ",".join("?" * len(pred_ids))
    preds = conn.execute(
        f"SELECT id, expected_value, combination_json "
        f"FROM predictions WHERE id IN ({placeholders})",
        pred_ids,
    ).fetchall()

    pred_map: dict[int, tuple[float, list]] = {}
    for p in preds:
        try:
            combos = json.loads(p["combination_json"]) if p["combination_json"] else []
            horse_nums = list(combos[0]) if combos else []
        except Exception:
            horse_nums = []
        pred_map[p["id"]] = (p["expected_value"] or 0.0, horse_nums)

    odds_rows = conn.execute(
        "SELECT horse_number, win_odds FROM race_results WHERE race_id = ?",
        (race_id,),
    ).fetchall()
    odds_map: dict[int, float] = {
        r["horse_number"]: r["win_odds"] or 0.0 for r in odds_rows
    }

    seen: set[int] = set()
    result: list[dict[str, Any]] = []
    for h in hit_items:
        ev, horse_nums = pred_map.get(h.prediction_id, (0.0, []))
        if not horse_nums:
            continue
        main_horse = int(horse_nums[0])
        if main_horse in seen:
            continue
        seen.add(main_horse)
        odds = odds_map.get(main_horse, 0.0)
        gap = ev - (1.0 / odds) if odds > 0 else 0.0
        result.append({"horse_number": main_horse, "odds": odds, "ev": ev, "gap": gap})

    return result
```

- [ ] **Step 3-3: テストが通ることを確認する**

```
pytest tests/test_win_report.py::test_ev_vs_odds_table_populated_from_predictions -v
```

期待結果: PASSED。

- [ ] **Step 3-4: コミットする**

```
git add src/ops/win_report.py
git commit -m "feat: win_report _fetch_ev_vs_odds DB helper"
```

---

## Task 4: Discord送信・Note投稿ヘルパー + `publish_win_report()` の実装

**Files:**
- Modify: `src/ops/win_report.py`（`publish_win_report` スタブを本実装に置き換え＋ヘルパー3関数を追加）

- [ ] **Step 4-1: Task 4 テストが失敗することを確認する**

```
pytest tests/test_win_report.py::test_publish_win_report_skips_when_no_hits tests/test_win_report.py::test_publish_win_report_handles_playwright_failure_gracefully -v
```

期待結果: 2件とも `NotImplementedError` で FAILED。

- [ ] **Step 4-2: Discord送信・Note投稿ヘルパーを追加する**

`src/ops/win_report.py` の `publish_win_report` スタブの前に以下3関数を挿入する:

```python
def _send_discord_report(data: WinReportData, report_path: Path | None) -> None:
    """Discord 予想チャンネルへ Embed + X投稿テキストを2メッセージ送信する。"""
    import requests

    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook_url:
        logger.warning("[WinReport] DISCORD_WEBHOOK_URL 未設定 — Discord 送信スキップ")
        return

    color = (
        0xFF4500 if data.total_payout >= 100_000
        else 0xFFD700 if data.total_payout >= 10_000
        else 0x43B581
    )

    lines: list[str] = []
    for h in data.hit_items:
        combo_str = "-".join(str(c) for c in h.combination) if h.combination else "?"
        sign = "+" if h.profit >= 0 else ""
        lines.append(
            f"**{h.bet_type}**  {combo_str}  "
            f"¥{int(h.payout):,}  "
            f"(投資¥{int(h.invested):,} / 利益{sign}¥{int(h.profit):,})"
        )

    footer_text = data.date_str
    if report_path:
        footer_text += f" | {report_path} に保存済み"

    fields: list[dict] = [
        {"name": "EV最大値", "value": f"{data.top_ev:.2f}", "inline": True},
        {"name": "ROI", "value": f"{data.roi:.1f}%", "inline": True},
    ]
    if data.ev_vs_odds:
        fields.append({
            "name": "乖離スコア（主力馬）",
            "value": f"{data.ev_vs_odds[0]['gap']:+.2f}",
            "inline": True,
        })

    embed = {
        "title": f"🏆 的中レポート  {data.venue}{data.race_number}R「{data.race_name}」",
        "description": "\n".join(lines),
        "color": color,
        "fields": fields,
        "footer": {"text": footer_text},
    }

    resp = requests.post(webhook_url, json={"embeds": [embed]}, timeout=10)
    if resp.status_code not in (200, 204):
        logger.warning("[WinReport] Discord Embed 送信失敗: status=%d", resp.status_code)

    x_post = build_x_post(data)
    resp2 = requests.post(
        webhook_url,
        json={"content": f"📋 X投稿テキスト（コピーしてそのまま貼り付けてください）\n\n```\n{x_post}\n```"},
        timeout=10,
    )
    if resp2.status_code not in (200, 204):
        logger.warning("[WinReport] Discord X投稿テキスト送信失敗: status=%d", resp2.status_code)


def _post_note_draft(data: WinReportData, predictions: list[dict]) -> None:
    """note.com に分析記事を下書き保存する（Playwright 経由）。"""
    from src.ops.note_draft_publisher import save_draft

    title, body = build_note_draft(data, predictions)
    ok = save_draft(
        title=title,
        body=body,
        tags=["競馬", "UMALOGI", "AI予想", "的中実績"],
    )
    if not ok:
        raise RuntimeError("save_draft が False を返しました")


def _alert_note_failure(data: WinReportData) -> None:
    """Note 投稿失敗時にシステムチャンネルへアラートを送る（例外は握り潰す）。"""
    try:
        import requests

        system_url = os.environ.get("DISCORD_SYSTEM_WEBHOOK_URL", "")
        if not system_url:
            return
        date_nodash = data.date_str.replace("-", "")
        msg = (
            f"⚠️ **Note 投稿失敗 — 手動確認が必要**\n"
            f"レース: {data.venue}{data.race_number}R「{data.race_name}」\n"
            f"下書きファイル: `data/results/{date_nodash}/{data.race_id}_win_report.txt`\n"
            "対応: note.com に手動で下書き投稿してください。"
        )
        requests.post(system_url, json={"content": msg}, timeout=10)
    except Exception:
        pass
```

- [ ] **Step 4-3: `publish_win_report()` スタブを本実装に置き換える**

```python
def publish_win_report(
    result: Any,
    race_id: str,
    conn: sqlite3.Connection,
) -> None:
    """メインエントリーポイント。各ステップの例外は内部でキャッチしてログに落とす。"""
    hit_items = [h for h in result.hits if h.is_hit]
    if not hit_items:
        return

    race_row = conn.execute(
        "SELECT venue, race_number FROM races WHERE race_id = ?",
        (race_id,),
    ).fetchone()
    venue       = race_row["venue"]       if race_row else ""
    race_number = race_row["race_number"] if race_row else 0

    pred_ids = [h.prediction_id for h in hit_items]
    placeholders = ",".join("?" * len(pred_ids))
    preds = conn.execute(
        f"SELECT id, expected_value, model_type, combination_json "
        f"FROM predictions WHERE id IN ({placeholders})",
        pred_ids,
    ).fetchall() if pred_ids else []

    pred_ev_map: dict[int, float] = {p["id"]: p["expected_value"] or 0.0 for p in preds}
    top_ev = max(pred_ev_map.values(), default=0.0)

    ev_vs_odds = _fetch_ev_vs_odds(conn, race_id, hit_items)

    data = WinReportData(
        race_id=race_id,
        race_name=result.race_name,
        venue=venue,
        race_number=race_number,
        date_str=result.date,
        hit_items=hit_items,
        total_invested=result.total_invested,
        total_payout=result.total_payout,
        roi=result.roi,
        top_ev=top_ev,
        ev_vs_odds=ev_vs_odds,
    )

    report_path: Path | None = None
    try:
        report_path = generate_win_report_file(data)
        logger.info("[WinReport] ファイル保存: %s", report_path)
    except Exception as e:
        logger.warning("[WinReport] ファイル生成失敗: %s", e)

    try:
        _send_discord_report(data, report_path)
    except Exception as e:
        logger.warning("[WinReport] Discord 送信失敗: %s", e)

    predictions_list = [dict(p) for p in preds]
    try:
        _post_note_draft(data, predictions_list)
    except Exception as e:
        logger.warning("[WinReport] Note 投稿失敗: %s", e)
        _alert_note_failure(data)
```

- [ ] **Step 4-4: 全テストが通ることを確認する**

```
pytest tests/test_win_report.py -v
```

期待結果: 7件全て PASSED。

- [ ] **Step 4-5: コミットする**

```
git add src/ops/win_report.py
git commit -m "feat: win_report publish_win_report + Discord/Note helpers 全7テスト PASS"
```

---

## Task 5: `fetch_race_result.py` に呼び出しを追加する

**Files:**
- Modify: `scripts/fetch_race_result.py`（3箇所）

- [ ] **Step 5-1: `_try_publish_win_report()` を追加する**

`scripts/fetch_race_result.py` の `_send_hit_flash` 関数定義（231行目）の**直前**に以下を挿入する:

```python
def _try_publish_win_report(result: object, race_id: str, conn: object) -> None:
    """的中時のみ win_report パイプラインを起動する。失敗しても例外を漏らさない。"""
    if not hasattr(result, "hit_count") or result.hit_count == 0:  # type: ignore[union-attr]
        return
    try:
        from src.ops.win_report import publish_win_report
        publish_win_report(result, race_id, conn)
    except Exception as e:
        logger.warning("[WinReport] 失敗（スキップ）: %s", e)


```

- [ ] **Step 5-2: `fetch_single_race()` に呼び出しを追加する（364行目付近）**

変更前:
```python
        # 的中速報を Discord へ送信
        _send_hit_flash(result, result.race_name)
    except Exception as ee:
        logger.warning("評価失敗 race_id=%s: %s", race_id, ee)

    conn.close()
    return True
```

変更後:
```python
        # 的中速報を Discord へ送信
        _send_hit_flash(result, result.race_name)
        _try_publish_win_report(result, race_id, conn)
    except Exception as ee:
        logger.warning("評価失敗 race_id=%s: %s", race_id, ee)

    conn.close()
    return True
```

- [ ] **Step 5-3: `fetch_for_date()` に呼び出しを追加する（442行目付近）**

変更前:
```python
            _send_hit_flash(result, result.race_name)
        except Exception as e:
            logger.warning("評価失敗 %s: %s", race_id, e)
```

変更後:
```python
            _send_hit_flash(result, result.race_name)
            _try_publish_win_report(result, race_id, conn)
        except Exception as e:
            logger.warning("評価失敗 %s: %s", race_id, e)
```

- [ ] **Step 5-4: import が通ることを確認する**

```
py -c "import scripts.fetch_race_result" 2>&1 || py scripts/fetch_race_result.py --help
```

期待結果: エラーなし（または usage 表示）。

- [ ] **Step 5-5: 既存テストが壊れていないことを確認する**

```
pytest tests/ -v --ignore=tests/test_win_report.py -x -q 2>&1 | tail -20
```

期待結果: 既存テストの FAILED が増えていない。

- [ ] **Step 5-6: コミットする**

```
git add scripts/fetch_race_result.py
git commit -m "feat: fetch_race_result に _try_publish_win_report を統合"
```

---

## Task 6: ドキュメントを更新する

**Files:**
- Modify: `docs/1_prediction_logic.md`
- Modify: `docs/6_special_notes.md`

- [ ] **Step 6-1: `docs/1_prediction_logic.md` の Changelog 先頭行に追記する**

以下の行を Changelog テーブルの**先頭**（最新エントリとして）に追加する:

```
| 2026-05-24 | 【的中報告レポートパイプライン追加】`src/ops/win_report.py` 新設。的中確認後に `data/results/YYYYMMDD/{race_id}_win_report.txt` 生成・Discord 予想ch へ Embed + X投稿テキスト送信・note.com 下書き保存を自動実行。`scripts/fetch_race_result.py` に `_try_publish_win_report()` を追加し `fetch_single_race()`・`fetch_for_date()` から呼び出す。影響ファイル: `src/ops/win_report.py`（新規）, `scripts/fetch_race_result.py` |
```

- [ ] **Step 6-2: `docs/6_special_notes.md` の Changelog 先頭行に追記する**

```
| 2026-05-24 | 【win_report.py 運用メモ】出力先: `data/results/YYYYMMDD/{race_id}_win_report.txt`（3セクション: TITLE / BODY / X_POST）。Note 投稿失敗時は `DISCORD_SYSTEM_WEBHOOK_URL` にアラート送信し手動投稿を促す。環境変数: `DISCORD_WEBHOOK_URL`（予想ch）・`DISCORD_SYSTEM_WEBHOOK_URL`（systemch）・`NOTE_EMAIL`・`NOTE_PASSWORD`。 |
```

- [ ] **Step 6-3: 全テストが引き続き通ることを最終確認する**

```
pytest tests/test_win_report.py -v
```

期待結果: 7件全て PASSED。

- [ ] **Step 6-4: 最終コミットする**

```
git add docs/1_prediction_logic.md docs/6_special_notes.md
git commit -m "docs: 的中報告レポートパイプライン Changelog 追記"
```

---

## セルフレビューチェックリスト

- [x] **スペックカバレッジ**: 全5要件（ファイル生成・Discord Embed・X投稿テキスト・Note下書き・エラーハンドリング）をカバー
- [x] **プレースホルダーなし**: TBD / TODO なし、全ステップにコードあり
- [x] **型一貫性**: `WinReportData` は全タスクで同一定義を使用。`_fetch_ev_vs_odds` は Task 3 で定義、Task 4 の `publish_win_report` で呼び出し（一致）
- [x] **スコープ**: 1プラン・1実装サイクルで完結するスコープ
