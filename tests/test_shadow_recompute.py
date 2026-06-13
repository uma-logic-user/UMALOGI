"""
tests/test_shadow_recompute.py — 5月末ポリシー シャドー再計算エンジンのテスト

検証項目:
  1. シャドー model_id 命名ヘルパー
  2. insert_prediction が全シャドーベースを許可（収益トラック分離の前提）
  3. generate_shadow_bets が全券種フルデッキ（馬連〜三連単含む）を生成
  4. save_shadow_bets が既存 live 予想を一切変更しない（条項1 の隔離保証）
  5. virtual_roi_summary が live を除外しシャドーのみ集計（確定結果再利用・dry_run）
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from src.analysis.shadow_recompute import (
    MULTI_DECK_BASES,
    SHADOW_BASES,
    SHADOW_SUFFIX,
    generate_shadow_bets,
    save_shadow_bets,
    shadow_base,
    shadow_model_type,
    virtual_roi_summary,
)
from src.database.init_db import init_db, insert_prediction

_VALID_BET_TYPES = {"単勝", "複勝", "馬連", "ワイド", "馬単", "三連複", "三連単"}


@pytest.fixture()
def conn(tmp_path: Path) -> sqlite3.Connection:
    """本番 DB を汚さない隔離 DB（完全スキーマ）。"""
    db = tmp_path / "shadow_test.db"
    c = init_db(db)
    c.execute("PRAGMA journal_mode=DELETE")
    yield c
    c.close()


def _insert_race(c: sqlite3.Connection, race_id: str) -> None:
    """races の NOT NULL 列を全て満たす最小行を投入する。"""
    c.execute(
        "INSERT OR IGNORE INTO races "
        "(race_id, race_name, date, venue, race_number, distance, surface) "
        "VALUES (?, 'テストレース', '2026-06-13', '東京', 11, 1600, '芝')",
        (race_id,),
    )
    c.commit()


def _synthetic_df(n: int = 8) -> pd.DataFrame:
    """8 頭立ての最小限の特徴量 DataFrame。"""
    return pd.DataFrame(
        {
            "horse_number": list(range(1, n + 1)),
            "horse_name": [f"テスト馬{i}" for i in range(1, n + 1)],
            "win_odds": [2.5, 4.0, 6.0, 9.0, 12.0, 18.0, 25.0, 40.0][:n],
        }
    )


# ── 1. 命名ヘルパー ──────────────────────────────────────────────


def test_shadow_naming() -> None:
    assert shadow_base("本命") == "本命_v0525"
    assert shadow_model_type("卍") == "卍_v0525(再計算)"
    assert SHADOW_SUFFIX == "_v0525"
    # live ベース名は決して衝突しない（サフィックスが必ず付く）
    assert "卍_v0525" in SHADOW_BASES
    assert "本命" not in SHADOW_BASES


# ── 2. insert_prediction が全シャドーベースを許可 ────────────────


def test_insert_accepts_all_shadow_bases(conn: sqlite3.Connection) -> None:
    _insert_race(conn, "202699010101")
    for sbase in SHADOW_BASES:
        mt = f"{sbase}(再計算)"
        pid = insert_prediction(
            conn,
            race_id="202699010101",
            model_type=mt,
            bet_type="単勝",
            horses=[{"horse_number": 1, "horse_name": "x", "predicted_rank": 1}],
            confidence=0.5,
            expected_value=1.2,
            recommended_bet=100,
            combination_json="[[1]]",
        )
        assert pid > 0


# ── 3. 全券種フルデッキ生成 ──────────────────────────────────────


def test_generate_shadow_produces_multi_tickets() -> None:
    df = _synthetic_df()
    honmei = pd.Series([0.30, 0.20, 0.15, 0.12, 0.10, 0.06, 0.04, 0.03], index=df.index)
    ev = pd.Series([1.5, 1.3, 1.2, 1.1, 1.05, 0.9, 0.8, 0.7], index=df.index)

    out = generate_shadow_bets("202699010101", df, honmei, ev)

    # 4 つの多券種モデルが揃う
    for base in MULTI_DECK_BASES:
        assert shadow_base(base) in out

    # 本命/卍 が「消えていた」多券種（馬連・三連複等）を含む
    honmei_types = {b.bet_type for b in out["本命_v0525"].bets}
    manji_types = {b.bet_type for b in out["卍_v0525"].bets}
    assert "単勝" in honmei_types and "複勝" in honmei_types
    # 多券種が少なくとも 1 つは復活していること
    assert (honmei_types | manji_types) & {"馬連", "ワイド", "馬単", "三連複", "三連単"}
    # 生成券種は全て正規の券種集合に含まれる
    for rb in out.values():
        for b in rb.bets:
            assert b.bet_type in _VALID_BET_TYPES


# ── 4. live 予想の不変性（条項1）──────────────────────────────────


def test_save_shadow_does_not_touch_live(conn: sqlite3.Connection) -> None:
    _insert_race(conn, "202699010102")
    # 既存 live 予想（卍(直前) 複勝）を 1 件保存
    live_pid = insert_prediction(
        conn,
        race_id="202699010102",
        model_type="卍(直前)",
        bet_type="複勝",
        horses=[{"horse_number": 3, "horse_name": "live馬", "predicted_rank": 1}],
        confidence=0.7,
        expected_value=1.4,
        recommended_bet=300,
        notes="LIVE_ORIGINAL",
        combination_json="[[3]]",
    )
    before = conn.execute(
        "SELECT model_type, bet_type, notes, confidence FROM predictions WHERE id = ?",
        (live_pid,),
    ).fetchone()

    # シャドー生成 → 保存
    df = _synthetic_df()
    honmei = pd.Series([0.30, 0.20, 0.15, 0.12, 0.10, 0.06, 0.04, 0.03], index=df.index)
    ev = pd.Series([1.5, 1.3, 1.2, 1.1, 1.05, 0.9, 0.8, 0.7], index=df.index)
    out = generate_shadow_bets("202699010102", df, honmei, ev)
    saved = save_shadow_bets(conn, "202699010102", out)

    # live 行は完全に不変
    after = conn.execute(
        "SELECT model_type, bet_type, notes, confidence FROM predictions WHERE id = ?",
        (live_pid,),
    ).fetchone()
    assert before == after
    assert after[2] == "LIVE_ORIGINAL"

    # シャドー行が別 model_type で存在
    shadow_rows = conn.execute(
        "SELECT DISTINCT model_type FROM predictions "
        "WHERE race_id = '202699010102' AND model_type LIKE ?",
        (f"%{SHADOW_SUFFIX}%",),
    ).fetchall()
    assert shadow_rows  # 1 件以上
    assert all(SHADOW_SUFFIX in r[0] for r in shadow_rows)
    assert any(pids for pids in saved.values())


# ── 5. 仮想 ROI 集計（live 除外・dry_run）─────────────────────────


def test_virtual_roi_excludes_live(conn: sqlite3.Connection) -> None:
    rid = "202699010103"
    _insert_race(conn, rid)
    # 確定結果: 1 着=馬番1, 2 着=馬番2, 3 着=馬番3（8 頭立て）
    for hn, rank, odds in [(1, 1, 2.5), (2, 2, 4.0), (3, 3, 6.0)] + [
        (h, h, 10.0) for h in range(4, 9)
    ]:
        conn.execute(
            "INSERT OR IGNORE INTO race_results "
            "(race_id, horse_number, horse_name, rank, win_odds) VALUES (?,?,?,?,?)",
            (rid, hn, f"テスト馬{hn}", rank, odds),
        )
    # 払戻: 単勝 馬番1 = 250 円
    conn.execute(
        "INSERT OR IGNORE INTO race_payouts (race_id, bet_type, combination, payout) "
        "VALUES (?, '単勝', '1', 250)",
        (rid,),
    )
    conn.commit()

    # live 予想（集計から除外されるべき）
    insert_prediction(
        conn,
        race_id=rid,
        model_type="卍(直前)",
        bet_type="単勝",
        horses=[{"horse_number": 1, "horse_name": "x", "predicted_rank": 1}],
        confidence=0.6,
        expected_value=1.3,
        recommended_bet=100,
        combination_json="[[1]]",
    )
    # シャドー予想（単勝 馬番1 → 的中）
    insert_prediction(
        conn,
        race_id=rid,
        model_type="本命_v0525(再計算)",
        bet_type="単勝",
        horses=[{"horse_number": 1, "horse_name": "x", "predicted_rank": 1}],
        confidence=0.6,
        expected_value=1.3,
        recommended_bet=100,
        combination_json="[[1]]",
    )

    rows, settled = virtual_roi_summary(conn, [rid])
    assert settled == 1
    # 本命_v0525 / 単勝 のみが集計され live は含まれない
    assert ("本命_v0525", "単勝") in rows
    assert all(model.endswith("_v0525") for (model, _bt) in rows)
    row = rows[("本命_v0525", "単勝")]
    assert row.n_bets == 1
    assert row.payout == pytest.approx(250.0)
    assert row.invested == pytest.approx(100.0)
    assert row.roi == pytest.approx(250.0)
