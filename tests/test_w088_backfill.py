"""W-088/W-089 回帰テスト: 条件戦 grade 補完・W-011 delta・破損行 status 隔離。

カバー範囲:
  1. `_extract_grade` の全角グレード/条件クラス表記の拡充
  2. `_grade_from_ra` 競走条件名称(JyokenName)スキャンによる条件戦 grade 導出
  3. `_parse_ra` のフォールバック配線（競走名空でも条件戦 grade が入ること）
  4. W-011 クラス変化 delta が grade 充填時に 0 縮退しないこと
  5. マイグレーション #22: races.status 列の存在（破損行の論理隔離）
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from src.database.init_db import init_db
from src.ml.u_score import UScoreEngine
from src.scraper.jravan_client import _extract_grade, _grade_from_ra, _parse_ra


# ── 1. _extract_grade 拡充パターン ─────────────────────────────────────────


def test_extract_grade_fullwidth_graded() -> None:
    """全角 ＧⅠ／ＧⅡ／ＧⅢ・Ｇ１/Ｇ２/Ｇ３ も正規化されること。"""
    assert _extract_grade("第65回大阪杯（ＧⅠ）") == "G1"
    assert _extract_grade("第29回マーメイドＳ（ＧⅢ）") == "G3"
    assert _extract_grade("テスト（Ｇ２）") == "G2"


def test_extract_grade_fullwidth_condition_classes() -> None:
    """全角数字の N 勝クラスも導出できること。"""
    assert _extract_grade("３歳１勝クラス") == "1勝"
    assert _extract_grade("２勝クラス") == "2勝"
    assert _extract_grade("３勝クラス") == "3勝"


def test_extract_grade_fullwidth_listed() -> None:
    assert _extract_grade("ノベンバーＳ（L）") == "L"


# ── 2. _grade_from_ra 競走条件名称スキャン ─────────────────────────────────


def _ra_with_jyoken(jyoken: str, *, race_name: str = "", offset: int = 200) -> bytes:
    """RA 生バイトを模擬する。競走条件名称を窓内 offset に cp932 で埋め込む。"""
    buf = bytearray(b" " * 520)
    buf[0:2] = b"RA"
    buf[2:3] = b"1"
    buf[3:11] = b"20260613"  # データ作成日
    buf[11:19] = b"20260613"  # 開催年月日
    buf[19:21] = b"06"  # 場コード
    buf[21:23] = b"03"  # 開催回
    buf[23:25] = b"03"  # 開催日
    buf[25:27] = b"11"  # レース番号
    # 競走名本題（5桁プレフィックス + 名称）。条件戦は通常空。
    rn = ("00000" + race_name).encode("cp932")
    buf[27 : 27 + len(rn)] = rn
    jb = jyoken.encode("cp932")
    buf[offset : offset + len(jb)] = jb
    return bytes(buf)


def test_grade_from_ra_detects_condition_classes() -> None:
    """JyokenName 域の条件表記をスキャンして正準トークンへ導出できること。"""
    assert _grade_from_ra(_ra_with_jyoken("３歳未勝利")) == "未勝利"
    assert _grade_from_ra(_ra_with_jyoken("２歳新馬")) == "新馬"
    assert _grade_from_ra(_ra_with_jyoken("３歳以上１勝クラス")) == "1勝"
    assert _grade_from_ra(_ra_with_jyoken("サラ系３歳以上２勝クラス")) == "2勝"


def test_grade_from_ra_returns_empty_without_keyword() -> None:
    """クラスキーワードが無い／窓ズレ時は空文字を返し grade を汚染しないこと。"""
    # 距離だけの数値域 → キーワード不一致 → ''（誤充填しない安全側）
    assert _grade_from_ra(_ra_with_jyoken("2000", offset=242)) == ""
    assert _grade_from_ra(b" " * 520) == ""


# ── 3. _parse_ra フォールバック配線 ───────────────────────────────────────


def test_parse_ra_fills_condition_grade_when_name_blank() -> None:
    """競走名が空でも JyokenName スキャンで条件戦 grade が充填されること。"""
    rec = _parse_ra(_ra_with_jyoken("３歳未勝利", race_name=""))
    assert rec is not None
    assert rec["race_id"] == "202606030311"
    assert rec["race_name"] == ""  # 条件戦は競走名本題が空
    assert rec["grade"] == "未勝利"  # フォールバックで充填


def test_parse_ra_prefers_race_name_grade() -> None:
    """競走名からグレードが取れる場合はそちらを優先すること。"""
    rec = _parse_ra(_ra_with_jyoken("３歳未勝利", race_name="大阪杯(GI)"))
    assert rec is not None
    assert rec["grade"] == "G1"


# ── 4. W-011 クラス変化 delta（grade 充填で 0 縮退しないこと） ─────────────


def _seed_class_change(
    conn: sqlite3.Connection, *, cur_grade: str, prev_grade: str
) -> None:
    """今走 1 件＋前走 1 件（同一馬 H1 が前走で着順あり）を投入する。"""
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO horses (horse_id, horse_name) VALUES ('H1','テスト馬')"
        )
        for rid, rdate, grd in (
            ("202601100101", "2026-01-10", cur_grade),
            ("202512010101", "2025-12-01", prev_grade),
        ):
            conn.execute(
                """INSERT OR REPLACE INTO races
                   (race_id, race_name, date, venue, race_number, distance, surface, grade)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (rid, "", rdate, "中山", 1, 1800, "芝", grd),
            )
        # 前走の着順（rank>0 が prev grade 抽出の条件）
        conn.execute(
            """INSERT INTO race_results (race_id, horse_id, horse_name, horse_number, rank)
               VALUES ('202512010101','H1','テスト馬',1,3)"""
        )


def _delta(conn: sqlite3.Connection) -> float:
    df = pd.DataFrame(
        {"race_id": ["202601100101"], "horse_id": ["H1"], "win_rate_all": [0.1]}
    )
    out = UScoreEngine(conn)._calc_competition(df)
    return float(out["class_change_delta"].iloc[0])


def test_w011_delta_degenerates_to_zero_without_grade() -> None:
    """grade 空（旧状態）では前走・今走ともランク 5 相当で delta=0 に縮退する。"""
    conn = init_db(db_path=Path(":memory:"))
    _seed_class_change(conn, cur_grade="", prev_grade="")
    assert _delta(conn) == 0.0


def test_w011_delta_nonzero_with_grade_filled() -> None:
    """grade 充填後は前走(未勝利=9)→今走(1勝=8)で delta=+1.0（非ゼロ）になること。"""
    conn = init_db(db_path=Path(":memory:"))
    _seed_class_change(conn, cur_grade="1勝", prev_grade="未勝利")
    assert _delta(conn) == 1.0


# ── 5. マイグレーション #22: races.status 列 ──────────────────────────────


def test_races_has_status_column() -> None:
    conn = init_db(db_path=Path(":memory:"))
    cols = [r[1] for r in conn.execute("PRAGMA table_info(races)").fetchall()]
    assert "status" in cols, "マイグレーション #22 で status 列が追加されていない"


def test_races_status_defaults_to_valid() -> None:
    """新規 INSERT は既定で status='valid' になること。"""
    conn = init_db(db_path=Path(":memory:"))
    with conn:
        conn.execute(
            """INSERT INTO races
               (race_id, race_name, date, venue, race_number, distance, surface)
               VALUES ('202601010101','テスト','2026-01-01','中山',1,2000,'芝')"""
        )
    status = conn.execute(
        "SELECT status FROM races WHERE race_id='202601010101'"
    ).fetchone()[0]
    assert status == "valid"
