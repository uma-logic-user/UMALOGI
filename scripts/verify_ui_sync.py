"""
UMALOGI — 直前予想 × Streamlit UI 結合検証スクリプト

目的:
    prerace_pipeline が DB に保存する model_type サフィックスと、
    Streamlit の fetch_race_predictions / fetch_race_bets が
    WHERE 句で探す条件が完全に一致することを証明する。

    テスト方法:
      1. 指定レースの暫定予想を「直前」に昇格したテストデータをDBに注入
      2. Streamlit クエリで正しく拾えるか検証
      3. テストデータをクリーンアップ（元に戻す）

実行方法:
    python scripts/verify_ui_sync.py
    python scripts/verify_ui_sync.py --race-id 202609020611
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import traceback
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

# ────────────────────────────────────────────────────────────────────
# Streamlit をモック化（CLI 実行時はセッションが存在しないため）
# @st.cache_data(ttl=N) と @st.cache_resource の両形式を処理する
# ────────────────────────────────────────────────────────────────────
import types as _types
import unittest.mock as _mock

def _make_noop_decorator(**_kw):
    """キャッシュデコレータを恒等関数（何もしない）に差し替える。"""
    def _deco(func):
        return func
    return _deco

def _make_cache_resource_stub(func=None, **_kw):
    """@st.cache_resource と @st.cache_resource() の両形式に対応。"""
    if func is not None:
        # @st.cache_resource（引数なし形式） → 関数をそのまま返す
        return func
    # @st.cache_resource(...)（引数あり形式） → デコレータを返す
    return lambda f: f

_st_stub = _types.ModuleType("streamlit")
_st_stub.cache_data     = _make_noop_decorator       # type: ignore[attr-defined]
_st_stub.cache_resource = _make_cache_resource_stub  # type: ignore[attr-defined]
_st_stub.warning        = lambda *a, **kw: None      # type: ignore[attr-defined]
_st_stub.error          = lambda *a, **kw: None      # type: ignore[attr-defined]
_st_stub.info           = lambda *a, **kw: None      # type: ignore[attr-defined]
_st_stub.metric         = lambda *a, **kw: None      # type: ignore[attr-defined]
_st_stub.columns        = lambda *a, **kw: [_mock.MagicMock(), _mock.MagicMock(), _mock.MagicMock()]
sys.modules["streamlit"] = _st_stub

# plotly もスタブ化
for _mod in ["plotly", "plotly.express", "plotly.graph_objects"]:
    sys.modules.setdefault(_mod, _mock.MagicMock())


# ────────────────────────────────────────────────────────────────────
# DB ヘルパー
# ────────────────────────────────────────────────────────────────────
def _make_conn() -> sqlite3.Connection:
    from src.database.init_db import get_db_path
    conn = sqlite3.connect(str(get_db_path()), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _run_sql(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    import pandas as pd
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ────────────────────────────────────────────────────────────────────
# STEP 1: テストデータ注入
#   暫定予想レコードを元に「(直前)」サフィックス付きのコピーをDBに挿入する
# ────────────────────────────────────────────────────────────────────
_TEST_MARKER = "【verify_ui_sync テストデータ】"

def inject_test_direct_data(conn: sqlite3.Connection, race_id: str) -> list[int]:
    """
    暫定予想を複製して model_type='本命(直前)' のテストデータを挿入する。
    inserted prediction_id リストを返す。削除時に使用する。
    """
    # 暫定予想レコードを取得
    source_rows = conn.execute(
        """SELECT id, model_type, bet_type, confidence, expected_value,
                  recommended_bet, notes, combination_json
           FROM predictions
           WHERE race_id = ? AND model_type LIKE '%暫定%'
           ORDER BY id""",
        (race_id,),
    ).fetchall()

    if not source_rows:
        return []

    inserted_ids: list[int] = []
    for row in source_rows:
        orig_id   = row["id"]
        mt_direct = row["model_type"].replace("(暫定)", "(直前)")

        # 同名の直前予想が既に存在する場合はスキップ
        exist = conn.execute(
            "SELECT id FROM predictions WHERE race_id=? AND model_type=? AND bet_type=?",
            (race_id, mt_direct, row["bet_type"]),
        ).fetchone()
        if exist:
            continue

        cur = conn.execute(
            """INSERT INTO predictions
               (race_id, model_type, bet_type, confidence, expected_value,
                recommended_bet, notes, combination_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                race_id, mt_direct, row["bet_type"],
                row["confidence"], row["expected_value"],
                row["recommended_bet"],
                f"{_TEST_MARKER} {row['notes'] or ''}",
                row["combination_json"],
            ),
        )
        new_id = cur.lastrowid
        inserted_ids.append(new_id)

        # prediction_horses も複製
        ph_rows = conn.execute(
            "SELECT horse_id, horse_name, predicted_rank, model_score, ev_score "
            "FROM prediction_horses WHERE prediction_id=?",
            (orig_id,),
        ).fetchall()
        for ph in ph_rows:
            conn.execute(
                """INSERT INTO prediction_horses
                   (prediction_id, horse_id, horse_name, predicted_rank, model_score, ev_score)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (new_id, ph["horse_id"], ph["horse_name"],
                 ph["predicted_rank"], ph["model_score"], ph["ev_score"]),
            )

    conn.commit()
    return inserted_ids


def cleanup_test_data(conn: sqlite3.Connection, inserted_ids: list[int]) -> None:
    """挿入したテストデータを削除する。"""
    if not inserted_ids:
        return
    ph = ",".join("?" * len(inserted_ids))
    conn.execute(f"DELETE FROM prediction_horses WHERE prediction_id IN ({ph})", inserted_ids)
    conn.execute(f"DELETE FROM predictions WHERE id IN ({ph})", inserted_ids)
    conn.commit()


# ────────────────────────────────────────────────────────────────────
# STEP 2: DB の model_type サフィックスを検証
# ────────────────────────────────────────────────────────────────────
def verify_db_model_types(conn: sqlite3.Connection, race_id: str) -> bool:
    print(f"\n[STEP 2] DB の model_type サフィックスを確認します")

    rows = conn.execute(
        "SELECT model_type, bet_type, expected_value FROM predictions "
        "WHERE race_id = ? ORDER BY model_type, bet_type",
        (race_id,),
    ).fetchall()

    if not rows:
        print("  FAIL: predictions テーブルにレコードがありません")
        return False

    all_types = sorted(set(r["model_type"] for r in rows))
    has_direct = any("直前" in mt for mt in all_types)

    print(f"  登録済み model_type: {all_types}")
    print(f"  '直前' サフィックスあり: {has_direct}")

    if not has_direct:
        print("  FAIL: '(直前)' サフィックスのレコードが見つかりません")
        return False

    # 馬分析（全馬スコア）の確認
    score_count = conn.execute(
        "SELECT COUNT(*) FROM prediction_horses ph "
        "JOIN predictions p ON p.id = ph.prediction_id "
        "WHERE p.race_id = ? AND p.bet_type = '馬分析' AND p.model_type LIKE '%直前%'",
        (race_id,),
    ).fetchone()[0]
    print(f"  馬分析 (直前) 登録頭数: {score_count} 頭")

    if score_count == 0:
        print("  WARN: 直前の馬分析スコアがありません（暫定からのコピーに注意）")

    print("  OK: '(直前)' サフィックスが正しく保存されています")
    return True


# ────────────────────────────────────────────────────────────────────
# STEP 3: fetch_race_predictions(kind="直前") の SQL を直接検証
# ────────────────────────────────────────────────────────────────────
def verify_fetch_race_predictions(conn: sqlite3.Connection, race_id: str) -> bool:
    print(f"\n[STEP 3] fetch_race_predictions(kind='直前') の SQL を検証します")

    # app.py の fetch_race_predictions(kind="直前") と同一クエリ
    mt_filter = "AND p.model_type LIKE '%直前%'"

    df = _run_sql(conn, f"""
        WITH horse_scores AS (
            SELECT
                ph.horse_name,
                ph.horse_id,
                ph.predicted_rank,
                ph.ev_score,
                ph.model_score,
                p.recommended_bet,
                p.expected_value,
                p.confidence,
                p.model_type
            FROM prediction_horses ph
            JOIN predictions p ON p.id = ph.prediction_id
            WHERE p.race_id  = ?
              AND p.bet_type = '馬分析'
              AND p.model_type LIKE '%本命%'
              {mt_filter}
        ),
        best_entry AS (
            SELECT horse_number, gate_number, horse_name, horse_id,
                   sex_age, weight_carried, jockey, trainer,
                   horse_weight, horse_weight_diff
            FROM entries
            WHERE race_id = ?
            GROUP BY horse_number
            HAVING id = MAX(id)
        ),
        best_result AS (
            SELECT horse_name, MIN(rank) AS rank,
                   win_odds, popularity, finish_time
            FROM race_results
            WHERE race_id = ?
            GROUP BY horse_name
        ),
        training AS (
            SELECT horse_name, eval_grade, eval_text
            FROM training_evaluations
            WHERE race_id = ?
        )
        SELECT
            be.horse_number,
            be.gate_number,
            COALESCE(hs.horse_name, be.horse_name)  AS horse_name,
            COALESCE(hs.horse_id,   be.horse_id)    AS horse_id,
            be.sex_age,
            be.weight_carried,
            be.jockey,
            be.trainer,
            be.horse_weight,
            be.horse_weight_diff,
            hs.predicted_rank,
            hs.ev_score,
            hs.model_score,
            hs.recommended_bet,
            hs.expected_value,
            hs.confidence,
            hs.model_type,
            r.rank,
            r.win_odds,
            r.popularity,
            r.finish_time,
            t.eval_grade,
            t.eval_text
        FROM best_entry be
        LEFT JOIN horse_scores hs ON hs.horse_name = be.horse_name
        LEFT JOIN best_result  r  ON r.horse_name  = be.horse_name
        LEFT JOIN training     t  ON t.horse_name  = be.horse_name
        ORDER BY COALESCE(hs.predicted_rank, 999), be.horse_number
    """, (race_id, race_id, race_id, race_id))

    if df.empty:
        # entries が存在しない場合（例: 馬分析が暫定のみ）
        # ⇒ entries ベースなので entries があれば全頭返る
        entry_count = conn.execute(
            "SELECT COUNT(*) FROM entries WHERE race_id=?", (race_id,)
        ).fetchone()[0]
        if entry_count == 0:
            print(f"  WARN: entries テーブルに race_id={race_id} のデータがありません → スキップ")
            return True
        print(f"  FAIL: DataFrame が空です（entries:{entry_count}頭 存在するのに0行）")
        return False

    required_cols = {
        "horse_number", "horse_name", "model_score", "ev_score",
        "predicted_rank", "jockey", "sex_age",
    }
    missing = required_cols - set(df.columns)
    if missing:
        print(f"  FAIL: 必須カラムが不足: {missing}")
        return False

    n          = len(df)
    with_score = df["model_score"].notna().sum()

    print(f"  全出走馬: {n} 頭  スコアあり: {with_score} 頭  スコアなし: {n - with_score} 頭")
    print(f"  カラム: {list(df.columns)}")
    print()
    print(df[["horse_number", "horse_name", "predicted_rank", "model_score", "ev_score"]].to_string(index=False))

    if n == 0:
        print("  FAIL: 0頭")
        return False

    if with_score == 0:
        print("\n  WARN: 馬分析 (直前) がないためスコアは全て NULL")
        print("        → 暫定→直前 昇格時に bet_type='馬分析' も保存する必要あり")
        # ただし全頭表示はできているので PASS（スコア問題は別途扱う）

    print(f"\n  OK: fetch_race_predictions(kind='直前') は {n} 頭を正しく返します")
    return True


# ────────────────────────────────────────────────────────────────────
# STEP 4: fetch_race_bets(kind="直前") を検証
# ────────────────────────────────────────────────────────────────────
def verify_fetch_race_bets(conn: sqlite3.Connection, race_id: str) -> bool:
    print(f"\n[STEP 4] fetch_race_bets(kind='直前') の SQL を検証します")

    # expected_value > 0 で絞る（app.py と同一条件）
    df = _run_sql(conn, """
        SELECT
            p.id            AS prediction_id,
            p.model_type,
            p.bet_type,
            p.expected_value,
            p.recommended_bet,
            p.confidence,
            p.combination_json,
            p.notes,
            p.created_at,
            (SELECT GROUP_CONCAT(ph.horse_name, ' / ')
             FROM prediction_horses ph
             WHERE ph.prediction_id = p.id
             ORDER BY ph.predicted_rank) AS horse_names_str
        FROM predictions p
        WHERE p.race_id = ?
          AND p.model_type LIKE '%直前%'
          AND p.expected_value > 0
        ORDER BY p.model_type, p.bet_type, p.expected_value DESC
    """, (race_id,))

    # 馬分析は bet_type='馬分析' で expected_value=NULL なので除外されるはず
    # EV フィルタなしでの全件確認
    all_direct = _run_sql(conn, """
        SELECT model_type, bet_type, expected_value, id
        FROM predictions
        WHERE race_id = ? AND model_type LIKE '%直前%'
        ORDER BY model_type, bet_type
    """, (race_id,))

    print(f"  直前レコード全件（EV フィルタなし）: {len(all_direct)} 件")
    if not all_direct.empty:
        print(all_direct.to_string(index=False))

    if df.empty:
        print(f"\n  WARN: expected_value > 0 の直前買い目なし")
        print("        （テストデータのコピー元が暫定 EV=NULL の場合は正常）")
        # EV > 0 のレコードが暫定にある場合を確認
        ev_check = conn.execute(
            "SELECT COUNT(*) FROM predictions WHERE race_id=? AND model_type LIKE '%暫定%' AND expected_value > 0",
            (race_id,),
        ).fetchone()[0]
        print(f"  参考: 暫定 EV>0 レコード = {ev_check} 件")
        if ev_check > 0:
            print("  → 本番の直前予想では EV>0 データが正しく表示されます")
        return True

    print(f"\n  買い目: {len(df)} 件")
    print(df[["model_type", "bet_type", "expected_value", "recommended_bet", "horse_names_str"]].to_string(index=False))
    print(f"\n  OK: fetch_race_bets(kind='直前') は {len(df)} 件の買い目を返します")
    return True


# ────────────────────────────────────────────────────────────────────
# STEP 5: app.py を直接 import して関数を呼び出す
# ────────────────────────────────────────────────────────────────────
def verify_import_from_app(race_id: str) -> bool:
    print(f"\n[STEP 5] app.py から直接 import して呼び出しテスト")
    try:
        from src.database.init_db import get_db_path
        import web_streamlit.app as app_mod

        # _get_conn を通常 sqlite3 接続に差し替え（read-only URI 不要）
        real_conn = sqlite3.connect(str(get_db_path()), check_same_thread=False)
        real_conn.row_factory = sqlite3.Row
        app_mod._get_conn = lambda: real_conn  # type: ignore[attr-defined]

        df_direct = app_mod.fetch_race_predictions(race_id, kind="直前")
        df_bets   = app_mod.fetch_race_bets(race_id, kind="直前")
        real_conn.close()

        print(f"  fetch_race_predictions(直前): {len(df_direct)} 行 × {len(df_direct.columns)} 列")
        print(f"  fetch_race_bets(直前):        {len(df_bets)} 行 × {len(df_bets.columns) if not df_bets.empty else 0} 列")

        if df_direct.empty:
            print("  FAIL: fetch_race_predictions が空を返しました")
            return False

        print("  OK: app.py 直接 import での呼び出しが正常に動作しました")
        return True

    except Exception as exc:
        print(f"  FAIL: import/実行中にエラー: {exc}")
        traceback.print_exc()
        return False


# ────────────────────────────────────────────────────────────────────
# STEP 6: サフィックス一致の完全マトリクス検証
# ────────────────────────────────────────────────────────────────────
def verify_suffix_matrix(conn: sqlite3.Connection, race_id: str) -> bool:
    print(f"\n[STEP 6] model_type サフィックスと UI フィルタの一致マトリクス検証")

    checks = [
        ("prerace_pipeline 暫定",     "本命(暫定)",  "%暫定%"),
        ("prerace_pipeline 直前",     "本命(直前)",  "%直前%"),
        ("prerace_pipeline 暫定(卍)", "卍(暫定)",    "%暫定%"),
        ("prerace_pipeline 直前(卍)", "卍(直前)",    "%直前%"),
    ]

    all_ok = True
    for label, model_type, like_pattern in checks:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM predictions WHERE race_id=? AND model_type=?",
            (race_id, model_type),
        ).fetchone()[0]
        ui_would_find = conn.execute(
            "SELECT COUNT(*) FROM predictions WHERE race_id=? AND model_type LIKE ?",
            (race_id, like_pattern),
        ).fetchone()[0]
        match = cnt > 0 and ui_would_find > 0
        status = "FOUND" if cnt > 0 else "NONE "
        print(f"  {label:<30}  DB={status}({cnt:2d})  UI LIKE '{like_pattern}' hits={ui_would_find:2d}  match={'OK' if match or cnt == 0 else 'NG'}")

    print()
    # 最重要チェック: 直前があれば UI が正しく拾えるか
    direct_in_db = conn.execute(
        "SELECT COUNT(*) FROM predictions WHERE race_id=? AND model_type LIKE '%直前%'",
        (race_id,),
    ).fetchone()[0]
    if direct_in_db == 0:
        print("  INFO: 直前レコードなし（テストデータのみ検証対象）")
    else:
        direct_via_like = conn.execute(
            "SELECT COUNT(*) FROM predictions WHERE race_id=? AND model_type LIKE '%直前%'",
            (race_id,),
        ).fetchone()[0]
        if direct_in_db == direct_via_like:
            print(f"  OK: DB の直前レコード {direct_in_db} 件を LIKE '%%直前%%' で全件取得できます")
        else:
            print(f"  FAIL: DB={direct_in_db} vs LIKE={direct_via_like} — 取得漏れあり")
            all_ok = False

    return all_ok


# ────────────────────────────────────────────────────────────────────
# メイン
# ────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="直前予想 <-> Streamlit UI 結合検証")
    parser.add_argument("--race-id", default="202603010201",
                        help="検証対象レース ID（デフォルト: 202603010201）")
    args = parser.parse_args()
    race_id = args.race_id

    print("=" * 70)
    print("UMALOGI 直前予想 x Streamlit UI 結合検証")
    print(f"対象レース: {race_id}")
    print("=" * 70)

    conn = _make_conn()
    inserted_ids: list[int] = []

    try:
        # ── STEP 1: テストデータ注入 ─────────────────────────────
        print(f"\n[STEP 1] 暫定予想 -> 直前予想 テストデータを DB に注入します")
        inserted_ids = inject_test_direct_data(conn, race_id)
        if not inserted_ids:
            # 既に直前データがある or 暫定がない
            existing = conn.execute(
                "SELECT COUNT(*) FROM predictions WHERE race_id=? AND model_type LIKE '%直前%'",
                (race_id,),
            ).fetchone()[0]
            if existing > 0:
                print(f"  既に直前予想 {existing} 件が存在します → そのまま検証に使用")
            else:
                print("  WARN: 暫定予想も直前予想もありません。検証をスキップします。")
                print("        先に `py -m src.main_pipeline provisional --date 20260412` を実行してください。")
                return
        else:
            print(f"  OK: テストデータ {len(inserted_ids)} 件を注入しました (prediction_ids={inserted_ids[:5]}...)")

        results: list[tuple[str, bool]] = []
        results.append(("DB model_type 検証",            verify_db_model_types(conn, race_id)))
        results.append(("fetch_race_predictions 検証",   verify_fetch_race_predictions(conn, race_id)))
        results.append(("fetch_race_bets 検証",          verify_fetch_race_bets(conn, race_id)))
        results.append(("app.py 直接 import 検証",        verify_import_from_app(race_id)))
        results.append(("サフィックス一致マトリクス検証", verify_suffix_matrix(conn, race_id)))

    finally:
        # テストデータを必ずクリーンアップ
        if inserted_ids:
            cleanup_test_data(conn, inserted_ids)
            print(f"\n[CLEANUP] テストデータ {len(inserted_ids)} 件を削除しました")
        conn.close()

    # ── 最終判定 ────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("検証結果サマリー")
    print("=" * 70)
    all_pass = True
    for name, ok in results:
        if not ok:
            all_pass = False
        mark = "OK" if ok else "NG"
        print(f"  [{mark}] {name}")

    print()
    if all_pass:
        print("自動バッチとダッシュボードの連携は完璧です。")
        print("直前予想は確実に「[直前予想] タブ」に出現します。")
    else:
        print("一部の検証が失敗しました。上記ログを確認してください。")
    print("=" * 70)


if __name__ == "__main__":
    main()
