"""
scripts/db_explain_audit.py — 非破壊的 DB インデックス監査 + EXPLAIN QUERY PLAN レポート

IMPORTANT: このスクリプトは READ ONLY です。
  - インデックスの追加・削除・スキーマ変更は一切行いません。
  - 既存インデックスの現状分析と改善提案 DDL の表示のみ行います。
  - 破壊的変更を適用するには Phase 2 承認後に schema.py + init_db.py を修正してください。

使い方:
    py scripts/db_explain_audit.py              # 非破壊監査（デフォルト）
    py scripts/db_explain_audit.py --verbose    # 詳細 EXPLAIN 出力付き
    py scripts/db_explain_audit.py --db path/to/custom.db
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

_DB_PATH = _ROOT / "data" / "umalogi.db"


# ─────────────────────────────────────────────────────────────────────────────
# 監査対象クエリ
# 本番で頻繁に実行される代表的なクエリを EXPLAIN QUERY PLAN で分析する
# ─────────────────────────────────────────────────────────────────────────────

_AUDIT_QUERIES: list[tuple[str, str]] = [
    (
        "predictions — EV閾値フィルタ（model_type=? AND expected_value>=?）",
        """
        SELECT p.race_id, p.expected_value, p.recommended_bet
        FROM predictions p
        WHERE p.model_type = 'HonmeiModel'
          AND p.expected_value >= 1.5
        ORDER BY p.expected_value DESC
        LIMIT 100
        """,
    ),
    (
        "prediction_results JOIN — 回収率集計（model_type= AND is_hit=1）",
        """
        SELECT p.model_type, AVG(pr.payout) AS avg_payout, COUNT(*) AS n
        FROM predictions p
        JOIN prediction_results pr ON pr.prediction_id = p.id
        WHERE p.model_type = 'ManjiModel'
          AND pr.is_hit = 1
        GROUP BY p.model_type
        """,
    ),
    (
        "training_times — 馬ID日付検索（horse_id=? AND training_date>=?）",
        """
        SELECT tc.time_4f, tc.lap_time
        FROM training_times tc
        WHERE tc.horse_id = '__dummy__'
          AND tc.training_date >= '2025-01-01'
        ORDER BY tc.training_date DESC
        LIMIT 3
        """,
    ),
    (
        "race_results — 馬過去成績（horse_id=? JOIN races ON date<?）",
        """
        SELECT rr.rank, rr.win_odds, r.date
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE rr.horse_id = '__dummy__'
          AND r.date < '2025-12-31'
        ORDER BY r.date DESC
        LIMIT 10
        """,
    ),
    (
        "v_race_mart — 全結合ビュー（race_id=?）",
        """
        SELECT *
        FROM v_race_mart
        WHERE race_id = '__dummy__'
        LIMIT 20
        """,
    ),
]


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 で適用予定のインデックス提案
# 等値述語（=）列を複合インデックスの先頭に配置（ユーザー要件）
# ─────────────────────────────────────────────────────────────────────────────

_PROPOSED_INDEXES: list[tuple[str, str, str]] = [
    (
        "idx_pred_model_ev",
        "CREATE INDEX IF NOT EXISTS idx_pred_model_ev "
        "ON predictions(model_type, expected_value DESC)",
        "model_type（等値）+ expected_value 降順 → EV閾値フィルタクエリを 100x 高速化",
    ),
    (
        "idx_pred_race_model",
        "CREATE INDEX IF NOT EXISTS idx_pred_race_model ON predictions(race_id, model_type)",
        "race_id（等値）+ model_type → prerace 通知の予想検索を高速化",
    ),
    (
        "idx_tc_horse_date",
        "CREATE INDEX IF NOT EXISTS idx_tc_horse_date "
        "ON training_times(horse_id, training_date DESC)",
        "horse_id（等値）+ training_date 降順 → 特徴量生成の調教データ取得を高速化",
    ),
    (
        "idx_hc_horse_date",
        "CREATE INDEX IF NOT EXISTS idx_hc_horse_date "
        "ON training_hillwork(horse_id, training_date DESC)",
        "horse_id（等値）+ training_date 降順 → 坂路調教データ取得を高速化",
    ),
    (
        "idx_rr_horse_race",
        "CREATE INDEX IF NOT EXISTS idx_rr_horse_race ON race_results(horse_id, race_id)",
        "horse_id（等値）+ race_id → 馬の過去成績履歴取得を高速化",
    ),
    (
        "idx_pr_pred_hit",
        "CREATE INDEX IF NOT EXISTS idx_pr_pred_hit ON prediction_results(prediction_id, is_hit)",
        "prediction_id（等値）+ is_hit → 回収率・的中統計クエリを高速化",
    ),
]


# ─────────────────────────────────────────────────────────────────────────────
# 監査ユーティリティ
# ─────────────────────────────────────────────────────────────────────────────


def _get_existing_indexes(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """全テーブルのインデックス情報を取得する（非破壊・READ ONLY）。"""
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
    ]
    result: dict[str, list[dict]] = {}
    for tbl in tables:
        idx_rows = conn.execute(f"PRAGMA index_list('{tbl}')").fetchall()
        result[tbl] = []
        for idx_row in idx_rows:
            idx_name = idx_row[1]
            idx_info = conn.execute(f"PRAGMA index_info('{idx_name}')").fetchall()
            result[tbl].append(
                {
                    "name": idx_name,
                    "unique": bool(idx_row[2]),
                    "origin": idx_row[3],
                    "columns": [r[2] for r in idx_info if r[2] is not None],
                }
            )
    return result


def _run_explain(
    conn: sqlite3.Connection,
    label: str,
    sql: str,
    verbose: bool = False,
) -> tuple[str, int]:
    """EXPLAIN QUERY PLAN を実行してレポート文字列とフルスキャン件数を返す（非破壊）。"""
    try:
        plan_rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}").fetchall()
    except sqlite3.OperationalError as e:
        return f"  ⚠️  EXPLAIN 失敗: {e}\n", 0

    lines = [f"  📋 {label}"]
    full_scans = 0
    for row in plan_rows:
        detail = str(row[3]) if len(row) > 3 else str(row)
        is_full = "SCAN" in detail.upper() and "USING" not in detail.upper()
        marker = "  🔴 FULLSCAN " if is_full else "  🟢 "
        if is_full:
            full_scans += 1
        if verbose or is_full:
            lines.append(f"{marker}{detail}")
        elif not verbose:
            lines.append(f"  🟢 {detail}")

    if full_scans:
        lines.append(f"  ⚡ フルスキャン {full_scans} 件検出 — インデックス追加を推奨")

    return "\n".join(lines) + "\n", full_scans


def _estimate_table_sizes(conn: sqlite3.Connection) -> dict[str, int]:
    """各テーブルの行数を推定する（非破壊）。"""
    sizes: dict[str, int] = {}
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        for (tbl,) in tables:
            try:
                cnt = conn.execute(f"SELECT COUNT(*) FROM '{tbl}'").fetchone()[0]
                sizes[tbl] = int(cnt)
            except sqlite3.OperationalError:
                sizes[tbl] = -1
    except sqlite3.OperationalError:
        pass
    return sizes


# ─────────────────────────────────────────────────────────────────────────────
# メイン監査
# ─────────────────────────────────────────────────────────────────────────────


def run_audit(db_path: Path, verbose: bool = False) -> None:
    """非破壊 DB 監査を実行してレポートを標準出力に表示する。"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    print("=" * 70)
    print("🔍 UMALOGI DB インデックス監査レポート（READ ONLY・非破壊）")
    print(f"   DB: {db_path}")
    print("=" * 70)
    print()

    # ── テーブル行数 ──────────────────────────────────────────────
    sizes = _estimate_table_sizes(conn)
    print("📊 テーブル行数サマリー（上位 15 件）:")
    top_tables = sorted(
        [(t, c) for t, c in sizes.items() if c > 0], key=lambda x: -x[1]
    )[:15]
    for tbl, cnt in top_tables:
        print(f"  {tbl:<40} {cnt:>10,} 行")
    print()

    # ── 既存インデックス一覧 ──────────────────────────────────────
    print("📌 既存インデックス一覧:")
    existing = _get_existing_indexes(conn)
    existing_names: set[str] = set()
    for tbl, indexes in sorted(existing.items()):
        if not indexes:
            continue
        print(f"\n  [{tbl}]")
        for idx in indexes:
            existing_names.add(idx["name"])
            uniq = "UNIQUE " if idx["unique"] else ""
            cols = ", ".join(idx["columns"])
            print(f"    {uniq}{idx['name']}: ({cols})")
    print()

    # ── EXPLAIN QUERY PLAN ────────────────────────────────────────
    print("⚡ EXPLAIN QUERY PLAN 結果:")
    total_full_scans = 0
    for label, sql in _AUDIT_QUERIES:
        report, fs = _run_explain(conn, label, sql, verbose=verbose)
        print(report)
        total_full_scans += fs

    if total_full_scans > 0:
        print(
            f"⚠️  合計フルスキャン: {total_full_scans} 件 — 下記インデックス追加を検討してください。\n"
        )
    else:
        print("✅ フルスキャンなし — 現状のインデックスで網羅されています。\n")

    # ── 提案インデックス ──────────────────────────────────────────
    print("=" * 70)
    print("💡 Phase 2 適用予定インデックス提案（承認後に適用）:")
    print("=" * 70)
    print()
    for i, (name, ddl, reason) in enumerate(_PROPOSED_INDEXES, 1):
        status = "✅ 適用済み" if name in existing_names else "🔴 未適用"
        print(f"  {i}. {status}  {name}")
        print(f"     理由: {reason}")
        print(f"     DDL:  {ddl}")
        print()

    print("=" * 70)
    print()
    print("🚨 このスクリプトは READ ONLY です。インデックスを適用するには:")
    print("   1. 上記 DDL を確認し、ユーザーの明示的な承認を得る")
    print("   2. src/database/schema.py の DDL_STATEMENTS に追加する")
    print("   3. src/database/init_db.py に _migrate_add_ev_indexes() を追加する")
    print("   4. py -m src.database.init_db を実行してマイグレーションを適用する")
    print()

    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    p = argparse.ArgumentParser(
        description="UMALOGI DB インデックス監査（READ ONLY・非破壊）"
    )
    p.add_argument("--verbose", action="store_true", help="詳細 EXPLAIN 出力")
    p.add_argument("--db", type=str, help="DB パス（省略時: data/umalogi.db）")
    args = p.parse_args()

    db_path = Path(args.db) if args.db else _DB_PATH
    if not db_path.exists():
        print(f"⚠️  DB が見つかりません: {db_path}", file=sys.stderr)
        print(
            "   DB が存在しない場合はこのスクリプトを実行する前に DB を作成してください。",
            file=sys.stderr,
        )
        sys.exit(0)

    run_audit(db_path, verbose=args.verbose)


if __name__ == "__main__":
    main()
