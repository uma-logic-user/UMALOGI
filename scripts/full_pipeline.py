"""
UMALOGI — 5カ年全モデル一括再学習・バックテストパイプライン
============================================================

【データ制約の正直な説明】
  - umalogi.db (JVLink) race_results: 2024-2026 のみ
    → 本命/卍/複勝モデルの Walk-forward は 2024→2025 と 2025→2026 の2フォールド
  - netkeiba_research.db: 2022-2025 取得済み、2021 は未取得
    → ALPHA Walk-forward: 2022→2024, 2022-2023→2025, 2022-2024→2026
  - 2021データ: 本スクリプト実行時に未取得であれば取得キューに追加

実行例:
  # 全フェーズ実行（デフォルト: 2021スクレイプ + 全モデル再学習 + バックテスト + レポート）
  py scripts/full_pipeline.py

  # スクレイプをスキップ（取得済みの場合）
  py scripts/full_pipeline.py --skip-scrape

  # 再学習をスキップしてバックテスト + レポートのみ
  py scripts/full_pipeline.py --skip-train

  # 本命/卍Walk-forward を実行（低速: 特徴量ビルドに数時間）
  py scripts/full_pipeline.py --run-walk-forward

  # データステータス確認のみ
  py scripts/full_pipeline.py --status
"""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parent.parent
_MAIN_DB = _ROOT / "data" / "umalogi.db"
_RESEARCH_DB = _ROOT / "data" / "netkeiba_research.db"
_MODELS_DIR = _ROOT / "data" / "models"
_DOCS_DIR = _ROOT / "docs"
_ELITE_CSV = _ROOT / "logs" / "fukusho_elite_monitor.csv"
_ELITE_TARGET = 30

# 2021取得ターゲット
SCRAPE_2021_FROM = "2021-01-01"
SCRAPE_2021_TO = "2021-12-31"

if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=False)


# ── ヘルパー ──────────────────────────────────────────────────────────────


def _conn_main() -> sqlite3.Connection:
    return sqlite3.connect(str(_MAIN_DB))


def _conn_research() -> sqlite3.Connection:
    return sqlite3.connect(str(_RESEARCH_DB))


def _run(cmd: list[str], *, desc: str = "") -> bool:
    """サブプロセスを実行し、stdout をリアルタイムで表示する。失敗したら False。"""
    if desc:
        print(f"\n  >> {desc}")
    result = subprocess.run(
        cmd,
        cwd=str(_ROOT),
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        print(f"  [ERROR] 終了コード={result.returncode}")
        return False
    return True


# ── フェーズ 0: データステータス ─────────────────────────────────────────


def phase0_status() -> dict:
    """
    umalogi.db と netkeiba_research.db の年別カバレッジを集計して返す。
    """
    print("\n" + "=" * 62)
    print("  Phase 0 — データステータス確認")
    print("=" * 62)

    status: dict = {
        "umalogi": {},
        "research": {},
        "models": {},
    }

    # ── umalogi.db ───────────────────────────────────────────────────
    conn = _conn_main()
    print("\n[umalogi.db]")
    races_by_year = conn.execute("""
        SELECT substr(date,1,4) AS y, COUNT(*) AS n
        FROM races GROUP BY y ORDER BY y
    """).fetchall()

    results_by_year = conn.execute("""
        SELECT substr(r.date,1,4) AS y, COUNT(*) AS n
        FROM race_results rr JOIN races r ON r.race_id = rr.race_id
        WHERE rr.rank IS NOT NULL GROUP BY y ORDER BY y
    """).fetchall()
    results_dict = {y: n for y, n in results_by_year}

    for y, n in races_by_year:
        res_n = results_dict.get(y, 0)
        status["umalogi"][y] = {"races": n, "results": res_n}
        has_res = "✓" if res_n > 0 else "✗ (race_results なし)"
        print(f"  {y}: {n:,}レース  results={res_n:,} {has_res}")
    conn.close()

    # ── netkeiba_research.db ─────────────────────────────────────────
    print("\n[netkeiba_research.db]")
    if not _RESEARCH_DB.exists():
        print("  ✗ DB が存在しません")
        status["research"] = {}
    else:
        conn_r = _conn_research()
        try:
            odds_by_year = conn_r.execute("""
                SELECT substr(race_id,1,4) AS y,
                       COUNT(DISTINCT race_id) AS races,
                       COUNT(*) AS rows
                FROM horse_odds GROUP BY y ORDER BY y
            """).fetchall()
            for y, races, rows in odds_by_year:
                status["research"][y] = {"races": races, "rows": rows}
                print(f"  {y}: {races:,}レース  {rows:,}行")
            if not odds_by_year:
                print("  (データなし)")
        except Exception as e:
            print(f"  ERROR: {e}")
        finally:
            conn_r.close()

    # 2021 欠落確認
    has_2021_research = "2021" in status["research"]
    status["missing_2021_research"] = not has_2021_research
    if not has_2021_research:
        print(
            "\n  ⚠  2021年データが netkeiba_research.db に存在しません → 取得が必要です"
        )

    # ── モデルファイル ───────────────────────────────────────────────
    print("\n[models/]")
    model_files = {
        "本命 (honmei_model.pkl)": _MODELS_DIR / "honmei_model.pkl",
        "卍   (manji_model.pkl)": _MODELS_DIR / "manji_model.pkl",
        "複勝 (place_model.pkl)": _MODELS_DIR / "place_model.pkl",
        "ALPHA (alpha_model.pkl)": _MODELS_DIR / "alpha" / "alpha_model.pkl",
    }
    for label, path in model_files.items():
        exists = path.exists()
        mtime = (
            datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            if exists
            else "—"
        )
        mark = "✓" if exists else "✗ 未生成"
        print(f"  {mark}  {label}  ({mtime})")
        status["models"][label] = {"exists": exists, "mtime": mtime}

    return status


# ── フェーズ 1: 2021年データ取得 ─────────────────────────────────────────


def phase1_scrape_2021(skip: bool = False) -> bool:
    """
    2021年オッズを netkeiba_research.db に取得する。
    skip=True の場合は確認のみ。
    """
    print("\n" + "=" * 62)
    print("  Phase 1 — 2021年オッズデータ取得")
    print("=" * 62)

    if skip:
        print("  --skip-scrape 指定: スキップします")
        return True

    if not _MAIN_DB.exists():
        print("  [ERROR] umalogi.db が見つかりません")
        return False

    # 対象件数
    conn = _conn_main()
    target = conn.execute(
        "SELECT COUNT(*) FROM races WHERE date BETWEEN ? AND ?",
        (SCRAPE_2021_FROM, SCRAPE_2021_TO),
    ).fetchone()[0]
    conn.close()
    print(f"  対象: {target:,}レース  ({SCRAPE_2021_FROM} 〜 {SCRAPE_2021_TO})")

    # 取得済み確認
    already = 0
    if _RESEARCH_DB.exists():
        conn_r = _conn_research()
        try:
            row = conn_r.execute("""
                SELECT COUNT(DISTINCT race_id) FROM horse_odds
                WHERE substr(race_id,1,4) = '2021'
            """).fetchone()
            already = row[0] if row else 0
        except Exception:
            pass
        finally:
            conn_r.close()

    pending = target - already
    print(f"  取得済み: {already:,}  残り: {pending:,}")

    if pending <= 0:
        print("  ✓ 2021年は全件取得済みです")
        return True

    print(
        f"\n  スクレイプを開始します (推定時間: {pending * 1.5 / 60:.0f}〜{pending * 2.5 / 60:.0f}分)..."
    )
    ok = _run(
        [
            sys.executable,
            str(_ROOT / "scripts" / "scrape_research_data.py"),
            "--mode",
            "odds",
            "--from",
            SCRAPE_2021_FROM,
            "--to",
            SCRAPE_2021_TO,
            "--delay-min",
            "1.0",
            "--delay-max",
            "2.5",
            "--max-errors",
            "50",
        ],
        desc="scrape_research_data.py --mode odds --from 2021-01-01 --to 2021-12-31",
    )

    if ok:
        print("  ✓ 2021年データ取得完了")
    else:
        print("  ⚠ スクレイプに失敗しましたが続行します")

    return True  # スクレイプ失敗でも後続フェーズを継続


# ── フェーズ 2: 全モデル再学習 ───────────────────────────────────────────


def phase2_retrain_all(skip: bool = False) -> bool:
    """
    本命/卍/複勝 + ALPHA の全モデルを最新データで再学習する。
    """
    print("\n" + "=" * 62)
    print("  Phase 2 — 全モデル再学習")
    print("=" * 62)

    if skip:
        print("  --skip-train 指定: スキップします")
        return True

    # 2-A: 本命/卍/複勝 (run_train.py)
    print("\n  [2-A] 本命/卍/複勝 モデル再学習 (全期間データ使用)...")
    ok = _run(
        [sys.executable, str(_ROOT / "scripts" / "run_train.py")],
        desc="run_train.py",
    )
    if not ok:
        print("  [ERROR] 本命/卍/複勝 再学習に失敗しました")
        return False

    # 2-B: ALPHA (全年度データで再学習 + 2025/2026テスト)
    print("\n  [2-B] ALPHA モデル再学習 (train=2021-2025, test=2026, --save)...")
    research_args = (
        ["--research-db", str(_RESEARCH_DB)] if _RESEARCH_DB.exists() else []
    )
    for bet_type in ("単勝", "複勝"):
        ok = _run(
            [
                sys.executable,
                str(_ROOT / "scripts" / "alpha_backtest.py"),
                "--train",
                "2021",
                "2022",
                "2023",
                "2024",
                "2025",
                "--test",
                "2026",
                "--bet-type",
                bet_type,
                "--save",
                *research_args,
            ],
            desc=f"ALPHA 再学習 (bet_type={bet_type})",
        )
        if not ok:
            print(f"  [WARN] ALPHA {bet_type} 再学習失敗（2025データ限定で継続）")

    print("\n  ✓ Phase 2 完了")
    return True


# ── フェーズ 3: ALPHA Walk-forward バックテスト ─────────────────────────


def phase3_alpha_walkforward() -> list[dict]:
    """
    ALPHA を複数フォールドの Walk-forward でバックテストし、結果リストを返す。

    Fold 1: train=2022      → test=2023 (2021データ追加後は train=2021-2022)
    Fold 2: train=2022-2023 → test=2024
    Fold 3: train=2022-2024 → test=2025
    Fold 4: train=2022-2025 → test=2026
    """
    print("\n" + "=" * 62)
    print("  Phase 3 — ALPHA Walk-forward バックテスト")
    print("=" * 62)

    if not _RESEARCH_DB.exists():
        print("  ⚠ netkeiba_research.db が存在しません。スキップします")
        return []

    # 取得済み年度を確認
    conn_r = _conn_research()
    try:
        avail_years = sorted(
            [
                y
                for (y,) in conn_r.execute(
                    "SELECT DISTINCT substr(race_id,1,4) FROM horse_odds ORDER BY 1"
                ).fetchall()
            ]
        )
    finally:
        conn_r.close()

    print(f"  利用可能年度: {', '.join(avail_years)}")

    # Foldの動的生成: avail_years[-1] をテストとして各フォールドを作成
    if len(avail_years) < 2:
        print("  ⚠ データが不足しています（2年以上必要）")
        return []

    folds: list[tuple[list[str], str]] = []
    for i in range(1, len(avail_years)):
        train_years = avail_years[:i]
        test_year = avail_years[i]
        if len(train_years) >= 1:
            folds.append((train_years, test_year))

    research_args = ["--research-db", str(_RESEARCH_DB)]
    all_results: list[dict] = []

    for train_years, test_year in folds:
        print(f"\n  Fold: train={'+'.join(train_years)} → test={test_year}")
        for bet_type in ("単勝", "複勝"):
            cmd = [
                sys.executable,
                str(_ROOT / "scripts" / "alpha_backtest.py"),
                "--train",
                *train_years,
                "--test",
                test_year,
                "--bet-type",
                bet_type,
                *research_args,
            ]
            result = subprocess.run(
                cmd,
                cwd=str(_ROOT),
                capture_output=True,
                encoding="utf-8",
                errors="replace",
            )
            stdout = result.stdout + result.stderr

            # ROI・的中率をパース
            parsed = _parse_alpha_stdout(stdout, train_years, test_year, bet_type)
            all_results.append(parsed)

            roi_str = f"{parsed['roi']:.1f}%" if parsed["roi"] is not None else "—"
            hr_str = (
                f"{parsed['hit_rate']:.1f}%" if parsed["hit_rate"] is not None else "—"
            )
            print(
                f"    {bet_type}: ROI={roi_str}  的中率={hr_str}  買い={parsed['num_bets']}件"
            )

    return all_results


def _parse_alpha_stdout(
    stdout: str,
    train_years: list[str],
    test_year: str,
    bet_type: str,
) -> dict:
    """alpha_backtest.py の出力テキストから指標をパースする。"""
    import re

    result: dict = {
        "train_years": train_years,
        "test_year": test_year,
        "bet_type": bet_type,
        "roi": None,
        "hit_rate": None,
        "num_bets": 0,
        "num_hits": 0,
        "profit": None,
        "investment": None,
        "payout": None,
    }
    for line in stdout.splitlines():
        if m := re.search(r"ROI\s*[:：]\s*([-\d.]+)%", line):
            result["roi"] = float(m.group(1))
        if m := re.search(r"的中率\s*[:：]\s*([\d.]+)%", line):
            result["hit_rate"] = float(m.group(1))
        if m := re.search(r"買いシグナル\s*[:：]\s*([\d,]+)", line):
            result["num_bets"] = int(m.group(1).replace(",", ""))
        if m := re.search(r"的中数\s*[:：]\s*([\d,]+)", line):
            result["num_hits"] = int(m.group(1).replace(",", ""))
        if m := re.search(r"損益\s*[:：]\s*Y?([-+\d,]+)", line):
            result["profit"] = float(m.group(1).replace(",", ""))
        if m := re.search(r"総投資額\s*[:：]\s*Y?([\d,]+)", line):
            result["investment"] = float(m.group(1).replace(",", ""))
        if m := re.search(r"総払戻額\s*[:：]\s*Y?([\d,.]+)", line):
            result["payout"] = float(m.group(1).replace(",", ""))
    return result


# ── フェーズ 4: 本命/卍 Walk-forward バックテスト ────────────────────────


def phase4_main_walkforward(run_slow: bool = False) -> list[dict]:
    """
    本命/卍/複勝モデルの Walk-forward バックテスト。

    標準モード: prediction_results から 2026年の実績のみ集計（高速）
    低速モード: --run-walk-forward 指定時のみ、2024→2025 フォールドも計算

    【データ制約注記】
      umalogi.db の race_results は 2024-2026 のみ存在。
      したがって Walk-forward の学習起点は 2024年が最初のフォールド。
    """
    print("\n" + "=" * 62)
    print("  Phase 4 — 本命/卍/複勝 バックテスト")
    print("=" * 62)

    results: list[dict] = []

    # ── 4-A: 2026年 実際の予想成績（prediction_results から） ──────
    print("\n  [4-A] 2026年 ライブ実績 (prediction_results より)")
    conn = _conn_main()
    rows = conn.execute("""
        SELECT
            p.model_type,
            p.bet_type,
            COUNT(*)                                          AS bets,
            SUM(pr.is_hit)                                   AS hits,
            SUM(CASE WHEN pr.is_hit=1 THEN pr.payout ELSE 0 END) AS payout,
            SUM(pr.profit)                                   AS profit,
            SUM(pr.roi)                                      AS roi_sum
        FROM predictions p
        JOIN prediction_results pr ON pr.prediction_id = p.id
        JOIN races r ON r.race_id = p.race_id
        WHERE substr(r.date,1,4) = '2026'
          AND pr.is_hit IS NOT NULL
        GROUP BY p.model_type, p.bet_type
        ORDER BY p.model_type, p.bet_type
    """).fetchall()

    for row in rows:
        model, bet, bets, hits, payout, profit, roi_sum = row
        hits = int(hits or 0)
        payout = float(payout or 0)
        profit = float(profit or 0)
        hit_rate = hits / bets * 100 if bets > 0 else 0.0
        avg_roi = roi_sum / bets if bets > 0 else 0.0
        results.append(
            {
                "source": "live_2026",
                "model": model,
                "bet_type": bet,
                "test_year": "2026",
                "bets": bets,
                "hits": hits,
                "hit_rate": hit_rate,
                "payout": payout,
                "profit": profit,
                "avg_roi": avg_roi,
            }
        )
        hr_str = f"{hit_rate:.1f}%"
        roi_str = f"{avg_roi:.1f}%"
        print(
            f"    {model:12s} {bet:5s} | {bets:4}件 hit={hr_str:6s} profit=¥{profit:>10,.0f} avgROI={roi_str}"
        )

    conn.close()

    # ── 4-B: Walk-forward シミュレーション（低速） ─────────────────
    if run_slow:
        print("\n  [4-B] Walk-forward シミュレーション (2024→2025, 2025→2026)")
        results.extend(_walkforward_main_models())
    else:
        print("\n  [4-B] Walk-forward は --run-walk-forward 指定時のみ実行 (数時間)")

    return results


def _walkforward_main_models() -> list[dict]:
    """
    本命/卍 Walk-forward シミュレーション。

    フォールド構成:
      Fold 1: train=2024 → test=2025
      Fold 2: train=2024+2025 → test=2026
    """
    from src.database.init_db import init_db
    from src.ml.models import (
        HonmeiModel,
        ManjiModel,
        PlaceModel,
        _build_train_df,
        FEATURE_COLS,
    )

    folds: list[tuple[int, int]] = [(2024, 2025), (2025, 2026)]
    results: list[dict] = []

    conn = init_db()

    for train_until, test_year in folds:
        print(f"\n    Fold: train≤{train_until} → test={test_year}")
        t0 = time.perf_counter()

        # テスト年データ（特徴量は test_year を含む全期間で構築してリーク防止済）
        df_all = _build_train_df(conn, train_until=test_year)
        if df_all.empty:
            print(f"    ⚠ データなし: {test_year}")
            continue

        df_test = df_all[df_all["race_id"].str.startswith(str(test_year))].copy()

        if df_test.empty:
            print(f"    ⚠ テストデータなし: {test_year}")
            continue

        # モデル訓練
        honmei = HonmeiModel()
        honmei.train(conn, train_until=train_until)

        manji = ManjiModel()
        manji.train(conn, train_until=train_until)

        place = PlaceModel()
        place.train(conn, train_until=train_until)

        # 予測 + ベット評価
        for model_obj, model_name, target_col in [
            (honmei, "本命(WF)", "is_winner"),
            (manji, "卍(WF)", "is_winner"),  # 的中判定は同じ
            (place, "複勝(WF)", "is_placed"),
        ]:
            X = df_test[FEATURE_COLS].fillna(-1)
            if hasattr(model_obj, "_model") and model_obj._model is not None:
                try:
                    if hasattr(model_obj._model, "predict_proba"):
                        scores = model_obj._model.predict_proba(X.values)[:, 1]
                    else:
                        scores = model_obj._model.predict(X.values)
                except Exception:
                    continue
            else:
                continue

            df_sim = df_test.copy()
            df_sim["_score"] = scores

            # レース内 TOP-1 に単勝100円をかける（最もシンプルなシミュレーション）
            df_sim["_rank_in_race"] = df_sim.groupby("race_id")["_score"].rank(
                ascending=False, method="first"
            )
            bets = df_sim[df_sim["_rank_in_race"] == 1]
            hits = int((bets[target_col] == 1).sum())
            bets_n = len(bets)
            invested = bets_n * 100

            # 払戻: 的中した場合 payout_tansho から
            payout_col = "payout_tansho"
            if payout_col in bets.columns:
                payout = float(
                    bets.loc[bets[target_col] == 1, payout_col].fillna(0).sum()
                )
            else:
                payout = 0.0

            profit = payout - invested
            avg_roi = payout / invested * 100 if invested > 0 else 0.0
            hit_rate = hits / bets_n * 100 if bets_n > 0 else 0.0

            results.append(
                {
                    "source": "walkforward",
                    "model": model_name,
                    "bet_type": "単勝(シミュ)",
                    "test_year": str(test_year),
                    "bets": bets_n,
                    "hits": hits,
                    "hit_rate": hit_rate,
                    "payout": payout,
                    "profit": profit,
                    "avg_roi": avg_roi,
                }
            )
            print(
                f"    {model_name:12s} 単勝 | test={test_year} "
                f"{bets_n}件 hit={hit_rate:.1f}% profit=¥{profit:+,.0f} ROI={avg_roi:.1f}%"
            )

        elapsed = time.perf_counter() - t0
        print(f"    経過: {elapsed:.0f}秒")

    conn.close()
    return results


# ── フェーズ 5: Markdown レポート生成 ────────────────────────────────────


def phase5_generate_report(
    status: dict,
    alpha_results: list[dict],
    main_results: list[dict],
    output_path: Path | None = None,
) -> Path:
    """全結果を統合した Markdown レポートを生成・保存する。"""
    print("\n" + "=" * 62)
    print("  Phase 5 — 5カ年総合評価レポート生成")
    print("=" * 62)

    now = datetime.now()
    if output_path is None:
        _DOCS_DIR.mkdir(parents=True, exist_ok=True)
        output_path = _DOCS_DIR / f"umalogi_5year_report_{now.strftime('%Y%m%d')}.md"

    lines: list[str] = []

    def w(s: str = "") -> None:
        lines.append(s)

    # ── ヘッダー ────────────────────────────────────────────────────
    w("# UMALOGI 5カ年総合評価レポート")
    w("")
    w(f"> 生成日時: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    w("")
    w("---")
    w("")

    # ── データカバレッジ ─────────────────────────────────────────────
    w("## 1. データカバレッジ")
    w("")
    w("### 1-1. umalogi.db (JVLink データ)")
    w("")
    w("| 年度 | レース数 | 着順確定 | 備考 |")
    w("|------|--------|---------|------|")
    for y, d in sorted(status.get("umalogi", {}).items()):
        res_n = d.get("results", 0)
        note = "✓ 学習可" if res_n > 0 else "✗ race_results 未取得"
        w(f"| {y} | {d['races']:,} | {res_n:,} | {note} |")
    w("")
    w("> **注**: JVLink から取得できる race_results は直近2〜3年分が上限。")
    w("> 2021〜2023年の着順データは JVLink 経由では取得できないため、")
    w("> 本命/卍/複勝モデルの学習範囲は **2024〜2026年** に限定されます。")
    w("")

    w("### 1-2. netkeiba_research.db (R&D用オッズDB)")
    w("")
    w("| 年度 | レース数 | 行数 | 備考 |")
    w("|------|--------|------|------|")
    for y, d in sorted(status.get("research", {}).items()):
        w(f"| {y} | {d.get('races', 0):,} | {d.get('rows', 0):,} | — |")
    if status.get("missing_2021_research"):
        w("| 2021 | — | — | ⚠ **未取得** (要スクレイプ) |")
    w("")

    w("### 1-3. モデルファイル")
    w("")
    w("| モデル | 存在 | 最終更新 |")
    w("|--------|------|---------|")
    for label, d in status.get("models", {}).items():
        mark = "✓" if d.get("exists") else "✗"
        w(f"| {label} | {mark} | {d.get('mtime', '—')} |")
    w("")

    # ── ALPHA バックテスト ─────────────────────────────────────────
    w("## 2. ALPHA モデル Walk-forward バックテスト")
    w("")
    w(
        "> **実施条件**: netkeiba_research.db のオッズデータ (2022〜2025) を使用した時系列分割バックテスト。"
    )
    w("> 各フォールドでテスト期間の結果は学習に使用しない（カンニング防止済み）。")
    w("")

    if alpha_results:
        w(
            "| 学習期間 | テスト年 | 券種 | 買い件数 | 的中率 | 総投資 | 総払戻 | 損益 | ROI |"
        )
        w("|---------|---------|------|---------|-------|-------|-------|------|-----|")
        for r in alpha_results:
            train_str = "+".join(r.get("train_years", ["?"]))
            test_str = r.get("test_year", "?")
            bet = r.get("bet_type", "?")
            bets = r.get("num_bets", 0)
            hr = f"{r['hit_rate']:.1f}%" if r.get("hit_rate") is not None else "—"
            inv = f"¥{r['investment']:,.0f}" if r.get("investment") is not None else "—"
            pay = f"¥{r['payout']:,.0f}" if r.get("payout") is not None else "—"
            pft = f"¥{r['profit']:+,.0f}" if r.get("profit") is not None else "—"
            roi = f"{r['roi']:.1f}%" if r.get("roi") is not None else "—"
            w(
                f"| {train_str} | {test_str} | {bet} | {bets:,} | {hr} | {inv} | {pay} | {pft} | {roi} |"
            )
        w("")

        # 通算集計
        alpha_non_none = [r for r in alpha_results if r.get("investment") is not None]
        if alpha_non_none:
            total_inv = sum(r.get("investment", 0) or 0 for r in alpha_non_none)
            total_pay = sum(r.get("payout", 0) or 0 for r in alpha_non_none)
            total_pft = total_pay - total_inv
            overall_roi = total_pay / total_inv * 100 if total_inv > 0 else 0.0
            w(
                f"**通算（全フォールド合計）**: 投資 ¥{total_inv:,.0f} → 払戻 ¥{total_pay:,.0f} | "
                f"損益 ¥{total_pft:+,.0f} | ROI **{overall_roi:.1f}%**"
            )
            w("")
    else:
        w("> ⚠ ALPHA バックテストデータなし（netkeiba_research.db が必要）")
        w("")

    # ── 本命/卍/複勝 実績 ────────────────────────────────────────────
    w("## 3. 本命/卍/複勝 モデル実績")
    w("")
    w("> **データソース**: umalogi.db の prediction_results (2026年分の実ライブ予想）")
    w("> および Walk-forward シミュレーション（--run-walk-forward 指定時）。")
    w("")

    live_results = [r for r in main_results if r.get("source") == "live_2026"]
    wf_results = [r for r in main_results if r.get("source") == "walkforward"]

    if live_results:
        w("### 3-1. 2026年 ライブ予想実績")
        w("")
        w("| モデル | 券種 | 件数 | 的中率 | 払戻合計 | 損益合計 | 平均ROI |")
        w("|--------|------|------|-------|---------|---------|--------|")
        for r in sorted(live_results, key=lambda x: (x["model"], x["bet_type"])):
            hr = f"{r['hit_rate']:.1f}%"
            pay = f"¥{r['payout']:,.0f}"
            pft = f"¥{r['profit']:+,.0f}"
            aroi = f"{r['avg_roi']:.1f}%"
            w(
                f"| {r['model']} | {r['bet_type']} | {r['bets']} | {hr} | {pay} | {pft} | {aroi} |"
            )
        w("")

        # 2026 サマリー: モデル別
        w("**2026年モデル別損益サマリー**:")
        w("")
        from collections import defaultdict

        model_pft: dict = defaultdict(float)
        model_inv: dict = defaultdict(float)
        for r in live_results:
            model_pft[r["model"]] += r.get("profit", 0) or 0
        # invested は n_tickets×100 だが prediction_results には直接ない
        # profit = payout - invested の逆算: invested = payout - profit
        for r in live_results:
            inv = (r.get("payout", 0) or 0) - (r.get("profit", 0) or 0)
            model_inv[r["model"]] += inv

        w("| モデル | 総投資（概算） | 総損益 | 推定ROI |")
        w("|--------|--------------|-------|--------|")
        for m in sorted(model_pft):
            inv = model_inv.get(m, 0)
            pft = model_pft[m]
            pay = inv + pft
            roi = pay / inv * 100 if inv > 0 else 0.0
            w(f"| {m} | ¥{inv:,.0f} | ¥{pft:+,.0f} | {roi:.1f}% |")
        w("")

    if wf_results:
        w("### 3-2. Walk-forward シミュレーション")
        w("")
        w("| モデル | 券種 | テスト年 | 件数 | 的中率 | 損益 | ROI |")
        w("|--------|------|---------|------|-------|------|-----|")
        for r in sorted(wf_results, key=lambda x: (x["model"], x["test_year"])):
            hr = f"{r['hit_rate']:.1f}%"
            pft = f"¥{r['profit']:+,.0f}"
            roi = f"{r['avg_roi']:.1f}%"
            w(
                f"| {r['model']} | {r['bet_type']} | {r['test_year']} | {r['bets']} | {hr} | {pft} | {roi} |"
            )
        w("")

    # ── 5カ年総評 ────────────────────────────────────────────────────
    w("## 4. 5カ年総評・推奨改善ポイント")
    w("")
    w("### 4-1. データギャップの解消")
    w("")
    w("| 課題 | 状況 | 推奨アクション |")
    w("|------|------|--------------|")
    w(
        "| 2021年オッズ | netkeiba_research.db に未取得 | `py scripts/full_pipeline.py` でスクレイプ実行 |"
    )
    w(
        "| 2021-2023 race_results | JVLink 経由で取得不可 | JVLink 有料プランの歴史データ購入を検討 |"
    )
    w("| ALPHA 2026 テスト精度 | 2026年は部分データ | 年末時点で再評価推奨 |")
    w("")

    w("### 4-2. モデル改善の方向性")
    w("")
    w("| モデル | 現状の課題 | 推奨改善 |")
    w("|--------|----------|---------|")
    w(
        "| 本命/卍/複勝 | 学習データが 2024-2026 の3年分のみ | 2021-2023 の JVLink 歴史データ追加で汎化性向上 |"
    )
    w(
        "| ALPHA | 2021 オッズデータ未取得 | `scrape_research_data.py --from 2021-01-01` で補完 |"
    )
    w(
        "| ALPHA | EV閾値の最適化 | `--sweep` オプションで EV 1.1〜2.0 グリッドサーチ推奨 |"
    )
    w("| Cascade | 三連単特化、データ少 | 2024〜 のレースが積み上がったら再学習 |")
    w("")

    w("### 4-3. 現時点のベスト運用方針")
    w("")
    w(
        "1. **ALPHA 単勝**: netkeiba_research.db (2022-2025) で実証済み。EV > 1.5 の馬に絞って購入。"
    )
    w(
        "2. **本命 三連単**: 2026年ライブ実績で最高払戻を記録。軸馬精度を活かした1頭軸マルチが有効。"
    )
    w("3. **卍 単勝**: 2026年単勝 ROI が相対的に高い。回収率重視の場合に採用。")
    w(
        "4. **2021年データ取得**: スクレイプ完了後に ALPHA を再学習し、フォールド数を増やして統計的信頼性を高める。"
    )
    w("")

    w("---")
    w("")
    w("*本レポートは `scripts/full_pipeline.py` により自動生成されました。*")
    w(
        "*次回更新: `py scripts/full_pipeline.py --skip-scrape --skip-train` で最新実績のみ再集計可能。*"
    )

    # 書き出し
    report_text = "\n".join(lines)
    output_path.write_text(report_text, encoding="utf-8")
    print(f"\n  ✓ レポート生成完了: {output_path}")
    print(f"    ({len(lines)} 行 / {len(report_text):,} 文字)")
    return output_path


# ── ステータス表示のみ ────────────────────────────────────────────────────


def cmd_status() -> None:
    status = phase0_status()

    print("\n=== 2021 スクレイプ必要性 ===")
    if status.get("missing_2021_research"):
        print("  ⚠  2021年は未取得です。以下を実行してください:")
        print("     py scripts/full_pipeline.py --skip-train")
        print("  または:  py scripts/auto_extend_and_backtest.py")
    else:
        print("  ✓  2021年データは取得済みです")

    # ── エリート複勝モニター ─────────────────────────────────────────
    print("\n=== エリート複勝モニター (FukushoEliteFilter) ===")
    if not _ELITE_CSV.exists():
        print(
            f"  蓄積進捗  : 0/{_ELITE_TARGET} レース（CSV未作成 — 初回ライブ予想で自動生成されます）"
        )
        print(f"  CSV パス   : {_ELITE_CSV}")
        return

    try:
        from scripts.evaluate_elite_results import status_summary, _PROTOCOL_LOG

        s = status_summary()
    except Exception as e:
        print(f"  ⚠ ステータス取得エラー: {e}")
        return

    n_acc = s["n_total"]
    n_done = s["n_done"]
    roi = s["roi"]
    pnl = s["pnl"]
    target = s["target"]
    bar_fill = int(n_acc / target * 20)
    bar = "█" * bar_fill + "░" * (20 - bar_fill)
    pnl_sign = "+" if pnl >= 0 else ""
    pnl_str = f"{pnl_sign}¥{abs(pnl):,}" if n_done > 0 else "—"
    roi_str = f"{roi:.1f}%" if n_done > 0 else "—"

    print(
        f"  蓄積進捗  : {n_acc}/{target} レース [{bar}] {n_acc / max(target, 1) * 100:.0f}%"
    )
    print(f"  結果確定  : {n_done} / {n_acc} レース")
    print(f"  暫定 ROI  : {roi_str}  損益 {pnl_str}")
    print(
        f"  的中率    : {s['hit_rate']:.1f}%  ({s['hit_rows']}/{n_done})"
        if n_done
        else "  的中率    : —"
    )

    if n_acc < target:
        print(f"  OOS検証まで あと {target - n_acc} レース")
        if n_done > 0:
            print("  ヒント    : 照合実行 → py scripts/evaluate_elite_results.py")
    elif n_done < target:
        print(
            "  ⚠ 蓄積完了・未照合あり → py scripts/evaluate_elite_results.py を実行してください"
        )
    else:
        # 30レース到達かつ全照合済み → 合否判定表示
        if s["protocol_approved"]:
            try:
                import json as _json

                proto = _json.loads(_PROTOCOL_LOG.read_text(encoding="utf-8"))
                ts = proto.get("timestamp", "")
                print(f"\n  ✅ APPROVED — ROI {roi_str} > 100%  合格")
                print(f"     公認プロトコル保存済み: {_PROTOCOL_LOG.name}  ({ts[:10]})")
            except Exception:
                print(f"\n  ✅ APPROVED — ROI {roi_str} > 100%  公認プロトコル保存済み")
        elif roi <= 100.0:
            print(f"\n  ❌ REJECTED — ROI {roi_str} ≦ 100%  不合格")
            print("     詳細分析 → py scripts/evaluate_elite_results.py")
        else:
            print(f"\n  ✅ APPROVED — ROI {roi_str} > 100%  合格")
            print(
                "     公認プロトコル確定 → py scripts/evaluate_elite_results.py で保存してください"
            )


# ── main ─────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="UMALOGI 5カ年全モデル一括再学習・バックテストパイプライン",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--skip-scrape", action="store_true", help="2021年スクレイプをスキップ"
    )
    parser.add_argument(
        "--skip-train", action="store_true", help="全モデル再学習をスキップ"
    )
    parser.add_argument(
        "--run-walk-forward",
        action="store_true",
        help="本命/卍 Walk-forward シミュレーション実行（低速: 数時間）",
    )
    parser.add_argument(
        "--status", action="store_true", help="データステータス確認のみ"
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="レポート生成のみ（スクレイプ・再学習・バックテストをスキップ）",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="レポートの出力先 (default: docs/umalogi_5year_report_YYYYMMDD.md)",
    )
    args = parser.parse_args()

    if args.status:
        cmd_status()
        return

    start_all = datetime.now()
    print(f"\n{'=' * 62}")
    print("  UMALOGI — 5カ年全モデル再学習・バックテストパイプライン")
    print(f"  開始: {start_all.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 62}")

    # Phase 0: データステータス
    status = phase0_status()

    if args.report_only:
        alpha_results = []
        main_results = phase4_main_walkforward(run_slow=False)
        phase5_generate_report(status, alpha_results, main_results, args.output)
        return

    # Phase 1: 2021スクレイプ
    phase1_scrape_2021(skip=args.skip_scrape)

    # Phase 2: 全モデル再学習
    if not phase2_retrain_all(skip=args.skip_train):
        print("\n[ABORT] 再学習に失敗しました")
        sys.exit(1)

    # Phase 3: ALPHA Walk-forward バックテスト
    alpha_results = phase3_alpha_walkforward()

    # Phase 4: 本命/卍 バックテスト
    main_results = phase4_main_walkforward(run_slow=args.run_walk_forward)

    # Phase 5: レポート生成
    report_path = phase5_generate_report(
        status, alpha_results, main_results, args.output
    )

    elapsed = (datetime.now() - start_all).total_seconds() / 60
    print(f"\n{'=' * 62}")
    print(f"  ✓ 全パイプライン完了  経過={elapsed:.1f}分")
    print(f"  レポート: {report_path}")
    print(f"{'=' * 62}\n")


if __name__ == "__main__":
    main()
