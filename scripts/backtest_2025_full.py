"""
UMALOGI 完全バックテスト (2026年クリーンデータ対応版)

DB に蓄積された全 predictions + prediction_results を集計し、
モデル別・券種別の年間 ROI・的中率・損益を算出する。

さらに Oracle (ガチ予想) と WIN5 のシミュレーションを
全 2026 年レースに対して再実行し、払戻との突合を行う。

2025年データについては JVLink インポート時のフォーマット破損
（rank=2/3 がほぼゼロ、払戻 combination の単位系不一致）のため
信頼性のある突合が不可能であることをレポートに記載する。

使用例:
    python scripts/backtest_2025_full.py
    python scripts/backtest_2025_full.py --year 2026
    python scripts/backtest_2025_full.py --date-from 2026-01-01 --date-to 2026-04-30
    python scripts/backtest_2025_full.py --oracle-sim   # Oracle 再シミュレーション
    python scripts/backtest_2025_full.py --json-out results.json
"""

from __future__ import annotations

import argparse
import itertools
import json
import logging
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# UTF-8 出力（Windows cp932 対策）
_utf8_stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False)
sys.stdout = _utf8_stdout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=_utf8_stdout,
)
logger = logging.getLogger("backtest")


# ─────────────────────────────────────────────────────────────────
# ユーティリティ
# ─────────────────────────────────────────────────────────────────

class Stats(NamedTuple):
    n_bets:    int
    n_hits:    int
    hit_rate:  float   # %
    invested:  float   # 円
    payout:    float   # 円
    profit:    float   # 円
    roi:       float   # 回収率 %
    max_hit:   float   # 最大的中払戻 円


def _stats(rows: list[dict]) -> Stats:
    n      = len(rows)
    hits   = [r for r in rows if r["is_hit"]]
    invested = sum(r["recommended_bet"] or 100 for r in rows)
    payout   = sum(r["payout"] or 0 for r in rows)
    profit   = payout - invested
    roi      = payout / invested * 100 if invested > 0 else 0.0
    max_hit  = max((r["payout"] or 0 for r in rows), default=0)
    return Stats(
        n_bets=n,
        n_hits=len(hits),
        hit_rate=len(hits) / n * 100 if n > 0 else 0.0,
        invested=invested,
        payout=payout,
        profit=profit,
        roi=roi,
        max_hit=max_hit,
    )


def _fmt_stats(s: Stats, label: str) -> str:
    sign = "+" if s.profit >= 0 else ""
    flag = "🟢" if s.roi >= 100 else ("🟡" if s.roi >= 75 else "🔴")
    return (
        f"  {flag} {label:<25s} "
        f"{s.n_bets:>5}件 {s.n_hits:>4}的中 "
        f"({s.hit_rate:>5.1f}%) "
        f"ROI={s.roi:>6.1f}%  "
        f"損益={sign}{s.profit:>+10,.0f}円  "
        f"最大払戻=¥{s.max_hit:>10,.0f}"
    )


# ─────────────────────────────────────────────────────────────────
# DB から予想実績を読み込む
# ─────────────────────────────────────────────────────────────────

def load_predictions(
    conn: sqlite3.Connection,
    date_from: str | None = None,
    date_to: str | None = None,
    year: int | None = None,
) -> pd.DataFrame:
    params: list = []
    wheres = ["pr.is_hit IS NOT NULL"]

    if year:
        wheres.append("strftime('%Y', r.date) = ?")
        params.append(str(year))
    else:
        if date_from:
            wheres.append("r.date >= ?")
            params.append(date_from)
        if date_to:
            wheres.append("r.date <= ?")
            params.append(date_to)

    where_sql = " AND ".join(wheres)
    df = pd.read_sql_query(
        f"""
        SELECT
            p.id            AS pred_id,
            p.race_id,
            r.date          AS race_date,
            r.venue,
            p.model_type,
            p.bet_type,
            p.recommended_bet,
            p.expected_value,
            p.combination_json,
            p.notes,
            COALESCE(pr.is_hit, 0)   AS is_hit,
            COALESCE(pr.payout, 0.0) AS payout,
            COALESCE(pr.profit, 0.0) AS profit,
            COALESCE(pr.roi, 0.0)    AS roi_val
        FROM predictions p
        JOIN races r ON p.race_id = r.race_id
        LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
        WHERE {where_sql}
          AND p.bet_type NOT IN ('馬分析')
        ORDER BY r.date, p.race_id
        """,
        conn,
        params=params,
    )
    return df


def load_data_quality(conn: sqlite3.Connection) -> dict:
    """各年のデータ品質サマリーを返す。"""
    quality = {}
    for yr in ("2025", "2026"):
        races      = conn.execute(f"SELECT COUNT(*) FROM races WHERE date LIKE '{yr}%'").fetchone()[0]
        rank1      = conn.execute(f"SELECT COUNT(*) FROM race_results rr JOIN races r ON rr.race_id=r.race_id WHERE r.date LIKE '{yr}%' AND rr.rank=1").fetchone()[0]
        rank2      = conn.execute(f"SELECT COUNT(*) FROM race_results rr JOIN races r ON rr.race_id=r.race_id WHERE r.date LIKE '{yr}%' AND rr.rank=2").fetchone()[0]
        payouts    = conn.execute(f"SELECT COUNT(*) FROM race_payouts rp JOIN races r ON rp.race_id=r.race_id WHERE r.date LIKE '{yr}%'").fetchone()[0]
        tri_tan    = conn.execute(f"SELECT COUNT(*) FROM race_payouts rp JOIN races r ON rp.race_id=r.race_id WHERE r.date LIKE '{yr}%' AND rp.bet_type='三連単'").fetchone()[0]
        win5_pay   = conn.execute(f"SELECT COUNT(*) FROM race_payouts rp JOIN races r ON rp.race_id=r.race_id WHERE r.date LIKE '{yr}%' AND rp.bet_type='WIN5'").fetchone()[0]
        quality[yr] = {
            "races": races, "rank1": rank1, "rank2": rank2,
            "payouts": payouts, "triexacta": tri_tan, "win5_payouts": win5_pay,
            "usable": rank2 > races * 0.5,  # 2着以上がレース数の50%以上なら使用可能
        }
    return quality


# ─────────────────────────────────────────────────────────────────
# Oracle 再シミュレーション（Harville 確率最大化）
# ─────────────────────────────────────────────────────────────────

def _harville_trio(probs: list[float], i: int, j: int, k: int) -> float:
    s = sum(probs)
    if s <= 0:
        return 0.0
    p1 = probs[i] / s
    s2 = s - probs[i]
    p2 = probs[j] / s2 if s2 > 0 else 0.0
    s3 = s2 - probs[j]
    p3 = probs[k] / s3 if s3 > 0 else 0.0
    return p1 * p2 * p3


def _harville_trifecta(probs: list[float], i: int, j: int, k: int) -> float:
    return _harville_trio(probs, i, j, k)


def _parse_combination(combo_json: str) -> list[tuple[int, ...]]:
    """combination_json を [馬番タプル, ...] に変換する。"""
    try:
        parsed = json.loads(combo_json)
        if not parsed:
            return []
        if isinstance(parsed[0], list):
            return [tuple(int(x) for x in c) for c in parsed]
        return [tuple(int(x) for x in parsed)]
    except Exception:
        return []


def _get_winner_nums(conn: sqlite3.Connection, race_id: str) -> tuple[list[int], list[int], list[int]]:
    """1/2/3 着の馬番リストを返す (同着考慮)。"""
    rows = conn.execute(
        "SELECT rank, horse_number FROM race_results WHERE race_id=? AND rank IN (1,2,3) ORDER BY rank",
        (race_id,),
    ).fetchall()
    r1 = [r[1] for r in rows if r[0] == 1]
    r2 = [r[1] for r in rows if r[0] == 2]
    r3 = [r[1] for r in rows if r[0] == 3]
    return r1, r2, r3


def _check_sanrenpuku_hit(
    combo: tuple[int, ...],
    r1: list[int], r2: list[int], r3: list[int],
) -> bool:
    if not (r1 and r2 and r3):
        return False
    top3 = set(r1[:1] + r2[:1] + r3[:1])
    return set(combo) == top3


def _check_sanrentan_hit(
    combo: tuple[int, ...],
    r1: list[int], r2: list[int], r3: list[int],
) -> bool:
    if not (r1 and r2 and r3 and len(combo) == 3):
        return False
    return (combo[0] in r1) and (combo[1] in r2) and (combo[2] in r3)


def simulate_oracle(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
    top_n: int = 3,
) -> tuple[list[dict], list[dict]]:
    """
    全対象レースに対して Oracle (Harville 確率最大化) を再シミュレーションする。
    三連複 TOP_N 点・三連単 TOP_N 点を生成し actual payouts と突合する。
    Returns (三連複結果リスト, 三連単結果リスト)
    """
    from src.ml.models import HonmeiModel
    from src.ml.features import FeatureBuilder

    logger.info("Oracle シミュレーション開始: %s ~ %s", date_from, date_to)

    honmei = HonmeiModel()
    honmei.load()
    fb = FeatureBuilder(conn)

    # 対象レース取得（rank=1,2,3 がある完全なものだけ）
    races = conn.execute(
        """
        SELECT DISTINCT r.race_id, r.date
        FROM races r
        WHERE r.date >= ? AND r.date <= ?
          AND EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id=r.race_id AND rr.rank=1)
          AND EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id=r.race_id AND rr.rank=2)
          AND EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id=r.race_id AND rr.rank=3)
        ORDER BY r.date, r.race_id
        """,
        (date_from, date_to),
    ).fetchall()

    logger.info("対象レース: %d 件", len(races))

    trio_results:     list[dict] = []
    trifecta_results: list[dict] = []

    # 三連単・三連複の実際の払戻をキャッシュ
    payout_cache: dict[str, dict[str, dict[str, int]]] = defaultdict(lambda: defaultdict(dict))
    payout_rows = conn.execute(
        """
        SELECT rp.race_id, rp.bet_type, rp.combination, rp.payout
        FROM race_payouts rp
        JOIN races r ON rp.race_id=r.race_id
        WHERE r.date >= ? AND r.date <= ?
          AND rp.bet_type IN ('三連複','三連単')
        """,
        (date_from, date_to),
    ).fetchall()
    for row in payout_rows:
        payout_cache[row[0]][row[1]][row[2]] = row[3]

    processed = 0
    for race_id, race_date in races:
        try:
            df = fb.build_race_features_for_simulate(race_id)
            if df is None or len(df) < 3:
                continue

            scores = honmei.predict(df)
            df["_score"] = scores.values
            df = df[df["horse_number"] >= 1].sort_values("_score", ascending=False)
            if len(df) < 3:
                continue

            nums   = [int(r["horse_number"])  for _, r in df.iterrows()]
            probs  = [float(r["_score"])      for _, r in df.iterrows()]
            n      = len(nums)

            r1, r2, r3 = _get_winner_nums(conn, race_id)
            if not (r1 and r2 and r3):
                continue

            # ── 三連複 TOP_N ─────────────────────────────────────
            trio_probs = []
            for idx_combo in itertools.combinations(range(n), 3):
                ia, ib, ic = idx_combo
                prob_sum = sum(
                    _harville_trio(probs, *perm)
                    for perm in itertools.permutations([ia, ib, ic])
                )
                trio_probs.append((prob_sum, tuple(sorted([nums[ia], nums[ib], nums[ic]]))))
            trio_probs.sort(reverse=True)

            for rank_i, (prob, combo) in enumerate(trio_probs[:top_n]):
                is_hit = _check_sanrenpuku_hit(combo, r1, r2, r3)
                combo_str = "-".join(map(str, combo))
                payout = payout_cache[race_id]["三連複"].get(combo_str, 0) if is_hit else 0
                trio_results.append({
                    "race_id": race_id, "date": race_date,
                    "rank": rank_i + 1, "prob": round(prob, 5),
                    "combination": combo, "combination_str": combo_str,
                    "is_hit": is_hit, "payout": payout,
                    "bet": 100,
                })

            # ── 三連単 TOP_N ─────────────────────────────────────
            tan_probs = []
            for ia, ib, ic in itertools.permutations(range(n), 3):
                prob = _harville_trifecta(probs, ia, ib, ic)
                tan_probs.append((prob, (nums[ia], nums[ib], nums[ic])))
            tan_probs.sort(reverse=True)

            for rank_i, (prob, combo) in enumerate(tan_probs[:top_n]):
                is_hit = _check_sanrentan_hit(combo, r1, r2, r3)
                combo_str = "→".join(map(str, combo))
                payout = payout_cache[race_id]["三連単"].get(combo_str, 0) if is_hit else 0
                trifecta_results.append({
                    "race_id": race_id, "date": race_date,
                    "rank": rank_i + 1, "prob": round(prob, 5),
                    "combination": combo, "combination_str": combo_str,
                    "is_hit": is_hit, "payout": payout,
                    "bet": 100,
                })

            processed += 1
            if processed % 100 == 0:
                logger.info("  処理済み: %d / %d レース", processed, len(races))

        except Exception as e:
            logger.debug("race %s スキップ: %s", race_id, e)
            continue

    logger.info("Oracle シミュレーション完了: %d レース処理", processed)
    return trio_results, trifecta_results


# ─────────────────────────────────────────────────────────────────
# WIN5 シミュレーション (SABC ランク)
# ─────────────────────────────────────────────────────────────────

_WIN5_RANK_THRESHOLDS = {"S": 0.30, "A": 0.18, "B": 0.09}

def _win5_rank(prob: float) -> str:
    if prob >= _WIN5_RANK_THRESHOLDS["S"]: return "S"
    if prob >= _WIN5_RANK_THRESHOLDS["A"]: return "A"
    if prob >= _WIN5_RANK_THRESHOLDS["B"]: return "B"
    return "C"


def simulate_win5(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """
    日曜日の WIN5 対象レース (R4〜R8) を SABC 選択でシミュレーション。
    WIN5 の払戻データは DB に未整備のため「仮想的中フラグ」のみを返す。
    """
    from src.ml.models import HonmeiModel
    from src.ml.features import FeatureBuilder

    logger.info("WIN5 シミュレーション開始: %s ~ %s", date_from, date_to)

    honmei = HonmeiModel()
    honmei.load()
    fb = FeatureBuilder(conn)

    # 日曜の開催日を取得
    sundays = conn.execute(
        """
        SELECT DISTINCT date FROM races
        WHERE date >= ? AND date <= ?
          AND CAST(strftime('%w', date) AS INTEGER) = 0
        ORDER BY date
        """,
        (date_from, date_to),
    ).fetchall()
    sundays = [r[0] for r in sundays]
    logger.info("日曜開催日: %d 件", len(sundays))

    results = []
    for race_date in sundays:
        # R4〜R8 を WIN5 対象と仮定（実際の WIN5 指定レースは race_payouts.bet_type='WIN5' で確認）
        races = conn.execute(
            """
            SELECT race_id, race_number, venue
            FROM races
            WHERE date=?
              AND CAST(SUBSTR(race_id,11,2) AS INTEGER) BETWEEN 4 AND 8
              AND EXISTS(SELECT 1 FROM race_results rr WHERE rr.race_id=races.race_id AND rr.rank=1)
            ORDER BY race_id
            LIMIT 5
            """,
            (race_date,),
        ).fetchall()
        if len(races) < 5:
            continue

        daily_data = {"date": race_date, "races": [], "selections": {}, "horse_ranks": {}, "is_hit": False}
        all_correct = True

        for race_id, race_num, venue in races:
            try:
                df = fb.build_race_features_for_simulate(race_id)
                if df is None or len(df) < 2:
                    all_correct = False
                    continue

                scores = honmei.predict(df)
                df = df[df["horse_number"] >= 1].copy()
                df["_score"] = scores.reindex(df.index).values
                total_score = df["_score"].sum()
                if total_score <= 0:
                    continue
                df["win_prob"] = df["_score"] / total_score
                df["rank_label"] = df["win_prob"].apply(_win5_rank)

                # 選択: A以上を優先, なければ上位B, それでもなければ上位C
                for min_rank in ("A", "B", "C"):
                    selected = df[df["rank_label"].isin(
                        {"A": ["S", "A"], "B": ["S", "A", "B"], "C": ["S", "A", "B", "C"]}[min_rank]
                    )]["horse_number"].astype(int).tolist()
                    if len(selected) <= 3:
                        break

                r1, _, _ = _get_winner_nums(conn, race_id)
                race_hit = bool(r1 and r1[0] in selected)

                daily_data["races"].append({
                    "race_id": race_id, "venue": venue, "race_number": race_num,
                })
                daily_data["selections"][race_id] = selected
                daily_data["horse_ranks"][race_id] = [
                    {"horse_number": int(r["horse_number"]), "win_prob": round(r["win_prob"], 4),
                     "rank": r["rank_label"]}
                    for _, r in df.nlargest(8, "win_prob").iterrows()
                ]
                if not race_hit:
                    all_correct = False

            except Exception as e:
                logger.debug("WIN5 race %s スキップ: %s", race_id, e)
                all_correct = False
                continue

        if len(daily_data["races"]) == 5:
            daily_data["is_hit"] = all_correct
            results.append(daily_data)

    logger.info("WIN5 シミュレーション完了: %d 日曜処理 / %d 件", len(sundays), len(results))
    return results


# ─────────────────────────────────────────────────────────────────
# レポート出力
# ─────────────────────────────────────────────────────────────────

def print_section(title: str) -> None:
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def report_existing_predictions(
    df: pd.DataFrame,
    date_label: str,
) -> dict:
    print_section(f"モデル別・券種別 実績集計 ({date_label})")

    results_by_model: dict = {}
    oracle_rows: list[dict] = []
    high_value_hits: list[dict] = []

    # Oracle を別枠で集計
    df_oracle = df[df["model_type"].str.startswith("Oracle")]
    df_normal = df[~df["model_type"].str.startswith("Oracle")]

    for model_type in sorted(df_normal["model_type"].unique()):
        grp = df_normal[df_normal["model_type"] == model_type]
        results_by_model[model_type] = {}
        for bet_type in sorted(grp["bet_type"].unique()):
            rows = grp[grp["bet_type"] == bet_type].to_dict("records")
            s = _stats(rows)
            results_by_model[model_type][bet_type] = s
            if bet_type not in ("馬分析",):
                print(_fmt_stats(s, f"{model_type} {bet_type}"))
                # 高額的中を抽出
                for r in rows:
                    if r["is_hit"] and (r["payout"] or 0) >= 10000:
                        high_value_hits.append(r)

    # Oracle
    if not df_oracle.empty:
        print(f"\n  ── Oracle (ガチ予想) ──────────────────────────────────────")
        for bet_type in sorted(df_oracle["bet_type"].unique()):
            rows = df_oracle[df_oracle["bet_type"] == bet_type].to_dict("records")
            s = _stats(rows)
            print(_fmt_stats(s, f"Oracle {bet_type}"))
            for r in rows:
                if r["is_hit"]:
                    oracle_rows.append(r)
                    high_value_hits.append(r)

    # 高額的中一覧
    if high_value_hits:
        print(f"\n  ── 高額的中 (払戻¥10,000以上) ──────────────────────────────")
        high_value_hits.sort(key=lambda x: x["payout"] or 0, reverse=True)
        for r in high_value_hits[:20]:
            print(
                f"  🎯 {r['race_date']} {r['race_id']} "
                f"{r['model_type']} {r['bet_type']} "
                f"¥{r['payout']:>10,.0f}"
            )

    return results_by_model


def report_oracle_sim(
    trio_results: list[dict],
    trifecta_results: list[dict],
    label: str,
) -> None:
    print_section(f"Oracle シミュレーション結果 ({label})")

    for bet_type, results, check_fn in [
        ("三連複 (再シミュレーション)", trio_results, None),
        ("三連単 (再シミュレーション)", trifecta_results, None),
    ]:
        if not results:
            print(f"  {bet_type}: データなし")
            continue

        # TOP1 (最有力1点) だけを評価
        top1 = [r for r in results if r["rank"] == 1]
        hits_top1 = [r for r in top1 if r["is_hit"]]
        invested_top1  = len(top1) * 100
        payout_top1    = sum(r["payout"] for r in hits_top1)
        roi_top1       = payout_top1 / invested_top1 * 100 if invested_top1 > 0 else 0

        # TOP3 全点を評価
        all_hits  = [r for r in results if r["is_hit"]]
        invested_all   = len(results) * 100
        payout_all     = sum(r["payout"] for r in all_hits)
        roi_all        = payout_all / invested_all * 100 if invested_all > 0 else 0

        flag_t1 = "🟢" if roi_top1 >= 100 else ("🟡" if roi_top1 >= 75 else "🔴")
        flag_al = "🟢" if roi_all >= 100 else ("🟡" if roi_all >= 75 else "🔴")

        print(f"\n  {bet_type}")
        print(
            f"    {flag_t1} TOP-1 のみ: {len(top1):>5}件  "
            f"{len(hits_top1):>3}的中  "
            f"ROI={roi_top1:>6.1f}%  "
            f"払戻合計=¥{payout_top1:>10,.0f}"
        )
        print(
            f"    {flag_al} TOP-3 全点: {len(results):>5}件  "
            f"{len(all_hits):>3}的中  "
            f"ROI={roi_all:>6.1f}%  "
            f"払戻合計=¥{payout_all:>10,.0f}"
        )

        # 的中明細 TOP10
        hits_sorted = sorted(all_hits, key=lambda x: x["payout"], reverse=True)
        if hits_sorted:
            print(f"    的中明細 (上位10件):")
            for h in hits_sorted[:10]:
                print(
                    f"      🎯 {h['date']}  {h['race_id']}  "
                    f"組合={h['combination_str']}  "
                    f"P={h['prob']:.3f}  "
                    f"払戻=¥{h['payout']:>8,}"
                )


def report_win5_sim(win5_results: list[dict], label: str) -> None:
    print_section(f"WIN5 シミュレーション結果 ({label})")

    if not win5_results:
        print("  WIN5 対象データなし")
        return

    total = len(win5_results)
    hits  = [r for r in win5_results if r["is_hit"]]
    hit_rate = len(hits) / total * 100 if total > 0 else 0

    flag = "🎉" if hits else "📊"
    print(f"  {flag} 対象日曜: {total} 開催  全レース的中: {len(hits)} 開催  "
          f"的中率: {hit_rate:.1f}%")
    print(f"  ※ WIN5 払戻データは DB 未整備のため ROI 計算不可")
    print(f"    (実際の WIN5 買い目点数・払戻は JRA 公式で確認が必要)")

    if hits:
        print(f"\n  全レース的中日:")
        for r in hits:
            print(f"    🏆 {r['date']}")


def report_data_quality(quality: dict) -> None:
    print_section("データ品質レポート")

    for yr, q in quality.items():
        usable = "✅ 利用可能" if q["usable"] else "⚠️ 要注意 (バックテスト精度低下)"
        print(f"\n  [{yr}年] {usable}")
        print(f"    レース数         : {q['races']:>6,} 件")
        print(f"    rank=1 (勝ち馬)  : {q['rank1']:>6,} 件 (期待値 ≒ {q['races']:,})")
        print(f"    rank=2 (2着)     : {q['rank2']:>6,} 件 (期待値 ≒ {q['races']:,})")
        print(f"    払戻データ       : {q['payouts']:>6,} 件")
        print(f"    三連単払戻       : {q['triexacta']:>6,} 件")
        print(f"    WIN5払戻         : {q['win5_payouts']:>6,} 件")

        if yr == "2025" and not q["usable"]:
            print(
                "\n    [2025年バックテスト不可の理由]\n"
                "    JVLink 一括インポート時に race_results の rank フィールドが\n"
                "    「着順」ではなく「JVLink レコード内の項目コード」として格納された。\n"
                "    rank=2 が 26 件しかなく (期待値 3,456 件) 2着・3着の情報が欠損。\n"
                "    また race_payouts の combination と payout が JVLink 独自単位で\n"
                "    netkeiba 形式 (2026年データ) と互換性がなく突合不可能。\n"
                "    → 対処法: import_historical_force.py を修正して 2025 データを\n"
                "             再インポートし、race_results / race_payouts を正規化後に\n"
                "             本スクリプトを再実行してください。"
            )


# ─────────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="UMALOGI 完全バックテスト")
    parser.add_argument("--year",      type=int,   default=None, help="対象年 (例: 2026)")
    parser.add_argument("--date-from", default=None, help="開始日 YYYY-MM-DD")
    parser.add_argument("--date-to",   default=None, help="終了日 YYYY-MM-DD")
    parser.add_argument("--oracle-sim", action="store_true", help="Oracle 再シミュレーションを実行")
    parser.add_argument("--win5-sim",   action="store_true", help="WIN5 シミュレーションを実行")
    parser.add_argument("--json-out",   default=None, help="JSON 出力ファイルパス")
    args = parser.parse_args()

    from src.database.init_db import init_db
    conn = init_db()

    # デフォルト: 2026年全体
    if not args.year and not args.date_from:
        args.year = 2026

    # 日付範囲を確定
    if args.year:
        date_from = f"{args.year}-01-01"
        date_to   = f"{args.year}-12-31"
        date_label = f"{args.year}年"
    else:
        date_from = args.date_from or "2026-01-01"
        date_to   = args.date_to   or "2026-12-31"
        date_label = f"{date_from} ~ {date_to}"

    print_section("UMALOGI 完全バックテスト")
    print(f"  対象期間: {date_label}")
    print(f"  Oracle再シミュレーション: {'ON' if args.oracle_sim else 'OFF (--oracle-sim で有効化)'}")
    print(f"  WIN5再シミュレーション:   {'ON' if args.win5_sim   else 'OFF (--win5-sim で有効化)'}")

    # データ品質確認
    quality = load_data_quality(conn)
    report_data_quality(quality)

    # 既存予想実績の集計
    df = load_predictions(conn, date_from=date_from, date_to=date_to)
    if df.empty:
        print(f"\n  ⚠️ 対象期間 ({date_label}) の prediction_results が見つかりません。")
        print("  main_pipeline.py を実行してレース予想を生成してください。")
    else:
        report_existing_predictions(df, date_label)

    # Oracle 再シミュレーション
    trio_results: list[dict] = []
    trifecta_results: list[dict] = []
    if args.oracle_sim:
        trio_results, trifecta_results = simulate_oracle(conn, date_from, date_to)
        report_oracle_sim(trio_results, trifecta_results, date_label)
    else:
        print(f"\n  [Oracle 再シミュレーション] --oracle-sim オプションで実行可能")

    # WIN5 シミュレーション
    win5_results: list[dict] = []
    if args.win5_sim:
        win5_results = simulate_win5(conn, date_from, date_to)
        report_win5_sim(win5_results, date_label)
    else:
        print(f"  [WIN5 シミュレーション] --win5-sim オプションで実行可能")

    # 総合サマリー
    print_section(f"総合サマリー ({date_label})")
    if not df.empty:
        # 全モデル合計
        all_rows = df[~df["model_type"].str.startswith("Oracle")].to_dict("records")
        total_s  = _stats(all_rows)
        print(f"  全モデル合計 (Oracle除く):")
        print(f"    予想件数: {total_s.n_bets:,} 件  的中: {total_s.n_hits:,} 件")
        print(f"    投資額:   ¥{total_s.invested:,.0f}")
        print(f"    払戻額:   ¥{total_s.payout:,.0f}")
        sign = "+" if total_s.profit >= 0 else ""
        flag = "🟢" if total_s.roi >= 100 else ("🟡" if total_s.roi >= 75 else "🔴")
        print(f"    損益:     {flag} {sign}¥{total_s.profit:,.0f}  (回収率 {total_s.roi:.1f}%)")
        print(f"    最大的中: ¥{total_s.max_hit:,.0f}")

        # 最強券種
        print(f"\n  ── 回収率 TOP 3 ──────────────────────────────────")
        all_stats = []
        for mt in df["model_type"].unique():
            grp = df[df["model_type"] == mt]
            for bt in grp["bet_type"].unique():
                rows = grp[grp["bet_type"] == bt].to_dict("records")
                if len(rows) < 5:
                    continue
                s = _stats(rows)
                if s.roi > 0:
                    all_stats.append((s.roi, f"{mt} {bt}", s))
        all_stats.sort(reverse=True)
        for roi_v, label, s in all_stats[:3]:
            print(_fmt_stats(s, label))

    # JSON 出力
    if args.json_out:
        output = {
            "date_range":       {"from": date_from, "to": date_to},
            "data_quality":     quality,
            "oracle_trio":      trio_results[:200] if trio_results else [],
            "oracle_trifecta":  trifecta_results[:200] if trifecta_results else [],
            "win5":             win5_results[:52] if win5_results else [],
        }
        Path(args.json_out).write_text(
            json.dumps(output, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"\n  JSON 出力: {args.json_out}")

    conn.close()
    print(f"\n{'='*70}")
    print("  バックテスト完了")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
