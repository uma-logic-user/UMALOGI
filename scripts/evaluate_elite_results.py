"""
scripts/evaluate_elite_results.py — FukushoEliteFilter 事後結果照合スクリプト

週明けに実行することで、logs/fukusho_elite_monitor.csv の
「result・payout」欄を umalogi.db の race_payouts から自動照合・更新し、
実際の ROI を算出する。

照合ロジック:
  各選択馬番について race_payouts の 複勝 / 返還 を参照する。
  複勝的中 → 払戻額を加算。返還 → 100円を加算。外れ → 0円。

30レース到達時:
  合格（ROI > 100%）→ UMALOGI公認プロトコルとして logs/elite_protocol.json へ永久保存。
  不合格（ROI ≦ 100%）→ 会場別 ROI 解析 + 次の30レース向け改良フィルターを自動提案。

Usage:
    py scripts/evaluate_elite_results.py              # 照合 + レポート出力
    py scripts/evaluate_elite_results.py --dry-run    # CSV 更新せずレポートのみ
    py scripts/evaluate_elite_results.py --force      # 照合済み行も再照合
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

_MAIN_DB = _ROOT / "data" / "umalogi.db"
_ELITE_CSV = _ROOT / "logs" / "fukusho_elite_monitor.csv"
_PROTOCOL_LOG = _ROOT / "logs" / "elite_protocol.json"
_TARGET = 30  # OOS 検証目標レース数
_ROI_PASS = 100.0  # 合格ライン (%)
# 発見時の in-sample フィルター条件（参照用）
_ORIGINAL_FILTER = {
    "venues": ["東京", "京都", "福島", "新潟"],
    "n_horses_min": 13,
    "edge_threshold": 1.1,
}

# ── CSV I/O ──────────────────────────────────────────────────────────────────

_CSV_COLS = [
    "date",
    "race_id",
    "venue",
    "n_horses",
    "horse_numbers",
    "edge",
    "result",
    "payout",
]


def _load_csv() -> list[dict]:
    if not _ELITE_CSV.exists():
        return []
    with _ELITE_CSV.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _save_csv(rows: list[dict]) -> None:
    _ELITE_CSV.parent.mkdir(parents=True, exist_ok=True)
    with _ELITE_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLS)
        writer.writeheader()
        writer.writerows(rows)


# ── DB 照合 ───────────────────────────────────────────────────────────────────


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_MAIN_DB))
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _lookup_race_result(
    conn: sqlite3.Connection,
    race_id: str,
    horse_nums: list[int],
) -> tuple[str, int] | None:
    """
    race_payouts を参照して複勝結果を照合する。

    Returns:
        (result_str, total_return_yen) or None (払戻データ未登録の場合)
    """
    # 払戻データが存在するか確認（未登録レースはスキップ）
    has_payout = conn.execute(
        "SELECT 1 FROM race_payouts WHERE race_id = ? LIMIT 1",
        (race_id,),
    ).fetchone()
    if not has_payout:
        return None

    total_return = 0
    hit_count = 0
    refund_count = 0

    for hn in horse_nums:
        hn_str = str(hn)

        # 複勝的中チェック
        place_row = conn.execute(
            "SELECT payout FROM race_payouts WHERE race_id=? AND bet_type='複勝' AND combination=?",
            (race_id, hn_str),
        ).fetchone()
        if place_row:
            total_return += place_row[0]
            hit_count += 1
            continue

        # 返還チェック
        refund_row = conn.execute(
            "SELECT 1 FROM race_payouts WHERE race_id=? AND bet_type='返還' AND combination=?",
            (race_id, hn_str),
        ).fetchone()
        if refund_row:
            total_return += 100
            refund_count += 1

    n = len(horse_nums)
    if hit_count > 0:
        result_str = f"的中{hit_count}/{n}"
    elif refund_count == n:
        result_str = "全返還"
    elif refund_count > 0:
        result_str = f"一部返還{refund_count}/{n}"
    else:
        result_str = "全外れ"

    return result_str, total_return


# ── ROI 計算 ─────────────────────────────────────────────────────────────────


def _parse_horse_nums(s: str) -> list[int]:
    try:
        return [int(x) for x in s.split() if x]
    except ValueError:
        return []


def _parse_edges(s: str) -> list[float]:
    try:
        return [float(x) for x in s.split() if x]
    except ValueError:
        return []


def _calc_stats(rows: list[dict]) -> dict:
    """
    result / payout が記録済みの行から ROI・損益・的中率を集計する。
    """
    done = [r for r in rows if r.get("result") and r.get("payout")]
    if not done:
        return {"n_done": 0}

    total_investment = 0
    total_return = 0
    hit_rows = 0
    n_bets = 0
    payouts: list[int] = []
    cumulative = 0
    max_dd = 0
    peak = 0

    for r in done:
        nums = _parse_horse_nums(r["horse_numbers"])
        inv = len(nums) * 100
        ret = int(r["payout"])
        pnl = ret - inv

        total_investment += inv
        total_return += ret
        n_bets += len(nums)
        payouts.append(ret)

        if r["result"] != "全外れ":
            hit_rows += 1

        # Max Drawdown 計算
        cumulative += pnl
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    roi = total_return / total_investment * 100 if total_investment > 0 else 0.0
    pnl_total = total_return - total_investment
    hit_rate = hit_rows / len(done) * 100

    dates = [r["date"] for r in done if r.get("date")]
    period = f"{min(dates)} ~ {max(dates)}" if dates else "—"

    return {
        "n_done": len(done),
        "n_bets": n_bets,
        "investment": total_investment,
        "ret": total_return,
        "pnl": pnl_total,
        "roi": roi,
        "hit_rate": hit_rate,
        "hit_rows": hit_rows,
        "max_dd": max_dd,
        "period": period,
    }


# ── 会場別 ROI 分析（不合格時の失敗解析）───────────────────────────────────────


def _analyze_by_venue(rows: list[dict]) -> list[dict]:
    """結果記録済み行を会場ごとに集計して ROI を返す。"""
    done = [r for r in rows if r.get("result") and r.get("payout")]
    venue_data: dict[str, dict] = defaultdict(
        lambda: {"inv": 0, "ret": 0, "hits": 0, "n": 0}
    )

    for r in done:
        v = r.get("venue", "不明")
        nums = _parse_horse_nums(r["horse_numbers"])
        inv = len(nums) * 100
        ret = int(r["payout"])
        venue_data[v]["inv"] += inv
        venue_data[v]["ret"] += ret
        venue_data[v]["n"] += 1
        if r["result"] != "全外れ":
            venue_data[v]["hits"] += 1

    result = []
    for venue, d in venue_data.items():
        roi = d["ret"] / d["inv"] * 100 if d["inv"] > 0 else 0.0
        pnl = d["ret"] - d["inv"]
        result.append(
            {
                "venue": venue,
                "n": d["n"],
                "roi": roi,
                "pnl": pnl,
                "hits": d["hits"],
            }
        )
    return sorted(result, key=lambda x: x["roi"])


def _propose_next_filter(
    rows: list[dict],
    stats: dict,
    venue_stats: list[dict],
) -> dict:
    """
    不合格時の次の30レース向け改良フィルターを提案する。

    戦略:
      1. ROI < 85% の会場を除外候補にする
      2. Edge 閾値を引き上げて件数を絞る感度分析を行う
      3. 提案フィルターを返す
    """
    poor_venues = [v["venue"] for v in venue_stats if v["roi"] < 85.0]
    good_venues = [v["venue"] for v in venue_stats if v["roi"] >= 100.0]

    # Edge 感度分析: 現在の Edge 値から閾値別の件数・平均 Edge を算出
    done = [r for r in rows if r.get("result") and r.get("payout")]
    edge_thresholds = [1.1, 1.2, 1.3, 1.5]
    edge_sensitivity: list[dict] = []

    for thr in edge_thresholds:
        kept = []
        for r in done:
            edges = _parse_edges(r.get("edge", ""))
            if edges and min(edges) >= thr:
                kept.append(r)

        if kept:
            sub_stats = _calc_stats(kept)
            edge_sensitivity.append(
                {
                    "threshold": thr,
                    "n_races": len(kept),
                    "roi": sub_stats.get("roi", 0),
                    "pnl": sub_stats.get("pnl", 0),
                }
            )

    # 最良 edge 閾値（ROI が最高）
    best_edge = (
        max(edge_sensitivity, key=lambda x: x["roi"]) if edge_sensitivity else None
    )

    # 新フィルター提案
    proposed_venues = (
        good_venues
        if good_venues
        else [v["venue"] for v in venue_stats if v["roi"] >= 95.0]
    )
    if not proposed_venues:
        proposed_venues = _ORIGINAL_FILTER["venues"]

    proposed_edge = (
        best_edge["threshold"]
        if best_edge and best_edge["roi"] > stats.get("roi", 0) + 5
        else _ORIGINAL_FILTER["edge_threshold"]
    )

    return {
        "poor_venues": poor_venues,
        "good_venues": good_venues,
        "proposed_filter": {
            "venues": proposed_venues,
            "n_horses_min": _ORIGINAL_FILTER["n_horses_min"],
            "edge_threshold": proposed_edge,
        },
        "edge_sensitivity": edge_sensitivity,
        "best_edge": best_edge,
    }


# ── プロトコル保存 ────────────────────────────────────────────────────────────


def _save_protocol(stats: dict, rows: list[dict]) -> Path:
    """合格時に UMALOGI公認プロトコルを logs/elite_protocol.json に永久保存する。"""
    _PROTOCOL_LOG.parent.mkdir(parents=True, exist_ok=True)

    protocol = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "verdict": "APPROVED",
        "filter": _ORIGINAL_FILTER,
        "validation": {
            "n_races": stats["n_done"],
            "n_bets": stats["n_bets"],
            "total_investment": stats["investment"],
            "total_return": stats["ret"],
            "roi": round(stats["roi"], 2),
            "pnl": stats["pnl"],
            "hit_rate": round(stats["hit_rate"], 2),
            "max_drawdown": stats["max_dd"],
            "period": stats["period"],
        },
    }

    _PROTOCOL_LOG.write_text(
        json.dumps(protocol, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return _PROTOCOL_LOG


# ── 月次目標逆算 + RoR ────────────────────────────────────────────────────────


def _ror_mc(
    hit_p: float,
    pnl_when_hit: float,
    pnl_when_miss: float,
    bankroll: float,
    n_races: int,
    n_trials: int = 3_000,
) -> float:
    """モンテカルロ法で月次破綻確率を推定する（n_trials 試行）。"""
    ruin = 0
    stake = abs(pnl_when_miss)
    for _ in range(n_trials):
        bk = bankroll
        for _ in range(n_races):
            if bk < stake:
                ruin += 1
                break
            bk += pnl_when_hit if random.random() < hit_p else pnl_when_miss
    return ruin / n_trials


def _render_monthly_target(stats: dict) -> str | None:
    """
    月次 ¥100,000 達成逆算 + RoR 警告を CLI に表示する。
    Discord 送信が必要な場合は警告テキストを返す（不要なら None）。
    """
    n_done = stats.get("n_done", 0)
    if n_done < 5:
        return None

    roi = stats["roi"] / 100.0
    hit_p = stats.get("hit_rate", 0.0) / 100.0
    if roi <= 1.0 or hit_p <= 0:
        print("\n  ⚠️  ROI < 100% のため月次目標逆算をスキップします。")
        return None

    avg_inv = stats["investment"] / n_done
    avg_ret = stats["ret"] / n_done
    edge = avg_ret - avg_inv
    if edge <= 0:
        return None

    RACES = 56
    TARGET = 100_000

    scale = TARGET / (edge * RACES)
    bet_per_horse = max(100, round(100 * scale / 100) * 100)
    monthly_inv = avg_inv * scale * RACES

    # スケール後パラメータ
    avg_inv_s = avg_inv * scale
    pnl_hit = (avg_ret / hit_p) * scale - avg_inv_s
    pnl_miss = -avg_inv_s

    print("\n" + "─" * 68)
    print("  📊 月次 ¥100,000 目標逆算シミュレーター")
    print("─" * 68)
    print(
        f"\n  OOS実績 {n_done}レース | ROI {stats['roi']:.1f}% | 的中率 {stats['hit_rate']:.1f}%"
    )
    print()
    print(f"  月次 ¥{TARGET:,} 達成条件（週末 {RACES}レース/月 想定）")
    print(f"  ┌─────────────────────────────────────────────────────┐")
    print(f"  │  推奨ベット / 馬  : ¥{bet_per_horse:>8,}                 │")
    print(f"  │  月間総投資額     : ¥{monthly_inv:>8,.0f}                 │")
    print(f"  │  期待月次損益     : +¥{TARGET:>7,}                 │")
    print(f"  └─────────────────────────────────────────────────────┘")
    print()
    print("  ─── 軍資金別 破綻リスク（月次 Monte Carlo 3,000試行）───")

    ror_20: float = 0.0
    ror_lines: list[str] = []
    for bk_yen, label in [(100_000, "¥10万"), (200_000, "¥20万"), (300_000, "¥30万")]:
        ror = _ror_mc(hit_p, pnl_hit, pnl_miss, float(bk_yen), RACES)
        pct = ror * 100
        icon = "🔴" if pct >= 20 else ("🟡" if pct >= 10 else "🟢")
        print(f"  {icon}  軍資金 {label}: {pct:.1f}%")
        ror_lines.append(f"{icon} {label}: {pct:.1f}%")
        if bk_yen == 200_000:
            ror_20 = pct

    discord_msg: str | None = None
    if ror_20 >= 20.0:
        print(
            f"\n  ⚠️  警告: 破綻リスク {ror_20:.0f}% (¥20万) — ベットを推奨額の50%以下に"
        )
        discord_msg = (
            f"⚠️ RoR警告 — FukushoElite 軍資金¥20万 破綻リスク **{ror_20:.0f}%**\n"
            f"推奨ベット ¥{bet_per_horse:,}/馬 → **¥{bet_per_horse // 2:,} 以下**に引き下げを推奨\n"
            + "  ".join(ror_lines)
        )
    elif ror_20 >= 10.0:
        print(
            f"\n  📌  注意: 破綻リスク {ror_20:.0f}% (¥20万) — 2週連続赤字で投資額縮小を"
        )
        discord_msg = (
            f"📌 RoR注意 — FukushoElite 軍資金¥20万 破綻リスク {ror_20:.0f}%\n"
            f"2週連続赤字の場合はベット額縮小を検討してください\n"
            + "  ".join(ror_lines)
        )
    else:
        print(f"\n  ✅  破綻リスク {ror_20:.1f}% (¥20万) — 許容範囲内")

    return discord_msg


# ── レポート表示 ──────────────────────────────────────────────────────────────


def _render_report(rows: list[dict], stats: dict, updated_count: int) -> str | None:
    n_total = len(rows)
    n_done = stats.get("n_done", 0)

    print("\n" + "=" * 68)
    print("  FukushoEliteFilter — 事後結果照合レポート")
    print("=" * 68)
    print(f"  CSV総レース数 : {n_total}")
    print(f"  今回照合件数  : {updated_count} 件 新たに確定")
    print(f"  累計結果確定  : {n_done} / {n_total} レース")

    if n_done == 0:
        print("\n  結果確定レースなし。レース後に再実行してください。")
        return None

    pnl_sign = "+" if stats["pnl"] >= 0 else "-"
    bar_fill = int(n_done / _TARGET * 20)
    bar = "█" * bar_fill + "░" * (20 - bar_fill)

    print(
        f"\n  OOS 検証進捗 : {n_done}/{_TARGET} [{bar}] {n_done / _TARGET * 100:.0f}%"
    )
    print()
    print(f"  {'項目':<16}{'値':>16}")
    print("  " + "-" * 34)
    print(f"  {'総投資額':<16}¥{stats['investment']:>12,}")
    print(f"  {'総回収額':<16}¥{stats['ret']:>12,}")
    print(f"  {'損益':<16}{pnl_sign}¥{abs(stats['pnl']):>11,}")
    print(f"  {'ROI':<16}{stats['roi']:>15.1f}%")
    print(
        f"  {'的中レース率':<14}{stats['hit_rate']:>14.1f}%  ({stats['hit_rows']}/{n_done})"
    )
    print(f"  {'最大DD':<16}¥{stats['max_dd']:>12,}")
    print(f"  {'検証期間':<14}{stats['period']:>18}")

    return _render_monthly_target(stats)


def _render_verdict(
    stats: dict,
    rows: list[dict],
    venue_stats: list[dict],
    proposal: dict | None,
) -> None:
    roi = stats["roi"]
    passed = roi > _ROI_PASS

    print("\n" + "=" * 68)
    print("  🏁 30レース OOS 検証 — 最終判定")
    print("=" * 68)

    if passed:
        print(f"\n  ✅  APPROVED  — ROI {roi:.1f}% > {_ROI_PASS}%  合格")
        print()
        print("  このフィルターは UMALOGI公認プロトコルとして永久保存されました。")
        if _PROTOCOL_LOG.exists():
            print(f"  保存先: {_PROTOCOL_LOG}")
        print()
        print("  公認フィルター条件:")
        f = _ORIGINAL_FILTER
        print(f"    会場     : {', '.join(f['venues'])}")
        print(f"    頭数     : {f['n_horses_min']}頭以上")
        print(f"    Edge閾値 : ≥ {f['edge_threshold']}")
    else:
        print(f"\n  ❌  REJECTED  — ROI {roi:.1f}% ≦ {_ROI_PASS}%  不合格")
        print()
        print("  ── 会場別 ROI 分析 ─────────────────────────────────────")
        print(f"  {'会場':<6}  {'件数':>5}  {'ROI':>8}  {'損益':>10}")
        print("  " + "-" * 38)
        for v in venue_stats:
            pnl_sign = "+" if v["pnl"] >= 0 else "-"
            flag = " ⬅ 足引き" if v["roi"] < 85 else (" ✅" if v["roi"] >= 100 else "")
            print(
                f"  {v['venue']:<6}  {v['n']:>5}件  {v['roi']:>7.1f}%"
                f"  {pnl_sign}¥{abs(v['pnl']):>7,}{flag}"
            )

        if proposal:
            poor = proposal.get("poor_venues", [])
            if poor:
                print(f"\n  足を引っ張っている会場: {', '.join(poor)}")

            print("\n  ── 次の30レース向け 改良フィルター提案 ─────────────────")
            pf = proposal["proposed_filter"]
            print(f"    会場     : {', '.join(pf['venues'])}")
            print(f"    頭数     : {pf['n_horses_min']}頭以上 (変更なし)")
            print(f"    Edge閾値 : ≥ {pf['edge_threshold']}")

            sens = proposal.get("edge_sensitivity", [])
            if sens:
                print()
                print("  ── Edge 閾値 感度分析 ────────────────────────────────────")
                print(f"  {'閾値':>6}  {'件数':>5}  {'ROI':>8}  {'損益':>10}")
                print("  " + "-" * 38)
                for s in sens:
                    pnl_sign = "+" if s["pnl"] >= 0 else "-"
                    best_mark = (
                        " ← 最適"
                        if proposal.get("best_edge")
                        and s["threshold"] == proposal["best_edge"]["threshold"]
                        else ""
                    )
                    print(
                        f"  ≥{s['threshold']:.1f}  {s['n_races']:>5}件  {s['roi']:>7.1f}%"
                        f"  {pnl_sign}¥{abs(s['pnl']):>7,}{best_mark}"
                    )

            print()
            print("  提案フィルターを実装する場合は bet_generator.py の")
            print("  _FUKUSHO_ELITE_VENUES / _FUKUSHO_ELITE_EDGE を更新してください。")


# ── ステータスサマリー（full_pipeline.py から呼び出し用）────────────────────


def status_summary() -> dict:
    """
    full_pipeline.py --status から呼び出されるサマリー関数。
    CSV の現在状態をまとめた dict を返す。
    """
    rows = _load_csv()
    stats = _calc_stats(rows)
    return {
        "n_total": len(rows),
        "n_done": stats.get("n_done", 0),
        "roi": stats.get("roi", 0.0),
        "pnl": stats.get("pnl", 0),
        "investment": stats.get("investment", 0),
        "hit_rate": stats.get("hit_rate", 0.0),
        "hit_rows": stats.get("hit_rows", 0),
        "target": _TARGET,
        "protocol_approved": _PROTOCOL_LOG.exists() and _check_approved(),
    }


def _check_approved() -> bool:
    if not _PROTOCOL_LOG.exists():
        return False
    try:
        data = json.loads(_PROTOCOL_LOG.read_text(encoding="utf-8"))
        return data.get("verdict") == "APPROVED"
    except Exception:
        return False


# ── メイン ────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="FukushoEliteFilter 事後結果照合")
    parser.add_argument(
        "--dry-run", action="store_true", help="CSV を更新せずレポートのみ出力"
    )
    parser.add_argument("--force", action="store_true", help="照合済み行も再照合する")
    args = parser.parse_args()

    rows = _load_csv()
    if not rows:
        print("CSV が空または存在しません:", _ELITE_CSV)
        return

    conn = _conn()
    updated_count = 0

    for row in rows:
        # 照合スキップ条件: --force なし かつ 既に result が記録済み
        if row.get("result") and not args.force:
            continue

        horse_nums = _parse_horse_nums(row.get("horse_numbers", ""))
        if not horse_nums:
            continue

        result = _lookup_race_result(conn, row["race_id"], horse_nums)
        if result is None:
            continue  # 払戻データ未登録（レース未施行 or DB未更新）

        result_str, total_return = result
        if not args.dry_run:
            row["result"] = result_str
            row["payout"] = str(total_return)
        updated_count += 1

    conn.close()

    if not args.dry_run and updated_count > 0:
        _save_csv(rows)
        print(f"CSV 更新: {updated_count} 件 照合完了 → {_ELITE_CSV}")

    stats = _calc_stats(rows)
    discord_warning = _render_report(rows, stats, updated_count)

    if discord_warning:
        try:
            from src.notification.discord_notifier import DiscordNotifier

            DiscordNotifier().notify_ror_warning(discord_warning)
        except Exception as exc:
            print(f"  Discord RoR通知失敗: {exc}")

    n_done = stats.get("n_done", 0)
    if n_done < _TARGET:
        remain = _TARGET - n_done
        print(f"\n  OOS検証完了まで残り {remain} レース。週明けに再実行してください。")
        return

    # ── 30レース到達: 合否判定 ──────────────────────────────────────
    venue_stats = _analyze_by_venue(rows)
    roi = stats.get("roi", 0.0)
    passed = roi > _ROI_PASS

    proposal: dict | None = None
    if passed:
        if not args.dry_run:
            proto_path = _save_protocol(stats, rows)
            print(f"\n  公認プロトコル保存: {proto_path}")
    else:
        proposal = _propose_next_filter(rows, stats, venue_stats)

    _render_verdict(stats, rows, venue_stats, proposal)


if __name__ == "__main__":
    main()
