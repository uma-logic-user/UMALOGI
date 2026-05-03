"""
所持金推移シミュレーション（単勝・複勝特化）

初期資金 100,000円 / 1R×1券種あたり固定 1,000円 賭けで
本命・卍・複勝モデルの単勝・複勝買いを 2024-2026 年バックテストする。

使用例:
    py scripts/bankroll_sim_win_place.py
    py scripts/bankroll_sim_win_place.py --year-from 2024 --year-to 2026
    py scripts/bankroll_sim_win_place.py --ev-threshold 1.1
    py scripts/bankroll_sim_win_place.py --discord  # 結果をDiscordに送信
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

import sqlite3
import numpy as np
import pandas as pd
from src.database.init_db import init_db
from src.ml.features import FeatureBuilder
from src.ml.models import HonmeiModel, ManjiModel, PlaceModel, _MODEL_DIR, FEATURE_COLS

_INITIAL_BANKROLL = 100_000.0
_BET_AMOUNT       = 1_000.0   # 1R×1券種あたり固定賭け金
_EV_THRESHOLD     = 1.1       # 買い推奨の EV 閾値


def _load_tansho_payouts(conn: sqlite3.Connection) -> dict[tuple[str, str], float]:
    """race_id × combination の単勝払戻辞書（combination は馬番の文字列）。"""
    rows = conn.execute(
        "SELECT race_id, combination, payout FROM race_payouts WHERE bet_type = '単勝'"
    ).fetchall()
    return {(r[0], r[1]): float(r[2] or 0) for r in rows}


def _load_fukusho_payouts(conn: sqlite3.Connection) -> dict[tuple[str, str], float]:
    """race_id × combination の複勝払戻辞書。"""
    rows = conn.execute(
        "SELECT race_id, combination, payout FROM race_payouts WHERE bet_type = '複勝'"
    ).fetchall()
    return {(r[0], r[1]): float(r[2] or 0) for r in rows}


def _simulate(
    conn: sqlite3.Connection,
    honmei: HonmeiModel,
    manji: ManjiModel,
    place: PlaceModel,
    year_from: int,
    year_to: int,
    ev_threshold: float,
) -> dict:
    fb = FeatureBuilder(conn)
    tansho_pay  = _load_tansho_payouts(conn)
    fukusho_pay = _load_fukusho_payouts(conn)

    race_rows = conn.execute(
        """
        SELECT DISTINCT r.race_id, r.date
        FROM races r
        JOIN race_results rr ON rr.race_id = r.race_id
        WHERE rr.rank IS NOT NULL
          AND CAST(substr(r.date, 1, 4) AS INTEGER) BETWEEN ? AND ?
        ORDER BY r.date, r.race_id
        """,
        (year_from, year_to),
    ).fetchall()

    bankroll = _INITIAL_BANKROLL
    peak     = _INITIAL_BANKROLL
    max_dd   = 0.0
    balance_history: list[tuple[str, float]] = []
    n_bets = n_hits = 0
    total_invested = 0.0
    total_payout   = 0.0

    # モデル×券種ごとの詳細統計
    detail: dict[str, dict] = {
        "honmei_win":  {"bets": 0, "hits": 0, "invested": 0.0, "payout": 0.0},
        "manji_win":   {"bets": 0, "hits": 0, "invested": 0.0, "payout": 0.0},
        "place_fuku":  {"bets": 0, "hits": 0, "invested": 0.0, "payout": 0.0},
    }

    for race_id, race_date in race_rows:
        try:
            df = fb.build_race_features_for_simulate(race_id)
        except Exception:
            continue
        if df.empty:
            continue

        # 着順マップ: horse_name → rank (simulate では horse_name キーが信頼できる)
        actual = conn.execute(
            "SELECT horse_name, horse_number, rank FROM race_results WHERE race_id = ? AND rank IS NOT NULL",
            (race_id,),
        ).fetchall()
        rank_by_name:   dict[str, int] = {r[0]: int(r[2]) for r in actual}
        hnum_by_name:   dict[str, int] = {r[0]: int(r[1]) for r in actual}

        win_odds = df["win_odds"].fillna(0.0).astype(float)

        # ── 本命モデル単勝 ──────────────────────────────────────────
        h_scores = honmei.predict(df)
        h_ev     = h_scores * win_odds

        best_h_idx  = int(h_ev.idxmax()) if not h_ev.empty else -1
        best_h_ev   = float(h_ev.iloc[best_h_idx]) if best_h_idx >= 0 else 0.0
        if best_h_ev >= ev_threshold and bankroll >= _BET_AMOUNT:
            horse_name = str(df.iloc[best_h_idx].get("horse_name", ""))
            real_hnum  = hnum_by_name.get(horse_name, 0)
            combo      = str(real_hnum) if real_hnum else ""
            pay = tansho_pay.get((race_id, combo), 0.0)
            is_hit = (rank_by_name.get(horse_name) == 1) and pay > 0
            ret = (pay / 100 * _BET_AMOUNT) if is_hit else 0.0
            bankroll -= _BET_AMOUNT; bankroll += ret
            total_invested += _BET_AMOUNT; total_payout += ret
            n_bets += 1; n_hits += int(is_hit)
            d = detail["honmei_win"]
            d["bets"] += 1; d["hits"] += int(is_hit)
            d["invested"] += _BET_AMOUNT; d["payout"] += ret

        # ── 卍モデル単勝 ────────────────────────────────────────────
        m_ev = manji.ev_score(df)

        best_m_idx = int(m_ev.idxmax()) if not m_ev.empty else -1
        best_m_ev  = float(m_ev.iloc[best_m_idx]) if best_m_idx >= 0 else 0.0
        if best_m_ev >= ev_threshold and bankroll >= _BET_AMOUNT:
            horse_name = str(df.iloc[best_m_idx].get("horse_name", ""))
            real_hnum  = hnum_by_name.get(horse_name, 0)
            combo      = str(real_hnum) if real_hnum else ""
            pay = tansho_pay.get((race_id, combo), 0.0)
            is_hit = (rank_by_name.get(horse_name) == 1) and pay > 0
            ret = (pay / 100 * _BET_AMOUNT) if is_hit else 0.0
            bankroll -= _BET_AMOUNT; bankroll += ret
            total_invested += _BET_AMOUNT; total_payout += ret
            n_bets += 1; n_hits += int(is_hit)
            d = detail["manji_win"]
            d["bets"] += 1; d["hits"] += int(is_hit)
            d["invested"] += _BET_AMOUNT; d["payout"] += ret

        # ── 複勝モデル複勝 ──────────────────────────────────────────
        p_scores = place.predict(df)
        # 複勝EV: 複勝オッズは記録がないためフォールバック推定
        # 実際の払戻を照合: 的中馬の複勝払戻を後から確認するのみ（EV買い判断は p_score 閾値）
        best_p_idx = int(p_scores.idxmax()) if not p_scores.empty else -1
        best_p_score = float(p_scores.iloc[best_p_idx]) if best_p_idx >= 0 else 0.0
        # 複勝は P(place) > 0.45 で推奨（複勝理論EV≒1.1 の基準）
        if best_p_score >= 0.45 and bankroll >= _BET_AMOUNT:
            horse_name = str(df.iloc[best_p_idx].get("horse_name", ""))
            real_hnum  = hnum_by_name.get(horse_name, 0)
            combo      = str(real_hnum) if real_hnum else ""
            pay = fukusho_pay.get((race_id, combo), 0.0)
            is_hit = (int(rank_by_name.get(horse_name) or 99) <= 3) and pay > 0
            ret = (pay / 100 * _BET_AMOUNT) if is_hit else 0.0
            bankroll -= _BET_AMOUNT; bankroll += ret
            total_invested += _BET_AMOUNT; total_payout += ret
            n_bets += 1; n_hits += int(is_hit)
            d = detail["place_fuku"]
            d["bets"] += 1; d["hits"] += int(is_hit)
            d["invested"] += _BET_AMOUNT; d["payout"] += ret

        # ドローダウン更新
        peak   = max(peak, bankroll)
        dd     = peak - bankroll
        max_dd = max(max_dd, dd)
        balance_history.append((race_date, bankroll))

    roi      = total_payout / total_invested * 100 if total_invested > 0 else 0.0
    hit_rate = n_hits / n_bets * 100 if n_bets > 0 else 0.0

    return {
        "initial_bankroll": _INITIAL_BANKROLL,
        "final_bankroll":   bankroll,
        "profit":           bankroll - _INITIAL_BANKROLL,
        "roi":              roi,
        "max_drawdown":     max_dd,
        "n_bets":           n_bets,
        "n_hits":           n_hits,
        "hit_rate":         hit_rate,
        "total_invested":   total_invested,
        "total_payout":     total_payout,
        "balance_history":  balance_history,
        "detail":           detail,
    }


def _ascii_bar_chart(balance_history: list[tuple[str, float]]) -> str:
    if not balance_history:
        return ""
    df = pd.DataFrame(balance_history, columns=["date", "balance"])
    df["ym"] = df["date"].str[:7]
    monthly = df.groupby("ym")["balance"].last()
    _MAX_BAR = 28
    max_b = max(monthly.max(), _INITIAL_BANKROLL) or 1
    lines = ["\n── 月次残高推移 ─────────────────────────"]
    for ym, bal in monthly.items():
        bar_len = max(0, int(bal / max_b * _MAX_BAR))
        bar  = "#" * bar_len
        mark = "▲" if bal > _INITIAL_BANKROLL else ("▼" if bal < _INITIAL_BANKROLL else "─")
        lines.append(f"  {ym}: {bar:<{_MAX_BAR}} ¥{bal:>10,.0f} {mark}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="UMALOGI 所持金推移シミュレーション")
    parser.add_argument("--year-from",    type=int,   default=2024)
    parser.add_argument("--year-to",      type=int,   default=2026)
    parser.add_argument("--ev-threshold", type=float, default=_EV_THRESHOLD)
    parser.add_argument("--discord",      action="store_true", help="結果をDiscordに送信")
    args = parser.parse_args()

    conn   = init_db()
    honmei = HonmeiModel()
    manji  = ManjiModel()
    place  = PlaceModel()

    for model, name in [(honmei, "本命"), (manji, "卍"), (place, "複勝")]:
        try:
            model.load()  # type: ignore[union-attr]
        except FileNotFoundError:
            print(f"⚠️  {name}モデルが見つかりません。先に scripts/run_train.py を実行してください")

    print(f"\n=== 所持金推移シミュレーション {args.year_from}-{args.year_to} ===")
    print(f"初期資金: ¥{_INITIAL_BANKROLL:,.0f}  固定賭け金: ¥{_BET_AMOUNT:,.0f}  EV閾値: {args.ev_threshold}")
    print("計算中 (数分かかります)...")

    res = _simulate(conn, honmei, manji, place, args.year_from, args.year_to, args.ev_threshold)
    conn.close()

    summary = (
        f"\n── 結果サマリー ({args.year_from}〜{args.year_to}年) ──────────────────\n"
        f"初期資金      : ¥{res['initial_bankroll']:>12,.0f}\n"
        f"最終残高      : ¥{res['final_bankroll']:>12,.0f}  (損益: {res['profit']:+,.0f})\n"
        f"最大ドローダウン: ¥{res['max_drawdown']:>12,.0f}\n"
        f"回収率        : {res['roi']:.1f}%\n"
        f"賭け回数      : {res['n_bets']:,}回  的中: {res['n_hits']:,}回  的中率: {res['hit_rate']:.1f}%\n"
        f"総投資額      : ¥{res['total_invested']:>12,.0f}\n"
        f"総払戻額      : ¥{res['total_payout']:>12,.0f}"
    )
    print(summary)

    print("\n── モデル×券種別詳細 ──────────────────")
    for key, d in res["detail"].items():
        roi_d = d["payout"] / d["invested"] * 100 if d["invested"] > 0 else 0.0
        hr_d  = d["hits"] / d["bets"] * 100 if d["bets"] > 0 else 0.0
        label = {"honmei_win": "本命モデル 単勝", "manji_win": "卍モデル  単勝", "place_fuku": "複勝モデル 複勝"}.get(key, key)
        print(f"  {label}: {d['bets']:>4}回  的中率{hr_d:5.1f}%  ROI{roi_d:6.1f}%  損益{d['payout']-d['invested']:+,.0f}円")

    bar_chart = _ascii_bar_chart(res["balance_history"])
    print(bar_chart)

    if args.discord:
        try:
            import os, requests
            url = os.getenv("DISCORD_WEBHOOK_URL", "")
            if url:
                discord_msg = (
                    f"📊 **バックテスト結果 {args.year_from}-{args.year_to}** (単複特化)\n"
                    f"```\n{summary.strip()}\n```"
                )
                requests.post(url, json={"content": discord_msg}, timeout=10)
                print("\n✅ Discord に結果を送信しました")
        except Exception as e:
            print(f"Discord 送信失敗: {e}")


if __name__ == "__main__":
    main()
