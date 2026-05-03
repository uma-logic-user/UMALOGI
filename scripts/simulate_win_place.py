"""
単勝・複勝限定の 3 年間バックテスト + 資金管理シミュレーション

HonmeiModel（単勝）・PlaceModel（複勝）の両方を使い、
各レースで TOP-1 馬に ¥1,000 ずつ賭けた場合の損益・資金推移を算出する。

資金管理ルール:
  - 初期資本: ¥100,000
  - 賭け方:   単勝 TOP-1 に ¥1,000 + 複勝 TOP-1 に ¥1,000 = 1レース最大 ¥2,000
  - 残高が ¥1,000 未満になったらそのタイプのベットをスキップ（破産保護）

使用例:
    py scripts/simulate_win_place.py
    py scripts/simulate_win_place.py --year 2026
    py scripts/simulate_win_place.py --date-from 2024-01-01 --date-to 2026-12-31
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

import logging
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sim_wp")


BET_AMOUNT = 1_000
INITIAL_CAPITAL = 100_000


@dataclass
class RaceBetRecord:
    race_id: str
    race_date: str
    win_predicted_horse: int
    win_odds: float
    win_hit: bool
    win_payout: int
    win_invested: int
    place_predicted_horse: int
    place_odds_low: float
    place_odds_high: float
    place_hit: bool
    place_payout: int
    place_invested: int
    balance_after: float


@dataclass
class SimResult:
    records: list[RaceBetRecord] = field(default_factory=list)
    initial_capital: float = INITIAL_CAPITAL

    @property
    def final_balance(self) -> float:
        if not self.records:
            return self.initial_capital
        return self.records[-1].balance_after

    @property
    def total_invested_win(self) -> int:
        return sum(r.win_invested for r in self.records)

    @property
    def total_invested_place(self) -> int:
        return sum(r.place_invested for r in self.records)

    @property
    def total_invested(self) -> int:
        return self.total_invested_win + self.total_invested_place

    @property
    def total_payout_win(self) -> int:
        return sum(r.win_payout for r in self.records)

    @property
    def total_payout_place(self) -> int:
        return sum(r.place_payout for r in self.records)

    @property
    def n_win_hits(self) -> int:
        return sum(1 for r in self.records if r.win_hit)

    @property
    def n_place_hits(self) -> int:
        return sum(1 for r in self.records if r.place_hit)

    @property
    def n_win_bets(self) -> int:
        return sum(1 for r in self.records if r.win_invested > 0)

    @property
    def n_place_bets(self) -> int:
        return sum(1 for r in self.records if r.place_invested > 0)

    @property
    def roi_win(self) -> float:
        if self.total_invested_win == 0:
            return 0.0
        return self.total_payout_win / self.total_invested_win * 100

    @property
    def roi_place(self) -> float:
        if self.total_invested_place == 0:
            return 0.0
        return self.total_payout_place / self.total_invested_place * 100

    @property
    def max_drawdown(self) -> float:
        """最大ドローダウン（高値からの最大下落額）"""
        if not self.records:
            return 0.0
        peak = self.initial_capital
        max_dd = 0.0
        for r in self.records:
            bal = r.balance_after
            peak = max(peak, bal)
            dd = peak - bal
            max_dd = max(max_dd, dd)
        return max_dd

    @property
    def max_consecutive_losses(self) -> int:
        """最長連敗数（単勝・複勝のどちらかが当たればリセット）"""
        max_streak = current = 0
        for r in self.records:
            if not r.win_hit and not r.place_hit:
                current += 1
                max_streak = max(max_streak, current)
            else:
                current = 0
        return max_streak


def _load_payout_cache(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    """単勝・複勝払戻をキャッシュとして返す。"""
    win_cache: dict[str, int] = {}
    place_cache: dict[str, dict[str, int]] = {}

    rows = conn.execute(
        """
        SELECT rp.race_id, rp.bet_type, rp.combination, rp.payout
        FROM race_payouts rp
        JOIN races r ON rp.race_id=r.race_id
        WHERE r.date >= ? AND r.date <= ?
          AND rp.bet_type IN ('単勝','複勝')
        """,
        (date_from, date_to),
    ).fetchall()

    for race_id, bet_type, combo, payout in rows:
        if bet_type == "単勝":
            win_cache[f"{race_id}:{combo}"] = payout
        else:
            place_cache.setdefault(race_id, {})[combo] = payout

    return win_cache, place_cache


def simulate_win_place(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
    initial_capital: float = INITIAL_CAPITAL,
    bet_amount: int = BET_AMOUNT,
) -> SimResult:
    """単勝・複勝限定シミュレーション。"""
    from src.ml.models import HonmeiModel, PlaceModel
    from src.ml.features import FeatureBuilder

    honmei = HonmeiModel()
    place_model = PlaceModel()

    try:
        honmei.load()
    except FileNotFoundError:
        print("  ⚠  本命モデル未学習 — フォールバック予測を使用", flush=True)

    try:
        place_model.load()
    except FileNotFoundError:
        print("  ⚠  複勝モデル未学習 — フォールバック予測を使用", flush=True)

    fb = FeatureBuilder(conn)

    races = conn.execute(
        """
        SELECT DISTINCT r.race_id, r.date
        FROM races r
        WHERE r.date >= ? AND r.date <= ?
          AND EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id=r.race_id AND rr.rank=1)
        ORDER BY r.date, r.race_id
        """,
        (date_from, date_to),
    ).fetchall()

    win_cache, place_cache = _load_payout_cache(conn, date_from, date_to)

    result = SimResult(initial_capital=initial_capital)
    balance = initial_capital
    processed = 0

    for race_id, race_date in races:
        try:
            df = fb.build_race_features_for_simulate(race_id)
            if df is None or len(df) < 2:
                continue

            win_scores   = honmei.predict(df)
            place_scores = place_model.predict(df)

            win_top_idx   = win_scores.idxmax()
            place_top_idx = place_scores.idxmax()

            win_horse_name   = df.loc[win_top_idx,   "horse_name"]
            place_horse_name = df.loc[place_top_idx, "horse_name"]

            def _get_horse_num(name: str) -> int:
                row = conn.execute(
                    "SELECT horse_number FROM race_results WHERE race_id=? AND horse_name=? LIMIT 1",
                    (race_id, name),
                ).fetchone()
                return row[0] if row else 0

            win_horse_num   = _get_horse_num(win_horse_name)
            place_horse_num = _get_horse_num(place_horse_name)

            # ── 単勝 ─────────────────────────────────────────────
            # race_payouts.payout は ¥100 ベース払戻なので bet_amount/100 倍してスケール
            win_invested = 0
            win_payout   = 0
            win_hit      = False
            win_odds_val = 0.0
            if balance >= bet_amount:
                win_key = f"{race_id}:{win_horse_num}"
                raw_p = win_cache.get(win_key, 0)
                win_hit      = raw_p > 0
                win_payout   = int(raw_p * bet_amount / 100) if win_hit else 0
                win_invested = bet_amount
                win_odds_val = float(df.loc[win_top_idx, "win_odds"] or 0.0)
                balance      = balance - bet_amount + win_payout

            # ── 複勝 ─────────────────────────────────────────────
            place_invested  = 0
            place_payout    = 0
            place_hit       = False
            place_odds_low  = 0.0
            place_odds_high = 0.0
            if balance >= bet_amount:
                place_pays = place_cache.get(race_id, {})
                raw_p = place_pays.get(str(place_horse_num), 0)
                place_hit      = raw_p > 0
                place_payout   = int(raw_p * bet_amount / 100) if place_hit else 0
                place_invested = bet_amount
                balance        = balance - bet_amount + place_payout
                if place_pays:
                    vals = list(place_pays.values())
                    place_odds_low  = min(vals) / 100.0
                    place_odds_high = max(vals) / 100.0

            balance = max(balance, 0.0)

            result.records.append(RaceBetRecord(
                race_id=race_id,
                race_date=race_date,
                win_predicted_horse=win_horse_num,
                win_odds=win_odds_val,
                win_hit=win_hit,
                win_payout=win_payout,
                win_invested=win_invested,
                place_predicted_horse=place_horse_num,
                place_odds_low=place_odds_low,
                place_odds_high=place_odds_high,
                place_hit=place_hit,
                place_payout=place_payout,
                place_invested=place_invested,
                balance_after=balance,
            ))

            processed += 1
            if processed % 500 == 0:
                print(f"  処理済: {processed}/{len(races)}レース  残高: ¥{balance:,.0f}", flush=True)

        except Exception as e:
            logger.debug("race %s スキップ: %s", race_id, e)
            continue

    print(f"  シミュレーション完了: {processed}レース処理", flush=True)
    return result


def _write_report(
    results: dict[str, SimResult],
    output_path: Path,
) -> None:
    """docs/win_place_simulation_report.md を生成する。"""
    from datetime import datetime
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = [
        "# UMALOGI 単複特化 3年間シミュレーション レポート",
        "",
        f"**生成日時**: {now_str}",
        f"**初期資本**: ¥{INITIAL_CAPITAL:,}",
        f"**賭け金**:   ¥{BET_AMOUNT:,}/型/レース",
        "",
        "---",
        "",
        "## 1. モデル改善概要",
        "",
        "| 項目 | 修正前 | 修正後 |",
        "|---|---|---|",
        "| 学習サンプル/レース | ~3頭 (rank IS NOT NULL + inner join) | 全頭 (~14頭) |",
        "| AUC（推定） | >0.97 (過剰推定) | 0.60〜0.82（正常範囲） |",
        "| EV外れ値キャップ | 500倍 (¥50,000) | 100倍 (¥10,000) |",
        "| 対象券種 | 馬連/馬単/三連単 | **単勝・複勝** |",
        "",
        "---",
        "",
        "## 2. 年別シミュレーション結果",
        "",
    ]

    for label, sim in results.items():
        if label == "ALL":
            continue
        if not sim.records:
            lines.append(f"### {label}: データなし")
            lines.append("")
            continue

        lines += [
            f"### {label}",
            "",
            f"**対象レース数**: {len(sim.records):,}件",
            "",
            "| 券種 | ベット数 | 的中 | 的中率 | ROI | 損益 |",
            "|---|---|---|---|---|---|",
        ]

        wb = sim.n_win_bets
        wh = sim.n_win_hits
        wi = sim.total_invested_win
        wp = sim.total_payout_win
        wpl = wp - wi
        wroi = wp / wi * 100 if wi > 0 else 0.0
        wsign = "+" if wpl >= 0 else ""
        lines.append(f"| 単勝 | {wb:,} | {wh:,} | {wh/wb*100:.1f}% | {wroi:.1f}% | {wsign}¥{wpl:,.0f} |")

        pb = sim.n_place_bets
        ph = sim.n_place_hits
        pi = sim.total_invested_place
        pp = sim.total_payout_place
        ppl = pp - pi
        proi = pp / pi * 100 if pi > 0 else 0.0
        psign = "+" if ppl >= 0 else ""
        lines.append(f"| 複勝 | {pb:,} | {ph:,} | {ph/pb*100:.1f}% | {proi:.1f}% | {psign}¥{ppl:,.0f} |")
        lines.append("")

        lines += [
            "**資金管理シミュレーション**:",
            "",
            "| 項目 | 値 |",
            "|---|---|",
            f"| 初期資本 | ¥{sim.initial_capital:,.0f} |",
            f"| 最終残高 | ¥{sim.final_balance:,.0f} |",
            f"| 損益 | {'+'  if sim.final_balance >= sim.initial_capital else ''}¥{(sim.final_balance - sim.initial_capital):,.0f} |",
            f"| 最大ドローダウン | ¥{sim.max_drawdown:,.0f} |",
            f"| 最長連敗数 | {sim.max_consecutive_losses}レース |",
            f"| 残高<¥1万 発生件数 | {sum(1 for r in sim.records if r.balance_after < 10_000):,}件 |",
            "",
        ]

    # 3年合算
    if "ALL" in results:
        sim = results["ALL"]
        if sim.records:
            wb = sim.n_win_bets
            wh = sim.n_win_hits
            pb = sim.n_place_bets
            ph = sim.n_place_hits
            lines += [
                "---",
                "",
                "## 3. 3年合算 総合評価",
                "",
                "| 項目 | 単勝 | 複勝 |",
                "|---|---|---|",
                f"| ベット数 | {wb:,} | {pb:,} |",
                f"| 的中数 | {wh:,} | {ph:,} |",
                f"| 的中率 | {wh/wb*100:.1f}% | {ph/pb*100:.1f}% |",
                f"| ROI | {sim.roi_win:.1f}% | {sim.roi_place:.1f}% |",
                f"| 投資合計 | ¥{sim.total_invested_win:,} | ¥{sim.total_invested_place:,} |",
                f"| 払戻合計 | ¥{sim.total_payout_win:,} | ¥{sim.total_payout_place:,} |",
                "",
                f"**初期 ¥{sim.initial_capital:,.0f} → 最終 ¥{sim.final_balance:,.0f}**",
                "",
                f"**最大ドローダウン: ¥{sim.max_drawdown:,.0f}**",
                f"**最長連敗: {sim.max_consecutive_losses}レース**",
                "",
            ]

    lines += [
        "---",
        "",
        "## 4. 投資戦略考察",
        "",
        "### 単勝の特性",
        "- 的中率: 約 5〜10%（1/14〜1/18 の逆数）",
        "- 的中時の平均払戻: 数百円〜数万円",
        "- ROI 80%以上が目標ライン（控除率 約 20%）",
        "",
        "### 複勝の特性",
        "- 的中率: 約 15〜25%（3着以内 = 3/14〜3/18）",
        "- 平均払戻: 110〜300円（低リスク・低リターン）",
        "- ROI 85%以上が目標ライン（控除率 約 15%）",
        "",
        "### 資金管理推奨",
        "- 1レースベット額を資産の1〜2%以下に抑える",
        "- 単勝20連敗（= ¥20,000損失）は確率的に 1.5〜3ヶ月に1回発生",
        "- ¥100,000 資本では単勝のみでも年間 ¥50,000〜¥70,000 消費ペース",
        "",
        f"*このレポートは {now_str} に自動生成されました。*",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  レポート出力: {output_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="単勝・複勝 3年間シミュレーション")
    ap.add_argument("--date-from", default="2024-01-01")
    ap.add_argument("--date-to",   default="2026-12-31")
    ap.add_argument("--year",      default=None, help="単年のみ (例: 2025)")
    ap.add_argument("--initial-capital", type=int, default=INITIAL_CAPITAL)
    ap.add_argument("--bet-amount",      type=int, default=BET_AMOUNT)
    args = ap.parse_args()

    from src.database.init_db import init_db
    conn = init_db()

    print("=" * 60)
    print("  単勝・複勝 シミュレーション")
    print(f"  初期資本: ¥{args.initial_capital:,}  賭け金: ¥{args.bet_amount:,}/型")
    print("=" * 60)

    results: dict[str, SimResult] = {}

    if args.year:
        label = args.year
        print(f"\n[{label}] シミュレーション中...")
        t0 = time.time()
        results[label] = simulate_win_place(
            conn, f"{label}-01-01", f"{label}-12-31",
            initial_capital=args.initial_capital,
            bet_amount=args.bet_amount,
        )
        print(f"[{label}] 完了: {time.time()-t0:.1f}秒")
    else:
        for yr in ["2024", "2025", "2026"]:
            print(f"\n[{yr}] シミュレーション中...")
            t0 = time.time()
            results[yr] = simulate_win_place(
                conn, f"{yr}-01-01", f"{yr}-12-31",
                initial_capital=args.initial_capital,
                bet_amount=args.bet_amount,
            )
            print(f"[{yr}] 完了: {time.time()-t0:.1f}秒")

        print(f"\n[ALL 2024-2026] シミュレーション中...")
        t0 = time.time()
        results["ALL"] = simulate_win_place(
            conn, args.date_from, args.date_to,
            initial_capital=args.initial_capital,
            bet_amount=args.bet_amount,
        )
        print(f"[ALL] 完了: {time.time()-t0:.1f}秒")

    conn.close()

    print("\n" + "=" * 60)
    print("  結果サマリー")
    print("=" * 60)
    for label, sim in results.items():
        if not sim.records:
            print(f"  [{label}] データなし")
            continue
        print(f"\n  [{label}] {len(sim.records):,}レース")
        if sim.n_win_bets > 0:
            print(f"    単勝: {sim.n_win_hits:,}的中/{sim.n_win_bets:,}件 ROI={sim.roi_win:.1f}%")
        if sim.n_place_bets > 0:
            print(f"    複勝: {sim.n_place_hits:,}的中/{sim.n_place_bets:,}件 ROI={sim.roi_place:.1f}%")
        print(f"    初期¥{sim.initial_capital:,.0f} → 最終¥{sim.final_balance:,.0f}")
        print(f"    最大DD: ¥{sim.max_drawdown:,.0f}  最長連敗: {sim.max_consecutive_losses}レース")

    out_path = _ROOT / "docs" / "win_place_simulation_report.md"
    _write_report(results, out_path)


if __name__ == "__main__":
    main()
