"""未来資産曲線シミュレータ（Future Asset Curve Simulator）.

UMALOGI の武器（Pure_EV_Edge 単複戦略・卍較正器）を用いて、1年間（52週間 /
約2,000レース）運用した場合の資産推移をモンテカルロ法でシミュレーションする。

⚠️ 前提の健全性（誠実性の担保 / CLAUDE.md 条項・安全第一方針に準拠）:
    本スクリプトが扱う ROI には2系統がある。
      - バックテスト/out-of-sample 値（単複 222% / Pure_EV_Edge 252-270%）
      - 確定実績の真 ROI（約 80% = 現状は負け、会計是正後）
    前者は「理論上の最大ポテンシャル」、後者は「現実の足元」を表す。
    レポートでは両者を併記し、爆益の数値が "バックテスト前提" であることを明示する。
    （卍較正器 ECE=0.0177 は精度指標。ただし W-048 で実弾は停止中。）

設計:
    - 1レース = 1ベット。勝率 p と推定オッズ o を確率的に生成し、
      期待グロス ROI が設定値に近づくよう o を決める（o = target_roi / p をクリップ）。
    - Kelly 比率 f に基づきステークを決定。1レース上限投資額（cap）でクランプ。
    - SNS / Note 収益は週次でプールに加算（原資ゼロ・複利寄与）。
    - 試行を N 回（デフォルト 1,000）繰り返し、週次資産の中央値 / 上位10% /
      下位10% の3本の損益曲線と、最大ドローダウン・SNS累積を算出する。

依存: 標準ライブラリのみ（numpy 不要）。再現性のため seed を固定可能。
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")  # Windows UTF-8 強制（CLAUDE.md 条項6）

# --------------------------------------------------------------------------- #
# 定数
# --------------------------------------------------------------------------- #

WEEKS_PER_YEAR: Final[int] = 52
RACES_PER_YEAR: Final[int] = 2_000
RACES_PER_WEEK: Final[float] = RACES_PER_YEAR / WEEKS_PER_YEAR  # ≒ 38.5
DEFAULT_TRIALS: Final[int] = 1_000
DEFAULT_SEED: Final[int] = 20260601

# 勝率の生成レンジ（選択的高EVベッティングを想定: 単複の妙味馬）
WIN_PROB_LOW: Final[float] = 0.12
WIN_PROB_HIGH: Final[float] = 0.42
# オッズの現実的クリップ域
ODDS_MIN: Final[float] = 1.5
ODDS_MAX: Final[float] = 50.0


# --------------------------------------------------------------------------- #
# シナリオ定義
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Scenario:
    """1シナリオのパラメータ束。"""

    name: str
    label: str
    initial_bankroll: float
    kelly_fraction: float
    bet_cap: float  # 1レース上限投資額（円）
    cap_scales: bool  # True の場合、資産成長に応じて cap を流動性スケール
    target_roi: float  # 期待グロス ROI（1.0 = 収支トントン）
    sns_weekly_income: float  # 週次 SNS/Note 純利（円・原資ゼロ）
    basis: str  # "backtest" | "confirmed" — 数値の出所
    # >0 の場合 Kelly を使わず資産比 force_bet_fraction で強制ベット。
    # 確定実績(負けエッジ)の損失を現実的に再現するために使う。
    force_bet_fraction: float = 0.0


def build_scenarios() -> list[Scenario]:
    """要件のシナリオA/Bに加え、確定実績ベースの現実シナリオCを構成する。"""
    return [
        Scenario(
            name="A",
            label="手堅い現実路線（1/10 Kelly・上限5万円・Noteサブスク30名）",
            initial_bankroll=1_000_000.0,
            kelly_fraction=0.10,
            bet_cap=50_000.0,
            cap_scales=False,
            target_roi=2.22,  # 単複実績(バックテスト) ROI 222%
            sns_weekly_income=(3_980.0 * 30) / (WEEKS_PER_YEAR / 12.0),  # 月額→週次按分
            basis="backtest",
        ),
        Scenario(
            name="B",
            label="尖らせた攻め路線（1/5 Kelly・上限最大15万円・Noteショット5万円/週）",
            initial_bankroll=1_000_000.0,
            kelly_fraction=0.20,
            bet_cap=150_000.0,
            cap_scales=True,
            target_roi=2.52,  # Pure_EV_Edge 2年BT out-of-sample 下限 252%
            sns_weekly_income=50_000.0,  # 重賞バズ Note ショット（週次）
            basis="backtest",
        ),
        Scenario(
            name="C",
            label="確定実績ベース（現実の足元・真ROI約80%・会計是正後・SNSなし）",
            initial_bankroll=1_000_000.0,
            kelly_fraction=0.10,
            bet_cap=50_000.0,
            cap_scales=False,
            target_roi=0.80,  # 確定P&L 真ROI ≒ 80%（= 負け）
            sns_weekly_income=0.0,
            basis="confirmed",
            # 負けエッジでは Kelly が「見送り」となり横ばいになるため、
            # 実弾を打ち続けた現実（=損失の累積）を再現すべく資産1%で強制ベットする。
            force_bet_fraction=0.01,
        ),
    ]


# --------------------------------------------------------------------------- #
# コアロジック
# --------------------------------------------------------------------------- #


def kelly_stake(
    bankroll: float,
    p: float,
    odds: float,
    kelly_fraction: float,
    cap: float,
) -> float:
    """単一二項ベットの分数Kelly ステークを返す（0〜cap でクランプ）。

    f* = (p*(o-1) - (1-p)) / (o-1) = (p*o - 1) / (o-1)
    エッジが非正の場合はベットしない（0 を返す）。
    """
    denom = odds - 1.0
    if denom <= 0.0:  # オッズ1.0以下はゼロ除算・無意味ベット回避
        return 0.0
    kelly_f = (p * odds - 1.0) / denom
    if kelly_f <= 0.0:  # 負のエッジ → 見送り
        return 0.0
    stake = kelly_fraction * kelly_f * bankroll
    return max(0.0, min(stake, cap, bankroll))


def simulate_one_trial(
    scenario: Scenario,
    rng: random.Random,
) -> list[float]:
    """1試行を実行し、週次（53点: 開始 + 52週末）の資産推移を返す。"""
    bankroll = scenario.initial_bankroll
    curve: list[float] = [bankroll]
    races_done = 0.0

    for week in range(1, WEEKS_PER_YEAR + 1):
        # 当週に消化するレース数（端数を週ごとに累積して整数化）
        races_target = RACES_PER_WEEK * week
        n_races = int(round(races_target - races_done))
        races_done += n_races

        for _ in range(n_races):
            if bankroll <= 0.0:  # 破産したら以後ベット不能
                break
            p = rng.uniform(WIN_PROB_LOW, WIN_PROB_HIGH)
            # 期待グロス ROI が target に近づくよう o を決め、現実域にクリップ
            raw_odds = scenario.target_roi / p
            odds = max(ODDS_MIN, min(raw_odds, ODDS_MAX))

            cap = scenario.bet_cap
            if scenario.cap_scales:
                # 流動性スケール: 資産の3%か基準capの大きい方、ただし上限capで頭打ち
                cap = min(scenario.bet_cap, max(scenario.bet_cap, bankroll * 0.03))

            if scenario.force_bet_fraction > 0.0:
                # 確定実績シナリオ: エッジ符号に関わらず強制ベット（負けを実現）
                stake = min(scenario.force_bet_fraction * bankroll, cap, bankroll)
            else:
                stake = kelly_stake(bankroll, p, odds, scenario.kelly_fraction, cap)
            if stake <= 0.0:
                continue

            if rng.random() < p:  # 的中
                bankroll += stake * (odds - 1.0)
            else:  # 外れ
                bankroll -= stake

        # 週次 SNS/Note 純利をプールに加算（原資ゼロ・複利寄与）
        bankroll += scenario.sns_weekly_income
        curve.append(bankroll)

    return curve


@dataclass
class ScenarioResult:
    """1シナリオのモンテカルロ集計結果。"""

    scenario: Scenario
    median_curve: list[float] = field(default_factory=list)
    p90_curve: list[float] = field(default_factory=list)
    p10_curve: list[float] = field(default_factory=list)
    final_median: float = 0.0
    final_p90: float = 0.0
    final_p10: float = 0.0
    max_drawdown_median: float = 0.0  # 中央値曲線の最大DD（割合）
    sns_cumulative: float = 0.0
    ruin_probability: float = 0.0  # 最終資産 < 初期 になった試行の割合


def _percentile(sorted_vals: list[float], q: float) -> float:
    """ソート済みリストから線形補間でパーセンタイル値を返す（q: 0.0〜1.0）。"""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = q * (len(sorted_vals) - 1)
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return sorted_vals[lo]
    frac = idx - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


def _max_drawdown(curve: list[float]) -> float:
    """資産曲線の最大ドローダウン（ピークからの最大下落率, 0.0〜1.0）。"""
    peak = curve[0] if curve else 0.0
    max_dd = 0.0
    for v in curve:
        peak = max(peak, v)
        if peak > 0.0:
            dd = (peak - v) / peak
            max_dd = max(max_dd, dd)
    return max_dd


def run_scenario(
    scenario: Scenario,
    trials: int,
    seed: int,
) -> ScenarioResult:
    """1シナリオを trials 回試行し、3本の損益曲線と統計量を集計する。"""
    if trials <= 0:
        raise ValueError("trials は 1 以上である必要があります")

    rng = random.Random(seed)
    n_points = WEEKS_PER_YEAR + 1
    # week_index -> list of bankroll across trials
    per_week: list[list[float]] = [[] for _ in range(n_points)]
    finals: list[float] = []

    for _ in range(trials):
        curve = simulate_one_trial(scenario, rng)
        for i, v in enumerate(curve):
            per_week[i].append(v)
        finals.append(curve[-1])

    median_curve: list[float] = []
    p90_curve: list[float] = []
    p10_curve: list[float] = []
    for week_vals in per_week:
        sv = sorted(week_vals)
        median_curve.append(_percentile(sv, 0.50))
        p90_curve.append(_percentile(sv, 0.90))
        p10_curve.append(_percentile(sv, 0.10))

    sns_cumulative = scenario.sns_weekly_income * WEEKS_PER_YEAR
    ruin = sum(1 for f in finals if f < scenario.initial_bankroll) / trials

    return ScenarioResult(
        scenario=scenario,
        median_curve=median_curve,
        p90_curve=p90_curve,
        p10_curve=p10_curve,
        final_median=median_curve[-1],
        final_p90=p90_curve[-1],
        final_p10=p10_curve[-1],
        max_drawdown_median=_max_drawdown(median_curve),
        sns_cumulative=sns_cumulative,
        ruin_probability=ruin,
    )


# --------------------------------------------------------------------------- #
# レポート生成（Markdown + アスキーアート）
# --------------------------------------------------------------------------- #


def _fmt_yen(v: float) -> str:
    """円表記（億/万で見やすく丸める）。"""
    if abs(v) >= 1e8:
        return f"{v / 1e8:,.2f} 億円"
    if abs(v) >= 1e4:
        return f"{v / 1e4:,.1f} 万円"
    return f"{v:,.0f} 円"


def _sparkline(curve: list[float], width: int = 52) -> str:
    """資産曲線を Unicode ブロックのスパークラインで描く。"""
    blocks = "▁▂▃▄▅▆▇█"
    # 等間隔サンプリング
    if len(curve) <= width:
        sample = curve
    else:
        step = len(curve) / width
        sample = [curve[min(len(curve) - 1, int(i * step))] for i in range(width)]
    lo = min(sample)
    hi = max(sample)
    span = hi - lo
    if span <= 0:
        return blocks[0] * len(sample)
    out = []
    for v in sample:
        level = int((v - lo) / span * (len(blocks) - 1))
        out.append(blocks[max(0, min(level, len(blocks) - 1))])
    return "".join(out)


def _ascii_line_chart(
    series: list[tuple[str, list[float]]],
    height: int = 16,
    width: int = 60,
) -> str:
    """複数曲線を重ねた ASCII 折れ線グラフを描く。

    series: [(記号, 曲線), ...]。記号は1文字（例: 'M', 'H', 'L'）。
    """
    all_vals = [v for _, c in series for v in c]
    if not all_vals:
        return "(データなし)"
    lo = min(all_vals)
    hi = max(all_vals)
    span = hi - lo if hi > lo else 1.0

    # width 点に等間隔リサンプル
    def resample(curve: list[float]) -> list[float]:
        if len(curve) == width:
            return curve
        step = (len(curve) - 1) / (width - 1) if width > 1 else 0
        return [curve[int(round(i * step))] for i in range(width)]

    grid = [[" " for _ in range(width)] for _ in range(height)]
    for symbol, curve in series:
        rs = resample(curve)
        for x, v in enumerate(rs):
            y = int((v - lo) / span * (height - 1))
            y = max(0, min(y, height - 1))
            row = height - 1 - y  # 上が高値
            grid[row][x] = symbol

    lines = []
    for r, row in enumerate(grid):
        # 左端に Y軸ラベル（上端=hi, 下端=lo）
        if r == 0:
            label = f"{_fmt_yen(hi):>12} |"
        elif r == height - 1:
            label = f"{_fmt_yen(lo):>12} |"
        else:
            label = " " * 12 + " |"
        lines.append(label + "".join(row))
    axis = " " * 12 + " +" + "-" * width
    lines.append(axis)
    lines.append(" " * 14 + "0週" + " " * (width - 8) + "52週")
    return "\n".join(lines)


def build_report(results: list[ScenarioResult], trials: int, seed: int) -> str:
    """シミュレーション結果から Markdown レポート本文を生成する。"""
    res = {r.scenario.name: r for r in results}
    a = res.get("A")
    b = res.get("B")
    c = res.get("C")

    lines: list[str] = []
    add = lines.append

    add("# UMALOGI 未来資産曲線シミュレーション — 最大収益ポテンシャル・レポート")
    add("")
    add("> 自動生成: `scripts/simulate_max_revenue.py`")
    add(
        f"> モンテカルロ試行回数: **{trials:,} 回** / シード: `{seed}` / "
        f"運用期間: 52週・約2,000レース"
    )
    add("")

    # ----- 更新履歴（CLAUDE.md 最重要ルール） -----
    add("## 更新履歴")
    add("")
    add("| 日付 | 変更内容 |")
    add("|------|----------|")
    add(
        "| 2026-06-01 | 初版自動生成。未来資産モンテカルロ"
        "（A手堅い/B攻め/C確定実績）の3本曲線を算出。影響ファイル: "
        "scripts/simulate_max_revenue.py |"
    )
    add("")

    # ----- 前提の健全性（最重要・冒頭明示） -----
    add("## ⚠️ 前提の健全性 — 必ず最初に読むこと")
    add("")
    add("本レポートの数値は **2系統の ROI 前提** に基づく。混同は意思決定を誤らせる。")
    add("")
    add("| 系統 | 出所 | ROI | 位置づけ |")
    add("|------|------|-----|----------|")
    add(
        "| バックテスト | 単複 out-of-sample / Pure_EV_Edge 2年BT(400R) | "
        "**222〜270%** | 理論上の最大ポテンシャル（シナリオ A・B） |"
    )
    add(
        "| 確定実績 | 実弾 P&L（会計是正後の真ROI） | **約 80%（＝負け）** | "
        "現実の足元（シナリオ C） |"
    )
    add("")
    add(
        "- シナリオ **A・B は「バックテスト前提」**。実弾でこのROIが再現される保証はない。"
    )
    add("- シナリオ **C が現状の実力**。会計是正後の真ROIは約80%で、現時点では損失。")
    add(
        "- 卍較正器（ECE=0.0177）は確率較正の精度は高いが、**W-048により実弾は停止中**。"
    )
    add(
        "- 結論として、A・Bの爆益は「武器が想定通り機能した場合の上限」であり、"
        "**まず C を A・B 水準へ引き上げる改善が先決**である。"
    )
    add("")

    # ----- サマリーテーブル -----
    add("## 1. エグゼクティブ・サマリー（1年後の到達資産）")
    add("")
    add(
        "| シナリオ | 前提 | 中央値(P50) | 上位10%(P90)爆発値 | 下位10%(P10)最悪値 | "
        "中央値の最大DD | SNS累積純利 |"
    )
    add(
        "|----------|------|------------|------------------|------------------|"
        "--------------|------------|"
    )
    for r in results:
        sc = r.scenario
        add(
            f"| **{sc.name}** {sc.label.split('（')[0]} | "
            f"{'BT' if sc.basis == 'backtest' else '実績'} | "
            f"{_fmt_yen(r.final_median)} | {_fmt_yen(r.final_p90)} | "
            f"{_fmt_yen(r.final_p10)} | {r.max_drawdown_median * 100:.1f}% | "
            f"{_fmt_yen(r.sns_cumulative)} |"
        )
    add("")
    add("> 初期原資はいずれも 100 万円。P10 が初期割れ＝下振れ時は損失を意味する。")
    add("")

    # ----- A vs B 比較チャート -----
    if a and b:
        add("## 2. シナリオ A vs B — 資産推移（中央値曲線の比較）")
        add("")
        add("```")
        add(
            _ascii_line_chart(
                [("B", b.median_curve), ("A", a.median_curve)],
            )
        )
        add("")
        add("凡例:  A = 手堅い現実路線(1/10 Kelly)   B = 攻め路線(1/5 Kelly)")
        add("```")
        add("")

    # ----- 各シナリオ詳細 -----
    add("## 3. シナリオ別詳細")
    add("")
    for r in results:
        sc = r.scenario
        add(f"### シナリオ {sc.name}: {sc.label}")
        add("")
        add(
            f"- 前提: Kelly={sc.kelly_fraction:.2f} / 上限投資="
            f"{_fmt_yen(sc.bet_cap)}{'（流動性スケール有）' if sc.cap_scales else ''}"
            f" / 期待グロスROI={sc.target_roi * 100:.0f}% / 出所="
            f"{'バックテスト' if sc.basis == 'backtest' else '確定実績'}"
        )
        add(
            f"- 1年後 中央値: **{_fmt_yen(r.final_median)}**"
            f"（初期比 {r.final_median / sc.initial_bankroll:.2f}倍）"
        )
        add(f"- 上位10%（爆発シナリオ）: {_fmt_yen(r.final_p90)}")
        add(f"- 下位10%（最悪シナリオ）: {_fmt_yen(r.final_p10)}")
        add(f"- 中央値曲線の最大ドローダウン: {r.max_drawdown_median * 100:.1f}%")
        add(f"- 初期割れ確率（最終資産 < 100万円）: {r.ruin_probability * 100:.1f}%")
        add(f"- SNS/Note 累積純利: {_fmt_yen(r.sns_cumulative)}")
        add("")
        add("中央値資産スパークライン（0→52週）:")
        add("")
        add("```")
        add(_sparkline(r.median_curve))
        add("```")
        add("")

    # ----- バンド（P10-P50-P90）テーブル: シナリオA -----
    if a:
        add("## 4. シナリオA 月次バンド（不確実性レンジ）")
        add("")
        add("| 月 | 下位10% | 中央値 | 上位10% |")
        add("|----|---------|--------|---------|")
        for m in range(1, 13):
            wk = min(WEEKS_PER_YEAR, round(m * WEEKS_PER_YEAR / 12))
            add(
                f"| {m}ヶ月 | {_fmt_yen(a.p10_curve[wk])} | "
                f"{_fmt_yen(a.median_curve[wk])} | {_fmt_yen(a.p90_curve[wk])} |"
            )
        add("")

    # ----- 解釈と提言 -----
    add("## 5. 解釈と提言（オーナー Naofumi 向け）")
    add("")
    if c:
        add(
            f"1. **現実の足元（C）**: 確定実績ベースでは1年後中央値 "
            f"{_fmt_yen(c.final_median)}・初期割れ確率 {c.ruin_probability * 100:.0f}%。"
            f"**今のまま実弾を増やすと損失が拡大する**。"
        )
    if a:
        add(
            f"2. **武器が機能すれば（A）**: 1/10 Kelly + 上限5万 + Noteサブスクで "
            f"中央値 {_fmt_yen(a.final_median)}。ただしこれは "
            f"**バックテストROIが実弾で再現された場合**の姿。"
        )
    if b:
        add(
            f"3. **攻めの上限（B）**: 1/5 Kelly + 上限15万で中央値 "
            f"{_fmt_yen(b.final_median)}、最大DD {b.max_drawdown_median * 100:.0f}%。"
            f"リターンは大きいが下振れ・DDも深い。"
        )
    add(
        "4. **最優先アクション**: 「爆益の皮算用」より先に、確定実績ROI(80%)を"
        "バックテスト水準へ近づける検証（卍W-048の解消・会計是正の本番反映・"
        "out-of-sample の実弾追試）が先決。本シミュレータはその"
        "ギャップを定量化する物差しとして使う。"
    )
    add("")
    add("---")
    add("")
    add(
        "*本レポートはモンテカルロ推計であり、将来の利益を保証するものではない。"
        "競馬は不確定要素が大きく、バックテストの優位性が実弾で必ず再現される保証はない。*"
    )
    add("")

    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# エントリポイント
# --------------------------------------------------------------------------- #


def run_all(
    trials: int = DEFAULT_TRIALS, seed: int = DEFAULT_SEED
) -> list[ScenarioResult]:
    """全シナリオを実行して結果リストを返す。"""
    return [run_scenario(sc, trials, seed) for sc in build_scenarios()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="UMALOGI 未来資産曲線シミュレータ")
    parser.add_argument(
        "--trials",
        type=int,
        default=DEFAULT_TRIALS,
        help="モンテカルロ試行回数（デフォルト 1000）",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="乱数シード")
    parser.add_argument(
        "--report",
        type=str,
        default="docs/REVENUE_POTENTIAL_REPORT.md",
        help="出力レポートパス",
    )
    parser.add_argument("--json", type=str, default="", help="結果JSONの出力先（任意）")
    args = parser.parse_args(argv)

    print(f"[simulate_max_revenue] trials={args.trials} seed={args.seed} 実行中...")
    results = run_all(args.trials, args.seed)

    report = build_report(results, args.trials, args.seed)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")
    print(f"[simulate_max_revenue] レポート出力: {report_path}")

    for r in results:
        print(
            f"  シナリオ {r.scenario.name}: "
            f"中央値={_fmt_yen(r.final_median)} "
            f"P90={_fmt_yen(r.final_p90)} P10={_fmt_yen(r.final_p10)} "
            f"初期割れ={r.ruin_probability * 100:.1f}%"
        )

    if args.json:
        payload = {
            r.scenario.name: {
                "label": r.scenario.label,
                "basis": r.scenario.basis,
                "final_median": r.final_median,
                "final_p90": r.final_p90,
                "final_p10": r.final_p10,
                "max_drawdown_median": r.max_drawdown_median,
                "sns_cumulative": r.sns_cumulative,
                "ruin_probability": r.ruin_probability,
            }
            for r in results
        }
        Path(args.json).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"[simulate_max_revenue] JSON出力: {args.json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
