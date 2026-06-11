"""
src/marketing/premium_pack.py — サブスク向けプレミアムデータ＋SNSチラ見せの自動生成

年間600万円（月額5,000円×100人）のサブスクリプション収益の中核となる
「購読者だけが受け取れる高付加価値データ」を週末の開催日に完全自動で生成する:

  1. プレミアム買い目リスト（premium_sanren.md）
     — 三連単・三連複の EV 1.30 超え候補を割引 Harville（all_ticket_optimizer）で抽出し、
       軸-相手フォーメーション＋bankroll_manager による推奨ベット額付きで提供。
  2. SNS チラ見せポスト（sns_teaser.md）
     — Leak Story（リークを自ら監査・棄却した誠実なAI）の文脈で実績を訴求し、
       「本日 EV1.30 超の歪みを N レースで検知（最大 EV x.xx）」とだけ見せて
       買い目はプレミアム側へ誘導する。

誠実性の設計（feedback_no_unvalidated_overlays / Leak Story の継承）:
  - 勝率は本番予想（predictions.model_score）→ blend_with_market 較正のみを使い、
    未検証の手動係数は一切掛けない。
  - 三連系の組み合わせオッズは実オッズ未保存のため市場確率（Shin）からの推定であり、
    その旨を成果物に必ず明記する。
  - 三連系は実弾ポリシー外（観賞・購読コンテンツ）。実弾解禁は OOS ゲートが前提。

Usage:
    py -m src.marketing.premium_pack --date 20260614
    py -m src.marketing.premium_pack            # 既定: 今日
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import date as dt_date
from pathlib import Path

import numpy as np

from src.marketing.sns_generator import (
    fetch_hit_summary,
    generate_daily_pack,
    normalize_date8,
    pick_killer_phrase,
)
from src.ml.all_ticket_optimizer import (
    Formation,
    OptimizerConfig,
    TicketCandidate,
    build_formation,
    scan_all_tickets,
)
from src.ml.bankroll_manager import BankrollConfig, BetCandidate, allocate_stakes
from src.ml.ev_features import MarketProbabilityCalc
from src.ml.legacy_ensemble import (
    MANJI_ENSEMBLE_BET_TYPES,
    MANJI_ENSEMBLE_WEIGHT,
    ManjiScoreSource,
    ensemble_win_probs,
)
from src.ml.market_blend_calibration import blend_with_market
from src.utils.text import ensure_clean, is_garbled

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_DB_PATH = _ROOT / "data" / "umalogi.db"
_OUT_DIR = _ROOT / "outputs" / "marketing"

# プレミアム抽出条件（コードロック・変更時は OOS 再検証必須）
PREMIUM_EV_MIN: float = 1.30
PREMIUM_BET_TYPES: tuple[str, ...] = ("三連複", "三連単")
_MAX_SCAN_BETS: int = 12  # 全券種スキャン上限（三連系フィルタ前）
_MIN_PROB_COVERAGE: float = 0.8  # 出走頭数に対する勝率マッチ率の下限
_REFERENCE_BANKROLL: float = 100_000.0  # 推奨ベット額の参考バンクロール

# Leak Story — 誠実性の物語（SNS チラ見せの冒頭文脈）
LEAK_STORY_LINES: tuple[str, ...] = (
    "私たちのAIは一度、バックテストで回収率1173%という数字を叩き出しました。",
    "しかし自らリーク監査を行い、その数字が「未来情報の混入」だと突き止めて公開前に棄却。",
    "カンニングなしの厳密な検証で残ったのが、三連系 回収率110%（400レース）という現実の数字です。",
    "盛った数字ではなく、検証に生き残った数字だけをお届けします。",
)


@dataclass
class RacePremium:
    """1 レース分のプレミアム抽出結果。"""

    race_id: str
    venue: str
    race_number: int
    race_name: str
    candidates: list[TicketCandidate] = field(default_factory=list)
    formations: list[Formation] = field(default_factory=list)

    @property
    def max_ev(self) -> float:
        return max((c.ev for c in self.candidates), default=0.0)


@dataclass
class PremiumPack:
    """1 日分のプレミアム生成結果。"""

    date: str
    races: list[RacePremium] = field(default_factory=list)
    premium_text: str = ""
    premium_html: str = ""
    teaser_text: str = ""
    files: list[Path] = field(default_factory=list)


# ── DB 抽出 ──────────────────────────────────────────────────────────────
def _fetch_race_inputs(
    conn: sqlite3.Connection, race_id: str
) -> tuple[list[int], list[float], list[float]] | None:
    """レースの (馬番, 較正前勝率, 単勝オッズ) を取得する。

    勝率 = 最新の本命系予想（is_superseded=0）の model_score を馬名で entries と突合。
    オッズ = realtime_odds の馬番ごと最新値。
    マッチ率が _MIN_PROB_COVERAGE 未満・オッズ欠損・頭数 6 未満は None（対象外）。
    """
    pred = conn.execute(
        """
        SELECT p.id FROM predictions p
        WHERE p.race_id = ?
          AND p.model_type LIKE '本命%'
          AND COALESCE(p.is_superseded, 0) = 0
        ORDER BY p.created_at DESC LIMIT 1
        """,
        (race_id,),
    ).fetchone()
    if pred is None:
        return None
    prob_by_name: dict[str, float] = {}
    for name, score in conn.execute(
        "SELECT horse_name, model_score FROM prediction_horses "
        "WHERE prediction_id = ? AND model_score IS NOT NULL",
        (pred[0],),
    ).fetchall():
        clean = ensure_clean(str(name or ""))
        if clean and not is_garbled(clean):
            prob_by_name[clean] = float(score)
    if not prob_by_name:
        return None

    odds_by_num: dict[int, float] = {}
    for hn, wo in conn.execute(
        "SELECT horse_number, win_odds FROM realtime_odds "
        "WHERE id IN (SELECT MAX(id) FROM realtime_odds WHERE race_id = ? "
        "             GROUP BY horse_number)",
        (race_id,),
    ).fetchall():
        if hn is not None and wo is not None and float(wo) > 1.0:
            odds_by_num[int(hn)] = float(wo)
    if not odds_by_num:
        return None

    numbers: list[int] = []
    probs: list[float] = []
    odds: list[float] = []
    n_entries = 0
    for hn, name in conn.execute(
        "SELECT horse_number, horse_name FROM entries WHERE race_id = ? "
        "ORDER BY horse_number",
        (race_id,),
    ).fetchall():
        if hn is None:
            continue
        n_entries += 1
        clean = ensure_clean(str(name or ""))
        p = prob_by_name.get(clean)
        o = odds_by_num.get(int(hn))
        if p is None or o is None or p <= 0:
            continue
        numbers.append(int(hn))
        probs.append(p)
        odds.append(o)

    if n_entries < 6 or len(numbers) < 6:
        return None
    if len(numbers) / n_entries < _MIN_PROB_COVERAGE:
        logger.info(
            "premium 対象外 race_id=%s: 勝率マッチ率 %.0f%% < %.0f%%",
            race_id,
            100.0 * len(numbers) / n_entries,
            100.0 * _MIN_PROB_COVERAGE,
        )
        return None
    return numbers, probs, odds


def scan_premium_races(conn: sqlite3.Connection, date_str: str) -> list[RacePremium]:
    """指定日の全レースを走査し、三連系 EV 1.30 超えの抽出結果を返す。"""
    races = conn.execute(
        "SELECT race_id, COALESCE(venue,''), COALESCE(race_number,0), "
        "COALESCE(race_name,'') FROM races WHERE date = ? ORDER BY race_id",
        (date_str,),
    ).fetchall()
    calc = MarketProbabilityCalc()
    cfg = OptimizerConfig(ev_min=PREMIUM_EV_MIN, max_bets=_MAX_SCAN_BETS)
    manji_src = ManjiScoreSource(conn)
    results: list[RacePremium] = []
    for race_id, venue, race_number, race_name in races:
        try:
            inputs = _fetch_race_inputs(conn, str(race_id))
            if inputs is None:
                continue
            numbers, raw_probs, odds = inputs
            market = calc.compute_shin_probabilities(np.asarray(odds, dtype=float))

            def _scan(probs: list[float]) -> list[TicketCandidate]:
                blended = [
                    blend_with_market(p, o) for p, o in zip(probs, odds, strict=True)
                ]
                return [
                    c
                    for c in scan_all_tickets(
                        numbers, blended, market_win_probs=market, config=cfg
                    )
                    if c.bet_type in PREMIUM_BET_TYPES
                ]

            cands = [
                c
                for c in _scan(raw_probs)
                if c.bet_type not in MANJI_ENSEMBLE_BET_TYPES
            ]
            # 過去モデル昇華（legacy_ensemble・OOS検証済み）:
            # 三連複のみ卍EV回帰を w=0.4 で融合した勝率から抽出する。
            # 卍スコア取得に失敗したレースは従来確率にフォールバック（恒等）。
            ens_probs = raw_probs
            manji_map = manji_src.scores_for(str(race_id))
            if manji_map is not None and all(n in manji_map for n in numbers):
                ens_probs = list(
                    ensemble_win_probs(
                        np.asarray(raw_probs, dtype=float),
                        np.asarray([manji_map[n] for n in numbers], dtype=float),
                        np.asarray(odds, dtype=float),
                        weight=MANJI_ENSEMBLE_WEIGHT,
                    )
                )
            cands += [
                c for c in _scan(ens_probs) if c.bet_type in MANJI_ENSEMBLE_BET_TYPES
            ]
            cands.sort(key=lambda c: c.ev, reverse=True)
            if not cands:
                continue
            results.append(
                RacePremium(
                    race_id=str(race_id),
                    venue=ensure_clean(str(venue)),
                    race_number=int(race_number),
                    race_name=ensure_clean(str(race_name)),
                    candidates=cands,
                    formations=build_formation(cands),
                )
            )
        except Exception as exc:  # 1 レースの失敗で日次生成全体を殺さない
            logger.warning("premium スキャン失敗 race_id=%s: %s", race_id, exc)
    results.sort(key=lambda r: r.max_ev, reverse=True)
    _emit_cui_signals(results)
    return results


def _emit_cui_signals(results: list[RacePremium]) -> None:
    """高EV検知を CUI コックピット（src/ui/console.py）へ出力する（best-effort）。"""
    if not results:
        return
    try:
        from src.ui.console import get_console

        con = get_console()
        top = results[0]
        best = top.candidates[0]
        con.ev_signal(
            f"{top.venue}{top.race_number}R",
            best.bet_type,
            "-".join(str(n) for n in best.combo),
            ev=best.ev,
            odds=best.odds,
        )
        con.candidates_table(
            f"本日の高EVシグナル（{len(results)}レース）",
            [
                {
                    "label": "-".join(str(n) for n in c.combo),
                    "bet_type": c.bet_type,
                    "prob": c.prob,
                    "odds": c.odds,
                    "ev": c.ev,
                }
                for r in results
                for c in r.candidates[:3]
            ],
        )
    except Exception:  # noqa: BLE001 - 装飾失敗で本番を殺さない
        pass


# ── テキスト生成 ─────────────────────────────────────────────────────────
def _stake_map(candidates: list[TicketCandidate], race_id: str) -> dict[str, int]:
    """bankroll_manager で候補ごとの参考推奨ベット額を算出する。"""
    bets = [
        BetCandidate(
            race_id=race_id,
            bet_type=c.bet_type,
            horse_number=c.combo[0],
            prob=c.prob,
            odds=c.odds,
        )
        for c in candidates
    ]
    result = allocate_stakes(
        bets, bankroll=_REFERENCE_BANKROLL, config=BankrollConfig()
    )
    return {
        f"{a.candidate.bet_type}:{('-'.join(str(n) for n in c.combo))}": a.stake
        for a, c in zip(result.allocations, candidates, strict=True)
    }


def generate_premium_text(races: list[RacePremium], date_str: str) -> str:
    """サブスク購読者向けプレミアム買い目リスト（Markdown）を生成する。"""
    date_jp = _date_jp(date_str)
    lines = [
        f"# 🔑 UMA-Logic プレミアム — 三連系 高EV買い目（{date_jp}）",
        "",
        "> **購読者限定コンテンツ**（無断転載・再配布禁止）",
        ">",
        "> 抽出基準: 本番モデル勝率 × 市場ブレンド較正 → 割引 Harville 着順分布で",
        f"> 三連単・三連複の期待値を全組み合わせ計算し、**EV {PREMIUM_EV_MIN:.2f} 超**のみ掲載。",
        "",
        "## ⚠️ 誠実性に関する注意（必読）",
        "",
        "- 三連系の組み合わせオッズは JRA 実オッズ未取得のため**市場確率からの推定値**です。",
        "  実購入時は直前オッズで EV が変わります（オッズが下がれば EV も下がります）。",
        "- 推奨ベット額は参考バンクロール 10 万円・1/10 ケリー・1点上限 2%・"
        "1 日合計上限 10% の機械算出です。",
        "- 私たちはバックテストのリークを自ら監査・棄却した上で、OOS（カンニングなし）"
        "検証に生き残った戦略のみを配信しています。1 日の結果ではなく長期の期待値で"
        "ご活用ください。",
        "",
    ]
    if not races:
        lines += [
            "## 本日の抽出結果",
            "",
            f"本日は EV {PREMIUM_EV_MIN:.2f} 超の三連系候補が**ありませんでした**。",
            "基準を満たさない日は無理に買い目を作りません。それが長期回収率を守る規律です。",
        ]
        return "\n".join(lines)

    total_cands = sum(len(r.candidates) for r in races)
    max_ev_all = max((r.max_ev for r in races), default=0.0)
    lines += [
        f"## 📊 本日のサマリ: {len(races)} レース / {total_cands} 点 / 最大EV {max_ev_all:.2f}",
        "",
        "---",
        "",
    ]
    for i, r in enumerate(races, start=1):
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, "◆")
        label = f"{r.venue}{r.race_number}R" + (
            f"（{r.race_name}）" if r.race_name else ""
        )
        fire = " 🔥" if r.max_ev >= 1.5 else ""
        lines += [f"### {medal} {label} — 最大EV {r.max_ev:.2f}{fire}", ""]
        stakes = _stake_map(r.candidates, r.race_id)
        for f in r.formations:
            axis = "・".join(str(n) for n in f.axis) or "なし"
            partners = "・".join(str(n) for n in f.partners) or "—"
            lines += [
                f"**{f.bet_type} フォーメーション** "
                f"（軸: {axis} / 相手: {partners} / {f.n_points}点）",
                "",
                "| 買い目 | モデル確率 | 推定オッズ | EV | 参考ベット額 |",
                "|---|---|---|---|---|",
            ]
            for c in f.candidates:
                key = f"{c.bet_type}:{('-'.join(str(n) for n in c.combo))}"
                stake = stakes.get(key, 0)
                stake_disp = f"¥{stake:,}" if stake > 0 else "見送り可"
                lines.append(
                    f"| {c.label} | {c.prob * 100:.2f}% | {c.odds:,.1f} | "
                    f"**{c.ev:.2f}** | {stake_disp} |"
                )
            lines.append("")
    lines += [
        "---",
        "",
        f"💡 {pick_killer_phrase(date_str, offset=3)}",
    ]
    return "\n".join(lines)


# ── HTML レポート（Tailwind ラグジュアリー版）────────────────────────────
_HTML_HEAD = """<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>UMA-Logic PRIVATE — 三連系プレミアムレポート</title>
<script src="https://cdn.tailwindcss.com"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Shippori+Mincho:wght@500;700;800&family=Cormorant+Garamond:wght@500;600;700&family=Zen+Kaku+Gothic+New:wght@400;500;700&display=swap" rel="stylesheet">
<style>
  :root {
    --gold: #C9A227; --gold-bright: #E8C766; --ink: #0B0A08; --paper: #14120E;
  }
  body {
    font-family: 'Zen Kaku Gothic New', sans-serif;
    background:
      radial-gradient(1100px 500px at 85% -10%, rgba(201,162,39,.14), transparent 60%),
      radial-gradient(800px 420px at -10% 30%, rgba(201,162,39,.06), transparent 55%),
      var(--ink);
  }
  .serif { font-family: 'Shippori Mincho', serif; }
  .num { font-family: 'Cormorant Garamond', 'Shippori Mincho', serif; }
  .gold-text {
    background: linear-gradient(120deg, #8C6D1F 0%, #E8C766 38%, #FFF3C4 50%, #E8C766 62%, #8C6D1F 100%);
    -webkit-background-clip: text; background-clip: text; color: transparent;
  }
  .hairline { background: linear-gradient(90deg, transparent, var(--gold), transparent); height: 1px; }
  .card {
    background: linear-gradient(180deg, rgba(255,255,255,.035), rgba(255,255,255,.012));
    border: 1px solid rgba(201,162,39,.22);
    box-shadow: 0 24px 60px -32px rgba(0,0,0,.9), inset 0 1px 0 rgba(255,255,255,.05);
  }
  .grain::after {
    content: ""; position: fixed; inset: 0; pointer-events: none; opacity: .05; z-index: 50;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='160' height='160'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='2'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.6'/%3E%3C/svg%3E");
  }
  @keyframes rise { from { opacity: 0; transform: translateY(14px); } to { opacity: 1; transform: none; } }
  .rise { animation: rise .7s cubic-bezier(.22,.61,.36,1) both; }
</style>
</head>
<body class="grain text-stone-200 antialiased">
"""


def _esc(s: str) -> str:
    """HTML エスケープ（DB 由来文字列のインジェクション防止）。"""
    import html as _html

    return _html.escape(str(s), quote=True)


def _html_race_card(r: RacePremium, rank: int) -> str:
    """1 レース分のラグジュアリーカード HTML を生成する。"""
    label = f"{_esc(r.venue)}{r.race_number}R"
    name = _esc(r.race_name) if r.race_name else ""
    stakes = _stake_map(r.candidates, r.race_id)
    blocks: list[str] = []
    for f in r.formations:
        axis = "・".join(str(n) for n in f.axis) or "なし"
        partners = "・".join(str(n) for n in f.partners) or "—"
        rows: list[str] = []
        for c in f.candidates:
            key = f"{c.bet_type}:{('-'.join(str(n) for n in c.combo))}"
            stake = stakes.get(key, 0)
            stake_disp = f"&yen;{stake:,}" if stake > 0 else "見送り可"
            ev_cls = "text-amber-300" if c.ev >= 1.5 else "text-stone-100"
            rows.append(
                f'<tr class="border-t border-stone-800/80">'
                f'<td class="py-2.5 pr-4 num text-lg tracking-wide text-stone-100">{_esc(c.label)}</td>'
                f'<td class="py-2.5 pr-4 text-right num text-stone-300">{c.prob * 100:.2f}%</td>'
                f'<td class="py-2.5 pr-4 text-right num text-stone-300">{c.odds:,.1f}</td>'
                f'<td class="py-2.5 pr-4 text-right num text-xl font-semibold {ev_cls}">{c.ev:.2f}</td>'
                f'<td class="py-2.5 text-right num text-stone-200">{stake_disp}</td>'
                f"</tr>"
            )
        blocks.append(
            f"""
      <div class="mt-6">
        <div class="flex flex-wrap items-baseline gap-x-4 gap-y-1">
          <span class="serif text-lg text-amber-200/90">{_esc(f.bet_type)} フォーメーション</span>
          <span class="text-sm text-stone-400">軸 <span class="num text-stone-100 text-base">{_esc(axis)}</span>
            ／ 相手 <span class="num text-stone-100 text-base">{_esc(partners)}</span>
            ／ {f.n_points}点</span>
        </div>
        <table class="mt-3 w-full text-sm">
          <thead><tr class="text-[11px] tracking-[0.18em] text-stone-500 uppercase">
            <th class="pb-2 text-left font-medium">買い目</th>
            <th class="pb-2 text-right font-medium">モデル確率</th>
            <th class="pb-2 text-right font-medium">推定オッズ</th>
            <th class="pb-2 text-right font-medium">EV</th>
            <th class="pb-2 text-right font-medium">参考ベット額</th>
          </tr></thead>
          <tbody>{"".join(rows)}</tbody>
        </table>
      </div>"""
        )
    return f"""
    <section class="card rounded-2xl px-7 py-6 md:px-9 md:py-8 rise" style="animation-delay:{0.12 * rank:.2f}s">
      <div class="flex items-start justify-between gap-4">
        <div>
          <div class="flex items-baseline gap-3">
            <span class="num text-3xl md:text-4xl gold-text font-semibold">{label}</span>
            {f'<span class="serif text-lg text-stone-300">{name}</span>' if name else ""}
          </div>
        </div>
        <div class="text-right shrink-0">
          <div class="text-[10px] tracking-[0.3em] text-stone-500">MAX EV</div>
          <div class="num text-3xl font-semibold text-amber-300">{r.max_ev:.2f}</div>
        </div>
      </div>
      <div class="hairline mt-5 opacity-60"></div>
      {"".join(blocks)}
    </section>"""


def generate_premium_html(races: list[RacePremium], date_str: str) -> str:
    """購読者向けプレミアムレポートの Tailwind ラグジュアリー HTML を生成する。

    Markdown 版（generate_premium_text）と同一データ・同一の誠実性注意を
    「プライベートバンクの調査レポート」級のダークラグジュアリーで装飾する。
    自己完結ドキュメント（Tailwind CDN + Google Fonts）。

    Args:
        races: scan_premium_races の抽出結果（EV 降順）。
        date_str: 対象日（YYYY-MM-DD）。

    Returns:
        完全な HTML ドキュメント文字列。
    """
    date_jp = _date_jp(date_str)
    total_cands = sum(len(r.candidates) for r in races)
    max_ev = max((r.max_ev for r in races), default=0.0)

    if races:
        body_main = "\n".join(
            _html_race_card(r, i) for i, r in enumerate(races, start=1)
        )
        stats = f"""
    <div class="grid grid-cols-3 gap-px bg-stone-800/60 rounded-xl overflow-hidden card rise" style="animation-delay:.1s">
      <div class="bg-stone-950/70 px-5 py-4 text-center">
        <div class="text-[10px] tracking-[0.3em] text-stone-500">RACES</div>
        <div class="num text-3xl gold-text font-semibold">{len(races)}</div>
      </div>
      <div class="bg-stone-950/70 px-5 py-4 text-center">
        <div class="text-[10px] tracking-[0.3em] text-stone-500">SIGNALS</div>
        <div class="num text-3xl gold-text font-semibold">{total_cands}</div>
      </div>
      <div class="bg-stone-950/70 px-5 py-4 text-center">
        <div class="text-[10px] tracking-[0.3em] text-stone-500">MAX EV</div>
        <div class="num text-3xl text-amber-300 font-semibold">{max_ev:.2f}</div>
      </div>
    </div>"""
    else:
        body_main = """
    <section class="card rounded-2xl px-8 py-12 text-center rise">
      <div class="serif text-2xl text-stone-200">本日は基準を満たす買い目がありません</div>
      <p class="mt-4 text-stone-400 leading-relaxed">基準を満たさない日は無理に買い目を作りません。<br>
      それが長期回収率を守る<span class="text-amber-200">規律</span>です。</p>
    </section>"""
        stats = ""

    return f"""{_HTML_HEAD}
  <main class="mx-auto max-w-3xl px-5 py-12 md:py-16 space-y-8">

    <header class="text-center rise">
      <div class="text-[11px] tracking-[0.5em] text-amber-200/80">UMA-LOGIC&nbsp;PRIVATE</div>
      <h1 class="serif mt-4 text-4xl md:text-5xl font-bold gold-text leading-tight">三連系プレミアムレポート</h1>
      <div class="mt-3 num text-lg text-stone-400">{_esc(date_jp)}　<span class="text-stone-600">|</span>　EV {PREMIUM_EV_MIN:.2f} 超のみ掲載</div>
      <div class="mt-6 flex items-center gap-3">
        <div class="hairline flex-1"></div><div class="text-amber-300/80 text-xs">◆</div><div class="hairline flex-1"></div>
      </div>
      <p class="mt-3 text-xs tracking-[0.25em] text-stone-500">購読者限定 ・ 無断転載および再配布を禁じます</p>
    </header>

    {stats}

    <aside class="card rounded-2xl px-7 py-6 border-l-2 !border-l-amber-400/70 rise" style="animation-delay:.05s">
      <div class="serif text-amber-200/90">誠実性に関する注意 — 必読</div>
      <ul class="mt-3 space-y-2 text-sm text-stone-400 leading-relaxed list-disc list-inside">
        <li>三連系の組み合わせオッズは JRA 実オッズ未取得のため<span class="text-stone-200">市場確率からの推定値</span>です。実購入時は直前オッズで EV が変動します。</li>
        <li>参考ベット額はバンクロール 10 万円・1/10 ケリー・1点上限 2%・日次上限 10% の機械算出です。</li>
        <li>リークを自ら監査・棄却した OOS 検証に生き残った戦略のみを配信しています。1 日の結果ではなく長期の期待値でご活用ください。</li>
      </ul>
    </aside>

{body_main}

    <footer class="pt-4 text-center rise">
      <div class="hairline opacity-50"></div>
      <p class="mt-6 serif text-stone-400">{_esc(pick_killer_phrase(date_str, offset=3))}</p>
      <p class="mt-4 text-[10px] tracking-[0.35em] text-stone-600">UMA-LOGIC — HONEST&nbsp;AI&nbsp;RACING&nbsp;INTELLIGENCE</p>
    </footer>

  </main>
</body>
</html>"""


def generate_teaser_text(
    races: list[RacePremium], date_str: str, conn: sqlite3.Connection, prev_date: str
) -> str:
    """SNS 集客用チラ見せポスト（Leak Story 文脈・買い目は非公開）を生成する。"""
    date_jp = _date_jp(date_str)
    lines = [
        f"🧠 誠実なAIの競馬予想 — {date_jp}",
        "",
        *LEAK_STORY_LINES,
        "",
    ]
    summary = fetch_hit_summary(conn, prev_date)
    if summary.n_bets > 0:
        if summary.n_hits > 0:
            lines += [
                f"📊 前回実績: {summary.n_bets}点中 {summary.n_hits}点的中 / "
                f"回収率 {summary.roi:.0f}%",
                "",
            ]
        else:
            lines += [
                f"📊 前回実績: {summary.n_bets}点 的中なし（負けも隠さず公開します）",
                "",
            ]
    if races:
        top = races[0]
        label = f"{top.venue}{top.race_number}R"
        n_cands = sum(len(r.candidates) for r in races)
        bet_types = sorted({c.bet_type for r in races for c in r.candidates})
        lines += [
            f"🔥 本日、AIは {len(races)} レースで市場の歪み（EV {PREMIUM_EV_MIN:.2f} 超）を検知。",
            f"最大は {label} の {'/'.join(bet_types)} で **EV {top.max_ev:.2f}**。",
            f"高EV候補は合計 {n_cands} 点。",
            "",
            "🔑 具体的な買い目フォーメーションと推奨ベット額は購読者限定で配信中。",
            "プロフィールのリンクからどうぞ。",
        ]
    else:
        lines += [
            f"🛡 本日は EV {PREMIUM_EV_MIN:.2f} 超の三連系候補なし。",
            "「買わない」と言えるのも、期待値で運用するAIの強みです。",
        ]
    lines += [
        "",
        f"💡 {pick_killer_phrase(date_str, offset=4)}",
        "",
        "#競馬予想 #AI競馬 #期待値 #UMALOGI",
    ]
    return "\n".join(lines)


def _date_jp(date_str: str) -> str:
    s = date_str.replace("-", "")
    return f"{int(s[4:6])}月{int(s[6:8])}日"


# ── オーケストレーション ─────────────────────────────────────────────────
def generate_premium_pack(
    conn: sqlite3.Connection,
    target_date: str,
    prev_date: str,
    out_dir: Path | None = None,
) -> PremiumPack:
    """プレミアム買い目＋SNSチラ見せを生成しファイル出力する。

    Args:
        conn: DB 接続。
        target_date: 対象日（YYYY-MM-DD）。
        prev_date: 実績集計日（YYYY-MM-DD）。
        out_dir: 出力先（省略時 outputs/marketing/YYYYMMDD/）。
    """
    pack = PremiumPack(date=target_date)
    pack.races = scan_premium_races(conn, target_date)
    pack.premium_text = generate_premium_text(pack.races, target_date)
    pack.premium_html = generate_premium_html(pack.races, target_date)
    pack.teaser_text = generate_teaser_text(pack.races, target_date, conn, prev_date)

    dest = out_dir or (_OUT_DIR / target_date.replace("-", ""))
    dest.mkdir(parents=True, exist_ok=True)
    for name, content in (
        ("premium_sanren.md", pack.premium_text),
        ("premium_sanren.html", pack.premium_html),
        ("sns_teaser.md", pack.teaser_text),
    ):
        path = dest / name
        path.write_text(content + "\n", encoding="utf-8")
        pack.files.append(path)
    return pack


def generate_marketing_assets(
    target_date8: str, db_path: Path | None = None
) -> list[Path]:
    """週末自動運用のエントリポイント — 1 日分の全マーケティング資産を生成する。

    today_auto_runner から best-effort で呼ばれる（例外は呼び出し側で握る）。
    生成物: 無料予想/的中報告/動画台本（sns_generator）＋プレミアム買い目/チラ見せ。

    Args:
        target_date8: 対象日 "YYYYMMDD"。
        db_path: DB パス（テスト用に差し替え可）。

    Returns:
        生成したファイルパスのリスト。
    """
    target_d = normalize_date8(target_date8)
    if target_d is None:
        logger.warning("generate_marketing_assets: 不正な日付 %r", target_date8)
        return []
    target = target_d.isoformat()
    prev = dt_date.fromordinal(target_d.toordinal() - 1).isoformat()

    conn = sqlite3.connect(str(db_path or _DB_PATH))
    try:
        daily = generate_daily_pack(conn, target, prev)
        premium = generate_premium_pack(conn, target, prev)
    finally:
        conn.close()
    files = daily.files + premium.files
    logger.info(
        "マーケティング資産生成: %d ファイル（premium対象 %d レース）",
        len(files),
        len(premium.races),
    )
    return files


def main() -> int:
    """CLI: 指定日（既定=今日）のプレミアム＋チラ見せを生成する。"""
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    parser = argparse.ArgumentParser(description="サブスク向けプレミアムデータ自動生成")
    parser.add_argument("--date", default=None, help="対象日 YYYYMMDD（既定: 今日）")
    parser.add_argument("--db", default=str(_DB_PATH), help="DB パス")
    args = parser.parse_args()

    raw = args.date or dt_date.today().strftime("%Y%m%d")
    files = generate_marketing_assets(raw, db_path=Path(args.db))
    print(f"生成完了: {len(files)} ファイル")
    for f in files:
        print(f"  - {f}")
    return 0 if files else 2


if __name__ == "__main__":
    sys.exit(main())
