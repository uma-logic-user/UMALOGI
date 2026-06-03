"""
src/ops/sns_publisher.py — SNS 集客・マーケティング自動化エンジン

隔離された集客用モデル（Oracle / HitFocus = bet_policy.ORNAMENTAL_MODELS）の買い目・実績を
外部マネタイズ導線（X / note / Discord 集客チャンネル）向けに自動整形・配信する。

提供機能:
  - is_ornamental_model()        : 集客(観賞)用モデル判定（bet_policy 準拠）
  - format_x_post()              : X 文字数最適化のコピペ用予想（通常≤140 / premium長文）
  - HitFlash / generate_hit_flash / send_hit_flash : 的中ドヤ報告（高ROI/万馬券のみ発火・DI送信）
  - ModelWeeklyStat / export_weekly_report : note 貼付用 週次 Markdown
  - DB 連携グルー: detect_and_flash() / collect_weekly_stats() / run_weekly_report()

設計方針:
  - テキスト生成は純関数・dataclass 派生プロパティで完結（ネットワークは sender 注入で排除）。
  - 会計は bet_policy.flat_cost(¥100×点数) と同一の stake 基準。
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import date as _date
from pathlib import Path
from typing import Callable

from src.ml.bet_policy import ORNAMENTAL_MODELS, base_model

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_SNS_DIR = _ROOT / "outputs" / "sns"
_REPORTS_DIR = _ROOT / "outputs" / "sns" / "reports"

X_CHAR_LIMIT: int = 140
X_PREMIUM_LIMIT: int = 2000
_HASHTAGS_X: str = "#競馬予想 #UMALOGI #AI予想 #JRA"
_NOTE_URL: str = os.environ.get("NOTE_MYPAGE_URL", "https://note.com/umalogi")

# 的中速報の発火条件
HIT_FLASH_MIN_ROI: float = 150.0  # 回収率がこの%以上で速報
MANBAKEN_ROI: float = 10_000.0  # 100円あたり払戻1万円(=ROI 10000%)で万馬券

Sender = Callable[[str, str], bool]


# ─────────────────────────────────────────────────────────────────────
# モデル判定
# ─────────────────────────────────────────────────────────────────────
def is_ornamental_model(model_type: str) -> bool:
    """集客(観賞)用モデル（Oracle / HitFocus）か判定する（サフィックス許容）。"""
    return base_model(model_type) in ORNAMENTAL_MODELS


# ─────────────────────────────────────────────────────────────────────
# X 投稿（ShowcasePick → コピペ用テキスト）
# ─────────────────────────────────────────────────────────────────────
@dataclass
class ShowcaseHorse:
    horse_number: int
    horse_name: str
    odds: float
    win_prob: float
    ev: float


@dataclass
class ShowcasePick:
    race_id: str
    race_name: str
    venue: str
    model_name: str
    honmei: ShowcaseHorse
    hot_horses: list[ShowcaseHorse] = field(default_factory=list)


def format_x_post(pick: ShowcasePick, premium: bool = False) -> str:
    """集客モデルの買い目を X 投稿テキストへ整形する。

    通常モード(premium=False): 140 文字以内・絵文字+ハッシュタグ+本命のみ。
    プレミアム(premium=True): 激熱馬(EV妙味)まで展開した長文（≤2000）。
    """
    h = pick.honmei
    honmei_line = (
        f"\n◎本命 {h.horse_number}番 {h.horse_name}（{h.odds:.1f}倍/EV{h.ev:.2f}）"
    )

    if not premium:
        tags = "\n" + _HASHTAGS_X
        head = f"🏇【UMALOGI {pick.model_name}】🎯{pick.venue} {pick.race_name}"
        text = head + honmei_line + tags
        if len(text) > X_CHAR_LIMIT:
            head = f"🏇【UMALOGI {pick.model_name}】🎯{pick.venue}"
            text = head + honmei_line + tags
        if len(text) > X_CHAR_LIMIT:
            text = text[: X_CHAR_LIMIT - 1] + "…"
        return text

    # premium 長文
    lines = [
        f"🏇🔥【UMALOGI {pick.model_name} 厳選予想】🎯",
        f"{pick.venue} {pick.race_name}",
        honmei_line.lstrip("\n"),
        "",
        "🔥 激熱注目馬（オッズ妙味 / EV順）:",
    ]
    for x in sorted(pick.hot_horses, key=lambda z: z.ev, reverse=True):
        lines.append(
            f"・{x.horse_number}番 {x.horse_name}（{x.odds:.1f}倍 / EV{x.ev:.2f}）"
        )
    lines += ["", f"📊 全モデルの成績・買い目→{_NOTE_URL}", _HASHTAGS_X]
    return "\n".join(lines)[:X_PREMIUM_LIMIT]


# ─────────────────────────────────────────────────────────────────────
# 的中速報（ドヤ報告）
# ─────────────────────────────────────────────────────────────────────
@dataclass
class HitFlash:
    race_name: str
    venue: str
    model_name: str
    bet_type: str
    horse_desc: str
    stake: int
    payout: int

    @property
    def roi(self) -> float:
        """回収率(%) = payout / stake × 100。"""
        return (100.0 * self.payout / self.stake) if self.stake > 0 else 0.0

    @property
    def is_manbaiken(self) -> bool:
        """100円あたり払戻 1万円以上（= ROI 10000% 以上）なら万馬券。"""
        return self.roi >= MANBAKEN_ROI


@dataclass(frozen=True)
class BetResult:
    """レース結果 1 買い目分の実績データ（SNS 報告・日次総括用）。

    実弾投票とは無関係の表示専用データクラス。
    """

    race_name: str  # レース名 / 番号（"日本ダービー" や "11R"）
    venue: str  # 競馬場名（"東京"）
    bet_type: str  # 券種（"馬連"）
    horse_desc: str  # 買い目（"1-3"）
    ev: float  # 期待値（0.0 = 不明）
    stake: int  # 投資額（円）
    payout: int  # 払戻額（円）; 外れは 0
    is_hit: bool  # 的中フラグ
    date: str  # YYYYMMDD

    @property
    def roi(self) -> float:
        """回収率(%) = 100 × 払戻 / 投資。"""
        return (100.0 * self.payout / self.stake) if self.stake > 0 else 0.0

    @property
    def profit(self) -> int:
        """損益 = 払戻 - 投資。"""
        return self.payout - self.stake


def generate_hit_flash(hit: HitFlash) -> str | None:
    """的中ドヤ報告テキストを生成する。ROI 閾値未満かつ万馬券でなければ None。"""
    if hit.roi < HIT_FLASH_MIN_ROI and not hit.is_manbaiken:
        return None
    badge = (
        "💥🎊 **万馬券的中！！** 🎊💥" if hit.is_manbaiken else "🎯🔥 **的中速報** 🔥🎯"
    )
    return (
        f"{badge}\n"
        f"【UMALOGI {hit.model_name}】{hit.venue} {hit.race_name}\n"
        f"▶ {hit.bet_type}「{hit.horse_desc}」が **¥{hit.payout:,} 的中**！（投資¥{hit.stake:,}）\n"
        f"📈 回収率 **{hit.roi:,.0f}%**\n"
        f"{_HASHTAGS_X}\n📊 全成績→{_NOTE_URL}"
    )


def _default_sender(text: str, channel: str) -> bool:
    """既定の送信（Discord 集客チャンネル）。URL 未設定/失敗時 False（例外は出さない）。"""
    url = os.environ.get("DISCORD_WEBHOOK_SNS", "") or os.environ.get(
        "DISCORD_WEBHOOK_NOTE_DRAFT", ""
    )
    if not url:
        return False
    try:
        import requests

        resp = requests.post(url, json={"content": text}, timeout=10)
        return resp.status_code in (200, 204)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[SNS] 送信失敗: %s", exc)
        return False


def send_hit_flash(
    hit: HitFlash, sender: Sender | None = None, *, channel: str = "sns"
) -> bool:
    """的中速報を生成し、生成された場合のみ sender で配信する（閾値未満は無駄打ちしない）。"""
    text = generate_hit_flash(hit)
    if text is None:
        return False
    snd = sender or _default_sender
    return bool(snd(text, channel))


# ─────────────────────────────────────────────────────────────────────
# Note 用「おすすめ掛け金」（EV 連動・読者の資金配分導線）
# ─────────────────────────────────────────────────────────────────────
# (EV 下限, 1点あたり掛け金, ユニット数, 勝負ラベル) — 降順に評価する
_NOTE_BET_TIERS: tuple[tuple[float, int, int, str], ...] = (
    (1.40, 500, 5, "激熱勝負！"),
    (1.20, 300, 3, "中勝負"),
    (0.0, 100, 1, "安心投資"),
)
_NOTE_BET_NOTE = (
    "※上記は1点100円（1ユニット）を基準としたAI推奨比率です。"
    "ご自身のバンクロール（余剰資金）に合わせて、倍率（10倍、100倍など）を調整してください。"
)


def recommended_unit_stake(ev: float) -> tuple[int, int, str]:
    """期待値(EV)から読者向けおすすめ掛け金を算出する。

    Returns: (1点あたり掛け金[円], ユニット数, 勝負ラベル)。
      EV < 1.20         → 100円 / 1u / 安心投資
      1.20 <= EV < 1.40 → 300円 / 3u / 中勝負
      1.40 <= EV        → 500円 / 5u / 激熱勝負！
    """
    for floor, stake, units, label in _NOTE_BET_TIERS:
        if ev >= floor:
            return stake, units, label
    return 100, 1, "安心投資"


@dataclass
class NoteBet:
    """note 記事に載せる 1 買い目（馬券種・対象・期待値）。"""

    bet_type: str
    horse_desc: str
    ev: float


@dataclass
class RecommendedBet:
    """おすすめ掛け金を確定した 1 買い目。"""

    bet_type: str
    horse_desc: str
    ev: float
    stake: int
    units: int
    label: str

    @property
    def comment(self) -> str:
        """掛け金の根拠コメント（期待値＋勝負レベル）。"""
        return f"★期待値{self.ev:.2f}の{self.label}"


@dataclass
class RecommendedBetPlan:
    """1 レース分のおすすめ掛け金プラン。"""

    bets: list[RecommendedBet] = field(default_factory=list)

    @property
    def total_stake(self) -> int:
        """このレースの想定総投資額（1点100円ベース換算）。"""
        return sum(b.stake for b in self.bets)


def calculate_recommended_note_bets(bets: list[NoteBet]) -> RecommendedBetPlan:
    """各買い目に EV 連動のおすすめ掛け金を割り当てたプランを返す。"""
    out: list[RecommendedBet] = []
    for b in bets:
        stake, units, label = recommended_unit_stake(b.ev)
        out.append(
            RecommendedBet(
                bet_type=b.bet_type,
                horse_desc=b.horse_desc,
                ev=b.ev,
                stake=stake,
                units=units,
                label=label,
            )
        )
    return RecommendedBetPlan(bets=out)


def format_recommended_bets_block(plan: RecommendedBetPlan) -> str:
    """おすすめ掛け金プランを note 買い目セクション直下に差し込む Markdown へ整形する。

    買い目が無い場合は空文字を返す（余計な見出しを出さない）。
    """
    if not plan.bets:
        return ""
    lines = [
        "### 💰 AI推奨購入額（1点100円ベース換算）",
        "",
        "🏇 **買い目とおすすめ掛け金**",
        "",
    ]
    for b in plan.bets:
        lines += [
            f"- ■ {b.bet_type}：{b.horse_desc}",
            f"    - おすすめ掛け金：**{b.stake:,}円**（{b.comment}）",
        ]
    lines += [
        "",
        f"> 💰 **このレースの想定総投資額：{plan.total_stake:,}円**",
        f"> {_NOTE_BET_NOTE}",
        "",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────
# 週次 Note レポート
# ─────────────────────────────────────────────────────────────────────
@dataclass
class ModelWeeklyStat:
    model_name: str
    n_bets: int
    n_hits: int
    total_stake: int
    total_return: int
    best_payout: int
    best_payout_desc: str = ""

    @property
    def hit_rate(self) -> float:
        return (100.0 * self.n_hits / self.n_bets) if self.n_bets else 0.0

    @property
    def roi(self) -> float:
        return (
            (100.0 * self.total_return / self.total_stake) if self.total_stake else 0.0
        )


def export_weekly_report(
    stats: list[ModelWeeklyStat],
    *,
    period_label: str,
    out_dir: Path | None = None,
    report_date: _date | None = None,
) -> Path:
    """集客モデル週次統計を note 貼付用 Markdown で書き出し、その Path を返す。"""
    rd = report_date or _date.today()
    d = out_dir or _SNS_DIR
    d.mkdir(parents=True, exist_ok=True)

    # 主役は「最高配当」と「的中率」。赤字寄りの回収率は強調せず参考表示に留める。
    headline = ""
    if stats:
        top = max(stats, key=lambda s: s.best_payout)
        desc = (
            f"（{top.model_name}・{top.best_payout_desc}）"
            if top.best_payout_desc
            else f"（{top.model_name}）"
        )
        headline = f"## 🏆 今週の最高配当：**¥{top.best_payout:,}** {desc}"

    lines = [
        f"# 🏇 UMALOGI 集客モデル 週次成績（{period_label}）",
        "",
        "AI が高配当を狙う「観賞用」予想モデルの成績です。注目は **的中** と **最高配当**！",
        "",
    ]
    if headline:
        lines += [headline, ""]
    lines += [
        "| モデル | 件数 | 🎯的中率 | 🏆最高配当 | 回収率(参考) |",
        "|---|---:|---:|---|---:|",
    ]
    if not stats:
        lines.append("| （対象データなし） | - | - | - | - |")
    else:
        for s in stats:
            best = f"**¥{s.best_payout:,}**"
            if s.best_payout_desc:
                best += f" {s.best_payout_desc}"
            lines.append(
                f"| {s.model_name} | {s.n_bets} | **{s.hit_rate:.1f}%** | "
                f"{best} | {s.roi:.1f}% |"
            )
    lines += [
        "",
        f"📊 全モデルの詳細・買い目は note で公開中 → {_NOTE_URL}",
        "",
        _HASHTAGS_X,
    ]

    path = d / f"weekly_report_{rd.strftime('%Y%m%d')}.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("[SNS] 週次レポート出力: %s", path)
    return path


# ─────────────────────────────────────────────────────────────────────
# DB 連携グルー（本番運用）
# ─────────────────────────────────────────────────────────────────────
def _format_combo(combo_json: str | None, bet_type: str = "") -> str:
    """combination_json を SNS 映えする可読な買い目表記へ変換する。

    例:
      "[3]"                          → "3"
      "[3,5]"                        → "3-5"
      "[[6,8],[6,16],[6,2]]"         → "軸6→相手2,8,16（3点）"  （全組共通の軸あり）
      "[[6,8],[8,16],[6,16]]"        → "6-8 / 8-16 / 6-16（3点）"（軸なし=ボックス）
    パース不能時は記号類を除いた文字列にフォールバックする（最低限の可読化）。
    """
    if not combo_json:
        return ""
    try:
        data = json.loads(combo_json)
    except (ValueError, TypeError):
        return str(combo_json).strip("[]").replace('"', "")

    # フラット数値リスト（単勝/複勝など）: [3] / [3,5]
    if isinstance(data, list) and data and not isinstance(data[0], (list, tuple)):
        try:
            return "-".join(str(int(n)) for n in data)
        except (ValueError, TypeError):
            return ",".join(str(n) for n in data)

    # 組み合わせリスト（馬連/三連複など）: [[..],[..]]
    if isinstance(data, list) and data and isinstance(data[0], (list, tuple)):
        try:
            combos = [[int(n) for n in c] for c in data]
        except (ValueError, TypeError):
            combos = [[*c] for c in data]
        n = len(combos)
        common = set(combos[0])
        for c in combos[1:]:
            common &= set(c)
        if common and n > 1:  # 全組に共通する軸あり → 軸→相手表記
            axes = sorted(common)
            others = sorted({x for c in combos for x in c} - common)
            axis_s = ",".join(map(str, axes))
            if others:
                return f"軸{axis_s}→相手{','.join(map(str, others))}（{n}点）"
            return f"{axis_s}（{n}点）"
        parts = ["-".join(map(str, c)) for c in combos[:8]]  # 軸なし=ボックス
        body = " / ".join(parts)
        if n > 8:
            return f"{body} 他（計{n}点）"
        return f"{body}（{n}点）" if n > 1 else body

    return str(data)


def detect_and_flash(
    conn: sqlite3.Connection,
    race_id: str,
    *,
    race_label: str | None = None,
    venue: str = "",
    sender: Sender | None = None,
    out_dir: Path | None = None,
) -> list[str]:
    """確定レースで集客モデルの的中を検知し、的中速報を生成・配信する。

    Returns: 生成された速報テキストのリスト（高ROI/万馬券のみ・閾値未満は含まない）。
    """
    rows = conn.execute(
        """
        SELECT p.model_type, p.bet_type, p.combination_json,
               COALESCE(pr.payout, 0) AS payout, COALESCE(pr.profit, 0) AS profit,
               COALESCE(p.expected_value, 0.0) AS expected_value
          FROM predictions p
          JOIN prediction_results pr ON pr.prediction_id = p.id
         WHERE p.race_id = ? AND pr.is_hit = 1 AND COALESCE(p.is_superseded, 0) = 0
        """,
        (race_id,),
    ).fetchall()
    out: list[str] = []
    date_str = (
        race_id[:8]
        if len(race_id) >= 8 and race_id[:8].isdigit()
        else _date.today().strftime("%Y%m%d")
    )
    for model_type, bet_type, combo_json, payout, profit, expected_value in rows:
        if not is_ornamental_model(model_type):
            continue
        stake = int(round(payout - profit)) or 100  # flat_cost(¥100×点数)
        hit = HitFlash(
            race_name=race_label or race_id,
            venue=venue,
            model_name=base_model(model_type),
            bet_type=bet_type,
            horse_desc=_format_combo(combo_json, bet_type),
            stake=stake,
            payout=int(round(payout)),
        )
        text = generate_hit_flash(hit)
        if text:
            out.append(text)
            send_hit_flash(hit, sender)
            # X 速報ファイル書き出し（例外セーフ）
            try:
                bet_result = BetResult(
                    race_name=race_label or race_id,
                    venue=venue,
                    bet_type=bet_type,
                    horse_desc=_format_combo(combo_json, bet_type),
                    ev=float(expected_value),
                    stake=stake,
                    payout=int(round(payout)),
                    is_hit=True,
                    date=date_str,
                )
                tweet = generate_x_hit_tweet(bet_result)
                if tweet:
                    _d = out_dir if out_dir is not None else _REPORTS_DIR
                    _d.mkdir(parents=True, exist_ok=True)
                    # race_id + model の組み合わせでユニークなファイル名
                    safe_rid = "".join(c for c in race_id if c.isalnum())[-12:]
                    safe_bt = "".join(c for c in bet_type if "　" <= c or c.isalnum())[
                        :4
                    ]
                    x_path = _d / f"x_hit_{date_str}_{safe_rid}_{safe_bt}.txt"
                    x_path.write_text(tweet, encoding="utf-8")
                    logger.info("[post_race] X速報ファイル: %s", x_path)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[post_race] X速報書き出し失敗（続行）: %s", exc)

    if out:
        logger.info("[SNS] 集客モデル的中速報 %d件 (race_id=%s)", len(out), race_id)
    return out


def compute_ornamental_weekly_stats(
    conn: sqlite3.Connection,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[ModelWeeklyStat]:
    """集客モデル(Oracle/HitFocus)の統計を ModelWeeklyStat リストで返す（実弾モデル除外）。

    cost = payout - profit（pnl_accounting と同一基準）。start_date/end_date 指定時のみ
    races.date で期間を絞る（未指定なら全期間・races テーブル非依存）。
    """
    join, where, params = "", "WHERE COALESCE(p.is_superseded, 0) = 0", []
    if start_date and end_date:
        join = "JOIN races r ON r.race_id = p.race_id"
        where += " AND r.date BETWEEN ? AND ?"
        params = [start_date, end_date]
    rows = conn.execute(
        f"""
        SELECT p.model_type, p.bet_type,
               COALESCE(pr.payout, 0), COALESCE(pr.profit, 0), COALESCE(pr.is_hit, 0)
          FROM predictions p
          JOIN prediction_results pr ON pr.prediction_id = p.id
          {join}
          {where}
        """,
        params,
    ).fetchall()
    agg: dict[str, dict] = {}
    for model_type, bet_type, payout, profit, is_hit in rows:
        if not is_ornamental_model(model_type):
            continue
        base = base_model(model_type)
        e = agg.setdefault(
            base, {"n": 0, "hits": 0, "stake": 0, "ret": 0, "best": 0, "best_desc": ""}
        )
        e["n"] += 1
        e["hits"] += is_hit
        e["stake"] += int(round(payout - profit))
        e["ret"] += int(round(payout))
        if payout > e["best"]:
            e["best"] = int(round(payout))
            e["best_desc"] = str(bet_type)
    return [
        ModelWeeklyStat(
            model_name=m,
            n_bets=e["n"],
            n_hits=int(e["hits"]),
            total_stake=e["stake"],
            total_return=e["ret"],
            best_payout=e["best"],
            best_payout_desc=e["best_desc"],
        )
        for m, e in sorted(agg.items())
    ]


def run_weekly_report(target_date: str | None = None) -> Path:
    """土日17:00以降の一発実行用。対象週(月〜日)を集計し note 用 Markdown を出力する。"""
    from src.database.init_db import init_db
    from src.ml.pure_ev_edge import _iso_week_range

    d = target_date or _date.today().isoformat()
    start, end = _iso_week_range(d)
    conn = init_db()
    try:
        stats = compute_ornamental_weekly_stats(conn, start_date=start, end_date=end)
        return export_weekly_report(stats, period_label=f"{start} 〜 {end}")
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────
# 事後報告ジェネレーター（サブスク信頼構築）
# ─────────────────────────────────────────────────────────────────────


def generate_x_hit_tweet(
    result: BetResult,
    *,
    note_url: str | None = None,
) -> str | None:
    """的中した場合のみ X 速報テキストを生成する（外れは None）。

    140 文字以内を保証する。note_url 未指定時は環境変数 NOTE_MYPAGE_URL を使用。

    Args:
        result:   BetResult（is_hit=False なら None を返す）。
        note_url: 誘導先 note URL。

    Returns:
        140 文字以内のツイートテキスト、または None（外れ時）。
    """
    if not result.is_hit:
        return None

    url = note_url or _NOTE_URL
    ev_note = f"EV{result.ev:.2f}通り。" if result.ev > 0 else ""
    body = (
        f"🎯的中速報！{result.venue} {result.race_name}、"
        f"{result.bet_type}「{result.horse_desc}」が"
        f"¥{result.payout:,}的中！"
        f"{ev_note}明日の予想もNoteで👇"
    )
    tags = "#競馬的中 #UMALOGI #AI予想 #JRA"
    tweet = f"{body}\n{url}\n{tags}"
    return tweet[:140]


def generate_post_race_report(
    results: list[BetResult],
    *,
    date: str | None = None,
    note_url: str | None = None,
) -> str:
    """1 日の全予想結果をまとめた Note 向け総括レポート Markdown を返す（純関数）。

    Args:
        results:  当日の BetResult リスト（的中・外れ混在可）。空でも動作する。
        date:     対象日 YYYYMMDD / YYYY-MM-DD（省略時: results[0].date → 本日）。
        note_url: 末尾の誘導リンク（省略時: _NOTE_URL）。

    Returns:
        note.com に貼り付け可能な Markdown 文字列。
    """
    url = note_url or _NOTE_URL

    # 日付を解決
    ds = date or (results[0].date if results else _date.today().strftime("%Y%m%d"))
    ds = ds.replace("-", "")
    y, m, d = ds[:4], ds[4:6], ds[6:]

    # 集計
    n_total = len(results)
    n_hits = sum(1 for r in results if r.is_hit)
    total_stake = sum(r.stake for r in results)
    total_payout = sum(r.payout for r in results)
    hit_rate = 100.0 * n_hits / n_total if n_total else 0.0
    roi = 100.0 * total_payout / total_stake if total_stake else 0.0
    avg_ev = sum(r.ev for r in results) / n_total if n_total else 0.0
    hits = [r for r in results if r.is_hit]

    lines: list[str] = [
        f"# 【結果報告】{y}年{m}月{d}日 UMALOGI AI予想 サマリー",
        "",
        "> 本日のAI予想結果です。長期的なEV優位性に基づく運用の記録として公開します。",
        "",
        "---",
        "",
    ]

    if not results:
        lines += [
            "## ℹ️ 本日の結果データ",
            "",
            "本日は結果データがありません。レース確定後に自動更新されます。",
            "",
        ]
    else:
        lines += [
            "## 📊 本日の成績サマリー",
            "",
            "| 項目 | 数値 |",
            "|------|------|",
            f"| 総買い目数 | {n_total}点 |",
            f"| 的中数 | {n_hits}点 |",
            f"| 的中率 | {hit_rate:.1f}% |",
            f"| 総投資額 | ¥{total_stake:,} |",
            f"| 総払戻額 | ¥{total_payout:,} |",
            f"| 本日回収率 | **{roi:.1f}%** |",
            "",
            "---",
            "",
        ]
        if hits:
            lines += [
                "## 🎯 的中買い目一覧",
                "",
                "| レース | 券種 | 買い目 | 期待値(EV) | 払戻 |",
                "|--------|------|--------|:---------:|------|",
            ]
            for r in hits:
                lines.append(
                    f"| {r.venue} {r.race_name} | {r.bet_type} | {r.horse_desc}"
                    f" | {r.ev:.2f} | ¥{r.payout:,} |"
                )
            lines += ["", "---", ""]

        lines += [
            "## 📈 EV優位性の振り返り",
            "",
            f"- 本日の平均EV: **{avg_ev:.2f}**（EV &gt; 1.0 = 長期プラス期待）",
            f"- 的中率: {hit_rate:.1f}% / 回収率: {roi:.1f}%",
            "> ⚠️ 短期の結果に一喜一憂せず、長期の期待値優位性を信頼してください。",
            "",
        ]

    lines += [
        "---",
        "",
        f"📊 全成績・明日の予想は note で公開中 → {url}",
        "",
        _HASHTAGS_X,
        "",
    ]
    return "\n".join(lines)


def write_daily_reports(
    results: list[BetResult],
    *,
    date: str | None = None,
    note_url: str | None = None,
    out_dir: Path | None = None,
) -> Path:
    """Note 日次総括 + 的中 X 速報ファイルを outputs/sns/reports/ に書き出す。

    Args:
        results:  当日の BetResult リスト。空でも動作する。
        date:     対象日 YYYYMMDD / YYYY-MM-DD（省略時: results[0].date → 本日）。
        note_url: X ツイートに含める note URL。
        out_dir:  出力先ディレクトリ（省略時: _REPORTS_DIR）。テスト時は tmp_path を渡す。

    Returns:
        note_report_YYYYMMDD.md の Path。
    """
    ds = date or (results[0].date if results else _date.today().strftime("%Y%m%d"))
    ds = ds.replace("-", "")
    d = out_dir if out_dir is not None else _REPORTS_DIR
    d.mkdir(parents=True, exist_ok=True)

    # Note 日次総括
    report_md = generate_post_race_report(results, date=ds, note_url=note_url)
    note_path = d / f"note_report_{ds}.md"
    note_path.write_text(report_md, encoding="utf-8")
    logger.info("[post_race] Note総括: %s", note_path)

    # 的中買い目ごとに X 速報ファイルを出力
    hits = [r for r in results if r.is_hit]
    for i, r in enumerate(hits, 1):
        tweet = generate_x_hit_tweet(r, note_url=note_url)
        if tweet:
            x_path = d / f"x_hit_{ds}_{i:02d}.txt"
            x_path.write_text(tweet, encoding="utf-8")
            logger.info("[post_race] X速報: %s", x_path)

    return note_path
