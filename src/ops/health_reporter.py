"""
W-058 日次ヘルスレポート — システム稼働の可観測性。

当日のパイプライン健全性を集計し Discord #system へ自動送信する。
「12/24が1点」「odds_timeseries空」「結果3件欠落」のような劣化を
人間が気付ける状態にするための観測レイヤー（条項5・可観測性）。

集計指標:
  - 予想生成カバー率   : predictions(is_superseded=0)のある race / 当日 race
  - 直前予想カバー率   : "(直前)" 予想のある race / 当日 race
  - オッズ時系列健全性 : realtime_odds が 2点以上 / 1点 / 0点 の race 数
                         （2点以上 = odds_drift / odds_momentum が稼働可能）
  - 結果取得の欠損     : rank 未取得 race 数
  - Discord通知エラー  : 当日ログから best-effort 集計
"""

from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_LOG_DIR = _ROOT / "data"

# Discord 通知エラーをログから拾う際のパターン（best-effort）
_DISCORD_ERR_RE = re.compile(r"Discord.*(失敗|エラー|error)", re.IGNORECASE)

# 健全性しきい値
_COVERAGE_CRIT = 0.90  # 予想カバー率がこれ未満 → 重大
_ODDS_HEALTH_WARN = 1.0  # 2点以上率がこれ未満 → 警告


@dataclass
class HealthReport:
    """1 日分のヘルスレポート。"""

    date: str
    n_races: int
    n_predicted: int  # 予想あり(is_superseded=0)
    n_chokuzen: int  # 直前予想あり
    n_odds_ge2: int  # realtime_odds 2点以上
    n_odds_1: int  # 1点のみ
    n_odds_0: int  # 0点
    n_results: int  # rank あり
    n_results_missing: int  # rank 欠損
    n_discord_errors: int  # ログベース

    def _rate(self, n: int) -> float:
        return (n / self.n_races) if self.n_races else 0.0

    @property
    def coverage_rate(self) -> float:
        return self._rate(self.n_predicted)

    @property
    def odds_ge2_rate(self) -> float:
        return self._rate(self.n_odds_ge2)

    @property
    def severity(self) -> str:
        """'ok' / 'warn' / 'crit' を返す。"""
        if self.n_races == 0:
            return "ok"  # 非開催日は健全扱い
        if (
            self.coverage_rate < _COVERAGE_CRIT
            or self.n_results_missing > 0
            or self.n_discord_errors > 0
        ):
            return "crit"
        if self.odds_ge2_rate < _ODDS_HEALTH_WARN or self.n_chokuzen < self.n_races:
            return "warn"
        return "ok"


def _count_discord_errors(date_str: str) -> int:
    """当日ログから Discord 通知エラー行を best-effort で数える。

    Args:
        date_str: "YYYY-MM-DD"。ログ内の "YYYY-MM-DD" 出現行のみ対象。

    Returns:
        Discord エラーらしき行数（ログ未存在時は 0）。
    """
    count = 0
    for log_path in (_LOG_DIR / "auto_runner.log", _LOG_DIR / "scheduler.log"):
        if not log_path.exists():
            continue
        try:
            with open(log_path, encoding="utf-8", errors="replace") as f:
                for line in f:
                    if date_str in line and _DISCORD_ERR_RE.search(line):
                        count += 1
        except Exception as exc:  # noqa: BLE001 — ログ読み失敗で集計を止めない
            logger.debug("ログ読込失敗 %s: %s", log_path.name, exc)
    return count


def collect_health(
    conn: sqlite3.Connection, date_str: str | None = None
) -> HealthReport:
    """指定日（既定=当日）のヘルスレポートを集計する。

    Args:
        conn: umalogi.db 接続。
        date_str: "YYYY-MM-DD"。None なら当日。

    Returns:
        :class:`HealthReport`。
    """
    d = date_str or date.today().isoformat()

    race_ids = [
        r[0]
        for r in conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_id", (d,)
        ).fetchall()
    ]
    n_races = len(race_ids)

    n_predicted = n_chokuzen = 0
    n_odds_ge2 = n_odds_1 = n_odds_0 = 0
    n_results = 0
    for rid in race_ids:
        has_pred = conn.execute(
            "SELECT 1 FROM predictions WHERE race_id = ? "
            "AND COALESCE(is_superseded, 0) = 0 LIMIT 1",
            (rid,),
        ).fetchone()
        if has_pred:
            n_predicted += 1
        has_chokuzen = conn.execute(
            "SELECT 1 FROM predictions WHERE race_id = ? "
            "AND COALESCE(is_superseded, 0) = 0 AND model_type LIKE '%(直前)%' LIMIT 1",
            (rid,),
        ).fetchone()
        if has_chokuzen:
            n_chokuzen += 1

        snaps = conn.execute(
            "SELECT COUNT(DISTINCT recorded_at) FROM realtime_odds "
            "WHERE race_id = ? AND win_odds IS NOT NULL",
            (rid,),
        ).fetchone()[0]
        if snaps >= 2:
            n_odds_ge2 += 1
        elif snaps == 1:
            n_odds_1 += 1
        else:
            n_odds_0 += 1

        has_result = conn.execute(
            "SELECT 1 FROM race_results WHERE race_id = ? AND rank IS NOT NULL LIMIT 1",
            (rid,),
        ).fetchone()
        if has_result:
            n_results += 1

    return HealthReport(
        date=d,
        n_races=n_races,
        n_predicted=n_predicted,
        n_chokuzen=n_chokuzen,
        n_odds_ge2=n_odds_ge2,
        n_odds_1=n_odds_1,
        n_odds_0=n_odds_0,
        n_results=n_results,
        n_results_missing=n_races - n_results,
        n_discord_errors=_count_discord_errors(d),
    )


def _pct(n: int, total: int) -> str:
    return f"{n / total * 100:.0f}%" if total else "—"


def format_report_fields(r: HealthReport) -> list[dict]:
    """Discord Embed 用の fields リストを構築する。"""
    return [
        {
            "name": "🎯 予想生成カバー率",
            "value": f"{r.n_predicted}/{r.n_races}（{_pct(r.n_predicted, r.n_races)}） "
            f"／ 直前 {r.n_chokuzen}/{r.n_races}",
            "inline": False,
        },
        {
            "name": "📈 オッズ時系列の健全性",
            "value": (
                f"2点以上(drift稼働可): **{r.n_odds_ge2}** ／ "
                f"1点のみ: {r.n_odds_1} ／ 0点: {r.n_odds_0}"
            ),
            "inline": False,
        },
        {
            "name": "🏁 結果取得",
            "value": f"取得 {r.n_results}/{r.n_races} ／ 欠損 **{r.n_results_missing}**",
            "inline": True,
        },
        {
            "name": "📨 Discord通知エラー",
            "value": f"{r.n_discord_errors} 件（ログベース）",
            "inline": True,
        },
    ]


def format_report_text(r: HealthReport) -> str:
    """プレーンテキスト版（ログ・テスト用）。"""
    icon = {"ok": "✅", "warn": "⚠️", "crit": "🚨"}[r.severity]
    return (
        f"{icon} UMALOGI 日次ヘルスレポート {r.date}\n"
        f"予想: {r.n_predicted}/{r.n_races}（直前 {r.n_chokuzen}）\n"
        f"オッズ時系列: 2点+ {r.n_odds_ge2} / 1点 {r.n_odds_1} / 0点 {r.n_odds_0}\n"
        f"結果: 取得 {r.n_results} / 欠損 {r.n_results_missing}\n"
        f"Discord通知エラー: {r.n_discord_errors}"
    )


def _safe_ab_variants(conn: sqlite3.Connection) -> dict | None:
    """W-057 シャドーA/B 集計（例外時 None・レポート本体を止めない）。"""
    try:
        from src.ml.pnl_accounting import compute_ab_variants

        return compute_ab_variants(conn)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[ヘルスレポート] A/B 集計失敗（続行）: %s", exc)
        return None


def format_ab_field(ab: dict) -> dict:
    """Pure_EV_Edge vs 従来単複 の A/B 比較を Discord Embed フィールドにする（W-057）。"""
    pe, lg = ab["pure_ev"], ab["legacy"]
    if not ab.get("both_active"):
        val = (
            f"Pure_EV_Edge: n={pe['n']} 純益¥{pe['profit']:,.0f} ROI{pe['roi']}%\n"
            f"従来単複: n={lg['n']} 純益¥{lg['profit']:,.0f} ROI{lg['roi']}%\n"
            f"（{ab['winner']}）"
        )
    else:
        sign = "+" if ab["diff_profit"] >= 0 else ""
        val = (
            f"💎Pure_EV_Edge: ROI**{pe['roi']}%** 純益¥{pe['profit']:,.0f} (n={pe['n']}/的中{pe['hit_rate']}%)\n"
            f"📊従来単複: ROI**{lg['roi']}%** 純益¥{lg['profit']:,.0f} (n={lg['n']}/的中{lg['hit_rate']}%)\n"
            f"差分: 純益{sign}¥{ab['diff_profit']:,.0f} / ROI{sign}{ab['diff_roi']}pt → **勝者: {ab['winner']}**"
        )
    return {
        "name": "🅰️🅱️ Pure_EV_Edge シャドーA/B (W-057)",
        "value": val,
        "inline": False,
    }


def format_ab_text(ab: dict) -> str:
    """A/B のプレーンテキスト版（ログ/テスト用）。"""
    pe, lg = ab["pure_ev"], ab["legacy"]
    return (
        f"PureEV ROI{pe['roi']}%/¥{pe['profit']:,.0f}(n{pe['n']}) vs "
        f"従来単複 ROI{lg['roi']}%/¥{lg['profit']:,.0f}(n{lg['n']}) "
        f"→ {ab['winner']} (差¥{ab['diff_profit']:,.0f})"
    )


def send_health_report(
    date_str: str | None = None, *, dry_run: bool = False
) -> HealthReport:
    """ヘルスレポートを集計し Discord #system へ Embed 送信する。

    Args:
        date_str: 対象日 "YYYY-MM-DD"。None なら当日。
        dry_run: True なら集計のみで送信しない。

    Returns:
        集計した :class:`HealthReport`。
    """
    from src.database.init_db import init_db

    conn = init_db()
    try:
        report = collect_health(conn, date_str)
        ab = _safe_ab_variants(conn)  # W-057 シャドーA/B（best-effort）
    finally:
        conn.close()

    logger.info("[ヘルスレポート]\n%s", format_report_text(report))
    if ab:
        logger.info("[ヘルスレポート][A/B] %s", format_ab_text(ab))
    if dry_run:
        report.ab = ab  # type: ignore[attr-defined]
        return report

    color = {"ok": 0x2ECC71, "warn": 0xF1C40F, "crit": 0xE74C3C}[report.severity]
    title_icon = {"ok": "✅", "warn": "⚠️", "crit": "🚨"}[report.severity]
    try:
        from src.notification.router import NotificationRouter

        NotificationRouter().send_system_embed(
            f"{title_icon} 日次ヘルスレポート — {report.date}",
            (
                "本日のパイプライン健全性サマリー。"
                if report.n_races
                else "本日は非開催日です（レースなし）。"
            ),
            color=color,
            fields=format_report_fields(report) + ([format_ab_field(ab)] if ab else []),
        )
    except Exception as exc:  # noqa: BLE001 — 送信失敗でレポート集計は成功扱い
        logger.warning("[ヘルスレポート] Discord 送信失敗: %s", exc)
    return report


def main() -> int:
    """CLI: `py -m src.ops.health_reporter [YYYY-MM-DD] [--dry-run]`"""
    import sys

    # Windows CP932 端末でも絵文字を安全に出力する（規則6）
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    date_str = args[0] if args else None
    report = send_health_report(date_str, dry_run=dry)
    print(format_report_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
