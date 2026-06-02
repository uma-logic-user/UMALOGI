"""週末本番稼働前 統合ヘルスチェック（DB/モデル/通知 非汚染 dry-run）

確認項目:
  [T1] JRA-VAN / JVLink: COM利用可否・watchdog稼働・DB鮮度・entry同期状況
  [T2] モデルロード・予想生成ドライラン（新Challenger+複勝Platt較正器）
  [T3] 通知フォーマット検証（WATCH_ONLY/実弾の分離・Discord webhook設定）

本スクリプトは predictions テーブルや外部サービスへ一切書き込まない。
"""

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]

_REPORT_PATH = _ROOT / "logs" / "weekend_health_check.log"
_DB_PATH = _ROOT / "data" / "umalogi.db"

# 結果集計
_results: dict[str, str] = {}   # key → "OK" | "WARN" | "NG" | "INFO"
_lines: list[str] = []


def log(msg: str = "") -> None:
    print(msg, flush=True)
    _lines.append(msg)


def mark(key: str, status: str, detail: str = "") -> None:
    _results[key] = status
    badge = {"OK": "✅", "WARN": "⚠️", "NG": "❌", "INFO": "ℹ️"}.get(status, "?")
    log(f"  {badge} [{status}] {key}" + (f" — {detail}" if detail else ""))


# ─────────────────────────────────────────────────────────────────
# T1: JRA-VAN / JVLink / DB 鮮度
# ─────────────────────────────────────────────────────────────────
def check_jravan(conn: sqlite3.Connection) -> None:
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log("[T1] JRA-VAN / データ取得 / DB 鮮度チェック")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # --- T1-1: JVLink COM（32bit Python）利用可否
    try:
        import subprocess
        r = subprocess.run(
            ["py", "-3-32", "-c", "import win32com.client; print('COM_OK')"],
            capture_output=True, text=True, timeout=10, encoding="utf-8",
        )
        if "COM_OK" in r.stdout:
            mark("T1-1 JVLink COM (32bit)", "OK", "win32com.client ロード成功")
        else:
            mark("T1-1 JVLink COM (32bit)", "NG", r.stderr.strip()[:80])
    except Exception as exc:
        mark("T1-1 JVLink COM (32bit)", "NG", str(exc)[:80])

    # --- T1-2: watchdog プロセス確認
    try:
        import subprocess
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name like '%python%'\" | "
             "Select-Object -ExpandProperty CommandLine"],
            capture_output=True, text=True, timeout=10, encoding="utf-8",
        )
        procs = r.stdout
        wd_running = "watchdog.py" in procs
        ap_running = "today_auto_runner.py" in procs
        mark("T1-2 watchdog 稼働", "OK" if wd_running else "WARN",
             f"watchdog={'稼働中' if wd_running else '停止'} / "
             f"autopilot={'稼働中' if ap_running else '停止(金曜夜に起動)'}")
    except Exception as exc:
        mark("T1-2 watchdog 稼働", "WARN", str(exc)[:60])

    # --- T1-3: DB データ鮮度
    try:
        max_race_date = conn.execute("SELECT MAX(date) FROM races").fetchone()[0]
        max_pred = conn.execute("SELECT MAX(created_at) FROM predictions").fetchone()[0]
        entry_count = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        odds_count = conn.execute("SELECT COUNT(*) FROM realtime_odds").fetchone()[0]
        next_sat = conn.execute(
            "SELECT COUNT(DISTINCT r.race_id) FROM entries e "
            "JOIN races r ON e.race_id = r.race_id "
            "WHERE r.date >= '2026-06-06'"
        ).fetchone()[0]
        log(f"     races 最終日      : {max_race_date}")
        log(f"     predictions 最終  : {max_pred}")
        log(f"     entries 総数      : {entry_count:,}")
        log(f"     realtime_odds 件数: {odds_count:,}")
        log(f"     6/6 以降エントリ  : {next_sat} race (金曜夜の自動同期前のため0が正常)")
        stale = max_race_date < "2026-05-30" if max_race_date else True
        mark("T1-3 DB データ鮮度", "NG" if stale else "OK",
             f"最終レース日={max_race_date}" + (" ← 古すぎる" if stale else ""))
        mark("T1-4 6/6 エントリ同期状況",
             "INFO" if next_sat == 0 else "OK",
             "金曜夜 autopilot 起動後に自動取得予定" if next_sat == 0 else f"{next_sat}R 取得済み")
    except Exception as exc:
        mark("T1-3 DB データ鮮度", "NG", str(exc)[:80])

    # --- T1-5: netkeiba フォールバック import
    try:
        from src.scraper.entry_table import fetch_entry_table  # noqa: F401
        mark("T1-5 netkeiba フォールバック import", "OK", "entry_table モジュール OK")
    except Exception as exc:
        mark("T1-5 netkeiba フォールバック import", "NG", str(exc)[:80])


# ─────────────────────────────────────────────────────────────────
# T2: モデルロード + 予想生成ドライラン
# ─────────────────────────────────────────────────────────────────
def check_model_and_pipeline(conn: sqlite3.Connection) -> None:
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log("[T2] モデルロード・予想生成ドライラン")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # --- T2-1: 新 Challenger pkl ロード
    try:
        from src.ml.models import ManjiModel
        m = ManjiModel()
        m.load()
        pkl_size = (_ROOT / "data" / "models" / "manji_model.pkl").stat().st_size
        mark("T2-1 ManjiModel ロード(Challenger昇格pkl)", "OK",
             f"ロード成功 / pkl={pkl_size:,}bytes / trained={m.is_trained}")
    except Exception as exc:
        mark("T2-1 ManjiModel ロード", "NG", str(exc)[:80])
        return

    # --- T2-2: 複勝 Platt 較正器ロード
    try:
        from src.ml.manji_calibration import calibrate_place_prob, _load_place_cal
        cal = _load_place_cal()
        p_test = calibrate_place_prob(2.0)
        mark("T2-2 複勝 Platt 較正器ロード", "OK" if cal is not None else "WARN",
             f"P(複勝圏|ev=2.0)={p_test:.4f}" + ("(Platt)" if cal else "(フォールバック)"))
    except Exception as exc:
        mark("T2-2 複勝 Platt 較正器ロード", "NG", str(exc)[:80])

    # --- T2-3: bet_policy ポリシー確認（WATCH_ONLY / live 分離）
    try:
        from src.ml.bet_policy import (
            LIVE_MODELS, WATCH_ONLY_MODELS, MODEL_LIVE_BET_TYPES,
            is_live_bet, is_watch_only,
        )
        manji_live_bets = MODEL_LIVE_BET_TYPES.get("卍", {"単勝", "複勝"})
        manji_watch = WATCH_ONLY_MODELS.get("卍", set())
        live_place = is_live_bet("卍(直前)", "複勝")
        live_win   = is_live_bet("卍(直前)", "単勝")
        watch_win  = is_watch_only("卍(直前)", "単勝")
        ok = live_place and not live_win and watch_win
        mark("T2-3 ポリシー分離確認",
             "OK" if ok else "NG",
             f"卍複勝=実弾{live_place} / 卍単勝=live{live_win},watch{watch_win} "
             f"/ LIVE_MODELS={sorted(LIVE_MODELS)}")
    except Exception as exc:
        mark("T2-3 ポリシー分離確認", "NG", str(exc)[:80])

    # --- T2-4: FeatureBuilder + 卍 ev_score ドライラン（直近レース）
    try:
        from src.ml.features import FeatureBuilder
        recent_rid = conn.execute(
            "SELECT race_id FROM races WHERE date <= '2026-05-31' "
            "ORDER BY date DESC, race_id DESC LIMIT 1"
        ).fetchone()
        if recent_rid:
            rid = recent_rid[0]
            fb = FeatureBuilder(conn)
            df = fb.build_race_features(rid)
            if df is not None and not df.empty:
                ev = m.ev_score(df)
                top_ev = float(ev.max())
                mark("T2-4 特徴量生成 + ev_score ドライラン", "OK",
                     f"race_id={rid} / n_horses={len(df)} / max_ev={top_ev:.3f}")
            else:
                mark("T2-4 特徴量生成 + ev_score ドライラン", "WARN",
                     f"race_id={rid} でデータ空（稀な欠損）")
        else:
            mark("T2-4 特徴量生成 + ev_score ドライラン", "WARN", "直近レースIDなし")
    except Exception as exc:
        mark("T2-4 特徴量生成 + ev_score ドライラン", "NG", str(exc)[:120])

    # --- T2-5: BetGenerator dry-run（DB書込なし）
    try:
        from src.ml.bet_generator import ManjiStrategy
        from src.ml.models import _MODEL_CACHE

        _MODEL_CACHE.clear()

        recent_rid = conn.execute(
            "SELECT race_id FROM races WHERE date <= '2026-05-31' "
            "ORDER BY date DESC, race_id DESC LIMIT 1"
        ).fetchone()
        if recent_rid:
            rid = recent_rid[0]
            from src.ml.features import FeatureBuilder
            df = FeatureBuilder(conn).build_race_features(rid)
            if df is not None and not df.empty:
                manji_scores = m.ev_score(df)
                strat = ManjiStrategy()   # estimator=None でデフォルト OddsEstimator
                bets = strat.generate(rid, df, manji_scores, bankroll=10000)
                live_bets  = [b for b in bets.bets if is_live_bet("卍(直前)", b.bet_type)]
                watch_bets = [b for b in bets.bets if is_watch_only("卍(直前)", b.bet_type)]
                log(f"     生成買い目: 実弾={len(live_bets)}件 / WATCH_ONLY={len(watch_bets)}件")
                for b in live_bets[:3]:
                    log(f"       [実弾] {b.bet_type} EV={b.expected_value:.2f} "
                        f"conf={b.confidence:.3f}")
                for b in watch_bets[:2]:
                    log(f"       [WATCH] {b.bet_type} EV={b.expected_value:.2f} ← 投票しない")
                mark("T2-5 BetGenerator ドライラン", "OK",
                     f"実弾{len(live_bets)}件(複勝のみ) / WATCH_ONLY{len(watch_bets)}件(単勝)")
            else:
                mark("T2-5 BetGenerator ドライラン", "WARN", "特徴量DF空")
        else:
            mark("T2-5 BetGenerator ドライラン", "WARN", "直近レースIDなし")
    except Exception as exc:
        mark("T2-5 BetGenerator ドライラン", "NG", str(exc)[:120])


# ─────────────────────────────────────────────────────────────────
# T3: UI / 通知フォーマット検証
# ─────────────────────────────────────────────────────────────────
def check_notification() -> None:
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log("[T3] UI / Discord 通知フォーマット検証")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # --- T3-1: Discord webhook 設定確認
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
    webhook = os.getenv("DISCORD_WEBHOOK_URL", "")
    if webhook and len(webhook) > 50:
        mark("T3-1 Discord webhook 設定", "OK",
             f"設定済み ({len(webhook)}文字) — 本チェックでは実送信しない")
    else:
        mark("T3-1 Discord webhook 設定", "NG", "未設定 / 短すぎる")

    # --- T3-2: discord_notifier import
    try:
        from src.notification.discord_notifier import DiscordNotifier  # noqa: F401
        mark("T3-2 DiscordNotifier import", "OK")
    except Exception as exc:
        mark("T3-2 DiscordNotifier import", "NG", str(exc)[:80])

    # --- T3-3: WATCH_ONLY が「実弾」表示されないかロジック確認
    try:
        from src.ml.bet_policy import is_live_bet, is_watch_only, is_ornamental
        cases = [
            ("卍(直前)", "複勝",  True,  False, False),  # 実弾
            ("卍(直前)", "単勝",  False, True,  False),  # WATCH_ONLY
            ("本命(直前)", "単勝", False, False, False),  # NON_LIVE_RETIRED
            ("Oracle(直前)", "単勝", False, False, True), # 観賞用
        ]
        all_ok = True
        log("     モデル×券種   → is_live / is_watch / is_ornamental")
        for model, bet, exp_live, exp_watch, exp_orn in cases:
            live = is_live_bet(model, bet)
            watch = is_watch_only(model, bet)
            orn = is_ornamental(model)
            ok = (live == exp_live and watch == exp_watch and orn == exp_orn)
            flag = "✅" if ok else "❌"
            log(f"     {flag} {model:20s} × {bet:4s}  "
                f"live={live!s:5} watch={watch!s:5} orn={orn!s:5}")
            if not ok:
                all_ok = False
        mark("T3-3 WATCH_ONLY/実弾/観賞用 ラベル分離", "OK" if all_ok else "NG")
    except Exception as exc:
        mark("T3-3 WATCH_ONLY/実弾分離", "NG", str(exc)[:80])

    # --- T3-4: Discord フォーマットプレビュー（送信しない）
    log("")
    log("     ── Discord 通知フォーマット プレビュー ──")
    log("     【卍 週末予想】2026-06-07 東京11R 安田記念")
    log("     ┌─────────────────────────────────────┐")
    log("     │ 🟢 [実弾] 複勝  #5 ○○○○   EV 2.31 │")
    log("     │          P(複勝圏)=0.287 (Platt)    │")
    log("     │ 👁️ [監視] 単勝  #5 ○○○○   EV 2.31 │  ← 投票なし・ROI追跡のみ")
    log("     └─────────────────────────────────────┘")
    log("     ⚠️  注: 単勝は WATCH_ONLY のため Discord 通知に含まれるが")
    log("          実弾フラグ(is_live_bet)=False で投票処理には渡らない。")
    mark("T3-4 Discord フォーマット", "INFO", "プレビュー表示のみ（実送信なし）")


# ─────────────────────────────────────────────────────────────────
# サマリー
# ─────────────────────────────────────────────────────────────────
def print_summary() -> int:
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log("【ヘルスチェック サマリー】")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    counts = {"OK": 0, "WARN": 0, "NG": 0, "INFO": 0}
    for key, status in _results.items():
        badge = {"OK": "✅", "WARN": "⚠️", "NG": "❌", "INFO": "ℹ️"}.get(status, "?")
        log(f"  {badge} {status:5s}  {key}")
        counts[status] = counts.get(status, 0) + 1
    log("")
    ng = counts["NG"]
    warn = counts["WARN"]
    log(f"  OK={counts['OK']} / WARN={warn} / NG={ng} / INFO={counts['INFO']}")
    if ng == 0 and warn == 0:
        log("  ✅ All GREEN — 週末本番稼働 準備完了")
        overall = "ALL_GREEN"
    elif ng == 0:
        log(f"  ⚠️  WARN {warn}件 — 軽微な注意事項あり（稼働は可能）")
        overall = "WARN"
    else:
        log(f"  ❌ NG {ng}件 — 要対処")
        overall = "NG"
    log(f"\nOVERALL: {overall}")
    return 0 if ng == 0 else 1


def main() -> int:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("=" * 60)
    log(f"UMALOGI 週末本番稼働前 統合ヘルスチェック  {ts}")
    log("=" * 60)
    log("モード: DB非汚染 dry-run / Discord実送信なし")

    conn = sqlite3.connect(f"file:{_DB_PATH}?mode=ro", uri=True, timeout=5)

    check_jravan(conn)
    check_model_and_pipeline(conn)
    check_notification()
    rc = print_summary()

    # ログ保存
    _REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(_lines) + "\n")
    log(f"\n[レポート出力] {_REPORT_PATH}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
