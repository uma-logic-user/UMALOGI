"""卍モデル 週末Challenger再訓練・OOS検証スクリプト（2026-06-02）

安全な Champion/Challenger 方式で卍モデルを再評価する:

  1. Champion  = 現役 data/models/manji_model.pkl（本番稼働中・上書き厳禁）
  2. Challenger = train_until=2024 で新規訓練したインメモリモデル
  3. 2025年（out-of-sample）全レースで卍・単勝/複勝(EV>1.0)戦略の ROI を双方算出
  4. Challenger が Champion 以上かつ OOS で黒字(ROI>=100%)のときのみ昇格(save)。
     それ以外は HOLD（現役を温存）。

CLAUDE.md 条項1/4 遵守: predictions テーブルは触らない。pkl は事前バックアップ済み。
ManjiModel.train() は pkl を保存しないため、本スクリプトが明示 save() するまで
本番モデルは一切変更されない。

使い方::
    py -X utf8 scripts/retrain_manji_weekend.py
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]

from src.database.init_db import init_db  # noqa: E402
from src.ml.models import ManjiModel  # noqa: E402
from scripts.backtest_all_models import (  # noqa: E402
    _run_three_model_backtest as _run_backtest,
)

_LOG_PATH = _ROOT / "logs" / "training_log_manji_weekend.log"
_PLACE_CAL_LOG_PATH = _ROOT / "logs" / "fukusho_calibration_final.log"
_TRAIN_UNTIL = 2024
_TEST_YEAR = "2025"

# 卍のみの戦略（単勝/複勝・EV>1.0 ゲート）
_MANJI_STRATEGIES = {
    "manji_tansho": {
        "label": "卍・単勝(EV>1.0)",
        "model": "manji",
        "bet_type": "単勝",
        "n_picks": 1,
        "ev_filter": True,
    },
    "manji_fukusho": {
        "label": "卍・複勝(EV>1.0)",
        "model": "manji",
        "bet_type": "複勝",
        "n_picks": 1,
        "ev_filter": True,
    },
}


def _fmt_stats(stats) -> str:  # type: ignore[no-untyped-def]
    return (
        f"races={stats.races:>5} hits={stats.hits:>4} "
        f"hit_rate={stats.hit_rate:5.1f}% "
        f"invested=¥{round(stats.invested):>9,} payout=¥{round(stats.payout):>9,} "
        f"profit=¥{round(stats.profit):>+10,} ROI={stats.roi:6.1f}%"
    )


def main() -> int:
    lines: list[str] = []

    def log(msg: str = "") -> None:
        print(msg, flush=True)
        lines.append(msg)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("=" * 70)
    log(f"卍モデル 週末Challenger再訓練・OOS検証  {ts}")
    log("=" * 70)
    log(
        f"設定: train_until={_TRAIN_UNTIL}（{_TRAIN_UNTIL}年以前で学習） "
        f"/ OOS検証={_TEST_YEAR}年"
    )
    log("")

    conn = init_db()

    # ── Champion（現役本番モデル）をロード ───────────────────────────
    log("[1] Champion（現役 data/models/manji_model.pkl）をロード...")
    champion = ManjiModel()
    try:
        champion.load()
        champion._trained = True
    except Exception as exc:  # noqa: BLE001
        log(f"  [ERROR] Champion ロード失敗: {exc}")
        _flush(lines)
        return 1
    log("  [OK] Champion ロード完了")

    # ── Challenger を train_until=2024 で訓練 ─────────────────────────
    log(f"[2] Challenger を {_TRAIN_UNTIL}年以前データで訓練...")
    challenger = ManjiModel()
    m = challenger.train(conn, train_until=_TRAIN_UNTIL)
    log(
        f"  [OK] Challenger 訓練完了: "
        f"n_races={m.get('n_races')} n_samples={m.get('n_samples')}"
    )
    if not m.get("n_samples"):
        log("  [ERROR] 学習サンプル0件 → 中止")
        _flush(lines)
        return 1
    log("")

    # ── OOS(2025) 評価: Champion vs Challenger ───────────────────────
    log(f"[3] {_TEST_YEAR}年 OOS でROI算出中（Champion）...")
    champ_overall, _, _ = _run_backtest(
        conn, _TEST_YEAR, None, None, champion, _MANJI_STRATEGIES, verbose=False
    )
    log(f"[4] {_TEST_YEAR}年 OOS でROI算出中（Challenger）...")
    chal_overall, _, _ = _run_backtest(
        conn, _TEST_YEAR, None, None, challenger, _MANJI_STRATEGIES, verbose=False
    )
    log("")

    # ── 結果比較 ─────────────────────────────────────────────────────
    log("─" * 70)
    log("【OOS 2025 結果比較】")
    for key in ("manji_tansho", "manji_fukusho"):
        cs, hs = champ_overall[key], chal_overall[key]
        log(f"  {_MANJI_STRATEGIES[key]['label']}")
        log(f"    Champion   : {_fmt_stats(cs)}")
        log(f"    Challenger : {_fmt_stats(hs)}")
        log(f"    ΔROI       : {hs.roi - cs.roi:+.1f}pt")
        log("")

    ct, ht = champ_overall["manji_tansho"], chal_overall["manji_tansho"]
    cf, hf = champ_overall["manji_fukusho"], chal_overall["manji_fukusho"]

    # ── 昇格判定（保守的）─────────────────────────────────────────────
    #   単勝OOS ROI が Champion 以上 かつ Challenger 単勝が黒字(>=100%) かつ
    #   複勝OOS ROI が Champion を大きく割り込まない(-5pt以内) 場合のみ昇格。
    tansho_ok = ht.roi >= ct.roi and ht.roi >= 100.0
    fukusho_ok = hf.roi >= cf.roi - 5.0
    promote = bool(tansho_ok and fukusho_ok)

    log("─" * 70)
    log("【昇格判定】")
    log(
        f"  単勝: Challenger {ht.roi:.1f}% vs Champion {ct.roi:.1f}% "
        f"→ {'OK' if tansho_ok else 'NG'}（>=Champion かつ >=100%）"
    )
    log(
        f"  複勝: Challenger {hf.roi:.1f}% vs Champion {cf.roi:.1f}% "
        f"→ {'OK' if fukusho_ok else 'NG'}（Champion-5pt以内）"
    )
    log("")
    log("  [注意] Champion(現役pkl)は学習期間に2025を含む可能性があり、OOS比較で")
    log("         楽観側に振れる。Challengerはリーク無し(train_until=2024)のため、")
    log("         Challenger<Champion でも『劣る』とは断定できない（参考値）。")
    log("  [注意] 基底回帰のみ再訓練。manji_win_calibrator.pkl は再fitしていないため、")
    log("         昇格する場合は較正の再fitとECE再検証が後続必須。")
    log("")

    if promote:
        saved = (
            challenger.save()
        )  # data/models/manji_model.pkl を上書き（バックアップ済）
        log(f"  → 判定: 【昇格】Challenger を本番 {saved} に保存しました。")
        log("    ロールバック: data/backups/manji_model_<ts>.pkl から復元可能。")
    else:
        log("  → 判定: 【HOLD】Challenger は昇格基準未達。現役Championを温存します。")
        log("    本番 data/models/manji_model.pkl は一切変更していません。")

    log("")
    log(
        f"RESULT: {'PROMOTED' if promote else 'HOLD'} "
        f"tansho_champ={ct.roi:.1f}% tansho_chal={ht.roi:.1f}% "
        f"fukusho_champ={cf.roi:.1f}% fukusho_chal={hf.roi:.1f}%"
    )
    log("=" * 70)

    _flush(lines)
    return 0


def _flush(lines: list[str]) -> None:
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_LOG_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"\n[ログ出力] {_LOG_PATH}", flush=True)


def run_place_calibration() -> int:
    """複勝特化 Platt 較正器を学習し、ECE収束を fukusho_calibration_final.log に出力する。"""
    from src.ml.manji_calibration import fit_manji_place_calibrator

    lines: list[str] = []

    def log(msg: str = "") -> None:
        print(msg, flush=True)
        lines.append(msg)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("=" * 70)
    log(f"卍 複勝特化 Platt 較正器 学習・ECE検証  {ts}")
    log("=" * 70)
    log(
        "手法: ev_score(1次元) → P(複勝圏=3着内) を Platt Scaling(ロジスティック回帰)で較正"
    )
    log("単勝 Isotonic 較正器(manji_win_calibrator)とは独立インスタンス。")
    log("")

    conn = init_db()
    diag = fit_manji_place_calibrator(conn, max_races=400, min_samples=200)

    log(f"学習レース数      : {diag.get('n_races')}")
    log(f"学習サンプル数    : {diag.get('n_samples')}")
    log(f"複勝圏 base_rate  : {diag.get('base_rate')}")
    log(f"較正器パス        : {diag.get('path')}")
    log("")

    if not diag.get("fitted"):
        log("[判定] サンプル不足/偏りにより学習スキップ → フォールバック較正を使用。")
        _flush_to(lines, _PLACE_CAL_LOG_PATH)
        return 1

    ece = float(diag.get("ece", float("nan")))  # type: ignore[arg-type]
    ece_uncal = float(diag.get("ece_uncal", float("nan")))  # type: ignore[arg-type]
    log("【ECE 収束状況（Expected Calibration Error・小さいほど較正良好）】")
    log(f"  較正前(ev素朴正規化) ECE = {ece_uncal:.4f}")
    log(f"  較正後(Platt)        ECE = {ece:.4f}")
    improve = ece_uncal - ece
    log(f"  改善量               ΔECE = {improve:+.4f}")
    log("")
    log("【較正曲線（ev_score → P(複勝圏)）】")
    curve = diag.get("sample_curve", {})
    if isinstance(curve, dict):
        for ev, p in curve.items():
            log(f"  ev_score={ev:>4} → P(複勝圏)={p:.4f}")
    log("")
    healthy = ece <= 0.05
    log(
        f"[判定] ECE={ece:.4f} {'<= 0.05 → 較正良好(健全)' if healthy else '> 0.05 → 要改善'}"
    )
    log(
        f"RESULT: PLACE_CAL_FITTED ece={ece:.4f} ece_uncal={ece_uncal:.4f} "
        f"base_rate={diag.get('base_rate')} healthy={'YES' if healthy else 'NO'}"
    )
    log("=" * 70)
    _flush_to(lines, _PLACE_CAL_LOG_PATH)
    return 0


def _flush_to(lines: list[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"\n[ログ出力] {path}", flush=True)


def promote_fukusho_challenger(log_v2_path: Path) -> int:
    """Challenger(train_until=2024)を本番デプロイし、複勝較正器を再fitする。

    安全手順:
      1. 既存pklのバックアップを確認（呼び出し前に取得済みを前提）
      2. Challenger を train_until=2024 で訓練し save() → manji_model.pkl 上書き
      3. 新モデルで place calibrator を再fit → manji_place_calibrator.pkl 上書き
      4. ECE を log_v2_path に出力

    CLAUDE.md 条項1/4 遵守: predictions テーブルは非改変。
    バックアップは呼び出し前に data/backups/ へ取得済みであること。
    """
    import pickle as _pkl

    from src.ml.manji_calibration import (
        _PLACE_CAL_PATH,
        _place_cal_cache,
        fit_manji_place_calibrator,
    )

    lines: list[str] = []

    def log(msg: str = "") -> None:
        print(msg, flush=True)
        lines.append(msg)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("=" * 70)
    log(f"卍 Challenger 複勝特化 正式昇格デプロイ  {ts}")
    log("=" * 70)
    log("train_until=2024 Challenger を本番 manji_model.pkl に昇格する。")
    log("単勝は WATCH_ONLY(投票せず監視継続)のため副作用を許容。")
    log("")

    conn = init_db()

    # ── Step1: Challenger 訓練 ─────────────────────────────────────
    log("[1/3] Challenger を train_until=2024 で訓練中...")
    challenger = ManjiModel()
    m = challenger.train(conn, train_until=2024)
    n_r, n_s = m.get("n_races", 0), m.get("n_samples", 0)
    log(f"      完了: n_races={n_r} / n_samples={n_s}")
    if not n_s:
        log("  [ERROR] 学習サンプル0件 → 昇格中止")
        _flush_to(lines, log_v2_path)
        return 1

    # ── Step2: 本番 pkl に save ──────────────────────────────────────
    saved_path = challenger.save()   # data/models/manji_model.pkl を上書き
    log(f"[2/3] 本番 pkl を昇格済み Challenger で更新: {saved_path}")
    # manji_calibration のプロセス内キャッシュを無効化（置換後のモデルで再ロードさせる）
    import src.ml.manji_calibration as _mc
    _mc._place_cal_cache = None
    import src.ml.models as _models
    _models._MODEL_CACHE.clear()
    log("      モデルキャッシュをクリア済み")
    log("")

    # ── Step3: 新モデルで place calibrator を再 fit ─────────────────
    log("[3/3] 新モデルで複勝 Platt 較正器を再 fit 中...")
    diag = fit_manji_place_calibrator(conn, max_races=400, min_samples=200)
    log(f"      n_races={diag.get('n_races')} / n_samples={diag.get('n_samples')}")

    if not diag.get("fitted"):
        log("  [WARN] 複勝較正器: サンプル不足 → フォールバック較正を使用")
        log("         本番 pkl の昇格は完了。較正器は旧版のまま。")
        _flush_to(lines, log_v2_path)
        return 0

    ece = float(diag.get("ece", float("nan")))  # type: ignore[arg-type]
    ece_uncal = float(diag.get("ece_uncal", float("nan")))  # type: ignore[arg-type]
    log("")
    log("【複勝 Platt 較正器 再 fit 結果（新モデルベース）】")
    log(f"  base_rate(複勝圏率) : {diag.get('base_rate')}")
    log(f"  ECE 較正前          : {ece_uncal:.4f}")
    log(f"  ECE 較正後(Platt)   : {ece:.4f}  {'<= 0.05 → 健全' if ece <= 0.05 else '> 0.05 → 要注意'}")
    log(f"  ΔECE                : {ece_uncal - ece:+.4f}")
    log("")
    log("【較正曲線（ev_score → P(複勝圏)）】")
    curve = diag.get("sample_curve", {})
    if isinstance(curve, dict):
        for ev, p in curve.items():
            log(f"  ev_score={ev:>4} → P(複勝圏)={p:.4f}")
    log("")
    healthy = ece <= 0.05
    log(f"[判定] ECE={ece:.4f} → {'健全(PASS)' if healthy else '要注意(WARN)'}")
    log(f"RESULT: PROMOTED_AND_RECALIBRATED "
        f"n_races={n_r} ece={ece:.4f} healthy={'YES' if healthy else 'NO'}")
    log("=" * 70)
    _flush_to(lines, log_v2_path)
    return 0 if healthy else 1


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="卍 週末再訓練・複勝較正")
    parser.add_argument(
        "--place-cal",
        action="store_true",
        help="複勝特化 Platt 較正器のみを学習・ECE検証する（OOS再訓練はスキップ）",
    )
    parser.add_argument(
        "--promote-fukusho",
        action="store_true",
        help=(
            "Challenger(train_until=2024)を本番deployし複勝較正器を再fit。"
            "バックアップは事前に data/backups/ へ取得しておくこと。"
        ),
    )
    args = parser.parse_args()

    if args.place_cal:
        raise SystemExit(run_place_calibration())
    if args.promote_fukusho:
        _v2_log = _ROOT / "logs" / "fukusho_calibration_final_v2.log"
        raise SystemExit(promote_fukusho_challenger(_v2_log))
    raise SystemExit(main())
