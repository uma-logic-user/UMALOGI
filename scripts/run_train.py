"""
UMALOGI AI -- ベースライン学習スクリプト

HonmeiModel（本命・的中率特化）と ManjiModel（卍・回収率特化）を
SQLite DB から学習し、data/models/ へ保存する。

使用例:
    py scripts/run_train.py
    py scripts/run_train.py --db data/custom.db
    py scripts/run_train.py --top-n 15          # 特徴量重要度の表示件数
    py scripts/run_train.py --verbose           # DEBUG ログを表示
    py scripts/run_train.py --dry-run           # DB統計のみ表示（学習なし）
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
import time
from pathlib import Path

# ── プロジェクトルートを sys.path に追加 ──────────────────────────
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import get_db_path, init_db
from src.ml.models import (
    FEATURE_COLS,
    HonmeiModel,
    ManjiModel,
    _MODEL_DIR,
)

logger = logging.getLogger(__name__)

# 本命モデルの CV AUC 目標値（ml_guidelines.md 準拠）
_AUC_TARGET = 0.70
_WIDTH      = 62


# ════════════════════════════════════════════════════════════════
#  表示ユーティリティ（ASCII のみ使用 — Windows cp932 対応）
# ════════════════════════════════════════════════════════════════

def _banner(text: str) -> None:
    """タイトルバナーを表示する。"""
    inner  = f"  {text}  "
    pad    = max(0, _WIDTH - 2 - len(inner))
    left   = pad // 2
    right  = pad - left
    border = "=" * _WIDTH
    print(f"\n{border}")
    print(f"|{' ' * left}{inner}{' ' * right}|")
    print(f"{border}")


def _section(text: str) -> None:
    """セクション区切りを表示する。"""
    print(f"\n{'- ' * (_WIDTH // 2)}")
    print(f"  {text}")
    print(f"{'- ' * (_WIDTH // 2)}")


def _kv_table(rows: list[tuple[str, str]], title: str = "") -> None:
    """キー・バリューの2列テーブルを表示する。"""
    if title:
        print(f"\n  {title}")
    col_w = max(len(k) for k, _ in rows) + 2
    val_w = max(len(v) for _, v in rows) + 2
    sep   = f"  +{'-' * col_w}+{'-' * val_w}+"
    print(sep)
    for i, (k, v) in enumerate(rows):
        # 最終行の前（達成フラグ行）に区切り線を入れる
        if i > 0 and i == len(rows) - 1:
            print(sep)
        print(f"  | {k:<{col_w - 2}} | {v:>{val_w - 2}} |")
    print(sep)


def _feat_imp_table(pairs: list[tuple[str, int]], title: str = "") -> None:
    """特徴量重要度テーブルを表示する。"""
    if title:
        print(f"\n  {title}")
    feat_w  = max(len(f) for f, _ in pairs) + 2
    gain_w  = max(len(str(g)) for _, g in pairs) + 2
    _BAR    = 20
    max_g   = pairs[0][1] if pairs else 1

    header = f"  +{'- ' * 2}+{'-' * feat_w}+{'-' * gain_w}+{'-' * (_BAR + 2)}+"
    print(header)
    print(
        f"  | {'No':<2} | {'Feature':<{feat_w - 2}} | {'Gain':>{gain_w - 2}} | {'Bar':<{_BAR}} |"
    )
    print(header)
    for i, (feat, gain) in enumerate(pairs, 1):
        bar_len = math.ceil(gain / max_g * _BAR) if max_g > 0 else 0
        bar     = "#" * bar_len
        print(
            f"  | {i:>2} | {feat:<{feat_w - 2}} | {gain:>{gain_w - 2}} | {bar:<{_BAR}} |"
        )
    print(header)


# ════════════════════════════════════════════════════════════════
#  DB 統計
# ════════════════════════════════════════════════════════════════

def _db_stats(conn) -> dict[str, int]:
    """レース数・出走結果数などの基本統計を返す。"""
    (n_races,)   = conn.execute("SELECT COUNT(*) FROM races").fetchone()
    (n_results,) = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE rank IS NOT NULL"
    ).fetchone()
    (n_payouts,) = conn.execute("SELECT COUNT(*) FROM race_payouts").fetchone()
    return {"races": n_races, "results": n_results, "payouts": n_payouts}


# ════════════════════════════════════════════════════════════════
#  学習ルーティン
# ════════════════════════════════════════════════════════════════

def _train_honmei(
    conn,
    model_dir: Path,
    top_n: int,
    train_until: int | None = None,
) -> tuple[HonmeiModel, dict]:
    """本命モデルを学習・保存し、結果 dict を返す。"""
    print(f"\n[1/2] 本命モデル (HonmeiModel) を学習中 ...")
    t0 = time.perf_counter()

    honmei = HonmeiModel()
    result = honmei.train(conn, train_until=train_until)
    elapsed = time.perf_counter() - t0

    if result["n_races"] == 0:
        print("  [!] 学習データが 0 件のためスキップしました。")
        return honmei, result

    # ── 特徴量重要度テーブル ──────────────────────────────
    importances = honmei._model.feature_importances_
    pairs: list[tuple[str, int]] = sorted(
        zip(FEATURE_COLS, importances.tolist()),
        key=lambda t: t[1],
        reverse=True,
    )[:top_n]
    _feat_imp_table(pairs, title=f"特徴量重要度 Top{top_n}")

    # ── スコアサマリーテーブル ────────────────────────────
    cv_mean = result.get("cv_auc_mean", float("nan"))
    cv_std  = result.get("cv_auc_std",  float("nan"))
    ok      = (not math.isnan(cv_mean)) and cv_mean >= _AUC_TARGET
    mark    = f"[OK] {cv_mean:.4f} >= {_AUC_TARGET}" if ok else f"[NG] {cv_mean:.4f} < {_AUC_TARGET} (目標未達)"
    until_str = str(result.get("train_until")) if result.get("train_until") is not None else "全期間"

    _kv_table(
        [
            ("学習期間",           until_str + "年以前" if until_str != "全期間" else until_str),
            ("学習レース数",       f"{result['n_races']:,}"),
            ("学習サンプル数",     f"{result['n_samples']:,}"),
            ("CV AUC (mean)",      f"{cv_mean:.4f}" if not math.isnan(cv_mean) else "N/A"),
            ("CV AUC (std)",       f"{cv_std:.4f}"  if not math.isnan(cv_std)  else "N/A"),
            ("AUC 目標 (> 0.70)", mark),
        ],
        title="学習結果",
    )

    # ── 保存 ─────────────────────────────────────────────
    save_path = honmei.save(model_dir / "honmei_model.pkl")
    print(f"\n  [OK] 保存完了: {save_path}  ({elapsed:.1f}s)")
    return honmei, result


def _train_manji(
    conn,
    model_dir: Path,
    train_until: int | None = None,
) -> tuple[ManjiModel, dict]:
    """卍モデルを学習・保存し、結果 dict を返す。"""
    print(f"\n[2/2] 卍モデル (ManjiModel) を学習中 ...")
    t0 = time.perf_counter()

    manji   = ManjiModel()
    result  = manji.train(conn, train_until=train_until)
    elapsed = time.perf_counter() - t0

    if result["n_races"] == 0:
        print("  [!] 学習データが 0 件のためスキップしました。")
        return manji, result

    until_str_m = str(result.get("train_until")) if result.get("train_until") is not None else "全期間"
    _kv_table(
        [
            ("学習期間",       until_str_m + "年以前" if until_str_m != "全期間" else until_str_m),
            ("学習レース数",   f"{result['n_races']:,}"),
            ("学習サンプル数", f"{result['n_samples']:,}"),
        ],
        title="学習結果",
    )

    save_path = manji.save(model_dir / "manji_model.pkl")
    print(f"\n  [OK] 保存完了: {save_path}  ({elapsed:.1f}s)")
    return manji, result


# ════════════════════════════════════════════════════════════════
#  エントリポイント
# ════════════════════════════════════════════════════════════════

def main() -> int:
    parser = argparse.ArgumentParser(
        description="UMALOGI AI ベースライン学習スクリプト",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="DB ファイルパス（デフォルト: DB_PATH 環境変数 or data/umalogi.db）",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        dest="top_n",
        help="特徴量重要度の表示件数（デフォルト: 10）",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="DEBUG レベルのログを表示する",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="DB 統計のみ表示して終了（学習なし）",
    )
    parser.add_argument(
        "--train-until",
        type=int,
        default=None,
        dest="train_until",
        help="学習に使う最終年 (例: 2023 → 2023年以前のみ学習。アウト・オブ・サンプル評価用)",
    )
    args = parser.parse_args()

    # ── Windows ターミナルを UTF-8 出力に切り替える ──────
    # cp932 (Shift-JIS) のまま日本語を print するとガーベージになるため、
    # reconfigure() で UTF-8 に切り替える（Python 3.7+）。
    # ターミナル側で `chcp 65001` またはWindows Terminal / VS Code を使うと
    # 日本語が正しく表示される。
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    # ── ロギング設定 ─────────────────────────────────────
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # lightgbm の冗長ログを抑制（モデル params に verbose=-1 指定済みだが念のため）
    logging.getLogger("lightgbm").setLevel(logging.WARNING)

    # ── バナー ───────────────────────────────────────────
    _banner("UMALOGI AI  --  Baseline Training")

    # ── DB 接続 ──────────────────────────────────────────
    db_path = args.db or get_db_path()
    print(f"\n  DB  : {db_path}")

    if not Path(db_path).exists():
        print(f"\n  [NG] DB ファイルが見つかりません: {db_path}")
        print("       先に fetch_all_masters.bat や src/scraper/ でデータを取得してください。")
        return 1

    conn   = init_db(db_path=Path(db_path))
    stats  = _db_stats(conn)
    print(
        f"  Data: races={stats['races']:,} / results={stats['results']:,}"
        f" / payouts={stats['payouts']:,}"
    )

    if stats["results"] == 0:
        print("\n  [NG] race_results が 0 件です。学習データがありません。")
        conn.close()
        return 1

    if args.dry_run:
        print("\n  --dry-run: DB 統計のみ表示して終了します。")
        conn.close()
        return 0

    # ── モデル保存先 ─────────────────────────────────────
    model_dir = _MODEL_DIR
    model_dir.mkdir(parents=True, exist_ok=True)
    print(f"  保存先: {model_dir}")

    _section("学習フェーズ開始")
    wall_start = time.perf_counter()

    # ── 本命モデル ───────────────────────────────────────
    if args.train_until:
        print(f"  学習期間: {args.train_until}年以前（アウト・オブ・サンプル分割）")
    try:
        _, h_result = _train_honmei(conn, model_dir, top_n=args.top_n, train_until=args.train_until)
    except Exception as exc:
        logger.exception("本命モデルの学習中にエラーが発生しました: %s", exc)
        conn.close()
        return 1

    # ── 卍モデル ─────────────────────────────────────────
    try:
        _, m_result = _train_manji(conn, model_dir, train_until=args.train_until)
    except Exception as exc:
        logger.exception("卍モデルの学習中にエラーが発生しました: %s", exc)
        conn.close()
        return 1

    conn.close()

    # ── 最終サマリー ─────────────────────────────────────
    elapsed_total = time.perf_counter() - wall_start
    mins, secs    = divmod(int(elapsed_total), 60)
    elapsed_str   = f"{mins}m {secs:02d}s" if mins else f"{secs}s"

    _banner(f"Training Complete  (elapsed: {elapsed_str})")

    print("\n  Next steps:")
    print("    Backtest  :  py src/simulate_year.py --year 2024")
    print("    Predict   :  py src/main_pipeline.py")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
