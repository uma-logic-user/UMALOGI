"""
scripts/recompute_today_shadow.py — 本日レースの 5月末ポリシー シャドー再計算（Task 4）

DB 内に既に保存済みの「本日（または指定日）のレースデータ」のみを用いて、全モデル×全券種の
予想を再計算し、別名 model_id ``{base}_v0525(再計算)`` で predictions へ **追記 INSERT** する。
外部 API 通信は一切行わない（FeatureBuilder + 学習済みモデルで DB 内データのみを使用）。

既存の本日分 live 予想（``卍(直前)`` 等）は別 model_type のため一切変更されない（条項1）。
処理の最後に「もし本復活ロジック×全券種で運用していた場合の仮想 ROI・収支サマリー」を出力する。

使い方:
    py scripts/recompute_today_shadow.py                # 本日(localtime)を再計算
    py scripts/recompute_today_shadow.py --date 2026-06-13
    py scripts/recompute_today_shadow.py --no-save      # 生成のみ（DB 書き込みなし・確認用）
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

# scripts/ から実行されるため src を import path に追加
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]

from src.analysis.shadow_recompute import (  # noqa: E402
    OddsEstimator,
    recompute_race,
    virtual_roi_summary,
)
from src.database.init_db import init_db  # noqa: E402
from src.ml.bet_generator import get_current_bankroll  # noqa: E402


def _today_local() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="本日レースの5月末ポリシー シャドー再計算"
    )
    parser.add_argument("--date", default=None, help="対象日 YYYY-MM-DD（既定: 本日）")
    parser.add_argument(
        "--no-save", action="store_true", help="生成のみで DB に書き込まない"
    )
    args = parser.parse_args(argv)

    target_date = args.date or _today_local()
    save = not args.no_save

    conn = init_db()
    try:
        race_ids = [
            r[0]
            for r in conn.execute(
                "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
                (target_date,),
            ).fetchall()
        ]
        print(f"=== シャドー再計算 (5月末ポリシー / _v0525) — {target_date} ===")
        print(
            f"対象レース: {len(race_ids)} 件  保存={'ON' if save else 'OFF(確認のみ)'}\n"
        )
        if not race_ids:
            print("対象レースが DB にありません。終了します。")
            return 0

        estimator = OddsEstimator(conn)
        bankroll = get_current_bankroll(conn)

        n_ok = 0
        n_skip = 0
        total_bets = 0
        for race_id in race_ids:
            res = recompute_race(
                conn, race_id, estimator=estimator, bankroll=bankroll, save=save
            )
            if res.skipped_reason:
                n_skip += 1
                continue
            n_ok += 1
            total_bets += res.total_bets
        print(
            f"再計算完了: 成功 {n_ok} / スキップ {n_skip} / 生成買い目 {total_bets} 件\n"
        )

        # ── 仮想 ROI サマリ（確定結果のあるレースのみ）──────────────────
        rows, settled = virtual_roi_summary(conn, race_ids)
        print("=== 仮想回収率サマリー（本復活ロジック×全券種で運用した場合）===")
        print(f"確定結果のあるレース: {settled} / {len(race_ids)} 件")
        if not rows:
            print(
                "→ 本日の確定結果がまだ無いため仮想 ROI は未確定です"
                "（レース確定後に再実行してください）。"
            )
            return 0

        # モデル別 → 券種別に整形出力
        by_model: dict[str, list] = {}
        for (model, _bt), row in sorted(rows.items()):
            by_model.setdefault(model, []).append(row)

        grand_inv = 0.0
        grand_pay = 0.0
        header = f"{'モデル/券種':<28}{'点数':>6}{'投資':>12}{'払戻':>12}{'収支':>12}{'ROI':>9}"
        print(header)
        print("-" * len(header))
        for model in sorted(by_model):
            m_inv = m_pay = 0.0
            for row in by_model[model]:
                grand_inv += row.invested
                grand_pay += row.payout
                m_inv += row.invested
                m_pay += row.payout
                print(
                    f"  {model}/{row.bet_type:<18}{row.n_bets:>6}"
                    f"{row.invested:>12,.0f}{row.payout:>12,.0f}"
                    f"{row.profit:>+12,.0f}{row.roi:>8.1f}%"
                )
            m_roi = (m_pay / m_inv * 100.0) if m_inv > 0 else 0.0
            print(
                f"{model + ' 計':<28}{'':>6}{m_inv:>12,.0f}{m_pay:>12,.0f}"
                f"{m_pay - m_inv:>+12,.0f}{m_roi:>8.1f}%"
            )
            print()

        grand_roi = (grand_pay / grand_inv * 100.0) if grand_inv > 0 else 0.0
        print("=" * len(header))
        print(
            f"{'総合計':<28}{'':>6}{grand_inv:>12,.0f}{grand_pay:>12,.0f}"
            f"{grand_pay - grand_inv:>+12,.0f}{grand_roi:>8.1f}%"
        )
        if grand_roi < 100.0:
            print(
                "\n⚠️ 仮想 ROI が 100% 未満（赤字）です。多券種は控除率＋点数増で構造的に"
                "不利であり、5月末高 ROI 数値の多くはバックテスト/OOS 値であった点に留意。"
            )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
