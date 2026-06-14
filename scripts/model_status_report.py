"""
scripts/model_status_report.py — モデル構造の可視化レポート（W-096 Task3）

ブラックボックス化を防ぐため、現行モデルの構造・重み・特徴量寄与・OOS比較を
1 本にまとめて `docs/model_status_report.md`（既定）へ出力する。

内容:
  1. 3連系「本気」アンサンブル（gachi_trifecta）の構成と選定ロジック。
  2. V2 特徴量重要度（LightGBM・新特徴量 prev_trouble_proxy の寄与を含む）。
  3. OOS（未知データ）でのアブレーション比較:
       BASE / BASE+prev_trouble_proxy単体 / BASE+全新特徴量
     を AUC・ROI・的中率で比較し、過学習と回収率改善の有無を厳格に判定する。

使い方::
    py scripts/model_status_report.py --cutoff 2026-03-01 --train-cap 1800 --test-cap 800
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402

from scripts.backtest_v2_oos import (  # noqa: E402
    BASE_COLS,
    NEW_COLS,
    _assemble,
    _evaluate,
    _race_ids,
)
from src.features.pedigree_te import SireEncoder  # noqa: E402


def _train(train: pd.DataFrame, cols: list[str]):
    """LightGBM を学習して (model, used_cols) を返す。"""
    import lightgbm as lgb

    xtr = train[cols].apply(pd.to_numeric, errors="coerce").astype(float)
    ytr = train["is_win"].astype(int)
    model = lgb.LGBMClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        verbose=-1,
    )
    model.fit(xtr, ytr)
    return model


def _predict_into(model, test: pd.DataFrame, cols: list[str], prob_col: str) -> None:
    xte = test[cols].apply(pd.to_numeric, errors="coerce").astype(float)
    test[prob_col] = model.predict_proba(xte)[:, 1]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", default="2026-03-01")
    ap.add_argument("--train-lo", default="2024-01-01")
    ap.add_argument("--test-hi", default="2026-07-01")
    ap.add_argument("--train-cap", type=int, default=1800)
    ap.add_argument("--test-cap", type=int, default=800)
    ap.add_argument("--ev", type=float, default=1.0)
    ap.add_argument("--out", default="docs/model_status_report.md")
    args = ap.parse_args()

    conn = sqlite3.connect(str(_ROOT / "data" / "umalogi.db"))
    try:
        enc = SireEncoder().fit(conn, cutoff_date=args.cutoff, surface="芝")
        train_ids = _race_ids(conn, args.train_lo, args.cutoff, args.train_cap)
        test_ids = _race_ids(conn, args.cutoff, args.test_hi, args.test_cap)
        print(f"train races={len(train_ids)} / test races={len(test_ids)} 組立中...")
        train = _assemble(conn, train_ids, enc)
        test = _assemble(conn, test_ids, enc)
        print(f"train rows={len(train)} / test rows={len(test)}")

        trouble_only = BASE_COLS + ["prev_trouble_proxy"]
        full = BASE_COLS + NEW_COLS

        variants: dict[str, list[str]] = {
            "BASE": BASE_COLS,
            "BASE+prev_trouble_proxy": trouble_only,
            "BASE+全新特徴量": full,
        }
        results: dict[str, dict] = {}
        for name, cols in variants.items():
            m = _train(train, cols)
            pc = f"p_{name}"
            _predict_into(m, test, cols, pc)
            results[name] = _evaluate(test, pc, args.ev)

        # 全特徴量モデルの重要度
        full_model = _train(train, full)
        imp = (
            pd.DataFrame(
                {"feature": full, "importance": full_model.feature_importances_}
            )
            .sort_values("importance", ascending=False)
            .reset_index(drop=True)
        )
        imp["share_%"] = imp["importance"] / imp["importance"].sum() * 100
        trouble_rank = int(imp.index[imp["feature"] == "prev_trouble_proxy"][0]) + 1
        trouble_share = float(
            imp.loc[imp["feature"] == "prev_trouble_proxy", "share_%"].iloc[0]
        )

        # ── gachi_trifecta 構成（コードから取得）──
        import src.ml.gachi_trifecta as gt

        lines: list[str] = []
        a = lines.append
        a("# UMALOGI モデル構造レポート（model_status_report）\n")
        a(f"> 生成日時: {pd.Timestamp.now():%Y-%m-%d %H:%M:%S}  ")
        a(
            f"OOS cutoff: {args.cutoff}（train {args.train_lo}〜{args.cutoff} / test {args.cutoff}〜{args.test_hi}）\n"
        )

        a("## 1. 3連系「本気」アンサンブル（gachi_trifecta / W-095）\n")
        a(
            "買い目を当てにいく 3 連系エンジン。単一スコアではなく**役割の異なる 2 系統を"
        )
        a("アンサンブル**して軸と紐を選び分ける。\n")
        a("| 役割 | 選定ロジック | 重み/設定 |")
        a("|------|------------|-----------|")
        a(
            f"| 軸（担保） | 本命勝率ランク + 複勝率ランクの**合算が小さい**上位馬 | 上位 `_N_AXIS={gt._N_AXIS}` 頭 |"
        )
        a(
            f"| 紐（妙味穴） | 軸を除き**卍 EV 降順**（人気の盲点） | 上位 `_N_PARTNERS={gt._N_PARTNERS}` 頭 |"
        )
        a(
            "| 確率推定 | Harville 公式（本命勝率ベクトルを正規化） | 馬連/馬単/三連複/三連単 |"
        )
        a(
            "| EV 付与 | `OddsEstimator.ev`（軸単勝オッズ×券種スケール） | 券種別 EV キャップ |"
        )
        a("\n点数キャップ（過剰購入の抑制）:\n")
        a(
            f"- 馬連 `_MAX_UMAREN={gt._MAX_UMAREN}` / 馬単 `_MAX_UMATAN={gt._MAX_UMATAN}` / "
            f"三連複 `_MAX_TRIO={gt._MAX_TRIO}` / 三連単 `_MAX_TRIFECTA={gt._MAX_TRIFECTA}`\n"
        )
        a(
            "> 注: アンサンブルの「重み」は固定の係数ではなく**ランク合算（軸）と EV 降順（紐）という"
        )
        a("> 選定規則**で表現される。軸＝的中率の担保、紐＝回収率の妙味、の役割分担。")
        a("> 実弾単複ロックの外（記録専用 model_type `本気3連系`）。\n")

        a("## 2. V2 特徴量重要度（LightGBM・BASE+全新特徴量モデル）\n")
        a(
            "> 重要度は LightGBM の split gain ベース。新特徴量 `prev_trouble_proxy` の寄与位置に注目。\n"
        )
        a("| 順位 | 特徴量 | importance | シェア% |")
        a("|----:|--------|-----------:|-------:|")
        for i, row in imp.head(20).iterrows():
            mark = " ⭐(新)" if row["feature"] == "prev_trouble_proxy" else ""
            a(
                f"| {i + 1} | {row['feature']}{mark} | {int(row['importance'])} | {row['share_%']:.1f}% |"
            )
        a(
            f"\n**新特徴量 `prev_trouble_proxy` の順位 = 全{len(full)}列中 第{trouble_rank}位（シェア {trouble_share:.1f}%）**\n"
        )

        a("## 3. OOS アブレーション比較（旧=BASE vs 新特徴量）\n")
        a(
            "単勝 EV>1.0 フラットベット（100円）での未知データ回収率。過学習は test AUC で監視。\n"
        )
        a("| モデル | AUC(test) | ROI(%) | 的中率(%) | ベット数 |")
        a("|--------|----------:|-------:|---------:|--------:|")
        for name, r in results.items():
            a(
                f"| {name} | {r['auc']:.4f} | {r['roi']:.1f} | {r['hit']:.1f} | {r['n_bets']} |"
            )
        base_roi = results["BASE"]["roi"]
        full_roi = results["BASE+全新特徴量"]["roi"]
        tro_roi = results["BASE+prev_trouble_proxy"]["roi"]
        a("")
        a("### 判定\n")
        verdict_full = "改善" if full_roi > base_roi else "改善せず"
        verdict_tro = "改善" if tro_roi > base_roi else "改善せず"
        a(
            f"- 全新特徴量: ROI {base_roi:.1f}% → {full_roi:.1f}%（**{verdict_full}**・差 {full_roi - base_roi:+.1f}pp）"
        )
        a(
            f"- prev_trouble_proxy 単体: ROI {base_roi:.1f}% → {tro_roi:.1f}%（**{verdict_tro}**・差 {tro_roi - base_roi:+.1f}pp）"
        )
        a("")
        a("### 結論・本番反映の可否\n")
        if full_roi <= base_roi and tro_roi <= base_roi:
            a(
                "- OOS で**回収率の改善が確認できない**ため、新特徴量を本番モデルへ投入**しない**"
            )
            a(
                "  （安全第一・未検証オーバーレイ禁止の方針／条項5「完了は数値改善確認時のみ」）。"
            )
            a(
                "- ライブ V1/V2 モデルは **base FEATURE_COLS（不変）** を使用するため、本特徴量追加による"
            )
            a(
                "  本番推論への影響は無い（`prev_trouble_proxy` は PRERUN/LEAKFREE 経由で研究系のみ）。"
            )
            a(
                "- 本特徴量はコード上に保持し、設計見直し（閾値再調整・他特徴量との交互作用）後に再検証する。"
            )
        else:
            a(
                "- OOS で改善が確認できたため、V2 系への正式投入を次段階で検討する（要・複数 cutoff 追試）。"
            )
        a("")

        out_path = _ROOT / args.out
        out_path.write_text("\n".join(lines), encoding="utf-8")
        print(f"\nレポート出力: {out_path}")
        print("\n".join(lines))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
