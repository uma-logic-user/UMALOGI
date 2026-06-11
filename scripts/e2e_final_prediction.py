"""全機能統合 E2E テスト — 「最終予想出力」の生成。

リーク修正済み AccuracyModelV2（勝率推計）× 見送り判定（no_bet_filter）×
全券種 EV オプティマイザ（all_ticket_optimizer）を結合し、過去の実レースに対して

  1. SNS 公開用の無料予想テキスト（盾）
  2. サブスク用の詳細分析（全券種の推奨買い目と EV 値・フォーメーション）

を outputs/e2e/<race_id>/ に出力する。学習は ablation キャッシュ
（scripts/ablation_leak_audit.py が生成）を再利用して高速化。

Usage:
    py scripts/e2e_final_prediction.py --race-id 202605021211
    py scripts/e2e_final_prediction.py            # 直近の確定レースを自動選択
"""

from __future__ import annotations

import argparse
import pickle
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_DB_PATH = _ROOT / "data" / "umalogi.db"
_CACHE = _ROOT / "data" / "ablation_cache.pkl"
_OUT_DIR = _ROOT / "outputs" / "e2e"


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    import warnings

    warnings.filterwarnings("ignore")
    ap = argparse.ArgumentParser()
    ap.add_argument("--race-id", default=None)
    args = ap.parse_args()

    import numpy as np

    from scripts.ablation_leak_audit import _build_frame
    from src.marketing.sns_generator import pick_killer_phrase
    from src.ml.accuracy_model_v2 import AccuracyModelV2
    from src.ml.all_ticket_optimizer import (
        OptimizerConfig,
        build_formation,
        scan_all_tickets,
    )
    from src.ml.ev_features import MarketProbabilityCalc
    from src.ml.market_blend_calibration import blend_with_market
    from src.ml.no_bet_filter import evaluate_no_bet
    from src.utils.text import ensure_clean

    conn = sqlite3.connect(str(_DB_PATH))
    try:
        rid = args.race_id
        if not rid:
            rid = conn.execute(
                """SELECT r.race_id FROM races r
                   WHERE EXISTS (SELECT 1 FROM race_results rr
                                 WHERE rr.race_id=r.race_id AND rr.rank>0
                                   AND rr.win_odds>1.0)
                   ORDER BY r.date DESC, r.race_id DESC LIMIT 1"""
            ).fetchone()[0]
        meta = conn.execute(
            "SELECT venue, race_number, race_name, date, condition "
            "FROM races WHERE race_id=?",
            (rid,),
        ).fetchone()
        venue, rno, rname, rdate, cond = (
            (meta[0] or "", meta[1] or 0, ensure_clean(meta[2] or ""), meta[3], meta[4])
            if meta
            else ("", 0, "", "", None)
        )
        print(f"[E2E] 対象: {rid} {venue}{rno}R {rname} ({rdate})")

        # ── Step1: 勝率推計（リーク修正済み AccuracyModelV2）────────────────
        if not _CACHE.exists():
            print(
                "⚠️ ablation キャッシュ未生成 → 先に scripts/ablation_leak_audit.py を実行"
            )
            return 1
        with open(_CACHE, "rb") as f:
            train_df, _ = pickle.load(f)
        model = AccuracyModelV2()
        model.fit(train_df, train_df["is_win"].astype(int).to_numpy())
        print(f"  AccuracyModelV2 学習: {len(train_df)}行 / 閾値={model.threshold:.3f}")

        race_df = _build_frame(conn, [rid], "2025-10-01")
        if race_df.empty:
            print("⚠️ 対象レースの特徴量を構築できません")
            return 1
        raw_probs = model.predict_proba(race_df)
        numbers = [int(n) for n in race_df["horse_number"].tolist()]
        odds = [float(o) for o in race_df["win_odds"].tolist()]
        # AccV2 は未較正(class_weight=balanced)で確率が過大 → v1.7.2 でOOS実証済みの
        # 市場アンカー型ブレンドを適用し、EVの幻影的暴騰(W-066系)を構造的に抑止する
        probs = np.array(
            [blend_with_market(float(p), o) for p, o in zip(raw_probs, odds)]
        )
        names_rows = conn.execute(
            "SELECT horse_number, horse_name FROM race_results WHERE race_id=?", (rid,)
        ).fetchall()
        name_map = {int(hn): ensure_clean(nm or "") for hn, nm in names_rows}

        # ── Step2: 見送り判定 ───────────────────────────────────────────────
        decision = evaluate_no_bet(
            rid, odds, model_win_probs=list(probs), condition=cond
        )
        print(
            f"  見送り判定: skip={decision.skip} chaos={decision.chaos_score} "
            f"理由={decision.reasons or 'なし'}"
        )

        # ── Step3: 全券種 EV スキャン ───────────────────────────────────────
        market = MarketProbabilityCalc().compute_shin_probabilities(np.array(odds))
        cands = scan_all_tickets(
            numbers,
            probs,
            market_win_probs=market,
            config=OptimizerConfig(ev_min=1.20, max_bets=8),
        )
        formations = build_formation(cands)
        print(
            f"  全券種スキャン: 高EV候補 {len(cands)}点 / フォーメーション {len(formations)}型"
        )

        # ── Step4: 出力生成 ─────────────────────────────────────────────────
        out = _OUT_DIR / rid
        out.mkdir(parents=True, exist_ok=True)
        order = np.argsort(-probs)
        top3 = [(numbers[i], name_map.get(numbers[i], ""), probs[i]) for i in order[:3]]

        race_label = f"{venue}{rno}R" + (f"（{rname}）" if rname else "")
        free_lines = [
            f"🐴 AI 無料予想 — {race_label}",
            "",
        ]
        if decision.skip:
            free_lines += [
                "⚠️ このレースは AI が「見送り」判定。",
                f"理由: {' / '.join(decision.reasons)}",
                "買わない勇気も投資です。",
            ]
        else:
            for mark, (num, nm, p) in zip("◎○▲", top3):
                free_lines.append(f"{mark} {num}番 {nm}（AI勝率 {p * 100:.0f}%）")
        free_lines += [
            "",
            f"💡 {pick_killer_phrase(str(rdate))}",
            "",
            "🔑 全券種のEV分析と買い目は購読者限定で公開中。",
        ]
        (out / "sns_free.md").write_text("\n".join(free_lines) + "\n", encoding="utf-8")

        sub_lines = [
            f"# サブスク限定 詳細分析 — {race_label}（{rdate}）",
            "",
            f"- 見送り判定: {'⚠️ 見送り' if decision.skip else '✅ 参戦'}"
            f"（chaos={decision.chaos_score}）",
            "",
            "## AI 勝率上位",
            "",
        ]
        for num, nm, p in top3:
            sub_lines.append(f"- {num}番 {nm}: 勝率 {p * 100:.1f}%")
        sub_lines += ["", "## 全券種 高EV 買い目（市場の歪み抽出）", ""]
        if cands:
            sub_lines.append("| 券種 | 買い目 | モデル確率 | 想定オッズ | EV |")
            sub_lines.append("|---|---|---|---|---|")
            for c in cands:
                sub_lines.append(
                    f"| {c.bet_type} | {c.label} | {c.prob * 100:.2f}% "
                    f"| {c.odds:.1f}倍 | **{c.ev:.2f}** |"
                )
            sub_lines += ["", "## フォーメーション要約", ""]
            for f_ in formations:
                axis = "-".join(map(str, f_.axis)) or "なし"
                partners = "-".join(map(str, f_.partners)) or "—"
                sub_lines.append(
                    f"- **{f_.bet_type}** 軸[{axis}] × 相手[{partners}] "
                    f"{f_.n_points}点（EV合計 {f_.total_ev}）"
                )
        else:
            sub_lines.append("市場に十分な歪みなし → 全券種見送り（EV≥1.20 該当なし）")
        (out / "subscription_detail.md").write_text(
            "\n".join(sub_lines) + "\n", encoding="utf-8"
        )

        print(f"  出力: {out / 'sns_free.md'}")
        print(f"        {out / 'subscription_detail.md'}")
        print("[E2E] ✅ 完了")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
