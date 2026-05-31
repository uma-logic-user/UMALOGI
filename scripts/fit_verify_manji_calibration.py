# -*- coding: utf-8 -*-
"""
卍較正器の学習 + ウォークフォワード検証（Phase 3 仕上げ）

1) fit_manji_win_calibrator() で本番用 pkl を学習。
2) 確定実績(卍 単勝)を時系列で train/test 分割し、train で学習した Isotonic を
   test に適用して「予測EV ≒ 実現倍率」と confidence 飽和解消を実測検証する。

実行: py scripts/fit_verify_manji_calibration.py
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db
from src.ml.manji_calibration import fit_manji_win_calibrator


def _win_odds(conn, race_id: str, horse: int):
    r = conn.execute(
        "SELECT win_odds FROM realtime_odds WHERE race_id=? AND horse_number=? "
        "ORDER BY recorded_at DESC LIMIT 1",
        (race_id, horse),
    ).fetchone()
    return float(r[0]) if r and r[0] else None


def main() -> int:
    conn = init_db()

    print("=== Step1: 本番較正器を学習 (fit_manji_win_calibrator) ===")
    diag = fit_manji_win_calibrator(conn, max_races=250, min_samples=120)
    print(json.dumps(diag, ensure_ascii=False, default=str))

    # ── Step2: 本番と同じ manji.ev_score() を再計算して時系列WF検証 ──
    # 本番経路 ManjiStrategy は ev_top=manji.ev_score(df) を calibrate_win_prob に渡すため、
    # 検証も同じスケールの再計算スコアで行う（DBの旧 expected_value は別スケールで不可）。
    print(
        "\n=== Step2: ウォークフォワード検証 (manji.ev_score 再計算・単勝1位の馬) ==="
    )
    from src.ml.features import FeatureBuilder
    from src.ml.models import load_models

    _h, _p, manji = load_models()
    # 直近180レースに限定（時系列順）— FeatureBuilder 再計算コスト抑制
    race_ids = [
        r[0]
        for r in conn.execute(
            """
            SELECT race_id FROM (
                SELECT r.race_id, r.date FROM races r
                WHERE EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id=r.race_id AND rr.rank=1)
                ORDER BY r.date DESC, r.race_id DESC LIMIT 180
            ) ORDER BY date ASC, race_id ASC
            """
        ).fetchall()
    ]
    print(f"WF対象レース数: {len(race_ids)}", flush=True)
    # 単勝1位ピックの (ev_top, odds, is_win) を時系列順に収集
    picks: list[tuple[float, float, int]] = []  # 1位馬のみ（本番の単勝）
    all_pairs: list[tuple[float, int]] = []  # 全馬 (ev, is_win)（較正学習用）
    for rid in race_ids:
        try:
            df = FeatureBuilder(conn).build_race_features(rid)
            if df is None or df.empty or "win_odds" not in df.columns:
                continue
            ev = manji.ev_score(df)
            winners = {
                row[0]
                for row in conn.execute(
                    "SELECT horse_name FROM race_results WHERE race_id=? AND rank=1",
                    (rid,),
                ).fetchall()
            }
            if not winners:
                continue
            best_i, best_ev = -1, -1e9
            for i, (_, hrow) in enumerate(df.iterrows()):
                if i >= len(ev):
                    break
                e = float(ev.iloc[i])
                is_w = 1 if str(hrow.get("horse_name", "")) in winners else 0
                all_pairs.append((e, is_w))
                if e > best_ev:
                    best_ev, best_i, best_row = e, i, hrow
            if best_i >= 0:
                odds = float(best_row.get("win_odds") or 0.0)
                is_w = 1 if str(best_row.get("horse_name", "")) in winners else 0
                if odds > 1.0:
                    picks.append((best_ev, odds, is_w))
        except Exception:
            continue

    def _mean(xs):
        return sum(xs) / len(xs) if xs else 0.0

    from sklearn.isotonic import IsotonicRegression

    if len(all_pairs) < 500:
        print(f"全馬ペア不足 (n={len(all_pairs)}) — スキップ")
        return 0

    # 全馬ペアを時系列 train70/test30（収集順=時系列順）に分割
    psplit = int(len(all_pairs) * 0.7)
    train_pairs, test_pairs = all_pairs[:psplit], all_pairs[psplit:]
    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    iso.fit([p[0] for p in train_pairs], [p[1] for p in train_pairs])

    test_ev = [p[0] for p in test_pairs]
    test_y = [p[1] for p in test_pairs]
    raw_implied = [
        min(max(e, 0.0), 1.0) for e in test_ev
    ]  # 旧: ev_score を直接確率扱い
    cal_p = [float(iso.predict([e])[0]) for e in test_ev]
    sat_old = sum(1 for x in raw_implied if x >= 0.99) / len(raw_implied) * 100
    sat_new = sum(1 for x in cal_p if x >= 0.99) / len(cal_p) * 100
    print(
        f"test(全馬) n={len(test_pairs)} 実勝率={_mean(test_y) * 100:.2f}%", flush=True
    )
    print(
        f"旧(ev_score直接)P: mean={_mean(raw_implied):.3f} 飽和(>=0.99)={sat_old:.1f}%"
    )
    print(
        f"較正 P(win)      : mean={_mean(cal_p):.3f} 飽和(>=0.99)={sat_new:.1f}% "
        f"max={max(cal_p):.3f}"
    )

    # 較正信頼性: 予測P(win) ビン別 [予測平均 vs 実勝率]（out-of-sample）
    bins = [(0.0, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 0.40), (0.40, 1.01)]
    print("予測P(win)ビン  n   予測平均  実勝率")
    ece = 0.0
    for lo, hi in bins:
        idx = [i for i, p in enumerate(cal_p) if lo <= p < hi]
        if not idx:
            continue
        pm = _mean([cal_p[i] for i in idx])
        am = _mean([test_y[i] for i in idx])
        ece += abs(pm - am) * len(idx) / len(cal_p)
        print(f"  [{lo:.2f},{hi:.2f})  {len(idx):4d}  {pm:.3f}    {am:.3f}")
    print(f"ECE(較正誤差・小さいほど良)= {ece:.4f}")

    if len(picks) >= 30:
        psp = int(len(picks) * 0.7)
        tst = picks[psp:]
        pev = [float(iso.predict([e])[0]) * o for e, o, _ in tst]
        rz = [(o if h else 0.0) for _, o, h in tst]
        print(
            f"単勝1位 test n={len(tst)} 予測EV mean={_mean(pev):.3f} "
            f"実現倍率 mean={_mean(rz):.3f} 乖離={abs(_mean(pev) - _mean(rz)):.3f}"
        )
    else:
        print(f"(単勝1位ピック {len(picks)}件 — EV直接検証は参考外)")

    ok_sat = sat_new < 20.0
    ok_ece = ece < 0.05
    print(
        f"\n[判定] 飽和解消={'OK' if ok_sat else 'NG'}(新飽和{sat_new:.1f}%) "
        f"較正良好={'OK' if ok_ece else 'NG'}(ECE={ece:.4f})"
    )
    print("RESULT:", "PASS" if (ok_sat and ok_ece and diag.get("fitted")) else "REVIEW")
    return 0


if __name__ == "__main__":
    sys.exit(main())
