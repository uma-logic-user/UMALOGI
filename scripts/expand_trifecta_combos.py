"""
scripts/expand_trifecta_combos.py — 三連単の過去combination_jsonをJRA公式1頭軸マルチに展開

問題:
  過去の三連単予想は「1着固定 top-12点」で生成されていた。
  JRA公式の「1頭軸マルチ」は 軸×相手N頭 → C(N,2)×6点 （軸が任意位置）。
  例: 軸=14, 相手=[2,3,6,10,11,13] → C(6,2)×6 = 90点 (¥9,000)

処理内容:
  1. 三連単予想で「1着固定(partial)」パターン（軸が常に1着位置）を検出
  2. 軸・相手馬番を推定し、全マルチ組み合わせを生成
  3. predictions.combination_json と recommended_bet を更新

Usage:
    py -3 scripts/expand_trifecta_combos.py --dry-run  # 変化量確認
    py -3 scripts/expand_trifecta_combos.py            # 実際に更新
"""
from __future__ import annotations

import argparse
import itertools
import json
import sys
from math import comb
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

from src.database.init_db import init_db


def _generate_multi_combos(axis: int, opponents: list[int]) -> list[list[int]]:
    """
    三連単1頭軸マルチの全組み合わせを生成する。

    JRA公式定義: 軸1頭 × 相手N頭 → C(N,2) × 3! = C(N,2)×6 点
    軸はどの位置（1着・2着・3着）にも入りうる。

    Args:
        axis:      軸馬の馬番
        opponents: 相手馬の馬番リスト（N頭）

    Returns:
        全組み合わせのリスト [[a,b,c], ...]
    """
    combos: list[list[int]] = []
    for pair in itertools.combinations(opponents, 2):
        for perm in itertools.permutations((axis,) + pair):
            combos.append(list(perm))
    return combos


def _infer_axis_and_opponents(
    combos: list[list[int]],
) -> tuple[int | None, list[int]]:
    """
    combination_json から軸馬と相手馬を推定する。

    軸馬: 全コンボに含まれ、かつ全コンボで1着位置にある馬
    相手: 軸以外の全馬番

    Returns:
        (axis, opponents) または (None, []) if 推定不可
    """
    if not combos:
        return None, []

    unique_horses = set(h for c in combos for h in c)
    firsts = set(c[0] for c in combos)

    # 1着固定（全コンボで同じ馬が1着）
    if len(firsts) == 1:
        axis = list(firsts)[0]
        opponents = sorted(unique_horses - {axis})
        return axis, opponents

    return None, []


def main() -> None:
    parser = argparse.ArgumentParser(description="三連単combination_jsonを正しい1頭軸マルチに展開する")
    parser.add_argument("--dry-run", action="store_true", help="変化量を表示するのみ（DB 更新なし）")
    args = parser.parse_args()

    conn = init_db()

    rows = conn.execute(
        "SELECT id, combination_json, recommended_bet FROM predictions WHERE bet_type='三連単'"
    ).fetchall()

    print(f"[展開] 三連単予想: {len(rows)}件")

    target_count    = 0
    skip_box        = 0
    skip_no_axis    = 0
    skip_already_ok = 0
    updated         = 0
    errors          = 0

    for row in rows:
        pred_id = row[0]
        combo_json = row[1] or "[]"
        rec_bet = row[2] or 0.0

        try:
            combo_list: list[list[int]] = json.loads(combo_json)
        except Exception:
            errors += 1
            continue

        if not combo_list or not isinstance(combo_list[0], list):
            skip_no_axis += 1
            continue

        n = len(combo_list)
        unique_horses = set(h for c in combo_list for h in c)
        num = len(unique_horses)

        # ボックスは対象外（すでに全順列）
        if n == num * (num - 1) * (num - 2):
            skip_box += 1
            continue

        axis, opponents = _infer_axis_and_opponents(combo_list)

        if axis is None or len(opponents) < 2:
            skip_no_axis += 1
            continue

        # 正確な点数を計算
        correct_n = comb(len(opponents), 2) * 6
        if n >= correct_n:
            skip_already_ok += 1
            continue

        target_count += 1

        # 新しい全マルチ組み合わせを生成
        new_combos = _generate_multi_combos(axis, opponents)
        new_rec_bet = len(new_combos) * 100

        if args.dry_run:
            print(
                f"  id={pred_id} 軸={axis} 相手={opponents} "
                f"{n}点→{len(new_combos)}点 "
                f"rec_bet={rec_bet:.0f}→{new_rec_bet}"
            )
        else:
            try:
                conn.execute(
                    "UPDATE predictions SET combination_json=?, recommended_bet=? WHERE id=?",
                    (json.dumps(new_combos), float(new_rec_bet), pred_id),
                )
                updated += 1
            except Exception as e:
                print(f"  ERROR id={pred_id}: {e}")
                errors += 1

    if not args.dry_run:
        conn.commit()

    print()
    print(f"[結果]")
    print(f"  展開対象 : {target_count}件")
    print(f"  ボックス除外: {skip_box}件")
    print(f"  軸推定不可 : {skip_no_axis}件")
    print(f"  正確な点数: {skip_already_ok}件")
    print(f"  エラー    : {errors}件")
    if args.dry_run:
        print(f"  --dry-run: DB は更新されていません")
    else:
        print(f"  更新済み  : {updated}件")

    conn.close()


if __name__ == "__main__":
    main()
