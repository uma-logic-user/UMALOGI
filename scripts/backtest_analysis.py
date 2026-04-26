# -*- coding: utf-8 -*-
"""
EV 閾値・券種別バックテスト分析
"""
from __future__ import annotations
import io
import sqlite3
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

conn = sqlite3.connect(_ROOT / "data" / "umalogi.db")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 全データ取得
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
rows = conn.execute("""
    SELECT
        p.bet_type,
        p.model_type,
        p.expected_value  AS ev,
        p.recommended_bet AS bet_amount,
        pr.is_hit,
        pr.payout,
        pr.profit,
        r.date
    FROM prediction_results pr
    JOIN predictions p ON p.id = pr.prediction_id
    JOIN races r ON r.race_id = p.race_id
    WHERE p.bet_type NOT IN ('馬分析', 'WIN5')
      AND p.expected_value IS NOT NULL
    ORDER BY r.date, p.bet_type
""").fetchall()

conn.close()

import pandas as pd
df = pd.DataFrame(rows, columns=["bet_type","model_type","ev","bet_amount","is_hit","payout","profit","date"])

df["ev"]         = pd.to_numeric(df["ev"], errors="coerce").fillna(0)
df["bet_amount"] = pd.to_numeric(df["bet_amount"], errors="coerce").fillna(100)
df["payout"]     = pd.to_numeric(df["payout"], errors="coerce").fillna(0)
df["profit"]     = pd.to_numeric(df["profit"], errors="coerce").fillna(-df["bet_amount"])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. 全体サマリー
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
total_invest = df["bet_amount"].sum()
total_payout = df["payout"].sum()
total_profit = df["profit"].sum()
roi_all      = total_payout / total_invest * 100 if total_invest else 0

print("=" * 70)
print("【現状サマリー】 2026-04-11 ～ 2026-04-25")
print("=" * 70)
print(f"  総件数   : {len(df):,} 件")
print(f"  的中件数 : {df['is_hit'].sum():,} 件  ({df['is_hit'].mean()*100:.1f}%)")
print(f"  総投資額 : ¥{total_invest:,.0f}")
print(f"  総払戻額 : ¥{total_payout:,.0f}")
print(f"  損益     : ¥{total_profit:,.0f}")
print(f"  ROI      : {roi_all:.1f}%")
print()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. 券種別分析
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("=" * 70)
print("【券種別 ROI 分析】")
print("=" * 70)
print(f"{'券種':<8}{'件数':>6}{'的中':>6}{'的中率':>7}{'投資':>10}{'払戻':>10}{'損益':>10}{'ROI':>8}")
print("-" * 70)

bet_stats = {}
for bt, grp in df.groupby("bet_type"):
    inv = grp["bet_amount"].sum()
    pay = grp["payout"].sum()
    pro = grp["profit"].sum()
    roi = pay / inv * 100 if inv else 0
    hit = grp["is_hit"].sum()
    cnt = len(grp)
    bet_stats[bt] = {"inv": inv, "pay": pay, "pro": pro, "roi": roi, "hit": hit, "cnt": cnt}
    print(f"{bt:<8}{cnt:>6,}{hit:>6,}{hit/cnt*100:>7.1f}%{inv:>10,.0f}{pay:>10,.0f}{pro:>10,.0f}{roi:>7.1f}%")

print()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. EV 閾値別 ROI 分析（全券種合計）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("=" * 70)
print("【EV 閾値別 ROI（全券種）】")
print("=" * 70)
print(f"{'EV≥':>6}{'件数':>6}{'的中':>6}{'投資':>10}{'払戻':>10}{'ROI':>8}{'損益':>10}")
print("-" * 70)

thresholds = [0.0, 0.5, 0.8, 1.0, 1.1, 1.2, 1.3, 1.5, 2.0]
for thr in thresholds:
    sub = df[df["ev"] >= thr]
    if len(sub) == 0:
        continue
    inv = sub["bet_amount"].sum()
    pay = sub["payout"].sum()
    pro = sub["profit"].sum()
    roi = pay / inv * 100 if inv else 0
    hit = sub["is_hit"].sum()
    print(f"{thr:>6.1f}{len(sub):>6,}{hit:>6,}{inv:>10,.0f}{pay:>10,.0f}{roi:>7.1f}%{pro:>10,.0f}")

print()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 券種 × EV 閾値クロス分析
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("=" * 70)
print("【券種 × EV 閾値クロス ROI】")
print("=" * 70)

key_thresholds = [0.0, 1.0, 1.1, 1.2, 1.5]
bet_types_order = ["単勝", "複勝", "馬連", "ワイド", "馬単", "三連複", "三連単"]

header = f"{'券種':<8}" + "".join(f"EV≥{t:.1f}     " for t in key_thresholds)
print(header)
print("-" * 70)

for bt in bet_types_order:
    grp = df[df["bet_type"] == bt]
    if len(grp) == 0:
        continue
    row_str = f"{bt:<8}"
    for thr in key_thresholds:
        sub = grp[grp["ev"] >= thr]
        if len(sub) == 0:
            row_str += f"{'---':>12}"
            continue
        inv = sub["bet_amount"].sum()
        pay = sub["payout"].sum()
        roi = pay / inv * 100 if inv else 0
        row_str += f"{roi:>7.1f}%({len(sub):>3})"
    print(row_str)

print()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. EV 分布（券種別）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("=" * 70)
print("【EV 分布統計（券種別）】")
print("=" * 70)
print(f"{'券種':<8}{'件数':>5}{'EV平均':>8}{'EV中央':>8}{'EV最大':>8}{'EV<1.0':>8}{'EV1.0-1.2':>11}{'EV≥1.2':>8}")
print("-" * 70)
for bt in bet_types_order:
    grp = df[df["bet_type"] == bt]
    if len(grp) == 0:
        continue
    n = len(grp)
    print(f"{bt:<8}{n:>5,}{grp['ev'].mean():>8.3f}{grp['ev'].median():>8.3f}{grp['ev'].max():>8.3f}"
          f"{(grp['ev']<1.0).sum():>8,}{((grp['ev']>=1.0)&(grp['ev']<1.2)).sum():>11,}{(grp['ev']>=1.2).sum():>8,}")

print()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. 的中時の平均払戻（券種別）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("=" * 70)
print("【的中時 平均払戻（券種別）】")
print("=" * 70)
hits_df = df[df["is_hit"] == 1]
for bt in bet_types_order:
    grp = hits_df[hits_df["bet_type"] == bt]
    if len(grp) == 0:
        print(f"{bt:<8}: 的中なし")
        continue
    print(f"{bt:<8}: 的中{len(grp):>3}件  平均払戻 ¥{grp['payout'].mean():>8,.0f}  "
          f"中央値 ¥{grp['payout'].median():>8,.0f}  最大 ¥{grp['payout'].max():>10,.0f}")

print()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 8. モデル別分析（本命 vs 卍）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("=" * 70)
print("【モデル別 ROI】")
print("=" * 70)
for mt_label, mask in [("本命モデル", df["model_type"].str.startswith("本命")),
                        ("卍モデル",   df["model_type"].str.startswith("卍"))]:
    grp = df[mask]
    if len(grp) == 0:
        continue
    inv = grp["bet_amount"].sum()
    pay = grp["payout"].sum()
    roi = pay / inv * 100 if inv else 0
    print(f"{mt_label}: {len(grp):>5,}件  ROI={roi:.1f}%  的中={grp['is_hit'].sum()}/{len(grp)}({grp['is_hit'].mean()*100:.1f}%)"
          f"  投資¥{inv:,.0f}  払戻¥{pay:,.0f}")

print()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 9. 暫定 vs 直前
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("=" * 70)
print("【暫定 vs 直前 ROI】")
print("=" * 70)
for label, kw in [("暫定予想", "暫定"), ("直前予想", "直前")]:
    grp = df[df["model_type"].str.contains(kw)]
    if len(grp) == 0:
        continue
    inv = grp["bet_amount"].sum()
    pay = grp["payout"].sum()
    roi = pay / inv * 100 if inv else 0
    print(f"{label}: {len(grp):>5,}件  ROI={roi:.1f}%  的中={grp['is_hit'].sum()}/{len(grp)}({grp['is_hit'].mean()*100:.1f}%)"
          f"  投資¥{inv:,.0f}  払戻¥{pay:,.0f}")

print()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 10. 最適戦略シミュレーション
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("=" * 70)
print("【最適戦略シミュレーション（全体 EV フィルタ × 券種限定）】")
print("=" * 70)

strategies = [
    ("現状（全件）",           df,                                        "全券種"),
    ("EV≥1.0 全券種",         df[df["ev"]>=1.0],                        "全券種"),
    ("EV≥1.1 全券種",         df[df["ev"]>=1.1],                        "全券種"),
    ("EV≥1.2 全券種",         df[df["ev"]>=1.2],                        "全券種"),
    ("EV≥1.0 単勝のみ",       df[(df["ev"]>=1.0)&(df["bet_type"]=="単勝")], "単勝"),
    ("EV≥1.0 複勝のみ",       df[(df["ev"]>=1.0)&(df["bet_type"]=="複勝")], "複勝"),
    ("EV≥1.0 ワイドのみ",     df[(df["ev"]>=1.0)&(df["bet_type"]=="ワイド")], "ワイド"),
    ("EV≥1.1 単勝+複勝",      df[(df["ev"]>=1.1)&(df["bet_type"].isin(["単勝","複勝"]))], "単複"),
    ("EV≥1.1 単勝+ワイド",    df[(df["ev"]>=1.1)&(df["bet_type"].isin(["単勝","ワイド"]))], "単勝+ワイド"),
    ("EV≥1.2 単勝+複勝+ワイド", df[(df["ev"]>=1.2)&(df["bet_type"].isin(["単勝","複勝","ワイド"]))], "単複ワイド"),
    ("EV≥1.0 馬連+ワイド",    df[(df["ev"]>=1.0)&(df["bet_type"].isin(["馬連","ワイド"]))], "馬連ワイド"),
    ("卍 EV≥1.1 単勝+複勝",   df[(df["model_type"].str.startswith("卍"))&(df["ev"]>=1.1)&(df["bet_type"].isin(["単勝","複勝"]))], "卍単複"),
]

print(f"{'戦略':<30}{'件数':>6}{'的中':>5}{'投資':>10}{'払戻':>10}{'ROI':>8}{'損益':>10}")
print("-" * 80)
for name, sub, _ in strategies:
    if len(sub) == 0:
        print(f"{name:<30}{'---':>6}")
        continue
    inv = sub["bet_amount"].sum()
    pay = sub["payout"].sum()
    pro = sub["profit"].sum()
    roi = pay / inv * 100 if inv else 0
    hit = sub["is_hit"].sum()
    mark = " ★" if roi >= 100 else ("  ▲" if roi >= 60 else "")
    print(f"{name:<30}{len(sub):>6,}{hit:>5,}{inv:>10,.0f}{pay:>10,.0f}{roi:>7.1f}%{pro:>10,.0f}{mark}")

print()
print("★ = ROI 100%超え  ▲ = ROI 60%超え")
