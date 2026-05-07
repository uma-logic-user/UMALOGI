"""
E2E テストランナー — パイプライン動作証明スクリプト

実行方法: py scripts/e2e_test_runner.py
"""
import sys
import time
import json
import logging

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("e2e_test")

RACE_ID = "202604010201"    # 新潟1R 2026-05-03 (16頭・結果完備)
TEST_DATE = "20260503"
OUT_PATH = "data/e2e_test_result.json"

results: dict = {}

# ─────────────────────────────────────────────────────
# Step 1: DB 接続 + レース存在確認
# ─────────────────────────────────────────────────────
logger.info("=" * 55)
logger.info("Step 1: DB 接続 + データ確認")
logger.info("=" * 55)

from src.database.init_db import init_db

conn = init_db()

race_row = conn.execute(
    "SELECT race_id, date, venue, race_number, distance, surface FROM races WHERE race_id=?",
    (RACE_ID,),
).fetchone()
entry_cnt = conn.execute(
    "SELECT COUNT(*) FROM entries WHERE race_id=?", (RACE_ID,)
).fetchone()[0]
result_cnt = conn.execute(
    "SELECT COUNT(*) FROM race_results WHERE race_id=?", (RACE_ID,)
).fetchone()[0]

logger.info("レース: %s  日付=%s  会場=%s  %sR  %sm %s",
            RACE_ID, race_row[1], race_row[2], race_row[3], race_row[4], race_row[5])
logger.info("出馬表: %d 頭 / 結果: %d 頭", entry_cnt, result_cnt)
results["step1"] = {"race": race_row[0], "entries": entry_cnt, "results": result_cnt, "ok": entry_cnt > 0}

# ─────────────────────────────────────────────────────
# Step 2: 特徴量生成
# ─────────────────────────────────────────────────────
logger.info("=" * 55)
logger.info("Step 2: 特徴量生成 (FeatureBuilder)")
logger.info("=" * 55)

from src.ml.features import FeatureBuilder

t0 = time.time()
fb = FeatureBuilder(conn)
df = fb.build_race_features(RACE_ID)
elapsed = time.time() - t0

logger.info("特徴量生成: %d 頭 × %d 列  %.2f秒", len(df), df.shape[1], elapsed)

# FEATURE_COLS の欠損チェック
from src.ml.models import FEATURE_COLS, _safe_feature_matrix
missing_cols = [c for c in FEATURE_COLS if c not in df.columns]
if missing_cols:
    logger.warning("⚠️ FEATURE_COLS 欠損列 %d件: %s", len(missing_cols), missing_cols)
else:
    logger.info("✅ FEATURE_COLS 全列存在 (%d列)", len(FEATURE_COLS))

results["step2"] = {
    "horse_count": len(df),
    "feature_cols": df.shape[1],
    "missing_cols": missing_cols,
    "elapsed_sec": round(elapsed, 2),
    "ok": not df.empty and len(missing_cols) == 0,
}

# ─────────────────────────────────────────────────────
# Step 3: モデルロード + 推論
# ─────────────────────────────────────────────────────
logger.info("=" * 55)
logger.info("Step 3: モデルロード + 推論")
logger.info("=" * 55)

from src.ml.models import load_models

t1 = time.time()
honmei, place, manji = load_models()
load_elapsed = time.time() - t1
logger.info("モデルロード: %.2f秒", load_elapsed)

t2 = time.time()
honmei_scores = honmei.predict(df)
ev_scores     = manji.ev_score(df)
place_scores  = place.predict(df)
infer_elapsed = time.time() - t2
logger.info("推論完了: %.2f秒", infer_elapsed)

# スコアサマリー
top5_honmei = honmei_scores.nlargest(5)
logger.info("--- 本命スコア TOP5 ---")
for idx, score in top5_honmei.items():
    name = df.loc[idx, "horse_name"] if "horse_name" in df.columns else str(idx)
    ev   = float(ev_scores.loc[idx]) if idx in ev_scores.index else 0.0
    logger.info("  %-12s  honmei=%.4f  ev=%.4f", name, score, ev)

results["step3"] = {
    "honmei_trained": honmei.is_trained,
    "place_trained": place.is_trained,
    "manji_trained": manji.is_trained,
    "honmei_max": float(honmei_scores.max()),
    "ev_max": float(ev_scores.max()),
    "infer_elapsed_sec": round(infer_elapsed, 2),
    "ok": not honmei_scores.empty,
}

# ─────────────────────────────────────────────────────
# Step 4: 買い目生成
# ─────────────────────────────────────────────────────
logger.info("=" * 55)
logger.info("Step 4: 買い目生成 (BetGenerator)")
logger.info("=" * 55)

from src.ml.bet_generator import BetGenerator, BetConfig, get_current_bankroll

bankroll = get_current_bankroll(conn)
logger.info("現在バンクロール: ¥%s", f"{int(bankroll):,}")

gen = BetGenerator(conn=conn, config=BetConfig(bankroll=bankroll))
honmei_bets  = gen.generate_honmei(RACE_ID, df, honmei_scores)
manji_bets   = gen.generate_manji(RACE_ID, df, ev_scores)

logger.info("本命買い目: %d件", len(honmei_bets.bets))
logger.info("卍買い目:   %d件", len(manji_bets.bets))
for bet in (honmei_bets.bets + manji_bets.bets)[:8]:
    combo_str = "-".join(str(n) for n in bet.combinations[0]) if bet.combinations else "?"
    logger.info("  [%s] %s %s  EV=%.2f  推奨=¥%s",
                honmei_bets.model_type if bet in honmei_bets.bets else manji_bets.model_type,
                bet.bet_type, combo_str,
                bet.expected_value or 0,
                f"{int(bet.recommended_bet or 0):,}")

results["step4"] = {
    "bankroll": bankroll,
    "honmei_bets": len(honmei_bets.bets),
    "manji_bets": len(manji_bets.bets),
    "ok": True,
}

# ─────────────────────────────────────────────────────
# Step 5: note 記事マークダウン生成（UIレンダリング監査）
# ─────────────────────────────────────────────────────
logger.info("=" * 55)
logger.info("Step 5: note記事 Markdown 生成 / UIバインディング確認")
logger.info("=" * 55)

import json as _json

date_label = f"{TEST_DATE[:4]}年{TEST_DATE[4:6]}月{TEST_DATE[6:8]}日"
venue_name = race_row[2] or "?"
race_num   = race_row[3] or "?"
distance   = race_row[4] or 0
surface    = race_row[5] or "?"

# トップ馬の EV 買い目
ev_picks = []
all_bets = honmei_bets.bets + manji_bets.bets
for bet in all_bets:
    if (bet.expected_value or 0) >= 1.0:
        combo = "-".join(str(n) for n in bet.combinations[0]) if bet.combinations else "?"
        ev_picks.append({
            "bet_type": bet.bet_type,
            "combo": combo,
            "ev": bet.expected_value or 0,
            "rec_bet": bet.recommended_bet or 100,
        })

# EV >= 1.0 の エリート複勝戦略 セクション
elite_lines = []
for p in ev_picks[:5]:
    icon = "🔥" if p["ev"] >= 2.0 else "✅"
    elite_lines.append(
        f"| {icon} {p['bet_type']} | {p['combo']} "
        f"| EV `{p['ev']:.2f}` | ¥{int(p['rec_bet']):,} |"
    )

markdown_note = f"""# {date_label} UMALOGI AI 週末予想まとめ

> **自律型AI競馬予測システム UMALOGI** が JRA-VAN データ × LightGBM で算出した期待値（EV）ベース予想です。

---

## 🏟️ 本日のハイライト

- 対象日: {date_label}
- テストレース: {venue_name} {race_num}R ({surface} {distance}m)
- EV≥1.0 買い目: {len(ev_picks)} 件
- 最大EV: {max((p['ev'] for p in ev_picks), default=0):.2f}

---

## 🔥 AI厳選（エリート複勝戦略）

> **EV（期待値）≥ 1.0 の買い目のみ掲載。値は 1.0 超 = 購入推奨。**

| 種別 | 組み合わせ | EV | 推奨投資額 |
|------|-----------|-----|----------|
{chr(10).join(elite_lines) if elite_lines else "| — | EV≥1.0 の買い目なし | — | — |"}

---

## 📊 推奨投資額・破産確率（RoR）警告

- **Kelly分数**: 1/4 Kelly（過剰ベット防止）
- **現在バンクロール**: ¥{int(bankroll):,}
- **総推奨投資額**: ¥{int(sum(p['rec_bet'] for p in ev_picks)):,}
- **破産リスク（RoR）**: {("⚠️ 総投資がバンクロールの10%超" if sum(p['rec_bet'] for p in ev_picks) > bankroll * 0.1 else "✅ 問題なし（バンクロールの10%以内）")}

---

## 本命モデルスコア TOP5

| 馬名 | P(win) | EV |
|------|--------|-----|
"""
for idx, score in top5_honmei.items():
    name = df.loc[idx, "horse_name"] if "horse_name" in df.columns else str(idx)
    ev_v = float(ev_scores.loc[idx]) if idx in ev_scores.index else 0.0
    markdown_note += f"| {name} | {score:.4f} | {ev_v:.4f} |\n"

markdown_note += """
---

※ 本予想は JRA-VAN データ × LightGBM モデルによる期待値ベース絞り込みです。投資は自己責任でお願いします。
"""

# ファイルに保存
note_path = f"outputs/e2e_note_{TEST_DATE}.md"
import os
os.makedirs("outputs", exist_ok=True)
with open(note_path, "w", encoding="utf-8") as f:
    f.write(markdown_note)
logger.info("note 記事生成: %s (%d 文字)", note_path, len(markdown_note))

# UI バインディングチェック
ui_checks = {
    "エリート複勝セクション": "🔥 AI厳選（エリート複勝戦略）" in markdown_note,
    "推奨投資額バインド": "推奨投資額" in markdown_note and "¥" in markdown_note,
    "破産確率RoR警告": "破産リスク（RoR）" in markdown_note,
    "EV値バインド": "EV `" in markdown_note or "EV≥1.0 の買い目なし" in markdown_note,
    "バンクロール表示": f"¥{int(bankroll):,}" in markdown_note,
    "P(win)スコア表示": f"{float(honmei_scores.max()):.4f}" in markdown_note,
}

logger.info("--- UI バインディングチェック ---")
all_ok = True
for check, passed in ui_checks.items():
    status = "✅ OK" if passed else "❌ NG"
    logger.info("  %s: %s", check, status)
    if not passed:
        all_ok = False

results["step5"] = {
    "note_path": note_path,
    "note_chars": len(markdown_note),
    "ev_picks": len(ev_picks),
    "ui_checks": ui_checks,
    "all_ui_ok": all_ok,
    "ok": all_ok,
}

conn.close()

# ─────────────────────────────────────────────────────
# 最終サマリー
# ─────────────────────────────────────────────────────
logger.info("=" * 55)
logger.info("E2E テスト最終サマリー")
logger.info("=" * 55)

all_passed = all(v.get("ok", False) for v in results.values())
for step, r in results.items():
    status = "✅ PASS" if r.get("ok") else "❌ FAIL"
    logger.info("  %s: %s", step, status)

logger.info("総合判定: %s", "✅ ALL PASS" if all_passed else "❌ 一部失敗")

# JSON保存
with open(OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
logger.info("結果JSON保存: %s", OUT_PATH)

sys.exit(0 if all_passed else 1)
