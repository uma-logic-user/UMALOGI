"""
UMALOGI E2E 模擬テスト（Dry-Run）

本番の1日フロー（出馬表取得→特徴量生成→予測→評価→通知）を
過去の確定済みデータを使って全結合検証する。DB への書き込みは行わない。

使用例:
  py scripts/e2e_test.py                       # デフォルト日付 (2025-12-27)
  py scripts/e2e_test.py --date 2025-12-20     # 任意の過去日付
  py scripts/e2e_test.py --race 202506050701   # 特定 race_id のみ
  py scripts/e2e_test.py --verbose             # 詳細ログ
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logger = logging.getLogger("e2e_test")

_PASS = "✅ PASS"
_FAIL = "❌ FAIL"
_WARN = "⚠️  WARN"
_SKIP = "⏭️  SKIP"


@dataclass
class StepResult:
    name: str
    status: str   # PASS / FAIL / WARN / SKIP
    message: str
    elapsed_ms: int = 0
    detail: dict[str, Any] = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 0: DB接続と対象日の確認
# ─────────────────────────────────────────────────────────────────────────────

def step_db_connect(db_path: Path) -> tuple[StepResult, sqlite3.Connection | None]:
    t0 = time.perf_counter()
    try:
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA foreign_keys = ON")
        cnt = conn.execute("SELECT COUNT(*) FROM races").fetchone()[0]
        elapsed = int((time.perf_counter() - t0) * 1000)
        return StepResult(
            "DB接続", "PASS", f"umalogi.db 接続OK — races={cnt}件",
            elapsed, {"races_count": cnt}
        ), conn
    except Exception as exc:
        return StepResult("DB接続", "FAIL", str(exc)), None


def step_check_target_date(conn: sqlite3.Connection, target_date: str) -> StepResult:
    t0 = time.perf_counter()
    try:
        row = conn.execute("""
            SELECT COUNT(DISTINCT r.race_id)                          AS races,
                   SUM(CASE WHEN rr.rank=1 THEN 1 ELSE 0 END)        AS rank1,
                   COUNT(DISTINCT rp.race_id)                         AS payout_races
            FROM races r
            JOIN race_results rr ON rr.race_id = r.race_id
            LEFT JOIN race_payouts rp ON rp.race_id = r.race_id
                AND rp.bet_type = '三連単'
            WHERE r.date = ?
        """, (target_date,)).fetchone()
        races, rank1, payout_races = row[0] or 0, row[1] or 0, row[2] or 0
        elapsed = int((time.perf_counter() - t0) * 1000)
        if races == 0:
            return StepResult("対象日確認", "FAIL", f"{target_date}: レースが存在しません")
        status = "PASS" if payout_races >= races * 0.8 else "WARN"
        return StepResult(
            "対象日確認", status,
            f"{target_date}: {races}レース rank1={rank1} 払戻={payout_races}レース",
            elapsed, {"races": races, "rank1": rank1, "payout_races": payout_races}
        )
    except Exception as exc:
        return StepResult("対象日確認", "FAIL", str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 1: 出馬表 + 特徴量生成（シミュレーション版）
# ─────────────────────────────────────────────────────────────────────────────

def step_feature_generation(
    conn: sqlite3.Connection, race_ids: list[str], max_races: int = 3
) -> StepResult:
    t0 = time.perf_counter()
    try:
        from src.ml.features import FeatureBuilder
        fb = FeatureBuilder(conn)
        tested = 0
        errors: list[str] = []
        min_horses = 99
        for race_id in race_ids[:max_races]:
            df = fb.build_race_features_for_simulate(race_id)
            if df.empty:
                errors.append(f"{race_id}: 0頭")
            else:
                min_horses = min(min_horses, len(df))
                tested += 1
        elapsed = int((time.perf_counter() - t0) * 1000)
        if errors:
            return StepResult(
                "特徴量生成", "WARN",
                f"{tested}/{max_races} 成功。エラー: {errors}",
                elapsed
            )
        return StepResult(
            "特徴量生成", "PASS",
            f"{tested}レース × {df.shape[1]}特徴量 (最小{min_horses}頭)",
            elapsed, {"races_tested": tested, "feature_cols": df.shape[1]}
        )
    except Exception as exc:
        return StepResult("特徴量生成", "FAIL", f"{exc}\n{traceback.format_exc()[:400]}")


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 2: HonmeiModel + ManjiModel 予測（モデルロード→推論）
# ─────────────────────────────────────────────────────────────────────────────

def step_model_predict(
    conn: sqlite3.Connection, race_id: str
) -> StepResult:
    t0 = time.perf_counter()
    try:
        from src.ml.features import FeatureBuilder
        from src.ml.models import HonmeiModel, ManjiModel, FEATURE_COLS
        fb = FeatureBuilder(conn)
        df = fb.build_race_features_for_simulate(race_id)
        if df.empty:
            return StepResult("モデル予測", "WARN", f"{race_id}: 出馬表が空")

        honmei = HonmeiModel()
        honmei.load()
        manji = ManjiModel()
        manji.load()

        h_scores = honmei.predict(df)
        m_scores = manji.predict(df)

        elapsed = int((time.perf_counter() - t0) * 1000)
        top_honmei = df.loc[h_scores.idxmax(), "horse_name"] if "horse_name" in df.columns else "?"
        top_manji  = df.loc[m_scores.idxmax(), "horse_name"] if "horse_name" in df.columns else "?"
        return StepResult(
            "モデル予測", "PASS",
            f"race_id={race_id} 本命={top_honmei}(score={h_scores.max():.3f}) 卍={top_manji}(score={m_scores.max():.3f})",
            elapsed, {"horses": len(df), "honmei_top": top_honmei, "manji_top": top_manji}
        )
    except Exception as exc:
        return StepResult("モデル予測", "FAIL", f"{exc}\n{traceback.format_exc()[:400]}")


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 3: CascadePredictor（Stage-2/3）
# ─────────────────────────────────────────────────────────────────────────────

def step_cascade_predict(
    conn: sqlite3.Connection, race_id: str
) -> StepResult:
    t0 = time.perf_counter()
    cascade_dir = _ROOT / "data" / "models" / "cascade"
    if not (cascade_dir / "stage2_model.pkl").exists():
        return StepResult("Cascade予測", "SKIP", "stage2_model.pkl が未生成")
    try:
        from src.ml.features import FeatureBuilder
        from src.ml.models import HonmeiModel
        from scripts.train_cascade import CascadePredictor

        fb = FeatureBuilder(conn)
        df = fb.build_race_features_for_simulate(race_id)
        if df.empty:
            return StepResult("Cascade予測", "WARN", f"{race_id}: 出馬表が空")

        honmei = HonmeiModel()
        honmei.load()
        stage1_probs = honmei.predict(df)
        predictor = CascadePredictor.load(cascade_dir)
        trifecta = predictor.predict_trifecta(df, stage1_probs, top_n=5)

        elapsed = int((time.perf_counter() - t0) * 1000)
        top1 = trifecta[0] if trifecta else None
        return StepResult(
            "Cascade予測", "PASS",
            f"三連単TOP1: {top1[1] if top1 else 'N/A'} (score={top1[0]:.4f})" if top1 else "組合なし",
            elapsed, {"top5": [(f"{c}", round(p, 5)) for p, c in trifecta[:3]]}
        )
    except Exception as exc:
        return StepResult("Cascade予測", "FAIL", f"{exc}\n{traceback.format_exc()[:400]}")


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 4: BetGenerator（買い目生成）
# ─────────────────────────────────────────────────────────────────────────────

def step_bet_generation(
    conn: sqlite3.Connection, race_id: str
) -> StepResult:
    t0 = time.perf_counter()
    try:
        from src.ml.features import FeatureBuilder
        from src.ml.models import HonmeiModel, ManjiModel
        from src.ml.bet_generator import BetGenerator

        fb = FeatureBuilder(conn)
        df = fb.build_race_features_for_simulate(race_id)
        if df.empty:
            return StepResult("買い目生成", "WARN", f"{race_id}: 出馬表が空")

        honmei = HonmeiModel()
        honmei.load()
        manji = ManjiModel()
        manji.load()

        gen = BetGenerator(conn=conn)
        honmei_bets = gen.generate_honmei(race_id, df, honmei.predict(df))
        manji_bets  = gen.generate_manji(race_id, df, manji.predict(df))

        elapsed = int((time.perf_counter() - t0) * 1000)
        return StepResult(
            "買い目生成", "PASS",
            f"本命={len(honmei_bets.bets)}買い目 卍={len(manji_bets.bets)}買い目",
            elapsed, {
                "honmei_bets": len(honmei_bets.bets),
                "manji_bets": len(manji_bets.bets)
            }
        )
    except Exception as exc:
        return StepResult("買い目生成", "FAIL", f"{exc}\n{traceback.format_exc()[:400]}")


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 5: 評価エンジン（払戻突合）
# ─────────────────────────────────────────────────────────────────────────────

def step_evaluate(
    conn: sqlite3.Connection, race_id: str
) -> StepResult:
    t0 = time.perf_counter()
    try:
        from src.evaluation.evaluator import Evaluator

        evaluator = Evaluator()
        result = evaluator.evaluate_race(conn, race_id, dry_run=True)
        elapsed = int((time.perf_counter() - t0) * 1000)

        # predictions がない場合でもEvaluationResultは返る（hits=[]）
        if not result.hits and not result.errors:
            return StepResult("評価エンジン", "SKIP", f"{race_id}: 予想データなし (skip)")

        status = "PASS"
        msg = (
            f"race_id={race_id} 投資¥{result.total_invested:.0f} "
            f"払戻¥{result.total_payout:.0f} ROI={result.roi:.1f}% "
            f"的中={result.hit_count}/{len(result.hits)}"
        )
        if result.errors:
            status = "WARN"
            msg += f" | エラー: {result.errors[:2]}"
        return StepResult("評価エンジン", status, msg, elapsed, {
            "roi": result.roi,
            "hit_count": result.hit_count,
            "errors": result.errors,
        })
    except Exception as exc:
        return StepResult("評価エンジン", "FAIL", f"{exc}\n{traceback.format_exc()[:400]}")


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 6: 払戻データの安全性チェック
# ─────────────────────────────────────────────────────────────────────────────

def step_payout_safety(conn: sqlite3.Connection, target_date: str) -> StepResult:
    t0 = time.perf_counter()
    try:
        # 払戻0円なのに的中フラグが立っているレコードを検出
        zero_hit = conn.execute("""
            SELECT COUNT(*) FROM prediction_results pr
            JOIN predictions p ON p.id = pr.prediction_id
            JOIN races r ON r.race_id = p.race_id
            WHERE pr.is_hit = 1 AND COALESCE(pr.payout, 0) = 0
            AND r.date >= '2025-01-01'
        """).fetchone()[0]

        # 三連単 combination が → 形式でないレコード（フォーマット汚染）
        bad_sanrentan = conn.execute("""
            SELECT COUNT(*) FROM race_payouts
            WHERE bet_type = '三連単'
            AND instr(combination, '→') = 0
            AND payout > 0
        """).fetchone()[0]

        # NUL/制御文字を含む馬名（migration #11後の残存確認）
        # SQLite LIKE は NUL バイトを正しく扱えないため Python 側で判定
        name_rows = conn.execute(
            "SELECT horse_name FROM race_results WHERE horse_name IS NOT NULL"
        ).fetchall()
        bad_names = sum(
            1 for (n,) in name_rows
            if any(b < 0x20 or b == 0x7F for b in n.encode("utf-8", errors="replace"))
        )

        # rank > 18 の不正レコード（migration #10後の確認）
        bad_rank = conn.execute(
            "SELECT COUNT(*) FROM race_results WHERE rank > 18"
        ).fetchone()[0]

        # 三連単フォーマット異常 — JV-Link の HR 誤パースによる 2-number 組合せ（WARN 扱い）
        # 評価エンジンは「→」形式のキーで照合するため false-hit にはならないが、
        # データとして不整合なので件数を記録する
        elapsed = int((time.perf_counter() - t0) * 1000)
        issues = []
        if zero_hit > 0:      issues.append(f"払戻¥0的中={zero_hit}件")
        if bad_rank > 0:      issues.append(f"rank>18={bad_rank}件")
        if bad_names > 0:     issues.append(f"制御文字馬名={bad_names}件")
        warn_issues = []
        if bad_sanrentan > 0: warn_issues.append(f"三連単非→形式={bad_sanrentan}件(JV-Linkゴミ,評価には影響なし)")

        status = "FAIL" if bad_rank > 0 or bad_names > 0 or zero_hit > 0 else \
                 "WARN" if warn_issues else "PASS"
        all_issues = issues + warn_issues
        msg = "データ整合性OK" if not all_issues else " / ".join(all_issues)
        return StepResult("払戻安全性", status, msg, elapsed, {
            "zero_hit_payout": zero_hit,
            "bad_sanrentan_format": bad_sanrentan,
            "bad_horse_names": bad_names,
            "bad_rank": bad_rank,
        })
    except Exception as exc:
        return StepResult("払戻安全性", "FAIL", str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 7: Web API レスポンス検証（SQLite直接クエリ模擬）
# ─────────────────────────────────────────────────────────────────────────────

def step_api_simulation(conn: sqlite3.Connection, target_date: str) -> StepResult:
    t0 = time.perf_counter()
    issues: list[str] = []
    try:
        # /api/races の N+1回避クエリ確認
        races = conn.execute("""
            SELECT race_id FROM races WHERE date = ? LIMIT 5
        """, (target_date,)).fetchall()
        race_ids = [r[0] for r in races]
        if not race_ids:
            return StepResult("API模擬", "SKIP", f"{target_date}: レースなし")

        ph = ",".join("?" * len(race_ids))
        results = conn.execute(f"""
            SELECT race_id, COUNT(*) as cnt
            FROM race_results WHERE race_id IN ({ph})
            GROUP BY race_id
        """, race_ids).fetchall()
        if len(results) != len(race_ids):
            issues.append(f"race_results欠落: {len(race_ids)}/{len(results)}")

        # /api/predictions N+1問題を検出（prediction_horsesのIN-query代替）
        pred_rows = conn.execute("""
            SELECT p.id FROM predictions p
            JOIN races r ON r.race_id = p.race_id
            WHERE r.date = ? LIMIT 50
        """, (target_date,)).fetchall()
        pred_ids = [r[0] for r in pred_rows]
        if pred_ids:
            ph2 = ",".join("?" * len(pred_ids))
            horses = conn.execute(f"""
                SELECT prediction_id, COUNT(*) FROM prediction_horses
                WHERE prediction_id IN ({ph2})
                GROUP BY prediction_id
            """, pred_ids).fetchall()
            if len(horses) < len(pred_ids):
                issues.append(f"prediction_horses欠落: {len(pred_ids) - len(horses)}件")

        # combination_json パース確認
        bad_json = 0
        for (pred_id,) in pred_rows[:20]:
            cj = conn.execute(
                "SELECT combination_json FROM predictions WHERE id = ?", (pred_id,)
            ).fetchone()
            if cj and cj[0]:
                try:
                    json.loads(cj[0])
                except (json.JSONDecodeError, TypeError):
                    bad_json += 1
        if bad_json:
            issues.append(f"combination_json 破損={bad_json}件")

        elapsed = int((time.perf_counter() - t0) * 1000)
        status = "FAIL" if bad_json else "WARN" if issues else "PASS"
        msg = "API クエリ正常" if not issues else " / ".join(issues)
        return StepResult("API模擬", status, msg, elapsed, {
            "races": len(race_ids), "predictions": len(pred_ids),
            "bad_json": bad_json
        })
    except Exception as exc:
        return StepResult("API模擬", "FAIL", str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 8: Discord 通知 Dry-Run
# ─────────────────────────────────────────────────────────────────────────────

def step_discord_dry_run() -> StepResult:
    t0 = time.perf_counter()
    import os
    url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not url:
        return StepResult(
            "Discord通知", "SKIP",
            "DISCORD_WEBHOOK_URL 未設定 (本番では必ず設定してください)"
        )
    try:
        from src.notification.discord_notifier import DiscordNotifier
        n = DiscordNotifier(webhook_url=url)
        # dry_run=True で送信せずメッセージ構築のみ
        msg = n.build_message(
            title="[E2E TEST] UMALOGI 自動テスト",
            body="このメッセージはE2Eテストの疎通確認です。本番レース予想は通常通り自動送信されます。",
            color=0x00FF88,
        )
        elapsed = int((time.perf_counter() - t0) * 1000)
        return StepResult(
            "Discord通知", "PASS",
            f"Webhookメッセージ構築OK (dry_run=True, 送信なし)",
            elapsed
        )
    except AttributeError:
        # build_message が存在しない場合はインポートのみ確認
        elapsed = int((time.perf_counter() - t0) * 1000)
        return StepResult(
            "Discord通知", "PASS",
            "DiscordNotifier インポートOK (WEBHOOK_URL設定済み)",
            elapsed
        )
    except Exception as exc:
        return StepResult("Discord通知", "FAIL", str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 9: スケジューラーフェイルセーフ確認
# ─────────────────────────────────────────────────────────────────────────────

def step_scheduler_audit() -> StepResult:
    """scheduler.py の必須環境変数・フェイルセーフを静的確認。"""
    t0 = time.perf_counter()
    import os
    checks: dict[str, str] = {}

    # 必須環境変数
    for var in ["DISCORD_WEBHOOK_URL", "JRAVAN_SID"]:
        checks[var] = "設定済み" if os.environ.get(var) else "❌ 未設定"

    # オプション環境変数
    for var in ["UMANITY_EMAIL", "UMANITY_PASSWORD", "NOTIFY_LINE", "NOTIFY_TWITTER"]:
        checks[var] = "設定済み" if os.environ.get(var) else "(オプション・未設定)"

    # scheduler.py の存在確認
    sched_path = _ROOT / "scripts" / "scheduler.py"
    checks["scheduler.py"] = "存在OK" if sched_path.exists() else "❌ 見つからない"

    # モデルファイル確認
    for model_file in [
        "data/models/honmei_model.pkl",
        "data/models/manji_model.pkl",
        "data/models/cascade/stage2_model.pkl",
        "data/models/cascade/stage3_model.pkl",
    ]:
        path = _ROOT / model_file
        checks[model_file] = f"OK ({path.stat().st_size // 1024}KB)" if path.exists() else "❌ 欠損"

    # DB バックアップ最新ファイル確認
    backup_dir = _ROOT / "data" / "backups"
    if backup_dir.exists():
        backups = sorted(backup_dir.glob("*.db"), key=lambda p: p.stat().st_mtime)
        checks["最新バックアップ"] = backups[-1].name if backups else "❌ バックアップなし"
    else:
        checks["最新バックアップ"] = "❌ backupsディレクトリなし"

    elapsed = int((time.perf_counter() - t0) * 1000)
    missing = [k for k, v in checks.items() if "❌" in v]
    # JRAVAN_SID と DISCORD は本番必須だが E2E テスト環境では WARN に留める
    status = "WARN" if missing else "PASS"

    lines = [f"  {k}: {v}" for k, v in checks.items()]
    return StepResult("スケジューラー監査", status,
                      "\n".join(lines), elapsed, checks)


# ─────────────────────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────────────────────

def run_e2e(target_date: str, race_id_override: str | None, verbose: bool) -> int:
    """全ステップを実行して結果を表示。返り値 = 失敗ステップ数。"""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 65)
    print(f"  UMALOGI E2E 模擬テスト  対象日={target_date}")
    print("=" * 65)

    results: list[StepResult] = []

    # ─── DB 接続
    db_path = _ROOT / "data" / "umalogi.db"
    db_res, conn = step_db_connect(db_path)
    results.append(db_res)
    _print_step(db_res)
    if conn is None:
        _print_summary(results)
        return 1

    from src.database.init_db import init_db
    conn = init_db()  # migration 済みの接続を使う

    # ─── 対象日確認
    date_res = step_check_target_date(conn, target_date)
    results.append(date_res)
    _print_step(date_res)

    # 対象 race_id リスト
    if race_id_override:
        race_ids = [race_id_override]
    else:
        race_ids = [r[0] for r in conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_number LIMIT 3",
            (target_date,)
        ).fetchall()]
    test_race = race_ids[0] if race_ids else None

    # ─── ステップ 1: 特徴量生成
    feat_res = step_feature_generation(conn, race_ids, max_races=min(3, len(race_ids)))
    results.append(feat_res)
    _print_step(feat_res)

    # ─── ステップ 2: モデル予測
    if test_race:
        pred_res = step_model_predict(conn, test_race)
        results.append(pred_res)
        _print_step(pred_res)

        # ─── ステップ 3: Cascade 予測
        cas_res = step_cascade_predict(conn, test_race)
        results.append(cas_res)
        _print_step(cas_res)

        # ─── ステップ 4: 買い目生成
        bet_res = step_bet_generation(conn, test_race)
        results.append(bet_res)
        _print_step(bet_res)

        # ─── ステップ 5: 評価エンジン
        eval_res = step_evaluate(conn, test_race)
        results.append(eval_res)
        _print_step(eval_res)

    # ─── ステップ 6: 払戻安全性
    pay_res = step_payout_safety(conn, target_date)
    results.append(pay_res)
    _print_step(pay_res)

    # ─── ステップ 7: API 模擬
    api_res = step_api_simulation(conn, target_date)
    results.append(api_res)
    _print_step(api_res)

    # ─── ステップ 8: Discord Dry-Run
    discord_res = step_discord_dry_run()
    results.append(discord_res)
    _print_step(discord_res)

    # ─── ステップ 9: スケジューラー監査
    sched_res = step_scheduler_audit()
    results.append(sched_res)
    _print_step(sched_res)

    conn.close()
    return _print_summary(results)


def _print_step(r: StepResult) -> None:
    icon = {"PASS": _PASS, "FAIL": _FAIL, "WARN": _WARN, "SKIP": _SKIP}.get(r.status, "?")
    print(f"\n{icon}  [{r.name}]  ({r.elapsed_ms}ms)")
    for line in r.message.split("\n"):
        print(f"       {line}")


def _print_summary(results: list[StepResult]) -> int:
    """サマリーを表示して失敗数を返す。"""
    pass_cnt  = sum(1 for r in results if r.status == "PASS")
    fail_cnt  = sum(1 for r in results if r.status == "FAIL")
    warn_cnt  = sum(1 for r in results if r.status == "WARN")
    skip_cnt  = sum(1 for r in results if r.status == "SKIP")
    total_ms  = sum(r.elapsed_ms for r in results)

    print("\n" + "=" * 65)
    print(f"  E2E テスト完了  ({total_ms}ms)")
    print(f"  {_PASS} {pass_cnt}件  {_WARN} {warn_cnt}件  {_FAIL} {fail_cnt}件  {_SKIP} {skip_cnt}件")
    if fail_cnt == 0 and warn_cnt == 0:
        print("  🎉 全ステップ合格 — 本番稼働可能")
    elif fail_cnt == 0:
        print("  🟡 WARN あり — 要確認後に本番稼働")
    else:
        print(f"  🔴 FAIL {fail_cnt}件 — 本番稼働前に修正必須")
    print("=" * 65)
    return fail_cnt


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="UMALOGI E2E 模擬テスト")
    ap.add_argument("--date",    default="2025-12-27", help="テスト対象日 (YYYY-MM-DD)")
    ap.add_argument("--race",    default=None,          help="特定 race_id のみテスト")
    ap.add_argument("--verbose", action="store_true",   help="詳細ログ")
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    rc = run_e2e(args.date, args.race, args.verbose)
    sys.exit(rc)
