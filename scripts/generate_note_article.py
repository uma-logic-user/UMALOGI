"""
scripts/generate_note_article.py — note 投稿用 Markdown 記事の自動生成

指定日・レースの卍複勝予想をDBから取得し、NarrativeGenerator で
根拠文を生成して note 投稿用 Markdown に整形する。

note の有料記事フォーマット:
  ▸ 無料エリア: レース概要・波乱度・無料公開の注目馬1頭
  ▸ 区切り:     ====ここから有料エリア====
  ▸ 有料エリア: 全買い目・EV・詳細根拠文・資金管理提案

Usage:
    py scripts/generate_note_article.py --date 20260503
    py scripts/generate_note_article.py --race-id 202608030411
    py scripts/generate_note_article.py --date 20260503 --top 5 --output docs/note/
    py scripts/generate_note_article.py --date 20260503 --stdout
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import date as dt_date
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("generate_note_article")


# ── 定数 ────────────────────────────────────────────────────────────

_EV_THRESHOLD      = 1.0   # 買い目対象 EV 下限
_MAX_PICKS_DEFAULT = 3      # 1レースあたり最大公開馬数
_FREE_PICKS        = 1      # 無料エリアに公開する馬数
_BET_PER_HORSE     = 300    # 推奨1点あたり購入額（円）

_SURFACE_JP = {"芝": "芝", "ダート": "ダート", "D": "ダート", "T": "芝", "": "芝/ダート"}
_CONDITION_JP = {
    "良": "良馬場", "稍": "稍重", "稍重": "稍重", "重": "重", "不良": "不良", "": "良馬場"
}

# 波乱度ラベル
_CHAOS_LABELS = [
    (0.0,  0.3, "🟢 堅い", "順当な決着が予想される。上位人気馬中心の手堅い本命買いが有効。"),
    (0.3,  0.6, "🟡 中程度", "中穴の台頭もあり得る。本命◎ + 紐1〜2頭の組み合わせが妙手。"),
    (0.6,  1.0, "🔴 高波乱", "人気薄の激走が起こりやすい条件が揃っている。複勝の払戻倍率に期待できる。"),
]


# ── DB ヘルパー ─────────────────────────────────────────────────────

def _fetch_race_info(conn: sqlite3.Connection, race_id: str) -> dict:
    row = conn.execute(
        """
        SELECT race_name, venue, race_number, distance,
               surface, condition, date, weather
        FROM races WHERE race_id = ?
        """,
        (race_id,),
    ).fetchone()
    if not row:
        return {}
    return {
        "race_name":   row[0] or "",
        "venue":       row[1] or "",
        "race_number": row[2] or 0,
        "distance":    row[3] or 0,
        "surface":     row[4] or "",
        "condition":   row[5] or "",
        "date":        row[6] or "",
        "weather":     row[7] or "",
    }


def _fetch_predictions(
    conn: sqlite3.Connection,
    race_id: str,
    ev_threshold: float = _EV_THRESHOLD,
) -> list[dict]:
    """卍複勝の EV 順予想リストを返す。"""
    rows = conn.execute(
        """
        SELECT p.id, p.model_type, p.combination_json,
               p.expected_value, p.recommended_bet, p.confidence, p.notes
        FROM predictions p
        WHERE p.race_id = ?
          AND p.model_type LIKE '卍%'
          AND p.bet_type  = '複勝'
          AND (p.expected_value IS NULL OR p.expected_value >= ?)
        ORDER BY p.expected_value DESC NULLS LAST
        LIMIT 1
        """,
        (race_id, ev_threshold),
    ).fetchall()
    result = []
    for row in rows:
        combo: list[list[int]] = []
        if row[2]:
            try:
                raw = json.loads(row[2])
                if raw and isinstance(raw[0], list):
                    combo = raw
                elif raw:
                    combo = [[n] for n in raw]
            except Exception:
                pass
        result.append({
            "prediction_id":  row[0],
            "model_type":     row[1],
            "combination_json": row[2],
            "combos":         combo,
            "ev":             row[3] or 1.0,
            "recommended_bet": row[4] or _BET_PER_HORSE,
            "confidence":     row[5] or 0.5,
            "notes":          row[6] or "",
        })
    return result


def _fetch_horse_info(
    conn: sqlite3.Connection,
    race_id: str,
    horse_numbers: list[int],
) -> dict[int, dict]:
    """指定馬番の馬情報を返す。"""
    placeholders = ",".join("?" * len(horse_numbers))
    rows = conn.execute(
        f"""
        SELECT horse_number, horse_name, jockey, trainer,
               win_odds, weight_carried, horse_weight,
               horse_weight_diff, gate_number, sex_age, popularity
        FROM race_results
        WHERE race_id = ? AND horse_number IN ({placeholders})
        ORDER BY horse_number
        """,
        [race_id] + horse_numbers,
    ).fetchall()
    return {
        row[0]: {
            "horse_number":     row[0],
            "horse_name":       row[1] or f"{row[0]}番",
            "jockey":           row[2] or "不明",
            "trainer":          row[3] or "不明",
            "win_odds":         row[4],
            "weight_carried":   row[5],
            "horse_weight":     row[6],
            "horse_weight_diff": row[7],
            "gate_number":      row[8] or row[0],
            "sex_age":          row[9] or "",
            "popularity":       row[10],
        }
        for row in rows
    }


def _fetch_top_races_by_ev(
    conn: sqlite3.Connection,
    target_date: str,
    top_n: int,
) -> list[str]:
    """指定日で EV が最も高い race_id を top_n 件返す。"""
    rows = conn.execute(
        """
        SELECT p.race_id, MAX(p.expected_value) AS max_ev
        FROM predictions p
        JOIN races r ON r.race_id = p.race_id
        WHERE r.date = ?
          AND p.model_type LIKE '卍%'
          AND p.bet_type  = '複勝'
          AND (p.expected_value IS NULL OR p.expected_value >= 1.0)
        GROUP BY p.race_id
        ORDER BY max_ev DESC NULLS LAST
        LIMIT ?
        """,
        (target_date, top_n),
    ).fetchall()
    return [row[0] for row in rows]


# ── 波乱度計算 ────────────────────────────────────────────────────

def _calc_chaos_score(conn: sqlite3.Connection, race_id: str) -> float:
    """
    単純な波乱度スコアを返す（0.0〜1.0）。
    出走頭数と EV の分散から推定する。
    """
    row = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE race_id = ?", (race_id,)
    ).fetchone()
    head_count = row[0] if row else 0

    evs = conn.execute(
        "SELECT expected_value FROM predictions WHERE race_id = ? AND model_type LIKE '卍%' AND bet_type='複勝'",
        (race_id,),
    ).fetchall()
    ev_list = [r[0] for r in evs if r[0] is not None]

    if not ev_list:
        return 0.3

    import statistics
    ev_cv = statistics.stdev(ev_list) / max(statistics.mean(ev_list), 0.01) if len(ev_list) > 1 else 0
    head_factor = min(head_count / 18.0, 1.0)  # 18頭立て=1.0
    return min((ev_cv * 0.6 + head_factor * 0.4), 1.0)


def _chaos_label(score: float) -> tuple[str, str]:
    for lo, hi, label, desc in _CHAOS_LABELS:
        if lo <= score < hi:
            return label, desc
    return _CHAOS_LABELS[-1][2], _CHAOS_LABELS[-1][3]


# ── 記事生成 ──────────────────────────────────────────────────────

def generate_article(
    conn: sqlite3.Connection,
    race_id: str,
    max_picks: int = _MAX_PICKS_DEFAULT,
    use_shap: bool = True,
) -> str:
    """
    1レース分の note 記事 Markdown を生成して返す。
    """
    race  = _fetch_race_info(conn, race_id)
    preds = _fetch_predictions(conn, race_id)

    if not preds:
        return f"<!-- {race_id}: 卍複勝予想なし（EV閾値未達または未予想）-->\n"

    pred   = preds[0]
    combos = pred["combos"]

    # 馬番リスト（EV順上位 max_picks 頭）
    all_horse_nums = [c[0] for c in combos if c][:max_picks]
    horse_info_map = _fetch_horse_info(conn, race_id, all_horse_nums)

    # レース属性
    race_name   = race.get("race_name") or f"R{race.get('race_number', '?')}（{race.get('venue', '')}）"
    venue       = race.get("venue", "")
    distance    = race.get("distance", 0)
    surface_raw = race.get("surface", "")
    condition   = race.get("condition", "")
    date_str    = race.get("date", "")
    race_no     = race.get("race_number", 0)
    surface_jp  = _SURFACE_JP.get(surface_raw, surface_raw or "芝")
    cond_jp     = _CONDITION_JP.get(condition, condition or "良馬場")
    dist_str    = f"{distance}m" if distance else "中距離"

    # 波乱度
    chaos_score = _calc_chaos_score(conn, race_id)
    chaos_lbl, chaos_desc = _chaos_label(chaos_score)

    # 予算計算
    total_bet    = len(all_horse_nums) * _BET_PER_HORSE
    ev           = pred.get("ev", 1.0)
    expected_ret = int(total_bet * ev)

    # SHAP 根拠文
    narratives: dict[int, str] = {}
    if use_shap:
        try:
            from src.ml.narrative_generator import NarrativeGenerator
            gen = NarrativeGenerator(conn)
            # 全馬をまとめて処理（特徴量はキャッシュで1回だけ計算）
            for hn in all_horse_nums:
                narratives[hn] = gen.generate(
                    race_id=race_id, horse_number=hn, ev=ev
                )
        except Exception as e:
            logger.warning("NarrativeGenerator エラー: %s — フォールバック使用", e)

    # ── 記事組み立て ─────────────────────────────────────────────

    lines: list[str] = []

    # ヘッダー
    lines += [
        f"# 🏇【UMALOGI AI予想】{date_str} {race_name}",
        "",
        f"> **{venue} {race_no}R｜{surface_jp}{dist_str}｜{cond_jp}**",
        f"> 卍モデル（回収率特化AI）が算出したEVトップの複勝予想です。",
        f"> JRA公式データ（JRA-VAN）のみ使用・オッズ補正済み期待値ベースで厳選。",
        "",
        "---",
        "",
        "## 📊 無料エリア",
        "",
    ]

    # 無料：レース概要
    lines += [
        "### レース概要",
        "",
        f"| 項目 | 内容 |",
        f"|---|---|",
        f"| 開催 | {venue} {race_no}R |",
        f"| 条件 | {surface_jp} {dist_str} |",
        f"| 馬場 | {cond_jp} |",
        "",
    ]

    # 無料：波乱度
    lines += [
        "### 波乱の可能性",
        "",
        f"**{chaos_lbl}**",
        "",
        chaos_desc,
        "",
    ]

    # 無料：注目馬1頭（EV最上位の馬のみ公開）
    free_picks = all_horse_nums[:_FREE_PICKS]
    if free_picks:
        hn_free    = free_picks[0]
        hi         = horse_info_map.get(hn_free, {})
        free_name  = hi.get("horse_name", f"{hn_free}番")
        free_gate  = hi.get("gate_number", hn_free)
        free_jky   = hi.get("jockey", "")
        free_odds  = hi.get("win_odds")
        odds_str   = f"（単勝 {free_odds:.1f}倍）" if free_odds else ""

        lines += [
            "### 🔍 無料公開：注目馬",
            "",
            f"**{hn_free}番 {free_name}**（{free_gate}枠 / {free_jky}騎手）{odds_str}",
            "",
            f"卍モデルのスコアが出走メンバー中トップ。詳細根拠と全買い目は有料エリアをご覧ください。",
            "",
        ]

    # 無料：月次実績サマリー（DB から集計）
    try:
        month_start = date_str[:7] + "-01" if date_str else None
        if month_start:
            stat = conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN pr.is_hit=1 THEN 1 ELSE 0 END) AS hits,
                       COALESCE(SUM(pr.profit), 0) AS profit
                FROM predictions p
                LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
                JOIN races r ON r.race_id = p.race_id
                WHERE p.model_type LIKE '卍%' AND p.bet_type='複勝'
                  AND r.date >= ?
                  AND pr.is_hit IS NOT NULL
                """,
                (month_start,),
            ).fetchone()
            if stat and stat[0] > 0:
                total, hits, profit = stat
                hits = hits or 0
                profit = profit or 0
                hit_rate = hits / total * 100
                profit_sign = "+" if profit >= 0 else ""
                lines += [
                    "### 📈 今月の実績サマリー（卍複勝モデル）",
                    "",
                    f"| 指標 | 値 |",
                    f"|---|---|",
                    f"| 予想件数 | {total} 件 |",
                    f"| 的中件数 | {hits} 件（的中率 {hit_rate:.1f}%）|",
                    f"| 損益合計 | {profit_sign}¥{int(abs(profit)):,} |",
                    "",
                ]
    except Exception:
        pass

    # 有料区切り
    lines += [
        "---",
        "",
        "# ここから有料エリア",
        "",
        "> 以下には **全買い目・EV値・詳細根拠文・推奨投資額** が含まれます。",
        "",
        "---",
        "",
        "## 💰 有料エリア：卍複勝 全買い目",
        "",
    ]

    # 有料：買い目一覧
    lines += [
        "### 🎯 推奨買い目（卍モデル EV順）",
        "",
        "| 馬番 | 馬名 | 騎手 | 複勝オッズ目安 | 推奨購入 |",
        "|---|---|---|---|---|",
    ]
    for hn in all_horse_nums:
        hi      = horse_info_map.get(hn, {})
        name    = hi.get("horse_name", f"{hn}番")
        jky     = hi.get("jockey", "—")
        odds    = hi.get("win_odds")
        # 複勝オッズは単勝の 1/3 程度を目安に表示
        fuku_lo = f"{odds/3:.1f}" if odds else "—"
        fuku_hi = f"{odds/1.5:.1f}" if odds else "—"
        fuku_str = f"{fuku_lo}〜{fuku_hi}倍" if odds else "—"
        lines.append(f"| {hn}番 | {name} | {jky} | {fuku_str} | ¥{_BET_PER_HORSE:,} |")

    lines += [
        "",
        f"> **合計購入額**: ¥{total_bet:,}（各{_BET_PER_HORSE}円）",
        f"> **モデル期待値 EV**: {ev:.2f}（期待回収額: 約 ¥{expected_ret:,}）",
        "",
    ]

    # 有料：詳細根拠文
    lines += [
        "---",
        "",
        "## 🧠 AI根拠解説",
        "",
        "### なぜこの馬たちが買いなのか",
        "",
    ]

    for i, hn in enumerate(all_horse_nums, 1):
        hi         = horse_info_map.get(hn, {})
        horse_name = hi.get("horse_name", f"{hn}番")
        narrative  = narratives.get(hn)

        if narrative:
            lines += [f"#### {i}. {horse_name}", "", narrative, ""]
        else:
            # フォールバック：シンプルな記述
            jky        = hi.get("jockey", "不明")
            gate       = hi.get("gate_number", hn)
            wc         = hi.get("weight_carried")
            wc_str     = f"斤量{wc:.0f}kg" if wc else ""
            exp_return = int(100 * ev)
            lines += [
                f"#### {i}. {hn}番 {horse_name}",
                "",
                f"**{gate}枠 / {jky}騎手** {wc_str}",
                "",
                f"卍モデルが期待回収率 EV={ev:.2f}（100円投資→約{exp_return}円回収期待）を算出。"
                f"通算成績・馬場適性・調教状態・騎手実績を総合評価した結果、"
                f"今回の出走メンバー中でトップクラスのスコアを記録した。",
                "",
            ]

    # 有料：資金管理
    lines += [
        "---",
        "",
        "## 💡 資金管理ガイドライン",
        "",
        "| 推奨事項 | 内容 |",
        "|---|---|",
        f"| 1点あたり | ¥{_BET_PER_HORSE} を推奨（損失許容額の5%以内） |",
        f"| 月間予算  | ¥30,000 を上限に週5,000円ペースで運用 |",
        "| 損切り基準 | 月次ROI < 80%が続く場合は一時休止を推奨 |",
        "| 買い方    | 全頭一律で購入（EV上位から順に点数調整可） |",
        "",
        "> ⚠️ 本予想はバックテストに基づく参考情報であり、的中を保証するものではありません。",
        "> 投票は余裕資金の範囲内でお願いします。",
        "",
        "---",
        "",
        "## 🤖 UMALOGIについて",
        "",
        "UMALOGI は JRA公式データ（JRA-VAN）のみを学習データとして使用する、",
        "完全自律型の競馬予測AIシステムです。",
        "",
        "- **使用データ**: JRA-VAN（JV-Data）— netkeiba等の二次ソースは一切不使用",
        "- **モデル**: LightGBM（勾配ブースティング）+ 期待回収率特化の卍モデル",
        "- **特徴量**: 調教タイム・騎手実績・血統・馬場バイアス・オッズ変動等 39項目",
        "- **絞り込み**: EV（期待値）≥ 1.0 の買い目のみを推奨（回収期待のある馬のみ公開）",
        "",
        f"次回の予想は次週土曜 7:30 に公開予定です。フォロー・マガジン購読でお知らせを受け取れます。",
        "",
    ]

    return "\n".join(lines)


# ── メイン ────────────────────────────────────────────────────────

_DEFAULT_OUT_DIR = _ROOT / "outputs" / "note"


def _safe_filename(s: str) -> str:
    """レース名をファイル名として安全な文字列に変換する。"""
    import re
    s = s.replace("（", "(").replace("）", ")")
    s = re.sub(r'[\\/:*?"<>|]', "_", s)
    return s.strip()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="note 用 Markdown 記事を自動生成する")
    p.add_argument("--date",    help="対象日 YYYYMMDD または YYYY-MM-DD（省略時=本日）")
    p.add_argument("--race-id", help="特定レース ID（指定時は --date より優先）")
    p.add_argument("--top",     type=int, default=1,
                   help="生成するレース数 EV 順（デフォルト 1）")
    p.add_argument("--max-picks", type=int, default=_MAX_PICKS_DEFAULT,
                   help="1レースあたり最大公開馬数（デフォルト 3）")
    p.add_argument("--output",  help=f"出力ディレクトリ（省略時: {_DEFAULT_OUT_DIR}）")
    p.add_argument("--stdout",  action="store_true", help="ファイル保存せず標準出力のみ")
    p.add_argument("--no-shap", action="store_true", help="SHAP 計算を省略（高速モード）")
    return p.parse_args()


def _build_out_path(out_dir: Path, race_id: str, race_info: dict) -> Path:
    """出力ファイルパスを構築する。形式: YYYYMMDD_R{N}_{レース名}.md"""
    date_raw   = (race_info.get("date") or "").replace("-", "")
    race_no    = race_info.get("race_number") or int(race_id[10:12])
    race_name  = race_info.get("race_name") or ""
    safe_name  = _safe_filename(race_name) if race_name else race_id
    filename   = f"{date_raw}_R{race_no:02d}_{safe_name}.md"
    return out_dir / filename


def main() -> None:
    args = _parse_args()

    from src.database.init_db import init_db
    conn = init_db()

    # 対象日の整形
    if args.race_id:
        race_ids = [args.race_id]
        target_date = args.race_id[2:6] + "-" + args.race_id[6:8] + "-" + args.race_id[8:10]
    else:
        raw_date = args.date or dt_date.today().strftime("%Y%m%d")
        raw_date = raw_date.replace("-", "")
        target_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
        race_ids = _fetch_top_races_by_ev(conn, target_date, top_n=args.top)

    if not race_ids:
        logger.warning("対象レースが見つかりません (date=%s)", target_date)
        conn.close()
        return

    logger.info("記事生成: %d レース", len(race_ids))

    out_dir = Path(args.output) if args.output else _DEFAULT_OUT_DIR

    for race_id in race_ids:
        logger.info("生成中: %s", race_id)
        article = generate_article(
            conn=conn,
            race_id=race_id,
            max_picks=args.max_picks,
            use_shap=not args.no_shap,
        )

        if args.stdout:
            print(article)
        else:
            race_info = _fetch_race_info(conn, race_id)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = _build_out_path(out_dir, race_id, race_info)
            out_path.write_text(article, encoding="utf-8")
            logger.info("保存: %s", out_path)
            # 標準出力にもサマリーを表示
            race_name = race_info.get("race_name") or race_id
            print(f"[UMALOGI] note記事保存: {out_path.name}")

    conn.close()


if __name__ == "__main__":
    main()
