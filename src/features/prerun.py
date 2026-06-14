"""前走詳細・同コース実績 特徴量（W-070 続き / タスク1）。

各馬の「現レース日より前」の出走履歴のみを参照してリークフリーに前走系特徴量を
計算する。稼働中モデル(v1.x)の ``FEATURE_COLS`` は変更せず、再学習用に並行計算する
（acceleration.py と同じ非破壊方針）。

データ源:
    race_results（horse_id / horse_name / rank / margin / last_3f）と
    races（date / venue / surface / distance）。horse_id を主キーに馬を同定し、
    欠損時は horse_name でフォールバックする。

⚠️ リークフリーの担保: 対象レースの ``races.date`` より **厳密に過去** の出走のみを
   集計する。同日・未来のレースは一切参照しない（test_no_future_leak で検証）。
"""

from __future__ import annotations

import math
import sqlite3

import pandas as pd

from src.features.acceleration import parse_time_to_seconds

PRERUN_FEATURE_COLS: list[str] = [
    "prev_last_3f_sec",  # 直近過去走の上がり3F秒
    "prev_rank",  # 直近過去走の着順
    "prev_margin_sec",  # 直近過去走の着差（秒・正=負け差）
    "days_since_prev",  # 直近過去走からの間隔日数
    "avg_last_3f_3",  # 直近3走の上がり3F平均
    "same_course_runs",  # 同会場・同馬場での過去出走数
    "same_course_place_rate",  # 同会場・同馬場での複勝率(rank<=3)
    # W-003: 不完全燃焼度スコア
    "uf_incompleteness",  # 前走力不発 + 条件好転の合算 [0, 1]
    # W-007: 斤量インパクト
    "weight_carried_diff",  # 前走比斤量増減（kg。正=増量）
    "uf_weight_impact",  # 斤量インパクトスコア [0, 1]（増量→低、減量→高）
    # W-096 (Task4): 前走不利プロキシ
    "prev_trouble_proxy",  # 前走「不利」推定スコア [0,1]（速い上がりで着順凡退＝展開/不利の巻き返し期待）
]


def _parse_margin(value: object) -> float | None:
    """着差文字列を秒に変換する。"クビ"/"ハナ"等は近似秒へ、数値はそのまま。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return v if math.isfinite(v) else None
    text = str(value).strip()
    if not text:
        return None
    approx = {
        "ハナ": 0.0,
        "アタマ": 0.0,
        "クビ": 0.1,
        "同着": 0.0,
        "大差": 3.0,
    }
    if text in approx:
        return approx[text]
    try:
        return float(text)
    except ValueError:
        return None


def _surface_band(surface: str | None) -> str:
    return (surface or "").strip()


def build_prerun_features(conn: sqlite3.Connection, race_id: str) -> pd.DataFrame:
    """1レース分の前走詳細・同コース実績特徴量をリークフリーに計算する。

    Returns:
        columns = ["horse_number", *PRERUN_FEATURE_COLS] の DataFrame。
        出走データが無い場合は空 DataFrame。過去走の無い馬は NaN/0 で安全に埋める。
    """
    cols = ["horse_number", *PRERUN_FEATURE_COLS]
    meta = conn.execute(
        "SELECT date, venue, surface, distance FROM races WHERE race_id = ?",
        (race_id,),
    ).fetchone()
    if not meta:
        return pd.DataFrame(columns=cols)
    cur_date, cur_venue, cur_surface, _cur_dist_raw = meta
    try:
        _cur_dist = int(_cur_dist_raw) if _cur_dist_raw else 0
    except (ValueError, TypeError):
        _cur_dist = 0
    cur_surface = _surface_band(cur_surface)

    runners = conn.execute(
        "SELECT horse_number, horse_id, horse_name FROM race_results "
        "WHERE race_id = ? AND horse_number IS NOT NULL ORDER BY horse_number",
        (race_id,),
    ).fetchall()
    if not runners:
        return pd.DataFrame(columns=cols)

    records: list[dict[str, object]] = []
    for horse_number, horse_id, horse_name in runners:
        # 馬の同定: horse_id 優先、無ければ horse_name。
        if horse_id:
            where = "rr.horse_id = ?"
            key: object = horse_id
        else:
            where = "rr.horse_name = ?"
            key = horse_name

        # リークフリー: 現レース日より厳密に過去の出走のみ（自レコード除外）。
        # W-007: weight_carried・distance・grade を追加取得（列が存在しない環境では NULL になる）
        # COALESCE 方式ではなく pragma で確認してから SELECT を切り替える防衛コードを採用。
        _rr_cols = {
            r[1] for r in conn.execute("PRAGMA table_info(race_results)").fetchall()
        }
        _r_cols = {r[1] for r in conn.execute("PRAGMA table_info(races)").fetchall()}
        _wc_sel = "rr.weight_carried" if "weight_carried" in _rr_cols else "NULL"
        _dist_sel = "r.distance" if "distance" in _r_cols else "NULL"
        _grade_sel = "r.grade" if "grade" in _r_cols else "NULL"

        past = conn.execute(
            f"""
            SELECT r.date, r.venue, r.surface, rr.rank, rr.margin, rr.last_3f,
                   {_wc_sel}, {_dist_sel}, {_grade_sel}
            FROM race_results rr JOIN races r ON rr.race_id = r.race_id
            WHERE {where} AND r.date < ? AND rr.race_id != ?
              AND rr.rank IS NOT NULL AND rr.rank > 0
            ORDER BY r.date DESC
            """,
            (key, cur_date, race_id),
        ).fetchall()

        # 現レース情報（斤量・entries テーブルから; 列が存在しない場合は None）
        cur_weight: float | None = None
        try:
            _e_cols = {
                r[1] for r in conn.execute("PRAGMA table_info(entries)").fetchall()
            }
            if "weight_carried" in _e_cols:
                cur_weight_row = conn.execute(
                    "SELECT weight_carried FROM entries WHERE race_id = ? AND horse_number = ?",
                    (race_id, horse_number),
                ).fetchone()
                if cur_weight_row and cur_weight_row[0]:
                    cur_weight = float(cur_weight_row[0])
        except Exception:
            pass

        feat: dict[str, object] = {
            "horse_number": horse_number,
            "prev_last_3f_sec": math.nan,
            "prev_rank": math.nan,
            "prev_margin_sec": math.nan,
            "days_since_prev": math.nan,
            "avg_last_3f_3": math.nan,
            "same_course_runs": 0,
            "same_course_place_rate": math.nan,
            # W-003
            "uf_incompleteness": math.nan,
            # W-007
            "weight_carried_diff": math.nan,
            "uf_weight_impact": math.nan,
            # W-096 (Task4)
            "prev_trouble_proxy": math.nan,
        }

        if past:
            p0 = past[0]
            prev_rank_val = float(p0[3]) if p0[3] is not None else math.nan
            feat["prev_rank"] = prev_rank_val
            l3 = parse_time_to_seconds(p0[5])
            feat["prev_last_3f_sec"] = l3 if l3 is not None else math.nan
            mg = _parse_margin(p0[4])
            feat["prev_margin_sec"] = mg if mg is not None else math.nan

            # ── W-096 (Task4): 前走不利プロキシ ───────────────────────────
            # 「速い上がりで差してきたのに着順が凡退」＝直線で不利/出遅れ/前が壁
            #   等の展開不利を受けた可能性が高い → 次走巻き返しを加点する。
            #   通過順位データが無い（W-073）ため、上がり3F×着順×着差で代理推定する。
            #   リークフリー（前走の確定結果のみ参照）。
            trouble = 0.0
            l3v = feat["prev_last_3f_sec"]
            if (
                isinstance(l3v, float)
                and not math.isnan(l3v)
                and not math.isnan(prev_rank_val)
                and prev_rank_val > 3
            ):
                # 上がり強度: 35.5秒=0.0 / 32.5秒=1.0（速い差し脚ほど高）
                closing_strength = min(max((35.5 - l3v) / 3.0, 0.0), 1.0)
                # 着順ペナルティ: 4着=0.1 … 13着=1.0
                rank_penalty = min((prev_rank_val - 3) / 10.0, 1.0)
                trouble = closing_strength * rank_penalty
            # 僅差負け（着差<=0.5秒）かつ4着以下 → 展開/不利で取りこぼし
            if (
                mg is not None
                and not math.isnan(mg)
                and 0.0 < mg <= 0.5
                and not math.isnan(prev_rank_val)
                and prev_rank_val >= 4
            ):
                trouble = max(trouble, 0.4)
            feat["prev_trouble_proxy"] = round(min(trouble, 1.0), 4)

            try:
                from datetime import datetime

                d_prev = datetime.strptime(str(p0[0]), "%Y-%m-%d")
                d_cur = datetime.strptime(str(cur_date), "%Y-%m-%d")
                feat["days_since_prev"] = float((d_cur - d_prev).days)
            except (ValueError, TypeError):
                pass

            l3_list = [
                s
                for s in (parse_time_to_seconds(p[5]) for p in past[:3])
                if s is not None
            ]
            if l3_list:
                feat["avg_last_3f_3"] = sum(l3_list) / len(l3_list)

            # 同会場・同馬場の実績
            same = [
                p
                for p in past
                if p[1] == cur_venue and _surface_band(p[2]) == cur_surface
            ]
            feat["same_course_runs"] = len(same)
            if same:
                placed = sum(1 for p in same if p[3] is not None and p[3] <= 3)
                feat["same_course_place_rate"] = placed / len(same)

            # ── W-003: 不完全燃焼度スコア ──────────────────────────────────
            # 前走が力不発（rank>5）かつ今走で条件が好転した馬を高評価する。
            # 条件好転シグナル: 馬場変更・距離帯変更・超過充電（休養28日以上）
            incompleteness = 0.0
            if not math.isnan(prev_rank_val) and prev_rank_val > 5:
                base_score = min((prev_rank_val - 5) / 13.0, 1.0) * 0.5  # 最大0.5
                # 馬場変更ボーナス（前走と馬場が変わった）
                prev_surface = _surface_band(p0[2])
                if prev_surface and prev_surface != cur_surface:
                    base_score += 0.2
                # 距離帯変更ボーナス（前走距離 vs 現走距離）
                prev_dist = p0[7]
                if prev_dist and _cur_dist and prev_dist > 0 and _cur_dist > 0:
                    dist_ratio = abs(_cur_dist - prev_dist) / max(_cur_dist, prev_dist)
                    if dist_ratio > 0.15:  # 15%以上の距離変化
                        base_score += 0.15
                # 超過充電ボーナス（休養28日以上で気力回復）
                dsince = feat.get("days_since_prev")
                if (
                    isinstance(dsince, float)
                    and not math.isnan(dsince)
                    and dsince >= 28
                ):
                    base_score += 0.15
                incompleteness = min(base_score, 1.0)
            feat["uf_incompleteness"] = incompleteness

            # ── W-007: 斤量インパクトスコア ─────────────────────────────────
            # 前走比の斤量変化。減量→追い風・増量→向かい風として数値化する。
            prev_weight_val = p0[6]
            if prev_weight_val is not None and cur_weight is not None:
                try:
                    prev_w = float(prev_weight_val)
                    diff = cur_weight - prev_w  # 正=増量、負=減量
                    feat["weight_carried_diff"] = diff
                    # -4kg以下の減量 → 1.0（最高）、0kg → 0.5（中立）、+4kg以上の増量 → 0.0
                    feat["uf_weight_impact"] = max(0.0, min(1.0, 0.5 - diff / 8.0))
                except (ValueError, TypeError):
                    pass

        records.append(feat)

    return pd.DataFrame(records, columns=cols)
