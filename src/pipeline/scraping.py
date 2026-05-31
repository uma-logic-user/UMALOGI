"""
スクレイピング・データ取得パイプライン

責務:
  - 金曜バッチ（JRA-VAN RACE dataspec → DB 保存）
  - リアルタイムオッズ取得（RTD キャッシュ → DB 既存値フォールバック）
  - races テーブル仮登録
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import date, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]

logger = logging.getLogger(__name__)


def friday_batch(target_date: str | None = None) -> list[str]:
    """翌日（または指定日）の全レース出馬表を JRA-VAN から取得して DB に保存する。

    JVLINK_DISABLED=1 の場合は JVLink 呼び出しをスキップし、
    既に races テーブルに存在するレース ID のみを返す（netkeiba 補完前提）。

    Args:
        target_date: 対象日 "YYYYMMDD"。None なら翌日。

    Returns:
        保存したレース ID のリスト
    """
    from src.database.init_db import init_db

    if target_date is None:
        target_date = (date.today() + timedelta(days=1)).strftime("%Y%m%d")

    if os.environ.get("JVLINK_DISABLED", "").strip() == "1":
        logger.info(
            "JVLINK_DISABLED=1: friday_batch の JVLink 同期をスキップ (date=%s)",
            target_date,
        )
        formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
        conn = init_db()
        race_ids: list[str] = [
            r[0]
            for r in conn.execute(
                "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
                (formatted,),
            ).fetchall()
        ]
        conn.close()
        logger.info(
            "friday_batch: DB 既存 %d レースを返却 (JVLink スキップ)", len(race_ids)
        )
        return race_ids

    from src.scraper.jravan_client import JVDataLoader, DATASPEC_RACE

    logger.info("金曜バッチ開始: 対象日=%s (JRA-VAN RACE)", target_date)

    from_dt = f"{target_date}000000"
    sid = os.environ.get("JRAVAN_SID", "")

    try:
        loader = JVDataLoader(sid=sid)
        stats = loader.load(dataspec=DATASPEC_RACE, fromtime=from_dt)
        logger.info("金曜バッチ JVLink 完了: %s", stats)
    except Exception as exc:
        logger.error(
            "JVLink 接続失敗 (JRAVAN_SID=%r): %s", sid[:4] + "..." if sid else "", exc
        )
        raise

    formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    conn = init_db()
    race_ids: list[str] = [
        r[0]
        for r in conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
            (formatted,),
        ).fetchall()
    ]
    conn.close()

    logger.info("金曜バッチ完了: %d レース保存 (対象日=%s)", len(race_ids), target_date)
    return race_ids


def ensure_race_record(conn: sqlite3.Connection, race_id: str, date_str: str) -> None:
    """races テーブルにレコードがなければ仮登録する。

    distance=0 / surface="未定" 等のプレースホルダー値で INSERT OR IGNORE する。
    後続の JVLink 同期や update_race_details_from_entry() で正式値に上書きされる。

    Args:
        conn: SQLite 接続オブジェクト。
        race_id: 対象レース ID。
        date_str: レース開催日を "YYYYMMDD" 形式で表した文字列。
    """
    exists = conn.execute("SELECT 1 FROM races WHERE race_id=?", (race_id,)).fetchone()
    if not exists:
        try:
            race_num = int(race_id[-2:])
        except ValueError:
            race_num = 0
        formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        with conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO races
                    (race_id, race_name, date, venue, race_number,
                     distance, surface, weather, condition)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    race_id,
                    f"レース{race_num}",
                    formatted,
                    "未定",
                    race_num,
                    0,
                    "未定",
                    "",
                    "",
                ),
            )


def _safe_distance(value: object) -> int:
    """distance 値を安全に int へ変換する（例外を出さない）。

    netkeiba の DOM 変更・JVLink 欠損・テスト用 Mock などで distance が
    文字列 ("1600m")・None・想定外の型になっても例外を送出せず、
    抽出できる数字があればそれを、無ければ 0 を返す。

    Args:
        value: distance 候補値（int / float / str / None / その他）。

    Returns:
        正規化済みの距離 (m)。変換不能なら 0。
    """
    if isinstance(value, bool):  # bool は int サブクラスなので先に除外
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        digits = "".join(ch for ch in value if ch.isdigit())
        return int(digits) if digits else 0
    return 0


def _safe_text(value: object) -> str:
    """テキストフィールドを安全に str へ変換する（非 str は空文字に）。"""
    return value if isinstance(value, str) else ""


def update_race_details_from_entry(conn: sqlite3.Connection, tbl: object) -> bool:
    """EntryTable に含まれる distance/surface/race_name で races テーブルを更新する。

    distance > 0 かつ surface が空でない場合のみ UPDATE を実行する（空値による上書きを防ぐ）。
    race_name は既存値が空または "レース" で始まる場合のみ上書きする。

    Args:
        conn: SQLite 接続オブジェクト。
        tbl: EntryTable 互換オブジェクト（distance / surface / race_name /
             track_direction / weather / condition / race_id 属性を持つ）。

    Returns:
        races テーブルを更新した場合は True、スキップした場合は False。
    """
    dist = _safe_distance(getattr(tbl, "distance", 0))
    surf = _safe_text(getattr(tbl, "surface", ""))
    rname = _safe_text(getattr(tbl, "race_name", ""))
    tdir = _safe_text(getattr(tbl, "track_direction", ""))
    weather = _safe_text(getattr(tbl, "weather", ""))
    cond = _safe_text(getattr(tbl, "condition", ""))
    ptime = _safe_text(getattr(tbl, "post_time", ""))

    if dist <= 0 or not surf:
        return False

    with conn:
        conn.execute(
            """
            UPDATE races SET
                distance        = ?,
                surface         = ?,
                track_direction = CASE WHEN ? != '' THEN ? ELSE track_direction END,
                weather         = CASE WHEN ? != '' THEN ? ELSE weather END,
                condition       = CASE WHEN ? != '' THEN ? ELSE condition END,
                post_time       = CASE WHEN ? != '' THEN ? ELSE post_time END,
                race_name       = CASE WHEN ? != '' AND (race_name = '' OR race_name LIKE 'レース%') THEN ? ELSE race_name END
            WHERE race_id = ?
            """,
            (
                dist,
                surf,
                tdir,
                tdir,
                weather,
                weather,
                cond,
                cond,
                ptime,
                ptime,
                rname,
                rname,
                tbl.race_id,  # type: ignore[attr-defined]
            ),
        )
    logger.info(
        "races 更新 race_id=%s dist=%dm surface=%s",
        tbl.race_id,
        dist,
        surf,  # type: ignore[attr-defined]
    )
    return True


def save_entries_to_db(conn: sqlite3.Connection, tbl: object) -> int:
    """EntryTable を entries テーブルに保存して保存件数を返す。

    horse_id は JVLink と netkeiba で形式が異なるため NULL で保存し、
    後続の JVLink 同期で上書きされることを想定する。
    保存前に update_race_details_from_entry() を呼び出して races テーブルも更新する。

    Args:
        conn: SQLite 接続オブジェクト。
        tbl: EntryTable 互換オブジェクト（race_id / entries 属性を持つ）。
             entries は horse_number / gate_number / horse_name / sex_age /
             weight_carried / jockey / trainer / horse_weight / horse_weight_diff
             属性を持つオブジェクトのリスト。

    Returns:
        entries テーブルへの保存に成功した頭数。
    """
    # EntryTable に race details が含まれている場合は races テーブルも更新
    update_race_details_from_entry(conn, tbl)

    saved = 0
    for h in tbl.entries:  # type: ignore[attr-defined]
        try:
            with conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO entries
                        (race_id, horse_number, gate_number, horse_id,
                         horse_name, sex_age, weight_carried,
                         jockey, trainer, horse_weight, horse_weight_diff)
                    VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tbl.race_id,  # type: ignore[attr-defined]
                        h.horse_number,
                        h.gate_number,
                        h.horse_name,
                        h.sex_age,
                        h.weight_carried,
                        h.jockey,
                        h.trainer,
                        h.horse_weight,
                        h.horse_weight_diff,
                    ),
                )
            saved += 1
        except Exception as exc:
            logger.warning(
                "entries 保存失敗 %s #%d: %s",
                tbl.race_id,
                h.horse_number,
                exc,  # type: ignore[attr-defined]
            )
    return saved


def fetch_and_save_odds(conn: sqlite3.Connection, race_id: str) -> int:
    """realtime_odds が空のとき、JRA-VAN速報 → RTD → netkeiba → DB既存値 の順でオッズを確保する。

    フォールバック戦略（4段階・JRA-VAN 一次優先）:
      Stage 0: JRA-VAN 速報オッズ（JVRTOpen 0B30, 32bit COM）— 公式一次ソース
      Stage 1: JRA-VAN ローカル RTD キャッシュ — TARGET frontier 由来
      Stage 2: netkeiba オッズ API — 二次ソース（ハイブリッドフォールバック）
      Stage 3: DB 内の既存 realtime_odds を確認して件数を返す（再取得なし）

    各段は失敗時に次段へフォールバックするため、いずれかが死んでも EV は算出される。

    Returns:
        確保済みの頭数（0 の場合は暫定モードで続行）
    """
    from src.database.init_db import insert_realtime_odds

    name_map: dict[int, str] = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT horse_number, horse_name FROM entries WHERE race_id=?", (race_id,)
        ).fetchall()
    }

    # Stage 0: JRA-VAN 速報（JVRTOpen）— 公式一次ソース最優先。
    # 32bit COM ワーカーを 1 回起動してオッズ(0B30)・馬体重(0B11)・天候馬場(0B12)を取得。
    # 馬体重・天候馬場はオッズの成否に関わらず DB へ反映する（best-effort・上書きは値があるときのみ）。
    date8 = _race_date8(conn, race_id)
    if date8:
        payload = _run_jvrt_worker(race_id, date8)
        if payload:
            _apply_jvrt_weight_weather(conn, race_id, payload)
            jvrt_odds = _payload_to_horse_odds(payload)
            if jvrt_odds and all(o.win_odds for o in jvrt_odds):
                n = insert_realtime_odds(conn, race_id, jvrt_odds, name_map)
                logger.info(
                    "オッズ取得 [JRA-VAN 速報 JVRTOpen]: %d 頭保存 (race_id=%s)",
                    n,
                    race_id,
                )
                return n
            if jvrt_odds:
                logger.warning(
                    "JRA-VAN 速報: NaN あり → 後続フォールバックへ (race_id=%s)",
                    race_id,
                )

    # Stage 1: JRA-VAN ローカル RTD キャッシュ
    # ゼロ許容: 1頭でも NaN があれば即 netkeiba にフォールバック
    odds_list = _fetch_odds_rtd(race_id)
    if odds_list and any(o.win_odds for o in odds_list):
        valid = [o for o in odds_list if o.win_odds]
        nan_ratio = 1.0 - len(valid) / max(len(odds_list), 1)
        if nan_ratio == 0.0:  # 全頭オッズ完備の場合のみ RTD を採用
            n = insert_realtime_odds(conn, race_id, odds_list, name_map)
            logger.info(
                "オッズ取得 [RTD キャッシュ]: %d 頭保存 (race_id=%s)", n, race_id
            )
            return n
        logger.warning(
            "RTD: NaN あり (%.0f%%) — netkeiba に即フォールバック (race_id=%s)",
            nan_ratio * 100,
            race_id,
        )
    elif odds_list is not None:
        logger.warning(
            "RTD: オッズ取得なし or 全 NaN (race_id=%s) — netkeiba にフォールバック",
            race_id,
        )

    # Stage 2: netkeiba オッズ API（ハイブリッドフォールバック）
    nb_odds = _fetch_odds_netkeiba(race_id)
    if nb_odds and any(o.win_odds for o in nb_odds):
        n = insert_realtime_odds(conn, race_id, nb_odds, name_map)
        logger.info("オッズ取得 [netkeiba API]: %d 頭保存 (race_id=%s)", n, race_id)
        return n
    if nb_odds is not None:
        logger.warning("netkeiba: オッズ取得なし or 全 NaN (race_id=%s)", race_id)

    # Stage 3: DB 内の既存 realtime_odds を確認（再フェッチなし）
    existing: int = conn.execute(
        "SELECT COUNT(*) FROM realtime_odds WHERE race_id=?", (race_id,)
    ).fetchone()[0]
    if existing > 0:
        logger.info(
            "オッズ確保 [DB 既存]: %d 頭分 の realtime_odds を流用 (race_id=%s)",
            existing,
            race_id,
        )
        return existing

    logger.warning(
        "⚠️ オッズ全段取得失敗 (race_id=%s) — RTD/netkeiba/DB ともに 0。暫定モードで続行。",
        race_id,
    )
    return 0


def _fetch_odds_netkeiba(race_id: str) -> list | None:
    """netkeiba オッズ API からオッズリストを返す。RTD 失敗時のフォールバック用。

    Args:
        race_id: 対象レース ID。

    Returns:
        HorseOdds オブジェクトのリスト。オッズが空の場合は空リスト。
        例外発生時は None を返す。
    """
    try:
        from src.scraper.entry_table import fetch_realtime_odds

        nb_list = fetch_realtime_odds(race_id, delay=0.5, max_retries=2)
        if not nb_list:
            return []
        # HorseOdds は place_odds_min/max を持つため insert_realtime_odds と完全互換
        return nb_list
    except Exception as exc:
        logger.warning("netkeiba オッズ取得失敗 (race_id=%s): %s", race_id, exc)
        return None


def _race_date8(conn: sqlite3.Connection, race_id: str) -> str:
    """races.date（YYYY-MM-DD）から速報キー用の YYYYMMDD を返す。取得不可なら空文字。

    races テーブル不在等の例外時は空文字を返し、呼び出し側の Stage0 を安全にスキップさせる。
    """
    try:
        row = conn.execute(
            "SELECT date FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()
    except sqlite3.Error:
        return ""
    if not row or not row[0]:
        return ""
    d = str(row[0]).replace("-", "")
    return d if len(d) == 8 and d.isdigit() else ""


def _run_jvrt_worker(race_id: str, date8: str) -> dict | None:
    """JRA-VAN 速報ワーカー（32bit COM）を 1 回起動し、payload dict を返す。

    オッズ(0B30)・馬体重(0B11)・天候馬場(0B12) を 1 セッションで取得する。
    JV-Link は 32bit COM のため py -3.14-32 の subprocess で呼ぶ。完全 fail-safe。

    Returns:
        payload dict（keys: race_id/head_count/odds/weights/weather/condition）。
        取得失敗・無効時は None。
    """
    if os.environ.get("JVLINK_DISABLED", "").strip() == "1":
        return None

    import json
    import subprocess

    try:
        proc = subprocess.run(
            [
                "py",
                "-3.14-32",
                str(_ROOT / "scripts" / "_jvrt_odds_worker.py"),
                "--race-id",
                race_id,
                "--date",
                date8,
            ],
            cwd=str(_ROOT),
            timeout=90,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
        )
    except subprocess.TimeoutExpired:
        logger.warning("JRA-VAN 速報ワーカー タイムアウト (90s): race_id=%s", race_id)
        return None
    except Exception as exc:
        logger.warning("JRA-VAN 速報ワーカー 起動失敗 (race_id=%s): %s", race_id, exc)
        return None

    if proc.returncode != 0:
        logger.warning(
            "JRA-VAN 速報ワーカー rc=%d (race_id=%s) stderr=%s",
            proc.returncode,
            race_id,
            (proc.stderr or "")[:200],
        )
        return None

    # 最終 JSON 行を抽出（ワーカーは診断ログを stderr に出すが念のため後方から探索）
    for line in reversed((proc.stdout or "").splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    logger.warning("JRA-VAN 速報ワーカー JSON 解析失敗 (race_id=%s)", race_id)
    return None


def _payload_to_horse_odds(payload: dict | None) -> list | None:
    """ワーカー payload の odds 部を HorseOdds リストに変換する。空なら None。"""
    if not payload:
        return None
    from src.scraper.entry_table import HorseOdds

    result = [
        HorseOdds(
            horse_number=int(o["horse_number"]),
            win_odds=o.get("win_odds"),
            place_odds_min=None,
            place_odds_max=None,
            popularity=o.get("popularity"),
        )
        for o in (payload.get("odds") or [])
        if o.get("horse_number")
    ]
    return result or None


def _apply_jvrt_weight_weather(
    conn: sqlite3.Connection, race_id: str, payload: dict
) -> None:
    """ワーカー payload の馬体重・天候馬場を DB へ反映する（best-effort・値があるときのみ）。

    - 馬体重 → entries.horse_weight / horse_weight_diff（NULL では上書きしない）
    - 天候馬場 → races.weather / condition（空文字では上書きしない）

    例外は握りつぶし、オッズ取得や予測本体を止めない。
    """
    try:
        weights: dict = payload.get("weights") or {}
        applied = 0
        for num_str, wd in weights.items():
            weight = wd.get("weight") if isinstance(wd, dict) else None
            diff = wd.get("weight_diff") if isinstance(wd, dict) else None
            if weight is None and diff is None:
                continue
            try:
                num = int(num_str)
            except (ValueError, TypeError):
                continue
            conn.execute(
                """
                UPDATE entries
                   SET horse_weight      = COALESCE(?, horse_weight),
                       horse_weight_diff = COALESCE(?, horse_weight_diff)
                 WHERE race_id = ? AND horse_number = ?
                """,
                (weight, diff, race_id, num),
            )
            applied += 1

        weather = (payload.get("weather") or "").strip()
        condition = (payload.get("condition") or "").strip()
        if weather or condition:
            conn.execute(
                """
                UPDATE races
                   SET weather   = CASE WHEN ?<>'' THEN ? ELSE weather END,
                       condition = CASE WHEN ?<>'' THEN ? ELSE condition END
                 WHERE race_id = ?
                """,
                (weather, weather, condition, condition, race_id),
            )
        if applied or weather or condition:
            conn.commit()
            logger.info(
                "JRA-VAN 速報 反映: 馬体重%d頭 天候=%r 馬場=%r (race_id=%s)",
                applied,
                weather,
                condition,
                race_id,
            )
    except Exception as exc:  # noqa: BLE001 — 反映失敗は予測を止めない
        logger.warning(
            "JRA-VAN 速報 馬体重/天候反映 失敗 (race_id=%s): %s", race_id, exc
        )


def _fetch_odds_rtd(race_id: str) -> list | None:
    """JRA-VAN RTD キャッシュからオッズリストを返す。

    rtd_reader.read_rtd_for_race() を呼び出し、
    rtd_odds_to_horse_odds() で HorseOdds 形式に変換して返す。

    Args:
        race_id: 対象レース ID。

    Returns:
        HorseOdds オブジェクトのリスト。RTD にオッズがなければ空リスト。
        例外発生時は None を返す。
    """
    try:
        from src.scraper.rtd_reader import read_rtd_for_race, rtd_odds_to_horse_odds

        rtd_info = read_rtd_for_race(race_id)
        if rtd_info and rtd_info.odds:
            return rtd_odds_to_horse_odds(rtd_info)
        return []
    except Exception as exc:
        logger.warning("RTD 読み込み失敗 (race_id=%s): %s", race_id, exc)
        return None
