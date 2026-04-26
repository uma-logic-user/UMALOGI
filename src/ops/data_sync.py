"""
JRA-VAN データ自動同期スクリプト

すべてのデータ取得は JVLinkClient (JRA-VAN 公式) のみを使用する。
netkeiba 等への外部スクレイピングは一切行わない。

スケジュール:
  金曜 20:00  : RACE (翌週末分出馬表)
  土日 8:00   : WOOD (当日調教タイム)
  土日 レース後: RACE (確定成績 / 払戻)
  月曜 6:00   : DIFN (騎手・調教師・競走馬マスタ更新)
  月1回       : SETUP (全マスタ初期化)

Usage:
    python -m src.ops.data_sync friday
    python -m src.ops.data_sync race_results
    python -m src.ops.data_sync wood
    python -m src.ops.data_sync masters
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# .env を明示的にロード（32bit Python 実行時も含め確実に環境変数を反映させる）
# _ROOT は sys.path 追加用（parents[3] = C:\dev）。.env は親を2つ辿ったプロジェクトルートにある。
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(_PROJECT_ROOT / ".env", override=True)
except ImportError:
    pass


def _get_conn() -> sqlite3.Connection:
    from src.database.init_db import init_db
    return init_db()


def _count_races_for_date(target_date: str) -> int:
    """races テーブルに対象日のレコードが何件あるか返す。"""
    formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    conn = _get_conn()
    count: int = conn.execute(
        "SELECT COUNT(*) FROM races WHERE date = ?", (formatted,)
    ).fetchone()[0]
    conn.close()
    return count


def _count_race_results_for_date(target_date: str) -> int:
    """race_results テーブルに対象日のレコードが何件あるか返す。"""
    formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    conn = _get_conn()
    count: int = conn.execute(
        """
        SELECT COUNT(*) FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        WHERE r.date = ?
        """,
        (formatted,),
    ).fetchone()[0]
    conn.close()
    return count


def sync_friday(target_date: str | None = None) -> int:
    """
    金曜夜バッチ: JRA-VAN RACE dataspec から翌週末の出馬表を取得して DB に保存する。

    フォールバック戦略（3段階）:
      1. OPT_NORMAL  : JRA-VANサーバーから差分ダウンロード（通常運用）
      2. OPT_STORED  : JVLink ローカルキャッシュから読み込み
      3. OPT_SETUP   : JRA-VANサーバーから強制フルダウンロード（最終手段）
                       ※ RACE + OPT_SETUP は大量データを取得するため時間がかかる場合あり

    各 Stage 後、対象日のレースが DB に存在するか確認する。
    total_read > 0 でも対象日のデータが取得できていない場合（過去データのみ返った場合等）
    は次の Stage へ進む。

    Returns:
        保存したレース数 (RA + SE の合計)
    """
    from datetime import date
    from src.scraper.jravan_client import (
        JVDataLoader, DATASPEC_RACE, OPT_NORMAL, OPT_STORED, OPT_SETUP,
    )

    if target_date is None:
        target_date = (date.today() + timedelta(days=1)).strftime("%Y%m%d")

    # JVLink の fromtime はデータ更新日時。出馬表は木曜〜金曜に配信されるが、
    # 配信タイミングのズレを吸収するため 14 日前に巻き戻す（7日だと取りこぼし事例あり）。
    target_dt   = datetime.strptime(target_date, "%Y%m%d")
    from_dt     = (target_dt - timedelta(days=14)).strftime("%Y%m%d000000")
    sid         = os.getenv("JRAVAN_SID", "")
    target_iso  = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"

    logger.info("金曜バッチ開始: 対象日=%s fromtime=%s (JRA-VAN RACE)", target_date, from_dt)
    loader = JVDataLoader(sid=sid)

    # ── Stage 1: OPT_NORMAL (差分ダウンロード) ──────────────────────
    stats = loader.load(dataspec=DATASPEC_RACE, fromtime=from_dt, option=OPT_NORMAL)
    if _count_races_for_date(target_date) > 0:
        logger.info("Stage1 OPT_NORMAL で対象日 %s のレース取得成功", target_iso)
    else:
        # ── Stage 2: OPT_STORED (ローカルキャッシュ) ────────────────
        logger.info(
            "Stage1 OPT_NORMAL 後も対象日 %s のレースが DB にない "
            "(total_read=%d, open_code=%d) → Stage2 OPT_STORED にフォールバック",
            target_iso, stats.get("total_read", 0), stats.get("open_code", 0),
        )
        stats = loader.load(dataspec=DATASPEC_RACE, fromtime=from_dt, option=OPT_STORED)

        if _count_races_for_date(target_date) > 0:
            logger.info("Stage2 OPT_STORED で対象日 %s のレース取得成功", target_iso)
        else:
            # ── Stage 3: OPT_SETUP (強制フルダウンロード) ───────────
            logger.warning(
                "Stage2 OPT_STORED 後も対象日 %s のレースが DB にない "
                "→ Stage3 OPT_SETUP (強制フルダウンロード) を実行。"
                "JRA-VANサーバーから RACE データを強制取得します。時間がかかります。",
                target_iso,
            )
            stats = loader.load(dataspec=DATASPEC_RACE, fromtime=from_dt, option=OPT_SETUP)

            final_count = _count_races_for_date(target_date)
            if final_count > 0:
                logger.info(
                    "Stage3 OPT_SETUP で対象日 %s のレース %d 件取得成功",
                    target_iso, final_count,
                )
            else:
                logger.error(
                    "Stage3 OPT_SETUP でも対象日 %s のレースを取得できませんでした。"
                    "JRA-VAN データ配信時刻前か、キャッシュが完全に空の可能性があります。"
                    "TARGET frontier JV を手動で起動してデータを同期してください。",
                    target_iso,
                )

    # save_records_to_db は "saved" キーを返さない。ra + jg + se の合計を保存件数とする。
    saved = stats.get("ra", 0) + stats.get("jg", 0) + stats.get("se", 0)
    target_in_db = _count_races_for_date(target_date)
    logger.info(
        "金曜バッチ完了: %d レコード保存 (RA=%d JG=%d SE=%d) 対象日=%s DBレース数=%d件",
        saved, stats.get("ra", 0), stats.get("jg", 0), stats.get("se", 0),
        target_date, target_in_db,
    )
    return target_in_db  # 対象日のレース数を返す（0なら取得失敗と明確に分かる）


def sync_race_results(from_date: str | None = None, stored: bool = False) -> int:
    """
    JRA-VAN RACE dataspec からレース結果（SE/HR）を取得して保存する。

    JVDataLoader が内部で JVLinkClient と DB コネクションを管理する。

    Args:
        from_date: 対象日 YYYYMMDD または YYYYMMDDHHMMSS。省略時は2日前から。
                   YYYYMMDD 形式の場合、JVLink の from_time は「データ更新日時」
                   であるため、対象日の1週間前に自動的に巻き戻す。
                   これにより当日分の更新差分を確実に取得できる。
        stored:    True の場合 OPT_STORED(4) でローカルキャッシュから読み込む。
                   OPT_NORMAL が -303 を返した際のフォールバックとしても使用。

    Returns:
        保存したレコード数
    """
    from src.scraper.jravan_client import (
        JVDataLoader, DATASPEC_RACE, OPT_NORMAL, OPT_STORED, OPT_SETUP,
    )

    sid = os.getenv("JRAVAN_SID", "")

    if from_date and len(from_date) == 8:
        # YYYYMMDD 指定時: JVLink の from_time はデータ更新日時のため
        # 対象日をそのまま渡すと更新差分なしで -303 になる場合がある。
        # 1週間前に巻き戻すことで当日レース結果の更新を確実に捕捉する。
        target_dt = datetime.strptime(from_date, "%Y%m%d")
        from_dt = (target_dt - timedelta(days=7)).strftime("%Y%m%d000000")
        logger.info("from_time を対象日(%s)の1週間前 %s に巻き戻し", from_date, from_dt)
    elif from_date:
        from_dt = from_date if len(from_date) == 14 else from_date + "000000"
    else:
        from_dt = (datetime.now() - timedelta(days=2)).strftime("%Y%m%d000000")

    loader = JVDataLoader(sid=sid)

    if stored:
        # 明示的にキャッシュ読み込みモードを指定（OPT_NORMAL で取りこぼした場合の手動再取得）
        logger.info("OPT_STORED モードで取得: fromtime=%s", from_dt)
        stats = loader.load(dataspec=DATASPEC_RACE, fromtime=from_dt, option=OPT_STORED)
        # OPT_STORED でも対象日結果が空 → OPT_SETUP で JRA-VAN サーバーから強制取得
        if from_date and len(from_date) == 8 and _count_race_results_for_date(from_date) == 0:
            logger.warning(
                "OPT_STORED でも対象日 %s の race_results が 0 → OPT_SETUP で強制再ダウンロード",
                from_date,
            )
            stats = loader.load(dataspec=DATASPEC_RACE, fromtime=from_dt, option=OPT_SETUP)
    else:
        # ── Stage 1: OPT_NORMAL (差分ダウンロード) ──────────────────
        stats = loader.load(dataspec=DATASPEC_RACE, fromtime=from_dt, option=OPT_NORMAL)

        # ── Stage 2: OPT_STORED (ローカルキャッシュ) ────────────────
        # 条件: total_read==0 OR 対象日を指定したのに race_results が 0 のまま
        # 後者は JVLink の OPT_NORMAL ポインタが既に進んでいて差分なしになる場合に発生する
        needs_stored = stats.get("total_read", 0) == 0
        if not needs_stored and from_date and len(from_date) == 8:
            if _count_race_results_for_date(from_date) == 0:
                logger.info(
                    "Stage1 OPT_NORMAL 後も対象日 %s の race_results が 0 件 "
                    "→ Stage2 OPT_STORED にフォールバック",
                    from_date,
                )
                needs_stored = True

        if needs_stored:
            if stats.get("total_read", 0) == 0:
                logger.info(
                    "Stage1 OPT_NORMAL で取得なし (open_code=%d) → Stage2 OPT_STORED にフォールバック",
                    stats.get("open_code", 0),
                )
            stats = loader.load(
                dataspec=DATASPEC_RACE, fromtime=from_dt, option=OPT_STORED
            )

        # ── Stage 3: OPT_SETUP (JRA-VAN サーバーから強制フルダウンロード) ─────
        # OPT_STORED のキャッシュにも対象日データがない場合（TARGET frontier JV 未実行等）
        still_empty = stats.get("total_read", 0) == 0
        if not still_empty and from_date and len(from_date) == 8:
            still_empty = _count_race_results_for_date(from_date) == 0
        if still_empty:
            logger.warning(
                "Stage2 OPT_STORED でも対象日 %s の結果が取得できず "
                "→ Stage3 OPT_SETUP で JRA-VAN サーバーから強制再取得します（数分かかります）",
                from_date or "unknown",
            )
            stats = loader.load(
                dataspec=DATASPEC_RACE, fromtime=from_dt, option=OPT_SETUP
            )
            if stats.get("total_read", 0) > 0:
                logger.info("Stage3 OPT_SETUP で %d レコード取得成功", stats["total_read"])
            else:
                logger.error(
                    "Stage3 OPT_SETUP でも取得できませんでした (open_code=%d)。"
                    "JRA-VAN データ配信時刻前の可能性があります。",
                    stats.get("open_code", -1),
                )

    # save_records_to_db は "saved" キーを返さない。ra + se の合計を保存件数とする。
    saved = stats.get("ra", 0) + stats.get("se", 0)
    logger.info("RACE 同期完了 (JV-Link): RA=%d SE=%d 払戻=%d (合計%d件)",
                stats.get("ra", 0), stats.get("se", 0), stats.get("payout", 0), saved)

    # ── Stage 4: netkeiba フォールバック ─────────────────────────────
    # OPT_SETUP でも対象日の結果が取得できない場合（JVLink 未配信等）
    if from_date and len(from_date) == 8 and _count_race_results_for_date(from_date) == 0:
        logger.warning(
            "JVLink 全段階失敗: 対象日 %s の race_results が 0 → netkeiba からフォールバック取得",
            from_date,
        )
        try:
            nb_saved = sync_results_from_netkeiba(from_date)
            logger.info("netkeiba フォールバック: %d レース保存 (date=%s)", nb_saved, from_date)
            saved += nb_saved
        except Exception as nb_exc:
            logger.error("netkeiba フォールバック失敗: %s", nb_exc)

    return saved


def sync_wood() -> int:
    """
    JRA-VAN WOOD dataspec から調教タイムを取得して保存する。
    """
    from src.scraper.jravan_client import JVDataLoader, DATASPEC_WOOD

    sid    = os.getenv("JRAVAN_SID", "")
    from_dt = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d000000")
    if len(from_dt) == 8:
        from_dt += "000000"
    loader = JVDataLoader(sid=sid)
    stats  = loader.load(dataspec=DATASPEC_WOOD, fromtime=from_dt)
    logger.info("WOOD 同期完了: %s", stats)
    return stats.get("saved", 0)


def sync_results_from_netkeiba(target_date: str, delay: float = 1.5) -> int:
    """
    JVLink にデータがない場合の緊急フォールバック: netkeiba からレース結果を取得して保存する。

    対象日の races は存在するが race_results が 0 のレースのみを処理する。

    Args:
        target_date: 対象日 YYYYMMDD
        delay:       リクエスト間隔（秒）

    Returns:
        保存したレース数（race_results に追加されたレース数）
    """
    from src.scraper.netkeiba import fetch_race_results, fetch_race_payouts
    from src.database.init_db import insert_race_payouts

    formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    conn = _get_conn()

    # race_results が存在しないレースを取得
    rows = conn.execute(
        """
        SELECT r.race_id FROM races r
        WHERE r.date = ?
        AND NOT EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id = r.race_id)
        ORDER BY r.race_id
        """,
        (formatted,),
    ).fetchall()
    race_ids = [r[0] for r in rows]

    if not race_ids:
        logger.info("netkeiba フォールバック: %s は既に結果あり、スキップ", formatted)
        conn.close()
        return 0

    logger.info("netkeiba フォールバック開始: %s 対象 %d レース", formatted, len(race_ids))
    saved = 0

    for race_id in race_ids:
        try:
            race_info = fetch_race_results(race_id, fetch_pedigree=False, delay=delay)
            if not race_info.results:
                logger.warning("netkeiba 結果なし: race_id=%s", race_id)
                continue

            # ── バリデーション ─────────────────────────────────────
            if len(race_info.results) < 4:
                logger.error(
                    "バリデーション失敗: 頭数不足 race_id=%s (%d頭) — スキップ",
                    race_id, len(race_info.results),
                )
                continue
            if not any(h.rank == 1 for h in race_info.results):
                logger.error(
                    "バリデーション失敗: 1着馬なし race_id=%s — スキップ", race_id
                )
                continue
            if race_info.distance == 0:
                logger.error(
                    "バリデーション失敗: 距離0m race_id=%s — スキップ", race_id
                )
                continue

            # race_results に保存
            # horse_id は netkeiba フォーマット（JRA-VAN と異なる）のため NULL で保存
            for h in race_info.results:
                try:
                    with conn:
                        conn.execute(
                            """
                            INSERT INTO race_results
                                (race_id, horse_name, rank,
                                 gate_number, horse_number,
                                 sex_age, weight_carried, jockey, trainer,
                                 finish_time, margin, popularity, win_odds,
                                 horse_weight, horse_weight_diff)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(race_id, horse_name) DO UPDATE SET
                                rank              = COALESCE(excluded.rank,      race_results.rank),
                                finish_time       = COALESCE(excluded.finish_time, race_results.finish_time),
                                margin            = COALESCE(excluded.margin,    race_results.margin),
                                popularity        = COALESCE(excluded.popularity, race_results.popularity),
                                win_odds          = COALESCE(excluded.win_odds,  race_results.win_odds),
                                horse_weight      = COALESCE(excluded.horse_weight, race_results.horse_weight),
                                horse_weight_diff = COALESCE(excluded.horse_weight_diff, race_results.horse_weight_diff)
                            """,
                            (
                                race_id,
                                h.horse_name,
                                h.rank,
                                h.gate_number,
                                h.horse_number,
                                h.sex_age,
                                h.weight_carried,
                                h.jockey,
                                h.trainer,
                                h.finish_time,
                                h.margin,
                                h.popularity,
                                h.win_odds,
                                h.horse_weight,
                                h.horse_weight_diff,
                            ),
                        )
                except sqlite3.IntegrityError as e:
                    logger.debug("race_results FK スキップ %s %s: %s", race_id, h.horse_name, e)

            # race_payouts も取得
            try:
                import time as _time
                _time.sleep(delay)
                payouts = fetch_race_payouts(race_id, delay=delay)
                if payouts:
                    insert_race_payouts(conn, race_id, payouts)
                    logger.info("払戻保存: race_id=%s (%d 件)", race_id, len(payouts))
            except Exception as pe:
                logger.warning("払戻取得失敗 race_id=%s: %s", race_id, pe)

            saved += 1
            logger.info("netkeiba 結果保存: race_id=%s (%d 頭)", race_id, len(race_info.results))

        except Exception as exc:
            logger.warning("netkeiba 結果取得失敗 race_id=%s: %s", race_id, exc)

    conn.close()
    logger.info("netkeiba フォールバック完了: %d/%d レース保存", saved, len(race_ids))
    return saved


def sync_masters(full: bool = False) -> int:
    """
    マスタデータを同期する。

    Args:
        full: True の場合 SETUP (全件)、False の場合 DIFN + BLOD (差分)
    """
    from src.scraper.jravan_client import (
        JVDataLoader,
        DATASPEC_DIFN,
        DATASPEC_BLOD,
        DATASPEC_SETUP,
    )

    sid   = os.getenv("JRAVAN_SID", "")
    total = 0
    loader = JVDataLoader(sid=sid)

    if full:
        stats = loader.load(dataspec=DATASPEC_SETUP, fromtime="")
        total += stats.get("saved", 0)
        logger.info("SETUP 完了: %s", stats)
    else:
        from_dt = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d000000")
        if len(from_dt) == 8:
            from_dt += "000000"
        for spec in (DATASPEC_DIFN, DATASPEC_BLOD):
            stats = loader.load(dataspec=spec, fromtime=from_dt)
            total += stats.get("saved", 0)
            logger.info("%s 完了: %s", spec, stats)

    return total


# ── CLI ──────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="JRA-VAN データ同期 (JVLink 専用)")
    parser.add_argument(
        "command",
        choices=["friday", "race_results", "payouts", "wood", "masters", "masters_full", "netkeiba_results"],
        help="実行するデータ同期コマンド (payouts は race_results の別名)",
    )
    parser.add_argument("--date", help="対象日 YYYYMMDD", default=None)
    parser.add_argument(
        "--stored", action="store_true",
        help="OPT_STORED(4) でローカルキャッシュから強制取得（-303 フォールバック用）",
    )
    args = parser.parse_args()

    if args.command == "friday":
        sync_friday(args.date)
    elif args.command in ("race_results", "payouts"):
        # payouts は race_results の別名: JVLink RACE dataspec に HR レコード(払戻)も含まれる
        sync_race_results(args.date, stored=getattr(args, "stored", False))
    elif args.command == "wood":
        sync_wood()
    elif args.command == "masters":
        sync_masters(full=False)
    elif args.command == "masters_full":
        sync_masters(full=True)
    elif args.command == "netkeiba_results":
        if not args.date:
            print("ERROR: --date YYYYMMDD が必要です")
            sys.exit(1)
        sync_results_from_netkeiba(args.date)


if __name__ == "__main__":
    main()
