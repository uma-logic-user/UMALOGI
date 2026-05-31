"""
ウマニティ自動予想投稿モジュール

UMALOGIが生成した本日の買い目を Playwright でウマニティ予想コロシアムに自動投稿する。

必須環境変数 (.env):
  UMANITY_EMAIL    : ウマニティ登録メールアドレス
  UMANITY_PASSWORD : ウマニティパスワード

使用例:
  python -m src.ops.umanity_uploader                  # 本日の予想を全件投稿
  python -m src.ops.umanity_uploader --date 20260419  # 指定日の予想を投稿
  python -m src.ops.umanity_uploader --dry-run        # 投稿なしで動作確認

URL 構造（DOM調査 2026-05-05 確定版）:
  ログイン     : https://umanity.jp/
  予想登録一覧 : https://umanity.jp/coliseum/registration_list.php
  コロシアム予想: https://umanity.jp/coliseum/enrollee_list.php?race_id=RACE_ID_16
  掲示板コメント: https://umanity.jp/racedata/race_8.php?code=RACE_ID_16

レース ID 変換:
  UMALOGI 12桁 (YYYYJJKKNNRR) → Umanity 16桁 (YYYYMMDDJJKKNNrr)
  MM/DD は races.date から取得する
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_DEBUG_DIR = _ROOT / "outputs" / "debug"
_DEBUG_DIR.mkdir(parents=True, exist_ok=True)


# ── 環境変数ロード ──────────────────────────────────────────────────


def _load_env() -> tuple[str, str]:
    """UMANITY_EMAIL / UMANITY_PASSWORD を環境変数（.env）から取得する。

    Returns:
        (email, password) のタプル。

    Raises:
        EnvironmentError: UMANITY_EMAIL または UMANITY_PASSWORD が未設定の場合。
    """
    try:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=_ROOT / ".env", override=False)
    except ImportError:
        pass

    email = os.environ.get("UMANITY_EMAIL", "")
    password = os.environ.get("UMANITY_PASSWORD", "")

    if not email or not password:
        raise EnvironmentError(
            "UMANITY_EMAIL / UMANITY_PASSWORD が設定されていません。\n"
            ".env ファイルまたは環境変数に設定してください。"
        )
    return email, password


# ── DB から本日の予想を取得 ────────────────────────────────────────


def _fetch_today_predictions(target_date: str) -> list[dict[str, Any]]:
    """指定日の predictions + races を結合して返す。

    1レースにつき EV 最高の予想 1件のみを返す（重複排除）。
    卍複勝を優先し、次に expected_value 降順で選択する。
    races.date も含めて Umanity ID 変換に使用する。

    Args:
        target_date: 対象日 YYYYMMDD 形式。

    Returns:
        予想情報の辞書リスト（race_id 昇順）。

    Raises:
        FileNotFoundError: umalogi.db が見つからない場合。
    """
    db_path = _ROOT / "data" / "umalogi.db"
    if not db_path.exists():
        raise FileNotFoundError(f"DB が見つかりません: {db_path}")

    date_fmt = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # レースごとに EV 最高の予想 1件のみ取得（卍複勝を優先）
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                p.id             AS prediction_id,
                p.race_id,
                r.race_name,
                r.venue,
                r.race_number,
                r.date           AS race_date,
                p.model_type,
                p.bet_type,
                p.combination_json,
                p.recommended_bet,
                p.notes,
                p.expected_value,
                ROW_NUMBER() OVER (
                    PARTITION BY p.race_id
                    ORDER BY
                        CASE WHEN p.model_type LIKE '卍%' AND p.bet_type='複勝' THEN 0 ELSE 1 END,
                        COALESCE(p.expected_value, 0) DESC
                ) AS rn
            FROM predictions p
            JOIN races r ON p.race_id = r.race_id
            WHERE r.date = ?
              AND (p.expected_value IS NULL OR p.expected_value >= 1.0)
        )
        SELECT * FROM ranked WHERE rn = 1
        ORDER BY race_id
        """,
        (date_fmt,),
    ).fetchall()
    conn.close()

    return [dict(r) for r in rows]


# ── ID 変換 ───────────────────────────────────────────────────────


def umalogi_to_umanity_race_id(race_id: str, race_date: str) -> str:
    """
    UMALOGI race_id (12桁) → Umanity race_id (16桁) に変換。

    UMALOGI: YYYY + JJ(venue) + KK(round) + NN(day) + RR(race)
    Umanity: YYYY + MM + DD + JJ(venue) + KK(round) + NN(day) + RR(race)

    race_date は "YYYY-MM-DD" または "YYYYMMDD" 形式。

    検証済み（2026-05-05 DOM調査）:
      UMALOGI 202608030412 + date 2026-05-03 → Umanity 2026050308030412 ✅
    """
    if len(race_id) != 12:
        raise ValueError(f"UMALOGI race_id は12桁である必要があります: {race_id}")

    if "-" in race_date:
        mm = race_date[5:7]
        dd = race_date[8:10]
    else:
        mm = race_date[4:6]
        dd = race_date[6:8]

    year = race_id[:4]
    venue = race_id[4:6]
    round_ = race_id[6:8]
    day = race_id[8:10]
    race = race_id[10:12]

    return f"{year}{mm}{dd}{venue}{round_}{day}{race}"


# ── Playwright セッション ─────────────────────────────────────────


class UmanityUploader:
    """
    Playwright でウマニティ予想コロシアムに予想を自動投稿するクラス。

    投稿戦略（優先順）:
      1. 予想コロシアム登録: enrollee_list.php?race_id=RACE_ID_16
         → ROI・ランキング追跡対象。週末開催前のみ利用可能。
      2. 掲示板コメント（フォールバック）: race_8.php?code=RACE_ID_16
         → 常時投稿可能。ランキング対象外だが可視性あり。
    """

    LOGIN_URL = "https://umanity.jp/"
    COLISEUM_URL = "https://umanity.jp/coliseum/registration_list.php"
    ENROLLEE_URL = "https://umanity.jp/coliseum/enrollee_list.php?race_id={race_id_16}"
    RACE_BOARD_URL = "https://umanity.jp/racedata/race_8.php?code={race_id_16}"

    def __init__(
        self,
        email: str,
        password: str,
        headless: bool = True,
        delay_sec: float = 1.5,
    ) -> None:
        """UmanityUploader を初期化する。

        Args:
            email:     ウマニティ登録メールアドレス。
            password:  ウマニティパスワード。
            headless:  True の場合ブラウザをヘッドレスモードで起動する。
            delay_sec: リクエスト間の待機秒数。
        """
        self._email = email
        self._password = password
        self._headless = headless
        self._delay = delay_sec
        # Playwright 実体（未インストール環境では None のまま）。
        # 実行時は __enter__ で各オブジェクトが入るため Any で型付けする。
        self._page: Any = None
        self._browser: Any = None
        self._pw_cm: Any = None
        self._pw: Any = None

    def __enter__(self) -> "UmanityUploader":
        """コンテキストマネージャー開始: Playwright ブラウザを起動してページを準備する。

        Returns:
            自分自身（UmanityUploader インスタンス）。
        """
        from playwright.sync_api import sync_playwright  # type: ignore[import]

        self._pw_cm = sync_playwright()
        self._pw = self._pw_cm.__enter__()
        self._browser = self._pw.chromium.launch(headless=self._headless)
        context = self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        self._page = context.new_page()
        return self

    def __exit__(self, *_: object) -> None:
        """コンテキストマネージャー終了: ブラウザと Playwright を安全にクリーンアップする。"""
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
        if self._pw:
            try:
                self._pw.stop()
            except Exception:
                pass

    def _save_screenshot(self, name: str) -> Path:
        """スクリーンショットを outputs/debug/ に保存して Path を返す。

        Args:
            name: ファイル名に使うラベル文字列（例: "login_fail"）。

        Returns:
            保存した PNG ファイルのパス。スクリーンショット取得失敗時もパスを返す。
        """
        path = _DEBUG_DIR / f"umanity_{name}.png"
        try:
            self._page.screenshot(path=str(path))
        except Exception:
            pass
        return path

    def _notify_error(self, step: str, error: str, ss_path: Path | None = None) -> None:
        """エラー時に Discord へ手動介入要請を送信する（例外は握り潰す）。

        Args:
            step:    エラーが発生したステップ名（例: "ウマニティ ログイン"）。
            error:   エラーメッセージ。
            ss_path: 添付するスクリーンショットの Path。省略可。
        """
        try:
            from src.notification.discord_notifier import DiscordNotifier

            DiscordNotifier().notify_intervention_required(
                step=step,
                error=error,
                action=(
                    f"ウマニティのDOMが変更された可能性があります。\n"
                    f"手動で予想を入力してください: {self.LOGIN_URL}"
                ),
                screenshot_path=ss_path,
            )
        except Exception as e:
            logger.warning("[Umanity] Discord 通知失敗: %s", e)

    # ── ログイン ────────────────────────────────────────────────

    def login(self) -> None:
        """
        ウマニティにログインする。

        フロー（DOM調査 2026-05-05 確定版）:
          1. https://umanity.jp/ にアクセス
          2. ページ右上「ログイン」テキストリンクをクリック → モーダル表示
          3. input[name="userid"] にメール、input[name="password"] にパスワードを入力
          4. #blackmask がクリックをブロックするため JS 経由でフォームを送信
          5. networkidle 待機後に「ログアウト」テキスト有無でログイン確認
        """
        logger.info("[Umanity] ログイン開始: %s", self._email)
        page = self._page

        page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        time.sleep(self._delay)

        page.click("text=ログイン", timeout=10000)
        time.sleep(1.0)

        page.wait_for_selector('input[name="userid"]', timeout=8000)
        page.fill('input[name="userid"]', self._email)
        page.fill('input[name="password"]', self._password)

        with page.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            page.evaluate("""() => {
                const submit = document.querySelector('input[name="submit"]');
                const mode   = document.querySelector('input[name="mode"]');
                if (submit && mode) {
                    mode.value = 'login';
                    submit.closest('form').submit();
                }
            }""")

        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        time.sleep(self._delay)

        content = page.content()
        if "ログアウト" not in content and "logout" not in content.lower():
            ss = self._save_screenshot("login_fail")
            self._notify_error(
                "ウマニティ ログイン",
                f"認証後に「ログアウト」テキストが見つかりません (url={page.url})",
                ss,
            )
            raise RuntimeError(
                "ウマニティログイン失敗。.env の認証情報を確認してください。"
            )

        logger.info("[Umanity] ログイン成功")

    # ── コロシアム予想登録 ────────────────────────────────────────

    def post_coliseum_prediction(
        self,
        race_id: str,
        race_date: str,
        bet_type: str,
        combos: list[list[int]],
        comment: str,
        dry_run: bool = False,
    ) -> bool:
        """
        予想コロシアムに予想を登録する。

        予想登録フォームは発走前のみ表示される。発走後・平日は enrollee_list が
        予想受付終了状態になるため、その場合は False を返す（掲示板コメントを検討）。

        Returns:
            True = 投稿成功（または dry_run）, False = フォーム非表示（受付終了）
        """
        page = self._page

        try:
            race_id_16 = umalogi_to_umanity_race_id(race_id, race_date)
        except ValueError as e:
            logger.warning("[Umanity] race_id 変換失敗: %s", e)
            return False

        url = self.ENROLLEE_URL.format(race_id_16=race_id_16)
        logger.info("[Umanity] コロシアム予想ページへ遷移: %s", url)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(self._delay)
        except Exception as e:
            ss = self._save_screenshot(f"coliseum_nav_{race_id}")
            self._notify_error("ウマニティ コロシアム遷移", str(e), ss)
            return False

        # 予想登録ボタンを探す（発走前のみ表示）
        # ナビゲーションの「予想登録」リンク（registration_list.php）を除外するため
        # メインコンテンツ内のボタン・リンクのみを対象にする
        entry_selectors = [
            # DOM調査 2026-05-05: club/entry.php は「入会」フォームのため除外
            # 週末レース開放後に確認が必要なセレクタ群
            'a[href*="registration_entry"]',
            'a[href*="yosou_entry"]',
            'a[href*="coliseum/entry"]',
            'button:has-text("予想を登録する")',
            'input[value="予想を登録する"]',
            'a:has-text("予想を登録する")',
            'a:has-text("予想登録する")',
            # ナビ除外: href に race_id を含むリンクのみ（ナビリンクは race_id を持たない）
            f'a[href*="race_id={umalogi_to_umanity_race_id(race_id, race_date)}"][href*="registration"]',
            f'a[href*="race_id={umalogi_to_umanity_race_id(race_id, race_date)}"][href*="entry"]',
        ]

        entry_found = False
        for sel in entry_selectors:
            try:
                if page.locator(sel).count() > 0:
                    logger.info("[Umanity] 予想登録ボタン発見: %s", sel)
                    entry_found = True
                    if not dry_run:
                        page.locator(sel).first.click(timeout=5000)
                        time.sleep(self._delay)
                    break
            except Exception:
                continue

        if not entry_found:
            logger.info(
                "[Umanity] 予想登録ボタン未表示（受付終了または平日）: %s", race_id
            )
            return False

        if dry_run:
            ss = self._save_screenshot(f"coliseum_dryrun_{race_id}")
            logger.info("[Umanity] [DRY-RUN] コロシアム予想フォーム到達 SS: %s", ss)
            return True

        # 予想フォーム入力（週末本番時に DOM を確認して調整）
        try:
            # 馬番チェックボックス
            for combo in combos[:1]:
                for horse_num in combo:
                    for sel in [
                        f'input[type="checkbox"][value="{horse_num}"]',
                        f'input[name="horse_{horse_num}"]',
                        f'[data-horse="{horse_num}"]',
                    ]:
                        try:
                            if page.locator(sel).count() > 0:
                                page.locator(sel).first.check()
                                break
                        except Exception:
                            continue

            # コメント入力
            for sel in [
                'textarea[name="comment"]',
                'textarea[name="memo"]',
                "textarea",
            ]:
                try:
                    if page.locator(sel).count() > 0:
                        page.fill(sel, comment[:200])
                        break
                except Exception:
                    continue

            # 送信
            for sel in [
                'button[type="submit"]',
                'input[type="submit"]',
                'button:has-text("登録")',
                'button:has-text("送信")',
            ]:
                try:
                    if page.locator(sel).count() > 0:
                        page.locator(sel).first.click(timeout=5000)
                        page.wait_for_load_state("domcontentloaded", timeout=10000)
                        logger.info("[Umanity] コロシアム予想登録完了: %s", race_id)
                        return True
                except Exception:
                    continue

            ss = self._save_screenshot(f"coliseum_submit_fail_{race_id}")
            self._notify_error(
                "ウマニティ コロシアム送信",
                "送信ボタンが見つかりません（DOM変更の可能性）",
                ss,
            )
            return False

        except Exception as e:
            ss = self._save_screenshot(f"coliseum_error_{race_id}")
            self._notify_error("ウマニティ コロシアム予想入力", str(e), ss)
            return False

    # ── 掲示板コメント（フォールバック） ───────────────────────────

    def post_board_comment(
        self,
        race_id: str,
        race_date: str,
        comment: str,
        dry_run: bool = False,
    ) -> bool:
        """
        race_8.php の掲示板にAI予想コメントを投稿する（コロシアムのフォールバック）。

        フォーム構造（DOM調査 2026-05-05 確定版）:
          - textarea[name="content"] : 投稿テキスト
          - input[type="submit"][value="投稿する"] : 送信ボタン
          - action は javascript:submitPost() なので JS 経由で送信
        """
        page = self._page

        try:
            race_id_16 = umalogi_to_umanity_race_id(race_id, race_date)
        except ValueError as e:
            logger.warning("[Umanity] race_id 変換失敗: %s", e)
            return False

        url = self.RACE_BOARD_URL.format(race_id_16=race_id_16)
        logger.info("[Umanity] 掲示板ページへ遷移: %s", url)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(self._delay)
        except Exception as e:
            ss = self._save_screenshot(f"board_nav_{race_id}")
            self._notify_error("ウマニティ 掲示板遷移", str(e), ss)
            return False

        # wait_for_selector の代わりに locator で確認（タイムアウト誤検知を回避）
        textarea = page.locator('textarea[name="content"]')
        try:
            textarea.wait_for(state="visible", timeout=8000)
            textarea.fill(comment[:500])
        except Exception as e:
            # フォールバック: count() で存在確認してから fill
            try:
                if textarea.count() > 0:
                    textarea.first.fill(comment[:500])
                else:
                    logger.warning(
                        "[Umanity] 掲示板テキストエリアが見つかりません: %s", e
                    )
                    return False
            except Exception as e2:
                logger.warning("[Umanity] 掲示板テキストエリア入力失敗: %s", e2)
                return False

        if dry_run:
            ss = self._save_screenshot(f"board_dryrun_{race_id}")
            logger.info("[Umanity] [DRY-RUN] 掲示板コメント入力完了 SS: %s", ss)
            return True

        try:
            page.evaluate("submitPost()")
            try:
                page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception:
                pass
            time.sleep(self._delay)
            logger.info("[Umanity] 掲示板コメント投稿完了: %s", race_id)
            return True
        except Exception as e:
            ss = self._save_screenshot(f"board_submit_fail_{race_id}")
            self._notify_error("ウマニティ 掲示板コメント送信", str(e), ss)
            return False

    # ── コメント文生成 ────────────────────────────────────────────

    @staticmethod
    def _build_comment(
        bet_type: str,
        combination_json: str | None,
        notes: str | None,
        expected_value: float | None,
    ) -> str:
        """ウマニティへの投稿用 AI 予想テキストを生成する。

        Args:
            bet_type:         券種名（"複勝" / "馬連" 等）。
            combination_json: 買い目の JSON 文字列（例: [[5, 3], ...]）。
            notes:            予想根拠テキスト。省略可。
            expected_value:   期待値。省略可。

        Returns:
            投稿用テキスト文字列（500文字以内を想定）。
        """
        combos: list = []
        if combination_json:
            try:
                raw = json.loads(combination_json)
                combos = raw if raw else []
            except Exception:
                pass

        combo_str = ""
        if combos:
            first = combos[0]
            if isinstance(first, list):
                combo_str = "-".join(str(n) for n in first)
            else:
                combo_str = str(first)

        ev_str = f" (EV={expected_value:.2f})" if expected_value else ""
        note_str = f"\n{notes}" if notes else ""

        return (
            f"🤖 UMALOGI AI予想\n"
            f"▶ {bet_type} {combo_str}{ev_str}\n"
            f"JRA-VANデータ×LightGBM 期待値ベース絞り込み{note_str}"
        )


# ── バッチ処理 ─────────────────────────────────────────────────────


def run_upload(
    target_date: str,
    dry_run: bool = False,
    headless: bool = True,
    model_filter: str | None = None,
) -> dict[str, int]:
    """
    指定日の予想を一括投稿する。

    Returns:
        {"total": N, "coliseum_ok": N, "board_ok": N, "skip": N, "error": N}
    """
    email, password = _load_env()
    predictions = _fetch_today_predictions(target_date)

    if model_filter:
        predictions = [
            p for p in predictions if p["model_type"].startswith(model_filter)
        ]

    # EV >= 1.0 の予想のみ投稿
    high_ev = [p for p in predictions if (p["expected_value"] or 0) >= 1.0]
    logger.info(
        "[Umanity] 投稿対象: %d 件 → EV>=1.0: %d 件 (date=%s model=%s)",
        len(predictions),
        len(high_ev),
        target_date,
        model_filter or "all",
    )

    stats: dict[str, int] = {
        "total": len(high_ev),
        "coliseum_ok": 0,
        "board_ok": 0,
        "skip": 0,
        "error": 0,
    }

    if not high_ev:
        logger.info("[Umanity] 投稿対象なし。接続をスキップします。")
        return stats

    with UmanityUploader(email, password, headless=headless) as uploader:
        try:
            uploader.login()
        except RuntimeError as e:
            logger.error("[Umanity] ログイン失敗: %s", e)
            stats["error"] = stats["total"]
            return stats

        for pred in high_ev:
            race_id = pred["race_id"]
            race_date = pred["race_date"]  # "YYYY-MM-DD"
            bet_type = pred["bet_type"]
            ev = pred["expected_value"]

            comment = UmanityUploader._build_comment(
                bet_type=bet_type,
                combination_json=pred["combination_json"],
                notes=pred["notes"],
                expected_value=ev,
            )

            combos: list[list[int]] = []
            if pred["combination_json"]:
                try:
                    raw = json.loads(pred["combination_json"])
                    if raw and isinstance(raw[0], list):
                        combos = raw
                    elif raw:
                        combos = [[n] for n in raw]
                except Exception:
                    pass

            try:
                # Primary: コロシアム予想登録を試みる
                ok = uploader.post_coliseum_prediction(
                    race_id=race_id,
                    race_date=race_date,
                    bet_type=bet_type,
                    combos=combos,
                    comment=comment,
                    dry_run=dry_run,
                )
                if ok:
                    stats["coliseum_ok"] += 1
                    continue

                # Fallback: 掲示板コメント
                ok = uploader.post_board_comment(
                    race_id=race_id,
                    race_date=race_date,
                    comment=comment,
                    dry_run=dry_run,
                )
                if ok:
                    stats["board_ok"] += 1
                else:
                    stats["skip"] += 1

            except Exception as exc:
                logger.error(
                    "[Umanity] 予想投稿中に例外: race_id=%s bet_type=%s: %s",
                    race_id,
                    bet_type,
                    exc,
                )
                stats["error"] += 1

    return stats


# ── CLI ────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ウマニティ自動予想投稿",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python -m src.ops.umanity_uploader                   # 本日の予想を全件投稿
  python -m src.ops.umanity_uploader --dry-run         # 投稿なしで動作確認
  python -m src.ops.umanity_uploader --date 20260419   # 指定日の予想を投稿
  python -m src.ops.umanity_uploader --model 卍        # 卍モデルのみ投稿
  python -m src.ops.umanity_uploader --no-headless     # ブラウザ画面を表示
""",
    )
    parser.add_argument("--date", help="対象日 YYYYMMDD（省略時=今日）")
    parser.add_argument("--dry-run", action="store_true", help="実際の送信なし")
    parser.add_argument("--model", help="卍 / 本命 でフィルタ")
    parser.add_argument("--no-headless", action="store_true", help="ブラウザを表示する")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    args = _parse_args()
    target = args.date or date.today().strftime("%Y%m%d")

    stats = run_upload(
        target_date=target,
        dry_run=args.dry_run,
        headless=not args.no_headless,
        model_filter=args.model,
    )

    mode = "[DRY-RUN] " if args.dry_run else ""
    print(f"\n{'=' * 55} {mode}")
    print(f"  ウマニティ投稿結果 (date={target})")
    print(f"{'=' * 55}")
    print(f"  対象件数      : {stats['total']:5d} 件")
    print(f"  コロシアム成功: {stats['coliseum_ok']:5d} 件")
    print(f"  掲示板成功    : {stats['board_ok']:5d} 件")
    print(f"  スキップ      : {stats['skip']:5d} 件")
    print(f"  エラー        : {stats['error']:5d} 件")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
