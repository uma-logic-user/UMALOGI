"""
ウマニティ自動予想投稿モジュール

UMALOGIが生成した本日の買い目を Playwright でウマニティに自動入力する。

必須環境変数 (.env):
  UMANITY_EMAIL    : ウマニティ登録メールアドレス
  UMANITY_PASSWORD : ウマニティパスワード

使用例:
  python -m src.ops.umanity_uploader                  # 本日の予想を全件投稿
  python -m src.ops.umanity_uploader --date 20260419  # 指定日の予想を投稿
  python -m src.ops.umanity_uploader --dry-run        # 投稿なしで動作確認

依存パッケージ (要インストール):
  pip install playwright python-dotenv
  playwright install chromium
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ── 環境変数ロード ─────────────────────────────────────────────────

def _load_env() -> tuple[str, str]:
    """UMANITY_EMAIL / UMANITY_PASSWORD を環境変数から取得する。"""
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=_ROOT / ".env", override=False)
    except ImportError:
        pass  # python-dotenv 未インストールでも os.environ から読む

    email    = os.environ.get("UMANITY_EMAIL", "")
    password = os.environ.get("UMANITY_PASSWORD", "")

    if not email or not password:
        raise EnvironmentError(
            "UMANITY_EMAIL / UMANITY_PASSWORD が設定されていません。\n"
            ".env ファイルまたは環境変数に設定してください。"
        )
    return email, password


# ── DB から本日の予想を取得 ────────────────────────────────────────

def _fetch_today_predictions(target_date: str) -> list[dict]:
    """
    指定日の predictions + races を結合して返す。

    Args:
        target_date: "YYYYMMDD" 形式

    Returns:
        list of {race_id, race_name, venue, race_number, model_type,
                 bet_type, combination_json, recommended_bet, notes}
    """
    db_path = _ROOT / "data" / "umalogi.db"
    if not db_path.exists():
        raise FileNotFoundError(f"DB が見つかりません: {db_path}")

    date_fmt = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
            p.id             AS prediction_id,
            p.race_id,
            r.race_name,
            r.venue,
            r.race_number,
            p.model_type,
            p.bet_type,
            p.combination_json,
            p.recommended_bet,
            p.notes,
            p.expected_value
        FROM predictions p
        JOIN races r ON p.race_id = r.race_id
        WHERE r.date = ?
        ORDER BY r.race_id, p.model_type, p.bet_type
        """,
        (date_fmt,),
    ).fetchall()
    conn.close()

    return [dict(r) for r in rows]


# ── Playwright セッション ─────────────────────────────────────────

class UmanityUploader:
    """
    Playwright を使ってウマニティに予想を自動投稿するクラス。

    使用するページ:
      ログイン   : https://umanity.jp/members/login.php
      予想一覧   : https://umanity.jp/racedata/race_5.php?code={race_id_10}
      予想フォーム: 各レースの「予想する」ボタン → 予想入力モーダル
    """

    LOGIN_URL = "https://umanity.jp/"
    RACE_URL  = "https://umanity.jp/racedata/race_5.php?code={race_code}"

    def __init__(
        self,
        email:    str,
        password: str,
        headless: bool = True,
        delay_sec: float = 1.5,
    ) -> None:
        self._email    = email
        self._password = password
        self._headless = headless
        self._delay    = delay_sec
        self._page     = None
        self._browser  = None
        self._pw       = None
        self._pw_cm    = None

    # ── コンテキストマネージャ ─────────────────────────────────

    def __enter__(self) -> "UmanityUploader":
        from playwright.sync_api import sync_playwright  # type: ignore[import]
        self._pw_cm   = sync_playwright()
        self._pw      = self._pw_cm.__enter__()
        self._browser = self._pw.chromium.launch(headless=self._headless)
        context       = self._browser.new_context(
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

    # ── ログイン ──────────────────────────────────────────────

    def _find_input(self, selectors: list[str], timeout: int = 5000) -> str | None:
        """複数セレクタを順に試して、最初に見つかったものを返す。"""
        for sel in selectors:
            try:
                self._page.wait_for_selector(sel, timeout=timeout)
                return sel
            except Exception:
                continue
        return None

    def login(self) -> None:
        """
        ウマニティにログインする。

        フロー:
          1. トップページ (https://umanity.jp/) に遷移
          2. ページ右上の「ログイン」リンクをクリック → モーダルが出現
          3. input[name="userid"] にメールアドレス、input[name="password"] にパスワードを入力
          4. input[name="submit"] (画像ボタン) をクリックして送信
        """
        logger.info("ウマニティ ログイン開始: %s", self._email)
        page = self._page
        page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        time.sleep(self._delay)

        # 右上の「ログイン」テキストリンクをクリックしてモーダルを開く
        page.click('text=ログイン', timeout=10000)
        time.sleep(1.0)

        # メールアドレス入力 (name="userid")
        page.wait_for_selector('input[name="userid"]', timeout=8000)
        page.fill('input[name="userid"]', self._email)
        logger.info("メール入力完了")

        # パスワード入力 (name="password")
        page.fill('input[name="password"]', self._password)
        logger.info("パスワード入力完了")

        # 送信ボタン (type="image" / name="submit")
        # #blackmask オーバーレイがクリックを遮断するため JS 経由でフォームを送信する
        page.evaluate("""() => {
            const submit = document.querySelector('input[name=\"submit\"]');
            if (submit) {
                document.querySelector('input[name=\"mode\"]').value = 'login';
                submit.closest('form').submit();
            }
        }""")
        page.wait_for_load_state("domcontentloaded")
        time.sleep(self._delay)

        # ログイン成功確認
        content = page.content()
        if "ログアウト" not in content and "logout" not in content.lower() and "mypage" not in content.lower():
            ss_path = _ROOT / "data" / "login_debug.png"
            page.screenshot(path=str(ss_path))
            logger.error("ログイン失敗。スクリーンショット: %s  URL: %s", ss_path, page.url)
            raise RuntimeError(
                "ログインに失敗しました。メールアドレスまたはパスワードを確認してください。"
            )
        logger.info("ログイン成功")

    # ── レース ID 変換 ────────────────────────────────────────

    @staticmethod
    def _race_id_to_code(race_id: str) -> str:
        """
        UMALOGI race_id (12桁) → ウマニティ race_code (10桁) に変換。

        UMALOGI:   202603010801  (YYYY + 場コード2 + 回次2 + 日次2 + レース番号2)
        ウマニティ: 2026030108    (上位10桁、末尾のレース番号を除外)

        NOTE: ウマニティのレースコード仕様に合わせて調整が必要な場合がある。
              実際のURLを確認して適宜修正すること。
        """
        return race_id[:10]

    # ── 予想フォーム入力 ──────────────────────────────────────

    def post_prediction(
        self,
        race_id:       str,
        bet_type:      str,
        combination_json: str | None,
        notes:         str | None,
        dry_run:       bool = False,
    ) -> bool:
        """
        1件の予想をウマニティに投稿する。

        Args:
            race_id:          UMALOGI race_id
            bet_type:         券種（"単勝" / "複勝" / ...）
            combination_json: 買い目 JSON
            notes:            メモ（AIの根拠）
            dry_run:          True なら実際の送信ボタンを押さない

        Returns:
            成功なら True、スキップなら False
        """
        page = self._page
        race_code = self._race_id_to_code(race_id)
        url = self.RACE_URL.format(race_code=race_code)

        logger.info("予想投稿: race_id=%s bet_type=%s url=%s", race_id, bet_type, url)
        page.goto(url, wait_until="domcontentloaded")
        time.sleep(self._delay)

        # 「予想する」ボタンを探してクリック
        try:
            page.click('a:has-text("予想する"), button:has-text("予想する")', timeout=5000)
            time.sleep(self._delay)
        except Exception:
            logger.warning("「予想する」ボタンが見つかりません: %s", url)
            return False

        # 券種タブを選択
        bet_type_map = {
            "単勝": "tansho", "複勝": "fukusho",
            "馬連": "umaren", "ワイド": "wide",
            "馬単": "umatan", "三連複": "sanrenpuku", "三連単": "sanrentan",
        }
        bt_key = bet_type_map.get(bet_type)
        if bt_key:
            try:
                page.click(f'[data-bet="{bt_key}"], a[href*="{bt_key}"]', timeout=3000)
                time.sleep(0.5)
            except Exception:
                logger.debug("券種タブ切り替えスキップ: %s", bt_key)

        # combination_json から馬番を取得して入力
        import json
        combos: list[list[int]] = []
        if combination_json:
            try:
                raw = json.loads(combination_json)
                if raw and isinstance(raw[0], list):
                    combos = raw
                elif raw:
                    combos = [raw]
            except Exception:
                pass

        for combo in combos[:1]:  # 1組み合わせ目を入力（フォーム構造に依存）
            for horse_num in combo:
                try:
                    # チェックボックスまたは入力フィールドを探す
                    page.check(f'input[type="checkbox"][value="{horse_num}"]', timeout=2000)
                except Exception:
                    try:
                        page.fill(f'input[name="horse_{horse_num}"]', "1", timeout=1000)
                    except Exception:
                        logger.debug("馬番 %d の入力フィールドが見つかりません", horse_num)

        # メモ欄に AI の根拠を入力
        comment = f"[UMALOGI AI] {bet_type} {notes or ''}"[:200]
        try:
            page.fill('textarea[name="comment"], textarea[name="memo"]', comment, timeout=2000)
        except Exception:
            pass

        if dry_run:
            logger.info("[DRY-RUN] 投稿をスキップ: race_id=%s bet_type=%s", race_id, bet_type)
            return True

        # 送信
        try:
            page.click(
                'button[type="submit"]:has-text("投稿"), input[type="submit"]:has-text("投稿")',
                timeout=5000,
            )
            page.wait_for_load_state("domcontentloaded")
            time.sleep(self._delay)
            logger.info("投稿完了: race_id=%s bet_type=%s", race_id, bet_type)
            return True
        except Exception as e:
            logger.error("投稿失敗: %s", e)
            return False


# ── バッチ処理 ─────────────────────────────────────────────────────

def run_upload(
    target_date: str,
    dry_run:     bool = False,
    headless:    bool = True,
    model_filter: str | None = None,
) -> dict[str, int]:
    """
    指定日の予想を一括投稿する。

    Args:
        target_date:  "YYYYMMDD"
        dry_run:      True なら実際の送信なし
        headless:     True なら GUI なしで実行
        model_filter: "卍" / "本命" でフィルタ（None = 全件）

    Returns:
        {"total": N, "success": N, "skip": N, "error": N}
    """
    email, password = _load_env()
    predictions = _fetch_today_predictions(target_date)

    if model_filter:
        predictions = [p for p in predictions if p["model_type"].startswith(model_filter)]

    logger.info(
        "投稿対象: %d 件 (date=%s model=%s)",
        len(predictions), target_date, model_filter or "all",
    )

    stats = {"total": len(predictions), "success": 0, "skip": 0, "error": 0}

    # EV > 1.0 の予想のみ投稿（低EVの買い目は除外）
    high_ev = [p for p in predictions if (p["expected_value"] or 0) >= 1.0]
    logger.info("EV >= 1.0 フィルタ後: %d 件", len(high_ev))

    with UmanityUploader(email, password, headless=headless) as uploader:
        try:
            uploader.login()
        except RuntimeError as e:
            logger.error("ログイン失敗: %s", e)
            stats["error"] = stats["total"]
            return stats

        for pred in high_ev:
            try:
                ok = uploader.post_prediction(
                    race_id=pred["race_id"],
                    bet_type=pred["bet_type"],
                    combination_json=pred["combination_json"],
                    notes=pred["notes"],
                    dry_run=dry_run,
                )
                if ok:
                    stats["success"] += 1
                else:
                    stats["skip"] += 1
            except Exception as exc:
                logger.error(
                    "投稿中に例外: race_id=%s bet_type=%s: %s",
                    pred["race_id"], pred["bet_type"], exc,
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
    parser.add_argument("--date",       help="対象日 YYYYMMDD（省略時=今日）")
    parser.add_argument("--dry-run",    action="store_true", help="実際の送信なし")
    parser.add_argument("--model",      help="卍 / 本命 でフィルタ")
    parser.add_argument("--no-headless", action="store_true", help="ブラウザを表示する")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    args   = _parse_args()
    target = args.date or date.today().strftime("%Y%m%d")

    stats = run_upload(
        target_date=target,
        dry_run=args.dry_run,
        headless=not args.no_headless,
        model_filter=args.model,
    )

    mode = "[DRY-RUN] " if args.dry_run else ""
    print(f"\n{'='*55} {mode}")
    print(f"  ウマニティ投稿結果 (date={target})")
    print(f"{'='*55}")
    print(f"  投稿対象  : {stats['total']:5d} 件")
    print(f"  成功      : {stats['success']:5d} 件")
    print(f"  スキップ  : {stats['skip']:5d} 件")
    print(f"  エラー    : {stats['error']:5d} 件")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
