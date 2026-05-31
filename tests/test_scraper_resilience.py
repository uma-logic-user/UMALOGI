"""スクレイピング堅牢化（netkeiba 503/429/403 対策）のリグレッションテスト。

2026-05-31 本番障害（netkeiba が 503×201 / 429 / 403 / 404 を多発し着順取得が
3回リトライ後に失敗）を受けて追加した http_client / _fetch_html の冗長化、
および JVLink → netkeiba フォールバックの発火を検証する。

実際の HTTP は一切発行せず、すべてモックでオフライン実行できる。
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.scraper import http_client as hc
from src.scraper import netkeiba

_URL = "https://race.netkeiba.com/race/result.html?race_id=202604010503"


def _mk_resp(
    status: int,
    text: str = "<html>ok</html>",
    retry_after: str | None = None,
) -> MagicMock:
    """status_code とヘッダを持つモック Response を生成する。

    4xx/5xx の場合は raise_for_status() が HTTPError(response=self) を送出する。
    """
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.text = text
    resp.apparent_encoding = "utf-8"
    headers = {"Content-Type": "text/html; charset=EUC-JP"}
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    resp.headers = headers
    if status >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


# ── http_client ユニット ──────────────────────────────────────────


class TestHttpClientHelpers:
    def test_build_headers_has_browser_fields(self) -> None:
        h = hc.build_headers()
        assert h["User-Agent"] in hc.USER_AGENTS
        assert "Accept" in h and "Accept-Language" in h
        assert h["Referer"].startswith("https://")

    def test_build_headers_rotates_user_agent(self) -> None:
        # 多数生成すれば複数 UA が観測されるはず（プールが 2 種以上）
        seen = {hc.build_headers()["User-Agent"] for _ in range(50)}
        assert len(seen) >= 2

    def test_retry_after_seconds_numeric(self) -> None:
        resp = MagicMock()
        resp.headers = {"Retry-After": "7"}
        assert hc.retry_after_seconds(resp) == 7.0

    def test_retry_after_seconds_absent_or_bad(self) -> None:
        resp = MagicMock()
        resp.headers = {}
        assert hc.retry_after_seconds(resp) is None
        resp.headers = {"Retry-After": "not-a-number"}
        assert hc.retry_after_seconds(resp) is None
        assert hc.retry_after_seconds(None) is None

    def test_retry_after_capped(self) -> None:
        resp = MagicMock()
        resp.headers = {"Retry-After": "99999"}
        assert hc.retry_after_seconds(resp) == hc._MAX_RETRY_AFTER

    def test_is_retryable_status(self) -> None:
        for s in (403, 429, 500, 502, 503, 504):
            assert hc.is_retryable_status(s) is True
        for s in (200, 301, 404, 410):
            assert hc.is_retryable_status(s) is False

    def test_backoff_rate_limit_longer(self) -> None:
        # 429/503 は通常より長い待機（最低 5 秒 × factor）
        normal = hc.backoff_seconds(1, base=1.0, status=500)
        rate = hc.backoff_seconds(1, base=1.0, status=503)
        assert rate >= 5.0
        assert rate > normal

    def test_rate_limiter_sleeps_when_called_rapidly(self) -> None:
        import time as _t

        rl = hc.RateLimiter(min_interval=5.0, jitter=0.0)
        rl._last_ts = _t.monotonic()  # 直前にアクセスしたことにする
        sleeps: list[float] = []
        with patch("time.sleep", side_effect=lambda s: sleeps.append(s)):
            rl.wait()
        assert sleeps and sleeps[0] > 0


# ── _fetch_html リトライ挙動 ───────────────────────────────────────


class TestFetchHtmlResilience:
    @patch("requests.get")
    def test_retries_on_503_then_success(self, mock_get: MagicMock) -> None:
        mock_get.side_effect = [_mk_resp(503), _mk_resp(200)]
        with patch("time.sleep"):
            html = netkeiba._fetch_html(_URL, delay=0)
        assert "ok" in html
        assert mock_get.call_count == 2

    @patch("requests.get")
    def test_404_aborts_immediately(self, mock_get: MagicMock) -> None:
        mock_get.side_effect = [_mk_resp(404), _mk_resp(200)]
        with patch("time.sleep"):
            with pytest.raises(requests.HTTPError):
                netkeiba._fetch_html(_URL, delay=0)
        # 404 は恒久エラー → リトライせず 1 回で中断
        assert mock_get.call_count == 1

    @patch("requests.get")
    def test_honors_retry_after_on_429(self, mock_get: MagicMock) -> None:
        mock_get.side_effect = [_mk_resp(429, retry_after="7"), _mk_resp(200)]
        sleeps: list[float] = []
        with patch("time.sleep", side_effect=lambda s: sleeps.append(s)):
            netkeiba._fetch_html(_URL, delay=0)
        # Retry-After=7 を尊重して 7.0 秒待機していること
        assert 7.0 in sleeps

    @patch("requests.get")
    def test_exhausts_retries_raises(self, mock_get: MagicMock) -> None:
        mock_get.side_effect = [_mk_resp(503), _mk_resp(503), _mk_resp(503)]
        with patch("time.sleep"):
            with pytest.raises(requests.RequestException):
                netkeiba._fetch_html(_URL, max_retries=3, delay=0)
        assert mock_get.call_count == 3


# ── JVLink → netkeiba フォールバック ──────────────────────────────


class TestJVLinkNetkeibaFallback:
    def _mem_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE race_results (race_id TEXT, rank INTEGER)")
        return conn

    def test_jvlink_failure_triggers_netkeiba_fallback(self) -> None:
        """JVLink 同期失敗かつ着順なし → _fetch_result_from_netkeiba が呼ばれる。"""
        import scripts.fetch_race_result as fr

        conn = self._mem_conn()  # race_results は空 → with_rank=0/rank1=0
        with (
            patch.object(fr, "_run_jvlink_race_sync", return_value=False) as m_jv,
            patch("src.database.init_db.init_db", return_value=conn),
            patch.object(fr, "_fetch_result_from_netkeiba", return_value=False) as m_nb,
        ):
            ok = fr.fetch_single_race("202604010503")

        m_jv.assert_called_once()
        m_nb.assert_called_once()  # フォールバックが発火したこと
        assert ok is False  # netkeiba も未確定 → False

    def test_jvlink_success_skips_netkeiba(self) -> None:
        """JVLink で着順取得済み（rank=1 あり）なら netkeiba を呼ばない。"""
        import scripts.fetch_race_result as fr

        conn = self._mem_conn()
        conn.execute(
            "INSERT INTO race_results (race_id, rank) VALUES ('202604010503', 1)"
        )
        conn.commit()
        with (
            patch.object(fr, "_run_jvlink_race_sync", return_value=True),
            patch("src.database.init_db.init_db", return_value=conn),
            patch.object(fr, "_fetch_result_from_netkeiba") as m_nb,
            patch.object(fr, "_send_hit_flash"),
            patch.object(fr, "_try_publish_win_report"),
            patch("src.evaluation.evaluator.Evaluator"),
        ):
            ok = fr.fetch_single_race("202604010503")

        m_nb.assert_not_called()  # JVLink 成功時はフォールバック不要
        assert ok is True
