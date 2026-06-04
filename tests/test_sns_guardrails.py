"""
tests/test_sns_guardrails.py — 例外セーフティネットの TDD テスト

テスト対象:
  - send_hit_flash() フォールバック
      sender が失敗 / 例外を投げた場合に fallback_sender を呼び出す
  - _ensure_paywall() ペイウォール安全ガード
      allocations がある場合に🔒が必ず含まれることを保証する

保証する不変条件:
  1. sender 失敗時はフォールバックメッセージに「フォールバック」が含まれる
  2. sender 例外時でもプロセスをクラッシュさせない
  3. sender 成功時はフォールバックを呼ばない
  4. 閾値未満のヒットはフォールバックを呼ばない（送信対象外のため）
  5. allocations がある場合、🔒がないテキストに安全ガードが発動する
  6. 🔒が既にある場合、ガードはテキストを変更しない
  7. allocations がない場合、ガードは発動しない
"""

from __future__ import annotations

import pytest

from src.ops.note_generator import _ensure_paywall
from src.ops.sns_publisher import HitFlash, send_hit_flash


# ─────────────────────────────────────────────────────────────────────
# フィクスチャ
# ─────────────────────────────────────────────────────────────────────


def _high_roi_hit() -> HitFlash:
    """ROI >= 150% (=HIT_FLASH_MIN_ROI) で generate_hit_flash が非 None を返す HitFlash。"""
    return HitFlash(
        race_name="日本ダービー",
        venue="東京",
        model_name="Oracle",
        bet_type="複勝",
        horse_desc="3番",
        stake=100,
        payout=300,  # roi = 300% >= 150%
    )


def _below_threshold_hit() -> HitFlash:
    """ROI < 150% で generate_hit_flash が None を返す HitFlash。"""
    return HitFlash(
        race_name="東京9R",
        venue="東京",
        model_name="Oracle",
        bet_type="単勝",
        horse_desc="7番",
        stake=100,
        payout=120,  # roi = 120% < 150%
    )


# ─────────────────────────────────────────────────────────────────────
# タスク1: send_hit_flash フォールバック
# ─────────────────────────────────────────────────────────────────────


def test_send_hit_flash_calls_fallback_when_sender_fails():
    """sender が False を返したとき fallback_sender が呼ばれる。"""
    fallback_calls: list[str] = []

    def fail_sender(text: str, ch: str) -> bool:
        return False

    def capture_fallback(text: str, ch: str) -> bool:
        fallback_calls.append(text)
        return True

    send_hit_flash(
        _high_roi_hit(), sender=fail_sender, fallback_sender=capture_fallback
    )

    assert len(fallback_calls) == 1


def test_send_hit_flash_fallback_message_contains_fallback_label():
    """フォールバックに渡されるメッセージに「フォールバック」が含まれる。"""
    fallback_calls: list[str] = []

    def capture_fallback(text: str, ch: str) -> bool:
        fallback_calls.append(text)
        return True

    send_hit_flash(
        _high_roi_hit(), sender=lambda t, c: False, fallback_sender=capture_fallback
    )

    assert len(fallback_calls) == 1
    assert "フォールバック" in fallback_calls[0]


def test_send_hit_flash_calls_fallback_when_sender_raises():
    """sender が例外を投げたとき fallback_sender が呼ばれる。"""
    fallback_calls: list[str] = []

    def raise_sender(text: str, ch: str) -> bool:
        raise ConnectionError("network error")

    def capture_fallback(text: str, ch: str) -> bool:
        fallback_calls.append(text)
        return True

    # 例外が呼び出し元に伝播しないこと
    result = send_hit_flash(
        _high_roi_hit(), sender=raise_sender, fallback_sender=capture_fallback
    )

    assert len(fallback_calls) == 1
    assert "フォールバック" in fallback_calls[0]


def test_send_hit_flash_does_not_raise_when_sender_raises():
    """sender が例外を投げてもプロセスをクラッシュさせない。"""

    def raise_sender(text: str, ch: str) -> bool:
        raise RuntimeError("simulated crash")

    try:
        send_hit_flash(_high_roi_hit(), sender=raise_sender)
    except Exception as e:
        pytest.fail(f"send_hit_flash が例外を再送出した: {e}")


def test_send_hit_flash_no_fallback_when_sender_succeeds():
    """sender が True を返したとき fallback_sender は呼ばれない。"""
    fallback_calls: list[str] = []

    def capture_fallback(text: str, ch: str) -> bool:
        fallback_calls.append(text)
        return True

    send_hit_flash(
        _high_roi_hit(), sender=lambda t, c: True, fallback_sender=capture_fallback
    )

    assert len(fallback_calls) == 0


def test_send_hit_flash_returns_false_on_sender_failure():
    """sender が失敗したとき戻り値は False。"""
    result = send_hit_flash(_high_roi_hit(), sender=lambda t, c: False)
    assert result is False


def test_send_hit_flash_no_fallback_when_below_threshold():
    """閾値未満のヒット（generate_hit_flash=None）はフォールバックを呼ばない。"""
    fallback_calls: list[str] = []

    def capture_fallback(text: str, ch: str) -> bool:
        fallback_calls.append(text)
        return True

    # below-threshold → generate_hit_flash returns None → return False immediately
    send_hit_flash(
        _below_threshold_hit(),
        sender=lambda t, c: False,
        fallback_sender=capture_fallback,
    )

    assert len(fallback_calls) == 0


# ─────────────────────────────────────────────────────────────────────
# タスク2: _ensure_paywall ペイウォール安全ガード
# ─────────────────────────────────────────────────────────────────────


def test_ensure_paywall_inserts_marker_when_missing():
    """allocations がある場合に 🔒 がないテキストには先頭にマーカーを挿入する。"""
    text = "# タイトル\n\n## 💰 配分表"
    result = _ensure_paywall(text, allocations_present=True)
    assert "🔒" in result


def test_ensure_paywall_does_not_change_text_when_marker_present():
    """🔒 がすでにあるテキストは変更しない。"""
    text = "# タイトル\n🔒 有料\n## 💰 配分表"
    result = _ensure_paywall(text, allocations_present=True)
    assert result == text


def test_ensure_paywall_noop_when_no_allocations():
    """allocations がない場合はガードを適用しない。"""
    text = "# タイトル\n\n本日は準備中です。"
    result = _ensure_paywall(text, allocations_present=False)
    assert result == text


def test_ensure_paywall_inserted_content_is_at_start():
    """ガード発動時、マーカーはテキスト先頭に挿入される（元の本文は後ろ）。"""
    original = "# タイトル\n内容"
    result = _ensure_paywall(original, allocations_present=True)
    assert result.index("🔒") < result.index("# タイトル")


def test_ensure_paywall_no_double_insert():
    """ガードが2回呼ばれても 🔒 は重複しない。"""
    text = "# タイトル\n\n## 💰 配分表"
    first = _ensure_paywall(text, allocations_present=True)
    second = _ensure_paywall(first, allocations_present=True)
    assert second.count("🔒") == first.count("🔒")
