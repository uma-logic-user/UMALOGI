"""src/ops/sns_publisher.py のユニットテスト。

集客用(観賞用)モデル Oracle / HitFocus 専用の SNS 自動フォーマッタ・
的中速報ジェネレータ・note 週次 Markdown エクスポートを検証する。

ネットワーク I/O は依存性注入(sender)で完全に排除し、純粋関数として検証する。
"""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from src.ops.sns_publisher import (
    HitFlash,
    ModelWeeklyStat,
    NoteBet,
    ShowcaseHorse,
    ShowcasePick,
    calculate_recommended_note_bets,
    compute_ornamental_weekly_stats,
    export_weekly_report,
    format_heartbeat,
    format_recommended_bets_block,
    format_x_post,
    generate_hit_flash,
    is_ornamental_model,
    recommended_unit_stake,
    send_heartbeat,
    send_hit_flash,
)


# ── フィクスチャ ──────────────────────────────────────────────────────────────


def _pick() -> ShowcasePick:
    """Oracle の標準的な集客用買い目を 1 件返す。"""
    honmei = ShowcaseHorse(
        horse_number=3, horse_name="アーバンシック", odds=4.2, win_prob=0.31, ev=1.30
    )
    hot = [
        ShowcaseHorse(
            horse_number=9,
            horse_name="ダノンデサイル",
            odds=18.0,
            win_prob=0.10,
            ev=1.80,
        ),
        ShowcaseHorse(
            horse_number=12,
            horse_name="シンエンペラー",
            odds=7.5,
            win_prob=0.16,
            ev=1.20,
        ),
    ]
    return ShowcasePick(
        race_id="202605021011",
        race_name="日本ダービー",
        venue="東京",
        model_name="Oracle",
        honmei=honmei,
        hot_horses=hot,
    )


# ── is_ornamental_model ───────────────────────────────────────────────────────


def test_is_ornamental_model_recognizes_oracle_and_hitfocus() -> None:
    assert is_ornamental_model("Oracle") is True
    assert is_ornamental_model("HitFocus") is True
    # 実弾モデルは観賞用ではない
    assert is_ornamental_model("本命") is False
    assert is_ornamental_model("卍") is False


# ── format_x_post（X最適化整形）─────────────────────────────────────────────


def test_format_x_post_normal_within_140_chars() -> None:
    """通常モードは 140 文字以内に収まり、絵文字・ハッシュタグ・本命馬を含む。"""
    post = format_x_post(_pick(), premium=False)

    assert len(post) <= 140
    assert "🏇" in post
    assert "🎯" in post
    assert "#UMALOGI" in post
    assert "#競馬予想" in post
    assert "アーバンシック" in post


def test_format_x_post_premium_is_longer_and_lists_hot_horses() -> None:
    """プレミアム長文モードは激熱馬とオッズ妙味まで展開し、通常版より情報量が多い。"""
    normal = format_x_post(_pick(), premium=False)
    premium = format_x_post(_pick(), premium=True)

    assert len(premium) > len(normal)
    assert len(premium) <= 2000
    # 激熱馬(EV最大)が含まれる
    assert "ダノンデサイル" in premium
    assert "#UMALOGI" in premium


# ── generate_hit_flash（的中ドヤ報告）────────────────────────────────────────


def test_generate_hit_flash_triggers_on_high_roi() -> None:
    """単勝 ROI 150% 超で射幸心を刺激する速報テキストを生成する。"""
    hit = HitFlash(
        race_name="日本ダービー",
        venue="東京",
        model_name="Oracle",
        bet_type="単勝",
        horse_desc="3番 アーバンシック",
        stake=1000,
        payout=4200,  # ROI 420%
    )
    text = generate_hit_flash(hit)

    assert text is not None
    assert "的中速報" in text
    assert "420" in text  # 回収率
    assert "日本ダービー" in text


def test_generate_hit_flash_flags_manbaiken() -> None:
    """払戻が 100 円あたり 10,000 円以上なら『万馬券』を明示する。"""
    hit = HitFlash(
        race_name="目黒記念",
        venue="東京",
        model_name="HitFocus",
        bet_type="三連複",
        horse_desc="3-9-12",
        stake=100,
        payout=38500,  # 100円→38,500円 = 万馬券
    )
    text = generate_hit_flash(hit)

    assert text is not None
    assert "万馬券" in text


def test_generate_hit_flash_returns_none_below_threshold() -> None:
    """ROI が閾値未満かつ万馬券でもない場合は速報を生成しない（None）。"""
    hit = HitFlash(
        race_name="平場戦",
        venue="中山",
        model_name="Oracle",
        bet_type="複勝",
        horse_desc="5番 テスト馬",
        stake=1000,
        payout=900,  # ROI 90% = 負け
    )
    assert generate_hit_flash(hit) is None


# ── send_hit_flash（依存性注入による配信）───────────────────────────────────


def test_send_hit_flash_uses_injected_sender_when_triggered() -> None:
    """的中速報が生成された場合のみ sender が呼ばれ、True を返す。"""
    calls: list[tuple[str, str]] = []

    def fake_sender(text: str, channel: str) -> bool:
        calls.append((text, channel))
        return True

    hit = HitFlash(
        race_name="日本ダービー",
        venue="東京",
        model_name="Oracle",
        bet_type="単勝",
        horse_desc="3番 アーバンシック",
        stake=1000,
        payout=4200,
    )
    result = send_hit_flash(hit, sender=fake_sender)

    assert result is True
    assert len(calls) == 1
    assert "的中速報" in calls[0][0]


def test_send_hit_flash_skips_sender_when_not_triggered() -> None:
    """閾値未満なら sender を呼ばず False を返す（無駄打ち防止）。"""
    calls: list[tuple[str, str]] = []

    def fake_sender(text: str, channel: str) -> bool:
        calls.append((text, channel))
        return True

    hit = HitFlash(
        race_name="平場戦",
        venue="中山",
        model_name="Oracle",
        bet_type="複勝",
        horse_desc="5番 テスト馬",
        stake=1000,
        payout=900,
    )
    result = send_hit_flash(hit, sender=fake_sender)

    assert result is False
    assert calls == []


# ── export_weekly_report（note 用 Markdown エクスポート）─────────────────────


def test_export_weekly_report_writes_markdown_file(tmp_path) -> None:
    """週次レポートを outputs/sns 配下へ Markdown として書き出す。"""
    stats = [
        ModelWeeklyStat(
            model_name="Oracle",
            n_bets=40,
            n_hits=14,
            total_stake=40000,
            total_return=52000,
            best_payout=38500,
            best_payout_desc="東京11R 三連複 3-9-12",
        ),
        ModelWeeklyStat(
            model_name="HitFocus",
            n_bets=30,
            n_hits=8,
            total_stake=30000,
            total_return=21000,
            best_payout=12000,
            best_payout_desc="中山10R ワイド 4-7",
        ),
    ]
    out = export_weekly_report(
        stats,
        period_label="2026-05-25 〜 2026-05-31",
        out_dir=tmp_path,
        report_date=date(2026, 5, 31),
    )

    assert out.exists()
    assert out.name == "weekly_report_20260531.md"
    body = out.read_text(encoding="utf-8")
    assert "Oracle" in body
    assert "HitFocus" in body
    assert "的中率" in body
    assert "回収率" in body
    assert "最高配当" in body
    assert "38,500" in body  # 最高配当の桁区切り
    assert "#UMALOGI" in body


def test_export_weekly_report_handles_empty_stats(tmp_path) -> None:
    """成績ゼロでも空のレポートを安全に書き出す（クラッシュしない）。"""
    out = export_weekly_report(
        [],
        period_label="2026-05-25 〜 2026-05-31",
        out_dir=tmp_path,
        report_date=date(2026, 5, 31),
    )
    assert out.exists()
    body = out.read_text(encoding="utf-8")
    assert "#UMALOGI" in body


# ── データクラスの派生プロパティ ──────────────────────────────────────────────


def test_model_weekly_stat_derived_metrics() -> None:
    stat = ModelWeeklyStat(
        model_name="Oracle",
        n_bets=40,
        n_hits=14,
        total_stake=40000,
        total_return=52000,
        best_payout=38500,
        best_payout_desc="東京11R 三連複 3-9-12",
    )
    assert stat.hit_rate == pytest.approx(35.0)
    assert stat.roi == pytest.approx(130.0)


def test_hit_flash_roi_and_manbaiken_properties() -> None:
    hit = HitFlash(
        race_name="日本ダービー",
        venue="東京",
        model_name="Oracle",
        bet_type="単勝",
        horse_desc="3番 アーバンシック",
        stake=1000,
        payout=4200,
    )
    assert hit.roi == pytest.approx(420.0)
    assert hit.is_manbaiken is False


# ── compute_ornamental_weekly_stats（DB 連動・実弾モデル除外）────────────────


def _seed_db() -> sqlite3.Connection:
    """観賞用(Oracle/HitFocus)と実弾(本命)が混在する最小 DB を構築する。

    コスト基準は pnl_accounting と同じく cost = payout - profit。
    """
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE predictions(
            id INTEGER PRIMARY KEY,
            race_id TEXT,
            model_type TEXT,
            bet_type TEXT,
            created_at TEXT,
            is_superseded INTEGER DEFAULT 0
        );
        CREATE TABLE prediction_results(
            prediction_id INTEGER,
            payout REAL,
            profit REAL,
            is_hit INTEGER
        );
        """
    )
    # Oracle: 2点・1的中（cost=100 ずつ）
    conn.execute(
        "INSERT INTO predictions VALUES (1,'202605021011','Oracle','単勝','2026-05-26',0)"
    )
    conn.execute("INSERT INTO prediction_results VALUES (1, 4200, 4100, 1)")
    conn.execute(
        "INSERT INTO predictions VALUES (2,'202605021012','Oracle','単勝','2026-05-26',0)"
    )
    conn.execute("INSERT INTO prediction_results VALUES (2, 0, -100, 0)")
    # HitFocus: 1点・万馬券的中
    conn.execute(
        "INSERT INTO predictions VALUES (3,'202605021013','HitFocus','三連複','2026-05-27',0)"
    )
    conn.execute("INSERT INTO prediction_results VALUES (3, 38500, 38400, 1)")
    # 本命（実弾モデル → 観賞用統計からは除外されるべき）
    conn.execute(
        "INSERT INTO predictions VALUES (4,'202605021014','本命','単勝','2026-05-26',0)"
    )
    conn.execute("INSERT INTO prediction_results VALUES (4, 500, 400, 1)")
    conn.commit()
    return conn


def test_compute_ornamental_weekly_stats_excludes_live_models() -> None:
    conn = _seed_db()
    stats = compute_ornamental_weekly_stats(conn)

    names = {s.model_name for s in stats}
    assert names == {"Oracle", "HitFocus"}  # 本命(実弾)は含まれない

    oracle = next(s for s in stats if s.model_name == "Oracle")
    assert oracle.n_bets == 2
    assert oracle.n_hits == 1
    assert oracle.total_stake == 200  # cost = payout - profit
    assert oracle.total_return == 4200
    assert oracle.best_payout == 4200


# ── recommended_unit_stake（EV連動・おすすめ掛け金の傾斜）─────────────────────


def test_recommended_unit_stake_low_ev_is_safe_100yen() -> None:
    """EV<1.20 は 1ユニット=100円（安心投資）。"""
    stake, units, label = recommended_unit_stake(1.05)
    assert stake == 100
    assert units == 1
    assert label == "安心投資"


def test_recommended_unit_stake_mid_ev_is_300yen() -> None:
    """1.20<=EV<1.40 は 3ユニット=300円（中勝負）。境界 1.20 を含む。"""
    assert recommended_unit_stake(1.20) == (300, 3, "中勝負")
    assert recommended_unit_stake(1.32) == (300, 3, "中勝負")
    assert recommended_unit_stake(1.39) == (300, 3, "中勝負")


def test_recommended_unit_stake_high_ev_is_hot_500yen() -> None:
    """EV>=1.40 は 5ユニット=500円（激熱勝負！）。境界 1.40 を含む。"""
    assert recommended_unit_stake(1.40) == (500, 5, "激熱勝負！")
    assert recommended_unit_stake(1.80) == (500, 5, "激熱勝負！")


def test_recommended_unit_stake_boundary_just_below_mid() -> None:
    """1.20 の直下 1.199 は安心投資（100円）に留まる。"""
    stake, units, label = recommended_unit_stake(1.199)
    assert (stake, units, label) == (100, 1, "安心投資")


# ── calculate_recommended_note_bets（買い目→推奨掛け金プラン）───────────────


def test_calculate_recommended_note_bets_assigns_per_bet_stake() -> None:
    """各買い目に EV 連動の推奨掛け金を割り当て、合計を算出する。"""
    bets = [
        NoteBet(bet_type="単勝", horse_desc="5番（マイネルエッジ）", ev=1.32),
        NoteBet(bet_type="複勝", horse_desc="5番（マイネルエッジ）", ev=1.05),
    ]
    plan = calculate_recommended_note_bets(bets)

    assert [b.stake for b in plan.bets] == [300, 100]
    assert [b.units for b in plan.bets] == [3, 1]
    assert plan.bets[0].label == "中勝負"
    assert plan.bets[1].label == "安心投資"
    assert plan.total_stake == 400  # 想定総投資額


def test_calculate_recommended_note_bets_hot_race_totals_higher() -> None:
    """激熱(EV>=1.40)買い目は 500 円で総投資額に反映される。"""
    bets = [
        NoteBet(bet_type="複勝", horse_desc="3番", ev=1.55),
        NoteBet(bet_type="複勝", horse_desc="9番", ev=1.45),
    ]
    plan = calculate_recommended_note_bets(bets)
    assert plan.total_stake == 1000
    assert all(b.stake == 500 for b in plan.bets)


def test_calculate_recommended_note_bets_empty_is_zero() -> None:
    """買い目ゼロでも安全に空プラン（総投資額0）を返す。"""
    plan = calculate_recommended_note_bets([])
    assert plan.bets == []
    assert plan.total_stake == 0


def test_recommended_bet_comment_includes_ev_and_label() -> None:
    """各買い目のコメントは期待値と勝負レベルを含む。"""
    bets = [NoteBet(bet_type="単勝", horse_desc="5番", ev=1.32)]
    plan = calculate_recommended_note_bets(bets)
    assert "1.32" in plan.bets[0].comment
    assert "中勝負" in plan.bets[0].comment


# ── format_recommended_bets_block（note 埋め込み用 Markdown）─────────────────


def test_format_recommended_bets_block_renders_expected_elements() -> None:
    """note 買い目セクション直下に差し込む推奨購入額ブロックを整形する。"""
    bets = [
        NoteBet(bet_type="単勝", horse_desc="5番（マイネルエッジ）", ev=1.32),
        NoteBet(bet_type="複勝", horse_desc="5番（マイネルエッジ）", ev=1.05),
    ]
    block = format_recommended_bets_block(calculate_recommended_note_bets(bets))

    # 見出し（要件③）
    assert "💰 AI推奨購入額" in block
    assert "1点100円ベース換算" in block
    # 各買い目の掛け金
    assert "単勝" in block
    assert "マイネルエッジ" in block
    assert "300円" in block
    assert "100円" in block
    assert "おすすめ掛け金" in block
    # 期待値コメント
    assert "★期待値1.32の中勝負" in block
    # レース合計
    assert "想定総投資額" in block
    assert "400円" in block
    # 倍率調整の免責
    assert "倍率" in block


def test_format_recommended_bets_block_handles_empty() -> None:
    """買い目が無い場合は空文字を返す（note へ余計な見出しを出さない）。"""
    assert format_recommended_bets_block(calculate_recommended_note_bets([])) == ""


# ── 死活監視ハートビート ──────────────────────────────────────────────────────


def test_format_heartbeat_contains_required_text() -> None:
    """生存報告メッセージは 🟢・時刻・定型文を含む。"""
    msg = format_heartbeat(now="2026-06-05 12:00")
    assert msg.startswith("🟢 [2026-06-05 12:00]")
    assert "UMALOGI 定期生存報告" in msg
    assert "システムは正常に稼働し、待機中です" in msg


def test_send_heartbeat_uses_injected_sender() -> None:
    """sender 注入でネットワークI/Oを排除し、生存報告テキストが渡ることを検証する。"""
    sent: list[tuple[str, str]] = []

    def fake_sender(text: str, channel: str) -> bool:
        sent.append((text, channel))
        return True

    assert send_heartbeat(sender=fake_sender, now="2026-06-05 12:00") is True
    assert len(sent) == 1
    text, channel = sent[0]
    assert "定期生存報告" in text
    assert channel == "sns"


def test_send_heartbeat_swallows_exceptions() -> None:
    """送信が例外を投げても握りつぶし False を返す（メイン処理を絶対にブロックしない）。"""

    def boom(_text: str, _channel: str) -> bool:
        raise RuntimeError("network down")

    assert send_heartbeat(sender=boom) is False


def test_send_heartbeat_returns_false_when_sender_fails() -> None:
    """sender が False（Webhook 未設定相当）を返したら False を返す。"""
    assert send_heartbeat(sender=lambda _t, _c: False) is False
