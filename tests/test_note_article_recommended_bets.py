"""scripts/generate_note_article.generate_article のおすすめ掛け金 埋め込み統合テスト。

note 有料エリアの「買い目セクション」直下に EV 連動の AI 推奨購入額ブロックが
差し込まれることを、最小シードDB で検証する（Discord / SHAP は使わない）。
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.generate_note_article import generate_article


def _seed_conn() -> sqlite3.Connection:
    """卍複勝 1 予想（EV=1.32・3頭流し）の最小レースを構築する。"""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE races(
            race_id TEXT, race_name TEXT, venue TEXT, race_number INTEGER,
            distance INTEGER, surface TEXT, condition TEXT, date TEXT, weather TEXT
        );
        CREATE TABLE predictions(
            id INTEGER PRIMARY KEY, race_id TEXT, model_type TEXT, bet_type TEXT,
            combination_json TEXT, expected_value REAL, recommended_bet INTEGER,
            confidence REAL, notes TEXT
        );
        CREATE TABLE prediction_results(
            prediction_id INTEGER, payout REAL, profit REAL, is_hit INTEGER
        );
        CREATE TABLE race_results(
            race_id TEXT, horse_number INTEGER, horse_name TEXT, jockey TEXT,
            trainer TEXT, win_odds REAL, weight_carried REAL, horse_weight INTEGER,
            horse_weight_diff INTEGER, gate_number INTEGER, sex_age TEXT,
            popularity INTEGER, rank INTEGER
        );
        """
    )
    rid = "202605021011"
    conn.execute(
        "INSERT INTO races VALUES (?,?,?,?,?,?,?,?,?)",
        (rid, "日本ダービー", "東京", 11, 2400, "芝", "良", "2026-05-31", "晴"),
    )
    conn.execute(
        "INSERT INTO predictions VALUES (1,?, '卍', '複勝', '[[5],[9],[12]]', 1.32, 300, 0.7, '')",
        (rid,),
    )
    horses = [
        (5, "マイネルエッジ", "ルメール", 4.2, 1),
        (9, "ダノンデサイル", "川田", 7.5, 3),
        (12, "シンエンペラー", "武豊", 9.1, 4),
    ]
    for hn, name, jky, odds, pop in horses:
        conn.execute(
            "INSERT INTO race_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (rid, hn, name, jky, "美浦 厩舎", odds, 57.0, 480, 0, hn, "牡4", pop, 0),
        )
    conn.commit()
    return conn


def test_generate_article_embeds_recommended_bets_after_picks() -> None:
    """買い目セクションの直下に AI 推奨購入額ブロックが挿入される。"""
    conn = _seed_conn()
    try:
        article = generate_article(conn, "202605021011", max_picks=3, use_shap=False)
    finally:
        conn.close()

    # ブロックが存在する
    assert "💰 AI推奨購入額（1点100円ベース換算）" in article
    assert "おすすめ掛け金" in article
    assert "想定総投資額" in article
    # EV=1.32 → 300円（中勝負）が各複勝点に適用される
    assert "★期待値1.32の中勝負" in article
    assert "300円" in article

    # 「買い目セクション」直下であること（推奨買い目テーブルより後ろ）
    picks_idx = article.index("推奨買い目")
    rec_idx = article.index("AI推奨購入額")
    assert picks_idx < rec_idx
