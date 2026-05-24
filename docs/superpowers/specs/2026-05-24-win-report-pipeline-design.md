# 設計書：的中報告レポート生成パイプライン

**作成日**: 2026-05-24  
**ステータス**: 承認済み  
**スコープ**: `src/ops/win_report.py`（新規）+ `scripts/fetch_race_result.py`（最小変更）

---

## 概要

レース結果評価後に「的中」が確認された場合、以下を自動実行するパイプラインを追加する。

1. `data/results/YYYYMMDD/{race_id}_win_report.txt` を生成（タイトル・本文・X投稿テキスト含む）
2. Discord 予想チャンネルへ Embed（的中詳細＋EV根拠）＋ X投稿テキスト（コードブロック）を送信
3. note.com に分析記事を下書き保存（Playwright 経由）

的中がない場合は何もしない。失敗しても既存の Hit Flash 送信には一切影響しない。

---

## データフロー

```
scripts/fetch_race_result.py
  └─ evaluate_race(conn, race_id) → EvaluationResult
       ├─ _send_hit_flash()                   [既存・変更なし]
       └─ _try_publish_win_report()            [新規・1関数追加]
             └─ hit_count > 0 の場合のみ
                  └─ win_report.publish_win_report(result, race_id, conn)
                       ├─ 1. generate_win_report_file(data) → Path
                       │      data/results/YYYYMMDD/{race_id}_win_report.txt
                       ├─ 2. _send_discord_report(data)
                       │      予想ch Embed: 的中詳細 + EV値 + 乖離スコア
                       │      予想ch Text:  X投稿テキスト（コードブロック）
                       └─ 3. _post_note_draft(data, predictions)
                              note.com 下書き保存（save_draft）
                              失敗時 → system ch に「手動確認」アラート
```

---

## ファイル構成

### 新規ファイル

| ファイル | 役割 |
|---------|------|
| `src/ops/win_report.py` | レポート生成・Discord送信・Note投稿のメイン実装 |
| `tests/test_win_report.py` | ユニットテスト（7件以上） |

### 出力ファイル

```
data/results/
  YYYYMMDD/
    {race_id}_win_report.txt    # 3セクション1ファイル形式
```

`win_report.txt` フォーマット：

```
=== TITLE ===
【的中実績】期待値最適化アルゴリズムによる選別成功

=== BODY ===
本日、{venue}{race_number}R「{race_name}」において、アルゴリズムが市場の歪みを捉え的中を達成しました。

推奨根拠：対象馬のEV値は{top_ev:.2f}であり、ROIフィルター通過後の確実な選別を行いました。

■ 的中買い目
  {bet_type}  {combo}  ¥{payout:,} 払戻（投資¥{invested:,} / 利益{sign}¥{profit:,}）

本日の合計ROI：{roi:.1f}％

■ 市場オッズ vs AIスコア（検証データ）
  馬番{n}  市場オッズ {odds:.1f}倍 / AI推奨EV {ev:.2f} / 乖離スコア {gap:+.2f}

=== X_POST ===
🎉【的中】{venue}{race_number}R {race_name}
EV={top_ev:.2f}の歪みを捉えて{bet_type}的中

投資¥{invested:,} → 払戻¥{payout:,}（ROI {roi:.0f}%）

期待値アルゴリズムが市場の非効率を見抜きました📊

#競馬予想 #期待値アルゴリズム #的中実績 #{race_name}
```

---

## `src/ops/win_report.py` — 公開API

```python
@dataclass
class WinReportData:
    race_id:        str
    race_name:      str
    venue:          str
    race_number:    int
    date_str:       str           # YYYY-MM-DD
    hit_items:      list[Any]     # EvaluationResult.hits の is_hit=True のもの（HitItem 型）
    total_invested: float
    total_payout:   float
    roi:            float
    top_ev:         float         # 的中した買い目の中で最高の expected_value
    ev_vs_odds:     list[dict]    # [{horse_number, odds, ev, gap}, ...]
                                  # odds: race_results.odds（単勝オッズ）または realtime_odds から取得
                                  # gap = ev - (1.0 / odds)（AIスコアと市場期待値の乖離）

def generate_win_report_file(data: WinReportData) -> Path
    """data/results/YYYYMMDD/{race_id}_win_report.txt を生成して Path を返す。"""

def build_x_post(data: WinReportData) -> str
    """280字以内の X 投稿テキストを返す。"""

def build_note_draft(
    data: WinReportData,
    predictions: list[dict],
) -> tuple[str, str]
    """(note_title, note_body) の Markdown を返す。"""

def publish_win_report(
    result: Any,               # EvaluationResult
    race_id: str,
    conn: sqlite3.Connection,
) -> None
    """メインエントリーポイント。例外はすべて内部でキャッチしてログに落とす。"""
```

---

## `scripts/fetch_race_result.py` への変更

`_send_hit_flash()` 呼び出しの直後に1行追加するだけ。

```python
# 変更前
_send_hit_flash(result, result.race_name)

# 変更後
_send_hit_flash(result, result.race_name)
_try_publish_win_report(result, race_id, conn)
```

新規追加する防壁関数（同ファイル内）：

```python
def _try_publish_win_report(result: Any, race_id: str, conn: Any) -> None:
    """的中時のみ win_report パイプラインを起動する。失敗しても例外を漏らさない。"""
    if not hasattr(result, "hit_count") or result.hit_count == 0:
        return
    try:
        from src.ops.win_report import publish_win_report
        publish_win_report(result, race_id, conn)
    except Exception as e:
        logger.warning("[WinReport] 失敗（スキップ）: %s", e)
```

`fetch_for_date()` 内も同様に1行追加。

---

## Discord通知の仕様

既存 `_send_hit_flash()` の Embed とは**別に**、`publish_win_report()` から追加送信する。

### メッセージ① — Embed（的中詳細＋EV根拠）

| フィールド | 内容 |
|-----------|------|
| title | `🏆 的中レポート  {venue}{race_number}R「{race_name}」` |
| description | 的中買い目一覧（bet_type / combo / payout / profit） |
| fields | EV値 / 乖離スコア / ROI |
| footer | `{date} \| {report_file_path} に保存済み` |
| color | 万馬券(≥10万): `0xFF4500` / 高配当(≥1万): `0xFFD700` / 通常: `0x43B581` |

送信先: `DISCORD_WEBHOOK_URL`（予想チャンネル）

### メッセージ② — X投稿テキスト（コードブロック）

```
📋 X投稿テキスト（コピーしてそのまま貼り付けてください）

```
{x_post_text}
```
```

送信先: 同じく `DISCORD_WEBHOOK_URL`（予想チャンネル）

---

## Note 分析記事の構成

```markdown
# 【的中実績】{YYYY}/{MM}/{DD} {venue}{race_number}R「{race_name}」— EV期待値アルゴリズム選別成功

## 的中サマリー
| 項目 | 内容 |
|------|------|
| 買い目 | {bet_type} {combo} |
| 投資 | ¥{invested:,} |
| 払戻 | ¥{payout:,} |
| ROI | {roi:.1f}% |

## なぜこのレースを選んだか
本日、{venue}{race_number}R「{race_name}」において、アルゴリズムが市場の歪みを捉え的中を達成しました。
推奨根拠：対象馬のEV値は{top_ev:.2f}であり、ROIフィルター通過後の確実な選別を行いました。

## 市場オッズ vs AIスコア（比較データ）
| 馬番 | 市場オッズ | AI推奨EV | 乖離スコア |
|------|----------|---------|-----------|
| {horse_number} | {odds:.1f}倍 | {ev:.2f} | {gap:+.2f} |

## フィルター貢献度
- {model_type}: EV={ev:.2f} で最高スコア
- ROIフィルター: 通過（EV > 1.0）
- ウマスギフィルター: 適用済み

## 免責事項
本記事は統計的期待値に基づく投資記録であり、的中を保証するものではありません。
投資は自己責任でお願いします。

*UMALOGI — AI 競馬予測プラットフォーム*
```

---

## エラーハンドリング

| 障害箇所 | 挙動 |
|---------|------|
| `data/results/` ディレクトリ作成失敗 | `logger.error` + 全処理スキップ |
| ファイル書き込み失敗 | `logger.warning` + 以降の処理は続行 |
| Discord 送信失敗 | `logger.warning` + スキップ（Hit Flash は別途送信済み） |
| Note 投稿失敗（Playwright） | `logger.warning` + system ch に「手動確認」アラート送信 |
| `predictions` テーブルに該当なし | EV値・乖離スコアを `N/A` として記事生成は続行 |
| `publish_win_report()` 全体 | `_try_publish_win_report()` がキャッチ。呼び出し元に例外を絶対に漏らさない |

---

## テスト仕様（`tests/test_win_report.py`）

| テスト名 | 検証内容 |
|---------|---------|
| `test_generate_win_report_file_creates_correct_content` | ファイルに TITLE / BODY / X_POST セクションが存在する |
| `test_build_x_post_under_280_chars` | X投稿テキストが280字以内 |
| `test_build_x_post_includes_required_hashtags` | `#競馬予想 #期待値アルゴリズム #的中実績` が含まれる |
| `test_build_note_draft_contains_ev_and_roi` | Note本文にEV値とROIが含まれる |
| `test_publish_win_report_skips_when_no_hits` | `hit_count=0` のとき何も実行されない |
| `test_publish_win_report_handles_playwright_failure_gracefully` | `save_draft` 例外時に `logger.warning` が呼ばれ、呼び出し元に例外が漏れない |
| `test_ev_vs_odds_table_populated_from_predictions` | `predictions` テーブルから `ev_vs_odds` が正しく構築される |

モック対象: `save_draft`（Playwright）・`requests.post`（Discord webhook）・`sqlite3.Connection`

---

## 影響ファイル一覧

| ファイル | 変更種別 |
|---------|---------|
| `src/ops/win_report.py` | 新規作成 |
| `tests/test_win_report.py` | 新規作成 |
| `scripts/fetch_race_result.py` | 小変更（`_try_publish_win_report` 追加 + 呼び出し2箇所） |
| `docs/1_prediction_logic.md` | Changelog 追記 |
| `docs/6_special_notes.md` | Changelog 追記 |

---

## 非対応事項（スコープ外）

- Discord ボタンコンポーネント（Interactions API）による X 自動投稿
- 的中なしレースのレポート生成
- 週次まとめへの的中レース自動集約（既存 `post_weekly_note_draft.py` の拡張は別タスク）
