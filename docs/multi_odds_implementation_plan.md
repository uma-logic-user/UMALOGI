# マルチ券種オッズ対応 実装・検証計画書

> 実施日: 2026-05-25 — 実装完了  
> ステータス: Phase 1 実装済み / Phase 2 運用検証中

---

## 目的

単勝・複勝のみだった realtime_odds テーブルを補完し、  
馬連・ワイド・馬単・三連複・三連単・枠連 の 6 券種オッズを  
**レース単位・買い目単位**で DB に蓄積して期待値（EV）計算へ組み込む。

---

## 実装済み内容（Phase 1 完了）

### 1. DB スキーマ拡張（migration #17）

| ファイル | 変更内容 |
|---------|---------|
| `src/database/schema.py` | `multi_odds` テーブル DDL 追加、インデックス 2 件 |
| `src/database/init_db.py` | migration #17 `_migrate_create_multi_odds()`、`MultiOddsEntry` dataclass、`insert_multi_odds()` 追加 |

**テーブル定義 (`multi_odds`)**

```sql
CREATE TABLE IF NOT EXISTS multi_odds (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id      TEXT    NOT NULL,
    bet_type     TEXT    NOT NULL CHECK (bet_type IN ('枠連','馬連','ワイド','馬単','三連複','三連単')),
    combination  TEXT    NOT NULL,   -- "1-2" / "1→2" / "1-2-3" 等
    odds         REAL,               -- 最低オッズ（ワイドは下限）
    odds_max     REAL,               -- ワイドの上限オッズ（その他は NULL）
    popularity   INTEGER,
    recorded_at  TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
    UNIQUE (race_id, bet_type, combination, recorded_at)
)
```

- `UNIQUE` 制約によりスナップショット履歴を保持（同時刻・同買い目の重複のみ除外）
- ワイドは `odds`（下限）+ `odds_max`（上限）の 2 フィールド構造

### 2. スクレイパー実装

| ファイル | 内容 |
|---------|------|
| `src/scraper/multi_odds_scraper.py` | netkeiba API (type=3〜8) からオッズ取得 |

**対応 API type**

| type | 券種 | 組み合わせ形式 |
|------|------|--------------|
| 3 | 枠連 | `"1-2"` |
| 4 | 馬連 | `"1-2"` |
| 5 | ワイド | `"1-2"` (odds_max あり) |
| 6 | 馬単 | `"1→2"` |
| 7 | 三連複 | `"1-2-3"` |
| 8 | 三連単 | `"1→2→3"` |

**モックモード**: 環境変数 `UMALOGI_MOCK_MULTI_ODDS=1` で HTTP 通信なし。

**エントリポイント**

```python
from src.scraper.multi_odds_scraper import scrape_and_save_multi_odds

inserted = scrape_and_save_multi_odds("202605311001")
print(f"{inserted} 件挿入")
```

### 3. フロントエンド API 拡張

| ファイル | 変更内容 |
|---------|---------|
| `web/src/types/race.ts` | `MultiOddsEntry` / `MultiOddsSnapshot` 型追加 |
| `web/src/app/api/races/[race_id]/route.ts` | `multi_odds` フィールドを `/api/races/[race_id]` レスポンスに追加 |

**API レスポンス例**

```json
{
  "race_id": "202605311001",
  "multi_odds": {
    "馬連": [
      { "combination": "3-7", "odds": 15.4, "odds_max": null, "popularity": 1, "recorded_at": "2026-05-31 09:30:00" }
    ],
    "ワイド": [
      { "combination": "3-7", "odds": 3.5, "odds_max": 5.2, "popularity": 2, "recorded_at": "2026-05-31 09:30:00" }
    ]
  }
}
```

### 4. テスト

| ファイル | テスト数 | 結果 |
|---------|---------|------|
| `tests/test_multi_odds_db.py` | 17 件 | PASS |
| `tests/test_multi_odds_scraper.py` | 25 件 | PASS |
| mypy 型チェック | — | エラーなし |

---

## Phase 2: 運用統合タスク（未着手）

### 2-1. スクレイパー手動実行検証

レース当日（土日）に実 race_id で `scrape_and_save_multi_odds()` を実行し、  
DB にオッズデータが正常に蓄積されることを確認する。

```bash
# 実行コマンド例（race_id は当日のものに置き換え）
py -c "
from src.scraper.multi_odds_scraper import scrape_and_save_multi_odds
n = scrape_and_save_multi_odds('202605311001')
print(f'挿入: {n} 件')
"
```

**確認クエリ**

```sql
SELECT bet_type, COUNT(*) AS cnt, MIN(odds), MAX(odds)
FROM multi_odds
WHERE race_id = '202605311001'
GROUP BY bet_type;
```

### 2-2. scheduler.py への統合

`job_fetch_multi_odds()` を追加し、レース当日の指定時刻に自動実行する。

```python
# scripts/scheduler.py に追加予定
def job_fetch_multi_odds() -> None:
    """当日全レースのマルチ券種オッズを取得して DB に保存する。"""
    from src.scraper.multi_odds_scraper import scrape_and_save_multi_odds
    for race_id in get_todays_race_ids():
        try:
            n = scrape_and_save_multi_odds(race_id, delay=2.0)
            logger.info("multi_odds %s: %d 件", race_id, n)
        except Exception as e:
            logger.error("multi_odds 取得失敗 %s: %s", race_id, e)
```

実行タイミング案: レース発走 30 分前 × レース数回

### 2-3. EV 計算への統合

`src/ml/ev_features.py` に `get_multi_odds_map(race_id)` を追加し、  
馬連・三連複の期待値算出でマルチオッズを特徴量として使用する。

### 2-4. UI 表示

`web/src/components/RaceDetail.tsx` にマルチオッズパネルを追加し、  
買い目ごとのオッズ一覧をタブ形式で表示する。

---

## 弱点台帳（docs/7_weakness_ledger.md）

本実装で解消される弱点:
- W-??? マルチ券種 EV 計算の不在（単勝・複勝のみ対応）→ Phase 2-3 完了で解消

---

## 参照ファイル

- `src/database/schema.py`
- `src/database/init_db.py`
- `src/scraper/multi_odds_scraper.py`
- `web/src/types/race.ts`
- `web/src/app/api/races/[race_id]/route.ts`
- `tests/test_multi_odds_db.py`
- `tests/test_multi_odds_scraper.py`
