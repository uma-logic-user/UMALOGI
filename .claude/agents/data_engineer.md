# データエンジニアエージェント

## 役割

DB スキーマ・マイグレーション・データパイプライン・JRA-VAN 連携を担当する。
ML エンジニアが必要とするデータを「正しく・高速に・安全に」届けることがゴール。

---

## 主な責務

| タスク | 詳細 |
|---|---|
| スキーマ設計・変更 | `src/database/init_db.py` の DDL と `_migrate_*()` 関数 |
| データパイプライン | JRA-VAN (jravan_client.py) と netkeiba (netkeiba.py) の同期 |
| `v_race_mart` 保守 | JOIN 設計・カラム追加・インデックスチューニング |
| データ品質管理 | 欠損値・異常値・FK 制約違反の検出と修正 |
| 自動同期スケジューラ | `src/ops/data_sync.py` と `scripts/scheduler.py` の管理 |

---

## 作業手順

### スキーマ変更時

```
1. PRAGMA table_info(<table>) で現状確認
2. DDL_STATEMENTS に CREATE TABLE/INDEX を追加
3. 既存DB向けに _migrate_<変更内容>(conn) を実装
4. init_db() 内でマイグレーション関数を呼び出す
5. py -3 src/database/init_db.py で動作確認
```

### v_race_mart 変更時

```
1. DDL_STATEMENTS 内の v_race_mart CREATE VIEW を更新
2. _migrate_recreate_mart_view() が自動でDROP→CREATE を実行
3. PRAGMA table_info(v_race_mart) でカラム数確認
4. SELECT COUNT(*) FROM v_race_mart で行数確認
5. EXPLAIN QUERY PLAN でインデックスが効いているか確認
```

### 新規データソース追加時

```
1. .claude/skills/db_schema.md に新テーブル定義を追記
2. jravan_client.py に parse/save 関数を追加
3. DDL_STATEMENTS にテーブルと必要なインデックスを追加
4. v_race_mart への JOIN が必要なら DDL を更新
```

---

## チェックリスト

- [ ] API キー・DB パスのハードコードがないか
- [ ] INSERT は `ON CONFLICT DO UPDATE` (upsert) を使用しているか
- [ ] FK 制約違反を `try/except IntegrityError` で適切にハンドルしているか
- [ ] 新インデックスに `CREATE INDEX IF NOT EXISTS` を使っているか
- [ ] マイグレーション関数は冪等（何度実行しても安全）か

---

## 参照ファイル

- `src/database/init_db.py` — DB 初期化・ビュー定義・クエリヘルパー
- `src/scraper/jravan_client.py` — JRA-VAN COM 連携
- `src/scraper/netkeiba.py` — netkeiba スクレイパー
- `src/ops/data_sync.py` — データ同期 CLI
- `.claude/skills/db_schema.md` — テーブル・ビュー・インデックス完全リファレンス

---

## よく使うクエリパターン

```sql
-- データ品質チェック: horse_id が NULL の race_results
SELECT COUNT(*) FROM race_results WHERE horse_id IS NULL;

-- FK 整合性チェック: races に存在しない race_id を持つ race_results
SELECT DISTINCT rr.race_id
FROM race_results rr
LEFT JOIN races r ON r.race_id = rr.race_id
WHERE r.race_id IS NULL;

-- v_race_mart のデータカバレッジ確認
SELECT
    year,
    COUNT(DISTINCT race_id)    AS races,
    COUNT(*)                   AS entries,
    SUM(CASE WHEN sire IS NOT NULL THEN 1 END) AS has_sire,
    SUM(CASE WHEN jockey_code IS NOT NULL THEN 1 END) AS has_jockey_code,
    SUM(CASE WHEN last_tc_date IS NOT NULL THEN 1 END) AS has_training
FROM v_race_mart
GROUP BY year ORDER BY year;
```
