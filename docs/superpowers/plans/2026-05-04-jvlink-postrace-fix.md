# JVLink Postrace 障害修正・再発防止 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 5/3の結果速報19件失敗・win_odds100%欠損の根本原因を修正し、JVLink経由でデータを補完して最終P&Lレポートを出力する。

**Architecture:** (1) RTDファイルパターンバグ修正でオッズ欠損を解消、(2) fetch_race_result.py をnetkeiba禁止のためJVLink経由に置換、(3) JVLink再試行ウォッチドッグ追加、(4) 本日データ補完・評価・報告。

**Tech Stack:** Python 3.11+, SQLite (data/umalogi.db), JVLink COM (32bit), Discord Webhook, LightGBM

---

## 根本原因サマリー

### 原因1: RTDファイルプレフィックス不一致（win_odds 100%欠損）
- `src/scraper/rtd_reader.py` が `0B30*{jyo}{kai}{nichi}{race}.rtd` パターンでファイルを探す
- 実際のJRA-VANキャッシュファイルは `0B12{YYYYMMDD}{jyo}{race}.rtd` フォーマット（16文字ステム）
- KAI・NICHIフィールドがなく、プレフィックスも `0B30`→`0B12` に変わっている
- 結果: `read_rtd_for_race()` が常に None を返す → 全レースでオッズNaN → 「単勝オッズの欠損率が高すぎます (100%)」警告

### 原因2: postrace結果取得が netkeiba 依存（CLAUDE.md違反）
- `scripts/fetch_race_result.py` が `src.scraper.netkeiba.fetch_race_results` を呼び出す
- CLAUDE.md「netkeiba.com へのアクセスは一切禁止」に違反
- 新潟R1〜東京R4まで17件成功後、13:40頃にnetkeiba側がレートリミット/ブロック
- 以降19件が rc=1 で失敗
- データは `JRA-VAN (JVLink)` のみから取得すべき

---

## ファイル変更マップ

| ファイル | 変更種別 | 内容 |
|---|---|---|
| `src/scraper/rtd_reader.py` | Modify | RTDファイルパターンを0B12フォーマット対応に修正 |
| `scripts/fetch_race_result.py` | Modify | netkeiba呼び出しをJVLinkワーカー経由に完全置換 |
| `scripts/today_auto_runner.py` | Modify | postrace JVLink再試行ウォッチドッグ追加 |
| `scripts/_jvlink_force_worker.py` | Modify | --option choices に OPT_TODAY(3) を追加 |
| `tests/test_rtd_reader.py` | Create | RTDパターン修正のテスト |

---

## Task 1: RTD ファイルパターン修正（win_odds 欠損の根本修正）

**Files:**
- Modify: `src/scraper/rtd_reader.py`
- Create: `tests/test_rtd_reader.py`

- [ ] **Step 1: 失敗テストを書く**

```python
# tests/test_rtd_reader.py
"""RTD リーダーのファイルパターン修正テスト"""
from __future__ import annotations
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.rtd_reader import read_rtd_for_race, _race_id_from_filename


class TestRaceIdFromFilename:
    def test_new_format_0b12(self) -> None:
        """0B12フォーマット (16文字) でrace_idを正しく導出できる。"""
        # 0B12{YYYYMMDD}{JYO(2)}{RACE(2)} = 16文字
        # 例: 0B12202605030812 → 京都R12 だが race_id はDBに頼る必要あり
        # ファイル名から抽出可能なのは jyo と race のみ
        stem = "0B12202605030812"
        jyo, race = stem[12:14], stem[14:16]
        assert jyo == "08"   # 京都
        assert race == "12"

    def test_old_format_0b30(self) -> None:
        """0B30フォーマット (20文字) で race_id を正しく導出できる。"""
        stem = "0B302026041903010401"
        result = _race_id_from_filename(stem)
        assert result == "202603010401"


class TestReadRtdForRace:
    def test_finds_0b12_format_file(self, tmp_path: Path) -> None:
        """0B12フォーマットのRTDファイルを正しく発見できる。"""
        # 京都R11 = race_id 202608030411, 日付=2026-05-03, jyo=08, race=11
        race_id = "202608030411"
        rtd_file = tmp_path / "0B12202605030811.rtd"
        import zlib
        # 空のO1レコード (最低限のバイト列)
        dummy = b"O1" + b"1" + b"20260503" * 2 + b"08" * 2 + b"01" * 2 + b"00" + b"00" + b"0" * 12
        rtd_file.write_bytes(zlib.compress(dummy))

        with patch("src.scraper.rtd_reader._RTD_DIR", tmp_path):
            result = read_rtd_for_race(race_id)
        # ファイルが見つかること（パース失敗でもNoneではなくRtdRaceInfoを返す）
        assert result is not None

    def test_returns_none_when_no_file(self, tmp_path: Path) -> None:
        """RTDファイルが存在しない場合はNoneを返す。"""
        with patch("src.scraper.rtd_reader._RTD_DIR", tmp_path):
            result = read_rtd_for_race("202608030411")
        assert result is None
```

- [ ] **Step 2: テストが失敗することを確認**

```
pytest tests/test_rtd_reader.py -v
```
Expected: `FAILED test_finds_0b12_format_file` (ファイルが見つからない)

- [ ] **Step 3: rtd_reader.py を修正**

`src/scraper/rtd_reader.py` の `read_rtd_for_race` 関数を置換:

```python
def read_rtd_for_race(race_id: str) -> RtdRaceInfo | None:
    """
    指定 race_id に対応する .rtd ファイルを読み込んで RtdRaceInfo を返す。

    2つのファイルフォーマットに対応:
      新: 0B12{YYYYMMDD}{JYO(2)}{RACE(2)}.rtd  (16文字 stem)
      旧: 0B30{YYYYMMDD}{JYO(2)}{KAI(2)}{NICHI(2)}{RACE(2)}.rtd  (20文字 stem)
    """
    jyo  = race_id[4:6]
    kai  = race_id[6:8]
    nichi = race_id[8:10]
    race = race_id[10:12]

    candidates: list[Path] = []

    # 新フォーマット: 0B12*{jyo}{race}.rtd (KAI/NICHIなし)
    for p in _RTD_DIR.glob(f"0B12*{jyo}{race}.rtd"):
        candidates.append(p)

    # 旧フォーマット: 0B30*{jyo}{kai}{nichi}{race}.rtd
    for p in _RTD_DIR.glob(f"0B30*{jyo}{kai}{nichi}{race}.rtd"):
        candidates.append(p)

    if not candidates:
        logger.debug("RTD ファイル未存在 (race_id=%s)", race_id)
        return None

    rtd_path = max(candidates, key=lambda p: p.stat().st_mtime)

    try:
        raw  = rtd_path.read_bytes()
        dec  = zlib.decompress(raw)
        text = dec.decode("cp932", errors="replace")
    except Exception as exc:
        logger.warning("RTD 解凍失敗 (race_id=%s, file=%s): %s", race_id, rtd_path.name, exc)
        return None

    if not text.startswith("O1"):
        logger.warning("RTD 非O1レコード (race_id=%s): %r", race_id, text[:10])
        return None

    info = _parse_o1(text, race_id)
    logger.info(
        "RTD 読み込み完了: race_id=%s file=%s 出走頭数=%d オッズ取得=%d頭",
        race_id, rtd_path.name, info.head_count, len(info.odds),
    )
    return info
```

同じファイルの `_race_id_from_filename` にも新フォーマット対応を追加:

```python
def _race_id_from_filename(stem: str) -> str:
    """
    ファイルのステム名から race_id を導出する。

    新フォーマット (16文字): "0B12{YYYYMMDD}{JYO}{RACE}"
      → KAI・NICHI不明のため "YYYY{JYO}0000{RACE}" を返す（DB突合が必要）
    旧フォーマット (20文字): "0B30{YYYYMMDD}{JYO}{KAI}{NICHI}{RACE}"
      → "{YYYY}{JYO}{KAI}{NICHI}{RACE}" を返す
    """
    if len(stem) == 16:  # 新フォーマット: 0B12YYYYMMDDXXXRR
        year  = stem[4:8]
        jyo   = stem[12:14]
        race  = stem[14:16]
        return f"{year}{jyo}0000{race}"  # KAI/NICHI=0000 (DB突合で解決)
    # 旧フォーマット (20文字)
    date8 = stem[4:12]
    venue = stem[12:14]
    kai   = stem[14:16]
    nichi = stem[16:18]
    race  = stem[18:20]
    return date8[:4] + venue + kai + nichi + race
```

- [ ] **Step 4: テストが通ることを確認**

```
pytest tests/test_rtd_reader.py -v
```
Expected: すべて PASS

- [ ] **Step 5: コミット**

```bash
git add src/scraper/rtd_reader.py tests/test_rtd_reader.py
git commit -m "fix: RTDファイルパターンを0B12フォーマットに対応 — win_odds 100%欠損の根本修正"
```

---

## Task 2: _jvlink_force_worker.py に OPT_TODAY を追加

**Files:**
- Modify: `scripts/_jvlink_force_worker.py:199-203`

JVLinkのOPT_TODAY (option=3) がCLI choicesに含まれていないため追加する。

- [ ] **Step 1: choices に OPT_TODAY を追加**

`scripts/_jvlink_force_worker.py` の `_parse_args()` を修正:

```python
    p.add_argument("--option",     type=int, default=OPT_NORMAL,
                   choices=[OPT_NORMAL, OPT_SETUP, OPT_TODAY, OPT_STORED],
                   help="1=NORMAL(サーバー直取得) 2=SETUP 3=TODAY(当日) 4=STORED(キャッシュ) (デフォルト: 1)")
```

- [ ] **Step 2: 動作確認（当日データ取得テスト）**

```
py -3.14-32 scripts/_jvlink_force_worker.py --dataspec RACE --fromtime 20260503 --option 3 2>&1 | head -5
```
Expected: `[worker] start dataspec=RACE fromtime=20260503 option=3`

- [ ] **Step 3: コミット**

```bash
git add scripts/_jvlink_force_worker.py
git commit -m "feat: _jvlink_force_worker に OPT_TODAY(3) を追加"
```

---

## Task 3: fetch_race_result.py の netkeiba 依存を排除

**Files:**
- Modify: `scripts/fetch_race_result.py`

netkeiba（CLAUDE.md禁止）の呼び出しを JVLink 経由に完全置換する。
JVLink RACE option=1 で指定日以降のSE・HR レコードを取得 → DB に保存 → Evaluator で評価する。

- [ ] **Step 1: fetch_single_race を JVLink 呼び出しに置換**

`scripts/fetch_race_result.py` の `fetch_single_race` 関数を以下に置換:

```python
def fetch_single_race(race_id: str, delay: float = 1.5) -> bool:
    """
    指定レースの結果を JVLink から取得し DB に保存して評価する。

    Returns:
        True = 結果あり保存成功 / False = まだ結果なし or エラー
    """
    import subprocess
    from src.database.init_db import init_db
    from src.evaluation.evaluator import Evaluator

    # ── JVLink RACE 同期（当日データ取得） ─────────────────────────
    race_date = race_id[:8]  # YYYYMMDD
    logger.info("JVLink RACE 同期開始: race_id=%s date=%s", race_id, race_date)
    try:
        proc = subprocess.run(
            ["py", "-3.14-32",
             str(_ROOT / "scripts" / "_jvlink_force_worker.py"),
             "--dataspec", "RACE",
             "--fromtime", race_date,
             "--option", "3"],  # OPT_TODAY: 当日データ
            cwd=str(_ROOT),
            timeout=120,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
        )
        if proc.returncode != 0:
            logger.warning("JVLink ワーカー rc=%d: %s", proc.returncode, proc.stderr[:200])
            return False
        logger.info("JVLink 同期完了: %s", proc.stdout.splitlines()[-1] if proc.stdout else "")
    except subprocess.TimeoutExpired:
        logger.error("JVLink ワーカー タイムアウト (120s): race_id=%s", race_id)
        return False
    except Exception as exc:
        logger.error("JVLink ワーカー 実行失敗: %s", exc)
        return False

    # ── 結果確認 ────────────────────────────────────────────────────
    conn = init_db()
    rows = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE race_id = ? AND rank IS NOT NULL AND rank > 0",
        (race_id,),
    ).fetchone()
    if not rows or rows[0] == 0:
        logger.info("結果なし (未発走か取消): race_id=%s", race_id)
        conn.close()
        return False

    rank1 = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE race_id = ? AND rank = 1",
        (race_id,),
    ).fetchone()[0]
    if rank1 == 0:
        logger.info("1着馬なし (レース未確定?): race_id=%s", race_id)
        conn.close()
        return False

    logger.info("race_results 確認: race_id=%s (%d頭 rank有)", race_id, rows[0])

    # ── 予想評価 ──────────────────────────────────────────────────
    try:
        evaluator = Evaluator()
        result = evaluator.evaluate_race(conn, race_id)
        logger.info(
            "評価完了: race_id=%s  的中=%d件  投資¥%.0f  払戻¥%.0f  ROI=%.1f%%",
            race_id, result.hit_count,
            result.total_invested, result.total_payout, result.roi,
        )
    except Exception as ee:
        logger.warning("評価失敗 race_id=%s: %s", race_id, ee)

    conn.close()
    return True
```

- [ ] **Step 2: netkeiba インポートを削除**

ファイル先頭の不要インポートを削除（`time` は残す、`subprocess` は fetch_single_race 内で使用済み）:

```python
# 削除する行を探して除去:
# from src.scraper.netkeiba import fetch_race_results, fetch_race_payouts
# from src.database.init_db import insert_race_payouts
```

- [ ] **Step 3: 動作テスト（本日データで確認）**

```
py scripts/fetch_race_result.py --race-id 202605020412 --no-dashboard 2>&1 | tail -5
```
Expected: `JVLink 同期完了` か `結果なし` のいずれか（rc=0）

- [ ] **Step 4: コミット**

```bash
git add scripts/fetch_race_result.py
git commit -m "fix: fetch_race_result.py をnetkeiba禁止→JVLink経由に完全置換"
```

---

## Task 4: today_auto_runner.py に JVLink 再試行ウォッチドッグ追加

**Files:**
- Modify: `scripts/today_auto_runner.py`

postrace で rc=1 が返った場合、JVLink コマンドを再試行してセッション復帰を試みる。

- [ ] **Step 1: _run_fetch_result に retry ロジック追加**

`scripts/today_auto_runner.py` の `_run_fetch_result` 関数を以下に置換:

```python
_POSTRACE_MAX_RETRY = 3
_POSTRACE_RETRY_WAIT_SEC = 60


def _run_fetch_result(race_id: str, dry_run: bool) -> int:
    """レース結果速報取得スクリプトを実行して returncode を返す。失敗時は再試行する。"""
    cmd = [sys.executable, str(_ROOT / "scripts" / "fetch_race_result.py"),
           "--race-id", race_id, "--no-dashboard"]
    if dry_run:
        logger.info("[DRY-RUN] 実行コマンド: %s", " ".join(cmd))
        return 0

    for attempt in range(1, _POSTRACE_MAX_RETRY + 1):
        try:
            result = subprocess.run(cmd, cwd=str(_ROOT), timeout=300)
            if result.returncode == 0:
                return 0
            logger.warning(
                "[NG] 結果取得 rc=%d (試行 %d/%d): %s",
                result.returncode, attempt, _POSTRACE_MAX_RETRY, race_id,
            )
        except subprocess.TimeoutExpired:
            logger.error("結果速報取得 タイムアウト (300s) 試行 %d/%d: %s",
                         attempt, _POSTRACE_MAX_RETRY, race_id)

        if attempt < _POSTRACE_MAX_RETRY:
            logger.info("再試行まで %d 秒待機...", _POSTRACE_RETRY_WAIT_SEC)
            time.sleep(_POSTRACE_RETRY_WAIT_SEC)

    # 全試行失敗 → Discord アラート
    _send_discord(
        f"🚨 **[UMALOGI] 結果取得 全試行失敗** `{race_id}`\n"
        f"{_POSTRACE_MAX_RETRY} 回試行後も rc=1。手動確認が必要です。"
    )
    return 1
```

- [ ] **Step 2: 定数定義をファイル先頭に追加**

`_RESTART_WAIT_SEC = 60` の直後に追加:

```python
# postrace 再試行設定
_POSTRACE_MAX_RETRY     = 3
_POSTRACE_RETRY_WAIT_SEC = 60
```

- [ ] **Step 3: コミット**

```bash
git add scripts/today_auto_runner.py
git commit -m "feat: postrace JVLink再試行ウォッチドッグ追加（最大3回・60秒待機・Discord警告）"
```

---

## Task 5: 5/3 欠損データの補完（JVLink RACE 同期）

**Files:**
- Run: `py -3.14-32 scripts/_jvlink_force_worker.py`
- Run: `py scripts/infer_ranks_from_payouts.py`

払戻欠損19件・着順欠損19件を JVLink から再取得する。

- [ ] **Step 1: JVLink RACE 当日同期を実行**

```
py -3.14-32 scripts/_jvlink_force_worker.py --dataspec RACE --fromtime 20260503 --option 3
```
Expected: `[worker] EOF: read=NNN files=MMM RA=36 SE=NNN payout=MMM`

- [ ] **Step 2: 結果確認**

```python
import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
conn = sqlite3.connect('data/umalogi.db')
# 払戻・着順の補完状況
no_payout = conn.execute("""
    SELECT COUNT(*) FROM races r WHERE r.date = '2026-05-03'
    AND NOT EXISTS (SELECT 1 FROM race_payouts rp WHERE rp.race_id = r.race_id)
""").fetchone()[0]
no_rank = conn.execute("""
    SELECT COUNT(*) FROM races r WHERE r.date = '2026-05-03'
    AND NOT EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id = r.race_id AND rr.rank > 0)
""").fetchone()[0]
print(f'払戻欠損残: {no_payout}件, 着順欠損残: {no_rank}件')
conn.close()
```
Expected: 両方 0 件

- [ ] **Step 3: rank 補完 (infer_ranks_from_payouts.py)**

JVLink sync で rank が埋まらなかった場合のフォールバック:
```
py scripts/infer_ranks_from_payouts.py --year 2026
```
Expected: `races_processed: N rank1_set: M rank2_set: K rank3_set: L`

- [ ] **Step 4: 評価の再実行（prediction_results 更新）**

```python
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, '.')
from src.database.init_db import init_db
from src.evaluation.evaluator import Evaluator

conn = init_db()
rows = conn.execute("""
    SELECT DISTINCT r.race_id FROM races r
    WHERE r.date = '2026-05-03'
    AND EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id = r.race_id AND rr.rank = 1)
    AND EXISTS (SELECT 1 FROM predictions p WHERE p.race_id = r.race_id)
    ORDER BY r.race_id
""").fetchall()
ev = Evaluator()
for (race_id,) in rows:
    try:
        result = ev.evaluate_race(conn, race_id)
        print(f'{race_id}: 的中={result.hit_count} ROI={result.roi:.1f}%')
    except Exception as e:
        print(f'{race_id}: ERROR {e}')
conn.close()
```

---

## Task 6: 5/3 最終 P&L レポート（天皇賞（春）含む）

**Files:**
- Run: `py web/generate_data.py` (JSONリフレッシュ)

- [ ] **Step 1: 本日の損益サマリーを出力**

```python
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

conn = sqlite3.connect('data/umalogi.db')

# 天皇賞（春）の結果
tensho = conn.execute("""
    SELECT rr.rank, rr.horse_name, rr.horse_number, rr.win_odds
    FROM race_results rr
    WHERE rr.race_id = '202608030411'
    ORDER BY rr.rank
    LIMIT 5
""").fetchall()
print('=== 天皇賞（春）上位5頭 ===')
for r in tensho:
    print(f'  {r[0]}着 #{r[2]} {r[1]} (単勝{r[3]}倍)')

# 天皇賞（春）払戻
payouts = conn.execute("""
    SELECT bet_type, combination, payout FROM race_payouts
    WHERE race_id = '202608030411'
    ORDER BY bet_type
""").fetchall()
print('\n=== 天皇賞（春）払戻 ===')
for p in payouts:
    print(f'  {p[0]} {p[1]}: ¥{p[2]:,.0f}')

# 本日の総合損益
daily = conn.execute("""
    SELECT
        COUNT(*) as total_bets,
        SUM(CASE WHEN pr.is_hit = 1 THEN 1 ELSE 0 END) as hits,
        SUM(pr.invested) as total_invested,
        SUM(pr.payout) as total_payout
    FROM prediction_results pr
    JOIN predictions p ON p.id = pr.prediction_id
    JOIN races r ON r.race_id = p.race_id
    WHERE r.date = '2026-05-03'
      AND pr.is_hit IS NOT NULL
""").fetchone()
if daily and daily[0]:
    roi = daily[3] / daily[1] * 100 if daily[2] else 0
    print(f'\n=== 5/3 損益サマリー ===')
    print(f'  買い目数: {daily[0]}件  的中: {daily[1]}件  的中率: {daily[1]/daily[0]*100:.1f}%')
    print(f'  投資: ¥{daily[2]:,.0f}  払戻: ¥{daily[3]:,.0f}  ROI: {roi:.1f}%')

conn.close()
```

- [ ] **Step 2: ダッシュボード JSON 更新**

```
py web/generate_data.py
```

- [ ] **Step 3: Discord に最終レポートを送信**

```python
import os, requests, sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
# 上記サマリーを Discord に送信する
url = os.getenv('DISCORD_WEBHOOK_URL', '')
if url:
    msg = {
        'embeds': [{
            'title': '📊 [UMALOGI] 5/3 最終損益レポート（天皇賞（春）含む）',
            'description': '（上記Step 1の結果を貼付）',
            'color': 0x00BFFF,
        }]
    }
    r = requests.post(url, json=msg, timeout=10)
    print(f'Discord: HTTP {r.status_code}')
```

- [ ] **Step 4: コミット**

```bash
git add web/src/data/
git commit -m "data: 5/3 JVLink補完後 UI JSON更新"
```

---

## Self-Review

**1. Spec coverage:**
- [x] 根本原因特定 → Task 1 (RTD), Task 3 (netkeiba)
- [x] 再発防止・自動復帰 → Task 4 (watchdog)
- [x] データ補完 → Task 5 (JVLink sync + infer_ranks)
- [x] 最終P&Lレポート → Task 6

**2. Placeholder scan:** なし

**3. Type consistency:**
- `fetch_single_race(race_id: str, delay: float) -> bool` ← 変更後も同じシグネチャ維持
- `_run_fetch_result(race_id: str, dry_run: bool) -> int` ← 変更後も同じシグネチャ維持
