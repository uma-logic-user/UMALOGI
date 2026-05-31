"""
スクレイプ完了監視 → 全自動バックテスト → レポート保存パイプライン
=====================================================================

実行方法:
  py -3 scripts/wait_and_test.py

処理フロー:
  1. data/netkeiba_research.db の進捗を 60 秒おきにポーリング
  2. スクレイプ完了を検知（進捗停止 10 分 or 99% 到達）
  3. ALPHA 2024→2025 バックテスト実行
  4. WIN5 2025年バックテスト実行（--start 2025-01-01 --end 2025-12-31）
  5. outputs/reports/final_backtest_2024_2025.md に結果を保存

完了条件:
  - scraped_count >= target_count * 0.99（99%達成）
  - 10 分間新規スクレイプなし（プロセス自然完了とみなす）
  - 8 時間超過（タイムアウトフォールバック）
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
_RESEARCH_DB = _ROOT / "data" / "netkeiba_research.db"
_MAIN_DB = _ROOT / "data" / "umalogi.db"
_REPORT_DIR = _ROOT / "outputs" / "reports"
_REPORT_PATH = _REPORT_DIR / "final_backtest_2024_2025.md"

POLL_INTERVAL = 60  # 進捗確認間隔（秒）
STALL_SECONDS = 600  # 進捗停止→完了とみなすまでの秒数（10分）
MAX_WAIT_SECONDS = 28800  # 最長待機 8時間


# ── 進捗確認ヘルパー ───────────────────────────────────────────────────


def _get_scraped_count() -> int:
    conn = sqlite3.connect(str(_RESEARCH_DB))
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM scraped_races WHERE status='ok'"
        ).fetchone()[0]
    finally:
        conn.close()


def _get_target_count() -> int:
    conn = sqlite3.connect(str(_MAIN_DB))
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM races WHERE date >= '2024-02-01' AND date <= '2025-12-31'"
        ).fetchone()[0]
    finally:
        conn.close()


# ── フェーズ 1: スクレイピング完了待機 ────────────────────────────────


def wait_for_scraping() -> int:
    """スクレイピング完了を監視し、完了時の取得件数を返す。"""
    target = _get_target_count()
    logger.info(f"対象レース数: {target}件（umalogi.db 2024-02〜2025-12）")

    last_count: int = -1
    last_change_time: float = time.time()
    start_time: float = time.time()

    while True:
        elapsed = time.time() - start_time
        if elapsed > MAX_WAIT_SECONDS:
            logger.warning(
                "最長待機時間（8時間）超過。現状データでバックテストを開始します。"
            )
            break

        current = _get_scraped_count()
        pct = current / target * 100 if target > 0 else 0
        remaining = target - current
        eta_min = (remaining * 1.6) / 60  # 1.6秒/レースで推定

        logger.info(
            f"スクレイプ進捗: {current}/{target} ({pct:.1f}%) 残り推定: {eta_min:.0f}分"
        )

        if current != last_count:
            last_count = current
            last_change_time = time.time()

        stall_elapsed = time.time() - last_change_time
        is_complete = pct >= 99.0
        is_stalled = stall_elapsed >= STALL_SECONDS

        if is_complete:
            logger.info(f"スクレイプ完了！ {current}/{target} ({pct:.1f}%)")
            break
        if is_stalled:
            logger.info(
                f"進捗が {STALL_SECONDS}秒 停止 → スクレイプ完了とみなします "
                f"({current}/{target}, {pct:.1f}%)"
            )
            break

        time.sleep(POLL_INTERVAL)

    return _get_scraped_count()


# ── フェーズ 2: バックテスト実行 ─────────────────────────────────────


def _run_command(cmd: list[str], label: str) -> str:
    """コマンドを実行し、stdout+stderr を返す。プログレスを逐次ログ出力。"""
    logger.info(f"{'=' * 60}")
    logger.info(f" {label} 開始")
    logger.info(f"{'=' * 60}")
    logger.info(f"コマンド: {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(_ROOT),
    )
    lines: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip()
        lines.append(line)
        print(line, flush=True)
    proc.wait()

    if proc.returncode != 0:
        logger.error(f"{label} 異常終了 (exit={proc.returncode})")
    else:
        logger.info(f"{label} 正常完了")

    return "\n".join(lines)


def run_alpha_backtest() -> str:
    cmd = [
        sys.executable,
        str(_ROOT / "scripts" / "alpha_backtest.py"),
        "--train",
        "2024",
        "--test",
        "2025",
        "--research-db",
        str(_RESEARCH_DB),
    ]
    return _run_command(cmd, "ALPHA モデル バックテスト（2024 学習 → 2025 テスト）")


def run_win5_backtest() -> str:
    cmd = [
        sys.executable,
        str(_ROOT / "scripts" / "win5_strategy.py"),
        "--backtest",
        "--start",
        "2025-01-01",
        "--end",
        "2025-12-31",
        "--research-db",
        str(_RESEARCH_DB),
        "--max-combos",
        "5000",
        "--verbose",
    ]
    return _run_command(cmd, "WIN5 累積勝率カバー法 バックテスト（2025年）")


# ── フェーズ 3: レポート保存 ─────────────────────────────────────────


def save_report(alpha_output: str, win5_output: str, scraped_count: int) -> None:
    """バックテスト結果を Markdown レポートとして保存する。"""
    _REPORT_DIR.mkdir(parents=True, exist_ok=True)

    target = _get_target_count()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pct = scraped_count / target * 100 if target > 0 else 0

    # === サマリー行を抽出 ===
    def _extract_lines(output: str, keywords: list[str]) -> str:
        return "\n".join(
            line for line in output.splitlines() if any(kw in line for kw in keywords)
        )

    alpha_summary = _extract_lines(
        alpha_output,
        [
            "ROI",
            "損益",
            "的中",
            "投資",
            "払戻",
            "NG",
            "OK",
            "!!",
            "通算",
            "AUC",
            "LogLoss",
        ],
    )
    win5_summary = _extract_lines(
        win5_output,
        ["週", "的中", "ROI", "払戻", "カバー", "スキップ", "対象", "投資", "収支"],
    )

    content = f"""# UMALOGI 最終バックテストレポート 2024-2025

- **生成日時**: {now}
- **Research DB 取得率**: {scraped_count:,}/{target:,} レース ({pct:.1f}%)
- **テスト条件**: ALPHA 2024学習→2025テスト / WIN5 2025年全週

---

## 1. ALPHA モデル バックテスト（2024 学習 → 2025 テスト）

### サマリー
```
{alpha_summary}
```

### 全出力
```
{alpha_output}
```

---

## 2. WIN5 累積勝率カバー法 バックテスト（2025年）

### サマリー
```
{win5_summary}
```

### 全出力
```
{win5_output}
```

---

## 3. 目標達成評価

| 指標 | 目標 | 結果 |
|---|---|---|
| ALPHA ROI（単勝） | 110% 以上 | ← 上記参照 |
| ALPHA ROI（複勝） | 110% 以上 | ← 上記参照 |
| WIN5 的中カバー率 | 80% 以上 | ← 上記参照 |

---

*生成: UMALOGI 自律バックテストパイプライン (scripts/wait_and_test.py)*
"""

    _REPORT_PATH.write_text(content, encoding="utf-8")

    print(f"\n{'=' * 65}")
    print(f"  レポート保存完了: {_REPORT_PATH}")
    print(f"{'=' * 65}\n")
    logger.info(f"レポート保存: {_REPORT_PATH}")


# ── メインエントリ ────────────────────────────────────────────────────


def main() -> None:
    logger.info("=" * 60)
    logger.info(" UMALOGI 全自動バックテストパイプライン 起動")
    logger.info("=" * 60)
    logger.info(f"Research DB : {_RESEARCH_DB}")
    logger.info(f"レポート出力: {_REPORT_PATH}")
    logger.info(f"完了条件    : 99%到達 or {STALL_SECONDS}秒停止")

    # フェーズ 1: スクレイピング完了待機
    scraped_count = wait_for_scraping()

    # フェーズ 2: バックテスト実行
    alpha_output = run_alpha_backtest()
    win5_output = run_win5_backtest()

    # フェーズ 3: レポート保存
    save_report(alpha_output, win5_output, scraped_count)

    logger.info("=" * 60)
    logger.info(" 全自動パイプライン 完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
