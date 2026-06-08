#!/usr/bin/env bash
# 仮置き連続実行パイプライン（W-077 消化）。
# ① 2025 SE コード backfill（NORMAL）→ ② 2025+ クリーンデータでモデル再学習。
# 各ステップは冪等。backfill は JVLink 保持期間に依存し 2025 前半は届かない場合がある。
set -u
cd "$(dirname "$0")/.."

echo "===== [1/2] SE コード backfill (2025/NORMAL) ====="
py -3-32 -X utf8 scripts/backfill_se_codes_w076.py 20250101 1

echo "===== [2/2] モデル再学習 (--train-from 2025) ====="
py -X utf8 scripts/retrain_win_place.py --train-from 2025

echo "===== パイプライン完了 ====="
