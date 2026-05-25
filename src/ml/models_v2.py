"""
V2モデル: U score 18因子 + W-004 大衆心理乖離を完全統合した新世代モデル。

A/Bテスト設計:
  V1 (models.py)  : HonmeiModel    / ManjiModel    → model_type "本命(直前)" "卍(直前)"
  V2 (models_v2.py): HonmeiModelV2 / ManjiModelV2 → model_type "本命V2(直前)" "卍V2(直前)"

【V1との違い】
  - pkl ファイル: honmei_model_v2.pkl / manji_model_v2.pkl  (V1とは独立した世代管理)
  - 予想戦略:     BetGeneratorV2 を使用（W-004 EV調整 + 動的EV閾値 + 1/4 Kelly）
  - 初期状態:     V2 pkl = V1 pkl のコピーから開始。週次再学習で独立した世代に成長する。

【週次再学習】
  - V2 再学習: retrain_trigger.weekly_retrain(model_version="v2") で V2 専用 pkl を更新
  - V1 再学習: retrain_trigger.weekly_retrain(model_version="v1") で V1 pkl を更新
  - 両方独立して運用し、月次で ROI を比較して主戦略を決定する。
"""
from __future__ import annotations

import logging
import shutil
from pathlib import Path

from src.ml.models import HonmeiModel, ManjiModel, PlaceModel, _MODEL_DIR

logger = logging.getLogger(__name__)


# ── V2 モデルクラス ──────────────────────────────────────────────────


class HonmeiModelV2(HonmeiModel):
    """V2 本命モデル: V1と同じアーキテクチャ、独立した pkl で A/B テスト可能。"""

    _filename = "honmei_model_v2"


class ManjiModelV2(ManjiModel):
    """V2 卍モデル: V1と同じアーキテクチャ、独立した pkl で A/B テスト可能。"""

    _filename = "manji_model_v2"


class PlaceModelV2(PlaceModel):
    """V2 複勝モデル: V1と同じアーキテクチャ、独立した pkl で A/B テスト可能。"""

    _filename = "place_model_v2"


# ── ヘルパー ────────────────────────────────────────────────────────


def _ensure_v2_pkls() -> None:
    """V2 pkl が存在しない場合、V1 pkl からコピーして初期化する。

    honmei_model_v2.pkl / manji_model_v2.pkl / place_model_v2.pkl の3ファイルを対象とし、
    V2 pkl がない場合のみ V1 pkl をコピーする（V2 pkl が既存の場合は何もしない）。
    """
    pairs = [
        ("honmei_model", "honmei_model_v2"),
        ("manji_model",  "manji_model_v2"),
        ("place_model",  "place_model_v2"),
    ]
    for v1_name, v2_name in pairs:
        v1_path = _MODEL_DIR / f"{v1_name}.pkl"
        v2_path = _MODEL_DIR / f"{v2_name}.pkl"
        if not v2_path.exists() and v1_path.exists():
            shutil.copy2(v1_path, v2_path)
            logger.info("V2 pkl 初期化: %s → %s", v1_path.name, v2_path.name)


def load_models_v2() -> tuple[HonmeiModelV2, PlaceModelV2, ManjiModelV2]:
    """V2モデルをロードする。pkl がなければ V1 からコピーして自動初期化。"""
    _ensure_v2_pkls()

    honmei = HonmeiModelV2()
    honmei.load()

    place = PlaceModelV2()
    place.load()

    manji = ManjiModelV2()
    manji.load()

    return honmei, place, manji
