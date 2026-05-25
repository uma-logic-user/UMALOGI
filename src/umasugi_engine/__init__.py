"""
umasugi_engine — AIウマスギ次世代予測エンジン

legacy_logic (src/ml/) をラッパー型で拡張し、以下を追加する:
  - 30因子スコアリング（世論分析・小回り・野芝/洋芝 を先行実装）
  - 大衆心理による EV 減算フィルター（負の相関）
  - legacy_logic との並列比較インターフェース
"""

from .engine import UmasugiEngine
from .comparator import compare_predictions

__all__ = ["UmasugiEngine", "compare_predictions"]
