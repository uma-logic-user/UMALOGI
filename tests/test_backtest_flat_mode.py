"""backtest_2024_2025.py の --mode flat フラグテスト。"""

from __future__ import annotations
import subprocess
import sys


class TestFlatBetMode:
    def test_flat_mode_flag_accepted(self) -> None:
        """--mode flat が受け付けられ、エラーなく --help が出る。"""
        result = subprocess.run(
            [sys.executable, "scripts/backtest_2024_2025.py", "--help"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            cwd="C:/dev/horse-racing-ai",
        )
        assert "--mode" in result.stdout, (
            f"--mode フラグが見当たらない: {result.stdout}"
        )
        assert "flat" in result.stdout, (
            f"flat オプションが見当たらない: {result.stdout}"
        )

    def test_flat_roi_calculation(self) -> None:
        """フラットベット ROI 計算のユニットテスト。"""
        import importlib.util
        import os

        spec = importlib.util.spec_from_file_location(
            "backtest_2024_2025",
            os.path.join("C:/dev/horse-racing-ai", "scripts", "backtest_2024_2025.py"),
        )
        assert spec is not None
        mod = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(mod)  # type: ignore[attr-defined]

        calc_flat_roi = mod.calc_flat_roi  # type: ignore[attr-defined]

        bets = [
            {"is_hit": 1, "payout": 350, "invest": 100},
            {"is_hit": 0, "payout": 0, "invest": 100},
            {"is_hit": 1, "payout": 500, "invest": 100},
            {"is_hit": 0, "payout": 0, "invest": 100},
        ]
        roi = calc_flat_roi(bets)
        # 総投資 400, 総回収 850 → ROI = 850/400 * 100 = 212.5%
        assert abs(roi - 212.5) < 0.1, f"ROI={roi:.1f}% (expected 212.5%)"
