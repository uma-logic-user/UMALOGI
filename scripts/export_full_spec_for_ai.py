"""
UMA-Logic 完全版仕様・コード集約ファイル生成スクリプト

外部 LLM にシステム全容（アーキテクチャ・EV算出ロジック・仕様）を分析させるための
単一エクスポートファイルを生成する。

出力: export/umalogic_full_spec_for_ai_analysis.txt
形式: 各ファイルを <file path="..."> ... </file> の XML タグで区切って結合。

対象:
  1. ルートの主要ドキュメント (CLAUDE.md / logic_map.md / VERSION)
  2. docs/ 配下のすべての .md
  3. src/ml/ 配下のすべての .py (特徴量・モデル推論・EV/閾値判定)
  4. src/database/ 配下のすべての .py (スキーマ・DB定義)
  5. scripts/ 配下の OOS 検証・EVバックテスト・較正検証スクリプト
  6. accuracy-model worktree の未マージ実装 (accuracy_model_v2 / hybrid ensemble)
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = REPO_ROOT / "export" / "umalogic_full_spec_for_ai_analysis.txt"
WORKTREE = REPO_ROOT / ".claude" / "worktrees" / "accuracy-model"

# scripts/ のうち OOS 検証・ハイブリッドアンサンブル・EV歪み検証に関わるもの
EVAL_SCRIPTS: list[str] = [
    "scripts/backtest_v2_oos.py",
    "scripts/run_backtest_v2.py",
    "scripts/evaluate_accuracy_model.py",
    "scripts/fit_verify_manji_calibration.py",
    "scripts/walk_forward_backtest.py",
    "scripts/train_integrated_v2.py",
    "scripts/ev_backtest_vectorized.py",
    "scripts/backtest_pure_ev_edge.py",
    "scripts/calibration_kelly_audit.py",
]

# accuracy-model worktree にのみ存在する未マージ実装（出所を path で明示）
WORKTREE_FILES: list[str] = [
    "src/ml/accuracy_model_v2.py",
    "scripts/evaluate_hybrid_ensemble.py",
    "scripts/backtest_all_models.py",
]

EXCLUDE_DIR_PARTS = {"__pycache__", "node_modules", ".git"}


def _read_text(path: Path) -> str:
    """UTF-8 優先でファイルを読む。失敗時は CP932 → replace でフォールバック。"""
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            return path.read_text(encoding="cp932")
        except UnicodeDecodeError:
            return path.read_text(encoding="utf-8", errors="replace")


def _collect(base: Path, pattern: str) -> list[Path]:
    """base 配下から pattern に一致するファイルを除外ディレクトリ抜きで収集する。"""
    return sorted(
        p
        for p in base.glob(pattern)
        if p.is_file() and not (EXCLUDE_DIR_PARTS & set(p.parts))
    )


def build_sections() -> list[tuple[str, list[tuple[str, Path]]]]:
    """(セクション名, [(タグ用パス, 実体パス), ...]) のリストを構築する。"""
    sections: list[tuple[str, list[tuple[str, Path]]]] = []

    root_files = [
        ("CLAUDE.md", REPO_ROOT / "CLAUDE.md"),
        ("logic_map.md", REPO_ROOT / "logic_map.md"),
        ("VERSION", REPO_ROOT / "VERSION"),
    ]
    sections.append(
        ("1_PROJECT_ROOT_DOCS", [(t, p) for t, p in root_files if p.exists()])
    )

    docs = _collect(REPO_ROOT / "docs", "**/*.md")
    sections.append(
        ("2_SPECIFICATION_DOCS", [(p.relative_to(REPO_ROOT).as_posix(), p) for p in docs])
    )

    ml = _collect(REPO_ROOT / "src" / "ml", "*.py")
    sections.append(
        ("3_CORE_ML_LOGIC", [(p.relative_to(REPO_ROOT).as_posix(), p) for p in ml])
    )

    db = _collect(REPO_ROOT / "src" / "database", "*.py")
    sections.append(
        ("4_DATABASE_SCHEMA", [(p.relative_to(REPO_ROOT).as_posix(), p) for p in db])
    )

    evals = [(rel, REPO_ROOT / rel) for rel in EVAL_SCRIPTS]
    sections.append(
        ("5_EVALUATION_EV_SCRIPTS", [(t, p) for t, p in evals if p.exists()])
    )

    wt = [
        (f"[worktree:accuracy-model(未マージ)] {rel}", WORKTREE / rel)
        for rel in WORKTREE_FILES
    ]
    sections.append(
        ("6_UNMERGED_WORKTREE_ACCURACY_MODEL", [(t, p) for t, p in wt if p.exists()])
    )

    return sections


def generate(out_path: Path = OUT_PATH) -> tuple[int, int]:
    """エクスポートファイルを生成し (ファイル数, 出力バイト数) を返す。"""
    sections = build_sections()
    version = (REPO_ROOT / "VERSION").read_text(encoding="utf-8").strip() \
        if (REPO_ROOT / "VERSION").exists() else "unknown"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    total_files = 0

    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("<export_metadata>\n")
        f.write("project: UMA-Logic (UMALOGI) — 自律型・競馬予測プラットフォーム\n")
        f.write(f"version: {version}\n")
        f.write(f"generated_at: {datetime.now().isoformat(timespec='seconds')}\n")
        f.write(
            "purpose: 外部LLMによるアーキテクチャ・EV算出ロジック・仕様の徹底分析用\n"
        )
        f.write(
            "format: 各ファイルは <file path=\"...\"> 内容 </file> で区切られている。\n"
        )
        f.write(
            "note: path が [worktree:accuracy-model(未マージ)] で始まるファイルは\n"
            "      master 未マージの開発中実装である。\n"
        )
        f.write("</export_metadata>\n\n")

        f.write("<table_of_contents>\n")
        for name, files in sections:
            f.write(f"## {name} ({len(files)} files)\n")
            for tag_path, _ in files:
                f.write(f"  - {tag_path}\n")
        f.write("</table_of_contents>\n\n")

        for name, files in sections:
            f.write(f"\n<!-- ==================== SECTION: {name} ==================== -->\n\n")
            for tag_path, real_path in files:
                content = _read_text(real_path).rstrip("\n")
                f.write(f'<file path="{tag_path}">\n')
                f.write(content)
                f.write("\n</file>\n\n")
                total_files += 1

    return total_files, out_path.stat().st_size


if __name__ == "__main__":
    n, size = generate()
    print(f"完了: {n} ファイルを結合 → {OUT_PATH}")
    print(f"サイズ: {size / 1024 / 1024:.2f} MB")
