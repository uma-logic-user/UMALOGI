"""src/ui/console.py — CUI Rich化ラッパー（ハッカーのコックピット）

オートパイロット・スクレイピング・プレミアム生成などの標準出力を
rich ライブラリで劇的に装飾する共通レイヤー。

設計原則（本番常駐プロセスを絶対に殺さない）:
  - rich 未インストール / import 失敗時は plain テキストへ完全フォールバック。
  - 非 TTY（リダイレクト・サービス実行）ではプログレスバー描画を自動無効化
    （rich Console の端末検知に委譲）。
  - Windows cp932 コンソールでも UnicodeEncodeError を出さない
    （plain 経路は errors="replace" で安全に書き出す）。

Usage:
    from src.ui.console import get_console

    con = get_console()
    con.banner("UMA-LOGIC", "週次オートパイロット v1.12")
    con.ev_signal("東京11R", "三連複", "5-9-12", ev=1.42, odds=48.3)
    for race in con.track(races, description="レース走査"):
        ...
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import IO, Any

logger = logging.getLogger(__name__)

try:  # rich はオプショナル依存（無くても全機能 plain で動く）
    from rich import box
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        Task,
        TaskProgressColumn,
        TextColumn,
        TimeRemainingColumn,
    )
    from rich.rule import Rule
    from rich.style import Style
    from rich.table import Table
    from rich.text import Text

    RICH_AVAILABLE = True
except Exception:  # pragma: no cover - rich 未導入環境
    RICH_AVAILABLE = False

# ── カラーパレット（コックピットテーマ）─────────────────────────────────
_NEON_CYAN = "#00E5FF"
_NEON_MAGENTA = "#FF3CAC"
_GOLD = "#FFD700"
_EMERALD = "#2BD9A8"
_AMBER = "#FFB300"
_CRIMSON = "#FF4D4D"
_STEEL = "#7A8B99"

# プログレスバーのグラデーション（左→右）
_GRADIENT_STOPS: tuple[str, ...] = (_NEON_CYAN, _NEON_MAGENTA, _GOLD)


def _as_float(v: object) -> float:
    """object 値を安全に float へ変換する（失敗時 0.0）。"""
    try:
        return float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    """'#RRGGBB' を (r, g, b) に変換する。"""
    h = hex_color.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _lerp_color(ratio: float, stops: Sequence[str] = _GRADIENT_STOPS) -> str:
    """0.0〜1.0 の比率をグラデーション上の '#RRGGBB' に線形補間する。"""
    r = min(max(ratio, 0.0), 1.0) * (len(stops) - 1)
    i = min(int(r), len(stops) - 2)
    t = r - i
    c1, c2 = _hex_to_rgb(stops[i]), _hex_to_rgb(stops[i + 1])
    rgb = tuple(round(a + (b - a) * t) for a, b in zip(c1, c2, strict=True))
    return "#{:02X}{:02X}{:02X}".format(*rgb)


if RICH_AVAILABLE:

    class GradientBarColumn(BarColumn):
        """シアン→マゼンタ→ゴールドのグラデーションで塗るプログレスバー。"""

        def render(self, task: "Task") -> "Text":  # type: ignore[override]
            width = self.bar_width or 40
            ratio = (task.completed / task.total) if task.total else 0.0
            filled = int(width * min(max(ratio, 0.0), 1.0))
            bar = Text()
            for i in range(width):
                if i < filled:
                    color = _lerp_color(i / max(width - 1, 1))
                    bar.append("━", style=Style(color=color, bold=True))
                else:
                    bar.append("━", style=Style(color="grey23"))
            return bar


class UmaConsole:
    """UMA-Logic 共通のコックピット風コンソール。

    rich が使えるときは Panel / Table / グラデーションバーで装飾し、
    使えないとき（未導入・force_plain）は同内容を plain テキストで出力する。
    """

    def __init__(
        self,
        *,
        file: IO[str] | None = None,
        force_plain: bool = False,
        width: int | None = None,
    ) -> None:
        """初期化。

        Args:
            file: 出力先ストリーム（省略時 stdout。テストでは StringIO を注入）。
            force_plain: True で rich を使わず plain 出力に固定する。
            width: コンソール幅の固定値（テスト・ログ出力用）。
        """
        self._file = file
        self._plain = force_plain or not RICH_AVAILABLE
        self._console: Any = None
        if not self._plain:
            if file is None:
                # Windows cp932 コンソールで絵文字等が UnicodeEncodeError に
                # ならないよう、エンコード不能文字は置換して出力する。
                import sys

                try:
                    enc = (getattr(sys.stdout, "encoding", "") or "").lower()
                    if enc and enc not in ("utf-8", "utf8"):
                        sys.stdout.reconfigure(errors="replace")  # type: ignore[union-attr]
                except Exception:  # noqa: BLE001
                    pass
            self._console = Console(
                file=file, width=width, highlight=False, emoji=True, soft_wrap=False
            )

    def _print_rich(self, renderable: Any, plain_fallback: str) -> None:
        """rich 出力を試み、失敗時は plain テキストへ縮退する（本番安全）。"""
        try:
            self._console.print(renderable)
        except Exception:  # noqa: BLE001 - 装飾失敗で本番を殺さない
            self._write_plain(plain_fallback)

    @property
    def is_rich(self) -> bool:
        """rich 装飾が有効かどうか。"""
        return not self._plain

    # ── 内部ヘルパー ────────────────────────────────────────────────────
    def _write_plain(self, text: str) -> None:
        """plain 出力（cp932 コンソールでも例外を出さない）。"""
        import sys

        stream = self._file or sys.stdout
        try:
            stream.write(text + "\n")
        except UnicodeEncodeError:  # pragma: no cover - cp932 端末のみ
            enc = getattr(stream, "encoding", "utf-8") or "utf-8"
            stream.write(
                (text + "\n")
                .encode(enc, errors="replace")
                .decode(enc, errors="replace")
            )

    # ── 基本出力 ────────────────────────────────────────────────────────
    def banner(self, title: str, subtitle: str = "") -> None:
        """起動バナーを二重枠 Panel で表示する。"""
        if self._plain:
            self._write_plain("=" * 64)
            self._write_plain(f"  {title}" + (f" — {subtitle}" if subtitle else ""))
            self._write_plain("=" * 64)
            return
        body = Text()
        for i, ch in enumerate(title):
            body.append(
                ch,
                style=Style(color=_lerp_color(i / max(len(title) - 1, 1)), bold=True),
            )
        if subtitle:
            body.append("\n")
            body.append(subtitle, style=Style(color=_STEEL, italic=True))
        self._print_rich(
            Panel(body, box=box.DOUBLE_EDGE, border_style=_NEON_CYAN, padding=(1, 4)),
            f"== {title}" + (f" — {subtitle}" if subtitle else "") + " ==",
        )

    def section(self, title: str) -> None:
        """セクション区切り線を表示する。"""
        if self._plain:
            self._write_plain(f"── {title} " + "─" * 40)
            return
        self._print_rich(
            Rule(f"[bold {_NEON_CYAN}]{title}[/]", style=_STEEL), f"── {title} ──"
        )

    def success(self, msg: str) -> None:
        """成功メッセージ（緑）。"""
        if self._plain:
            self._write_plain(f"[OK] {msg}")
            return
        self._print_rich(f"[bold {_EMERALD}]✔[/] {msg}", f"[OK] {msg}")

    def warning(self, msg: str) -> None:
        """警告メッセージ（琥珀の Panel）。"""
        if self._plain:
            self._write_plain(f"[WARN] {msg}")
            return
        self._print_rich(
            Panel(
                msg,
                title=f"[bold {_AMBER}]⚠ WARNING[/]",
                border_style=_AMBER,
                box=box.HEAVY,
            ),
            f"[WARN] {msg}",
        )

    def error(self, msg: str) -> None:
        """エラーメッセージ（深紅の Panel）。"""
        if self._plain:
            self._write_plain(f"[ERROR] {msg}")
            return
        self._print_rich(
            Panel(
                msg,
                title=f"[bold {_CRIMSON}]✖ ERROR[/]",
                border_style=_CRIMSON,
                box=box.HEAVY,
            ),
            f"[ERROR] {msg}",
        )

    def info(self, msg: str) -> None:
        """情報メッセージ。"""
        if self._plain:
            self._write_plain(f"[INFO] {msg}")
            return
        self._print_rich(f"[{_STEEL}]·[/] {msg}", f"[INFO] {msg}")

    # ── 高EVシグナル ────────────────────────────────────────────────────
    def ev_signal(
        self,
        race_label: str,
        bet_type: str,
        combo: str,
        ev: float,
        odds: float | None = None,
        stake: int | None = None,
    ) -> None:
        """「EV 超えの買い目発見！」シグナルを金色の Panel で出力する。

        Args:
            race_label: レース表示名（例: '東京11R'）。
            bet_type: 券種（例: '三連複'）。
            combo: 買い目表記（例: '5-9-12'）。
            ev: 期待値。
            odds: 想定オッズ（任意）。
            stake: 推奨ベット額（円・任意）。
        """
        if self._plain:
            extra = ""
            if odds is not None:
                extra += f"  想定オッズ {odds:,.1f}"
            if stake is not None:
                extra += f"  推奨 ¥{stake:,}"
            self._write_plain(
                f"[SIGNAL] EV {ev:.2f} 超えの買い目発見！ {race_label} {bet_type} {combo}{extra}"
            )
            return
        body = Text()
        body.append(f"{race_label}  ", style=Style(color=_NEON_CYAN, bold=True))
        body.append(f"{bet_type}  ", style=Style(color=_NEON_MAGENTA, bold=True))
        body.append(combo, style=Style(color="white", bold=True))
        body.append("\n")
        body.append(f"EV {ev:.2f}", style=Style(color=_GOLD, bold=True))
        if odds is not None:
            body.append(f"   想定オッズ {odds:,.1f}", style=Style(color=_STEEL))
        if stake is not None:
            body.append(f"   推奨 ¥{stake:,}", style=Style(color=_EMERALD, bold=True))
        self._print_rich(
            Panel(
                body,
                title=f"[bold {_GOLD}]🔥 EV {ev:.2f} 超えの買い目発見！[/]",
                border_style=_GOLD,
                box=box.DOUBLE,
                padding=(0, 2),
            ),
            f"[SIGNAL] EV {ev:.2f} 超えの買い目発見！ {race_label} {bet_type} {combo}",
        )

    def candidates_table(
        self, title: str, rows: Sequence[Mapping[str, object]]
    ) -> None:
        """高EV候補の一覧テーブルを出力する。

        Args:
            title: テーブルタイトル。
            rows: 各行 dict（label / bet_type / prob / odds / ev キー）。
        """
        if self._plain:
            self._write_plain(f"== {title} ==")
            for r in rows:
                self._write_plain(
                    f"  {r.get('bet_type', '')} {r.get('label', '')}  "
                    f"p={_as_float(r.get('prob', 0)):.3f}  "
                    f"odds={_as_float(r.get('odds', 0)):,.1f}  "
                    f"EV={_as_float(r.get('ev', 0)):.2f}"
                )
            return
        table = Table(
            title=f"[bold {_NEON_CYAN}]{title}[/]",
            box=box.SIMPLE_HEAVY,
            border_style=_STEEL,
            header_style=f"bold {_NEON_MAGENTA}",
        )
        table.add_column("券種", style="bold white")
        table.add_column("買い目", style=_NEON_CYAN)
        table.add_column("確率", justify="right", style=_STEEL)
        table.add_column("オッズ", justify="right", style=_STEEL)
        table.add_column("EV", justify="right")
        for r in rows:
            ev = _as_float(r.get("ev", 0))
            ev_style = _GOLD if ev >= 1.3 else _EMERALD if ev >= 1.0 else _STEEL
            table.add_row(
                str(r.get("bet_type", "")),
                str(r.get("label", "")),
                f"{_as_float(r.get('prob', 0)) * 100:.2f}%",
                f"{_as_float(r.get('odds', 0)):,.1f}",
                Text(f"{ev:.2f}", style=Style(color=ev_style, bold=ev >= 1.0)),
            )
        self._print_rich(table, f"== {title} == ({len(rows)} 件)")

    # ── プログレス ──────────────────────────────────────────────────────
    def _make_progress(self, transient: bool = True) -> "Progress":
        """グラデーションバー付きの rich Progress を生成する。"""
        return Progress(
            SpinnerColumn(style=_NEON_CYAN),
            TextColumn("[bold white]{task.description}"),
            GradientBarColumn(bar_width=36),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=self._console,
            transient=transient,
        )

    def track(
        self,
        iterable: Iterable[Any],
        description: str = "処理中",
        total: int | None = None,
    ) -> Iterator[Any]:
        """イテレーションをグラデーションプログレスバーで可視化する。

        rich 無効時はそのまま素通しする（挙動互換）。
        """
        if self._plain:
            yield from iterable
            return
        if total is None:
            try:
                total = len(iterable)  # type: ignore[arg-type]
            except TypeError:
                total = None
        progress = self._make_progress()
        with progress:
            task_id = progress.add_task(description, total=total)
            for item in iterable:
                yield item
                progress.advance(task_id, 1)

    @contextmanager
    def progress(self, description: str, total: int) -> Iterator[Callable[[int], None]]:
        """手動 advance 型のプログレスバー context manager。

        Yields:
            advance(n): n ステップ進めるコールバック。
        """
        if self._plain:

            def _noop(n: int = 1) -> None:
                return None

            yield _noop
            return
        prog = self._make_progress()
        with prog:
            task_id = prog.add_task(description, total=total)

            def _advance(n: int = 1) -> None:
                prog.advance(task_id, n)

            yield _advance


# ── ロギング統合 ─────────────────────────────────────────────────────────
def create_rich_log_handler(level: int = logging.INFO) -> logging.Handler | None:
    """rich の RichHandler を生成する（未導入環境では None）。

    時刻・レベルがカラー装飾され、Traceback も rich 形式で整形される。
    """
    if not RICH_AVAILABLE:
        return None
    handler = RichHandler(
        level=level,
        rich_tracebacks=True,
        markup=False,
        show_path=False,
        omit_repeated_times=False,
        log_time_format="%Y-%m-%d %H:%M:%S",
    )
    return handler


_singleton: UmaConsole | None = None


def get_console() -> UmaConsole:
    """プロセス共通の UmaConsole シングルトンを返す。"""
    global _singleton
    if _singleton is None:
        _singleton = UmaConsole()
    return _singleton
