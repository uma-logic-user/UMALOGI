"""
UMALOGI Discord ボット — /paddock コマンド対応

Discord スラッシュコマンド /paddock でパドック気配コメントを受け付け、
paddock_notes テーブルに保存する。

必要環境変数:
    DISCORD_BOT_TOKEN  : Discord ボットトークン
    DISCORD_GUILD_ID   : コマンド登録対象のサーバー ID（省略=グローバル登録）

起動方法:
    python src/notification/discord_bot.py

依存: pip install "discord.py>=2.3" python-dotenv

コマンド一覧:
    /paddock race_id:[レースID] comment:[気配コメント]
        例: /paddock race_id:202608031001 comment:3番 気配良し スムーズ
        → 全馬コメントとして保存（horse_number=NULL）

    /paddock race_id:[レースID] horse:[馬番] comment:[気配コメント]
        例: /paddock race_id:202608031001 horse:7 comment:発汗気味 イライラ
        → 指定馬番コメントとして保存
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

try:
    import discord
    from discord import app_commands

    _DISCORD_AVAILABLE = True
except ImportError:
    _DISCORD_AVAILABLE = False
    logger.warning("discord.py が未インストールです: pip install 'discord.py>=2.3'")


def run_bot() -> None:
    """Discord ボットを起動する。

    DISCORD_BOT_TOKEN 環境変数からトークンを読み込み、discord.py クライアントを
    初期化して /paddock スラッシュコマンドを登録・起動する。
    discord.py 未インストール時または TOKEN 未設定時は sys.exit(1) で終了する。

    Raises:
        SystemExit: discord.py 未インストール時または TOKEN 未設定時。
    """
    if not _DISCORD_AVAILABLE:
        logger.error(
            "discord.py をインストールしてください: pip install 'discord.py>=2.3'"
        )
        sys.exit(1)

    token = os.environ.get("DISCORD_BOT_TOKEN", "")
    if not token:
        logger.error("DISCORD_BOT_TOKEN 環境変数が設定されていません")
        sys.exit(1)

    guild_id_str = os.environ.get("DISCORD_GUILD_ID", "")

    intents = discord.Intents.default()
    client = discord.Client(intents=intents)
    tree = app_commands.CommandTree(client)

    guild: discord.Object | None = (
        discord.Object(id=int(guild_id_str)) if guild_id_str else None
    )

    @client.event
    async def on_ready() -> None:
        if guild:
            tree.copy_global_to(guild=guild)
            await tree.sync(guild=guild)
            logger.info("コマンド登録完了 (guild=%s) bot=%s", guild_id_str, client.user)
        else:
            await tree.sync()
            logger.info("コマンド登録完了 (global) bot=%s", client.user)

    @tree.command(
        name="paddock",
        description="パドック気配・馬場状態をレースに紐付けて保存します",
    )
    @app_commands.describe(
        race_id="レース ID（例: 202608031001）",
        comment="気配コメント（例: 3番 気配良し スムーズ）",
        horse="馬番（省略時=レース全体へのメモ）",
    )
    async def paddock_command(
        interaction: discord.Interaction,
        race_id: str,
        comment: str,
        horse: int | None = None,
    ) -> None:
        """パドックコメントを DB に保存するスラッシュコマンド。"""

        from src.database.init_db import init_db
        from src.umasugi_engine.factors.paddock import save_paddock_note

        # race_id バリデーション（12桁数字）
        if not race_id.isdigit() or len(race_id) != 12:
            await interaction.response.send_message(
                f"❌ race_id の形式が不正です: `{race_id}`\n正しい形式: 12桁の数字 (例: 202608031001)",
                ephemeral=True,
            )
            return

        try:
            conn = init_db()
            boost = save_paddock_note(conn, race_id, horse, comment, source="discord")
            conn.close()
        except Exception as exc:
            logger.exception("paddock_note 保存エラー: %s", exc)
            await interaction.response.send_message(
                f"❌ 保存エラー: {exc}", ephemeral=True
            )
            return

        boost_label = (
            f"🟢 +{boost * 100:.0f}% ポジティブ"
            if boost > 0
            else f"🔴 {boost * 100:.0f}% ネガティブ"
            if boost < 0
            else "⚪ 中立"
        )
        horse_label = f"#{horse}番" if horse else "全馬共通"
        await interaction.response.send_message(
            f"✅ パドックメモを保存しました\n"
            f"レース: `{race_id}` / 対象: {horse_label}\n"
            f"コメント: {comment}\n"
            f"判定: {boost_label}",
            ephemeral=False,
        )

    client.run(token)


if __name__ == "__main__":
    run_bot()
