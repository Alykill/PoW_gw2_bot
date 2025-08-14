# bot.py
import logging
import discord
from discord.ext import commands
from config import settings, setup_logging
from infra.scheduler import scheduler
from cogs.events_cog import EventsCog

class RaidBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.members = True  # enable in Dev Portal if you use member data
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self) -> None:
        await self.add_cog(EventsCog(self))

        # Start scheduler
        try:
            scheduler.start()
        except Exception:
            pass

        # --- Slash command sync ---
        try:
            if settings.GUILD_ID:
                guild = discord.Object(id=int(settings.GUILD_ID))
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)  # instant in guild
                logging.getLogger("raidbot").info(f"Synced {len(synced)} app commands to guild {settings.GUILD_ID}")
            else:
                synced = await self.tree.sync()  # global (can take a while)
                logging.getLogger("raidbot").info(f"Synced {len(synced)} global app commands")
        except Exception as e:
            logging.getLogger("raidbot").exception(f"App command sync failed: {e}")

    async def on_ready(self):
        logging.getLogger("raidbot").info(f"Logged in as {self.user} (ID: {self.user.id})")

def main():
    setup_logging()
    bot = RaidBot()
    bot.run(settings.DISCORD_TOKEN)

if __name__ == "__main__":
    main()
