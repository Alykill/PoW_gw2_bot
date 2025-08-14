
import discord, datetime
class DisabledSignupView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="✅ Sign Up", style=discord.ButtonStyle.success, disabled=True))
        self.add_item(discord.ui.Button(label="❌ Sign Out", style=discord.ButtonStyle.danger, disabled=True))

class CreateEventModal(discord.ui.Modal, title="Create Event"):
    def __init__(self, on_submit_cb):
        super().__init__(timeout=None)
        self._cb = on_submit_cb
        self.name_input = discord.ui.TextInput(
            label="Event name", placeholder="Raid Full Clear",
            required=True, max_length=100
        )
        self.start_input = discord.ui.TextInput(
            label="Start (YYYY-MM-DD HH:MM)", placeholder="2025-08-12 18:00",
            required=True
        )
        self.duration_input = discord.ui.TextInput(
            label="Duration (e.g., 2h30m, 45m)", placeholder="2h30m",
            required=True
        )
        self.add_item(self.name_input)
        self.add_item(self.start_input)
        self.add_item(self.duration_input)

    async def on_submit(self, interaction: discord.Interaction):
        # No channel here anymore — we’ll ask via dropdown after this modal
        await self._cb(
            interaction,
            self.name_input.value.strip(),
            self.start_input.value.strip(),
            self.duration_input.value.strip(),
        )

class EventCreatorView(discord.ui.View):
    def __init__(self, on_open_modal):
        super().__init__(timeout=None)
        self._open = on_open_modal
    @discord.ui.button(label="📅 Create Event", style=discord.ButtonStyle.primary, custom_id="open_create_event_modal")
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._open(interaction)

class SignupView(discord.ui.View):
    def __init__(self, event_name, event_start: datetime.datetime, channel_id: int, message_id: int, on_signup, on_signout):
        super().__init__(timeout=None)
        self.event_name = event_name; self.event_start = event_start
        self.channel_id = channel_id; self.message_id = message_id
        self._signup = on_signup; self._signout = on_signout
    @discord.ui.button(label="✅ Sign Up", style=discord.ButtonStyle.success, custom_id="signup_button")
    async def signup(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._signup(interaction, self)
    @discord.ui.button(label="❌ Sign Out", style=discord.ButtonStyle.danger, custom_id="signout_button")
    async def signout(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._signout(interaction, self)

class ChannelPicker(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(
            channel_types=[discord.ChannelType.text],
            placeholder="Select a channel to post the event…",
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        # self.values[0] is an AppCommandChannel proxy; store its ID.
        selected = self.values[0]
        self.view.selected_channel_id = int(selected.id)
        await interaction.response.defer()  # acknowledge the selection ephemerally

class EventFinalizeView(discord.ui.View):
    def __init__(self, name: str, start: str, duration: str, on_confirm):
        super().__init__(timeout=300)
        self.name = name
        self.start = start
        self.duration = duration
        self.on_confirm = on_confirm
        self.selected_channel_id: int | None = None
        self.add_item(ChannelPicker())

    @discord.ui.button(label="Create in selected channel", style=discord.ButtonStyle.primary)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_channel_id:
            await interaction.response.send_message(
                "Please select a channel first.", ephemeral=True, delete_after=5
            )
            return

        # ✅ Acknowledge the button click so we can do work and reply later
        await interaction.response.defer(ephemeral=True)

        # Now call back into the cog to finalize
        await self.on_confirm(interaction, self)

class EditEventModal(discord.ui.Modal, title="Edit Event"):
    def __init__(self, event_name: str, start_default: str, duration_default: str, on_submit_cb):
        super().__init__(timeout=None)
        self.event_name = event_name
        self._cb = on_submit_cb

        self.start_input = discord.ui.TextInput(
            label="New Start (YYYY-MM-DD HH:MM)",
            default=start_default,
            required=True,
        )
        self.duration_input = discord.ui.TextInput(
            label="New Duration (e.g., 2h30m, 45m)",
            default=duration_default,
            required=True,
        )
        self.add_item(self.start_input)
        self.add_item(self.duration_input)

    async def on_submit(self, interaction: discord.Interaction):
        await self._cb(
            interaction,
            self.event_name,
            self.start_input.value.strip(),
            self.duration_input.value.strip(),
        )

class EventMessageView(discord.ui.View):
    """Event message view = Sign Up / Sign Out + admin controls."""
    def __init__(self, event_name: str, event_start: datetime.datetime, channel_id: int, message_id: int,
                 on_signup, on_signout, on_edit, on_cancel, on_end_now):
        super().__init__(timeout=None)
        self.event_name = event_name
        self.event_start = event_start
        self.channel_id = channel_id
        self.message_id = message_id
        self._signup = on_signup
        self._signout = on_signout
        self._edit = on_edit
        self._cancel = on_cancel
        self._end_now = on_end_now

    # Row 1 — roster buttons
    @discord.ui.button(label="✅ Sign Up", style=discord.ButtonStyle.success, custom_id="evt_signup")
    async def signup(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._signup(interaction, self)

    @discord.ui.button(label="❌ Sign Out", style=discord.ButtonStyle.danger, custom_id="evt_signout")
    async def signout(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._signout(interaction, self)

    # Row 2 — admin buttons
    @discord.ui.button(label="✏️ Edit", style=discord.ButtonStyle.primary, custom_id="evt_edit", row=1)
    async def edit(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._edit(interaction, self)

    @discord.ui.button(label="🛑 Cancel", style=discord.ButtonStyle.secondary, custom_id="evt_cancel", row=1)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._cancel(interaction, self)

    @discord.ui.button(label="⏹️ End Now", style=discord.ButtonStyle.secondary, custom_id="evt_end_now", row=1)
    async def end_now(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._end_now(interaction, self)