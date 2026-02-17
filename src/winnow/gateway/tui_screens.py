"""Modal screens used by the gateway TUI."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Static

class PromptScreen(ModalScreen[str | None]):
    """Simple one-line input modal."""

    CSS = """
    PromptScreen {
        align: center middle;
    }
    #prompt_box {
        width: 70;
        padding: 1 2;
        border: solid $accent;
        background: $surface;
    }
    #prompt_actions {
        height: auto;
        margin-top: 1;
    }
    """

    def __init__(self, *, title: str, placeholder: str = "") -> None:
        super().__init__()
        self.title = title
        self.placeholder = placeholder

    def compose(self) -> ComposeResult:
        with Vertical(id="prompt_box"):
            yield Static(self.title)
            yield Input(placeholder=self.placeholder, id="prompt_input")
            with Horizontal(id="prompt_actions"):
                yield Button("Save", variant="primary", id="prompt_ok")
                yield Button("Cancel", id="prompt_cancel")

    def on_mount(self) -> None:
        self.query_one("#prompt_input", Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "prompt_ok":
            value = self.query_one("#prompt_input", Input).value.strip()
            self.dismiss(value or None)
            return
        self.dismiss(None)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value.strip() or None)


class ConfirmScreen(ModalScreen[bool]):
    """Simple yes/no confirm modal."""

    CSS = """
    ConfirmScreen {
        align: center middle;
    }
    #confirm_box {
        width: 72;
        padding: 1 2;
        border: solid $warning;
        background: $surface;
    }
    #confirm_actions {
        height: auto;
        margin-top: 1;
    }
    """

    def __init__(self, *, message: str, ok_label: str = "Confirm") -> None:
        super().__init__()
        self.message = message
        self.ok_label = ok_label

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm_box"):
            yield Static(self.message)
            with Horizontal(id="confirm_actions"):
                yield Button(self.ok_label, variant="warning", id="confirm_ok")
                yield Button("Cancel", id="confirm_cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "confirm_ok")


class GatewayActionScreen(ModalScreen[str | None]):
    """Daemon action picker modal."""

    CSS = """
    GatewayActionScreen {
        align: center middle;
    }
    #action_box {
        width: 62;
        padding: 1 2;
        border: solid $accent;
        background: $surface;
    }
    #action_buttons {
        height: auto;
        margin-top: 1;
    }
    """

    def __init__(self, *, running: bool) -> None:
        super().__init__()
        self.running = running

    def compose(self) -> ComposeResult:
        label = "Gateway is running." if self.running else "Gateway is stopped."
        with Vertical(id="action_box"):
            yield Static(f"{label} Choose an action:")
            with Horizontal(id="action_buttons"):
                yield Button("Start", variant="success", id="action_start")
                yield Button("Stop", variant="error", id="action_stop")
                yield Button("Restart", variant="warning", id="action_restart")
                yield Button("Cancel", id="action_cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        choice = event.button.id or ""
        if choice == "action_cancel":
            self.dismiss(None)
            return
        self.dismiss(choice.removeprefix("action_"))

