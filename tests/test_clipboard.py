import pyperclip
import pytest

from nless.app import NlessApp
from nless.types import CliArgs


async def _wait(pilot, app):
    settled = 0
    for _ in range(300):
        await pilot.pause(delay=0.01)
        if all(not b.loading_state.reason for b in app.buffers):
            settled += 1
            if settled >= 5:
                return
        else:
            settled = 0


@pytest.fixture
def cli_args():
    return CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)


def _raise_pyperclip(text):
    raise pyperclip.PyperclipException("no clipboard")


class TestClipboardErrorMessages:
    @pytest.mark.asyncio
    async def test_wayland_error_mentions_wl_clipboard(self, cli_args, monkeypatch):
        monkeypatch.setenv("WAYLAND_DISPLAY", "wayland-0")
        monkeypatch.setattr(pyperclip, "copy", _raise_pyperclip)

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            app.buffers[0].add_logs(["name,city", "Alice,NYC"])
            await _wait(pilot, app)
            await pilot.press("y")
            await pilot.pause(0.1)

        messages = [n.message for n in app._notifications]
        assert any("wl-clipboard" in m for m in messages)

    @pytest.mark.asyncio
    async def test_x11_error_mentions_xclip_xsel(self, cli_args, monkeypatch):
        monkeypatch.delenv("WAYLAND_DISPLAY", raising=False)
        monkeypatch.setattr(pyperclip, "copy", _raise_pyperclip)

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            app.buffers[0].add_logs(["name,city", "Alice,NYC"])
            await _wait(pilot, app)
            await pilot.press("y")
            await pilot.pause(0.1)

        messages = [n.message for n in app._notifications]
        assert any("xclip" in m or "xsel" in m for m in messages)
