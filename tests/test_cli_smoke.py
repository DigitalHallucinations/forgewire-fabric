from __future__ import annotations

from click.testing import CliRunner

from forgewire_fabric.cli import cli


def test_all_top_level_cli_commands_show_help() -> None:
    runner = CliRunner()
    for command_name in sorted(cli.commands):
        result = runner.invoke(cli, [command_name, "--help"])
        assert result.exit_code == 0, (command_name, result.output, result.exception)
