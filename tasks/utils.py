"""Tasks utils."""
import platform
from pathlib import Path

from invoke import run as invoke_run
from invoke import UnexpectedExit, Result
from termcolor import cprint


def print_heavy(text):
    """Print `text` to terminal with 'heavy' styling."""
    cprint(text, "cyan", attrs=["bold", "underline", "dark"], on_color=None)


def run(cmd, pty=None, env=None, *args, **kwargs):
    """
    invoke's default runner, with some customisation.

    Customisations:
    - Ensure PYTHONPATH is present and allows package to be imported.
    - Do not use UnexpectedExit's default message format. stderr & stdout will be output and then the task will exit
    """
    if pty is None:
        # if we're under Windows, set pty to False as it's not supported
        if platform.system() == "Windows":
            pty = False
        else:
            pty = True
    if env is None:
        env = {"PYTHONPATH": "./"}

    try:
        return invoke_run(cmd, env=env, pty=pty, *args, **kwargs)
    except UnexpectedExit as err:
        result: Result = err.result
        exit(result.return_code)


class ProjectInfo:
    """Information about this project."""

    source_directory: Path = Path("sparkorm/")  # top of source code tree
    tests_directory: Path = Path("tests/")
    tasks_directory: Path = Path("tasks/")
    project_toml: Path = Path("pyproject.toml")
    reports_directory: Path = Path("reports/")


PROJECT_INFO = ProjectInfo()


def prepare_reports_dir():
    PROJECT_INFO.reports_directory.mkdir(parents=True, exist_ok=True)
