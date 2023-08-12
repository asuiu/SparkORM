from invoke import Result

from tasks.utils import run, PROJECT_INFO, print_heavy


#
# Commands


def lint():
    """Apply all linting checks."""
    _run_project_needs_reformatting()
    _run_pylint(PROJECT_INFO.source_directory)
    _run_pycodestyle(PROJECT_INFO.source_directory)
    _run_pydocstyle(PROJECT_INFO.source_directory)


#
# Utils


def _run_pylint(source_dir: str, pylintrc_fpath: str = ".pylintrc"):
    """Run pylint with a given configuration on a given code tree."""
    print_heavy(f"\n🔍 PYLINT '{source_dir}' 🔎\n")
    run(f'find {source_dir} -type f -name "*.py" | xargs pylint --rcfile {pylintrc_fpath}')


def _run_pycodestyle(source_dir: str):
    """Run pycodestyle with a given configuration on a given code tree."""
    print_heavy(f"\n🔍 PYCODESTYLE '{source_dir}' 🔎\n")
    run(f"pycodestyle --ignore=E501,W503 --exclude=.svn,CVS,.bzr,.hg,.git,__pycache__,.tox {source_dir}")
    # Ignores explained:
    # - E501: Line length is checked by PyLint
    # - W503: Black's convention is to put line break before binary operator. This is also the (new) Python convention


def _run_pydocstyle(source_dir: str):
    """
    Run docstring linting on source code.

    Docstring linting is done via pydocstyle. The pydocstyle config can be found in the `.pydocstyle` file.
    This ensures compliance with PEP 257, with a few exceptions. Note that pylint also carries out additional
    docstyle checks.
    """
    print_heavy(f"\n🔍 PYDOCSTYLE '{source_dir}' 🔎\n")
    run(f"pydocstyle {PROJECT_INFO.source_directory}")


def _run_project_needs_reformatting():
    """Check if the code needs re-formatting via black."""
    print_heavy(f"\n🔍 CHECK CODE FORMATTING 🔎\n")

    result: Result = run(
        f"black {PROJECT_INFO.source_directory} {PROJECT_INFO.tasks_directory} "
        f"{PROJECT_INFO.tests_directory}  --check --diff",
        warn=True,
    )
    if result.return_code:
        print(
            """
Please re-format your code with:
    poetry run reformat
"""
        )
        exit(result.return_code)
