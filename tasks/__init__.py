from tasks.formatting import reformat
from tasks.testing import test
from tasks.linting import lint
from tasks.releasing import prepare_release, find_releasable_changes, debug_auto_git_tag
from tasks.typechecking import typecheck


def verify_all():
    """Run all checks."""
    lint()
    typecheck()
    test()
