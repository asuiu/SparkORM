from tasks.utils import run, PROJECT_INFO, print_heavy, prepare_reports_dir


#
# Commands

MINIMUM_COVERAGE_PERCENTAGE = 100


def test():
    print_heavy(f"\n🔍 TESTING 🔎\n")
    prepare_reports_dir()
    coverage_dat = PROJECT_INFO.reports_directory / ".coverage"
    run(
        f"""
        export COVERAGE_FILE={coverage_dat};
        pytest -vv --cov={PROJECT_INFO.source_directory} --cov-report="" -c {PROJECT_INFO.tests_directory}/.pytest.ini"""
    )
    run(
        f"""
        export COVERAGE_FILE={coverage_dat};
        poetry run coverage html -d {PROJECT_INFO.reports_directory / "htmlcov"}"""
    )
    run(
        f"""
        export COVERAGE_FILE={coverage_dat};
        poetry run coverage xml -o {PROJECT_INFO.reports_directory / "coverage.xml"}"""
    )

    print("Coverage report...")
    print(f"Note: Coverage below {MINIMUM_COVERAGE_PERCENTAGE}% will be rejected")
    run(
        f"""
        export COVERAGE_FILE={coverage_dat};
        poetry run coverage report --fail-under={MINIMUM_COVERAGE_PERCENTAGE}"""
    )
