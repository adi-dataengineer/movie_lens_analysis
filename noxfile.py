import nox

nox.options.stop_on_first_error = True
python_versions = ["3.8"]

src = "src"

@nox.session(python=python_versions,
             reuse_venv=True,
             tags = ['lint-files', 'lint-alert'])
def lint_alert(session: nox.Session, folder_name: str = src) -> None:
    """Lint files"""
    session.run("mypy", folder_name, external=True)