"""
Fixture builder for Enterprise Analytics Python UDF library tests.

shiv is unusable on the executor nodes (1.0.8 on Python 3.9.2 always fails
"Pip install failed!", even with --site-packages/--no-deps). A plain zip
with the module placed under `site-packages/` is accepted and executes, so
fixtures are generated in-test with `zipfile` -- no build step, no shiv.
"""

import os
import tempfile
import zipfile


def build_pyz_fixture(module_name, module_source, extra_files=None, out_dir=None):
    """
    Builds a `.pyz` library archive containing one module under
    `site-packages/`.

    :param module_name: str, e.g. "mylib" -- importable as `mylib` inside
    the sandbox.
    :param module_source: str, the module's Python source.
    :param extra_files: dict/None of {relative_path: content} for
    additional files inside the archive (e.g. a bundled data file read via
    os.path.dirname(__file__)), written alongside the module under
    `site-packages/`.
    :param out_dir: str/None, directory to write the archive into;
    defaults to a fresh `tempfile.mkdtemp()`.
    :return: local filesystem path to the generated `.pyz` file.
    """
    out_dir = out_dir or tempfile.mkdtemp()
    archive_path = os.path.join(out_dir, f"{module_name}.pyz")
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(f"site-packages/{module_name}.py", module_source)
        for relative_path, content in (extra_files or {}).items():
            archive.writestr(f"site-packages/{relative_path}", content)
    return archive_path


def build_malformed_fixture(kind, out_dir=None):
    """
    Builds a deliberately broken archive for the negative upload cases.

    :param kind: one of:
      - "truncated" -- a valid zip whose bytes are cut short.
      - "no_site_packages" -- a well-formed zip with a module at the
        archive root instead of under `site-packages/`.
      - "empty" -- a zero-byte file.
    :param out_dir: str/None, directory to write the archive into;
    defaults to a fresh `tempfile.mkdtemp()`.
    :return: local filesystem path to the generated file.
    """
    out_dir = out_dir or tempfile.mkdtemp()
    archive_path = os.path.join(out_dir, f"malformed_{kind}.pyz")

    if kind == "empty":
        open(archive_path, "wb").close()
        return archive_path

    if kind == "no_site_packages":
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("mylib.py", 'class Echo(object):\n    def hello(self, *args):\n        return "hello"\n')
        return archive_path

    if kind == "truncated":
        well_formed = build_pyz_fixture(
            "mylib", 'class Echo(object):\n    def hello(self, *args):\n        return "hello"\n', out_dir=out_dir
        )
        with open(well_formed, "rb") as f:
            data = f.read()
        with open(archive_path, "wb") as f:
            f.write(data[: max(1, len(data) // 2)])
        return archive_path

    raise ValueError(f"Unknown malformed fixture kind: {kind}")
