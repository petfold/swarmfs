"""docs/REFERENCE.md is pinned to the code: if a name, a parameter, or the
export list in that file and the package disagree, this suite fails.

Conventions the reference follows (and this test enforces):
- The §3 Exports table lists exactly ``swarmfs.__all__`` minus
  ``__version__``.
- In API tables, the first column is a dotted name resolvable on the
  ``swarmfs`` package (submodules like `localstore.LocalStore` included).
- Where the second column is a backticked signature, every parameter name
  in it exists on the real callable.
- The "version this file describes" line matches pyproject.toml.
"""

import importlib
import inspect
import re
from pathlib import Path

import pytest

pytest.importorskip("eth_hash")  # localstore default addressing needs BMT

import swarmfs  # noqa: E402

DOC = Path(__file__).parent.parent / "docs" / "REFERENCE.md"
TEXT = DOC.read_text(encoding="utf-8")


def _table_rows(section: str) -> list[list[str]]:
    m = re.search(rf"^## {re.escape(section)}.*?(?=^## |\Z)", TEXT,
                  re.M | re.S)
    assert m, f"section {section!r} missing from REFERENCE.md"
    rows = []
    for line in m.group(0).splitlines():
        if line.startswith("|") and not re.match(r"^\|[\s\-|]+\|$", line):
            rows.append([c.strip() for c in line.strip("|").split("|")])
    return rows[1:] if rows else []


def _first_code(cell: str) -> str | None:
    m = re.match(r"`([^`]+)`", cell)
    return m.group(1) if m else None


def test_exports_table_is_exactly_dunder_all():
    documented = {
        _first_code(row[0])
        for row in _table_rows("3. Exports")
        if _first_code(row[0])
    }
    expected = set(swarmfs.__all__) - {"__version__"}
    assert documented == expected, (
        f"only in docs: {documented - expected}; "
        f"only in __all__: {expected - documented}")


API_SECTIONS = [
    "4. `SwarmFileSystem`",
    "6. Stamps (policy tier, `swarmfs.stamps`)",
    "7. Local-first store (`swarmfs.localstore`)",
    "8. Sync worker (`swarmfs.localsync`)",
]


def _resolve(dotted: str):
    parts = dotted.split(".")
    if parts[0] in ("localstore", "localsync", "stamps"):
        obj = importlib.import_module(f"swarmfs.{parts[0]}")
        parts = parts[1:]
    else:
        obj = swarmfs
    for part in parts:
        obj = getattr(obj, part)
    return obj


def _api_rows():
    # Only dotted members or class names are API rows — bare lowercase
    # words are storage options, covered by their own test below.
    for section in API_SECTIONS:
        for row in _table_rows(section):
            name = _first_code(row[0])
            if name and re.fullmatch(r"[A-Za-z_][\w.]*", name) and \
                    ("." in name or name[0].isupper()):
                yield section, name, row


def test_every_documented_name_resolves():
    checked = 0
    for section, name, _ in _api_rows():
        try:
            _resolve(name)
        except AttributeError as e:
            raise AssertionError(
                f"{section}: `{name}` does not resolve: {e}") from None
        checked += 1
    assert checked > 35


def test_documented_parameters_exist():
    checked = 0
    for section, name, row in _api_rows():
        sig_cell = _first_code(row[1]) if len(row) > 1 else None
        if not sig_cell or not sig_cell.startswith("("):
            continue
        obj = _resolve(name)
        target = obj.__init__ if inspect.isclass(obj) else obj
        try:
            params = inspect.signature(target).parameters
        except (ValueError, TypeError):
            continue
        real = set(params) | {"self"}
        has_kwargs = any(p.kind is inspect.Parameter.VAR_KEYWORD
                         for p in params.values())
        for chunk in sig_cell.strip("()").split(","):
            param = re.split(r"[=:]", chunk.strip())[0].strip("* ")
            if not re.fullmatch(r"[A-Za-z_]\w*", param):
                continue
            assert has_kwargs or param in real, (
                f"{section}: `{name}` documents parameter {param!r} "
                f"which the code does not have (real: {sorted(real)})")
            checked += 1
    assert checked > 30


def test_storage_options_exist_on_constructor():
    real = set(inspect.signature(swarmfs.SwarmFileSystem.__init__).parameters)
    for row in _table_rows("4. `SwarmFileSystem`"):
        opt = _first_code(row[0])
        if opt and re.fullmatch(r"[a-z_]+", opt):
            assert opt in real, (
                f"storage option `{opt}` documented but not a "
                f"SwarmFileSystem parameter")


def test_client_method_list_matches_sync_facade():
    m = re.search(r"^## 5\. Client tier.*?(?=^## )", TEXT, re.M | re.S)
    documented = set(re.findall(r"`(\w+)`", m.group(0))) - {
        "SwarmClient", "SyncSwarmClient"}
    for name in documented:
        assert hasattr(swarmfs.SyncSwarmClient, name), (
            f"client method `{name}` documented but missing on "
            "SyncSwarmClient")


def test_store_status_fields_documented():
    from swarmfs.localstore import StoreStatus
    for field in StoreStatus.__dataclass_fields__:
        assert f"`{field}`" in TEXT, (
            f"StoreStatus field {field!r} missing from REFERENCE.md")


def test_sync_policy_defaults_match():
    from swarmfs.localsync import SyncPolicy
    row = next(r for _, n, r in _api_rows() if n == "localsync.SyncPolicy")
    documented = dict(re.findall(r"`(\w+)=([^`]+)`", row[2]))
    policy = SyncPolicy()
    for field, value in documented.items():
        real = getattr(policy, field)
        norm = "None" if real is None else str(real)
        assert norm == value.replace(" (→ budget/4)", ""), (
            f"SyncPolicy.{field}: doc says {value}, code default is {real}")


def test_described_version_matches_pyproject():
    doc_version = re.search(
        r"version this file describes: `([\d.]+)`", TEXT).group(1)
    pyproject = (Path(__file__).parent.parent / "pyproject.toml").read_text()
    real = re.search(r'^version = "([\d.]+)"', pyproject, re.M).group(1)
    assert doc_version == real, (
        f"REFERENCE.md describes {doc_version}, pyproject says {real} — "
        "update the reference as part of the release docs sweep")
