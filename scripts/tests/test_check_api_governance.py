from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "check_api_governance.py"


def _load():
    spec = importlib.util.spec_from_file_location("check_api_governance", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_api_governance"] = module
    spec.loader.exec_module(module)
    return module


def test_load_inventory_reads_remaining_contract_categories(tmp_path: Path) -> None:
    inventory = tmp_path / "inventory.toml"
    inventory.write_text(
        """\
[[package]]
name = "smg-client"
path = "clients/rust"
classification = "public-sdk"
semver = true
release = "release-crates"
owner = "CODEOWNERS"

[[package]]
name = "smg"
path = "model_gateway"
classification = "external-application"
semver = false
release = "release-crates"
owner = "CODEOWNERS"

[[package]]
name = "smg-python"
path = "bindings/python"
classification = "version-locked-binding"
semver = false
release = "core-version-sync"
owner = "CODEOWNERS"
"""
    )

    module = _load()

    assert module.load_inventory(inventory) == [
        module.InventoryEntry(
            "smg-client",
            "clients/rust",
            "public-sdk",
            True,
            "release-crates",
            "CODEOWNERS",
        ),
        module.InventoryEntry(
            "smg",
            "model_gateway",
            "external-application",
            False,
            "release-crates",
            "CODEOWNERS",
        ),
        module.InventoryEntry(
            "smg-python",
            "bindings/python",
            "version-locked-binding",
            False,
            "core-version-sync",
            "CODEOWNERS",
        ),
    ]


def test_generated_inventory_path_is_under_governance() -> None:
    module = _load()

    assert module.INVENTORY_DOC_PATH == module.REPO_ROOT / "governance/api-surface-inventory.md"


def test_packages_from_metadata_normalizes_crates_and_filters_out_of_scope(
    tmp_path: Path,
) -> None:
    module = _load()
    metadata = {
        "packages": [
            {
                "name": "published-library",
                "manifest_path": str(tmp_path / "crates/library/Cargo.toml"),
                "publish": None,
                "targets": [{"kind": ["lib"]}],
            },
            {
                "name": "crates-binary",
                "manifest_path": str(tmp_path / "crates/binary/Cargo.toml"),
                "publish": [],
                "targets": [{"kind": ["bin"]}],
            },
            {
                "name": "nested-library",
                "manifest_path": str(tmp_path / "crates/foo/bar/Cargo.toml"),
                "publish": None,
                "targets": [{"kind": ["lib"]}],
            },
            {
                "name": "private-tool",
                "manifest_path": str(tmp_path / "tools/private_tool/Cargo.toml"),
                "publish": [],
                "targets": [{"kind": ["bin"]}],
            },
        ]
    }

    packages = module.packages_from_metadata(metadata, tmp_path)

    assert packages == [
        module.PackageRecord("crates-binary", "crates/binary", False, False),
        module.PackageRecord("nested-library", "crates/foo/bar", True, True),
        module.PackageRecord("published-library", "crates/library", True, True),
    ]
    assert all(package.name != "private-tool" for package in packages)


def test_check_reports_unsupported_schema_before_parsing_package_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    inventory = tmp_path / "inventory.toml"
    inventory.write_text(
        """\
schema-version = 2

[[package]]
renamed-package-field = "future-schema"
"""
    )
    module = _load()
    monkeypatch.setattr(module, "INVENTORY_PATH", inventory)

    assert module.main(["--check"]) == 1
    assert capsys.readouterr().err == (
        "unsupported API surface inventory schema-version; expected 1\n"
    )


def test_release_crates_reads_workflow_matrix_entries() -> None:
    module = _load()
    workflow_text = """\
jobs:
  tier1:
    strategy:
      matrix:
        include:
          - crate: openai-protocol
            path: crates/protocols
          - crate: smg-mcp
            path: crates/mcp
"""

    assert module.release_crates(workflow_text) == {
        "openai-protocol": "crates/protocols",
        "smg-mcp": "crates/mcp",
    }


def test_release_crates_rejects_duplicate_names() -> None:
    module = _load()
    workflow_text = """\
jobs:
  one:
    crate: known
    path: crates/known
  two:
    crate: known
    path: crates/not_known
"""

    with pytest.raises(ValueError, match="duplicate release-workflow crate: known"):
        module.release_crates(workflow_text)


def test_unclassified_crate_is_an_error(tmp_path: Path) -> None:
    module = _load()
    packages = [
        module.PackageRecord("known", "crates/known", True, True),
        module.PackageRecord("new-crate", "crates/new_crate", True, True),
    ]
    entries = [
        module.InventoryEntry(
            "known",
            "crates/known",
            "published-library",
            True,
            "release-crates",
            "CODEOWNERS",
        )
    ]

    assert module.validate_inventory(packages, entries) == [
        "unclassified crates/ package: new-crate (crates/new_crate)"
    ]


@pytest.mark.parametrize(
    ("publishable", "classification", "semver", "expected"),
    [
        (
            True,
            "quality-only",
            False,
            "publishable crate known must be classified published-library",
        ),
        (
            False,
            "published-library",
            True,
            "private crate known cannot be classified published-library",
        ),
    ],
)
def test_publish_state_must_match_classification(
    publishable: bool, classification: str, semver: bool, expected: str
) -> None:
    module = _load()
    packages = [module.PackageRecord("known", "crates/known", publishable, True)]
    entries = [
        module.InventoryEntry(
            "known",
            "crates/known",
            classification,
            semver,
            "release-crates" if publishable else "none",
            "CODEOWNERS",
        )
    ]

    assert expected in module.validate_inventory(packages, entries)


@pytest.mark.parametrize(
    ("semver", "release", "expected"),
    [
        (
            False,
            "release-crates",
            "published-library package known must set semver = true",
        ),
        (
            True,
            "none",
            'published-library package known must set release = "release-crates"',
        ),
    ],
)
def test_published_library_requires_semver_and_release_governance(
    semver: bool, release: str, expected: str
) -> None:
    module = _load()
    packages = [module.PackageRecord("known", "crates/known", True, True)]
    entries = [
        module.InventoryEntry(
            "known",
            "crates/known",
            "published-library",
            semver,
            release,
            "CODEOWNERS",
        )
    ]

    assert expected in module.validate_inventory(packages, entries)


def test_release_governed_package_missing_from_release_workflow_fails() -> None:
    module = _load()
    entries = [
        module.InventoryEntry(
            "engine-zmq-client",
            "crates/engine_zmq_client",
            "published-library",
            True,
            "release-crates",
            "CODEOWNERS",
        )
    ]

    assert module.validate_release_coverage(entries, {"openai-protocol": "crates/protocols"}) == [
        "release-governed package missing from release-crates workflow: engine-zmq-client"
    ]


def test_release_workflow_path_must_match_inventory() -> None:
    module = _load()
    entries = [
        module.InventoryEntry(
            "engine-zmq-client",
            "crates/engine_zmq_client",
            "published-library",
            True,
            "release-crates",
            "CODEOWNERS",
        )
    ]

    assert module.validate_release_coverage(
        entries, {"engine-zmq-client": "crates/wrong_package"}
    ) == [
        "release workflow path mismatch for engine-zmq-client: "
        "expected crates/engine_zmq_client, found crates/wrong_package"
    ]


def test_version_registry_crates_reads_only_the_crates_array() -> None:
    module = _load()
    registry_text = """\
CRATES=(
    "known|crates/known|known"
    "openapi-gen|clients/openapi-gen|-"
)

PYTHON_PACKAGES=(
    "ignored|python/ignored|python/ignored/pyproject.toml"
)
"""

    assert module.version_registry_crates(registry_text) == {
        "known": "crates/known",
        "openapi-gen": "clients/openapi-gen",
    }


@pytest.mark.parametrize(
    ("registry_text", "expected"),
    [
        (
            'CRATES=(\n    "other|crates/other|other"\n)\n',
            "release-governed package missing from version registry: known",
        ),
        (
            'CRATES=(\n    "known|crates/not_known|known"\n)\n',
            "version registry path mismatch for known: "
            "expected crates/known, found crates/not_known",
        ),
    ],
)
def test_release_governed_package_requires_version_registry_coverage(
    registry_text: str, expected: str
) -> None:
    module = _load()
    entries = [
        module.InventoryEntry(
            "known",
            "crates/known",
            "published-library",
            True,
            "release-crates",
            "CODEOWNERS",
        )
    ]

    registry = module.version_registry_crates(registry_text)
    assert module.validate_version_registry_coverage(entries, registry) == [expected]


def test_render_inventory_is_sorted_and_marks_semver() -> None:
    module = _load()
    entries = [
        module.InventoryEntry("zeta", "crates/zeta", "quality-only", False, "none", "CODEOWNERS"),
        module.InventoryEntry(
            "alpha", "crates/alpha", "published-library", True, "release-crates", "CODEOWNERS"
        ),
    ]

    rendered = module.render_inventory(entries)
    assert rendered.index("`alpha`") < rendered.index("`zeta`")
    assert "| `alpha` | `crates/alpha` | published-library | yes |" in rendered


def test_write_doc_rejects_missing_release_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load()
    inventory = tmp_path / "api-surfaces.toml"
    inventory.write_text(
        """\
schema-version = 1

[[package]]
name = "known"
path = "crates/known"
classification = "published-library"
semver = true
release = "release-crates"
owner = "CODEOWNERS"
"""
    )
    workflow = tmp_path / "release-crates.yml"
    workflow.write_text("jobs: {}\n")
    registry = tmp_path / "check_release_versions.sh"
    registry.write_text('CRATES=(\n    "known|crates/known|known"\n)\n')
    generated = tmp_path / "api-surface-inventory.md"

    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(module, "INVENTORY_PATH", inventory)
    monkeypatch.setattr(module, "RELEASE_WORKFLOW_PATH", workflow)
    monkeypatch.setattr(module, "VERSION_REGISTRY_PATH", registry)
    monkeypatch.setattr(module, "INVENTORY_DOC_PATH", generated)
    monkeypatch.setattr(
        module,
        "_cargo_metadata",
        lambda: {
            "packages": [
                {
                    "name": "known",
                    "manifest_path": str(tmp_path / "crates/known/Cargo.toml"),
                    "publish": None,
                    "targets": [{"kind": ["lib"]}],
                }
            ]
        },
    )

    assert module._run("write-doc") == [
        "release-governed package missing from release-crates workflow: known"
    ]
    assert not generated.exists()
