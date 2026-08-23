#!/usr/bin/env python3
"""Validate the governed API surface inventory and release coverage."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
INVENTORY_PATH = REPO_ROOT / "governance/api-surfaces.toml"
RELEASE_WORKFLOW_PATH = REPO_ROOT / ".github/workflows/release-crates.yml"
INVENTORY_DOC_PATH = REPO_ROOT / "docs/api-surface-inventory.md"

RELEASE_CRATE = re.compile(
    r"^\s*(?:-\s*)?crate:\s*([A-Za-z0-9_-]+)\s*$", re.MULTILINE
)


@dataclass(frozen=True)
class PackageRecord:
    name: str
    path: str
    publishable: bool
    has_lib_target: bool


@dataclass(frozen=True)
class InventoryEntry:
    name: str
    path: str
    classification: str
    semver: bool
    release: str
    owner: str


def load_inventory(path: Path) -> list[InventoryEntry]:
    """Load inventory entries from the authoritative TOML schema."""
    data = tomllib.loads(path.read_text())
    return [
        InventoryEntry(
            name=package["name"],
            path=package["path"],
            classification=package["classification"],
            semver=package["semver"],
            release=package["release"],
            owner=package["owner"],
        )
        for package in data.get("package", [])
    ]


def packages_from_metadata(
    metadata: dict[str, Any], repo_root: Path
) -> list[PackageRecord]:
    """Normalize direct ``crates/<directory>`` Cargo packages."""
    root = repo_root.resolve()
    packages: list[PackageRecord] = []

    for package in metadata["packages"]:
        manifest_parent = Path(package["manifest_path"]).resolve().parent
        try:
            relative_path = manifest_parent.relative_to(root)
        except ValueError:
            continue

        if len(relative_path.parts) != 2 or relative_path.parts[0] != "crates":
            continue

        packages.append(
            PackageRecord(
                name=package["name"],
                path=relative_path.as_posix(),
                publishable=package.get("publish") != [],
                has_lib_target=any(
                    "lib" in target.get("kind", [])
                    for target in package.get("targets", [])
                ),
            )
        )

    return sorted(packages, key=lambda package: package.name)


def release_crates(workflow_text: str) -> set[str]:
    """Return crate names from release-workflow matrix and input entries."""
    return set(RELEASE_CRATE.findall(workflow_text))


def validate_inventory(
    packages: Sequence[PackageRecord], entries: Sequence[InventoryEntry]
) -> list[str]:
    """Return deterministic metadata-to-inventory validation errors."""
    errors: list[str] = []
    entries_by_name: dict[str, InventoryEntry] = {}
    entries_by_path: dict[str, InventoryEntry] = {}

    for entry in sorted(entries, key=lambda item: (item.name, item.path)):
        if entry.name in entries_by_name:
            errors.append(f"duplicate inventory package name: {entry.name}")
        else:
            entries_by_name[entry.name] = entry

        if entry.path in entries_by_path:
            errors.append(f"duplicate inventory package path: {entry.path}")
        else:
            entries_by_path[entry.path] = entry

    packages_by_name = {package.name: package for package in packages}
    packages_by_path = {package.path: package for package in packages}

    for package in sorted(packages, key=lambda item: item.name):
        entry = entries_by_name.get(package.name)
        if entry is None:
            errors.append(
                f"unclassified crates/ package: {package.name} ({package.path})"
            )
            continue

        if entry.path != package.path:
            errors.append(
                f"inventory path mismatch for {package.name}: "
                f"expected {package.path}, found {entry.path}"
            )

        if package.publishable and entry.classification != "published-library":
            errors.append(
                f"publishable crate {package.name} must be classified "
                "published-library"
            )
        if not package.publishable and entry.classification == "published-library":
            errors.append(
                f"private crate {package.name} cannot be classified "
                "published-library"
            )
        if entry.classification == "published-library" and not package.has_lib_target:
            errors.append(f"published-library crate {package.name} has no lib target")

    for entry in sorted(entries, key=lambda item: item.name):
        if not entry.path.startswith("crates/"):
            continue

        package_by_name = packages_by_name.get(entry.name)
        package_by_path = packages_by_path.get(entry.path)
        if package_by_name is None and package_by_path is None:
            errors.append(
                f"inventory references missing crates/ package: "
                f"{entry.name} ({entry.path})"
            )

    return errors


def validate_release_coverage(
    entries: Sequence[InventoryEntry], workflow_crates: set[str]
) -> list[str]:
    """Return governed release entries missing from the release workflow."""
    return [
        f"publishable crate missing from release-crates workflow: {entry.name}"
        for entry in sorted(entries, key=lambda item: item.name)
        if entry.release == "release-crates" and entry.name not in workflow_crates
    ]


def render_inventory(entries: Sequence[InventoryEntry]) -> str:
    """Render the authoritative inventory as deterministic Markdown."""
    lines = [
        "<!-- Generated by scripts/check_api_governance.py --write-doc. -->",
        "",
        "# API Surface Inventory",
        "",
        "This inventory is generated from `governance/api-surfaces.toml`.",
        "",
        "| Package | Path | Classification | SemVer | Release | Owner |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    lines.extend(
        "| "
        f"`{entry.name}` | `{entry.path}` | {entry.classification} | "
        f"{'yes' if entry.semver else 'no'} | {entry.release} | {entry.owner} |"
        for entry in sorted(entries, key=lambda item: item.name)
    )
    return "\n".join(lines) + "\n"


def _cargo_metadata() -> dict[str, Any]:
    completed = subprocess.run(
        [
            "cargo",
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--locked",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def _inventory_schema_error() -> str | None:
    data = tomllib.loads(INVENTORY_PATH.read_text())
    if data.get("schema-version") != 1:
        return "unsupported API surface inventory schema-version; expected 1"
    return None


def _run(command: str) -> list[str]:
    entries = load_inventory(INVENTORY_PATH)
    packages = packages_from_metadata(_cargo_metadata(), REPO_ROOT)
    errors = validate_inventory(packages, entries)

    schema_error = _inventory_schema_error()
    if schema_error is not None:
        errors.insert(0, schema_error)

    if errors:
        return errors

    rendered = render_inventory(entries)
    if command == "write-doc":
        INVENTORY_DOC_PATH.write_text(rendered)
        return []

    workflow_crates = release_crates(RELEASE_WORKFLOW_PATH.read_text())
    errors.extend(validate_release_coverage(entries, workflow_crates))
    if not INVENTORY_DOC_PATH.exists() or INVENTORY_DOC_PATH.read_text() != rendered:
        errors.append(
            "generated API surface inventory is out of date; "
            "run scripts/check_api_governance.py --write-doc"
        )
    return errors


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate SMG API surface governance"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write-doc", action="store_true")
    args = parser.parse_args(argv)

    try:
        errors = _run("write-doc" if args.write_doc else "check")
    except (OSError, KeyError, TypeError, ValueError, subprocess.CalledProcessError) as exc:
        errors = [f"API governance check failed: {exc}"]

    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
