"""Tests for scripts/eval/reference.py.

The reference table decides whether a run is called a reproduction or a
regression, so the band arithmetic and the lookup need to be exact. A silently
wrong band is worse than no band at all: it converts a real breakage into a
green check.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "eval"))

import reference  # noqa: E402


class TestBand:
    def test_band_is_symmetric_around_the_published_score(self) -> None:
        ref = reference.REFERENCES[("aime_2026", reference.MUSE_GLIMMER)]
        low, high = ref.band()
        assert low == pytest.approx(ref.score - ref.tolerance)
        assert high == pytest.approx(ref.score + ref.tolerance)

    def test_band_clamps_to_valid_accuracy(self) -> None:
        """A near-ceiling reference must not produce an upper bound above 100."""
        ref = reference.Reference(
            suite="s",
            model="m",
            score=98.0,
            tolerance=5.0,
            runs=1,
            sampling=reference.Sampling(temperature=1.0, top_p=0.95),
            protocol="p",
            source="u",
        )
        assert ref.band() == (93.0, 100.0)

    @pytest.mark.parametrize(
        ("observed", "expected"),
        [(94.7, "WITHIN"), (89.7, "WITHIN"), (99.7, "WITHIN"), (89.6, "BELOW"), (99.8, "ABOVE")],
    )
    def test_verdict_boundaries(self, observed: float, expected: str) -> None:
        ref = reference.REFERENCES[("aime_2026", reference.MUSE_GLIMMER)]
        assert ref.verdict(observed) == expected


class TestSampling:
    def test_top_k_included_by_default(self) -> None:
        fields = reference.MUSE_GLIMMER_SAMPLING.as_request_fields()
        assert fields["top_k"] == 64
        assert fields["temperature"] == 1.0
        assert fields["top_p"] == 0.95

    def test_top_k_can_be_dropped(self) -> None:
        """top_k is not OpenAI-standard; some servers reject the whole request."""
        assert "top_k" not in reference.MUSE_GLIMMER_SAMPLING.as_request_fields(include_top_k=False)

    def test_absent_top_k_is_never_emitted(self) -> None:
        sampling = reference.Sampling(temperature=0.7, top_p=1.0)
        assert "top_k" not in sampling.as_request_fields()


class TestLookup:
    def test_exact_match(self) -> None:
        assert reference.lookup("aime_2026", reference.MUSE_GLIMMER) is not None

    def test_suffixed_model_name_resolves(self) -> None:
        """Harnesses append suffixes like -FC to the served name."""
        ref = reference.lookup("aime_2026", f"{reference.MUSE_GLIMMER}-FC")
        assert ref is not None and ref.score == 94.7

    def test_unknown_suite_or_model_is_none(self) -> None:
        assert reference.lookup("nope", reference.MUSE_GLIMMER) is None
        assert reference.lookup("aime_2026", "someone/else") is None


def test_every_reference_carries_its_provenance() -> None:
    """A number without a protocol and a source cannot be argued with later."""
    for ref in reference.REFERENCES.values():
        assert ref.protocol.strip(), f"{ref.suite}/{ref.model} has no protocol"
        assert ref.source.startswith("http"), f"{ref.suite}/{ref.model} has no source URL"
        assert 0 < ref.tolerance <= 15, "tolerance should be a meaningful, narrow band"
        assert ref.runs >= 1
