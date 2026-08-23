#!/usr/bin/env python3
"""Published accuracy numbers, paired with the protocol needed to reproduce them.

A score is only comparable to a published one if the *protocol* matches: the
same problem set, the same number of runs, the same sampling settings, and the
same idea of what counts as correct. A table of bare numbers invites the
mistake of diffing two things that were never measured the same way.

So each entry here carries its protocol and its source alongside the number,
and `run_eval.py` prints them next to every result. When a run lands outside
the band, the first question is always "did we run what they ran?" — these
fields are what let someone answer that without re-reading a methodology PDF.

Tolerance is deliberately generous. These references exist to catch *gross*
serving breakage — a chat template that renders wrong, a reasoning parser that
leaks chain-of-thought into the answer, sampling params that never arrive.
Landing inside the band is consistent with correct serving; it is not a parity
certificate, and nobody should quote it as one.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Sampling:
    """Decode settings a reference was produced with.

    ``top_k`` is not an OpenAI-standard field. SMG accepts it (see
    ``crates/protocols/src/sampling_params.rs``), but a server that does not
    will usually reject the whole request rather than ignore the key, so
    ``run_eval.py`` can be told to drop it.
    """

    temperature: float
    top_p: float
    top_k: int | None = None
    max_tokens: int = 32768

    def as_request_fields(self, include_top_k: bool = True) -> dict[str, object]:
        body: dict[str, object] = {
            "temperature": self.temperature,
            "top_p": self.top_p,
            "max_tokens": self.max_tokens,
        }
        if include_top_k and self.top_k is not None:
            body["top_k"] = self.top_k
        return body


@dataclass(frozen=True)
class Reference:
    """One published score plus everything needed to judge a reproduction."""

    suite: str
    model: str
    score: float
    """Published accuracy, in percent."""

    tolerance: float
    """Half-width of the accept band, in percentage points."""

    runs: int
    """Independent runs the published number averaged over."""

    sampling: Sampling
    protocol: str
    """One-paragraph statement of how the published number was produced."""

    source: str
    caveats: tuple[str, ...] = field(default_factory=tuple)
    """Known ways our reproduction may legitimately differ from the published run."""

    def band(self) -> tuple[float, float]:
        """Accept band, clamped to a valid accuracy range."""
        return (max(0.0, self.score - self.tolerance), min(100.0, self.score + self.tolerance))

    def verdict(self, observed: float) -> str:
        low, high = self.band()
        if observed < low:
            return "BELOW"
        if observed > high:
            return "ABOVE"
        return "WITHIN"


MUSE_GLIMMER = "meta-models/Muse-Glimmer-30B"

# Sampling the publisher used for every Muse-Glimmer benchmark: "we use high
# reasoning strength, and temperature=1.0/top_p=0.95/top_k=64 across all
# benchmarks". Reasoning strength needs no plumbing — the model's own chat
# template defaults it to 'high', which is the setting that was benchmarked.
MUSE_GLIMMER_SAMPLING = Sampling(temperature=1.0, top_p=0.95, top_k=64, max_tokens=32768)


REFERENCES: dict[tuple[str, str], Reference] = {
    ("aime_2026", MUSE_GLIMMER): Reference(
        suite="aime_2026",
        model=MUSE_GLIMMER,
        score=94.7,
        # Wide on purpose. Thirty problems means one problem is worth 3.3pp, and
        # the ten runs share those same thirty problems, so run-to-run variance
        # is correlated and a naive binomial interval understates it. A band
        # this wide still separates "serving works" from every breakage mode we
        # care about, which score in the 0-60 range rather than the high 80s.
        tolerance=5.0,
        runs=10,
        sampling=MUSE_GLIMMER_SAMPLING,
        protocol=(
            "Full 30-question AIME 2026 set. Answers are integers in [0, 999], graded by "
            "exact match. Results averaged over 10 runs to reduce variance. Publisher "
            "reports this as an internal run of their own model, not a third-party number."
        ),
        source="https://research.meta.ai/static/muse-glimmer-methodology",
        caveats=(
            "The methodology does not publish the prompt used. We use the MathArena "
            "convention (step-by-step, final answer in \\boxed{}), which is the standard "
            "for this dataset but is not guaranteed to be theirs.",
            "Hardware, engine and checkpoint precision differ from the publisher's setup.",
        ),
    ),
}


def lookup(suite: str, model: str) -> Reference | None:
    """Find a reference, tolerating the ``-FC``-style suffixes harnesses append."""
    if (suite, model) in REFERENCES:
        return REFERENCES[(suite, model)]
    for (ref_suite, ref_model), ref in REFERENCES.items():
        if ref_suite == suite and model.startswith(ref_model):
            return ref
    return None
