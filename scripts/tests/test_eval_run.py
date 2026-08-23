"""Tests for scripts/eval/run_eval.py.

Two things here are load-bearing. The averaging must match the published
protocol (a mean over runs, not over all generations pooled), and the
incompleteness guard must fire before a verdict is issued — a harness that
cannot reach the server scores 0.00%, which is indistinguishable by eye from a
catastrophic regression. Reporting that as a regression has wasted real time on
this repo before, so it is pinned here.
"""

from __future__ import annotations

import sys
import urllib.error
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "eval"))

import reference  # noqa: E402
import run_eval  # noqa: E402
import suites  # noqa: E402


def _reference(runs: int = 2) -> reference.Reference:
    return reference.Reference(
        suite="aime_2026",
        model="test/model",
        score=75.0,
        tolerance=5.0,
        runs=runs,
        sampling=reference.Sampling(temperature=1.0, top_p=0.95, top_k=64),
        protocol="test protocol",
        source="https://example.invalid/methodology",
    )


class TestIncompleteness:
    def _summary(self, **overrides: object) -> run_eval.RunSummary:
        summary = run_eval.RunSummary(suite="aime_2026", model="m", runs=2, problems=2)
        summary.attempts_total = 100
        summary.failures = 0
        summary.unparsable = 0
        for key, value in overrides.items():
            setattr(summary, key, value)
        return summary

    def test_healthy_run_is_complete(self) -> None:
        assert run_eval.incompleteness(self._summary()) is None

    def test_no_attempts_is_incomplete(self) -> None:
        assert run_eval.incompleteness(self._summary(attempts_total=0)) is not None

    def test_mass_request_failure_is_incomplete_not_a_regression(self) -> None:
        """0% because the server was unreachable is not a score."""
        reason = run_eval.incompleteness(self._summary(failures=90))
        assert reason is not None and "transport failures" in reason

    def test_a_few_failures_still_scores(self) -> None:
        assert run_eval.incompleteness(self._summary(failures=2)) is None

    def test_nothing_parsable_is_incomplete(self) -> None:
        reason = run_eval.incompleteness(self._summary(unparsable=100))
        assert reason is not None and "readable answer" in reason


class TestEvaluateAggregation:
    """Averaging is per-run, matching "results averaged over N runs"."""

    def setup_method(self) -> None:
        self.problems = [
            suites.Problem(key="1", prompt="always", answer="1"),
            suites.Problem(key="2", prompt="sometimes", answer="2"),
        ]

    def _fake_chat(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: dict[str, int] = {}

        def fake(base_url, model, messages, body_fields, template_kwargs, timeout, retries):  # noqa: ANN001, ARG001
            prompt = messages[0]["content"]
            key = "1" if prompt.startswith("always") else "2"
            index = seen.get(key, 0)
            seen[key] = index + 1
            # Problem 1 is always right; problem 2 only on the first run.
            answer = key if (key == "1" or index == 0) else "999"
            return rf"\boxed{{{answer}}}", True, None

        monkeypatch.setattr(run_eval, "chat_once", fake)

    def test_mean_of_per_run_scores(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._fake_chat(monkeypatch)
        summary, attempts = run_eval.evaluate(
            suite=suites.Aime2026(),
            problems=self.problems,
            base_url="http://x",
            model="test/model",
            ref=_reference(),
            runs=2,
            concurrency=1,  # keeps the fake's call ordering deterministic
            timeout=5,
            retries=0,
            include_top_k=True,
            template_kwargs=None,
        )
        assert summary.per_run_scores == [100.0, 50.0]
        assert summary.score == 75.0
        assert summary.attempts_total == 4 and summary.failures == 0
        assert summary.reasoning_present == 4
        assert summary.per_problem_solve_rate == {"1": 1.0, "2": 0.5}
        assert len(attempts) == 4

    def test_request_errors_are_counted_not_scored(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def failing(*args, **kwargs):  # noqa: ANN002, ANN003, ARG001
            return "", False, "HTTP 503: unavailable"

        monkeypatch.setattr(run_eval, "chat_once", failing)
        summary, _ = run_eval.evaluate(
            suite=suites.Aime2026(),
            problems=self.problems,
            base_url="http://x",
            model="test/model",
            ref=_reference(),
            runs=1,
            concurrency=1,
            timeout=5,
            retries=0,
            include_top_k=True,
            template_kwargs=None,
        )
        assert summary.failures == 2
        assert summary.per_run_scores == []
        assert run_eval.incompleteness(summary) is not None


class TestTransportRobustness:
    """A dead or misbehaving server must degrade to a counted failure, never a crash."""

    def test_unreadable_error_body_does_not_escape(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Found by the end-to-end smoke test: reading an error body can itself raise.

        A peer that resets the connection while we read its 5xx body used to
        take the whole run down with a traceback, which reports as a crash
        instead of the INCOMPLETE verdict a dead server deserves.
        """

        class ResettingError(urllib.error.HTTPError):
            def __init__(self) -> None:
                super().__init__("http://x", 503, "unavailable", {}, None)

            def read(self, *args, **kwargs):  # noqa: ANN002, ANN003, ARG002
                raise ConnectionResetError(54, "Connection reset by peer")

        def raising(*args, **kwargs):  # noqa: ANN002, ANN003, ARG001
            raise ResettingError()

        monkeypatch.setattr(run_eval.urllib.request, "urlopen", raising)
        content, had_reasoning, error = run_eval.chat_once(
            "http://x", "m", [{"role": "user", "content": "hi"}], {}, None, timeout=1, retries=0
        )
        assert content == "" and not had_reasoning
        assert error is not None and "503" in error

    def test_unexpected_worker_error_becomes_an_attempt_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def exploding(*args, **kwargs):  # noqa: ANN002, ANN003, ARG001
            raise RuntimeError("boom")

        monkeypatch.setattr(run_eval, "chat_once", exploding)
        summary, attempts = run_eval.evaluate(
            suite=suites.Aime2026(),
            problems=[suites.Problem(key="1", prompt="p", answer="1")],
            base_url="http://x",
            model="m",
            ref=_reference(),
            runs=1,
            concurrency=1,
            timeout=1,
            retries=0,
            include_top_k=True,
            template_kwargs=None,
        )
        assert summary.failures == 1
        assert attempts[0].error is not None and "RuntimeError" in attempts[0].error


class TestReport:
    def test_report_states_verdict_and_provenance(self) -> None:
        summary = run_eval.RunSummary(suite="aime_2026", model="m", runs=2, problems=2)
        summary.score = 75.0
        summary.attempts_total = 4
        summary.reasoning_present = 4
        report = run_eval.build_report(summary, _reference())
        assert "WITHIN" in report
        assert "test protocol" in report
        assert "https://example.invalid/methodology" in report
        assert "top_k=64" in report

    def test_missing_reasoning_is_called_out(self) -> None:
        """Silence here would hide a disengaged reasoning parser."""
        summary = run_eval.RunSummary(suite="aime_2026", model="m", runs=1, problems=2)
        summary.attempts_total = 2
        summary.reasoning_present = 0
        report = run_eval.build_report(summary, _reference())
        assert "reasoning parser is not engaged" in report
