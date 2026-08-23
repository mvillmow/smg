"""Tests for scripts/eval/suites.py.

Grading is the part of an accuracy harness that fails silently. A broken
grader does not raise; it returns a number that is merely too low, and that
number then gets blamed on whatever was being tested. These tests pin the
answer-extraction behaviour that the score depends on.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "eval"))

import suites  # noqa: E402


class TestExtractBoxed:
    def test_simple_box(self) -> None:
        assert suites.extract_boxed(r"so the answer is \boxed{277}.") == "277"

    def test_nested_braces_survive(self) -> None:
        r"""A regex like \\boxed\{([^}]*)\} truncates this to ``\frac{1``."""
        assert suites.extract_boxed(r"\boxed{\frac{1}{2}}") == r"\frac{1}{2}"

    def test_last_box_wins(self) -> None:
        """Models box an intermediate result and then box the real answer."""
        text = r"first \boxed{100}, on reflection \boxed{277}"
        assert suites.extract_boxed(text) == "277"

    def test_whitespace_before_brace(self) -> None:
        assert suites.extract_boxed(r"\boxed {277}") == "277"

    def test_absent_box_is_none(self) -> None:
        assert suites.extract_boxed("the answer is 277") is None

    def test_unbalanced_box_is_none(self) -> None:
        """A truncated generation must not silently yield a partial answer."""
        assert suites.extract_boxed(r"\boxed{27") is None

    def test_empty_input(self) -> None:
        assert suites.extract_boxed("") is None


class TestNormalizeInteger:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("277", "277"),
            (r"\text{277}", "277"),
            (r"\mathrm{62}", "62"),
            ("1,000", "1000"),
            ("$277$", "277"),
            ("277.", "277"),
            ("077", "77"),  # AIME prints answers zero-padded to three digits
            ("+277", "277"),
            (" 277 ", "277"),
            (r"27\,7", "277"),
        ],
    )
    def test_accepts_dressed_integers(self, raw: str, expected: str) -> None:
        assert suites.normalize_integer(raw) == expected

    @pytest.mark.parametrize("raw", [r"\frac{1}{2}", "abc", "", "27.5", "2/3", "~277"])
    def test_rejects_non_integers(self, raw: str) -> None:
        """None means "unreadable", which the harness counts apart from "wrong"."""
        assert suites.normalize_integer(raw) is None


class TestAimeGrading:
    def setup_method(self) -> None:
        self.suite = suites.Aime2026()
        self.problem = suites.Problem(key="1", prompt="...", answer="277")

    def test_correct_answer(self) -> None:
        grade = self.suite.grade(self.problem, r"Therefore \boxed{277}.")
        assert grade.correct
        assert grade.extracted == "277"
        assert not grade.unparsable

    def test_wrong_answer_is_not_unparsable(self) -> None:
        grade = self.suite.grade(self.problem, r"\boxed{123}")
        assert not grade.correct
        assert not grade.unparsable

    def test_missing_box_is_unparsable(self) -> None:
        grade = self.suite.grade(self.problem, "The answer is 277.")
        assert not grade.correct
        assert grade.unparsable

    def test_zero_padded_expectation_matches(self) -> None:
        """Datasets store 077; models emit 77. Both must normalize the same way."""
        padded = suites.Problem(key="2", prompt="...", answer="077")
        assert self.suite.grade(padded, r"\boxed{77}").correct

    def test_empty_content_is_unparsable(self) -> None:
        assert self.suite.grade(self.problem, "").unparsable

    def test_final_box_decides(self) -> None:
        """Scoring the first box would mark a self-corrected answer wrong."""
        assert self.suite.grade(self.problem, r"\boxed{5} ... actually \boxed{277}").correct

    def test_render_includes_problem_and_instruction(self) -> None:
        messages = self.suite.render(self.problem)
        assert len(messages) == 1 and messages[0]["role"] == "user"
        assert self.problem.prompt in messages[0]["content"]
        assert "boxed" in messages[0]["content"]


class TestLoad:
    def test_partial_dataset_is_rejected(self, tmp_path: Path) -> None:
        """The published protocol is the full 30; a subset is not comparable."""
        dataset = tmp_path / "partial.json"
        dataset.write_text('[{"problem_idx": 1, "problem": "p", "answer": 277}]')
        with pytest.raises(ValueError, match="expected 30 problems"):
            suites.Aime2026().load(str(dataset))

    def test_full_local_dataset_loads(self, tmp_path: Path) -> None:
        rows = [{"problem_idx": i, "problem": f"p{i}", "answer": i} for i in range(30)]
        dataset = tmp_path / "full.json"
        dataset.write_text(str(rows).replace("'", '"'))
        problems = suites.Aime2026().load(str(dataset))
        assert len(problems) == 30
        assert problems[0].answer == "0"


def test_registry_exposes_aime() -> None:
    assert suites.get_suite("aime_2026").name == "aime_2026"
    with pytest.raises(KeyError):
        suites.get_suite("nope")
