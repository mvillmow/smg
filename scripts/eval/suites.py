#!/usr/bin/env python3
"""Evaluation suites: how to load a problem set, prompt it, and grade a reply.

A suite is deliberately three small pure-ish pieces — ``load``, ``render``,
``grade`` — because grading is where accuracy harnesses go wrong quietly. A
mis-graded run does not crash; it produces a plausible number that is simply
too low, and the number then gets attributed to whatever change is being
tested. That failure has already cost this repo once, so every grader here is
a pure function over text with unit tests in ``scripts/tests/``.

Grading reads the assistant's ``content`` only, never ``reasoning_content``.
That is not incidental: on a reasoning model the chain-of-thought is full of
discarded candidate answers, so a grader that saw it would score noise. It
also makes the suite a real test of the serving path — a reasoning parser that
leaks thinking into ``content`` shows up here as a score drop.
"""

from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass

ROWS_API = "https://datasets-server.huggingface.co/rows"


@dataclass(frozen=True)
class Problem:
    """One graded item."""

    key: str
    prompt: str
    answer: str


@dataclass(frozen=True)
class Grade:
    """Outcome of grading one reply."""

    correct: bool
    extracted: str | None

    @property
    def unparsable(self) -> bool:
        """No answer could be located at all.

        Worth counting separately from "wrong": a wrong answer is the model's
        problem, but a spike in unparsable replies usually means ours — a
        truncated generation, or reasoning leaking into ``content`` and burying
        the final answer.
        """
        return self.extracted is None


def _fetch_rows(dataset: str, config: str, split: str, limit: int) -> list[dict]:
    """Read rows over the datasets-server REST API.

    Deliberately not the ``datasets`` library: this keeps the harness free of a
    heavy dependency that CI would otherwise have to install just to read a few
    hundred short rows.
    """
    rows: list[dict] = []
    while len(rows) < limit:
        query = urllib.parse.urlencode(
            {
                "dataset": dataset,
                "config": config,
                "split": split,
                "offset": len(rows),
                "length": min(100, limit - len(rows)),
            }
        )
        with urllib.request.urlopen(f"{ROWS_API}?{query}", timeout=60) as response:
            page = json.load(response).get("rows", [])
        if not page:
            break
        rows.extend(entry["row"] for entry in page)
    return rows


def extract_boxed(text: str) -> str | None:
    r"""Return the contents of the LAST ``\boxed{...}``, honouring nested braces.

    Brace matching rather than a regex, because real answers nest:
    ``\boxed{\frac{1}{2}}`` truncates to ``\frac{1`` under ``\{([^}]*)\}``.
    The last box wins — models routinely box an intermediate result and then
    box the final answer again.
    """
    marker = r"\boxed"
    start = text.rfind(marker)
    if start == -1:
        return None

    cursor = start + len(marker)
    while cursor < len(text) and text[cursor].isspace():
        cursor += 1
    if cursor >= len(text) or text[cursor] != "{":
        return None

    depth = 0
    for index in range(cursor, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return text[cursor + 1 : index]
    return None  # unbalanced: generation was probably truncated


_TEXT_WRAPPER = re.compile(r"\\(?:text|mathrm|mbox)\s*\{([^{}]*)\}")
_LATEX_SPACING = re.compile(r"\\[,!;:\s]|\\quad|\\qquad|\\ ")


def normalize_integer(raw: str) -> str | None:
    """Reduce a boxed fragment to a canonical integer string, or None.

    AIME answers are integers in [0, 999], but models dress them up: ``\text{277}``,
    ``1,000``, ``$277$``, ``277.``, ``077``. Strip the dressing, then insist on
    something that is genuinely an integer — returning None rather than guessing
    keeps "we could not read this" distinguishable from "the model was wrong".
    """
    value = _TEXT_WRAPPER.sub(r"\1", raw)
    value = _LATEX_SPACING.sub("", value)
    value = value.replace("$", "").replace(",", "").replace(" ", "")
    value = value.rstrip(".")
    if value.startswith("+"):
        value = value[1:]

    if not re.fullmatch(r"-?\d+", value):
        return None
    return str(int(value))


class Aime2026:
    """AIME 2026: 30 problems, integer answers, exact-match grading.

    Chosen as the first suite because it is the only benchmark on the target
    model card that is simultaneously run by the publisher themselves (so it is
    reproducible at all), graded deterministically (no judge model, no judge
    variance, no judge cost), text-only (no sandbox, no container), small (300
    generations at the published protocol), and scored near ceiling — which is
    what makes it a sharp instrument. A serving path that is subtly broken
    cannot accidentally land at 94.
    """

    name = "aime_2026"
    dataset = "MathArena/aime_2026"
    config = "default"
    split = "train"
    expected_size = 30

    # The methodology does not publish its prompt. This is the MathArena
    # convention for the dataset and the standard way to make the final answer
    # machine-locatable; reference.py records the divergence as a caveat.
    INSTRUCTION = (
        "Please reason step by step, and put your final answer within \\boxed{}. "
        "The answer is an integer between 0 and 999 inclusive."
    )

    def load(self, path: str | None = None) -> list[Problem]:
        """Load the problem set, from a local JSON file if one is given.

        The local path exists so a run is not hostage to outbound network
        access from a CI runner.
        """
        if path:
            with open(path, encoding="utf-8") as handle:
                rows = json.load(handle)
        else:
            rows = _fetch_rows(self.dataset, self.config, self.split, self.expected_size)

        problems = [
            Problem(
                key=str(row["problem_idx"]),
                prompt=row["problem"],
                answer=str(row["answer"]).strip(),
            )
            for row in rows
        ]
        if len(problems) != self.expected_size:
            raise ValueError(
                f"{self.dataset}: expected {self.expected_size} problems, got {len(problems)}. "
                "The published protocol is the full set; a partial set is not comparable."
            )
        return problems

    def render(self, problem: Problem) -> list[dict[str, str]]:
        return [{"role": "user", "content": f"{problem.prompt}\n\n{self.INSTRUCTION}"}]

    def grade(self, problem: Problem, content: str) -> Grade:
        boxed = extract_boxed(content or "")
        if boxed is None:
            return Grade(correct=False, extracted=None)

        extracted = normalize_integer(boxed)
        if extracted is None:
            return Grade(correct=False, extracted=None)

        expected = normalize_integer(problem.answer)
        return Grade(correct=expected is not None and extracted == expected, extracted=extracted)


SUITES: dict[str, Aime2026] = {Aime2026.name: Aime2026()}


def get_suite(name: str) -> Aime2026:
    if name not in SUITES:
        raise KeyError(f"unknown suite {name!r}; available: {', '.join(sorted(SUITES))}")
    return SUITES[name]
