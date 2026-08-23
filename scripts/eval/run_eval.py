#!/usr/bin/env python3
"""Score one already-serving OpenAI endpoint against a published reference.

This is not an A/B. ``scripts/bfcl`` and ``scripts/tau2`` compare two arms and
report a delta, which answers "did this change move the number". A different
question comes up whenever SMG picks up a new model family: *is our serving of
this model faithful at all?* A delta cannot answer that — two arms can agree
and both be wrong, and when a model is new enough that no baseline engine can
serve it, there is no second arm to compare against in the first place.

So this driver produces an absolute score and holds it against a number the
model's publisher put in print, with the reproduction protocol recorded next to
it (see ``reference.py``). The endpoint must already be serving —
``scripts/bfcl/launch_arm.sh b`` brings up SMG in front of vLLM or SGLang and
prints a base URL, and there is no reason to reimplement that here.

    python scripts/eval/run_eval.py \\
        --base-url http://127.0.0.1:31200 \\
        --model meta-models/Muse-Glimmer-30B \\
        --suite aime_2026 --runs 10 \\
        --out /tmp/aime.md --json-out /tmp/aime.json

Exit codes: ``0`` inside the reference band, ``1`` outside it, ``2`` the run did
not produce a comparable result (too many request failures, or nothing scored).
Exit 2 matters — a harness that reports 0.00% after failing to reach the server
looks exactly like a catastrophic regression, and telling those apart by eye has
burned this repo before.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import reference as reference_mod  # noqa: E402
import suites as suites_mod  # noqa: E402

EXIT_OK = 0
EXIT_OUT_OF_BAND = 1
EXIT_INCOMPLETE = 2

# Below this share of successful requests the run is not comparable to anything.
MIN_COMPLETION_RATE = 0.95


@dataclass
class Attempt:
    """One generation for one problem on one run."""

    problem_key: str
    run: int
    correct: bool = False
    extracted: str | None = None
    had_reasoning: bool = False
    content_chars: int = 0
    error: str | None = None


@dataclass
class RunSummary:
    suite: str
    model: str
    runs: int
    problems: int
    per_run_scores: list[float] = field(default_factory=list)
    score: float = 0.0
    unparsable: int = 0
    failures: int = 0
    reasoning_present: int = 0
    attempts_total: int = 0
    per_problem_solve_rate: dict[str, float] = field(default_factory=dict)
    wall_seconds: float = 0.0


def chat_once(
    base_url: str,
    model: str,
    messages: list[dict[str, str]],
    body_fields: dict[str, object],
    template_kwargs: dict[str, object] | None,
    timeout: int,
    retries: int,
) -> tuple[str, bool, str | None]:
    """POST one chat completion. Returns (content, had_reasoning, error)."""
    payload: dict[str, object] = {"model": model, "messages": messages, **body_fields}
    if template_kwargs:
        payload["chat_template_kwargs"] = template_kwargs
    data = json.dumps(payload).encode()

    last_error: str | None = None
    for attempt in range(retries + 1):
        request = urllib.request.Request(
            f"{base_url.rstrip('/')}/v1/chat/completions",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                parsed = json.load(response)
            message = parsed["choices"][0]["message"]
            content = message.get("content") or ""
            reasoning = message.get("reasoning_content") or ""
            return content, bool(reasoning), None
        except urllib.error.HTTPError as exc:
            # Reading the error body can itself fail if the peer resets the
            # connection. Letting that escape would kill the whole run with a
            # traceback instead of counting one failed attempt — turning a dead
            # server into a crash rather than the INCOMPLETE verdict it is.
            try:
                detail = exc.read().decode(errors="replace")[:200]
            except Exception:  # noqa: BLE001 - diagnostics only
                detail = "<error body unavailable>"
            last_error = f"HTTP {exc.code}: {detail}"
            # A rejected request body will be rejected identically on retry.
            if exc.code < 500:
                break
        except Exception as exc:  # noqa: BLE001 - transport errors vary widely
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt < retries:
            time.sleep(2 * (attempt + 1))
    return "", False, last_error


def evaluate(
    suite: suites_mod.Aime2026,
    problems: list[suites_mod.Problem],
    base_url: str,
    model: str,
    ref: reference_mod.Reference,
    runs: int,
    concurrency: int,
    timeout: int,
    retries: int,
    include_top_k: bool,
    template_kwargs: dict[str, object] | None,
) -> tuple[RunSummary, list[Attempt]]:
    body_fields = ref.sampling.as_request_fields(include_top_k=include_top_k)
    work = [(problem, run) for run in range(runs) for problem in problems]

    def one(item: tuple[suites_mod.Problem, int]) -> Attempt:
        problem, run = item
        try:
            content, had_reasoning, error = chat_once(
                base_url,
                model,
                suite.render(problem),
                body_fields,
                template_kwargs,
                timeout,
                retries,
            )
        except Exception as exc:  # noqa: BLE001 - one bad attempt must not end the run
            return Attempt(problem.key, run, error=f"{type(exc).__name__}: {exc}")
        if error is not None:
            return Attempt(problem.key, run, error=error)
        grade = suite.grade(problem, content)
        return Attempt(
            problem_key=problem.key,
            run=run,
            correct=grade.correct,
            extracted=grade.extracted,
            had_reasoning=had_reasoning,
            content_chars=len(content),
        )

    started = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        attempts = list(pool.map(one, work))
    elapsed = time.time() - started

    summary = RunSummary(
        suite=suite.name,
        model=model,
        runs=runs,
        problems=len(problems),
        attempts_total=len(attempts),
        wall_seconds=round(elapsed, 1),
    )
    summary.failures = sum(1 for a in attempts if a.error)
    scored = [a for a in attempts if not a.error]
    summary.unparsable = sum(1 for a in scored if a.extracted is None)
    summary.reasoning_present = sum(1 for a in scored if a.had_reasoning)

    for run in range(runs):
        run_attempts = [a for a in scored if a.run == run]
        if run_attempts:
            correct = sum(1 for a in run_attempts if a.correct)
            summary.per_run_scores.append(round(100.0 * correct / len(run_attempts), 2))
    if summary.per_run_scores:
        summary.score = round(statistics.fmean(summary.per_run_scores), 2)

    for problem in problems:
        graded = [a for a in scored if a.problem_key == problem.key]
        if graded:
            rate = sum(1 for a in graded if a.correct) / len(graded)
            summary.per_problem_solve_rate[problem.key] = round(rate, 3)

    return summary, attempts


def incompleteness(summary: RunSummary) -> str | None:
    """Reasons the run cannot be compared to the reference at all."""
    if summary.attempts_total == 0:
        return "no attempts were made"
    completed = summary.attempts_total - summary.failures
    rate = completed / summary.attempts_total
    if rate < MIN_COMPLETION_RATE:
        return (
            f"only {completed}/{summary.attempts_total} requests succeeded "
            f"({rate:.1%} < {MIN_COMPLETION_RATE:.0%}); the score reflects transport "
            "failures, not model accuracy"
        )
    if completed and summary.unparsable == completed:
        return (
            "no reply contained a readable answer; this is a serving or prompting "
            "failure rather than a model score"
        )
    return None


def build_report(summary: RunSummary, ref: reference_mod.Reference | None) -> str:
    lines = [
        f"# {summary.suite} — {summary.model}",
        "",
        f"**Observed: {summary.score:.2f}%**"
        + (f" vs published **{ref.score:.2f}%**" if ref else " (no published reference)"),
        "",
    ]

    if ref:
        low, high = ref.band()
        verdict = ref.verdict(summary.score)
        symbol = {"WITHIN": "✅", "BELOW": "❌", "ABOVE": "⚠️"}[verdict]
        lines += [
            f"{symbol} **{verdict}** the accept band [{low:.2f}, {high:.2f}] "
            f"(±{ref.tolerance:.1f}pp)",
            "",
            "## Published protocol",
            "",
            ref.protocol,
            "",
            f"Source: {ref.source}",
            "",
            f"Sampling: temperature={ref.sampling.temperature}, top_p={ref.sampling.top_p}"
            + (f", top_k={ref.sampling.top_k}" if ref.sampling.top_k is not None else ""),
            "",
        ]
        if ref.caveats:
            lines += ["### Known divergences from the published run", ""]
            lines += [f"- {caveat}" for caveat in ref.caveats]
            lines.append("")

    completed = summary.attempts_total - summary.failures
    lines += [
        "## Run",
        "",
        "| metric | value |",
        "| --- | --- |",
        f"| problems | {summary.problems} |",
        f"| runs | {summary.runs} |",
        f"| attempts | {summary.attempts_total} |",
        f"| request failures | {summary.failures} |",
        f"| unparsable replies | {summary.unparsable} |",
        f"| replies with reasoning_content | {summary.reasoning_present}/{completed} |",
        f"| per-run scores | {', '.join(f'{s:.2f}' for s in summary.per_run_scores)} |",
        f"| wall time | {summary.wall_seconds}s |",
        "",
    ]

    if summary.per_run_scores and len(summary.per_run_scores) > 1:
        spread = max(summary.per_run_scores) - min(summary.per_run_scores)
        lines += [f"Run-to-run spread: {spread:.2f}pp.", ""]

    unsolved = sorted(k for k, v in summary.per_problem_solve_rate.items() if v == 0.0)
    if unsolved:
        lines += [f"Never solved in any run: {', '.join(unsolved)}.", ""]

    if summary.reasoning_present == 0 and completed:
        lines += [
            "> No reply carried `reasoning_content`. For a reasoning model that "
            "usually means the reasoning parser is not engaged, so thinking text is "
            "being scored as the answer.",
            "",
        ]

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--base-url", required=True, help="OpenAI base URL of a serving arm")
    parser.add_argument("--model", required=True, help="served model name")
    parser.add_argument("--suite", default="aime_2026", help="suite to run")
    parser.add_argument(
        "--runs", type=int, default=0, help="runs to average (default: the reference's)"
    )
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--timeout", type=int, default=1800, help="per-request timeout, seconds")
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--dataset-file", default=None, help="local JSON instead of fetching")
    parser.add_argument(
        "--max-tokens", type=int, default=0, help="override the reference's max_tokens"
    )
    parser.add_argument(
        "--no-top-k", action="store_true", help="omit top_k (servers that reject it)"
    )
    parser.add_argument(
        "--reasoning-strength",
        default=None,
        help="pass chat_template_kwargs.reasoning_strength; omit to use the template default",
    )
    parser.add_argument(
        "--reference-model", default=None, help="look the reference up under this name"
    )
    parser.add_argument("--out", default=None, help="write the markdown report here")
    parser.add_argument("--json-out", default=None, help="write raw results here")
    args = parser.parse_args()

    suite = suites_mod.get_suite(args.suite)
    ref = reference_mod.lookup(args.suite, args.reference_model or args.model)
    if ref is None:
        print(f"no published reference for suite={args.suite} model={args.model}", file=sys.stderr)
        return EXIT_INCOMPLETE

    if args.max_tokens:
        ref = reference_mod.Reference(
            **{
                **asdict(ref),
                "sampling": reference_mod.Sampling(
                    temperature=ref.sampling.temperature,
                    top_p=ref.sampling.top_p,
                    top_k=ref.sampling.top_k,
                    max_tokens=args.max_tokens,
                ),
            }
        )

    problems = suite.load(args.dataset_file)
    runs = args.runs or ref.runs
    template_kwargs = (
        {"reasoning_strength": args.reasoning_strength} if args.reasoning_strength else None
    )

    print(
        f"[eval] {suite.name}: {len(problems)} problems x {runs} runs "
        f"= {len(problems) * runs} generations against {args.base_url}",
        file=sys.stderr,
    )

    summary, attempts = evaluate(
        suite=suite,
        problems=problems,
        base_url=args.base_url,
        model=args.model,
        ref=ref,
        runs=runs,
        concurrency=args.concurrency,
        timeout=args.timeout,
        retries=args.retries,
        include_top_k=not args.no_top_k,
        template_kwargs=template_kwargs,
    )

    report = build_report(summary, ref)
    if args.out:
        Path(args.out).write_text(report, encoding="utf-8")
    print(report)

    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps(
                {
                    "summary": asdict(summary),
                    "reference": asdict(ref),
                    "attempts": [asdict(a) for a in attempts],
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    reason = incompleteness(summary)
    if reason:
        print(f"\n[eval] INCOMPLETE: {reason}", file=sys.stderr)
        return EXIT_INCOMPLETE

    verdict = ref.verdict(summary.score)
    if verdict != "WITHIN":
        low, high = ref.band()
        print(
            f"\n[eval] {verdict}: {summary.score:.2f}% is outside [{low:.2f}, {high:.2f}]",
            file=sys.stderr,
        )
        return EXIT_OUT_OF_BAND

    print(
        f"\n[eval] OK: {summary.score:.2f}% reproduces the published {ref.score:.2f}%",
        file=sys.stderr,
    )
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
