# Accuracy references

Score one serving arm against a number the model's publisher put in print.

## Why this exists alongside `scripts/bfcl` and `scripts/tau2`

Those two are **A/B** harnesses: they hold the model fixed, vary the frontend,
and report a delta. That answers *did this change move the number*.

It cannot answer a different question that comes up every time SMG picks up a
new model family: **is our serving of this model faithful at all?** A delta is
blind to a fault that both arms share, and for a genuinely new format there is
often no second arm — no other engine can serve the model yet, so there is
nothing to A/B against.

This harness produces an **absolute** score and holds it against a published
one, with the reproduction protocol recorded beside it.

## What it validates

A run exercises the whole serving path end to end: chat-template rendering,
the reasoning parser, sampling-parameter plumbing, and detokenization.

Grading reads the assistant's `content` only, never `reasoning_content`. That
is deliberate, and it is what makes the score a real test of our parsing: if
the reasoning parser stops separating channels and chain-of-thought lands in
`content`, the final answer gets buried and the score falls. The report calls
out a run where no reply carried `reasoning_content` at all, because on a
reasoning model that means the parser never engaged.

It does **not** validate tool parsing — no suite here calls tools. Use
`scripts/bfcl` for that.

## Running

The endpoint must already be serving. `scripts/bfcl/launch_arm.sh b` brings up
SMG in front of vLLM or SGLang and prints a base URL, so there is no reason to
reimplement launching here:

```bash
export BFCL_MODEL=meta-models/Muse-Glimmer-30B
export BFCL_ARM_B_WORKER=sglang BFCL_GPU=0,1 BFCL_TP=2
BASE_URL=$(scripts/bfcl/launch_arm.sh b)

python scripts/eval/run_eval.py \
    --base-url "$BASE_URL" \
    --model meta-models/Muse-Glimmer-30B \
    --suite aime_2026 \
    --out /tmp/aime.md --json-out /tmp/aime.json

scripts/bfcl/launch_arm.sh stop
```

Useful flags: `--runs` to override the reference's run count, `--dataset-file`
to read the problem set from local JSON instead of the network, `--no-top-k`
for servers that reject the non-standard `top_k` field, and
`--reasoning-strength` to force a `chat_template_kwargs` value instead of
letting the template default.

## Exit codes

| code | meaning |
| --- | --- |
| 0 | inside the reference band |
| 1 | outside it |
| 2 | not comparable — too many request failures, or nothing scored |

Exit 2 is the one that matters most. A harness that cannot reach the server
scores 0.00%, which looks exactly like a catastrophic regression; telling those
apart by eye has cost real time on this repo before. The guard fires when the
success rate drops below 95%, or when no reply contained a readable answer.

## Suites

### `aime_2026`

Thirty AIME 2026 problems from `MathArena/aime_2026`, integer answers in
[0, 999], exact-match graded, averaged over ten runs — 300 generations.

It is the first suite because it is the only benchmark on the Muse-Glimmer card
that is all of: run by the publisher themselves (so it is reproducible at all,
unlike the entries sourced from a third party), graded deterministically (no
judge model, so no judge cost and no judge variance), text-only (no container,
no sandbox), small, and scored near ceiling — which is what makes it sharp. A
subtly broken serving path cannot accidentally land at 94.

## Adding a suite

Implement `load`/`render`/`grade` in `suites.py`, register it in `SUITES`, add a
`Reference` with its protocol and source, and unit-test the grader.

**Test the grader.** It is the one place an accuracy harness fails silently: a
broken grader does not raise, it returns a number that is merely too low, and
that number gets blamed on whatever was being tested. This repo has already
lost 38 percentage points to one mis-set grading flag.

For the same reason, prefer suites whose official verifier you can *use* over
suites whose verifier you would have to reimplement. IFBench was the obvious
second suite here and was deliberately left out: there is no installable
verifier for it, and its published set is 294 tasks against the 300 shipped on
the Hub, so a local reimplementation would diverge from the published protocol
in two ways at once and produce a number that looks comparable but is not.

## Reading the result

A score inside the band is *consistent with* correct serving. It is not proof
of it, and it is not a parity claim about the model — the band is wide enough
to catch gross breakage, not to certify agreement to a point.

Every reference records known divergences from the published run in its
`caveats`, and the report prints them. For `aime_2026` the prompt is one:
the publisher does not say what prompt they used, so we use the dataset's
conventional one.
