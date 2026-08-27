#!/usr/bin/env python3
"""Named comparison scenarios on top of sim.py.

Each scenario is a list of legs; a leg is a profile-override set (dotted keys)
plus, for policy A/B, which prebuilt gateway binary to use. `compare` runs the
legs sequentially against a fresh fleet each and emits a side-by-side markdown
table built from every leg's report.json. Per-turn rows are always included,
so `turn-ab` (t2_ratio 1.0) needs only one leg.

Usage:
  scripts/generate_sim/scenarios.py list
  scripts/generate_sim/scenarios.py compare --scenario smg1-vs-smg8 \
      --profile scripts/generate_sim/profiles/local-small.json
  scripts/generate_sim/scenarios.py compare --scenario policy-ab \
      --profile ... --smg-bin-a /path/A/smg --smg-bin-b /path/B/smg
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sim  # noqa: E402

# Leg: (label, {dotted override: value}, smg_bin slot: None | "a" | "b").
# smg1-vs-smg8 keeps the same session_rps: aggregate rps is a loadgen-side
# property, so halving the gateway count concentrates rather than shrinks load.
SCENARIOS = {
    "smg1-vs-smg8": [
        ("smg1", {"smg_count": 1}, None),
        ("smg8", {"smg_count": 8}, None),
    ],
    "stable-key-vs-random-ingress": [
        ("hash-ingress", {"loadgen.ingress": "hash"}, None),
        ("random-ingress", {"loadgen.ingress": "random"}, None),
    ],
    # What does it take to sustain >= 0.80 aggregate cached/prompt? The
    # aggregate is (1-f)*prefix_share + f*followup_ratio where f is the
    # follow-up share of requests — a traffic property. Legs: today's
    # 1.5-turn mix; a ~5-6-turn conversational mix; the same plus a larger
    # shared prefix; and the same with the SMG placement TTL below the
    # think time — the config trap that silently forfeits the hits.
    "hit-rate-calibration": [
        ("baseline-1p5turn", {}, None),
        (
            "multiturn",
            {
                "loadgen.t2_ratio": 0.85,
                "loadgen.max_turns": 8,
                "loadgen.t2_suffix_tokens": 256,
            },
            None,
        ),
        (
            "multiturn-prefix4k",
            {
                "loadgen.t2_ratio": 0.85,
                "loadgen.max_turns": 8,
                "loadgen.t2_suffix_tokens": 256,
                "loadgen.system_prefix_tokens": 4096,
            },
            None,
        ),
        (
            "multiturn-ttl-trap",
            {
                "loadgen.t2_ratio": 0.85,
                "loadgen.max_turns": 8,
                "loadgen.t2_suffix_tokens": 256,
                "loadgen.think_secs": 6,
                "smg_flags": [
                    "--policy", "cache_aware",
                    "--cache-index", "hash",
                    "--cache-threshold", "0.60",
                    "--block-size", "128",
                    "--cache-boundaries", "3072,4096,6144,8192,12288,16384",
                    "--balance-abs-threshold", "8",
                    "--balance-rel-threshold", "1.2",
                    "--overlap-decay", "1.0",
                    "--disable-retries",
                    "--upstream-http2",
                    "--max-concurrent-requests", "180000",
                    "--queue-size", "512",
                    "--queue-timeout-secs", "5",
                    "--worker-overload-waiting-requests", "64",
                    "--worker-overload-token-usage", "0.9",
                    "--cache-ttl-secs", "2",
                ],
            },
            None,
        ),
    ],
    # The hash placement index is LOCAL to each SMG: turn-2 affinity only
    # survives if turn 2 reaches the same SMG. This isolates that effect.
    # Does a smaller SMG fleet raise cache hit rates? With sticky turns the
    # placement index fragmentation shouldn't matter; with scattered turns
    # the chance of landing on the SMG that holds the placement is 1/K.
    "fleet-size-sweep": [
        ("smg2-sticky", {"smg_count": 2}, None),
        ("smg8-sticky", {"smg_count": 8}, None),
        (
            "smg2-t2random",
            {
                "smg_count": 2,
                "loadgen.ingress": "random",
                "loadgen.turn2_ingress": "random",
            },
            None,
        ),
        (
            "smg8-t2random",
            {
                "smg_count": 8,
                "loadgen.ingress": "random",
                "loadgen.turn2_ingress": "random",
            },
            None,
        ),
    ],
    "turn2-same-vs-random-smg": [
        ("t2-same-smg", {"loadgen.turn2_ingress": "same"}, None),
        (
            "t2-random-smg",
            {"loadgen.ingress": "random", "loadgen.turn2_ingress": "random"},
            None,
        ),
    ],
    "cold-vs-warm-prefix": [
        ("cold", {"loadgen.system_prefix_tokens": 0}, None),
        ("warm", {"loadgen.system_prefix_tokens": 2048}, None),
    ],
    "turn-ab": [
        ("turn-ab", {"loadgen.t2_ratio": 1.0}, None),
    ],
    "policy-ab": [
        ("policy-a", {}, "a"),
        ("policy-b", {}, "b"),
    ],
}


def _get(mapping, *keys):
    node = mapping
    for key in keys:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def extract_rows(report):
    """Flatten one report.json into the comparison rows (all guarded)."""
    summary = report.get("loadgen_summary", {})
    req = report.get("requests", {})
    samples = report.get("samples", [])
    branches = report.get("cache_aware_branches", [])

    rss_peaks = [_get(s, "rss_kib", "peak") for s in samples]
    rss_peaks = [v for v in rss_peaks if v is not None]
    cpu_means = [_get(s, "cpu_pct", "mean") for s in samples]
    cpu_means = [v for v in cpu_means if v is not None]
    queue_peaks = [_get(s, "queue_depth", "peak") for s in samples]
    queue_peaks = [v for v in queue_peaks if v is not None]
    rejected = sum(s.get("rejected_total") or 0 for s in samples)

    branch_totals = {}
    for entry in branches:
        for name, count in entry.get("branches", {}).items():
            branch_totals[name] = branch_totals.get(name, 0) + count
    total_decisions = sum(branch_totals.values())
    hash_hit = branch_totals.get("hash_hit", 0)

    rows = {}
    totals = summary.get("totals", {})
    errors = totals.get("errors", {})
    err_count = sum(errors.values()) if isinstance(errors, dict) else errors
    requests_total = totals.get("requests")
    rows["ok"] = (
        requests_total - err_count
        if isinstance(requests_total, int) and isinstance(err_count, int)
        else None
    )
    rows["err"] = err_count
    rows["achieved_rps"] = summary.get("achieved_rps")
    for metric in ("ttft_ms", "e2e_ms"):
        for pct in ("p50", "p90", "p99"):
            rows["%s_%s" % (metric, pct)] = _get(summary, metric, pct)
    for turn in ("turn1", "turn2", "followup"):
        rows[turn + " cached ratio (loadgen)"] = _get(
            summary, "turns", turn, "cached_ratio_mean"
        )
    # Aggregate mean cached/prompt over all requests, from the per-turn
    # blocks (turn1 + all follow-ups), weighted by request count.
    parts = []
    for block in ("turn1", "followup"):
        n = _get(summary, "turns", block, "ok")
        r = _get(summary, "turns", block, "cached_ratio_mean")
        if isinstance(n, int) and n > 0 and isinstance(r, (int, float)):
            parts.append((n, r))
    total_n = sum(n for n, _ in parts)
    rows["AGGREGATE cached/prompt"] = (
        round(sum(n * r for n, r in parts) / total_n, 4) if total_n else None
    )
    rows["mean turns/session"] = summary.get("mean_turns_per_session")
    rows["t2 same-worker (loadgen)"] = summary.get("turn2_same_worker_rate")
    rows["followup same-worker"] = summary.get("followup_same_worker_rate")
    rows["t1 max worker share"] = _get(summary, "turn1_workers", "max_share")
    rows["t1 entropy (norm)"] = _get(summary, "turn1_workers", "normalized_entropy")
    for turn in ("turn1", "turn2"):
        rows[turn + " cached/prompt"] = _get(req, "turns", turn, "cached_over_prompt")
        rows[turn + " hit rate"] = _get(req, "turns", turn, "hit_rate")
        rows[turn + " CoV (fleet)"] = _get(req, "turns", turn, "imbalance", "cov_fleet")
    rows["t2 same-worker rate"] = req.get("t2_same_worker_rate")
    imb = req.get("overall_imbalance", {})
    rows["overall CoV (fleet)"] = imb.get("cov_fleet")
    rows["distinct workers"] = imb.get("distinct_workers")
    rows["hash_hit share"] = (
        round(hash_hit / total_decisions, 4) if total_decisions else None
    )
    rows["rss peak MiB (max smg)"] = (
        round(max(rss_peaks) / 1024, 1) if rss_peaks else None
    )
    rows["cpu mean % (max smg)"] = max(cpu_means) if cpu_means else None
    rows["queue depth peak"] = max(queue_peaks) if queue_peaks else None
    rows["rejected total"] = rejected
    return rows


def write_compare_md(scenario, leg_results, path):
    labels = [label for label, _ in leg_results]
    row_keys = []
    for _, rows in leg_results:
        for key in rows:
            if key not in row_keys:
                row_keys.append(key)
    lines = ["# generate-sim compare — %s" % scenario, ""]
    lines.append("| metric | " + " | ".join(labels) + " |")
    lines.append("|---|" + "---|" * len(labels))
    for key in row_keys:
        cells = [sim._fmt(rows.get(key)) for _, rows in leg_results]
        lines.append("| %s | %s |" % (key, " | ".join(cells)))
    lines.append("")
    for label, _ in leg_results:
        lines.append("- %s: see its run dir for report.md / report.json" % label)
    lines.append("")
    text = "\n".join(lines)
    with open(path, "w") as f:
        f.write(text)
    print(text)


def cmd_compare(args):
    legs = SCENARIOS[args.scenario]
    bins = {"a": args.smg_bin_a, "b": args.smg_bin_b, None: args.smg_bin}
    for _, _, slot in legs:
        if slot is not None and not bins[slot]:
            raise SystemExit(
                "scenario %s needs --smg-bin-%s (a prebuilt gateway binary)"
                % (args.scenario, slot)
            )

    base = sim.load_profile(args.profile)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_root = Path(args.out_root or sim.REPO_ROOT / "target" / "generate-sim")
    scenario_dir = out_root / ("%s-%s-%s" % (args.scenario, base.get("name", "run"), stamp))

    leg_results = []
    for idx, (label, overrides, slot) in enumerate(legs):
        profile = json.loads(json.dumps(base))  # deep copy: legs must not leak
        for key, val in overrides.items():
            sim.apply_override(profile, key, val)
        for raw in args.override:
            key, val = sim.parse_override_arg(raw)
            sim.apply_override(profile, key, val)
        sim.log("scenario %s leg %d/%d: %s" % (args.scenario, idx + 1, len(legs), label))
        run_dir = sim.run_profile(
            profile,
            scenario_dir / label,
            smg_bin=bins[slot],
            # Build once, in the first leg; later legs reuse the binaries.
            skip_build=args.skip_build or idx > 0,
        )
        with open(Path(run_dir) / "report.json") as f:
            leg_results.append((label, extract_rows(json.load(f))))
        time.sleep(2)

    write_compare_md(args.scenario, leg_results, scenario_dir / "compare.md")
    sim.log("compare: %s" % (scenario_dir / "compare.md"))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="list scenario names")

    cmp_p = sub.add_parser("compare", help="run a scenario's legs and emit compare.md")
    cmp_p.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    cmp_p.add_argument("--profile", required=True, help="base profile JSON")
    cmp_p.add_argument("--skip-build", action="store_true")
    cmp_p.add_argument("--smg-bin", help="gateway binary for non-A/B legs")
    cmp_p.add_argument("--smg-bin-a", help="gateway binary A (policy-ab)")
    cmp_p.add_argument("--smg-bin-b", help="gateway binary B (policy-ab)")
    cmp_p.add_argument("--out-root", help="parent dir for the scenario run dirs")
    cmp_p.add_argument(
        "--override",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="extra dotted profile override applied to every leg (repeatable)",
    )

    args = parser.parse_args()
    if args.cmd == "list":
        for name, legs in sorted(SCENARIOS.items()):
            print("%-30s %s" % (name, " vs ".join(label for label, _, _ in legs)))
    elif args.cmd == "compare":
        cmd_compare(args)


if __name__ == "__main__":
    main()
