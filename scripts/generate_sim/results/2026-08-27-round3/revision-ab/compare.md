# generate-sim compare — revision-ab

| metric | deployed-rev | latest-main | latest-main-min-group |
|---|---|---|---|
| ok | 6.863e+04 ±5.8e+02 | 6.863e+04 ±5.8e+02 | 6.863e+04 ±5.8e+02 |
| err | 0 ±0 | 0 ±0 | 0 ±0 |
| achieved_rps | 417.6 ±3.5 | 417.6 ±3.5 | 417.6 ±3.5 |
| ttft_ms_p50 | 50.08 ±0.53 | 50.09 ±0.42 | 50.12 ±0.44 |
| ttft_ms_p90 | 189.6 ±0.99 | 189.7 ±0.88 | 189.7 ±0.89 |
| ttft_ms_p99 | 225.3 ±0.18 | 225.4 ±0.32 | 225.4 ±0.16 |
| e2e_ms_p50 | 7798 ±54 | 7799 ±55 | 7799 ±55 |
| e2e_ms_p90 | 1.874e+04 ±1.2e+02 | 1.874e+04 ±1.2e+02 | 1.874e+04 ±1.2e+02 |
| e2e_ms_p99 | 2.133e+04 ±27 | 2.133e+04 ±27 | 2.133e+04 ±26 |
| AGG cached tokens (sum/sum) | 0.4732 ±0.003 | 0.4732 ±0.0029 | 0.4732 ±0.0029 |
| AGG cached (request mean) | 0.5058 ±0.0014 | 0.5058 ±0.0013 | 0.5058 ±0.0012 |
| turn1 cached tokens (sum/sum) | 0.2021 ±0.00028 | 0.2021 ±0.00027 | 0.2021 ±0.00031 |
| turn1 cached (request mean) | 0.3102 ±0.00092 | 0.3102 ±0.00098 | 0.3102 ±0.0012 |
| turn1 prompt tokens sum | 4.341e+08 ±4.7e+06 | 4.341e+08 ±4.7e+06 | 4.341e+08 ±4.7e+06 |
| turn1 cached tokens sum | 8.774e+07 ±9.2e+05 | 8.773e+07 ±9.7e+05 | 8.775e+07 ±9.9e+05 |
| followup cached tokens (sum/sum) | 0.9492 ±0.00059 | 0.9491 ±0.00061 | 0.9492 ±0.0005 |
| followup cached (request mean) | 0.934 ±0.00084 | 0.9339 ±0.00087 | 0.934 ±0.00081 |
| followup prompt tokens sum | 2.472e+08 ±1.8e+06 | 2.472e+08 ±1.8e+06 | 2.472e+08 ±1.8e+06 |
| followup cached tokens sum | 2.346e+08 ±1.8e+06 | 2.346e+08 ±1.8e+06 | 2.346e+08 ±1.8e+06 |
| mean turns/session | 1.499 ±0.0038 | 1.499 ±0.0038 | 1.499 ±0.0038 |
| t2 same-worker (loadgen) | 1 ±0 | 1 ±0 | 1 ±0 |
| followup same-worker | 1 ±0 | 1 ±0 | 1 ±0 |
| t1 max worker share | 0.008861 ±0.00033 | 0.009915 ±0.0001 | 0.00907 ±0.00017 |
| t1 entropy (norm) | 0.9999 ±1.6e-05 | 0.9994 ±0.00029 | 0.9999 ±6.5e-06 |
| turn1 cached/prompt | 0.2023 ±0.00052 | 0.2022 ±0.00057 | 0.2023 ±0.00043 |
| turn1 hit rate | 0.3308 ±0.0021 | 0.3309 ±0.002 | 0.3308 ±0.0022 |
| turn1 CoV (fleet) | 0.02223 ±0.0049 | 0.0761 ±0.016 | 0.0329 ±0.002 |
| turn2 cached/prompt | 0.9495 ±0.0005 | 0.9494 ±0.00057 | 0.9495 ±0.00038 |
| turn2 hit rate | 1 ±0 | 0.9999 ±0.00025 | 1 ±0 |
| turn2 CoV (fleet) | 0.04583 ±0.0028 | 0.1041 ±0.025 | 0.0453 ±0.004 |
| t2 same-worker rate | 1 ±0 | 1 ±0 | 1 ±0 |
| overall CoV (fleet) | 0.02173 ±0.00063 | 0.08287 ±0.021 | 0.0291 ±0.0031 |
| distinct workers | 120 ±0 | 120 ±0 | 120 ±0 |
| hash_hit share | 0 ±0 | 0 ±0 | n/a |
| sticky occupied_hit share | 0.3144 ±0.0023 | 0.3144 ±0.0024 | 0.3144 ±0.0025 |
| sticky cap_respill count | 0 ±0 | 0 ±0 | 0 ±0 |
| body path streamed share | 1 ±0 | 1 ±0 | 1 ±0 |
| offered session rps | 305 ±0 | 305 ±0 | 305 ±0 |
| drain requests (excluded) | 5983 ±1.1e+02 | 5983 ±1.1e+02 | 5982 ±1.1e+02 |
| rss peak MiB (max smg) | 135 ±2.9 | 136.5 ±4.2 | 129.9 ±15 |
| cpu mean % (max smg) | 5.067 ±0.63 | 5.167 ±0.14 | 5.233 ±0.14 |
| queue depth peak | 0 ±0 | 0 ±0 | 0 ±0 |
| rejected total | 0 ±0 | 0 ±0 | 0 ±0 |

- deployed-rev: see its run dir for report.md / report.json
- latest-main: see its run dir for report.md / report.json
- latest-main-min-group: see its run dir for report.md / report.json
