# generate-sim compare — assignment-mode-ab

| metric | delegate | min-group |
|---|---|---|
| ok | 6.863e+04 ±2.6e+02 | 6.863e+04 ±2.6e+02 |
| err | 0 ±0 | 0 ±0 |
| achieved_rps | 333.1 ±11 | 333.1 ±11 |
| ttft_ms_p50 | 45.9 ±0.3 | 45.96 ±0.3 |
| ttft_ms_p90 | 188.5 ±0.41 | 188.6 ±0.32 |
| ttft_ms_p99 | 225.2 ±0.096 | 225.3 ±0.098 |
| e2e_ms_p50 | 8334 ±30 | 8334 ±30 |
| e2e_ms_p90 | 1.897e+04 ±65 | 1.897e+04 ±65 |
| e2e_ms_p99 | 2.136e+04 ±9.1 | 2.136e+04 ±9 |
| AGG cached tokens (sum/sum) | 0.49 ±0.00099 | 0.49 ±0.00094 |
| AGG cached (request mean) | 0.5181 ±0.00077 | 0.5181 ±0.00069 |
| turn1 cached tokens (sum/sum) | 0.2023 ±0.00024 | 0.2023 ±0.00022 |
| turn1 cached (request mean) | 0.3104 ±0.00075 | 0.3104 ±0.00062 |
| turn1 prompt tokens sum | 4.62e+08 ±2.4e+06 | 4.62e+08 ±2.4e+06 |
| turn1 cached tokens sum | 9.344e+07 ±4.2e+05 | 9.344e+07 ±4.5e+05 |
| followup cached tokens (sum/sum) | 0.9493 ±0.00012 | 0.9493 ±8.5e-05 |
| followup cached (request mean) | 0.9344 ±0.00016 | 0.9344 ±0.00016 |
| followup prompt tokens sum | 2.893e+08 ±8.7e+05 | 2.893e+08 ±8.7e+05 |
| followup cached tokens sum | 2.746e+08 ±8.4e+05 | 2.746e+08 ±8.2e+05 |
| mean turns/session | 1.499 ±0.0017 | 1.499 ±0.0017 |
| t2 same-worker (loadgen) | 1 ±0 | 1 ±0 |
| followup same-worker | 1 ±0 | 1 ±0 |
| t1 max worker share | 0.008788 ±9.5e-05 | 0.009057 ±0.00011 |
| t1 entropy (norm) | 1 ±8.1e-06 | 0.9999 ±1.3e-05 |
| turn1 cached/prompt | 0.2023 ±0.00024 | 0.2023 ±0.00024 |
| turn1 hit rate | 0.3309 ±0.00088 | 0.3308 ±0.00096 |
| turn1 CoV (fleet) | 0.02057 ±0.0019 | 0.03283 ±0.0018 |
| turn2 cached/prompt | 0.9493 ±0.00011 | 0.9493 ±0.00011 |
| turn2 hit rate | 0.9998 ±0.00011 | 0.9998 ±0.00013 |
| turn2 CoV (fleet) | 0.04663 ±0.0027 | 0.0509 ±0.0024 |
| t2 same-worker rate | 1 ±0 | 1 ±0 |
| overall CoV (fleet) | 0.0218 ±0.00063 | 0.03137 ±0.0023 |
| distinct workers | 120 ±0 | 120 ±0 |
| hash_hit share | 0 ±0 | n/a |
| sticky occupied_hit share | 0.3329 ±0.00079 | 0.3329 ±0.00079 |
| sticky cap_respill count | 0 ±0 | 0 ±0 |
| rss peak MiB (max smg) | 141.8 ±7.4 | 142.3 ±5.3 |
| cpu mean % (max smg) | 4.133 ±0.13 | 4.133 ±0.13 |
| queue depth peak | 0 ±0 | 0 ±0 |
| rejected total | 0 ±0 | 0 ±0 |

- delegate: see its run dir for report.md / report.json
- min-group: see its run dir for report.md / report.json
