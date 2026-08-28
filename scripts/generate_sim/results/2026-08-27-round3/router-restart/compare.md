# generate-sim compare — router-restart

| metric | restart-mid-run |
|---|---|
| ok | 5.438e+04 ±6.7e+02 |
| err | 3879 ±76 |
| achieved_rps | 286 ±2.6 |
| ttft_ms_p50 | 8.508 ±0.075 |
| ttft_ms_p90 | 160.4 ±2.7 |
| ttft_ms_p99 | 224.8 ±0.73 |
| e2e_ms_p50 | 6796 ±1.1e+02 |
| e2e_ms_p90 | 1.823e+04 ±84 |
| e2e_ms_p99 | 2.125e+04 ±36 |
| AGG cached tokens (sum/sum) | 0.7313 ±0.0063 |
| AGG cached (request mean) | 0.7129 ±0.0049 |
| turn1 cached tokens (sum/sum) | 0.2019 ±0.001 |
| turn1 cached (request mean) | 0.309 ±0.0036 |
| turn1 prompt tokens sum | 1.432e+08 ±1.1e+06 |
| turn1 cached tokens sum | 2.892e+07 ±1.7e+05 |
| followup cached tokens (sum/sum) | 0.9506 ±0.0036 |
| followup cached (request mean) | 0.9453 ±0.0031 |
| followup prompt tokens sum | 3.456e+08 ±5.4e+06 |
| followup cached tokens sum | 3.286e+08 ±6.3e+06 |
| mean turns/session | 3.555 ±0.024 |
| t2 same-worker (loadgen) | 0.9801 ±0.0026 |
| followup same-worker | 0.9748 ±0.0033 |
| t1 max worker share | 0.01151 ±0.00019 |
| t1 entropy (norm) | 0.9987 ±0.00012 |
| turn1 cached/prompt | 0.1878 ±0.0013 |
| turn1 hit rate | 0.308 ±0.0048 |
| turn1 CoV (fleet) | 0.1245 ±0.0057 |
| turn2 cached/prompt | 0.8873 ±0.0094 |
| turn2 hit rate | 0.9162 ±0.0059 |
| turn2 CoV (fleet) | 0.1416 ±0.015 |
| t2 same-worker rate | 0.9834 ±0.0022 |
| overall CoV (fleet) | 0.1473 ±0.029 |
| distinct workers | 120 ±0 |
| hash_hit share | 0 ±0 |
| sticky occupied_hit share | 0.6545 ±0.0049 |
| sticky cap_respill count | 0 ±0 |
| body path streamed share | 0.99 ±0.00094 |
| offered session rps | 110 ±0 |
| drain requests (excluded) | 1.536e+04 ±5.9e+02 |
| rss peak MiB (max smg) | 123.4 ±5.6 |
| cpu mean % (max smg) | 4.533 ±0.38 |
| queue depth peak | 0 ±0 |
| rejected total | 0 ±0 |

- restart-mid-run: see its run dir for report.md / report.json
