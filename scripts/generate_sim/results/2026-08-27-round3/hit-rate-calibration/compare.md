# generate-sim compare — hit-rate-calibration

| metric | baseline-1p5turn | multiturn | multiturn-prefix4k |
|---|---|---|---|
| ok | 6.863e+04 ±5.8e+02 | 6.722e+04 ±5.6e+02 | 6.709e+04 ±5.3e+02 |
| err | 0 ±0 | 0 ±0 | 0 ±0 |
| achieved_rps | 417.6 ±3.5 | 344.9 ±2 | 344.7 ±2 |
| ttft_ms_p50 | 50.08 ±0.42 | 8.031 ±0.036 | 7.908 ±0.048 |
| ttft_ms_p90 | 189.6 ±0.81 | 137.6 ±0.73 | 112.4 ±1.1 |
| ttft_ms_p99 | 225.4 ±0.31 | 220.5 ±1.1 | 195.1 ±0.73 |
| e2e_ms_p50 | 7799 ±56 | 7672 ±39 | 7668 ±34 |
| e2e_ms_p90 | 1.874e+04 ±1.2e+02 | 1.867e+04 ±73 | 1.866e+04 ±76 |
| e2e_ms_p99 | 2.133e+04 ±28 | 2.129e+04 ±17 | 2.128e+04 ±16 |
| AGG cached tokens (sum/sum) | 0.4732 ±0.0029 | 0.8008 ±0.0017 | 0.8432 ±0.0012 |
| AGG cached (request mean) | 0.5058 ±0.0014 | 0.7719 ±0.0011 | 0.8334 ±0.00051 |
| turn1 cached tokens (sum/sum) | 0.2021 ±0.00027 | 0.2019 ±0.00073 | 0.3926 ±0.0015 |
| turn1 cached (request mean) | 0.3102 ±0.00099 | 0.3091 ±0.0023 | 0.5105 ±0.0023 |
| turn1 prompt tokens sum | 4.341e+08 ±4.7e+06 | 1.547e+08 ±1.1e+06 | 1.591e+08 ±1.2e+06 |
| turn1 cached tokens sum | 8.774e+07 ±9.8e+05 | 3.123e+07 ±3.3e+05 | 6.245e+07 ±6.7e+05 |
| followup cached tokens (sum/sum) | 0.9491 ±0.00054 | 0.9739 ±0.00011 | 0.9745 ±0.00017 |
| followup cached (request mean) | 0.9339 ±0.00086 | 0.9677 ±0.00022 | 0.9702 ±0.00025 |
| followup prompt tokens sum | 2.472e+08 ±1.8e+06 | 5.35e+08 ±3.6e+06 | 5.462e+08 ±3.1e+06 |
| followup cached tokens sum | 2.346e+08 ±1.8e+06 | 5.21e+08 ±3.5e+06 | 5.322e+08 ±3.1e+06 |
| mean turns/session | 1.499 ±0.0038 | 4.102 ±0.033 | 4.094 ±0.034 |
| t2 same-worker (loadgen) | 1 ±0 | 1 ±0 | 1 ±0 |
| followup same-worker | 1 ±0 | 1 ±0 | 1 ±0 |
| t1 max worker share | 0.009907 ±0.0006 | 0.01229 ±0.00016 | 0.01242 ±0.00074 |
| t1 entropy (norm) | 0.9993 ±0.00036 | 0.9973 ±0.00035 | 0.9971 ±0.00021 |
| turn1 cached/prompt | 0.2023 ±0.00043 | 0.2018 ±0.00066 | 0.3925 ±0.0018 |
| turn1 hit rate | 0.3309 ±0.0021 | 0.3311 ±0.0068 | 0.6967 ±0.0036 |
| turn1 CoV (fleet) | 0.0792 ±0.018 | 0.183 ±0.015 | 0.1885 ±0.0061 |
| turn2 cached/prompt | 0.9494 ±0.00038 | 0.969 ±0 | 0.9698 ±0.00014 |
| turn2 hit rate | 1 ±0.00014 | 1 ±0.00014 | 1 ±0 |
| turn2 CoV (fleet) | 0.1093 ±0.03 | 0.192 ±0.018 | 0.1963 ±0.0043 |
| t2 same-worker rate | 1 ±0 | 1 ±0 | 1 ±0 |
| overall CoV (fleet) | 0.08687 ±0.021 | 0.1975 ±0.017 | 0.2033 ±0.0066 |
| distinct workers | 120 ±0 | 120 ±0 | 120 ±0 |
| hash_hit share | 0 ±0 | 0 ±0 | 0 ±0 |
| sticky occupied_hit share | 0.3144 ±0.0024 | 0.7052 ±0.0031 | 0.705 ±0.0031 |
| sticky cap_respill count | 0 ±0 | 0 ±0 | 0 ±0 |
| body path streamed share | 1 ±0 | 1 ±0 | 1 ±0 |
| offered session rps | 305 ±0 | 110 ±0 | 110 ±0 |
| drain requests (excluded) | 5983 ±1.1e+02 | 1.549e+04 ±6e+02 | 1.539e+04 ±5.7e+02 |
| rss peak MiB (max smg) | 135.4 ±15 | 135 ±4 | 135.9 ±14 |
| cpu mean % (max smg) | 5.033 ±0.29 | 4.567 ±0.38 | 4.533 ±0.14 |
| queue depth peak | 0 ±0 | 0 ±0 | 0 ±0 |
| rejected total | 0 ±0 | 0 ±0 | 0 ±0 |

- baseline-1p5turn: see its run dir for report.md / report.json
- multiturn: see its run dir for report.md / report.json
- multiturn-prefix4k: see its run dir for report.md / report.json
