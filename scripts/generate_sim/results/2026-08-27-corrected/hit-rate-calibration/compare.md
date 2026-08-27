# generate-sim compare — hit-rate-calibration

| metric | baseline-1p5turn | multiturn | multiturn-prefix4k |
|---|---|---|---|
| ok | 6.863e+04 ±2.6e+02 | 6.722e+04 ±2.5e+02 | 6.709e+04 ±2.4e+02 |
| err | 0 ±0 | 0 ±0 | 0 ±0 |
| achieved_rps | 333.1 ±11 | 241.9 ±7.7 | 246.5 ±4.6 |
| ttft_ms_p50 | 45.93 ±0.38 | 8.068 ±0.018 | 7.97 ±0.026 |
| ttft_ms_p90 | 188.6 ±0.46 | 117.5 ±0.23 | 92.55 ±0.12 |
| ttft_ms_p99 | 225.2 ±0.12 | 218.4 ±0.42 | 193 ±0.47 |
| e2e_ms_p50 | 8334 ±30 | 8324 ±36 | 8320 ±35 |
| e2e_ms_p90 | 1.897e+04 ±65 | 1.894e+04 ±29 | 1.893e+04 ±30 |
| e2e_ms_p99 | 2.136e+04 ±9.3 | 2.132e+04 ±4.8 | 2.131e+04 ±6.1 |
| AGG cached tokens (sum/sum) | 0.4899 ±0.00093 | 0.8396 ±0.00064 | 0.8726 ±0.00049 |
| AGG cached (request mean) | 0.5181 ±0.00067 | 0.8084 ±0.00066 | 0.8589 ±0.00038 |
| turn1 cached tokens (sum/sum) | 0.2022 ±0.0002 | 0.2018 ±0.00036 | 0.3925 ±0.00064 |
| turn1 cached (request mean) | 0.3103 ±0.00058 | 0.3089 ±0.0004 | 0.5104 ±0.00091 |
| turn1 prompt tokens sum | 4.62e+08 ±2.4e+06 | 1.65e+08 ±7.7e+05 | 1.696e+08 ±8.1e+05 |
| turn1 cached tokens sum | 9.343e+07 ±4.5e+05 | 3.329e+07 ±2e+05 | 6.657e+07 ±3.9e+05 |
| followup cached tokens (sum/sum) | 0.9493 ±6.8e-05 | 0.9749 ±6.1e-05 | 0.9754 ±6.8e-05 |
| followup cached (request mean) | 0.9345 ±0.00016 | 0.9694 ±0.0001 | 0.9716 ±0.0001 |
| followup prompt tokens sum | 2.893e+08 ±8.7e+05 | 7.778e+08 ±3.1e+06 | 7.922e+08 ±3.7e+06 |
| followup cached tokens sum | 2.746e+08 ±8.1e+05 | 7.583e+08 ±3.1e+06 | 7.728e+08 ±3.6e+06 |
| mean turns/session | 1.499 ±0.0017 | 4.102 ±0.015 | 4.094 ±0.015 |
| t2 same-worker (loadgen) | 1 ±0 | 1 ±0 | 1 ±0 |
| followup same-worker | 1 ±0 | 1 ±0 | 1 ±0 |
| t1 max worker share | 0.008796 ±0.00011 | 0.009173 ±0.00016 | 0.009093 ±0.00011 |
| t1 entropy (norm) | 1 ±2.4e-06 | 0.9999 ±1.1e-05 | 0.9999 ±1.6e-05 |
| turn1 cached/prompt | 0.2023 ±0.00024 | 0.2018 ±0.00036 | 0.3925 ±0.00064 |
| turn1 hit rate | 0.3307 ±0.00097 | 0.3311 ±0.0028 | 0.6966 ±0.0017 |
| turn1 CoV (fleet) | 0.02103 ±0.00051 | 0.03417 ±0.0015 | 0.03347 ±0.0023 |
| turn2 cached/prompt | 0.9493 ±6.5e-05 | 0.969 ±6.5e-05 | 0.9698 ±6.5e-05 |
| turn2 hit rate | 0.9998 ±0.00013 | 1 ±0 | 1 ±0 |
| turn2 CoV (fleet) | 0.04713 ±0.0023 | 0.03793 ±0.0022 | 0.03633 ±0.0029 |
| t2 same-worker rate | 1 ±0 | 1 ±0 | 1 ±0 |
| overall CoV (fleet) | 0.02177 ±0.00064 | 0.04183 ±0.0018 | 0.04223 ±0.0024 |
| distinct workers | 120 ±0 | 120 ±0 | 120 ±0 |
| hash_hit share | 0 ±0 | 0 ±0 | 0 ±0 |
| sticky occupied_hit share | 0.3329 ±0.00079 | 0.7562 ±0.0009 | 0.7557 ±0.00092 |
| sticky cap_respill count | 0 ±0 | 0 ±0 | 0 ±0 |
| rss peak MiB (max smg) | 146.6 ±3.1 | 146.4 ±3.9 | 145.1 ±2.6 |
| cpu mean % (max smg) | 4.167 ±0.13 | 3.3 ±0.11 | 3.333 ±0.065 |
| queue depth peak | 0 ±0 | 0 ±0 | 0 ±0 |
| rejected total | 0 ±0 | 0 ±0 | 0 ±0 |

- baseline-1p5turn: see its run dir for report.md / report.json
- multiturn: see its run dir for report.md / report.json
- multiturn-prefix4k: see its run dir for report.md / report.json
