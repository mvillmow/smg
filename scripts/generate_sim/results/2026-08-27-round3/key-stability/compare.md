# generate-sim compare — key-stability

| metric | stable-key | key-per-turn | shared-keys |
|---|---|---|---|
| ok | 6.863e+04 ±5.8e+02 | 6.863e+04 ±5.8e+02 | 6.847e+04 ±5.3e+02 |
| err | 0 ±0 | 0 ±0 | 0 ±0 |
| achieved_rps | 417.6 ±3.5 | 417.5 ±3.5 | 416.5 ±3.8 |
| ttft_ms_p50 | 50.08 ±0.49 | 106.1 ±1.1 | 106.3 ±0.43 |
| ttft_ms_p90 | 189.6 ±0.82 | 211.5 ±0.44 | 211.5 ±1.2 |
| ttft_ms_p99 | 225.3 ±0.27 | 261.4 ±0.76 | 261.2 ±1.7 |
| e2e_ms_p50 | 7799 ±55 | 7841 ±57 | 7762 ±1e+02 |
| e2e_ms_p90 | 1.874e+04 ±1.2e+02 | 1.878e+04 ±1.3e+02 | 1.874e+04 ±63 |
| e2e_ms_p99 | 2.133e+04 ±27 | 2.138e+04 ±30 | 2.136e+04 ±20 |
| AGG cached tokens (sum/sum) | 0.4732 ±0.003 | 0.1901 ±0.00091 | 0.1899 ±0.00034 |
| AGG cached (request mean) | 0.5058 ±0.0013 | 0.2811 ±0.0013 | 0.2808 ±0.0014 |
| turn1 cached tokens (sum/sum) | 0.2022 ±0.00035 | 0.2021 ±0.00039 | 0.2018 ±0.00014 |
| turn1 cached (request mean) | 0.3102 ±0.0012 | 0.3102 ±0.0012 | 0.3096 ±0.0017 |
| turn1 prompt tokens sum | 4.341e+08 ±4.7e+06 | 4.341e+08 ±4.7e+06 | 4.34e+08 ±5.4e+06 |
| turn1 cached tokens sum | 8.775e+07 ±9.6e+05 | 8.775e+07 ±9.3e+05 | 8.756e+07 ±1e+06 |
| followup cached tokens (sum/sum) | 0.9491 ±0.00076 | 0.1689 ±0.0019 | 0.169 ±0.00072 |
| followup cached (request mean) | 0.9339 ±0.00096 | 0.2174 ±0.0024 | 0.2171 ±0.00038 |
| followup prompt tokens sum | 2.472e+08 ±1.8e+06 | 2.469e+08 ±1.7e+06 | 2.455e+08 ±2.2e+06 |
| followup cached tokens sum | 2.346e+08 ±1.8e+06 | 4.17e+07 ±2.2e+05 | 4.149e+07 ±2.9e+05 |
| mean turns/session | 1.499 ±0.0038 | 1.499 ±0.0038 | 1.496 ±0.004 |
| t2 same-worker (loadgen) | 1 ±0 | 0.008102 ±0.0011 | 0.008037 ±0.0015 |
| followup same-worker | 1 ±0 | 0.008102 ±0.0011 | 0.008037 ±0.0015 |
| t1 max worker share | 0.009915 ±0.00032 | 0.01031 ±0.00016 | 0.009547 ±0.00026 |
| t1 entropy (norm) | 0.9994 ±0.00029 | 0.999 ±0.00021 | 0.9995 ±0.00013 |
| turn1 cached/prompt | 0.2023 ±0.00043 | 0.2023 ±0.00066 | 0.2018 ±0.00038 |
| turn1 hit rate | 0.3308 ±0.0024 | 0.3308 ±0.002 | 0.3288 ±0.0022 |
| turn1 CoV (fleet) | 0.07543 ±0.015 | 0.09583 ±0.0079 | 0.06697 ±0.01 |
| turn2 cached/prompt | 0.9494 ±0.00063 | 0.1677 ±0.0013 | 0.168 ±0.00099 |
| turn2 hit rate | 0.9999 ±0.00014 | 0.18 ±0.0034 | 0.1787 ±0.0037 |
| turn2 CoV (fleet) | 0.1003 ±0.016 | 0.1096 ±0.0071 | 0.07873 ±0.008 |
| t2 same-worker rate | 1 ±0 | 0.0079 ±0.0013 | 0.007967 ±0.0015 |
| overall CoV (fleet) | 0.0812 ±0.015 | 0.0903 ±0.005 | 0.0569 ±0.0027 |
| distinct workers | 120 ±0 | 120 ±0 | 120 ±0 |
| hash_hit share | 0 ±0 | 0 ±0 | 0 ±0 |
| sticky occupied_hit share | 0.3144 ±0.0023 | 0 ±0 | 0.3038 ±0.0025 |
| sticky cap_respill count | 0 ±0 | 0 ±0 | 4.523e+04 ±2.7e+02 |
| body path streamed share | 1 ±0 | 1 ±0 | 1 ±0 |
| offered session rps | 305 ±0 | 305 ±0 | 305 ±0 |
| drain requests (excluded) | 5982 ±1.1e+02 | 6002 ±1.2e+02 | 6002 ±1e+02 |
| rss peak MiB (max smg) | 138 ±1.5 | 135.3 ±2.8 | 137.3 ±4.9 |
| cpu mean % (max smg) | 5.2 ±0.43 | 4.933 ±0.29 | 8.1 ±0.9 |
| queue depth peak | 0 ±0 | 0 ±0 | 0 ±0 |
| rejected total | 0 ±0 | 0 ±0 | 0 ±0 |

- stable-key: see its run dir for report.md / report.json
- key-per-turn: see its run dir for report.md / report.json
- shared-keys: see its run dir for report.md / report.json
