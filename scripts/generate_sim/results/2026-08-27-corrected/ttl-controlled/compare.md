# generate-sim compare — ttl-controlled

| metric | ttl-18s | ttl-2s |
|---|---|---|
| ok | 6.722e+04 ±2.5e+02 | 6.722e+04 ±2.5e+02 |
| err | 0 ±0 | 0 ±0 |
| achieved_rps | 209.2 ±12 | 209.2 ±12 |
| ttft_ms_p50 | 8.15 ±0.0077 | 8.146 ±0.019 |
| ttft_ms_p90 | 118.9 ±0.41 | 119 ±0.12 |
| ttft_ms_p99 | 218.9 ±0.64 | 219.1 ±0.45 |
| e2e_ms_p50 | 8324 ±36 | 8325 ±37 |
| e2e_ms_p90 | 1.894e+04 ±28 | 1.894e+04 ±29 |
| e2e_ms_p99 | 2.132e+04 ±5.3 | 2.132e+04 ±5.5 |
| AGG cached tokens (sum/sum) | 0.8383 ±0.00091 | 0.838 ±0.00063 |
| AGG cached (request mean) | 0.8072 ±0.00081 | 0.807 ±0.00069 |
| turn1 cached tokens (sum/sum) | 0.2018 ±0.00032 | 0.2018 ±0.00037 |
| turn1 cached (request mean) | 0.3088 ±0.00039 | 0.3088 ±0.00042 |
| turn1 prompt tokens sum | 1.65e+08 ±7.7e+05 | 1.65e+08 ±7.7e+05 |
| turn1 cached tokens sum | 3.329e+07 ±2e+05 | 3.328e+07 ±2e+05 |
| followup cached tokens (sum/sum) | 0.9732 ±0.00038 | 0.9729 ±6.4e-05 |
| followup cached (request mean) | 0.9678 ±0.00034 | 0.9676 ±0.0001 |
| followup prompt tokens sum | 7.778e+08 ±3.1e+06 | 7.778e+08 ±3.1e+06 |
| followup cached tokens sum | 7.57e+08 ±3.2e+06 | 7.567e+08 ±3.1e+06 |
| mean turns/session | 4.102 ±0.015 | 4.102 ±0.015 |
| t2 same-worker (loadgen) | 1 ±0 | 1 ±0 |
| followup same-worker | 1 ±0 | 1 ±0 |
| t1 max worker share | 0.009194 ±8.9e-05 | 0.009133 ±0.0002 |
| t1 entropy (norm) | 0.9999 ±2.4e-05 | 0.9999 ±6.7e-06 |
| turn1 cached/prompt | 0.2018 ±0.0003 | 0.2018 ±0.00035 |
| turn1 hit rate | 0.3311 ±0.0031 | 0.3311 ±0.003 |
| turn1 CoV (fleet) | 0.03607 ±0.0032 | 0.03537 ±0.00095 |
| turn2 cached/prompt | 0.9674 ±0.00064 | 0.9672 ±0.00072 |
| turn2 hit rate | 0.9983 ±0.00062 | 0.9982 ±0.00073 |
| turn2 CoV (fleet) | 0.03977 ±0.0041 | 0.03743 ±0.0021 |
| t2 same-worker rate | 1 ±0 | 1 ±0 |
| overall CoV (fleet) | 0.0457 ±0.0032 | 0.04067 ±0.003 |
| distinct workers | 120 ±0 | 120 ±0 |
| hash_hit share | 0 ±0 | 0 ±0 |
| sticky occupied_hit share | 0.7562 ±0.0009 | 0.7562 ±0.0009 |
| sticky cap_respill count | 0 ±0 | 0 ±0 |
| rss peak MiB (max smg) | 151.4 ±3.3 | 150.5 ±5.7 |
| cpu mean % (max smg) | 3.133 ±0.17 | 3.033 ±0.24 |
| queue depth peak | 0 ±0 | 0 ±0 |
| rejected total | 0 ±0 | 0 ±0 |

- ttl-18s: see its run dir for report.md / report.json
- ttl-2s: see its run dir for report.md / report.json
