# generate-sim compare — kv-events

| metric | event-affine | event-sprayed | sticky-control |
|---|---|---|---|
| ok | 6.863e+04 ±5.8e+02 | 6.848e+04 ±5.4e+02 | 6.863e+04 ±5.8e+02 |
| err | 0 ±0 | 0 ±0 | 0 ±0 |
| achieved_rps | 417.6 ±3.5 | 417 ±2.9 | 416 ±2.9 |
| ttft_ms_p50 | n/a | n/a | n/a |
| ttft_ms_p90 | n/a | n/a | n/a |
| ttft_ms_p99 | n/a | n/a | n/a |
| e2e_ms_p50 | 7820 ±66 | 7769 ±92 | 8353 ±1.2e+02 |
| e2e_ms_p90 | 1.877e+04 ±1.1e+02 | 1.872e+04 ±1e+02 | 1.931e+04 ±97 |
| e2e_ms_p99 | 2.136e+04 ±27 | 2.134e+04 ±52 | 2.265e+04 ±2.5e+02 |
| AGG cached tokens (sum/sum) | 0.4623 ±0.0026 | 0.4653 ±0.0063 | 0.4324 ±0.012 |
| AGG cached (request mean) | 0.5024 ±0.00059 | 0.5056 ±0.0055 | 0.4801 ±0.0076 |
| turn1 cached tokens (sum/sum) | 0.2032 ±0.00029 | 0.2032 ±0.00027 | 0.203 ±0.00026 |
| turn1 cached (request mean) | 0.3209 ±0.0015 | 0.321 ±0.0014 | 0.3206 ±0.0012 |
| turn1 prompt tokens sum | 4.331e+08 ±4.7e+06 | 4.331e+08 ±4.7e+06 | 4.319e+08 ±4.2e+06 |
| turn1 cached tokens sum | 8.802e+07 ±9.8e+05 | 8.803e+07 ±9.6e+05 | 8.769e+07 ±8.8e+05 |
| followup cached tokens (sum/sum) | 0.917 ±0.0052 | 0.9278 ±0.011 | 0.8365 ±0.032 |
| followup cached (request mean) | 0.8994 ±0.0038 | 0.9116 ±0.012 | 0.8305 ±0.027 |
| followup prompt tokens sum | 2.467e+08 ±1.8e+06 | 2.454e+08 ±1.9e+06 | 2.451e+08 ±2e+06 |
| followup cached tokens sum | 2.263e+08 ±2.3e+06 | 2.277e+08 ±3.9e+06 | 2.051e+08 ±8e+06 |
| mean turns/session | 1.499 ±0.0038 | 1.496 ±0.0036 | 1.499 ±0.0038 |
| t2 same-worker (loadgen) | 0.941 ±0.0053 | 0.9581 ±0.018 | 1 ±0 |
| followup same-worker | 0.941 ±0.0053 | 0.9581 ±0.018 | 1 ±0 |
| t1 max worker share | 0.0149 ±0.0021 | 0.01588 ±0.0016 | 0.01828 ±0.0005 |
| t1 entropy (norm) | 0.9929 ±0.004 | 0.986 ±0.0045 | 0.994 ±0.02 |
| turn1 cached/prompt | 0.2033 ±0.00057 | 0.2033 ±0.00057 | 0.2031 ±0.00043 |
| turn1 hit rate | 0.3319 ±0.0026 | 0.3319 ±0.0026 | 0.3315 ±0.0026 |
| turn1 CoV (fleet) | 0.2552 ±0.071 | 0.3628 ±0.047 | 0.9572 ±0.023 |
| turn2 cached/prompt | 0.9218 ±0.0043 | 0.931 ±0.0094 | 0.8452 ±0.023 |
| turn2 hit rate | 0.9676 ±0.0055 | 0.9794 ±0.011 | 0.8919 ±0.021 |
| turn2 CoV (fleet) | 0.2418 ±0.045 | 0.3435 ±0.065 | 0.959 ±0.025 |
| t2 same-worker rate | 0.9473 ±0.0039 | 0.9618 ±0.016 | 1 ±0 |
| overall CoV (fleet) | 0.2475 ±0.065 | 0.3553 ±0.053 | 0.9575 ±0.024 |
| distinct workers | 120 ±0 | 120 ±0 | 64.67 ±5.2 |
| hash_hit share | 0 ±0 | 0 ±0 | 0 ±0 |
| sticky occupied_hit share | n/a | n/a | 0.3139 ±0.0028 |
| sticky cap_respill count | n/a | n/a | 0 ±0 |
| body path streamed share | 0 ±0 | 0 ±0 | 0 ±0 |
| offered session rps | 305 ±0 | 305 ±0 | 305 ±0 |
| drain requests (excluded) | 5982 ±1.1e+02 | 5928 ±1.6e+02 | 6225 ±3.2e+02 |
| rss peak MiB (max smg) | 673.7 ±3.4 | 666.2 ±34 | 645.6 ±41 |
| cpu mean % (max smg) | 20.97 ±1.3 | 20.8 ±0.25 | 21.13 ±1.4 |
| queue depth peak | 0 ±0 | 0 ±0 | 0 ±0 |
| rejected total | 0 ±0 | 0 ±0 | 0 ±0 |

- event-affine: see its run dir for report.md / report.json
- event-sprayed: see its run dir for report.md / report.json
- sticky-control: see its run dir for report.md / report.json
