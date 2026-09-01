//! Generic prefix-membership index — SPEC STAGE.
//!
//! The contract lives in `SPEC.md`. Per its §12, no core code lands
//! before the verification harness exists: `tests/` holds the
//! model-referee differential harness (a trivially-correct model of
//! the §6/§7 contract, the `kv_index` oracle behind the engine's
//! replicated glue, and the seeded workload generator). The R1 flat
//! core will be implemented against that harness.
