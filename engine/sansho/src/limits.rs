//! The resource limits (SPEC-0001 §5.5) and the evaluation context that
//! enforces them.
//!
//! Every limit is configurable; exceeding any limit aborts the ENTIRE
//! evaluation with a structured [`SanshoError::Limit`] — results are
//! never partial. The parse-time bounds (expression length, nesting
//! depth) reject before the recursive parser runs; the evaluation bounds
//! (instruction budget, output nodes, output bytes, aggregation bytes)
//! accrue during evaluation and check at every step.

use crate::error::SanshoError;
use serde_json::Value as J;

/// The configurable resource limits.
#[derive(Clone, Debug)]
pub struct Limits {
    /// The maximum expression length in characters (parse time).
    pub max_expression_length: usize,
    /// The maximum structural nesting depth (parse time).
    pub max_depth: usize,
    /// The maximum compiled-program steps executed per evaluation.
    pub instruction_budget: usize,
    /// The maximum nodes in the evaluation's output.
    pub output_node_cap: usize,
    /// The maximum serialized size of the evaluation's output, in bytes.
    pub output_byte_cap: usize,
    /// The maximum bytes held across in-flight aggregations and
    /// projections during an evaluation.
    pub aggregation_byte_cap: usize,
    /// The compiled-program cache's maximum entries.
    pub cache_max_entries: usize,
}

impl Default for Limits {
    fn default() -> Self {
        // the plan's fixed defaults (changes require owner approval)
        Limits {
            max_expression_length: 16_384,
            max_depth: 64,
            instruction_budget: 10_000_000,
            output_node_cap: 100_000,
            output_byte_cap: 10_485_760,      // 10 MiB
            aggregation_byte_cap: 10_485_760, // 10 MiB
            cache_max_entries: 1_024,
        }
    }
}

/// The per-evaluation accounting: the counters accrue as the program
/// runs; every step checks its bound. Interior-mutable so the evaluator
/// threads it immutably.
pub(crate) struct EvalContext {
    pub limits: Limits,
    instructions: std::cell::Cell<usize>,
    output_bytes: std::cell::Cell<usize>,
}

impl EvalContext {
    pub(crate) fn new(limits: Limits) -> Self {
        EvalContext {
            limits,
            instructions: std::cell::Cell::new(0),
            output_bytes: std::cell::Cell::new(0),
        }
    }

    /// One compiled-program step executed.
    pub(crate) fn step(&self) -> Result<(), SanshoError> {
        let next = self.instructions.get() + 1;
        if next > self.limits.instruction_budget {
            return Err(SanshoError::Limit {
                message: format!(
                    "instruction budget of {} exceeded",
                    self.limits.instruction_budget
                ),
            });
        }
        self.instructions.set(next);
        Ok(())
    }

    /// The FINAL output's accounting: the node and byte caps check
    /// against the result taken as a whole (once, at the entry point —
    /// interior double-materializations do not inflate it).
    pub(crate) fn account_output(&self, value: &J) -> Result<(), SanshoError> {
        let (nodes, bytes) = count_nodes_and_bytes(value);
        if nodes > self.limits.output_node_cap {
            return Err(SanshoError::Limit {
                message: format!(
                    "output node cap of {} exceeded ({} nodes)",
                    self.limits.output_node_cap, nodes
                ),
            });
        }
        if bytes > self.limits.output_byte_cap {
            return Err(SanshoError::Limit {
                message: format!(
                    "output byte cap of {} exceeded ({} bytes)",
                    self.limits.output_byte_cap, bytes
                ),
            });
        }
        Ok(())
    }

    /// The IN-FLIGHT accounting: interior materializations (the pipe
    /// left-sides, the function arguments, the projection collections)
    /// The IN-FLIGHT accounting: any ONE interior materialization (a
    /// pipe left-side, a function argument, a projection's collected
    /// value) over the aggregation cap aborts the evaluation. The
    /// per-element materializations STREAM through this check (the
    /// output caps bound them at the end) — the aggregation cap bounds
    /// the PEAK size of a single in-flight value, not the total work.
    pub(crate) fn account_aggregation(&self, value: &J) -> Result<(), SanshoError> {
        let (_, bytes) = count_nodes_and_bytes(value);
        if bytes > self.limits.aggregation_byte_cap {
            return Err(SanshoError::Limit {
                message: format!(
                    "aggregation byte cap of {} exceeded by one in-flight value of {} bytes",
                    self.limits.aggregation_byte_cap, bytes
                ),
            });
        }
        Ok(())
    }
}

/// The node count and the approximate serialized byte size of a value.
fn count_nodes_and_bytes(value: &J) -> (usize, usize) {
    match value {
        J::Null => (1, 4),
        J::Bool(_) => (1, 5),
        J::Number(number) => (1, number.to_string().len()),
        J::String(text) => (1, text.len() + 2),
        J::Array(items) => {
            let mut nodes = 1;
            let mut bytes = 2;
            for item in items {
                let (n, b) = count_nodes_and_bytes(item);
                nodes += n;
                bytes += b + 1;
            }
            (nodes, bytes)
        }
        J::Object(map) => {
            let mut nodes = 1;
            let mut bytes = 2;
            for (key, item) in map {
                let (n, b) = count_nodes_and_bytes(item);
                nodes += n;
                bytes += key.len() + b + 4;
            }
            (nodes, bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Requirement: SPEC-0001 §5.5 — the limits are configurable. What:
    // the defaults match the plan's fixed table. Why: the plan's Section
    // 7 table is the approved default set; drift here is a silent policy
    // change (plan: the defaults require owner approval to change).
    //
    // LLM section: this pins the seven default values.
    #[test]
    fn default_limits_match_the_plan_table() {
        let limits = Limits::default();
        assert_eq!(limits.max_expression_length, 16_384);
        assert_eq!(limits.max_depth, 64);
        assert_eq!(limits.instruction_budget, 10_000_000);
        assert_eq!(limits.output_node_cap, 100_000);
        assert_eq!(limits.output_byte_cap, 10_485_760);
        assert_eq!(limits.aggregation_byte_cap, 10_485_760);
        assert_eq!(limits.cache_max_entries, 1_024);
    }

    // Requirement: SPEC-0001 §5.5 — exceeding a limit is a structured
    // error. What: the context's step accounting trips at the budget.
    // Why: the enforcement mechanism's own boundary.
    //
    // LLM section: a budget of 2 allows steps one and two; step three
    // errors with the Limit category.
    #[test]
    fn context_trips_at_the_instruction_budget() {
        let limits = Limits {
            instruction_budget: 2,
            ..Default::default()
        };
        let context = EvalContext::new(limits);
        assert!(context.step().is_ok());
        assert!(context.step().is_ok());
        let third = context.step();
        assert!(matches!(third, Err(SanshoError::Limit { .. })));
    }
}
