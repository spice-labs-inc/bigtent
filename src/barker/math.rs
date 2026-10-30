//! Numeric functions: what Bazel's `math` module offers, and the statistics and
//! float handling a summary needs.
//!
//! Two namespaces rather than one, because they answer different questions:
//!
//! - `math.*` is Bazel's `math` module — rounding, powers, roots, trigonometry,
//!   `e` and `pi` — so a script written against Bazel's module runs unchanged.
//! - `stats.*` is the summarising side: mean, median, standard deviation,
//!   percentiles, tallies, and the float guards that keep a NaN out of a row.
//!
//! Every function accepts integers and floats alike, and returns a float unless
//! its result is inherently an integer (`sign`, `tally` counts, `round`).

use crate::barker::error::BarkerError;
use crate::barker::values::{as_numbers, number_argument};
use starlark::environment::GlobalsBuilder;
use starlark::starlark_module;
use starlark::values::Value;

/// A Starlark number as an `f64`, or an error naming what arrived instead.
///
/// Every function takes `Value` rather than a numeric type because
/// `#[starlark_module]` does not unpack floats, and the enum that accepts both
/// integers and floats is crate-private.
/// A number argument, or a TYPE error naming the function it was passed to.
fn number(function: &'static str, value: Value) -> Result<f64, BarkerError> {
    number_argument(function, "x", value)
}

/// `math.*`: the module Bazel's Starlark defines.
#[starlark_module]
pub fn math(builder: &mut GlobalsBuilder) {
    /// The base of natural logarithms.
    #[allow(non_upper_case_globals)]
    const e: f64 = std::f64::consts::E;
    /// The ratio of a circle's circumference to its diameter.
    #[allow(non_upper_case_globals)]
    const pi: f64 = std::f64::consts::PI;

    /// The smallest integer greater than or equal to `x`, as a float.
    fn ceil(x: Value) -> starlark::Result<f64> {
        Ok(number("ceil", x)?.ceil())
    }

    /// The largest integer less than or equal to `x`, as a float.
    fn floor(x: Value) -> starlark::Result<f64> {
        Ok(number("floor", x)?.floor())
    }

    /// The nearest integer, rounding half away from zero, as a float.
    fn round(x: Value) -> starlark::Result<f64> {
        Ok(number("round", x)?.round())
    }

    /// The integer part of `x`, towards zero.
    ///
    /// Distinct from `floor` for negative numbers: `trunc(-1.5)` is -1 and
    /// `floor(-1.5)` is -2.
    fn trunc(x: Value) -> starlark::Result<f64> {
        Ok(number("trunc", x)?.trunc())
    }

    /// `x` raised to the power `y`.
    fn pow(x: Value, y: Value) -> starlark::Result<f64> {
        Ok(number("pow", x)?.powf(number("pow", y)?))
    }

    /// The square root of `x`.
    fn sqrt(x: Value) -> starlark::Result<f64> {
        Ok(number("sqrt", x)?.sqrt())
    }

    /// `e` raised to the power `x`.
    fn exp(x: Value) -> starlark::Result<f64> {
        Ok(number("exp", x)?.exp())
    }

    /// The logarithm of `x` in `base`, or the natural logarithm when no base is
    /// given.
    fn log(x: Value, base: Option<Value>) -> starlark::Result<f64> {
        let x = number("log", x)?;
        Ok(match base {
            Some(base) => x.log(number("log", base)?),
            None => x.ln(),
        })
    }

    /// The absolute value of `x`, as a float.
    fn fabs(x: Value) -> starlark::Result<f64> {
        Ok(number("fabs", x)?.abs())
    }

    /// The floating-point remainder of `x / y`, with the sign of `x`.
    fn r#mod(x: Value, y: Value) -> starlark::Result<f64> {
        Ok(number("fabs", x)? % number("fabs", y)?)
    }

    /// A value with the magnitude of `x` and the sign of `y`.
    fn copysign(x: Value, y: Value) -> starlark::Result<f64> {
        Ok(number("copysign", x)?.copysign(number("copysign", y)?))
    }

    /// The arc cosine of `x`, in radians.
    fn acos(x: Value) -> starlark::Result<f64> {
        Ok(number("acos", x)?.acos())
    }

    /// The arc sine of `x`, in radians.
    fn asin(x: Value) -> starlark::Result<f64> {
        Ok(number("asin", x)?.asin())
    }

    /// The arc tangent of `x`, in radians.
    fn atan(x: Value) -> starlark::Result<f64> {
        Ok(number("atan", x)?.atan())
    }

    /// `atan(y / x)`, in radians, using the signs of both to pick the quadrant.
    fn atan2(y: Value, x: Value) -> starlark::Result<f64> {
        Ok(number("atan2", y)?.atan2(number("atan2", x)?))
    }

    /// The cosine of `x` radians.
    fn cos(x: Value) -> starlark::Result<f64> {
        Ok(number("cos", x)?.cos())
    }

    /// The sine of `x` radians.
    fn sin(x: Value) -> starlark::Result<f64> {
        Ok(number("sin", x)?.sin())
    }

    /// The tangent of `x` radians.
    fn tan(x: Value) -> starlark::Result<f64> {
        Ok(number("tan", x)?.tan())
    }

    /// `sqrt(x*x + y*y)`, without the intermediate overflow.
    fn hypot(x: Value, y: Value) -> starlark::Result<f64> {
        Ok(number("hypot", x)?.hypot(number("hypot", y)?))
    }

    /// The inverse hyperbolic cosine of `x`.
    fn acosh(x: Value) -> starlark::Result<f64> {
        Ok(number("acosh", x)?.acosh())
    }

    /// The inverse hyperbolic sine of `x`.
    fn asinh(x: Value) -> starlark::Result<f64> {
        Ok(number("asinh", x)?.asinh())
    }

    /// The inverse hyperbolic tangent of `x`.
    fn atanh(x: Value) -> starlark::Result<f64> {
        Ok(number("atanh", x)?.atanh())
    }

    /// The hyperbolic cosine of `x`.
    fn cosh(x: Value) -> starlark::Result<f64> {
        Ok(number("cosh", x)?.cosh())
    }

    /// The hyperbolic sine of `x`.
    fn sinh(x: Value) -> starlark::Result<f64> {
        Ok(number("sinh", x)?.sinh())
    }

    /// The hyperbolic tangent of `x`.
    fn tanh(x: Value) -> starlark::Result<f64> {
        Ok(number("tanh", x)?.tanh())
    }

    /// The angle `x` in degrees, converted to radians.
    fn radians(x: Value) -> starlark::Result<f64> {
        Ok(number("radians", x)?.to_radians())
    }

    /// The angle `x` in radians, converted to degrees.
    fn degrees(x: Value) -> starlark::Result<f64> {
        Ok(number("degrees", x)?.to_degrees())
    }
}

/// `stats.*`: summarising a stream, and keeping floats representable.
#[starlark_module]
pub fn stats(builder: &mut GlobalsBuilder) {
    /// The arithmetic mean.
    fn mean(values: Value) -> starlark::Result<f64> {
        let values = as_numbers("mean", values)?;
        if values.is_empty() {
            return Err(BarkerError::value("mean", "no values: a mean needs at least one").into());
        }
        Ok(values.iter().sum::<f64>() / values.len() as f64)
    }

    /// The median: the middle value, or the mean of the two middle values.
    fn median(values: Value) -> starlark::Result<f64> {
        let mut sorted = as_numbers("median", values)?;
        if sorted.is_empty() {
            return Err(BarkerError::value("median", "no values: a median needs at least one").into());
        }
        // NaN has no ordering: a summary over it is undefined, so it is an
        // error named to the script, never a panic
        if let Some(position) = sorted.iter().position(|v| v.is_nan()) {
            return Err(BarkerError::value("median", format!("element {position} is NaN: a median over NaN is undefined")).into());
        }
        // NaN is refused above; Equal keeps the sort panic-free even if that
        // guard ever drifts
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let middle = sorted.len() / 2;
        Ok(if sorted.len() % 2 == 0 {
            (sorted[middle - 1] + sorted[middle]) / 2.0
        } else {
            sorted[middle]
        })
    }

    /// The POPULATION variance: the average of the squared deviations.
    fn variance(values: Value) -> starlark::Result<f64> {
        Ok(population_variance(&as_numbers("variance", values)?))
    }

    /// The SAMPLE variance — the estimator, dividing by n-1.
    fn sample_variance(values: Value) -> starlark::Result<f64> {
        Ok(sample_variance(&as_numbers("sample_variance", values)?))
    }

    /// The POPULATION standard deviation.
    fn stdev(values: Value) -> starlark::Result<f64> {
        Ok(population_variance(&as_numbers("stdev", values)?).sqrt())
    }

    /// The SAMPLE standard deviation, dividing by n-1.
    fn sample_stdev(values: Value) -> starlark::Result<f64> {
        Ok(sample_variance(&as_numbers("sample_stdev", values)?).sqrt())
    }

    /// The value at percentile `p` (0..=100), by linear interpolation between
    /// the two nearest ranks.
    fn percentile(values: Value, p: Value) -> starlark::Result<f64> {
        let mut sorted = as_numbers("percentile", values)?;
        let p = number("percentile", p)?;
        if sorted.is_empty() {
            return Err(BarkerError::value("percentile", "no values: a percentile needs at least one").into());
        }
        if !(0.0..=100.0).contains(&p) {
            return Err(BarkerError::range("percentile", format!("a percentile is between 0 and 100, found {p}")).into());
        }
        if let Some(position) = sorted.iter().position(|v| v.is_nan()) {
            return Err(BarkerError::value("percentile", format!("element {position} is NaN: a percentile over NaN is undefined")).into());
        }
        // NaN is refused above; Equal keeps the sort panic-free even if that
        // guard ever drifts
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if sorted.len() == 1 {
            return Ok(sorted[0]);
        }
        // the rank, in the "exclusive" convention: p% of the way from the first
        // value to the last
        let rank = p / 100.0 * (sorted.len() - 1) as f64;
        let lower = rank.floor() as usize;
        let upper = rank.ceil() as usize;
        let weight = rank - lower as f64;
        Ok(sorted[lower] * (1.0 - weight) + sorted[upper] * weight)
    }

    /// How many times each value appears, as a dict.
    ///
    /// The values are rendered by their Starlark form, so integers and strings
    /// are their own keys.
    fn tally<'v>(
        values: Value<'v>,
        eval: &mut starlark::eval::Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        use starlark::values::list::ListRef;
        let heap = eval.heap();
        let Some(list) = ListRef::from_value(values) else {
            return Err(crate::barker::values::list_argument("tally", "values", values).err().unwrap().into());
        };
        // (the value's identity, the value itself as the key, its count)
        let mut counts: Vec<(crate::barker::values::KeyValue, Value<'v>, i64)> = Vec::new();
        for element in list.iter() {
            // a value with no representation is an ERROR — never a silent
            // empty-string bucket
            let classified = crate::barker::values::key_of(element)?;
            match counts.iter_mut().find(|(k, _, _)| *k == classified) {
                Some((_, _, count)) => *count += 1,
                None => counts.push((classified, element, 1)),
            }
        }
        // a NATIVE Starlark dict: the keys are the values themselves, so a
        // string stays a string and a number a number
        let entries: Vec<(Value<'v>, Value<'v>)> = counts
            .into_iter()
            .map(|(_, key, count)| (key, heap.alloc(count)))
            .collect();
        Ok(heap.alloc(starlark::values::dict::AllocDict(entries)))
    }

    /// `x` confined to `low..=high`.
    fn clamp(x: Value, low: Value, high: Value) -> starlark::Result<f64> {
        let (x, low, high) = (number("clamp", x)?, number("clamp", low)?, number("clamp", high)?);
        // f64::clamp PANICS when low > high: validate instead
        if low > high {
            return Err(BarkerError::value("clamp", format!("low ({low}) exceeds high ({high})")).into());
        }
        Ok(x.clamp(low, high))
    }

    /// -1, 0 or 1 according to the sign of `x`, as an integer.
    fn sign(x: Value) -> starlark::Result<i64> {
        let x = number("sign", x)?;
        Ok(if x > 0.0 {
            1
        } else if x < 0.0 {
            -1
        } else {
            0
        })
    }

    /// `x` rounded to `digits` decimal places, as a float.
    ///
    /// A report wants `2.33`, not `2.3333333333333335`.
    fn round_to(x: Value, digits: i32) -> starlark::Result<f64> {
        let x = number("round_to", x)?;
        let factor = 10f64.powi(digits);
        Ok((x * factor).round() / factor)
    }

    /// Whether `x` is NaN.
    fn is_nan(x: Value) -> starlark::Result<bool> {
        Ok(number("is_nan", x)?.is_nan())
    }

    /// Whether `x` is infinite.
    fn is_infinite(x: Value) -> starlark::Result<bool> {
        Ok(number("is_infinite", x)?.is_infinite())
    }

    /// Whether `x` is representable as JSON — neither NaN nor infinite.
    ///
    /// `serde_json` has no NaN and no infinity, so a value that is one cannot
    /// become a row; a script that may compute such a value should test before
    /// emitting it.
    fn is_finite(x: Value) -> starlark::Result<bool> {
        Ok(number("is_finite", x)?.is_finite())
    }

    /// Whether `a` and `b` differ by no more than `tolerance`.
    fn approx_eq(a: Value, b: Value, tolerance: Value) -> starlark::Result<bool> {
        Ok((number("approx_eq", a)? - number("approx_eq", b)?).abs() <= number("approx_eq", tolerance)?.abs())
    }
}

/// The population variance; `NaN` for no values, so a caller reports it rather
/// than pretending.
fn population_variance(values: &[f64]) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    values.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / values.len() as f64
}

/// The sample variance, dividing by n-1; `NaN` below two values, because one
/// value says nothing about spread.
fn sample_variance(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return f64::NAN;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    values.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / (values.len() - 1) as f64
}

#[cfg(test)]
mod panic_red_tests {
    use crate::barker::run_with;

    /// The harness: run one script, return (the error rows, the value rows).
    fn run(script: &str) -> (Vec<String>, Vec<serde_json::Value>) {
        let program = crate::barker::parse(script).expect("the script parses");
        let (sender, input) = flume::unbounded();
        sender.send(serde_json::json!(1)).expect("sends");
        drop(sender);
        let rows: Vec<serde_json::Value> = run_with(program, input, None, None, None)
            .expect("the run starts")
            .filter_map(|row| row.ok())
            .collect();
        let errors: Vec<String> = rows
            .iter()
            .filter_map(|row| row.get("error").and_then(|e| e.as_str().map(str::to_string)))
            .collect();
        (errors, rows)
    }

    /// FINDING 10: `median`/`percentile` panicked on NaN (`.expect("no NaN in
    /// a summary")` over `partial_cmp`, which is None for NaN) — the thread
    /// died and the client got a clean 200 with no error. A NaN input must be
    /// an ERROR ROW.
    #[test]
    fn median_over_nan_is_an_error_row() {
        let (errors, _) = run("x = stats.median([1.0, float(\"nan\")])\nemit(x)\n");
        assert!(
            !errors.is_empty(),
            "median over NaN must produce an error row, not a dead thread"
        );
        let (errors, _) = run("x = stats.percentile([1.0, float(\"nan\")], 50)\nemit(x)\n");
        assert!(
            !errors.is_empty(),
            "percentile over NaN must produce an error row too"
        );
    }

    /// FINDING 11: `f64::clamp` panics when low > high — `stats.clamp(5, 10, 1)`
    /// killed the thread with a clean 200. A reversed range must be an ERROR ROW.
    #[test]
    fn clamp_with_reversed_bounds_is_an_error_row() {
        let (errors, _) = run("x = stats.clamp(5, 10, 1)\nemit(x)\n");
        assert!(
            !errors.is_empty(),
            "clamp(5, 10, 1) must produce an error row, not a dead thread"
        );
    }
}
