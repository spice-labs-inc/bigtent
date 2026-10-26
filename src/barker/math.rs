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

use crate::barker::values::{as_number, as_numbers};
use starlark::environment::GlobalsBuilder;
use starlark::starlark_module;
use starlark::values::Value;

/// A Starlark number as an `f64`, or an error naming what arrived instead.
///
/// Every function takes `Value` rather than a numeric type because
/// `#[starlark_module]` does not unpack floats, and the enum that accepts both
/// integers and floats is crate-private.
fn number(value: Value) -> anyhow::Result<f64> {
    as_number(value)
        .ok_or_else(|| anyhow::anyhow!("a number is required, found `{}`", value.get_type()))
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
    fn ceil(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.ceil())
    }

    /// The largest integer less than or equal to `x`, as a float.
    fn floor(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.floor())
    }

    /// The nearest integer, rounding half away from zero, as a float.
    fn round(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.round())
    }

    /// The integer part of `x`, towards zero.
    ///
    /// Distinct from `floor` for negative numbers: `trunc(-1.5)` is -1 and
    /// `floor(-1.5)` is -2.
    fn trunc(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.trunc())
    }

    /// `x` raised to the power `y`.
    fn pow(x: Value, y: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.powf(number(y)?))
    }

    /// The square root of `x`.
    fn sqrt(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.sqrt())
    }

    /// `e` raised to the power `x`.
    fn exp(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.exp())
    }

    /// The logarithm of `x` in `base`, or the natural logarithm when no base is
    /// given.
    fn log(x: Value, base: Option<Value>) -> anyhow::Result<f64> {
        let x = number(x)?;
        Ok(match base {
            Some(base) => x.log(number(base)?),
            None => x.ln(),
        })
    }

    /// The absolute value of `x`, as a float.
    fn fabs(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.abs())
    }

    /// The floating-point remainder of `x / y`, with the sign of `x`.
    fn r#mod(x: Value, y: Value) -> anyhow::Result<f64> {
        Ok(number(x)? % number(y)?)
    }

    /// A value with the magnitude of `x` and the sign of `y`.
    fn copysign(x: Value, y: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.copysign(number(y)?))
    }

    /// The arc cosine of `x`, in radians.
    fn acos(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.acos())
    }

    /// The arc sine of `x`, in radians.
    fn asin(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.asin())
    }

    /// The arc tangent of `x`, in radians.
    fn atan(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.atan())
    }

    /// `atan(y / x)`, in radians, using the signs of both to pick the quadrant.
    fn atan2(y: Value, x: Value) -> anyhow::Result<f64> {
        Ok(number(y)?.atan2(number(x)?))
    }

    /// The cosine of `x` radians.
    fn cos(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.cos())
    }

    /// The sine of `x` radians.
    fn sin(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.sin())
    }

    /// The tangent of `x` radians.
    fn tan(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.tan())
    }

    /// `sqrt(x*x + y*y)`, without the intermediate overflow.
    fn hypot(x: Value, y: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.hypot(number(y)?))
    }

    /// The inverse hyperbolic cosine of `x`.
    fn acosh(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.acosh())
    }

    /// The inverse hyperbolic sine of `x`.
    fn asinh(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.asinh())
    }

    /// The inverse hyperbolic tangent of `x`.
    fn atanh(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.atanh())
    }

    /// The hyperbolic cosine of `x`.
    fn cosh(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.cosh())
    }

    /// The hyperbolic sine of `x`.
    fn sinh(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.sinh())
    }

    /// The hyperbolic tangent of `x`.
    fn tanh(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.tanh())
    }

    /// The angle `x` in degrees, converted to radians.
    fn radians(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.to_radians())
    }

    /// The angle `x` in radians, converted to degrees.
    fn degrees(x: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.to_degrees())
    }
}

/// `stats.*`: summarising a stream, and keeping floats representable.
#[starlark_module]
pub fn stats(builder: &mut GlobalsBuilder) {
    /// The arithmetic mean.
    fn mean(values: Value) -> anyhow::Result<f64> {
        let values = as_numbers(values)?;
        if values.is_empty() {
            anyhow::bail!("mean of no values");
        }
        Ok(values.iter().sum::<f64>() / values.len() as f64)
    }

    /// The median: the middle value, or the mean of the two middle values.
    fn median(values: Value) -> anyhow::Result<f64> {
        let mut sorted = as_numbers(values)?;
        if sorted.is_empty() {
            anyhow::bail!("median of no values");
        }
        sorted.sort_by(|a, b| a.partial_cmp(b).expect("no NaN in a summary"));
        let middle = sorted.len() / 2;
        Ok(if sorted.len() % 2 == 0 {
            (sorted[middle - 1] + sorted[middle]) / 2.0
        } else {
            sorted[middle]
        })
    }

    /// The POPULATION variance: the average of the squared deviations.
    fn variance(values: Value) -> anyhow::Result<f64> {
        Ok(population_variance(&as_numbers(values)?))
    }

    /// The SAMPLE variance — the estimator, dividing by n-1.
    fn sample_variance(values: Value) -> anyhow::Result<f64> {
        Ok(sample_variance(&as_numbers(values)?))
    }

    /// The POPULATION standard deviation.
    fn stdev(values: Value) -> anyhow::Result<f64> {
        Ok(population_variance(&as_numbers(values)?).sqrt())
    }

    /// The SAMPLE standard deviation, dividing by n-1.
    fn sample_stdev(values: Value) -> anyhow::Result<f64> {
        Ok(sample_variance(&as_numbers(values)?).sqrt())
    }

    /// The value at percentile `p` (0..=100), by linear interpolation between
    /// the two nearest ranks.
    fn percentile(values: Value, p: Value) -> anyhow::Result<f64> {
        let mut sorted = as_numbers(values)?;
        let p = number(p)?;
        if sorted.is_empty() {
            anyhow::bail!("percentile of no values");
        }
        if !(0.0..=100.0).contains(&p) {
            anyhow::bail!("a percentile is between 0 and 100, found {p}");
        }
        sorted.sort_by(|a, b| a.partial_cmp(b).expect("no NaN in a summary"));
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
    fn tally(values: Value) -> anyhow::Result<serde_json::Value> {
        use starlark::values::list::ListRef;
        let Some(list) = ListRef::from_value(values) else {
            anyhow::bail!("a list is required, found `{}`", values.get_type());
        };
        let mut counts = serde_json::Map::new();
        let rendered: Vec<String> = list.iter().map(|element| element.to_json().unwrap_or_default()).collect();
        for value in rendered {
            let key = match serde_json::from_str::<serde_json::Value>(&value) {
                Ok(serde_json::Value::String(text)) => text,
                _ => value,
            };
            let entry = counts.entry(key).or_insert_with(|| 0.into());
            let n = entry.as_i64().expect("a count is an integer");
            *entry = (n + 1).into();
        }
        Ok(serde_json::Value::Object(counts))
    }

    /// `x` confined to `low..=high`.
    fn clamp(x: Value, low: Value, high: Value) -> anyhow::Result<f64> {
        Ok(number(x)?.clamp(number(low)?, number(high)?))
    }

    /// -1, 0 or 1 according to the sign of `x`, as an integer.
    fn sign(x: Value) -> anyhow::Result<i64> {
        let x = number(x)?;
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
    fn round_to(x: Value, digits: i32) -> anyhow::Result<f64> {
        let x = number(x)?;
        let factor = 10f64.powi(digits);
        Ok((x * factor).round() / factor)
    }

    /// Whether `x` is NaN.
    fn is_nan(x: Value) -> anyhow::Result<bool> {
        Ok(number(x)?.is_nan())
    }

    /// Whether `x` is infinite.
    fn is_infinite(x: Value) -> anyhow::Result<bool> {
        Ok(number(x)?.is_infinite())
    }

    /// Whether `x` is representable as JSON — neither NaN nor infinite.
    ///
    /// `serde_json` has no NaN and no infinity, so a value that is one cannot
    /// become a row; a script that may compute such a value should test before
    /// emitting it.
    fn is_finite(x: Value) -> anyhow::Result<bool> {
        Ok(number(x)?.is_finite())
    }

    /// Whether `a` and `b` differ by no more than `tolerance`.
    fn approx_eq(a: Value, b: Value, tolerance: Value) -> anyhow::Result<bool> {
        Ok((number(a)? - number(b)?).abs() <= number(tolerance)?.abs())
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
