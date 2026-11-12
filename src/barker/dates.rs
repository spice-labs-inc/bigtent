//! Dates as RFC 3339 strings, durations as milliseconds.
//!
//! Two decisions shape this module, and both are about what a script can do with
//! the result:
//!
//! - A date is a **string** in canonical RFC 3339 — fixed width, UTC, `Z`. Because
//!   the form is canonical, plain string comparison is chronological comparison,
//!   so a date needs no value type to be sorted or compared.
//! - A duration is an **`isize` of milliseconds**, so arithmetic on times is
//!   ordinary integer arithmetic and `now_ms() - t0` is a duration.
//!
//! Everything reduces to epoch milliseconds for arithmetic and to the canonical
//! string for text, so there is one conversion in each direction.

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use crate::barker::error::BarkerError;
use starlark::environment::GlobalsBuilder;
use starlark::starlark_module;
use starlark::values::Value;

use crate::barker::values::as_string;

/// Parse any RFC 3339 instant and render it canonically: UTC, millisecond
/// precision, `Z`.
fn canonical(text: &str) -> starlark::Result<String> {
    let parsed = DateTime::parse_from_rfc3339(text)
        .map_err(|error| BarkerError::Date {
            function: "dates",
            text: text.to_string(),
            reason: error.to_string(),
        })?;
    Ok(render(&parsed.with_timezone(&Utc)))
}

/// A UTC instant in the canonical form.
fn render(instant: &DateTime<Utc>) -> String {
    instant.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Parse a canonical string back into an instant, for the accessors.
fn parse(function: &'static str, text: &str) -> Result<DateTime<Utc>, BarkerError> {
    Ok(DateTime::parse_from_rfc3339(text)
        .map_err(|error| BarkerError::Date {
            function,
            text: text.to_string(),
            reason: error.to_string(),
        })?
        .with_timezone(&Utc))
}

/// A Starlark number as epoch milliseconds.
fn millis(function: &'static str, value: Value) -> starlark::Result<i64> {
    // a whole, finite millisecond count: a fractional or non-finite shift is
    // refused by name rather than truncated or saturated
    Ok(crate::barker::values::integer_argument(
        function,
        "milliseconds",
        value,
    )?)
}

#[starlark_module]
pub fn dates(builder: &mut GlobalsBuilder) {
    /// The current instant, as epoch milliseconds.
    ///
    /// The primitive for measuring duration: `t0 = now_ms()`, then
    /// `now_ms() - t0` is how long the work took.
    fn now_ms() -> starlark::Result<i64> {
        Ok(Utc::now().timestamp_millis())
    }

    /// The current instant, as canonical RFC 3339.
    fn now() -> starlark::Result<String> {
        Ok(render(&Utc::now()))
    }

    /// `text` as canonical RFC 3339 in UTC, or an error if it does not parse.
    fn parse_date(text: Value) -> starlark::Result<String> {
        canonical(as_string("parse_date", text)?)
    }

    /// Whether `text` parses as a date — for use in a filter, without raising.
    fn is_date(text: Value) -> starlark::Result<bool> {
        let Some(text) = text.unpack_str() else {
            return Ok(false);
        };
        Ok(DateTime::parse_from_rfc3339(text).is_ok())
    }

    /// The epoch milliseconds of a date.
    fn to_millis(text: Value) -> starlark::Result<i64> {
        Ok(parse("to_millis", as_string("to_millis", text)?)?.timestamp_millis())
    }

    /// A date from epoch milliseconds, as canonical RFC 3339.
    fn from_millis(milliseconds: Value) -> starlark::Result<String> {
        let instant = DateTime::<Utc>::from_timestamp_millis(millis("from_millis", milliseconds)?)
            .ok_or_else(|| {
            BarkerError::range("from_millis", format!("{milliseconds} is not a representable instant"))
        })?;
        Ok(render(&instant))
    }

    /// The year.
    fn year(text: Value) -> starlark::Result<i64> {
        Ok(i64::from(parse("year", as_string("year", text)?)?.year()))
    }

    /// The month, 1 to 12.
    fn month(text: Value) -> starlark::Result<i64> {
        Ok(i64::from(parse("month", as_string("month", text)?)?.month()))
    }

    /// The day of the month.
    fn day(text: Value) -> starlark::Result<i64> {
        Ok(i64::from(parse("day", as_string("day", text)?)?.day()))
    }

    /// The hour, 0 to 23.
    fn hour(text: Value) -> starlark::Result<i64> {
        Ok(i64::from(parse("hour", as_string("hour", text)?)?.hour()))
    }

    /// The minute.
    fn minute(text: Value) -> starlark::Result<i64> {
        Ok(i64::from(parse("minute", as_string("minute", text)?)?.minute()))
    }

    /// The second.
    fn second(text: Value) -> starlark::Result<i64> {
        Ok(i64::from(parse("second", as_string("second", text)?)?.second()))
    }

    /// The millisecond within the second.
    fn millisecond(text: Value) -> starlark::Result<i64> {
        Ok(i64::from(
            parse("millisecond", as_string("millisecond", text)?)?.timestamp_subsec_millis(),
        ))
    }

    /// The day of the week, 0 = Monday through 6 = Sunday.
    fn weekday(text: Value) -> starlark::Result<i64> {
        Ok(i64::from(parse("weekday", as_string("weekday", text)?)?.weekday().num_days_from_monday()))
    }

    /// `text` shifted by a number of milliseconds.
    ///
    /// CHECKED arithmetic: the duration constructor and the shift both panic
    /// on overflow, so both are tested and an out-of-range shift is an error.
    fn add_milliseconds(text: Value, milliseconds: Value) -> starlark::Result<String> {
        let duration = Duration::try_milliseconds(millis("add_milliseconds", milliseconds)?)
            .ok_or_else(|| BarkerError::range("add_milliseconds", "the duration is out of range"))?;
        let instant = parse("add_milliseconds", as_string("add_milliseconds", text)?)?
            .checked_add_signed(duration)
            .ok_or_else(|| BarkerError::range("add_milliseconds", "the shifted instant is out of range"))?;
        Ok(render(&instant))
    }

    /// `text` shifted by a number of days.
    ///
    /// CHECKED arithmetic, as `add_milliseconds`.
    fn add_days(text: Value, days: Value) -> starlark::Result<String> {
        let duration = Duration::try_days(millis("add_days", days)?)
            .ok_or_else(|| BarkerError::range("add_days", "the duration is out of range"))?;
        let instant = parse("add_days", as_string("add_days", text)?)?
            .checked_add_signed(duration)
            .ok_or_else(|| BarkerError::range("add_days", "the shifted instant is out of range"))?;
        Ok(render(&instant))
    }

    /// `end - start`, in milliseconds — negative when `end` is earlier.
    fn milliseconds_between(start: Value, end: Value) -> starlark::Result<i64> {
        let start = parse("milliseconds_between", as_string("milliseconds_between", start)?)?.timestamp_millis();
        let end = parse("milliseconds_between", as_string("milliseconds_between", end)?)?.timestamp_millis();
        Ok(end - start)
    }

    /// Whether `text` falls between `start` and `end` inclusive.
    fn in_range(text: Value, start: Value, end: Value) -> starlark::Result<bool> {
        let instant = parse("in_range", as_string("in_range", text)?)?.timestamp_millis();
        let start = parse("in_range", as_string("in_range", start)?)?.timestamp_millis();
        let end = parse("in_range", as_string("in_range", end)?)?.timestamp_millis();
        Ok(instant >= start && instant <= end)
    }

    /// `text` rendered with a `strftime` pattern, for a report that wants
    /// `2026-10-16` rather than the full timestamp.
    fn format_date(text: Value, pattern: Value) -> starlark::Result<String> {
        use chrono::format::{Item, StrftimeItems};
        let instant = parse("format_date", as_string("format_date", text)?)?;
        let items: Vec<Item> = StrftimeItems::new(as_string("format_date", pattern)?).collect();
        // an invalid specifier arrives as `Item::Error`; formatting one
        // PANICS, so the pattern is tested and the script gets an error
        if items.iter().any(|item| matches!(item, Item::Error)) {
            return Err(BarkerError::Value {
                function: "format_date",
                detail: format!("invalid strftime pattern: {:?}", as_string("format_date", pattern)?),
            }
            .into());
        }
        Ok(instant.format_with_items(items.into_iter()).to_string())
    }
}

#[cfg(test)]
mod panic_red_tests {
    use crate::barker::run_with;

    /// The harness: run one script, return the error rows.
    fn run(script: &str) -> Vec<String> {
        let program = crate::barker::parse(script).expect("the script parses");
        let (sender, input) = flume::unbounded();
        sender.send(serde_json::json!(1)).expect("sends");
        drop(sender);
        run_with(program, input, None, None, None)
            .expect("the run starts")
            .filter_map(|row| row.ok())
            .filter_map(|row| row.get("error").and_then(|e| e.as_str().map(str::to_string)))
            .collect()
    }

    const DATE: &str = "\"2026-01-01T00:00:00Z\"";

    /// FINDING 12: `DateTime + Duration` panicked on overflow —
    /// `add_milliseconds(d, 10**16)` and `add_days(d, 100000000)` killed the
    /// thread with a clean 200. An out-of-range shift must be an ERROR ROW.
    #[test]
    fn date_overflow_is_an_error_row() {
        for script in [
            format!("x = dates.add_milliseconds({DATE}, 10000000000000000)\nemit(x)\n"),
            format!("x = dates.add_days({DATE}, 100000000)\nemit(x)\n"),
        ] {
            let errors = run(&script);
            assert!(
                !errors.is_empty(),
                "an out-of-range shift must produce an error row: {script}"
            );
        }
    }

    /// FINDING 13: an invalid strftime pattern (`%Q`) panicked the format
    /// path. chrono's `StrftimeItems` yields `Item::Error` for a bad
    /// specifier — the code must TEST for it and produce an error row.
    #[test]
    fn an_invalid_pattern_is_an_error_row() {
        let errors = run(&format!(
            "x = dates.format_date({DATE}, \"%Q\")\nemit(x)\n"
        ));
        assert!(
            !errors.is_empty(),
            "an invalid strftime pattern must produce an error row"
        );
    }
}
