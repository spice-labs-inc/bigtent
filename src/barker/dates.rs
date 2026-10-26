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
use starlark::environment::GlobalsBuilder;
use starlark::starlark_module;
use starlark::values::Value;

use crate::barker::values::{as_number, as_string};

/// Parse any RFC 3339 instant and render it canonically: UTC, millisecond
/// precision, `Z`.
fn canonical(text: &str) -> anyhow::Result<String> {
    let parsed = DateTime::parse_from_rfc3339(text)
        .map_err(|e| anyhow::anyhow!("`{text}` is not an RFC 3339 date: {e}"))?;
    Ok(render(&parsed.with_timezone(&Utc)))
}

/// A UTC instant in the canonical form.
fn render(instant: &DateTime<Utc>) -> String {
    instant.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Parse a canonical string back into an instant, for the accessors.
fn parse(text: &str) -> anyhow::Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(text)
        .map_err(|e| anyhow::anyhow!("`{text}` is not an RFC 3339 date: {e}"))?
        .with_timezone(&Utc))
}

/// A Starlark number as epoch milliseconds.
fn millis(value: Value) -> anyhow::Result<i64> {
    let number = as_number(value).ok_or_else(|| anyhow::anyhow!("a number is required"))?;
    Ok(number as i64)
}

#[starlark_module]
pub fn dates(builder: &mut GlobalsBuilder) {
    /// The current instant, as epoch milliseconds.
    ///
    /// The primitive for measuring duration: `t0 = now_ms()`, then
    /// `now_ms() - t0` is how long the work took.
    fn now_ms() -> anyhow::Result<i64> {
        Ok(Utc::now().timestamp_millis())
    }

    /// The current instant, as canonical RFC 3339.
    fn now() -> anyhow::Result<String> {
        Ok(render(&Utc::now()))
    }

    /// `text` as canonical RFC 3339 in UTC, or an error if it does not parse.
    fn parse_date(text: Value) -> anyhow::Result<String> {
        canonical(as_string(text)?)
    }

    /// Whether `text` parses as a date — for use in a filter, without raising.
    fn is_date(text: Value) -> anyhow::Result<bool> {
        let Some(text) = text.unpack_str() else {
            return Ok(false);
        };
        Ok(DateTime::parse_from_rfc3339(text).is_ok())
    }

    /// The epoch milliseconds of a date.
    fn to_millis(text: Value) -> anyhow::Result<i64> {
        Ok(parse(as_string(text)?)?.timestamp_millis())
    }

    /// A date from epoch milliseconds, as canonical RFC 3339.
    fn from_millis(milliseconds: Value) -> anyhow::Result<String> {
        let instant = DateTime::<Utc>::from_timestamp_millis(millis(milliseconds)?)
            .ok_or_else(|| anyhow::anyhow!("{milliseconds} is not a representable instant"))?;
        Ok(render(&instant))
    }

    /// The year.
    fn year(text: Value) -> anyhow::Result<i64> {
        Ok(i64::from(parse(as_string(text)?)?.year()))
    }

    /// The month, 1 to 12.
    fn month(text: Value) -> anyhow::Result<i64> {
        Ok(i64::from(parse(as_string(text)?)?.month()))
    }

    /// The day of the month.
    fn day(text: Value) -> anyhow::Result<i64> {
        Ok(i64::from(parse(as_string(text)?)?.day()))
    }

    /// The hour, 0 to 23.
    fn hour(text: Value) -> anyhow::Result<i64> {
        Ok(i64::from(parse(as_string(text)?)?.hour()))
    }

    /// The minute.
    fn minute(text: Value) -> anyhow::Result<i64> {
        Ok(i64::from(parse(as_string(text)?)?.minute()))
    }

    /// The second.
    fn second(text: Value) -> anyhow::Result<i64> {
        Ok(i64::from(parse(as_string(text)?)?.second()))
    }

    /// The millisecond within the second.
    fn millisecond(text: Value) -> anyhow::Result<i64> {
        Ok(i64::from(
            parse(as_string(text)?)?.timestamp_subsec_millis(),
        ))
    }

    /// The day of the week, 0 = Monday through 6 = Sunday.
    fn weekday(text: Value) -> anyhow::Result<i64> {
        Ok(i64::from(parse(as_string(text)?)?.weekday().num_days_from_monday()))
    }

    /// `text` shifted by a number of milliseconds.
    fn add_milliseconds(text: Value, milliseconds: Value) -> anyhow::Result<String> {
        let instant = parse(as_string(text)?)? + Duration::milliseconds(millis(milliseconds)?);
        Ok(render(&instant))
    }

    /// `text` shifted by a number of days.
    fn add_days(text: Value, days: Value) -> anyhow::Result<String> {
        let instant = parse(as_string(text)?)? + Duration::days(millis(days)?);
        Ok(render(&instant))
    }

    /// `end - start`, in milliseconds — negative when `end` is earlier.
    fn milliseconds_between(start: Value, end: Value) -> anyhow::Result<i64> {
        let start = parse(as_string(start)?)?.timestamp_millis();
        let end = parse(as_string(end)?)?.timestamp_millis();
        Ok(end - start)
    }

    /// Whether `text` falls between `start` and `end` inclusive.
    fn in_range(text: Value, start: Value, end: Value) -> anyhow::Result<bool> {
        let instant = parse(as_string(text)?)?.timestamp_millis();
        let start = parse(as_string(start)?)?.timestamp_millis();
        let end = parse(as_string(end)?)?.timestamp_millis();
        Ok(instant >= start && instant <= end)
    }

    /// `text` rendered with a `strftime` pattern, for a report that wants
    /// `2026-10-16` rather than the full timestamp.
    fn format_date(text: Value, pattern: Value) -> anyhow::Result<String> {
        use chrono::format::StrftimeItems;
        let instant = parse(as_string(text)?)?;
        let items = StrftimeItems::new(as_string(pattern)?);
        Ok(instant.format_with_items(items).to_string())
    }
}
