//! The JMESPath parser: the complete grammar, parsed with nom.
//!
//! Every syntactic production of the published grammar parses here,
//! even where evaluation lags (later phases turn more of the corpus
//! green). The nesting-depth limit is enforced during parsing, so
//! hostile nesting is rejected before recursion can run away
//! (SPEC-0001 §5.5).

use crate::ast::*;
use crate::error::SanshoError;
use nom::IResult;
use nom::branch::alt;
use nom::bytes::complete::{take_while, take_while1};
use nom::character::complete::char;
use nom::combinator::{cut, eof, opt, recognize, value};
use nom::multi::separated_list1;
use nom::sequence::{delimited, preceded, terminated, tuple};

/// Parse a complete JMESPath expression.
pub fn parse(input: &str) -> Result<Expr, SanshoError> {
    parse_with_limits(input, &crate::limits::Limits::default())
}

/// The limit-configurable parse: the expression-length and nesting-depth
/// bounds come from the limits (SPEC-0001 §5.5).
pub fn parse_with_limits(input: &str, limits: &crate::limits::Limits) -> Result<Expr, SanshoError> {
    // the length bound first (the cheapest rejection)
    if input.chars().count() > limits.max_expression_length {
        return Err(SanshoError::Limit {
            message: format!(
                "expression length {} exceeds the maximum of {}",
                input.chars().count(),
                limits.max_expression_length
            ),
        });
    }
    // Nesting is bounded BEFORE the recursive parser runs: an iterative
    // scan over the expression's structural characters (skipping string,
    // raw-string, and literal contexts) rejects hostile nesting without
    // recursing at all.
    if let Some(position) = excessive_depth_at(input, limits.max_depth) {
        return Err(SanshoError::Limit {
            message: format!(
                "expression nesting exceeds the maximum depth of {} at byte {position}",
                limits.max_depth
            ),
        });
    }
    match terminated(parse_expr, eof)(input) {
        Ok((_rest, expr)) => Ok(expr),
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => Err(SanshoError::Parse {
            message: "syntax error".to_string(),
            position: input.len() - e.input.len(),
        }),
        Err(nom::Err::Incomplete(_)) => Err(SanshoError::Parse {
            message: "incomplete expression".to_string(),
            position: input.len(),
        }),
    }
}

/// The byte position of the first character where structural nesting
/// exceeds `max_depth`, or `None` if it never does. Iterative; skips
/// quoted strings, raw strings, and backtick literals (which may legally
/// contain bracket characters).
fn excessive_depth_at(input: &str, max_depth: usize) -> Option<usize> {
    #[derive(Clone, Copy)]
    enum Inside {
        None,
        Quoted,
        Raw,
        Literal,
    }
    let mut depth = 0usize;
    let mut inside = Inside::None;
    let mut escape = false;
    for (position, character) in input.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match inside {
            Inside::Quoted => match character {
                '\\' => escape = true,
                '"' => inside = Inside::None,
                _ => {}
            },
            Inside::Raw => {
                if character == '\'' {
                    inside = Inside::None;
                }
            }
            Inside::Literal => match character {
                '\\' => escape = true,
                '`' => inside = Inside::None,
                _ => {}
            },
            Inside::None => match character {
                '"' => inside = Inside::Quoted,
                '\'' => inside = Inside::Raw,
                '`' => inside = Inside::Literal,
                '[' | '{' => {
                    depth += 1;
                    if depth > max_depth {
                        return Some(position);
                    }
                }
                ']' | '}' => depth = depth.saturating_sub(1),
                _ => {}
            },
        }
    }
    None
}

type PResult<'a> = IResult<&'a str, Expr, nom::error::Error<&'a str>>;

fn ws(input: &str) -> &str {
    input.trim_start_matches(|c: char| c.is_whitespace())
}

/// The whitespace combinator, for use inside `preceded`/`terminated`:
/// consumes as much whitespace as there is.
fn wsp(input: &str) -> IResult<&str, &str, nom::error::Error<&str>> {
    take_while(|c: char| c.is_whitespace())(input)
}

// ---------- precedence ladder (loosest first) ----------

fn parse_expr(input: &str) -> PResult<'_> {
    parse_pipe(input)
}

/// `a | b | c` — pipes bind loosest; left-associative.
fn parse_pipe(input: &str) -> PResult<'_> {
    let (mut rest, mut left) = parse_or(input)?;
    loop {
        let trimmed = ws(rest);
        // "||" is logical-or, not a pipe
        if !trimmed.starts_with('|') || trimmed.starts_with("||") {
            return Ok((rest, left));
        }
        let (after, _) = char('|')(trimmed)?;
        let (next_rest, right) = parse_or(after)?;
        left = Expr::Pipe(Box::new(left), Box::new(right));
        rest = next_rest;
    }
}

fn parse_or(input: &str) -> PResult<'_> {
    let (mut rest, mut left) = parse_and(input)?;
    loop {
        let trimmed = ws(rest);
        if !trimmed.starts_with("||") {
            return Ok((rest, left));
        }
        let (after, _) = tag2(trimmed, "||")?;
        let (next_rest, right) = parse_and(after)?;
        left = Expr::Or(Box::new(left), Box::new(right));
        rest = next_rest;
    }
}

fn tag2<'a>(
    input: &'a str,
    pattern: &'a str,
) -> IResult<&'a str, &'a str, nom::error::Error<&'a str>> {
    if input.starts_with(pattern) {
        Ok((&input[pattern.len()..], pattern))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )))
    }
}

fn parse_and(input: &str) -> PResult<'_> {
    let (mut rest, mut left) = parse_compare(input)?;
    loop {
        let trimmed = ws(rest);
        if !trimmed.starts_with("&&") {
            return Ok((rest, left));
        }
        let (after, _) = tag2(trimmed, "&&")?;
        let (next_rest, right) = parse_compare(after)?;
        left = Expr::And(Box::new(left), Box::new(right));
        rest = next_rest;
    }
}

/// Comparators bind tighter than `&&` and do not chain:
/// `a == b`, never `a == b == c`.
fn parse_compare(input: &str) -> PResult<'_> {
    let (rest, left) = parse_not(input)?;
    let trimmed = ws(rest);
    let (after, op) = match parse_cmp_op(trimmed) {
        Ok(v) => v,
        Err(_) => return Ok((rest, left)),
    };
    let (rest2, right) = cut(parse_not)(after)?;
    Ok((rest2, Expr::Compare(op, Box::new(left), Box::new(right))))
}

fn parse_cmp_op(input: &str) -> IResult<&str, CmpOp, nom::error::Error<&str>> {
    // longest-first so "<=" never mis-parses as "<"
    let op = if input.starts_with("==") {
        CmpOp::Eq
    } else if input.starts_with("!=") {
        CmpOp::Ne
    } else if input.starts_with("<=") {
        CmpOp::Le
    } else if input.starts_with(">=") {
        CmpOp::Ge
    } else if input.starts_with('<') {
        CmpOp::Lt
    } else if input.starts_with('>') {
        CmpOp::Gt
    } else {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    };
    Ok((&input[op.as_str().len()..], op))
}

/// `!expr` — negation binds tighter than comparators, looser than the
/// postfix chain: `!a.b` negates `a.b`.
fn parse_not(input: &str) -> PResult<'_> {
    let trimmed = ws(input);
    match trimmed.starts_with('!') {
        true if !trimmed.starts_with("!=") => {
            let (rest, inner) = parse_not(&trimmed[1..])?;
            Ok((rest, Expr::Not(Box::new(inner))))
        }
        _ => parse_chain(trimmed),
    }
}

// ---------- postfix chains ----------

fn parse_postfixes(input: &str) -> Result<(&str, Vec<Postfix>), nom::Err<nom::error::Error<&str>>> {
    let mut rest = ws(input);
    let mut postfixes = Vec::new();
    loop {
        let (next, post) = match parse_one_postfix(rest) {
            Ok(v) => v,
            Err(_) => return Ok((rest, postfixes)),
        };
        postfixes.push(post);
        rest = ws(next);
    }
}

fn parse_one_postfix(input: &str) -> IResult<&str, Postfix, nom::error::Error<&str>> {
    // "." rhs — a field, a wildcard, a multi-select-hash, or a bracket
    // form (the sub-expression right-hand side may be any selector); or
    // a bracket form directly on the chain.
    let trimmed = ws(input);
    if trimmed.starts_with('.') {
        dot_form(trimmed)
    } else {
        parse_bracket(trimmed)
    }
}

fn rebuild_chain(inner: Expr, postfixes: Vec<Postfix>) -> Expr {
    // the grouped expression becomes the head; its own pipe/logical
    // continuations were already absorbed by the inner parse
    match (inner, postfixes) {
        (Expr::Chain(primary, existing), extra) if existing.is_empty() => {
            let mut all = existing;
            all.extend(extra);
            Expr::Chain(primary, all)
        }
        (Expr::Chain(primary, existing), extra) => {
            let mut all = existing;
            all.extend(extra);
            Expr::Chain(primary, all)
        }
        (other, extra) if extra.is_empty() => other,
        (other, extra) => Expr::Pipe(
            Box::new(other),
            Box::new(Expr::Chain(Primary::Current, extra)),
        ),
    }
}

fn dot_form(input: &str) -> IResult<&str, Postfix, nom::error::Error<&str>> {
    let (rest, _) = char('.')(input)?;
    let rest = ws(rest);
    if rest.starts_with('*') {
        let (rest, _) = char('*')(rest)?;
        return Ok((rest, Postfix::Wildcard { bracket: false }));
    }
    if rest.starts_with('{') {
        return parse_multihash_body(rest);
    }
    if rest.starts_with('[') {
        // dot-bracket is a multi-select list ONLY: foo.[bar, baz];
        // foo.[0] is invalid (the slot `0` is not an expression)
        let (rest, _) = char('[')(rest)?;
        let (rest, items) = cut(separated_list1(
            preceded(wsp, char(',')),
            preceded(wsp, parse_expr),
        ))(rest)?;
        let (rest, _) = preceded(wsp, cut(char(']')))(rest)?;
        return Ok((rest, Postfix::MultiList(items)));
    }
    let (rest, name) = cut(parse_identifier)(rest)?;
    let rest = ws(rest);
    if rest.starts_with('(') {
        // a function call as the sub-expression right-hand side:
        // decimals[].to_string(@)
        let (rest, args) = parse_function_args_primary(rest, name.clone())?;
        return Ok((rest, Postfix::Function(name, args)));
    }
    Ok((rest, Postfix::Field(name)))
}

/// Everything that can appear between "[" and "]", as a postfix.
fn parse_bracket(input: &str) -> IResult<&str, Postfix, nom::error::Error<&str>> {
    let (rest, _) = char('[')(input)?;
    // the filter's ? must immediately follow the bracket: foo[ ?x] is a
    // syntax error
    if rest.starts_with('?') {
        let (rest, _) = char('?')(rest)?;
        let (rest, pred) = cut(parse_expr)(rest)?;
        let rest = ws(rest);
        let (rest, _) = cut(char(']'))(rest)?;
        return Ok((rest, Postfix::Filter(Box::new(pred))));
    }
    let rest = ws(rest);
    if rest.starts_with(']') {
        let (rest, _) = char(']')(rest)?;
        return Ok((rest, Postfix::Flatten));
    }
    if rest.starts_with('*') {
        let (rest, _) = char('*')(rest)?;
        let rest = ws(rest);
        // no cut: [*.*] must recover into a multi-select list
        let (rest, _) = char(']')(rest)?;
        return Ok((rest, Postfix::Wildcard { bracket: true }));
    }
    // slice or index: optional integer, then "]" (index) or ":" (slice);
    // a comma after an integer is invalid (bare numbers are not
    // expressions in JMESPath)
    let (after_int, value) = opt(preceded(wsp, parse_int))(rest)?;
    match value {
        Some(int) => {
            let rest = ws(after_int);
            if rest.starts_with(']') {
                let (rest, _) = char(']')(rest)?;
                return Ok((rest, Postfix::Index(int)));
            }
            let (rest, _) = cut(char(':'))(rest)?;
            finish_slice(rest, Some(int))
        }
        None => {
            // a bare postfix bracket is never a multi-select list:
            // foo[abc] and foo[bar==baz] are invalid
            let (rest, _) = char(':')(rest)?;
            finish_slice(rest, None)
        }
    }
}

/// The expression-start form: brackets may also begin a multi-select
/// list ([a, b] at top level or inside another); everything else as in
/// the postfix form.
fn parse_bracket_start(input: &str) -> IResult<&str, Postfix, nom::error::Error<&str>> {
    match parse_bracket(input) {
        Ok(ok) => Ok(ok),
        Err(nom::Err::Error(_)) | Err(nom::Err::Incomplete(_)) => {
            let (rest, _) = char('[')(input)?;
            let (rest, items) = cut(separated_list1(
                preceded(wsp, char(',')),
                preceded(wsp, parse_expr),
            ))(rest)?;
            let (rest, _) = preceded(wsp, cut(char(']')))(rest)?;
            Ok((rest, Postfix::MultiList(items)))
        }
        Err(nom::Err::Failure(e)) => Err(nom::Err::Failure(e)),
    }
}

fn finish_slice(
    input: &str,
    start: Option<i64>,
) -> IResult<&str, Postfix, nom::error::Error<&str>> {
    let (rest, stop) = opt(preceded(wsp, parse_int))(input)?;
    let rest = ws(rest);
    let (rest, step) = if rest.starts_with(':') {
        let (rest, _) = char(':')(rest)?;
        let (rest, step) = opt(preceded(wsp, parse_int))(rest)?;
        (rest, step)
    } else {
        (rest, None)
    };
    let rest = ws(rest);
    let (rest, _) = cut(char(']'))(rest)?;
    Ok((rest, Postfix::Slice(SliceSpec { start, stop, step })))
}

fn parse_int(input: &str) -> IResult<&str, i64, nom::error::Error<&str>> {
    let (rest, text) = take_while1(|c: char| c.is_ascii_digit() || c == '-')(input)?;
    // "-" alone is not a number
    if text == "-" {
        return Err(nom::Err::Failure(nom::error::Error::new(
            rest,
            nom::error::ErrorKind::Digit,
        )));
    }
    let value: i64 = text.parse().map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(rest, nom::error::ErrorKind::Digit))
    })?;
    Ok((rest, value))
}

fn parse_multihash_body(input: &str) -> IResult<&str, Postfix, nom::error::Error<&str>> {
    let (rest, _) = char('{')(input)?;
    let (rest, entries) = cut(separated_list1(preceded(wsp, char(',')), multihash_entry))(rest)?;
    let (rest, _) = preceded(wsp, cut(char('}')))(rest)?;
    Ok((rest, Postfix::MultiHash(entries)))
}

fn multihash_entry(input: &str) -> IResult<&str, (String, Expr), nom::error::Error<&str>> {
    let (rest, key) = preceded(wsp, parse_identifier)(input)?;
    let (rest, _) = preceded(wsp, cut(char(':')))(rest)?;
    let (rest, value) = cut(parse_expr)(rest)?;
    Ok((rest, (key, value)))
}

// ---------- primaries ----------

type PrimaryResult<'a> = IResult<&'a str, Primary, nom::error::Error<&'a str>>;

fn parse_chain(input: &str) -> PResult<'_> {
    let input = ws(input);
    if input.starts_with('*') && !input.starts_with("&&") {
        // `*` alone is a wildcard over the current node
        let (rest, _) = char('*')(input)?;
        let (rest, mut postfixes) = parse_postfixes(rest)?;
        postfixes.insert(0, Postfix::Wildcard { bracket: false });
        return Ok((rest, Expr::Chain(Primary::Current, postfixes)));
    }
    if input.starts_with('(') {
        let (rest, inner) = cut(delimited(
            preceded(char('('), wsp),
            parse_expr,
            preceded(wsp, char(')')),
        ))(input)?;
        // the grouped expression re-enters the postfix chain: (a || b).c
        let (rest, postfixes) = parse_postfixes(rest)?;
        return Ok((rest, rebuild_chain(inner, postfixes)));
    }
    // an expression may begin with a bracket selector: [0] indexes the
    // current node
    if input.starts_with('[') {
        let (rest, first) = parse_bracket_start(input)?;
        let (rest, mut postfixes) = parse_postfixes(rest)?;
        postfixes.insert(0, first);
        return Ok((rest, Expr::Chain(Primary::Current, postfixes)));
    }
    let (rest, primary) = parse_primary(input)?;
    let (rest, postfixes) = parse_postfixes(rest)?;
    Ok((rest, Expr::Chain(primary, postfixes)))
}

fn parse_primary(input: &str) -> PrimaryResult<'_> {
    let input = ws(input);
    alt((
        parse_current,
        parse_function_or_field,
        parse_quoted_identifier,
        parse_literal,
        parse_raw_string,
        parse_multilist,
        parse_multihash_primary,
    ))(input)
}

fn parse_current(input: &str) -> PrimaryResult<'_> {
    value(Primary::Current, char('@'))(input)
}

/// An UNQUOTED identifier is a function call when "(" follows; otherwise
/// a field. A quoted identifier is never callable (`"foo"(bar)` is
/// invalid).
fn parse_function_or_field(input: &str) -> PrimaryResult<'_> {
    let (rest, name) = unquoted_identifier(input)?;
    let rest = ws(rest);
    if rest.starts_with('(') {
        return parse_function_args(rest, name);
    }
    Ok((rest, Primary::Field(name)))
}

fn parse_function_args(input: &str, name: String) -> PrimaryResult<'_> {
    let (rest, args) = parse_function_args_primary(input, name.clone())?;
    Ok((rest, Primary::Function(name, args)))
}

fn parse_function_args_primary(
    input: &str,
    _name: String,
) -> IResult<&str, Vec<FnArg>, nom::error::Error<&str>> {
    let (rest, _) = char('(')(input)?;
    let (rest, parsed_args) = if ws(rest).starts_with(')') {
        (rest, vec![])
    } else {
        separated_list1(preceded(wsp, char(',')), preceded(wsp, parse_fn_arg))(rest)?
    };
    let (rest, _) = preceded(wsp, cut(char(')')))(rest)?;
    Ok((rest, parsed_args))
}

fn parse_fn_arg(input: &str) -> IResult<&str, FnArg, nom::error::Error<&str>> {
    let input = ws(input);
    if input.starts_with('&') {
        let (rest, _) = char('&')(input)?;
        let (rest, inner) = cut(parse_pipe)(rest)?;
        Ok((rest, FnArg::Ref(inner)))
    } else {
        let (rest, e) = cut(parse_pipe)(input)?;
        Ok((rest, FnArg::Value(e)))
    }
}

fn parse_quoted_identifier(input: &str) -> PrimaryResult<'_> {
    let (rest, quoted) = parse_quoted_string(input)?;
    Ok((rest, Primary::Field(quoted)))
}

/// A double-quoted string with JSON escapes; returns the unescaped content.
fn parse_quoted_string(input: &str) -> IResult<&str, String, nom::error::Error<&str>> {
    let (rest, _) = char('"')(input)?;
    let mut out = String::new();
    let mut pending_high: Option<u16> = None;
    let mut rest = rest;
    loop {
        let (next, chunk) = take_while(|c: char| c != '"' && c != '\\')(rest)?;
        out.push_str(chunk);
        rest = next;
        if let Some(after) = rest.strip_prefix('"') {
            return Ok((after, out));
        }
        // escape sequence: the next char must be a backslash
        let first = match rest.chars().next() {
            Some(c) => c,
            None => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    rest,
                    nom::error::ErrorKind::Eof,
                )));
            }
        };
        if first != '\\' {
            return Err(nom::Err::Failure(nom::error::Error::new(
                rest,
                nom::error::ErrorKind::Tag,
            )));
        }
        let after_backslash = &rest[1..];
        let (next, code) = cut(any_char)(after_backslash)?;
        rest = match code {
            '"' | '\\' | '/' => {
                out.push(code);
                next
            }
            'b' => {
                out.push('\u{0008}');
                next
            }
            'f' => {
                out.push('\u{000C}');
                next
            }
            'n' => {
                out.push('\n');
                next
            }
            'r' => {
                out.push('\r');
                next
            }
            't' => {
                out.push('\t');
                next
            }
            'u' => {
                let (next, hex) = cut(take_while1(|c: char| c.is_ascii_hexdigit()))(next)?;
                if hex.len() != 4 {
                    return Err(nom::Err::Failure(nom::error::Error::new(
                        next,
                        nom::error::ErrorKind::HexDigit,
                    )));
                }
                let code = u16::from_str_radix(hex, 16).map_err(|_| {
                    nom::Err::Failure(nom::error::Error::new(
                        next,
                        nom::error::ErrorKind::HexDigit,
                    ))
                })?;
                // surrogate pairs combine; lone surrogates become the
                // replacement character
                const HIGH: u16 = 0xD800;
                const LOW: u16 = 0xDC00;
                if (HIGH..LOW).contains(&code) {
                    pending_high = Some(code);
                } else if (LOW..=0xDFFF).contains(&code) {
                    match pending_high.take() {
                        Some(high) => {
                            let combined =
                                0x10000 + (((high - HIGH) as u32) << 10) + (code - LOW) as u32;
                            out.push(char::from_u32(combined).unwrap_or('\u{FFFD}'));
                        }
                        None => out.push('\u{FFFD}'),
                    }
                } else {
                    pending_high = None;
                    out.push(char::from_u32(code as u32).unwrap_or('\u{FFFD}'));
                }
                next
            }
            _ => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    next,
                    nom::error::ErrorKind::Escaped,
                )));
            }
        };
    }
}

fn any_char(input: &str) -> IResult<&str, char, nom::error::Error<&str>> {
    let first = input.chars().next().ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Eof))
    })?;
    Ok((&input[first.len_utf8()..], first))
}

/// An identifier: unquoted (`[A-Za-z_][A-Za-z0-9_]*`) or double-quoted
/// (with JSON escapes). Returns the identifier's content.
fn parse_identifier(input: &str) -> IResult<&str, String, nom::error::Error<&str>> {
    let (rest, name) = alt((parse_quoted_string, unquoted_identifier))(input)?;
    Ok((rest, name))
}

fn unquoted_identifier(input: &str) -> IResult<&str, String, nom::error::Error<&str>> {
    let (rest, text) = recognize(tuple((
        take_while1(|c: char| c.is_ascii_alphabetic() || c == '_'),
        take_while(|c: char| c.is_ascii_alphanumeric() || c == '_'),
    )))(input)?;
    Ok((rest, text.to_string()))
}

/// A JSON literal in backticks: `` `{"a": 1}` ``, `` `[1, 2]` ``, with
/// embedded backticks escaped as `` \` ``.
fn parse_literal(input: &str) -> PrimaryResult<'_> {
    let (rest, _) = char('`')(input)?;
    let mut raw = String::new();
    let mut rest = rest;
    loop {
        match rest.chars().next() {
            None => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    rest,
                    nom::error::ErrorKind::Eof,
                )));
            }
            Some('`') => {
                rest = &rest[1..];
                break;
            }
            Some('\\') => {
                let after_backslash = &rest[1..];
                match after_backslash.chars().next() {
                    Some('`') => {
                        raw.push('`');
                        rest = &after_backslash[1..];
                    }
                    Some(other) => {
                        raw.push('\\');
                        raw.push(other);
                        rest = &after_backslash[other.len_utf8()..];
                    }
                    None => {
                        return Err(nom::Err::Failure(nom::error::Error::new(
                            rest,
                            nom::error::ErrorKind::Escaped,
                        )));
                    }
                }
            }
            Some(other) => {
                raw.push(other);
                rest = &rest[other.len_utf8()..];
            }
        }
    }
    let value = parse_literal_json(&raw)
        .map_err(|_| nom::Err::Failure(nom::error::Error::new(rest, nom::error::ErrorKind::Tag)))?;
    Ok((rest, Primary::Literal(value)))
}

/// Parse the inside of a literal as JSON, with the specification's
/// single-quoted-string allowance.
fn parse_literal_json(text: &str) -> std::result::Result<serde_json::Value, ()> {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
        return Ok(v);
    }
    // Single-quoted string literal inside backticks: ` 'abc' `
    let trimmed = text.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        return Ok(serde_json::Value::String(
            trimmed[1..trimmed.len() - 1].to_string(),
        ));
    }
    Err(())
}

/// A raw string: `'abc'` — no escape sequences; content up to the next
/// single quote.
fn parse_raw_string(input: &str) -> PrimaryResult<'_> {
    let (rest, _) = char('\'')(input)?;
    let mut content = String::new();
    let mut rest = rest;
    loop {
        // the only escape inside a raw string is the quote: \'
        let (next, chunk) = take_while(|c: char| c != '\'' && c != '\\')(rest)?;
        content.push_str(chunk);
        rest = next;
        match rest.chars().next() {
            Some('\'') => {
                rest = &rest[1..];
                break;
            }
            Some('\\') => {
                let after = &rest[1..];
                match after.chars().next() {
                    Some('\'') => {
                        content.push('\'');
                        rest = &after[1..];
                    }
                    Some(other) => {
                        // a backslash that is not an escaped quote is
                        // itself raw content
                        content.push('\\');
                        content.push(other);
                        rest = &after[other.len_utf8()..];
                    }
                    None => {
                        return Err(nom::Err::Failure(nom::error::Error::new(
                            rest,
                            nom::error::ErrorKind::Eof,
                        )));
                    }
                }
            }
            _ => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    rest,
                    nom::error::ErrorKind::Eof,
                )));
            }
        }
    }
    Ok((rest, Primary::RawString(content)))
}

/// A multi-select list at the primary position: `[expr, expr]`.
fn parse_multilist(input: &str) -> PrimaryResult<'_> {
    let (rest, _) = char('[')(input)?;
    let input = ws(rest);
    // a bare integer here would be an index, which is not a valid
    // expression — reject early so `[0]` does not parse as an empty list
    if input.starts_with(|c: char| c.is_ascii_digit() || c == '-') {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }
    let (rest, items) = cut(separated_list1(
        preceded(wsp, char(',')),
        preceded(wsp, parse_expr),
    ))(input)?;
    let (rest, _) = preceded(wsp, cut(char(']')))(rest)?;
    Ok((rest, Primary::MultiList(items)))
}

/// A multi-select hash at the primary position: `{key: expr, ...}`.
fn parse_multihash_primary(input: &str) -> PrimaryResult<'_> {
    let (rest, post) = parse_multihash_body(input)?;
    match post {
        Postfix::MultiHash(entries) => Ok((rest, Primary::MultiHash(entries))),
        _ => unreachable!("multihash body always yields MultiHash"),
    }
}

// The parser ends here; the corpus (not this file's helpers) defines
// conformance.
