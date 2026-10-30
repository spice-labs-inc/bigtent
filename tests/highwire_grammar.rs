//! The walk grammar's levels are semantics, not shapes.
//!
//! WHAT THIS TESTS
//!
//! `grammar.rs` declares one trait per semantic level of a walk query. This file
//! implements two of them — `ExpressionBuilder` (the expression level) and `ItemFilter` (the
//! item filter level) — for a representation that is deliberately unlike JSON:
//! `Node`, a tagged enum with **no keys, no objects, and therefore no
//! single-entry rule and nothing to look up**. A compound gate in this
//! representation is `Tagged("and", List([…]))`, not `{"and": […]}`.
//!
//! WHY IT IS WRITTEN THIS WAY
//!
//! A test that only ever exercises `serde_json::Value` cannot tell "the level is
//! a semantic" from "the level happens to be JSON". The stronger demonstration
//! is a representation in which the concepts JSON uses to express the grammar do
//! not exist. If the level traits were reaching for an object, a key, or a
//! single-entry rule, this file could not be written.
//!
//! It is also why the single-entry rule is not on a trait: it is a fact about
//! JSON — a compound node there is a one-entry object — and a representation
//! that tags its compound nodes never needs it.
//!
//! LLM-FRIENDLY SUMMARY
//!
//! - Requirement: each level of the walk query grammar is a trait expressing
//!   that level's semantic.
//! - `Node` implements `ExpressionBuilder` and `ItemFilter` in its own terms.
//! - The level traits return sub-levels as `Rc<dyn _>` — behaviour, not layout —
//!   so no implementation is ever required to agree on a struct's fields.
//! - `serde_json::Value` answers the same semantics, which is what makes the
//!   traits the interface rather than a description of JSON.

use anyhow::Result;
use bigtent::highwire::grammar::{GrammarError, ItemFilter, LevelResult, Step};
use bigtent::highwire::walk::ExpressionBuilder;
use bigtent::highwire::walk::CompareTypes;
use std::rc::Rc;

/// A walk query in a representation that is not JSON.
///
/// There are no objects here and no keys. A level's name and a compound node's
/// operator are TAGS, and a comparison carries its operator directly instead of
/// spelling it as a one-entry object.
#[derive(Debug, Clone, PartialEq)]
enum Node {
    /// A node named by a tag and carrying one child: how this representation
    /// writes a level wrapper (`filter`) or a compound gate node (`not`, `and`,
    /// `or`).
    Tagged(String, Box<Node>),
    /// A list of children.
    List(Vec<Node>),
    /// A comparison: its operator, the walk-state variable it reads, and the
    /// value it compares against.
    Compare(CompareTypes, String, Box<Node>),
    /// Text: a sansho expression, at the gate level.
    Text(String),
    /// An integer.
    Int(isize),
    /// A boolean.
    Bool(bool),
}

fn tagged(name: &str, child: Node) -> Node {
    Node::Tagged(name.to_string(), Box::new(child))
}

/// The expression level, answered from this representation's own shape.
///
/// Every method is a semantic question about a node, and each is answered by
/// matching a tag. Nothing is looked up and nothing is iterated. Children come
/// back as `Rc<dyn ExpressionBuilder>` because the level trait is a trait object — which is
/// what lets a level own sub-levels without any implementation agreeing on a
/// layout.
impl ExpressionBuilder for Node {
    fn not(&self) -> LevelResult<Option<Rc<dyn ExpressionBuilder>>> {
        match self {
            Node::Tagged(name, operand) if name == "not" => {
                Ok(Some(Rc::new(operand.as_ref().clone())))
            }
            _ => Ok(None),
        }
    }

    fn and(&self) -> LevelResult<Option<Vec<Rc<dyn ExpressionBuilder>>>> {
        match self {
            Node::Tagged(name, children) if name == "and" => match children.as_ref() {
                Node::List(items) => Ok(Some(
                    items.iter().map(|i| Rc::new(i.clone()) as Rc<dyn ExpressionBuilder>).collect(),
                )),
                other => Err(GrammarError::at(
                    Step::Gate,
                    format!("an `and` holds a list of gates, found {other:?}"),
                )),
            },
            _ => Ok(None),
        }
    }

    fn or(&self) -> LevelResult<Option<Vec<Rc<dyn ExpressionBuilder>>>> {
        match self {
            Node::Tagged(name, children) if name == "or" => match children.as_ref() {
                Node::List(items) => Ok(Some(
                    items.iter().map(|i| Rc::new(i.clone()) as Rc<dyn ExpressionBuilder>).collect(),
                )),
                other => Err(GrammarError::at(
                    Step::Gate,
                    format!("an `or` holds a list of gates, found {other:?}"),
                )),
            },
            _ => Ok(None),
        }
    }

    fn as_compare(&self) -> LevelResult<Option<(CompareTypes, &str, Rc<dyn ExpressionBuilder>)>> {
        match self {
            Node::Compare(operator, variable, value) => Ok(Some((
                *operator,
                variable.as_str(),
                Rc::new(value.as_ref().clone()),
            ))),
            _ => Ok(None),
        }
    }

    fn as_string(&self) -> Option<&str> {
        match self {
            Node::Text(expression) => Some(expression.as_str()),
            _ => None,
        }
    }

    fn as_int(&self) -> Option<isize> {
        match self {
            Node::Int(value) => Some(*value),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Node::Bool(value) => Some(*value),
            _ => None,
        }
    }
}

/// The item filter level, answered from this representation's own shape: the
/// filter is whatever is tagged `filter`, and its gate is what it carries.
impl ItemFilter for Node {
    fn gate(&self) -> LevelResult<Rc<dyn ExpressionBuilder>> {
        match self {
            Node::Tagged(name, gate) if name == "filter" => Ok(Rc::new(gate.as_ref().clone())),
            other => Err(GrammarError::at(
                Step::Filter,
                format!("a filter is tagged `filter`, found {other:?}"),
            )),
        }
    }
}

/// Requirement: the expression level is a semantic, so a representation with no
/// keys at all can express it and the level answers from its own shape.
///
/// Theory: if `ExpressionBuilder` had been built out of JSON's accessors — an object, a key,
/// a single-entry rule — this implementation could not exist. It matches tags.
#[test]
fn the_expression_level_is_answered_from_the_representations_own_shape() -> Result<()> {
    let conjunction = tagged(
        "and",
        Node::List(vec![Node::Text("length(@) > `1`".into()), Node::Bool(true)]),
    );

    let operands = conjunction
        .and()?
        .expect("a tagged `and` answers with its operands");
    assert_eq!(operands.len(), 2, "both operands come back");
    assert_eq!(
        operands[0].as_string(),
        Some("length(@) > `1`"),
        "the first operand is the text leaf it was built from"
    );
    assert_eq!(
        operands[1].as_bool(),
        Some(true),
        "the second operand is the boolean leaf it was built from"
    );

    assert!(
        conjunction.not()?.is_none(),
        "an `and` is not a `not`, and the probe says so rather than guessing"
    );
    assert!(conjunction.as_compare()?.is_none(), "an `and` is not a comparison");
    Ok(())
}

/// Requirement: a comparison is a semantic with three parts — operator,
/// variable, value — and the value is a sub-node of the same document.
///
/// Theory: `as_compare` hands back the value as `Rc<dyn ExpressionBuilder>`, so the
/// comparison's shape is the representation's business and only its meaning is
/// the trait's.
#[test]
fn a_comparison_answers_operator_variable_and_value() -> Result<()> {
    let comparison = Node::Compare(CompareTypes::LTE, "depth".to_string(), Box::new(Node::Int(2)));

    let (operator, variable, value) = comparison
        .as_compare()?
        .expect("a comparison node answers as one");
    assert_eq!(operator, CompareTypes::LTE);
    assert_eq!(variable, "depth");
    assert_eq!(value.as_int(), Some(2), "the value node comes back and is readable");

    assert!(comparison.and()?.is_none(), "a comparison is not an `and`");
    assert!(comparison.as_string().is_none(), "a comparison carries no sansho expression");
    Ok(())
}

/// Requirement: the item filter level owns a gate, and owns it as a sub-level
/// rather than as a layout.
///
/// Theory: `ItemFilter::gate` returns `Rc<dyn ExpressionBuilder>`. A tagged representation
/// answers with the gate it carries, and nothing above the trait ever sees how
/// the filter was written.
#[test]
fn the_item_filter_level_owns_its_gate() -> Result<()> {
    let filter = tagged("filter", Node::Text("contains(body.mime_type, 'application/java-vm')".to_string()));

    let gate = filter.gate()?;
    assert_eq!(
        gate.as_string(),
        Some("contains(body.mime_type, 'application/java-vm')"),
        "the filter's gate is the expression it carries"
    );
    Ok(())
}

/// Requirement: a document that is not a filter says so by failing, because the
/// level's contract is that a filter HAS a gate.
///
/// Theory: the absence of a filter is not the filter level's business — only the
/// caller knows whether a filter was expected — so the level answers with an
/// error naming what it found.
#[test]
fn a_document_that_is_not_a_filter_reports_what_it_found() {
    let not_a_filter = tagged("emit", Node::Text("@".to_string()));
    let error = not_a_filter
        .gate()
        .expect_err("an emit is not an item filter")
        .to_string();
    assert!(
        error.contains("filter"),
        "the error names the level it was reading: {error}"
    );
}
