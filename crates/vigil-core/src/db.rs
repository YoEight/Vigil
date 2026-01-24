use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{BTreeMap, HashMap, VecDeque},
    f64, iter,
    str::Split,
};

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc};
use eventql_parser::{
    Query, Type,
    prelude::{AnalysisOptions, Operator, Typed},
};
use rand::Rng;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, Serialize)]
pub enum Error {
    #[error(transparent)]
    Query(eventql_parser::prelude::Error),

    #[error("subject cannot start with a '/'")]
    IllegalSubject,
}

impl From<eventql_parser::prelude::Error> for Error {
    fn from(value: eventql_parser::prelude::Error) -> Self {
        Self::Query(value)
    }
}

pub type Result<A> = std::result::Result<A, Error>;

#[derive(Default, Serialize)]
pub struct Event {
    pub spec_version: String,
    pub id: Uuid,
    pub source: String,
    pub subject: String,
    pub event_type: String,
    pub datacontenttype: String,
    pub data: String,
}

impl Event {
    fn project<'a>(&'a self, expected: &'a Type) -> QueryValue<'a> {
        if let eventql_parser::Type::Record(rec) = expected {
            let mut props = BTreeMap::new();
            for (name, value) in rec.iter() {
                match name.as_str() {
                    "spec_version" => match value {
                        Type::String => {
                            props.insert(
                                name.as_str(),
                                QueryValue::String(self.spec_version.as_str().into()),
                            );
                        }

                        _ => {
                            props.insert(name.as_str(), QueryValue::Null);
                        }
                    },

                    "id" => match value {
                        Type::String => {
                            props.insert(
                                name.as_str(),
                                QueryValue::String(self.id.to_string().into()),
                            );
                        }

                        _ => {
                            props.insert(name.as_str(), QueryValue::Null);
                        }
                    },

                    "source" => match value {
                        Type::String => {
                            props.insert(
                                name.as_str(),
                                QueryValue::String(self.source.as_str().into()),
                            );
                        }

                        _ => {
                            props.insert(name.as_str(), QueryValue::Null);
                        }
                    },

                    "subject" => match value {
                        Type::String | Type::Subject => {
                            props.insert(
                                name.as_str(),
                                QueryValue::String(self.subject.as_str().into()),
                            );
                        }

                        _ => {
                            props.insert(name.as_str(), QueryValue::Null);
                        }
                    },

                    "type" => match value {
                        Type::String => {
                            props.insert(
                                name.as_str(),
                                QueryValue::String(self.event_type.as_str().into()),
                            );
                        }

                        _ => {
                            props.insert(name.as_str(), QueryValue::Null);
                        }
                    },

                    "datacontenttype" => match value {
                        Type::String => {
                            props.insert(
                                name.as_str(),
                                QueryValue::String(self.datacontenttype.as_str().into()),
                            );
                        }

                        _ => {
                            props.insert(name.as_str(), QueryValue::Null);
                        }
                    },

                    "data" => match value {
                        Type::String => {
                            props.insert(
                                name.as_str(),
                                QueryValue::String(self.data.as_str().into()),
                            );
                        }

                        Type::Record(_props) => match self.datacontenttype.as_str() {
                            "application/json" => {
                                todo!("use serde_json to get a record out of the data payload")
                            }

                            _ => {
                                props.insert(name.as_str(), QueryValue::Null);
                            }
                        },

                        _ => {
                            props.insert(name.as_str(), QueryValue::Null);
                        }
                    },

                    _ => {
                        props.insert(name.as_str(), QueryValue::Null);
                    }
                }
            }

            QueryValue::Record(Cow::Owned(props))
        } else {
            QueryValue::Null
        }
    }
}

#[derive(Default)]
pub struct Subject {
    events: Vec<usize>,
    nodes: HashMap<String, Subject>,
}

impl Subject {
    fn entries<'a>(&mut self, mut path: impl Iterator<Item = &'a str>) -> &mut Vec<usize> {
        let name = path.next().unwrap_or_default();

        if name != "" {
            return self.nodes.entry(name.to_owned()).or_default().entries(path);
        }

        &mut self.events
    }
}

pub enum Subjects<'a> {
    Dive {
        split: Split<'a, char>,
        current: &'a Subject,
    },

    Browse {
        queue: VecDeque<&'a Subject>,
    },
}

impl<'a> Subjects<'a> {
    pub fn new(path: &'a str, subject: &'a Subject) -> Self {
        Self::Dive {
            split: path.split('/'),
            current: subject,
        }
    }
}

impl<'a> Iterator for Subjects<'a> {
    type Item = &'a Subject;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self {
                Subjects::Dive { split, current } => {
                    let path = split.next().unwrap_or_default();

                    if path.trim().is_empty() {
                        let mut queue = VecDeque::new();

                        queue.push_back(*current);

                        *self = Self::Browse { queue };
                        continue;
                    }

                    *current = current.nodes.get(path)?;
                }

                Subjects::Browse { queue } => {
                    let current = queue.pop_front()?;

                    queue.extend(current.nodes.values());

                    return Some(current);
                }
            }
        }
    }
}

pub struct IndexedEvents<'a, I> {
    indexes: I,
    events: &'a [Event],
}

impl<'a, I> IndexedEvents<'a, I> {
    pub fn new(indexes: I, events: &'a [Event]) -> Self {
        Self { indexes, events }
    }
}

impl<'a, I> Iterator for IndexedEvents<'a, I>
where
    I: Iterator<Item = usize> + 'a,
{
    type Item = &'a Event;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.indexes.next()?;
        self.events.get(idx)
    }
}

#[derive(Default)]
pub struct Db {
    types: HashMap<String, Vec<usize>>,
    subjects: Subject,
    events: Vec<Event>,
}

impl Db {
    pub fn append(&mut self, subject: &str, events: Vec<Event>) -> Result<()> {
        if subject.starts_with('/') {
            return Err(Error::IllegalSubject);
        }

        let subject_entries = self.subjects.entries(subject.split('/'));
        let mut next_id = self.events.len();

        for event in events {
            // index by types
            self.types
                .entry(event.event_type.clone())
                .or_default()
                .push(next_id);

            // index by subject
            subject_entries.push(next_id);

            // store the event in the persistent storage
            self.events.push(event);
            next_id += 1;
        }

        Ok(())
    }

    pub fn iter_type<'a>(&'a self, tpe: &'a str) -> impl Iterator<Item = &'a Event> + 'a {
        let type_events = self
            .types
            .get(tpe)
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .copied();

        IndexedEvents::new(type_events, self.events.as_slice())
    }

    pub fn iter_subject<'a>(&'a self, path: &'a str) -> impl Iterator<Item = &'a Event> + 'a {
        let subject_events =
            Subjects::new(path, &self.subjects).flat_map(|sub| sub.events.iter().copied());

        IndexedEvents::new(subject_events, self.events.as_slice())
    }

    pub fn run_query<'a>(
        &'a self,
        options: &'a AnalysisOptions,
        query: &'a Query<Typed>,
    ) -> Row<'a> {
        catalog(self, options, query)
    }
}

type Row<'a> = Box<dyn Iterator<Item = QueryValue<'a>> + 'a>;

type Sources<'a> = HashMap<&'a str, Row<'a>>;

pub struct EventQuery<'a> {
    srcs: Sources<'a>,
    query: &'a Query<Typed>,
    options: &'a AnalysisOptions,
    buffer: HashMap<&'a str, QueryValue<'a>>,
}

impl<'a> EventQuery<'a> {
    pub fn new(srcs: Sources<'a>, options: &'a AnalysisOptions, query: &'a Query<Typed>) -> Self {
        Self {
            srcs,
            query,
            options,
            buffer: Default::default(),
        }
    }
}

impl<'a> Iterator for EventQuery<'a> {
    type Item = QueryValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.buffer.clear();

            for (binding, row) in self.srcs.iter_mut() {
                self.buffer.insert(binding, row.next()?);
            }

            if let Some(predicate) = &self.query.predicate
                && !evaluate_predicate(&self.options, &self.buffer, &predicate.value)
            {
                continue;
            }

            return Some(evaluate_value(
                &self.options,
                &self.buffer,
                &self.query.projection.value,
            ));
        }
    }
}

#[derive(Clone, Serialize)]
pub enum QueryValue<'a> {
    Null,
    String(Cow<'a, str>),
    Number(f64),
    Bool(bool),
    Record(Cow<'a, BTreeMap<&'a str, QueryValue<'a>>>),
    Array(Cow<'a, [QueryValue<'a>]>),
    DateTime(DateTime<Utc>),
    Date(NaiveDate),
    Time(NaiveTime),
}

impl QueryValue<'_> {
    pub fn as_bool_or_panic(&self) -> bool {
        if let Self::Bool(b) = self {
            return *b;
        }

        panic!("expected a boolean but got something else")
    }
}

fn evaluate_value<'a>(
    options: &AnalysisOptions,
    env: &HashMap<&'a str, QueryValue<'a>>,
    value: &'a eventql_parser::Value,
) -> QueryValue<'a> {
    match value {
        eventql_parser::Value::Number(n) => QueryValue::Number(*n),
        eventql_parser::Value::String(s) => QueryValue::String(Cow::Borrowed(s.as_str())),
        eventql_parser::Value::Bool(b) => QueryValue::Bool(*b),
        eventql_parser::Value::Id(id) => env.get(id.as_str()).cloned().expect("id to be defined"),
        eventql_parser::Value::Array(exprs) => {
            let mut arr = Vec::with_capacity(exprs.capacity());

            for expr in exprs {
                arr.push(evaluate_value(options, env, &expr.value));
            }

            QueryValue::Array(Cow::Owned(arr))
        }

        eventql_parser::Value::Record(fields) => {
            let mut record = BTreeMap::new();

            for field in fields {
                record.insert(
                    field.name.as_str(),
                    evaluate_value(options, env, &field.value.value),
                );
            }

            QueryValue::Record(Cow::Owned(record))
        }

        eventql_parser::Value::Access(access) => {
            match evaluate_value(options, env, &access.target.value) {
                QueryValue::Record(rec) => rec
                    .get(access.field.as_str())
                    .cloned()
                    .unwrap_or(QueryValue::Null),

                _ => unreachable!(
                    "the query was statically analyzed, rendering that situation impossible"
                ),
            }
        }

        eventql_parser::Value::App(app) => {
            let mut args = Vec::with_capacity(app.args.capacity());

            for arg in &app.args {
                args.push(evaluate_value(options, env, &arg.value));
            }

            // -------------
            // Math functions
            // ------------

            if app.func.eq_ignore_ascii_case("abs")
                && let QueryValue::Number(n) = &args[0]
            {
                return QueryValue::Number(n.abs());
            }

            if app.func.eq_ignore_ascii_case("ceil")
                && let QueryValue::Number(n) = &args[0]
            {
                return QueryValue::Number(n.ceil());
            }

            if app.func.eq_ignore_ascii_case("floor")
                && let QueryValue::Number(n) = &args[0]
            {
                return QueryValue::Number(n.floor());
            }

            if app.func.eq_ignore_ascii_case("floor")
                && let QueryValue::Number(n) = &args[0]
            {
                return QueryValue::Number(n.round());
            }

            if app.func.eq_ignore_ascii_case("cos")
                && let QueryValue::Number(n) = &args[0]
            {
                return QueryValue::Number(n.cos());
            }

            if app.func.eq_ignore_ascii_case("sin")
                && let QueryValue::Number(n) = &args[0]
            {
                return QueryValue::Number(n.sin());
            }

            if app.func.eq_ignore_ascii_case("tan")
                && let QueryValue::Number(n) = &args[0]
            {
                return QueryValue::Number(n.tan());
            }

            if app.func.eq_ignore_ascii_case("exp")
                && let QueryValue::Number(n) = &args[0]
            {
                return QueryValue::Number(n.exp());
            }

            if app.func.eq_ignore_ascii_case("pow")
                && let QueryValue::Number(x) = &args[0]
                && let QueryValue::Number(y) = &args[1]
            {
                return QueryValue::Number(x.powi(*y as i32));
            }

            if app.func.eq_ignore_ascii_case("sqrt")
                && let QueryValue::Number(n) = &args[0]
            {
                return QueryValue::Number(n.sqrt());
            }

            if app.func.eq_ignore_ascii_case("rand") {
                let mut rng = rand::rng();
                return QueryValue::Number(rng.random());
            }

            if app.func.eq_ignore_ascii_case("pi") {
                return QueryValue::Number(f64::consts::PI);
            }

            // ------------
            // String functions
            // ------------

            if app.func.eq_ignore_ascii_case("lower")
                && let QueryValue::String(s) = &args[0]
            {
                return QueryValue::String(s.to_lowercase().into());
            }

            if app.func.eq_ignore_ascii_case("upper")
                && let QueryValue::String(s) = &args[0]
            {
                return QueryValue::String(s.to_uppercase().into());
            }

            if app.func.eq_ignore_ascii_case("trim")
                && let QueryValue::String(s) = &args[0]
            {
                return QueryValue::String(s.trim().to_owned().into());
            }

            if app.func.eq_ignore_ascii_case("ltrim")
                && let QueryValue::String(s) = &args[0]
            {
                return QueryValue::String(s.trim_start().to_owned().into());
            }

            if app.func.eq_ignore_ascii_case("rtrim")
                && let QueryValue::String(s) = &args[0]
            {
                return QueryValue::String(s.trim_end().to_owned().into());
            }

            if app.func.eq_ignore_ascii_case("len")
                && let QueryValue::String(s) = &args[0]
            {
                return QueryValue::Number(s.len() as f64);
            }

            if app.func.eq_ignore_ascii_case("instr")
                && let QueryValue::String(x) = &args[0]
                && let QueryValue::String(y) = &args[1]
            {
                return QueryValue::Number(
                    x.find(y.as_ref()).map(|i| i + 1).unwrap_or_default() as f64
                );
            }

            if app.func.eq_ignore_ascii_case("substring")
                && let QueryValue::String(s) = &args[0]
                && let QueryValue::Number(start) = &args[1]
                && let QueryValue::Number(length) = &args[2]
            {
                let start = *start as usize;
                let length = *length as usize;

                return QueryValue::String(s.chars().skip(start).take(length).collect());
            }

            if app.func.eq_ignore_ascii_case("replace")
                && let QueryValue::String(x) = &args[0]
                && let QueryValue::String(y) = &args[1]
                && let QueryValue::String(z) = &args[2]
            {
                return QueryValue::String(x.replace(y.as_ref(), z.as_ref()).into());
            }

            if app.func.eq_ignore_ascii_case("startswith")
                && let QueryValue::String(x) = &args[0]
                && let QueryValue::String(y) = &args[1]
            {
                return QueryValue::Bool(x.starts_with(y.as_ref()));
            }

            if app.func.eq_ignore_ascii_case("endswith")
                && let QueryValue::String(x) = &args[0]
                && let QueryValue::String(y) = &args[1]
            {
                return QueryValue::Bool(x.ends_with(y.as_ref()));
            }

            // -------------
            // Date and Time functions
            // -------------

            if app.func.eq_ignore_ascii_case("now") {
                return QueryValue::DateTime(Utc::now());
            }

            if app.func.eq_ignore_ascii_case("year") {
                return match &args[0] {
                    QueryValue::DateTime(t) => QueryValue::Number(t.year() as f64),
                    QueryValue::Date(d) => QueryValue::Number(d.year() as f64),
                    _ => unreachable!(),
                };
            }

            if app.func.eq_ignore_ascii_case("month") {
                return match &args[0] {
                    QueryValue::DateTime(t) => QueryValue::Number(t.month() as f64),
                    QueryValue::Date(d) => QueryValue::Number(d.month() as f64),
                    _ => unreachable!(),
                };
            }

            if app.func.eq_ignore_ascii_case("day") {
                return match &args[0] {
                    QueryValue::DateTime(t) => QueryValue::Number(t.day() as f64),
                    QueryValue::Date(d) => QueryValue::Number(d.day() as f64),
                    _ => unreachable!(),
                };
            }

            if app.func.eq_ignore_ascii_case("hour") {
                return match &args[0] {
                    QueryValue::DateTime(t) => QueryValue::Number(t.hour() as f64),
                    QueryValue::Time(t) => QueryValue::Number(t.hour() as f64),
                    _ => unreachable!(),
                };
            }

            if app.func.eq_ignore_ascii_case("minute") {
                return match &args[0] {
                    QueryValue::DateTime(t) => QueryValue::Number(t.minute() as f64),
                    QueryValue::Time(t) => QueryValue::Number(t.minute() as f64),
                    _ => unreachable!(),
                };
            }

            if app.func.eq_ignore_ascii_case("weekday") {
                return match &args[0] {
                    QueryValue::DateTime(t) => {
                        QueryValue::Number(t.weekday().num_days_from_sunday() as f64)
                    }
                    QueryValue::Date(d) => {
                        QueryValue::Number(d.weekday().num_days_from_sunday() as f64)
                    }
                    _ => unreachable!(),
                };
            }

            // --------------
            // Conditional functions
            // --------------

            if app.func.eq_ignore_ascii_case("if")
                && let QueryValue::Bool(b) = args[0]
            {
                // TODO - cloning is not necessary here as we could evaluate args lazily but that'll do for now
                return if b { args[1].clone() } else { args[2].clone() };
            }

            unreachable!(
                "the query was statically analyzed so all the functions used in the query are known to the query planner and have their arguments properly typed"
            )
        }

        eventql_parser::Value::Binary(binary) => {
            let lhs = evaluate_value(options, env, &binary.lhs.value);

            if let Operator::As = binary.operator
                && let eventql_parser::Value::Id(tpe) = &binary.rhs.value
            {
                let tpe = eventql_parser::prelude::name_to_type(options, tpe)
                    .expect("to be defined because it has passed static analysis");

                return type_conversion(&lhs, tpe);
            }

            let rhs = evaluate_value(options, env, &binary.rhs.value);

            evaluate_binary_operation(binary.operator, &lhs, &rhs)
        }

        eventql_parser::Value::Unary(unary) => match unary.operator {
            Operator::Add => {
                if let QueryValue::Number(n) = evaluate_value(options, env, &unary.expr.value) {
                    QueryValue::Number(n)
                } else {
                    panic!("runtime error")
                }
            }

            Operator::Sub => {
                if let QueryValue::Number(n) = evaluate_value(options, env, &unary.expr.value) {
                    QueryValue::Number(-n)
                } else {
                    panic!("runtime error")
                }
            }

            Operator::Not => {
                if let QueryValue::Bool(b) = evaluate_value(options, env, &unary.expr.value) {
                    QueryValue::Bool(!b)
                } else {
                    panic!("runtime error")
                }
            }

            _ => panic!("runtime error"),
        },

        eventql_parser::Value::Group(expr) => evaluate_value(options, env, &expr.value),
    }
}

/// Many runtime error and most can be caught during static analysis.
fn type_conversion<'a>(value: &QueryValue<'a>, tpe: eventql_parser::Type) -> QueryValue<'a> {
    match value {
        QueryValue::Null => QueryValue::Null,

        QueryValue::String(cow) => match tpe {
            eventql_parser::Type::String | eventql_parser::Type::Subject => {
                QueryValue::String(cow.clone())
            }
            _ => panic!("runtime error"),
        },

        QueryValue::Number(n) => match tpe {
            eventql_parser::Type::Number => QueryValue::Number(*n),
            eventql_parser::Type::String => QueryValue::String(n.to_string().into()),
            _ => panic!("runtime error"),
        },

        QueryValue::Bool(b) => match tpe {
            eventql_parser::Type::String => QueryValue::String(b.to_string().into()),
            eventql_parser::Type::Bool => QueryValue::Bool(*b),
            _ => panic!("runtime error"),
        },

        QueryValue::Record(_) => panic!("runtime error"),
        QueryValue::Array(_) => panic!("runtime error"),

        QueryValue::DateTime(date_time) => match tpe {
            eventql_parser::Type::String => QueryValue::String(date_time.to_string().into()),
            eventql_parser::Type::Date => QueryValue::Date(date_time.date_naive()),
            eventql_parser::Type::Time => QueryValue::Time(date_time.time()),
            eventql_parser::Type::DateTime => QueryValue::DateTime(*date_time),
            _ => panic!("runtime error"),
        },

        QueryValue::Date(naive_date) => match tpe {
            eventql_parser::Type::String => QueryValue::String(naive_date.to_string().into()),
            eventql_parser::Type::Date => QueryValue::Date(*naive_date),
            _ => panic!("runtime error"),
        },

        QueryValue::Time(naive_time) => match tpe {
            eventql_parser::Type::String => QueryValue::String(naive_time.to_string().into()),
            eventql_parser::Type::Time => QueryValue::Time(*naive_time),
            _ => panic!("runtime error"),
        },
    }
}

fn evaluate_binary_operation<'a>(
    op: Operator,
    a: &QueryValue<'a>,
    b: &QueryValue<'a>,
) -> QueryValue<'a> {
    match (a, b) {
        (QueryValue::Null, QueryValue::Null) => QueryValue::Null,

        (QueryValue::String(a), QueryValue::String(b)) => match op {
            Operator::Eq => QueryValue::Bool(a == b),
            Operator::Neq => QueryValue::Bool(a != b),
            Operator::Lt => QueryValue::Bool(a < b),
            Operator::Lte => QueryValue::Bool(a <= b),
            Operator::Gt => QueryValue::Bool(a > b),
            Operator::Gte => QueryValue::Bool(a >= b),
            _ => panic!("runtime error"),
        },

        (QueryValue::Number(a), QueryValue::Number(b)) => match op {
            Operator::Add => QueryValue::Number(a + b),
            Operator::Sub => QueryValue::Number(a - b),
            Operator::Mul => QueryValue::Number(a * b),
            Operator::Div => QueryValue::Number(a / b),
            Operator::Eq => QueryValue::Bool(
                a.partial_cmp(b)
                    .map(|o| matches!(o, Ordering::Equal))
                    .unwrap_or_default(),
            ),
            Operator::Neq => QueryValue::Bool(
                a.partial_cmp(b)
                    .map(|o| !matches!(o, Ordering::Equal))
                    .unwrap_or_default(),
            ),
            Operator::Lt => QueryValue::Bool(a < b),
            Operator::Lte => QueryValue::Bool(a <= b),
            Operator::Gt => QueryValue::Bool(a > b),
            Operator::Gte => QueryValue::Bool(a >= b),
            _ => panic!("runtime error"),
        },

        (QueryValue::Bool(a), QueryValue::Bool(b)) => match op {
            Operator::Eq => QueryValue::Bool(a == b),
            Operator::Neq => QueryValue::Bool(a != b),
            Operator::Lt => QueryValue::Bool(a < b),
            Operator::Lte => QueryValue::Bool(a <= b),
            Operator::Gt => QueryValue::Bool(a > b),
            Operator::Gte => QueryValue::Bool(a >= b),
            Operator::And => QueryValue::Bool(*a && *b),
            Operator::Or => QueryValue::Bool(*a || *b),
            Operator::Xor => QueryValue::Bool(*a ^ *b),
            _ => panic!("runtime error"),
        },

        (this @ QueryValue::Record(a), that @ QueryValue::Record(b)) => match op {
            Operator::Eq => {
                if a.len() != b.len() {
                    return QueryValue::Bool(false);
                }

                for ((a_k, a_v), (b_k, b_v)) in a.iter().zip(b.iter()) {
                    if a_k != b_k
                        || evaluate_binary_operation(Operator::Eq, a_v, b_v).as_bool_or_panic()
                    {
                        return QueryValue::Bool(false);
                    }
                }

                QueryValue::Bool(true)
            }

            Operator::Neq => QueryValue::Bool(
                !evaluate_binary_operation(Operator::Eq, this, that).as_bool_or_panic(),
            ),

            _ => panic!("runtime error"),
        },

        (this @ QueryValue::Array(a), that @ QueryValue::Array(b)) => match op {
            Operator::Eq => {
                if a.len() != b.len() {
                    return QueryValue::Bool(false);
                }

                for (a, b) in a.iter().zip(b.iter()) {
                    if !evaluate_binary_operation(Operator::Eq, a, b).as_bool_or_panic() {
                        return QueryValue::Bool(false);
                    }
                }

                QueryValue::Bool(true)
            }

            Operator::Neq => QueryValue::Bool(
                !evaluate_binary_operation(Operator::Eq, this, that).as_bool_or_panic(),
            ),

            _ => panic!("runtime error"),
        },

        (QueryValue::DateTime(a), QueryValue::DateTime(b)) => match op {
            Operator::Eq => QueryValue::Bool(a == b),
            Operator::Neq => QueryValue::Bool(a != b),
            Operator::Lt => QueryValue::Bool(a < b),
            Operator::Lte => QueryValue::Bool(a <= b),
            Operator::Gt => QueryValue::Bool(a > b),
            Operator::Gte => QueryValue::Bool(a >= b),
            _ => panic!("runtime error"),
        },

        (QueryValue::Date(a), QueryValue::Date(b)) => match op {
            Operator::Eq => QueryValue::Bool(a == b),
            Operator::Neq => QueryValue::Bool(a != b),
            Operator::Lt => QueryValue::Bool(a < b),
            Operator::Lte => QueryValue::Bool(a <= b),
            Operator::Gt => QueryValue::Bool(a > b),
            Operator::Gte => QueryValue::Bool(a >= b),
            _ => panic!("runtime error"),
        },

        (QueryValue::Time(a), QueryValue::Time(b)) => match op {
            Operator::Eq => QueryValue::Bool(a == b),
            Operator::Neq => QueryValue::Bool(a != b),
            Operator::Lt => QueryValue::Bool(a < b),
            Operator::Lte => QueryValue::Bool(a <= b),
            Operator::Gt => QueryValue::Bool(a > b),
            Operator::Gte => QueryValue::Bool(a >= b),
            _ => panic!("runtime error"),
        },

        (QueryValue::Array(values), value) if matches!(op, Operator::Contains) => QueryValue::Bool(
            values
                .iter()
                .find(|a| evaluate_binary_operation(Operator::Eq, a, value).as_bool_or_panic())
                .is_some(),
        ),

        _ => panic!("runtime error"),
    }
}

fn evaluate_predicate<'a>(
    options: &AnalysisOptions,
    env: &HashMap<&'a str, QueryValue<'a>>,
    value: &'a eventql_parser::Value,
) -> bool {
    evaluate_value(options, env, value).as_bool_or_panic()
}

fn catalog<'a>(db: &'a Db, options: &'a AnalysisOptions, query: &'a Query<Typed>) -> Row<'a> {
    let mut srcs = Sources::new();
    for query_src in &query.sources {
        match &query_src.kind {
            eventql_parser::SourceKind::Name(name) => {
                if name.eq_ignore_ascii_case("events")
                    && let Some(tpe) = query.meta.scope.entries.get(&query_src.binding.name)
                {
                    srcs.insert(
                        &query_src.binding.name,
                        Box::new(db.events.iter().map(|e| e.project(tpe))),
                    );

                    continue;
                }

                srcs.insert(&query_src.binding.name, Box::new(iter::empty()));
            }

            eventql_parser::SourceKind::Subject(path) => {
                if let Some(tpe) = query.meta.scope.entries.get(&query_src.binding.name) {
                    srcs.insert(
                        &query_src.binding.name,
                        Box::new(db.iter_subject(path).map(|e| e.project(tpe))),
                    );

                    continue;
                }

                srcs.insert(&query_src.binding.name, Box::new(iter::empty()));
            }

            eventql_parser::SourceKind::Subquery(sub_query) => {
                let row = catalog(db, options, sub_query);

                srcs.insert(&query_src.binding.name, row);
            }
        }
    }

    Box::new(EventQuery::new(srcs, options, query))
}
