use core::f64;
use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{BTreeMap, HashMap, VecDeque},
    iter,
    str::Split,
};

use case_insensitive_hashmap::CaseInsensitiveHashMap;
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc};
use eventql_parser::{
    App, Query, Type,
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

#[derive(Debug, Error, Serialize)]
pub enum EvalError {
    #[error("runtime error: {0}")]
    Runtime(Cow<'static, str>),
}

pub type EvalResult<A> = std::result::Result<A, EvalError>;

#[derive(Default, Serialize)]
pub struct Event {
    pub spec_version: String,
    pub id: Uuid,
    pub source: String,
    pub subject: String,
    pub event_type: String,
    pub datacontenttype: String,
    pub data: Vec<u8>,
}

impl Event {
    fn project<'a>(&self, expected: &'a Type) -> QueryValue {
        if let eventql_parser::Type::Record(rec) = expected {
            let mut props = BTreeMap::new();
            for (name, value) in rec.iter() {
                match name.as_str() {
                    "spec_version" => match value {
                        Type::String => {
                            props.insert(
                                name.clone(),
                                QueryValue::String(self.spec_version.as_str().into()),
                            );
                        }

                        _ => {
                            props.insert(name.clone(), QueryValue::Null);
                        }
                    },

                    "id" => match value {
                        Type::String => {
                            props.insert(
                                name.clone(),
                                QueryValue::String(self.id.to_string().into()),
                            );
                        }

                        _ => {
                            props.insert(name.clone(), QueryValue::Null);
                        }
                    },

                    "source" => match value {
                        Type::String => {
                            props.insert(name.clone(), QueryValue::String(self.source.clone()));
                        }

                        _ => {
                            props.insert(name.clone(), QueryValue::Null);
                        }
                    },

                    "subject" => match value {
                        Type::String | Type::Subject => {
                            props.insert(
                                name.clone(),
                                QueryValue::String(self.subject.as_str().into()),
                            );
                        }

                        _ => {
                            props.insert(name.as_str().into(), QueryValue::Null);
                        }
                    },

                    "type" => match value {
                        Type::String => {
                            props.insert(
                                name.clone(),
                                QueryValue::String(self.event_type.as_str().into()),
                            );
                        }

                        _ => {
                            props.insert(name.clone(), QueryValue::Null);
                        }
                    },

                    "datacontenttype" => match value {
                        Type::String => {
                            props.insert(
                                name.clone(),
                                QueryValue::String(self.datacontenttype.as_str().into()),
                            );
                        }

                        _ => {
                            props.insert(name.as_str().into(), QueryValue::Null);
                        }
                    },

                    "data" => match value {
                        Type::String => {
                            props.insert(
                                name.clone(),
                                QueryValue::String(unsafe {
                                    str::from_utf8_unchecked(self.data.as_slice()).into()
                                }),
                            );
                        }

                        Type::Record(_) => match self.datacontenttype.as_str() {
                            "application/json" => {
                                if let Ok(payload) = serde_json::from_slice(&self.data) {
                                    props.insert(
                                        name.clone(),
                                        QueryValue::build_from_type_expectation(payload, value),
                                    );
                                } else {
                                    props.insert(name.clone(), QueryValue::Null);
                                }
                            }

                            _ => {
                                props.insert(name.clone(), QueryValue::Null);
                            }
                        },

                        _ => {
                            props.insert(name.clone(), QueryValue::Null);
                        }
                    },

                    _ => {
                        props.insert(name.clone(), QueryValue::Null);
                    }
                }
            }

            QueryValue::Record(props)
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

type Row<'a> = Box<dyn Iterator<Item = EvalResult<QueryValue>> + 'a>;

#[derive(Default)]
pub struct Sources<'a> {
    inner: HashMap<&'a str, Row<'a>>,
}

type Buffer<'a> = HashMap<&'a str, QueryValue>;

impl<'a> Sources<'a> {
    fn iter_mut(&mut self) -> impl Iterator<Item = (&&'a str, &mut Row<'a>)> {
        self.inner.iter_mut()
    }

    fn insert(&mut self, key: &'a str, row: Row<'a>) {
        self.inner.insert(key, row);
    }

    fn fill(&mut self, buffer: &mut Buffer<'a>) -> Option<EvalResult<()>> {
        for (binding, row) in self.iter_mut() {
            match row.next()? {
                Ok(value) => {
                    buffer.insert(binding, value);
                }

                Err(e) => return Some(Err(e)),
            }
        }

        Some(Ok(()))
    }
}

pub struct EventQuery<'a> {
    srcs: Sources<'a>,
    query: &'a Query<Typed>,
    options: &'a AnalysisOptions,
    buffer: HashMap<&'a str, QueryValue>,
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
    type Item = EvalResult<QueryValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.buffer.clear();

            let outcome = self.srcs.fill(&mut self.buffer)?;
            if let Err(e) = outcome {
                return Some(Err(e));
            }

            if let Some(predicate) = &self.query.predicate {
                match evaluate_predicate(&self.options, &self.buffer, &predicate.value) {
                    Ok(false) => continue,
                    Ok(true) => {}
                    Err(e) => return Some(Err(e)),
                }
            }

            return Some(evaluate_value(
                &self.options,
                &self.buffer,
                &self.query.projection.value,
            ));
        }
    }
}

#[derive(Copy, Clone, Default)]
struct Consts {
    now: Option<DateTime<Utc>>,
}

impl Consts {
    fn now(&mut self) -> DateTime<Utc> {
        if let Some(dt) = &self.now {
            return *dt;
        }

        let now = Utc::now();
        self.now = Some(now);
        now
    }
}

pub trait Agg {
    fn fold(&mut self, params: &[QueryValue]);
    fn complete(&self) -> QueryValue;
}

#[derive(Default)]
pub struct ConstAgg {
    inner: Option<QueryValue>,
}

impl Agg for ConstAgg {
    fn fold(&mut self, params: &[QueryValue]) {
        if params.is_empty() {
            return;
        }

        if self.inner.is_none() {
            self.inner = Some(params[0].clone());
        }
    }

    fn complete(&self) -> QueryValue {
        self.inner.clone().unwrap_or(QueryValue::Null)
    }
}

#[derive(Default)]
pub struct CountAgg {
    value: u64,
}

impl Agg for CountAgg {
    fn fold(&mut self, params: &[QueryValue]) {
        if !params.is_empty() {
            if let QueryValue::Bool(is_true) = params[0]
                && is_true
            {
                self.value += 1;
            }

            return;
        }

        self.value += 1;
    }

    fn complete(&self) -> QueryValue {
        QueryValue::Number(self.value as f64)
    }
}

#[derive(Default)]
pub struct AvgAgg {
    count: u64,
    acc: f64,
}

impl Agg for AvgAgg {
    fn fold(&mut self, params: &[QueryValue]) {
        if params.is_empty() {
            return;
        }

        if let QueryValue::Number(n) = params[0] {
            self.count += 1;
            self.acc += n;

            return;
        }

        self.acc = f64::NAN;
    }

    fn complete(&self) -> QueryValue {
        if self.acc.is_nan() {
            return QueryValue::Number(f64::NAN);
        }

        if self.count == 0 {
            QueryValue::Number(0f64)
        } else {
            QueryValue::Number(self.acc / self.count as f64)
        }
    }
}

enum AggState {
    Single(Box<dyn Agg>),
    Record(CaseInsensitiveHashMap<AggState>),
}

impl AggState {
    fn from(options: &AnalysisOptions, query: &Query<Typed>) -> Self {
        match &query.projection.value {
            eventql_parser::Value::Record(fields) => {
                let mut aggs = CaseInsensitiveHashMap::new();

                for field in fields.iter() {
                    if let eventql_parser::Value::App(app) = &field.value.value {
                        aggs.insert(
                            field.name.clone(),
                            Self::Single(agg_inst_from_func(options, app)),
                        );
                        continue;
                    }

                    let agg: Box<dyn Agg> = Box::new(ConstAgg::default());
                    aggs.insert(field.name.clone(), Self::Single(agg));
                }

                Self::Record(aggs)
            }

            eventql_parser::Value::App(app) => Self::Single(agg_inst_from_func(options, app)),

            _ => unreachable!("we expect an aggregate expression so this case should never happen"),
        }
    }

    fn complete(&self) -> QueryValue {
        match self {
            AggState::Single(agg) => agg.complete(),
            AggState::Record(aggs) => {
                let mut props = BTreeMap::new();

                for (key, agg) in aggs.iter() {
                    props.insert(key.as_ref().to_owned().into(), agg.complete());
                }

                QueryValue::Record(props)
            }
        }
    }
}

fn agg_inst_from_func(options: &AnalysisOptions, app: &App) -> Box<dyn Agg> {
    if let Type::App {
        aggregate: true, ..
    } = options
        .default_scope
        .entries
        .get(app.func.as_str())
        .expect("func to be defined")
    {
        return if app.func.eq_ignore_ascii_case("count") {
            Box::new(CountAgg::default())
        } else if app.func.eq_ignore_ascii_case("avg") {
            Box::new(AvgAgg::default())
        } else if app.func.eq_ignore_ascii_case("unique") {
            Box::new(ConstAgg::default())
        } else {
            unreachable!("impossible as such function wouldn't pass the static analysis")
        };
    }

    panic!("STATIC ANALYSIS BUG: expected an aggregate function but got a regular instead")
}

enum Emit {
    Single(AggState),
    Grouped(HashMap<String, AggState>),
}

pub struct AggQuery<'a> {
    srcs: Sources<'a>,
    query: &'a Query<Typed>,
    options: &'a AnalysisOptions,
    buffer: HashMap<&'a str, QueryValue>,
    emit: Emit,
    completed: bool,
    results: Vec<QueryValue>,
}

impl<'a> AggQuery<'a> {
    pub fn new(srcs: Sources<'a>, options: &'a AnalysisOptions, query: &'a Query<Typed>) -> Self {
        let emit = if query.group_by.is_some() {
            Emit::Grouped(Default::default())
        } else {
            Emit::Single(AggState::from(options, query))
        };

        Self {
            srcs,
            query,
            options,
            buffer: Default::default(),
            emit,
            completed: false,
            results: Vec::new(),
        }
    }
}

impl<'a> Iterator for AggQuery<'a> {
    type Item = EvalResult<QueryValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.completed {
            if let Some(result) = self.results.pop() {
                return Some(Ok(result));
            }

            return None;
        }

        loop {
            self.buffer.clear();

            let outcome = if let Some(outcome) = self.srcs.fill(&mut self.buffer) {
                outcome
            } else {
                self.completed = true;

                match &self.emit {
                    Emit::Single(agg) => self.results.push(agg.complete()),
                    Emit::Grouped(groups) => {
                        for group in groups.values() {
                            self.results.push(group.complete());
                        }
                    }
                }

                continue;
            };

            if let Err(e) = outcome {
                return Some(Err(e));
            }

            if let Some(predicate) = &self.query.predicate {
                match evaluate_predicate(&self.options, &self.buffer, &predicate.value) {
                    Ok(false) => continue,
                    Ok(true) => {}
                    Err(e) => return Some(Err(e)),
                }
            }

            let agg = if let Emit::Grouped(grouped) = &mut self.emit
                && let Some(group_by) = &self.query.group_by
            {
                let group_key =
                    match evaluate_value(&self.options, &self.buffer, &group_by.expr.value) {
                        Err(e) => return Some(Err(e)),
                        Ok(value) => match value {
                            QueryValue::String(s) => s.clone(),
                            QueryValue::Number(n) => n.to_string(),
                            QueryValue::Bool(b) => b.to_string(),
                            QueryValue::DateTime(date_time) => date_time.to_string(),
                            QueryValue::Date(naive_date) => naive_date.to_string(),
                            QueryValue::Time(naive_time) => naive_time.to_string(),
                            _ => {
                                return Some(Err(EvalError::Runtime(
                                    "unexpected group by value".into(),
                                )));
                            }
                        },
                    };

                grouped
                    .entry(group_key)
                    .or_insert_with(|| AggState::from(&self.options, &self.query))
            } else if let Emit::Single(agg) = &mut self.emit {
                agg
            } else {
                return Some(Err(EvalError::Runtime(
                    "wrong code path when running aggregate query".into(),
                )));
            };

            if let Err(e) = evaluate_agg_value(
                &self.options,
                &self.buffer,
                &self.query.projection.value,
                agg,
            ) {
                return Some(Err(e));
            }
        }
    }
}

#[derive(Clone, Serialize)]
pub enum QueryValue {
    Null,
    String(String),
    Number(f64),
    Bool(bool),
    Record(BTreeMap<String, QueryValue>),
    Array(Vec<QueryValue>),
    DateTime(DateTime<Utc>),
    Date(NaiveDate),
    Time(NaiveTime),
}

impl QueryValue {
    pub fn as_bool(&self) -> EvalResult<bool> {
        if let Self::Bool(b) = self {
            return Ok(*b);
        }

        Err(EvalError::Runtime(
            "expected a boolean but got something else".into(),
        ))
    }

    pub fn from(value: serde_json::Value, _tpe: &Type) -> QueryValue {
        match value {
            serde_json::Value::Null => QueryValue::Null,
            serde_json::Value::Bool(b) => QueryValue::Bool(b),
            serde_json::Value::Number(number) => {
                QueryValue::Number(number.as_f64().expect("we don't use arbitrary precision"))
            }
            serde_json::Value::String(s) => QueryValue::String(s.into()),
            serde_json::Value::Array(values) => {
                let values = values
                    .into_iter()
                    .map(|v| Self::from(v, _tpe))
                    .collect::<Vec<_>>();

                QueryValue::Array(values.into())
            }
            serde_json::Value::Object(map) => {
                let mut props = BTreeMap::new();
                for (name, value) in map {
                    props.insert(name, Self::from(value, _tpe));
                }

                QueryValue::Record(props)
            }
        }
    }

    pub fn build_from_type_expectation(value: serde_json::Value, expectation: &Type) -> QueryValue {
        match expectation {
            Type::Unspecified => Self::from(value, expectation),
            Type::Number => {
                if let serde_json::Value::Number(n) = value {
                    QueryValue::Number(n.as_f64().expect("we don't use arbitrary precision"))
                } else {
                    QueryValue::Null
                }
            }
            Type::String | Type::Subject => {
                if let serde_json::Value::String(s) = value {
                    QueryValue::String(s.into())
                } else {
                    QueryValue::Null
                }
            }
            Type::Bool => {
                if let serde_json::Value::Bool(b) = value {
                    QueryValue::Bool(b)
                } else {
                    QueryValue::Null
                }
            }
            Type::Array(tpe) => {
                if let serde_json::Value::Array(values) = value {
                    let values = values
                        .into_iter()
                        .map(|v| Self::build_from_type_expectation(v, tpe.as_ref()))
                        .collect();

                    QueryValue::Array(values)
                } else {
                    QueryValue::Null
                }
            }
            Type::Record(map) => {
                if let serde_json::Value::Object(mut values) = value {
                    let mut props = BTreeMap::new();

                    for (name, tpe) in map.iter() {
                        let value = if let Some(value) = values.remove(name) {
                            Self::build_from_type_expectation(value, tpe)
                        } else {
                            QueryValue::Null
                        };

                        // TODO - we might just not insert the value if not present, sparing the clone allocation
                        props.insert(name.clone(), value);
                    }

                    QueryValue::Record(props)
                } else {
                    QueryValue::Null
                }
            }

            // this one is unlikely because the user cannot expect a function at that level
            Type::App {
                args: _x,
                result: _y,
                aggregate: _z,
            } => todo!("use a proper result type so we can track it if it happens in real life"),

            Type::Date => {
                if let serde_json::Value::String(s) = value
                    && let Ok(date) = s.parse::<NaiveDate>()
                {
                    QueryValue::Date(date)
                } else {
                    QueryValue::Null
                }
            }

            Type::Time => {
                if let serde_json::Value::String(s) = value
                    && let Ok(time) = s.parse::<NaiveTime>()
                {
                    QueryValue::Time(time)
                } else {
                    QueryValue::Null
                }
            }

            Type::DateTime => {
                if let serde_json::Value::String(s) = value
                    && let Ok(date_time) = s.parse::<DateTime<Utc>>()
                {
                    QueryValue::DateTime(date_time)
                } else {
                    QueryValue::Null
                }
            }

            // currenlty we don't custom type but will change
            Type::Custom(_) => QueryValue::Null,
        }
    }
}

fn evaluate_agg_value<'a>(
    options: &AnalysisOptions,
    env: &HashMap<&'a str, QueryValue>,
    value: &'a eventql_parser::Value,
    state: &mut AggState,
) -> EvalResult<()> {
    match (state, value) {
        (AggState::Single(agg), eventql_parser::Value::App(app)) => {
            let mut args = Vec::with_capacity(app.args.len());

            for arg in &app.args {
                args.push(evaluate_value(options, env, &arg.value)?);
            }

            agg.fold(args.as_slice());

            Ok(())
        }

        (AggState::Single(agg), value) => {
            let value = evaluate_value(options, env, value)?;

            agg.fold(&[value]);

            Ok(())
        }

        (AggState::Record(aggs), eventql_parser::Value::Record(props)) => {
            for prop in props {
                if let Some(agg) = aggs.get_mut(prop.name.as_str()) {
                    evaluate_agg_value(options, env, &prop.value.value, agg)?;
                    continue;
                }

                return Err(EvalError::Runtime("tagged aggregate not found".into()));
            }

            Ok(())
        }

        _ => Err(EvalError::Runtime(
            "invalid aggregate evaluation code path".into(),
        )),
    }
}

fn evaluate_value<'a>(
    options: &AnalysisOptions,
    env: &HashMap<&'a str, QueryValue>,
    value: &'a eventql_parser::Value,
) -> EvalResult<QueryValue> {
    match value {
        eventql_parser::Value::Number(n) => Ok(QueryValue::Number(*n)),
        eventql_parser::Value::String(s) => Ok(QueryValue::String(s.clone())),
        eventql_parser::Value::Bool(b) => Ok(QueryValue::Bool(*b)),
        eventql_parser::Value::Id(id) => env
            .get(id.as_str())
            .cloned()
            .ok_or_else(|| EvalError::Runtime(format!("undefined identifier: {}", id).into())),
        eventql_parser::Value::Array(exprs) => {
            let mut arr = Vec::with_capacity(exprs.capacity());

            for expr in exprs {
                arr.push(evaluate_value(options, env, &expr.value)?);
            }

            Ok(QueryValue::Array(arr))
        }

        eventql_parser::Value::Record(fields) => {
            let mut record = BTreeMap::new();

            for field in fields {
                record.insert(
                    field.name.clone(),
                    evaluate_value(options, env, &field.value.value)?,
                );
            }

            Ok(QueryValue::Record(record))
        }

        eventql_parser::Value::Access(access) => {
            match evaluate_value(options, env, &access.target.value)? {
                QueryValue::Record(rec) => Ok(rec
                    .get(access.field.as_str())
                    .cloned()
                    .unwrap_or(QueryValue::Null)),

                _ => Err(EvalError::Runtime(
                    "expected a record for field access".into(),
                )),
            }
        }

        eventql_parser::Value::App(app) => {
            let mut args = Vec::with_capacity(app.args.capacity());

            for arg in &app.args {
                args.push(evaluate_value(options, env, &arg.value)?);
            }

            // -------------
            // Math functions
            // ------------

            if app.func.eq_ignore_ascii_case("abs")
                && let QueryValue::Number(n) = &args[0]
            {
                return Ok(QueryValue::Number(n.abs()));
            }

            if app.func.eq_ignore_ascii_case("ceil")
                && let QueryValue::Number(n) = &args[0]
            {
                return Ok(QueryValue::Number(n.ceil()));
            }

            if app.func.eq_ignore_ascii_case("floor")
                && let QueryValue::Number(n) = &args[0]
            {
                return Ok(QueryValue::Number(n.floor()));
            }

            if app.func.eq_ignore_ascii_case("floor")
                && let QueryValue::Number(n) = &args[0]
            {
                return Ok(QueryValue::Number(n.round()));
            }

            if app.func.eq_ignore_ascii_case("cos")
                && let QueryValue::Number(n) = &args[0]
            {
                return Ok(QueryValue::Number(n.cos()));
            }

            if app.func.eq_ignore_ascii_case("sin")
                && let QueryValue::Number(n) = &args[0]
            {
                return Ok(QueryValue::Number(n.sin()));
            }

            if app.func.eq_ignore_ascii_case("tan")
                && let QueryValue::Number(n) = &args[0]
            {
                return Ok(QueryValue::Number(n.tan()));
            }

            if app.func.eq_ignore_ascii_case("exp")
                && let QueryValue::Number(n) = &args[0]
            {
                return Ok(QueryValue::Number(n.exp()));
            }

            if app.func.eq_ignore_ascii_case("pow")
                && let QueryValue::Number(x) = &args[0]
                && let QueryValue::Number(y) = &args[1]
            {
                return Ok(QueryValue::Number(x.powi(*y as i32)));
            }

            if app.func.eq_ignore_ascii_case("sqrt")
                && let QueryValue::Number(n) = &args[0]
            {
                return Ok(QueryValue::Number(n.sqrt()));
            }

            if app.func.eq_ignore_ascii_case("rand") {
                let mut rng = rand::rng();
                return Ok(QueryValue::Number(rng.random()));
            }

            if app.func.eq_ignore_ascii_case("pi") {
                return Ok(QueryValue::Number(f64::consts::PI));
            }

            // ------------
            // String functions
            // ------------

            if app.func.eq_ignore_ascii_case("lower")
                && let QueryValue::String(s) = &args[0]
            {
                return Ok(QueryValue::String(s.to_lowercase().into()));
            }

            if app.func.eq_ignore_ascii_case("upper")
                && let QueryValue::String(s) = &args[0]
            {
                return Ok(QueryValue::String(s.to_uppercase().into()));
            }

            if app.func.eq_ignore_ascii_case("trim")
                && let QueryValue::String(s) = &args[0]
            {
                return Ok(QueryValue::String(s.trim().to_owned().into()));
            }

            if app.func.eq_ignore_ascii_case("ltrim")
                && let QueryValue::String(s) = &args[0]
            {
                return Ok(QueryValue::String(s.trim_start().to_owned().into()));
            }

            if app.func.eq_ignore_ascii_case("rtrim")
                && let QueryValue::String(s) = &args[0]
            {
                return Ok(QueryValue::String(s.trim_end().to_owned().into()));
            }

            if app.func.eq_ignore_ascii_case("len")
                && let QueryValue::String(s) = &args[0]
            {
                return Ok(QueryValue::Number(s.len() as f64));
            }

            if app.func.eq_ignore_ascii_case("instr")
                && let QueryValue::String(x) = &args[0]
                && let QueryValue::String(y) = &args[1]
            {
                return Ok(QueryValue::Number(
                    x.find(y).map(|i| i + 1).unwrap_or_default() as f64,
                ));
            }

            if app.func.eq_ignore_ascii_case("substring")
                && let QueryValue::String(s) = &args[0]
                && let QueryValue::Number(start) = &args[1]
                && let QueryValue::Number(length) = &args[2]
            {
                let start = *start as usize;
                let length = *length as usize;

                return Ok(QueryValue::String(
                    s.chars().skip(start).take(length).collect(),
                ));
            }

            if app.func.eq_ignore_ascii_case("replace")
                && let QueryValue::String(x) = &args[0]
                && let QueryValue::String(y) = &args[1]
                && let QueryValue::String(z) = &args[2]
            {
                return Ok(QueryValue::String(x.replace(y, z).into()));
            }

            if app.func.eq_ignore_ascii_case("startswith")
                && let QueryValue::String(x) = &args[0]
                && let QueryValue::String(y) = &args[1]
            {
                return Ok(QueryValue::Bool(x.starts_with(y)));
            }

            if app.func.eq_ignore_ascii_case("endswith")
                && let QueryValue::String(x) = &args[0]
                && let QueryValue::String(y) = &args[1]
            {
                return Ok(QueryValue::Bool(x.ends_with(y)));
            }

            // -------------
            // Date and Time functions
            // -------------

            if app.func.eq_ignore_ascii_case("now") {
                return Ok(QueryValue::DateTime(Utc::now()));
            }

            if app.func.eq_ignore_ascii_case("year") {
                return match &args[0] {
                    QueryValue::DateTime(t) => Ok(QueryValue::Number(t.year() as f64)),
                    QueryValue::Date(d) => Ok(QueryValue::Number(d.year() as f64)),
                    _ => Err(EvalError::Runtime(
                        "year() requires a DateTime or Date argument".into(),
                    )),
                };
            }

            if app.func.eq_ignore_ascii_case("month") {
                return match &args[0] {
                    QueryValue::DateTime(t) => Ok(QueryValue::Number(t.month() as f64)),
                    QueryValue::Date(d) => Ok(QueryValue::Number(d.month() as f64)),
                    _ => Err(EvalError::Runtime(
                        "month() requires a DateTime or Date argument".into(),
                    )),
                };
            }

            if app.func.eq_ignore_ascii_case("day") {
                return match &args[0] {
                    QueryValue::DateTime(t) => Ok(QueryValue::Number(t.day() as f64)),
                    QueryValue::Date(d) => Ok(QueryValue::Number(d.day() as f64)),
                    _ => Err(EvalError::Runtime(
                        "day() requires a DateTime or Date argument".into(),
                    )),
                };
            }

            if app.func.eq_ignore_ascii_case("hour") {
                return match &args[0] {
                    QueryValue::DateTime(t) => Ok(QueryValue::Number(t.hour() as f64)),
                    QueryValue::Time(t) => Ok(QueryValue::Number(t.hour() as f64)),
                    _ => Err(EvalError::Runtime(
                        "hour() requires a DateTime or Time argument".into(),
                    )),
                };
            }

            if app.func.eq_ignore_ascii_case("minute") {
                return match &args[0] {
                    QueryValue::DateTime(t) => Ok(QueryValue::Number(t.minute() as f64)),
                    QueryValue::Time(t) => Ok(QueryValue::Number(t.minute() as f64)),
                    _ => Err(EvalError::Runtime(
                        "minute() requires a DateTime or Time argument".into(),
                    )),
                };
            }

            if app.func.eq_ignore_ascii_case("weekday") {
                return match &args[0] {
                    QueryValue::DateTime(t) => {
                        Ok(QueryValue::Number(t.weekday().num_days_from_sunday() as f64))
                    }
                    QueryValue::Date(d) => {
                        Ok(QueryValue::Number(d.weekday().num_days_from_sunday() as f64))
                    }
                    _ => Err(EvalError::Runtime(
                        "weekday() requires a DateTime or Date argument".into(),
                    )),
                };
            }

            // --------------
            // Conditional functions
            // --------------

            if app.func.eq_ignore_ascii_case("if")
                && let QueryValue::Bool(b) = args[0]
            {
                // TODO - cloning is not necessary here as we could evaluate args lazily but that'll do for now
                return Ok(if b { args[1].clone() } else { args[2].clone() });
            }

            Err(EvalError::Runtime(
                format!("unknown function or invalid arguments: {}", app.func).into(),
            ))
        }

        eventql_parser::Value::Binary(binary) => {
            let lhs = evaluate_value(options, env, &binary.lhs.value)?;

            if let Operator::As = binary.operator
                && let eventql_parser::Value::Id(tpe) = &binary.rhs.value
            {
                let tpe = eventql_parser::prelude::name_to_type(options, tpe)
                    .ok_or_else(|| EvalError::Runtime(format!("unknown type: {}", tpe).into()))?;

                return type_conversion(&lhs, tpe);
            }

            let rhs = evaluate_value(options, env, &binary.rhs.value)?;

            evaluate_binary_operation(binary.operator, &lhs, &rhs)
        }

        eventql_parser::Value::Unary(unary) => match unary.operator {
            Operator::Add => {
                if let QueryValue::Number(n) = evaluate_value(options, env, &unary.expr.value)? {
                    Ok(QueryValue::Number(n))
                } else {
                    Err(EvalError::Runtime(
                        "unary + operator requires a number".into(),
                    ))
                }
            }

            Operator::Sub => {
                if let QueryValue::Number(n) = evaluate_value(options, env, &unary.expr.value)? {
                    Ok(QueryValue::Number(-n))
                } else {
                    Err(EvalError::Runtime(
                        "unary - operator requires a number".into(),
                    ))
                }
            }

            Operator::Not => {
                if let QueryValue::Bool(b) = evaluate_value(options, env, &unary.expr.value)? {
                    Ok(QueryValue::Bool(!b))
                } else {
                    Err(EvalError::Runtime(
                        "unary ! operator requires a boolean".into(),
                    ))
                }
            }

            _ => Err(EvalError::Runtime(
                format!("unsupported unary operator: {:?}", unary.operator).into(),
            )),
        },

        eventql_parser::Value::Group(expr) => evaluate_value(options, env, &expr.value),
    }
}

/// Many runtime error and most can be caught during static analysis.
fn type_conversion<'a>(value: &QueryValue, tpe: eventql_parser::Type) -> EvalResult<QueryValue> {
    match value {
        QueryValue::Null => Ok(QueryValue::Null),

        QueryValue::String(cow) => match tpe {
            eventql_parser::Type::String | eventql_parser::Type::Subject => {
                Ok(QueryValue::String(cow.clone()))
            }
            _ => Err(EvalError::Runtime(
                format!("cannot convert String to {tpe}").into(),
            )),
        },

        QueryValue::Number(n) => match tpe {
            eventql_parser::Type::Number => Ok(QueryValue::Number(*n)),
            eventql_parser::Type::String => Ok(QueryValue::String(n.to_string().into())),
            _ => Err(EvalError::Runtime(
                format!("cannot convert Number to {tpe}").into(),
            )),
        },

        QueryValue::Bool(b) => match tpe {
            eventql_parser::Type::String => Ok(QueryValue::String(b.to_string().into())),
            eventql_parser::Type::Bool => Ok(QueryValue::Bool(*b)),
            _ => Err(EvalError::Runtime(
                format!("cannot convert Bool to {tpe}").into(),
            )),
        },

        QueryValue::Record(_) => Err(EvalError::Runtime("cannot convert Record".into())),
        QueryValue::Array(_) => Err(EvalError::Runtime("cannot convert Array".into())),

        QueryValue::DateTime(date_time) => match tpe {
            eventql_parser::Type::String => Ok(QueryValue::String(date_time.to_string().into())),
            eventql_parser::Type::Date => Ok(QueryValue::Date(date_time.date_naive())),
            eventql_parser::Type::Time => Ok(QueryValue::Time(date_time.time())),
            eventql_parser::Type::DateTime => Ok(QueryValue::DateTime(*date_time)),
            _ => Err(EvalError::Runtime(
                format!("cannot convert DateTime to {tpe}").into(),
            )),
        },

        QueryValue::Date(naive_date) => match tpe {
            eventql_parser::Type::String => Ok(QueryValue::String(naive_date.to_string().into())),
            eventql_parser::Type::Date => Ok(QueryValue::Date(*naive_date)),
            _ => Err(EvalError::Runtime(
                format!("cannot convert Date to {tpe}").into(),
            )),
        },

        QueryValue::Time(naive_time) => match tpe {
            eventql_parser::Type::String => Ok(QueryValue::String(naive_time.to_string().into())),
            eventql_parser::Type::Time => Ok(QueryValue::Time(*naive_time)),
            _ => Err(EvalError::Runtime(
                format!("cannot convert Time to {tpe}").into(),
            )),
        },
    }
}

fn evaluate_binary_operation<'a>(
    op: Operator,
    a: &QueryValue,
    b: &QueryValue,
) -> EvalResult<QueryValue> {
    match (a, b) {
        (QueryValue::Null, QueryValue::Null) => Ok(QueryValue::Null),

        (QueryValue::String(a), QueryValue::String(b)) => match op {
            Operator::Eq => Ok(QueryValue::Bool(a == b)),
            Operator::Neq => Ok(QueryValue::Bool(a != b)),
            Operator::Lt => Ok(QueryValue::Bool(a < b)),
            Operator::Lte => Ok(QueryValue::Bool(a <= b)),
            Operator::Gt => Ok(QueryValue::Bool(a > b)),
            Operator::Gte => Ok(QueryValue::Bool(a >= b)),
            _ => Err(EvalError::Runtime(
                format!("unsupported operator {op} for String").into(),
            )),
        },

        (QueryValue::Number(a), QueryValue::Number(b)) => match op {
            Operator::Add => Ok(QueryValue::Number(a + b)),
            Operator::Sub => Ok(QueryValue::Number(a - b)),
            Operator::Mul => Ok(QueryValue::Number(a * b)),
            Operator::Div => Ok(QueryValue::Number(a / b)),
            Operator::Eq => Ok(QueryValue::Bool(
                a.partial_cmp(b)
                    .map(|o| matches!(o, Ordering::Equal))
                    .unwrap_or_default(),
            )),
            Operator::Neq => Ok(QueryValue::Bool(
                a.partial_cmp(b)
                    .map(|o| !matches!(o, Ordering::Equal))
                    .unwrap_or_default(),
            )),
            Operator::Lt => Ok(QueryValue::Bool(a < b)),
            Operator::Lte => Ok(QueryValue::Bool(a <= b)),
            Operator::Gt => Ok(QueryValue::Bool(a > b)),
            Operator::Gte => Ok(QueryValue::Bool(a >= b)),
            _ => Err(EvalError::Runtime(
                format!("unsupported operator {op} for Number").into(),
            )),
        },

        (QueryValue::Bool(a), QueryValue::Bool(b)) => match op {
            Operator::Eq => Ok(QueryValue::Bool(a == b)),
            Operator::Neq => Ok(QueryValue::Bool(a != b)),
            Operator::Lt => Ok(QueryValue::Bool(a < b)),
            Operator::Lte => Ok(QueryValue::Bool(a <= b)),
            Operator::Gt => Ok(QueryValue::Bool(a > b)),
            Operator::Gte => Ok(QueryValue::Bool(a >= b)),
            Operator::And => Ok(QueryValue::Bool(*a && *b)),
            Operator::Or => Ok(QueryValue::Bool(*a || *b)),
            Operator::Xor => Ok(QueryValue::Bool(*a ^ *b)),
            _ => Err(EvalError::Runtime(
                format!("unsupported operator {op} for Bool").into(),
            )),
        },

        (this @ QueryValue::Record(a), that @ QueryValue::Record(b)) => match op {
            Operator::Eq => {
                if a.len() != b.len() {
                    return Ok(QueryValue::Bool(false));
                }

                for ((a_k, a_v), (b_k, b_v)) in a.iter().zip(b.iter()) {
                    if a_k != b_k || evaluate_binary_operation(Operator::Eq, a_v, b_v)?.as_bool()? {
                        return Ok(QueryValue::Bool(false));
                    }
                }

                Ok(QueryValue::Bool(true))
            }

            Operator::Neq => Ok(QueryValue::Bool(
                !evaluate_binary_operation(Operator::Eq, this, that)?.as_bool()?,
            )),

            _ => Err(EvalError::Runtime(
                format!("unsupported operator {op} for Record").into(),
            )),
        },

        (this @ QueryValue::Array(a), that @ QueryValue::Array(b)) => match op {
            Operator::Eq => {
                if a.len() != b.len() {
                    return Ok(QueryValue::Bool(false));
                }

                for (a, b) in a.iter().zip(b.iter()) {
                    if !evaluate_binary_operation(Operator::Eq, a, b)?.as_bool()? {
                        return Ok(QueryValue::Bool(false));
                    }
                }

                Ok(QueryValue::Bool(true))
            }

            Operator::Neq => Ok(QueryValue::Bool(
                !evaluate_binary_operation(Operator::Eq, this, that)?.as_bool()?,
            )),

            _ => Err(EvalError::Runtime(
                format!("unsupported operator {op} for Array").into(),
            )),
        },

        (QueryValue::DateTime(a), QueryValue::DateTime(b)) => match op {
            Operator::Eq => Ok(QueryValue::Bool(a == b)),
            Operator::Neq => Ok(QueryValue::Bool(a != b)),
            Operator::Lt => Ok(QueryValue::Bool(a < b)),
            Operator::Lte => Ok(QueryValue::Bool(a <= b)),
            Operator::Gt => Ok(QueryValue::Bool(a > b)),
            Operator::Gte => Ok(QueryValue::Bool(a >= b)),
            _ => Err(EvalError::Runtime(
                format!("unsupported operator {op} for DateTime").into(),
            )),
        },

        (QueryValue::Date(a), QueryValue::Date(b)) => match op {
            Operator::Eq => Ok(QueryValue::Bool(a == b)),
            Operator::Neq => Ok(QueryValue::Bool(a != b)),
            Operator::Lt => Ok(QueryValue::Bool(a < b)),
            Operator::Lte => Ok(QueryValue::Bool(a <= b)),
            Operator::Gt => Ok(QueryValue::Bool(a > b)),
            Operator::Gte => Ok(QueryValue::Bool(a >= b)),
            _ => Err(EvalError::Runtime(
                format!("unsupported operator {op} for Date").into(),
            )),
        },

        (QueryValue::Time(a), QueryValue::Time(b)) => match op {
            Operator::Eq => Ok(QueryValue::Bool(a == b)),
            Operator::Neq => Ok(QueryValue::Bool(a != b)),
            Operator::Lt => Ok(QueryValue::Bool(a < b)),
            Operator::Lte => Ok(QueryValue::Bool(a <= b)),
            Operator::Gt => Ok(QueryValue::Bool(a > b)),
            Operator::Gte => Ok(QueryValue::Bool(a >= b)),
            _ => Err(EvalError::Runtime(
                format!("unsupported operator {op} for Time").into(),
            )),
        },

        (QueryValue::Array(values), value) if matches!(op, Operator::Contains) => {
            for a in values.iter() {
                if evaluate_binary_operation(Operator::Eq, a, value)?.as_bool()? {
                    return Ok(QueryValue::Bool(true));
                }
            }
            Ok(QueryValue::Bool(false))
        }

        _ => Err(EvalError::Runtime(
            format!("unsupported binary operation {op} for given types").into(),
        )),
    }
}

fn evaluate_predicate<'a>(
    options: &AnalysisOptions,
    env: &HashMap<&'a str, QueryValue>,
    value: &'a eventql_parser::Value,
) -> EvalResult<bool> {
    evaluate_value(options, env, value)?.as_bool()
}

fn catalog<'a>(db: &'a Db, options: &'a AnalysisOptions, query: &'a Query<Typed>) -> Row<'a> {
    let mut srcs = Sources::default();
    for query_src in &query.sources {
        match &query_src.kind {
            eventql_parser::SourceKind::Name(name) => {
                if name.eq_ignore_ascii_case("events")
                    && let Some(tpe) = query
                        .meta
                        .scope
                        .entries
                        .get(query_src.binding.name.as_str())
                {
                    srcs.insert(
                        &query_src.binding.name,
                        Box::new(db.events.iter().map(|e| Ok(e.project(tpe)))),
                    );

                    continue;
                }

                srcs.insert(&query_src.binding.name, Box::new(iter::empty()));
            }

            eventql_parser::SourceKind::Subject(path) => {
                if let Some(tpe) = query
                    .meta
                    .scope
                    .entries
                    .get(query_src.binding.name.as_str())
                {
                    srcs.insert(
                        &query_src.binding.name,
                        Box::new(db.iter_subject(path).map(|e| Ok(e.project(tpe)))),
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

    if query.meta.aggregate {
        Box::new(AggQuery::new(srcs, options, query))
    } else {
        Box::new(EventQuery::new(srcs, options, query))
    }
}
