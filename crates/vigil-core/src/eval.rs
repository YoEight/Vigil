use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    f64,
};

use chrono::{Datelike, Timelike, Utc};
use eventql_parser::prelude::{Operator, Typed};
use eventql_parser::{Query, Session, StrRef};
use rand::Rng;
use serde::Serialize;
use thiserror::Error;

use crate::values::QueryValue;

#[derive(Debug, Error, Serialize)]
pub enum EvalError {
    #[error("runtime error: {0}")]
    Runtime(Cow<'static, str>),
}

pub type EvalResult<A> = std::result::Result<A, EvalError>;

impl QueryValue {
    fn as_bool(&self) -> EvalResult<bool> {
        if let Self::Bool(b) = self {
            return Ok(*b);
        }

        Err(EvalError::Runtime(
            "expected a boolean but got something else".into(),
        ))
    }
}

pub struct Interpreter<'a> {
    pub(crate) session: &'a Session,
    env: HashMap<StrRef, QueryValue>,
}

impl<'a> Interpreter<'a> {
    pub fn new(session: &'a Session) -> Self {
        Self {
            session,
            env: Default::default(),
        }
    }

    pub fn env_mut(&mut self) -> &mut HashMap<StrRef, QueryValue> {
        self.env.clear();
        &mut self.env
    }

    fn lookup(&self, id: StrRef) -> EvalResult<QueryValue> {
        self.env.get(&id).cloned().ok_or_else(|| {
            let ident = self.session.arena().get_str(id);
            EvalError::Runtime(format!("undefined identifier: {ident}").into())
        })
    }

    fn coerce(&self, value: &QueryValue, tpe: eventql_parser::Type) -> EvalResult<QueryValue> {
        match value {
            QueryValue::Null => Ok(QueryValue::Null),

            QueryValue::String(cow) => match tpe {
                eventql_parser::Type::String | eventql_parser::Type::Subject => {
                    Ok(QueryValue::String(cow.clone()))
                }
                _ => Err(EvalError::Runtime(
                    format!(
                        "cannot convert String to {}",
                        self.session.display_type(tpe)
                    )
                    .into(),
                )),
            },

            QueryValue::Number(n) => match tpe {
                eventql_parser::Type::Number => Ok(QueryValue::Number(*n)),
                eventql_parser::Type::String => Ok(QueryValue::String(n.to_string())),
                _ => Err(EvalError::Runtime(
                    format!(
                        "cannot convert Number to {}",
                        self.session.display_type(tpe)
                    )
                    .into(),
                )),
            },

            QueryValue::Bool(b) => match tpe {
                eventql_parser::Type::String => Ok(QueryValue::String(b.to_string())),
                eventql_parser::Type::Bool => Ok(QueryValue::Bool(*b)),
                _ => Err(EvalError::Runtime(
                    format!("cannot convert Bool to {}", self.session.display_type(tpe)).into(),
                )),
            },

            QueryValue::Record(_) => Err(EvalError::Runtime("cannot convert Record".into())),
            QueryValue::Array(_) => Err(EvalError::Runtime("cannot convert Array".into())),

            QueryValue::DateTime(date_time) => match tpe {
                eventql_parser::Type::String => Ok(QueryValue::String(date_time.to_string())),
                eventql_parser::Type::Date => Ok(QueryValue::Date(date_time.date_naive())),
                eventql_parser::Type::Time => Ok(QueryValue::Time(date_time.time())),
                eventql_parser::Type::DateTime => Ok(QueryValue::DateTime(*date_time)),
                _ => Err(EvalError::Runtime(
                    format!(
                        "cannot convert DateTime to {}",
                        self.session.display_type(tpe)
                    )
                    .into(),
                )),
            },

            QueryValue::Date(naive_date) => match tpe {
                eventql_parser::Type::String => Ok(QueryValue::String(naive_date.to_string())),
                eventql_parser::Type::Date => Ok(QueryValue::Date(*naive_date)),
                _ => Err(EvalError::Runtime(
                    format!("cannot convert Date to {}", self.session.display_type(tpe)).into(),
                )),
            },

            QueryValue::Time(naive_time) => match tpe {
                eventql_parser::Type::String => Ok(QueryValue::String(naive_time.to_string())),
                eventql_parser::Type::Time => Ok(QueryValue::Time(*naive_time)),
                _ => Err(EvalError::Runtime(
                    format!("cannot convert Time to {}", self.session.display_type(tpe)).into(),
                )),
            },
        }
    }

    pub fn eval_binary(
        &self,
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
                Operator::Eq => Ok(QueryValue::Bool(a == b)),
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
                        if a_k != b_k || self.eval_binary(Operator::Eq, a_v, b_v)?.as_bool()? {
                            return Ok(QueryValue::Bool(false));
                        }
                    }

                    Ok(QueryValue::Bool(true))
                }

                Operator::Neq => Ok(QueryValue::Bool(
                    !self.eval_binary(Operator::Eq, this, that)?.as_bool()?,
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
                        if !self.eval_binary(Operator::Eq, a, b)?.as_bool()? {
                            return Ok(QueryValue::Bool(false));
                        }
                    }

                    Ok(QueryValue::Bool(true))
                }

                Operator::Neq => Ok(QueryValue::Bool(
                    !self.eval_binary(Operator::Eq, this, that)?.as_bool()?,
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
                    if self.eval_binary(Operator::Eq, a, value)?.as_bool()? {
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

    pub fn eval_unary(&self, operator: Operator, value: &QueryValue) -> EvalResult<QueryValue> {
        match operator {
            Operator::Add => {
                if let QueryValue::Number(n) = value {
                    Ok(QueryValue::Number(*n))
                } else {
                    Err(EvalError::Runtime(
                        "unary + operator requires a number".into(),
                    ))
                }
            }

            Operator::Sub => {
                if let QueryValue::Number(n) = value {
                    Ok(QueryValue::Number(-n))
                } else {
                    Err(EvalError::Runtime(
                        "unary - operator requires a number".into(),
                    ))
                }
            }

            Operator::Not => {
                if let QueryValue::Bool(b) = value {
                    Ok(QueryValue::Bool(!b))
                } else {
                    Err(EvalError::Runtime(
                        "unary ! operator requires a boolean".into(),
                    ))
                }
            }

            _ => Err(EvalError::Runtime(
                format!("unsupported unary operator: {:?}", operator).into(),
            )),
        }
    }

    pub fn eval_predicate(&self, query: &Query<Typed>) -> EvalResult<bool> {
        if let Some(predicate) = query.predicate.as_ref().copied() {
            return self.eval_expr(predicate)?.as_bool();
        }

        Ok(true)
    }

    pub fn eval_expr(&self, expr: eventql_parser::ExprRef) -> EvalResult<QueryValue> {
        self.eval(self.session.arena().get_expr(expr).value)
    }

    pub fn eval(&self, value: eventql_parser::Value) -> EvalResult<QueryValue> {
        match value {
            eventql_parser::Value::Number(n) => Ok(QueryValue::Number(n)),
            eventql_parser::Value::String(s) => Ok(QueryValue::String(
                self.session.arena().get_str(s).to_owned(),
            )),
            eventql_parser::Value::Bool(b) => Ok(QueryValue::Bool(b)),
            eventql_parser::Value::Id(id) => self.lookup(id),
            eventql_parser::Value::Array(exprs) => {
                let exprs = self.session.arena().get_vec(exprs);
                let mut arr = Vec::with_capacity(exprs.len());

                for expr in exprs {
                    arr.push(self.eval(self.session.arena().get_expr(*expr).value)?);
                }

                Ok(QueryValue::Array(arr))
            }

            eventql_parser::Value::Record(fields) => {
                let fields = self.session.arena().get_rec(fields);
                let mut record = BTreeMap::new();

                for field in fields {
                    record.insert(
                        self.session.arena().get_str(field.name).to_owned(),
                        self.eval(self.session.arena().get_expr(field.expr).value)?,
                    );
                }

                Ok(QueryValue::Record(record))
            }

            eventql_parser::Value::Access(access) => {
                match self.eval(self.session.arena().get_expr(access.target).value)? {
                    QueryValue::Record(rec) => Ok(rec
                        .get(self.session.arena().get_str(access.field))
                        .cloned()
                        .unwrap_or(QueryValue::Null)),

                    _ => Err(EvalError::Runtime(
                        "expected a record for field access".into(),
                    )),
                }
            }

            eventql_parser::Value::App(app) => {
                let fun_args = self.session.arena().get_vec(app.args);
                let mut args = Vec::with_capacity(fun_args.len());

                for arg in fun_args {
                    args.push(self.eval(self.session.arena().get_expr(*arg).value)?);
                }

                let fun_name = self.session.arena().get_str(app.func);
                // -------------
                // Math functions
                // ------------

                if fun_name.eq_ignore_ascii_case("abs")
                    && let QueryValue::Number(n) = &args[0]
                {
                    return Ok(QueryValue::Number(n.abs().into()));
                }

                if fun_name.eq_ignore_ascii_case("ceil")
                    && let QueryValue::Number(n) = &args[0]
                {
                    return Ok(QueryValue::Number(n.ceil().into()));
                }

                if fun_name.eq_ignore_ascii_case("floor")
                    && let QueryValue::Number(n) = &args[0]
                {
                    return Ok(QueryValue::Number(n.floor().into()));
                }

                if fun_name.eq_ignore_ascii_case("floor")
                    && let QueryValue::Number(n) = &args[0]
                {
                    return Ok(QueryValue::Number(n.round().into()));
                }

                if fun_name.eq_ignore_ascii_case("cos")
                    && let QueryValue::Number(n) = &args[0]
                {
                    return Ok(QueryValue::Number(n.cos().into()));
                }

                if fun_name.eq_ignore_ascii_case("sin")
                    && let QueryValue::Number(n) = &args[0]
                {
                    return Ok(QueryValue::Number(n.sin().into()));
                }

                if fun_name.eq_ignore_ascii_case("tan")
                    && let QueryValue::Number(n) = &args[0]
                {
                    return Ok(QueryValue::Number(n.tan().into()));
                }

                if fun_name.eq_ignore_ascii_case("exp")
                    && let QueryValue::Number(n) = &args[0]
                {
                    return Ok(QueryValue::Number(n.exp().into()));
                }

                if fun_name.eq_ignore_ascii_case("pow")
                    && let QueryValue::Number(x) = &args[0]
                    && let QueryValue::Number(y) = &args[1]
                {
                    return Ok(QueryValue::Number(x.powi(y.0 as i32).into()));
                }

                if fun_name.eq_ignore_ascii_case("sqrt")
                    && let QueryValue::Number(n) = &args[0]
                {
                    return Ok(QueryValue::Number(n.sqrt().into()));
                }

                if fun_name.eq_ignore_ascii_case("rand") {
                    let mut rng = rand::rng();
                    return Ok(QueryValue::Number(rng.random::<f64>().into()));
                }

                if fun_name.eq_ignore_ascii_case("pi") {
                    return Ok(QueryValue::Number(f64::consts::PI.into()));
                }

                // ------------
                // String functions
                // ------------

                if fun_name.eq_ignore_ascii_case("lower")
                    && let QueryValue::String(s) = &args[0]
                {
                    return Ok(QueryValue::String(s.to_lowercase()));
                }

                if fun_name.eq_ignore_ascii_case("upper")
                    && let QueryValue::String(s) = &args[0]
                {
                    return Ok(QueryValue::String(s.to_uppercase()));
                }

                if fun_name.eq_ignore_ascii_case("trim")
                    && let QueryValue::String(s) = &args[0]
                {
                    return Ok(QueryValue::String(s.trim().to_owned()));
                }

                if fun_name.eq_ignore_ascii_case("ltrim")
                    && let QueryValue::String(s) = &args[0]
                {
                    return Ok(QueryValue::String(s.trim_start().to_owned()));
                }

                if fun_name.eq_ignore_ascii_case("rtrim")
                    && let QueryValue::String(s) = &args[0]
                {
                    return Ok(QueryValue::String(s.trim_end().to_owned()));
                }

                if fun_name.eq_ignore_ascii_case("len")
                    && let QueryValue::String(s) = &args[0]
                {
                    return Ok(QueryValue::Number((s.len() as f64).into()));
                }

                if fun_name.eq_ignore_ascii_case("instr")
                    && let QueryValue::String(x) = &args[0]
                    && let QueryValue::String(y) = &args[1]
                {
                    return Ok(QueryValue::Number(
                        (x.find(y).map(|i| i + 1).unwrap_or_default() as f64).into(),
                    ));
                }

                if fun_name.eq_ignore_ascii_case("substring")
                    && let QueryValue::String(s) = &args[0]
                    && let QueryValue::Number(start) = &args[1]
                    && let QueryValue::Number(length) = &args[2]
                {
                    let start = start.0 as usize;
                    let length = length.0 as usize;

                    return Ok(QueryValue::String(
                        s.chars().skip(start).take(length).collect(),
                    ));
                }

                if fun_name.eq_ignore_ascii_case("replace")
                    && let QueryValue::String(x) = &args[0]
                    && let QueryValue::String(y) = &args[1]
                    && let QueryValue::String(z) = &args[2]
                {
                    return Ok(QueryValue::String(x.replace(y, z)));
                }

                if fun_name.eq_ignore_ascii_case("startswith")
                    && let QueryValue::String(x) = &args[0]
                    && let QueryValue::String(y) = &args[1]
                {
                    return Ok(QueryValue::Bool(x.starts_with(y)));
                }

                if fun_name.eq_ignore_ascii_case("endswith")
                    && let QueryValue::String(x) = &args[0]
                    && let QueryValue::String(y) = &args[1]
                {
                    return Ok(QueryValue::Bool(x.ends_with(y)));
                }

                // -------------
                // Date and Time functions
                // -------------

                if fun_name.eq_ignore_ascii_case("now") {
                    return Ok(QueryValue::DateTime(Utc::now()));
                }

                if fun_name.eq_ignore_ascii_case("year") {
                    return match &args[0] {
                        QueryValue::DateTime(t) => Ok(QueryValue::Number((t.year() as f64).into())),
                        QueryValue::Date(d) => Ok(QueryValue::Number((d.year() as f64).into())),
                        _ => Err(EvalError::Runtime(
                            "year() requires a DateTime or Date argument".into(),
                        )),
                    };
                }

                if fun_name.eq_ignore_ascii_case("month") {
                    return match &args[0] {
                        QueryValue::DateTime(t) => {
                            Ok(QueryValue::Number((t.month() as f64).into()))
                        }
                        QueryValue::Date(d) => Ok(QueryValue::Number((d.month() as f64).into())),
                        _ => Err(EvalError::Runtime(
                            "month() requires a DateTime or Date argument".into(),
                        )),
                    };
                }

                if fun_name.eq_ignore_ascii_case("day") {
                    return match &args[0] {
                        QueryValue::DateTime(t) => Ok(QueryValue::Number((t.day() as f64).into())),
                        QueryValue::Date(d) => Ok(QueryValue::Number((d.day() as f64).into())),
                        _ => Err(EvalError::Runtime(
                            "day() requires a DateTime or Date argument".into(),
                        )),
                    };
                }

                if fun_name.eq_ignore_ascii_case("hour") {
                    return match &args[0] {
                        QueryValue::DateTime(t) => Ok(QueryValue::Number((t.hour() as f64).into())),
                        QueryValue::Time(t) => Ok(QueryValue::Number((t.hour() as f64).into())),
                        _ => Err(EvalError::Runtime(
                            "hour() requires a DateTime or Time argument".into(),
                        )),
                    };
                }

                if fun_name.eq_ignore_ascii_case("minute") {
                    return match &args[0] {
                        QueryValue::DateTime(t) => {
                            Ok(QueryValue::Number((t.minute() as f64).into()))
                        }
                        QueryValue::Time(t) => Ok(QueryValue::Number((t.minute() as f64).into())),
                        _ => Err(EvalError::Runtime(
                            "minute() requires a DateTime or Time argument".into(),
                        )),
                    };
                }

                if fun_name.eq_ignore_ascii_case("weekday") {
                    return match &args[0] {
                        QueryValue::DateTime(t) => Ok(QueryValue::Number(
                            (t.weekday().num_days_from_sunday() as f64).into(),
                        )),
                        QueryValue::Date(d) => Ok(QueryValue::Number(
                            (d.weekday().num_days_from_sunday() as f64).into(),
                        )),
                        _ => Err(EvalError::Runtime(
                            "weekday() requires a DateTime or Date argument".into(),
                        )),
                    };
                }

                // --------------
                // Conditional functions
                // --------------

                if fun_name.eq_ignore_ascii_case("if")
                    && let QueryValue::Bool(b) = args[0]
                {
                    // TODO - cloning is not necessary here as we could evaluate args lazily but that'll do for now
                    return Ok(if b { args[1].clone() } else { args[2].clone() });
                }

                Err(EvalError::Runtime(
                    format!("unknown function or invalid arguments: {fun_name}").into(),
                ))
            }

            eventql_parser::Value::Binary(binary) => {
                let lhs = self.eval(self.session.arena().get_expr(binary.lhs).value)?;

                if let Operator::As = binary.operator
                    && let eventql_parser::Value::Id(tpe_name) =
                        self.session.arena().get_expr(binary.rhs).value
                {
                    let tpe_name = self.session.arena().get_str(tpe_name);
                    let tpe = self.session.resolve_type(tpe_name).ok_or_else(|| {
                        EvalError::Runtime(format!("unknown type: {tpe_name}").into())
                    })?;

                    return self.coerce(&lhs, tpe);
                }

                let rhs = self.eval(self.session.arena().get_expr(binary.rhs).value)?;

                self.eval_binary(binary.operator, &lhs, &rhs)
            }

            eventql_parser::Value::Unary(unary) => {
                let value = self.session.arena().get_expr(unary.expr).value;
                self.eval_unary(unary.operator, &self.eval(value)?)
            }

            eventql_parser::Value::Group(expr) => {
                self.eval(self.session.arena().get_expr(expr).value)
            }
        }
    }
}
