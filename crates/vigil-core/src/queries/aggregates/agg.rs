use crate::values::QueryValue;

#[derive(Clone)]
pub enum Agg {
    Avg { count: u64, acc: f64 },
    Count { value: u64 },
    Unique { value: Option<QueryValue> },
}

impl Agg {
    pub fn avg() -> Self {
        Self::Avg {
            count: 0,
            acc: 0f64,
        }
    }

    pub fn count() -> Self {
        Self::Count { value: 0 }
    }

    pub fn unique() -> Self {
        Self::Unique { value: None }
    }
}

impl Agg {
    pub fn fold(&mut self, params: &[QueryValue]) {
        match self {
            Agg::Avg { count, acc } => {
                if let QueryValue::Number(n) = params[0] {
                    *count += 1;
                    *acc += *n;

                    return;
                }

                *acc = f64::NAN;
            }

            Agg::Count { value } => {
                if !params.is_empty() {
                    if let QueryValue::Bool(is_true) = params[0]
                        && is_true
                    {
                        *value += 1;
                    }

                    return;
                }

                *value += 1;
            }

            Agg::Unique { value } => {
                if params.is_empty() || value.is_some() {
                    return;
                }

                *value = Some(params[0].clone());
            }
        }
    }

    pub fn complete(&self) -> QueryValue {
        match self {
            Agg::Avg { count, acc } => {
                if acc.is_nan() {
                    return QueryValue::Number(f64::NAN.into());
                }

                if *count == 0 {
                    QueryValue::Number(0f64.into())
                } else {
                    QueryValue::Number((*acc / *count as f64).into())
                }
            }

            Agg::Count { value } => QueryValue::Number((*value as f64).into()),
            Agg::Unique { value } => value.clone().unwrap_or(QueryValue::Null),
        }
    }
}
