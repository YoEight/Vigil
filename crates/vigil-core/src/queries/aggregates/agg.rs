use crate::values::QueryValue;

#[derive(Clone)]
pub enum Agg {
    Avg { count: u64, acc: f64 },
    Count { value: u64 },
    Unique { value: Option<QueryValue> },
    Sum { acc: f64 },
    Min { value: Option<f64> },
    Max { value: Option<f64> },
    Median { values: Vec<f64> },
    Stddev { count: u64, sum: f64, sum_sq: f64 },
    Variance { count: u64, sum: f64, sum_sq: f64 },
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

    pub fn sum() -> Self {
        Self::Sum { acc: 0f64 }
    }

    pub fn min() -> Self {
        Self::Min { value: None }
    }

    pub fn max() -> Self {
        Self::Max { value: None }
    }

    pub fn median() -> Self {
        Self::Median { values: Vec::new() }
    }

    pub fn stddev() -> Self {
        Self::Stddev {
            count: 0,
            sum: 0f64,
            sum_sq: 0f64,
        }
    }

    pub fn variance() -> Self {
        Self::Variance {
            count: 0,
            sum: 0f64,
            sum_sq: 0f64,
        }
    }

    pub fn fold(&mut self, params: &[QueryValue]) {
        match self {
            Agg::Avg { count, acc } => {
                if !params.is_empty()
                    && let QueryValue::Number(n) = params[0]
                {
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

            Agg::Sum { acc } => {
                if !params.is_empty()
                    && let QueryValue::Number(n) = params[0]
                {
                    *acc += *n;

                    return;
                }

                *acc = f64::NAN;
            }

            Agg::Min { value } => {
                if !params.is_empty()
                    && let QueryValue::Number(n) = params[0]
                {
                    if let Some(current) = value {
                        *current = current.min(*n);
                    } else {
                        *value = Some(*n);
                    }

                    return;
                }

                *value = Some(f64::NAN);
            }

            Agg::Max { value } => {
                if !params.is_empty()
                    && let QueryValue::Number(n) = params[0]
                {
                    if let Some(current) = value {
                        *current = current.max(*n);
                    } else {
                        *value = Some(*n);
                    }

                    return;
                }

                *value = Some(f64::NAN);
            }

            Agg::Median { values } => {
                if !params.is_empty()
                    && let QueryValue::Number(n) = params[0]
                {
                    values.push(*n);
                    return;
                }

                values.push(f64::NAN);
            }

            Agg::Stddev { count, sum, sum_sq } | Agg::Variance { count, sum, sum_sq } => {
                if !params.is_empty()
                    && let QueryValue::Number(n) = params[0]
                {
                    let n = *n;
                    *count += 1;
                    *sum += n;
                    *sum_sq += n * n;

                    return;
                }

                *sum = f64::NAN;
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

            Agg::Sum { acc } => QueryValue::Number((*acc).into()),

            Agg::Min { value } => value
                .map(|v| QueryValue::Number(v.into()))
                .unwrap_or(QueryValue::Null),

            Agg::Max { value } => value
                .map(|v| QueryValue::Number(v.into()))
                .unwrap_or(QueryValue::Null),

            Agg::Median { values } => {
                if values.is_empty() {
                    return QueryValue::Null;
                }

                let mut sorted = values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let mid = sorted.len() / 2;
                let median = if sorted.len() % 2 == 0 {
                    (sorted[mid - 1] + sorted[mid]) / 2.0
                } else {
                    sorted[mid]
                };

                QueryValue::Number(median.into())
            }

            Agg::Stddev { count, sum, sum_sq } => {
                if sum.is_nan() {
                    return QueryValue::Number(f64::NAN.into());
                }

                if *count == 0 {
                    return QueryValue::Null;
                }

                let mean = sum / *count as f64;
                let variance = sum_sq / *count as f64 - mean * mean;

                QueryValue::Number(variance.sqrt().into())
            }

            Agg::Variance { count, sum, sum_sq } => {
                if sum.is_nan() {
                    return QueryValue::Number(f64::NAN.into());
                }

                if *count == 0 {
                    return QueryValue::Null;
                }

                let mean = sum / *count as f64;
                let variance = sum_sq / *count as f64 - mean * mean;

                QueryValue::Number(variance.into())
            }
        }
    }
}
