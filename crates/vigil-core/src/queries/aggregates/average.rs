use crate::{queries::aggregates::Aggregate, values::QueryValue};

#[derive(Default)]
pub struct AverageAggregate {
    count: u64,
    acc: f64,
}

impl Aggregate for AverageAggregate {
    fn fold(&mut self, params: &[QueryValue]) {
        if params.is_empty() {
            return;
        }

        if let QueryValue::Number(n) = params[0] {
            self.count += 1;
            self.acc += *n;

            return;
        }

        self.acc = f64::NAN;
    }

    fn complete(&self) -> QueryValue {
        if self.acc.is_nan() {
            return QueryValue::Number(f64::NAN.into());
        }

        if self.count == 0 {
            QueryValue::Number(0f64.into())
        } else {
            QueryValue::Number((self.acc / self.count as f64).into())
        }
    }
}
