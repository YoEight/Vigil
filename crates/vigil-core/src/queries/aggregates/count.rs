use crate::{queries::aggregates::Aggregate, values::QueryValue};

#[derive(Default)]
pub struct CountAggregate {
    value: u64,
}

impl Aggregate for CountAggregate {
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
        QueryValue::Number((self.value as f64).into())
    }
}
