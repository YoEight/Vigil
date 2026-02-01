use crate::{queries::aggregates::Aggregate, values::QueryValue};

#[derive(Default)]
pub struct UniqueAggregate {
    inner: Option<QueryValue>,
}

impl Aggregate for UniqueAggregate {
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
