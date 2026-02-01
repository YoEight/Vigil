use crate::values::QueryValue;
use eventql_parser::Order;
use std::collections::BTreeMap;

pub struct QueryOrderer {
    order: Order,
    order_map: Option<BTreeMap<QueryValue, Vec<QueryValue>>>,
    order_iter: Option<Box<dyn Iterator<Item = Vec<QueryValue>>>>,
    batch_iter: Option<Box<dyn Iterator<Item = QueryValue>>>,
}

impl QueryOrderer {
    pub fn new(order: Order) -> Self {
        Self {
            order,
            order_map: Some(BTreeMap::new()),
            order_iter: None,
            batch_iter: None,
        }
    }

    pub fn insert(&mut self, key: QueryValue, value: QueryValue) {
        if let Some(order_map) = self.order_map.as_mut() {
            order_map.entry(key).or_default().push(value);
        }
    }

    pub fn next(&mut self) -> Option<QueryValue> {
        loop {
            let mut batch = self.batch_iter.take()?;
            if let Some(value) = batch.next() {
                self.batch_iter = Some(batch);
                return Some(value);
            }

            let next_batch = self.order_iter.as_mut()?.next()?;
            self.set_next_batch(next_batch);
        }
    }

    pub fn prepare_for_streaming(&mut self) -> Option<()> {
        let map = self.order_map.take()?;
        let mut order_iter: Box<dyn Iterator<Item = Vec<QueryValue>>> =
            if matches!(self.order, Order::Asc) {
                Box::new(map.into_values())
            } else {
                Box::new(map.into_values().rev())
            };

        let next_batch = order_iter.next()?;
        self.order_iter = Some(order_iter);
        self.set_next_batch(next_batch);

        Some(())
    }

    fn set_next_batch(&mut self, values: Vec<QueryValue>) {
        let batch: Box<dyn Iterator<Item = QueryValue>> = if matches!(self.order, Order::Asc) {
            Box::new(values.into_iter())
        } else {
            Box::new(values.into_iter().rev())
        };

        self.batch_iter = Some(batch);
    }
}
