use eventql_parser::{Limit, Order, Query, Session, prelude::Typed};

use crate::queries::orderer::QueryOrderer;
use crate::{
    eval::{EvalResult, Interpreter},
    queries::Sources,
    values::QueryValue,
};

pub struct EventQuery<'a> {
    srcs: Sources<'a>,
    query: Query<Typed>,
    interpreter: Interpreter<'a>,
    orderer: QueryOrderer,
    completed: bool,
    skipped: u64,
    emitted: u64,
}

impl<'a> EventQuery<'a> {
    pub fn new(srcs: Sources<'a>, session: &'a Session, query: Query<Typed>) -> Self {
        let order = query.order_by.map_or_else(|| Order::Asc, |o| o.order);
        Self {
            srcs,
            query,
            orderer: QueryOrderer::new(order),
            interpreter: Interpreter::new(session),
            completed: false,
            skipped: 0,
            emitted: 0,
        }
    }
}

impl<'a> Iterator for EventQuery<'a> {
    type Item = EvalResult<QueryValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.completed {
                if let Some(Limit::Top(n)) = self.query.limit
                    && self.emitted >= n
                {
                    return None;
                }

                let value = self.orderer.next()?;

                if let Some(Limit::Skip(n)) = self.query.limit
                    && self.skipped < n
                {
                    self.skipped += 1;
                    continue;
                }

                self.emitted += 1;
                return Some(Ok(value));
            }

            if let Some(outcome) = self.srcs.fill(self.interpreter.env_mut()) {
                if let Err(e) = outcome {
                    return Some(Err(e));
                }
            } else {
                self.completed = true;
                self.orderer.prepare_for_streaming()?;

                continue;
            }

            match self.interpreter.eval_predicate(&self.query) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => return Some(Err(e)),
            }

            if let Some(order_by) = &self.query.order_by {
                let key = match self.interpreter.eval_expr(order_by.expr) {
                    Err(e) => return Some(Err(e)),
                    Ok(key) => key,
                };

                let value = match self.interpreter.eval_expr(self.query.projection) {
                    Err(e) => return Some(Err(e)),
                    Ok(v) => v,
                };

                self.orderer.insert(key, value);
                continue;
            }

            if let Some(Limit::Top(n)) = self.query.limit
                && self.emitted >= n
            {
                return None;
            }

            let value = match self.interpreter.eval_expr(self.query.projection) {
                Err(e) => return Some(Err(e)),
                Ok(v) => v,
            };

            if let Some(Limit::Skip(n)) = self.query.limit
                && self.skipped < n
            {
                self.skipped += 1;
                continue;
            }

            self.emitted += 1;
            return Some(Ok(value));
        }
    }
}
