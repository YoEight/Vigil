use eventql_parser::{Order, Query, Session, prelude::Typed};

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
        }
    }
}

impl<'a> Iterator for EventQuery<'a> {
    type Item = EvalResult<QueryValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.completed {
                let value = self.orderer.next()?;
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

            return Some(self.interpreter.eval_expr(self.query.projection));
        }
    }
}
