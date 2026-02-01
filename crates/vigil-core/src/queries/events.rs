use eventql_parser::{
    Query,
    prelude::{AnalysisOptions, Typed},
};

use crate::{
    eval::{EvalResult, Interpreter},
    queries::Sources,
    values::QueryValue,
};

pub struct EventQuery<'a> {
    srcs: Sources<'a>,
    query: &'a Query<Typed>,
    interpreter: Interpreter<'a>,
}

impl<'a> EventQuery<'a> {
    pub fn new(srcs: Sources<'a>, options: &'a AnalysisOptions, query: &'a Query<Typed>) -> Self {
        Self {
            srcs,
            query,
            interpreter: Interpreter::new(options),
        }
    }
}

impl<'a> Iterator for EventQuery<'a> {
    type Item = EvalResult<QueryValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let outcome = self.srcs.fill(self.interpreter.env_mut())?;
            if let Err(e) = outcome {
                return Some(Err(e));
            }

            if let Some(predicate) = &self.query.predicate {
                match self.interpreter.eval_predicate(&predicate.value) {
                    Ok(false) => continue,
                    Ok(true) => {}
                    Err(e) => return Some(Err(e)),
                }
            }

            return Some(self.interpreter.eval(&self.query.projection.value));
        }
    }
}
