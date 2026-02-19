use eventql_parser::{Query, Session, Type, prelude::Typed};

use crate::queries::{QueryProcessor, Sources, aggregates::AggQuery, events::EventQuery};

pub trait DataProvider {
    fn instantiate_named_data_source<'a>(
        &'a self,
        name: &'a str,
        inferred_type: Type,
    ) -> Option<QueryProcessor<'a>>;

    fn instantiate_subject_data_source<'a>(
        &'a self,
        subject: &'a str,
        inferred_type: Type,
    ) -> Option<QueryProcessor<'a>>;
}

pub fn query_plan<'a, P>(
    session: &'a Session,
    provider: &'a P,
    query: Query<Typed>,
) -> QueryProcessor<'a>
where
    P: DataProvider,
{
    let mut srcs = Sources::default();
    for query_src in &query.sources {
        match &query_src.kind {
            eventql_parser::SourceKind::Name(name) => {
                let proc = if let Some(tpe) = query.meta.scope.get(query_src.binding.name) {
                    let name = session.arena().get_str(*name);

                    provider
                        .instantiate_named_data_source(name, tpe)
                        .unwrap_or(QueryProcessor::empty())
                } else {
                    QueryProcessor::empty()
                };

                srcs.insert(query_src.binding.name, proc);
            }

            eventql_parser::SourceKind::Subject(sub) => {
                let proc = if let Some(tpe) = query.meta.scope.get(query_src.binding.name) {
                    let sub = session.arena().get_str(*sub);
                    provider
                        .instantiate_subject_data_source(sub, tpe)
                        .unwrap_or(QueryProcessor::empty())
                } else {
                    QueryProcessor::empty()
                };

                srcs.insert(query_src.binding.name, proc);
            }

            eventql_parser::SourceKind::Subquery(sub_query) => {
                let name = query_src.binding.name;
                // TODO - get rid of that unnecessary clone
                let proc = query_plan(session, provider, sub_query.as_ref().clone());

                srcs.insert(name, proc);
            }
        }
    }

    if query.meta.aggregate {
        match AggQuery::new(srcs, session, query) {
            Ok(agg_query) => QueryProcessor::Aggregate(agg_query),
            Err(e) => QueryProcessor::Errored(Some(e)),
        }
    } else {
        QueryProcessor::Regular(EventQuery::new(srcs, session, query))
    }
}
