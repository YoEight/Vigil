use eventql_parser::{parse_query, prelude::AnalysisOptions};
use uuid::uuid;

use crate::db::{Db, EvalResult, Event, QueryValue};

#[test]
fn test_append() {
    let mut db = Db::default();
    insta::assert_yaml_snapshot!(db.append(
        "foo/bar",
        vec![Event {
            event_type: "user-created".to_string(),
            ..Default::default()
        }]
    ));
}

#[test]
fn test_illegal_subject() {
    let mut db = Db::default();
    insta::assert_yaml_snapshot!(db.append(
        "/path/to/file",
        vec![Event {
            event_type: "user-created".to_string(),
            ..Default::default()
        }]
    ));
}

#[test]
fn test_run_query_from_events() {
    let mut db = Db::default();
    let options = AnalysisOptions::default();

    db.append(
        "companies/krispy",
        vec![Event {
            event_type: "user-created".to_string(),
            id: uuid!("1e7b9531-1392-48fe-aaf1-94d4cae74a9d"),
            ..Default::default()
        }],
    )
    .unwrap();

    db.append(
        "companies/krispy",
        vec![Event {
            event_type: "user-deleted".to_string(),
            id: uuid!("77344193-67bb-44af-a854-77d5d56dbb3d"),
            ..Default::default()
        }],
    )
    .unwrap();

    let query = parse_query(include_str!("./resources/query_from_events.eql"))
        .unwrap()
        .run_static_analysis(&options)
        .unwrap();

    insta::assert_yaml_snapshot!(db.run_query(&options, &query).collect::<Vec<_>>());
}

#[test]
fn test_run_query_department_grouping() {
    let mut db = Db::default();
    let options = AnalysisOptions::default();

    db.append(
        "companies/krispy",
        vec![
            Event {
                event_type: "user-created".to_string(),
                datacontenttype: "application/json".to_string(),
                data: serde_json::to_vec(&serde_json::json!({
                    "id": 1,
                    "department": "engineering",
                    "salary": 95_000,
                }))
                .unwrap(),
                ..Default::default()
            },
            Event {
                event_type: "user-created".to_string(),
                datacontenttype: "application/json".to_string(),
                data: serde_json::to_vec(&serde_json::json!({
                    "id": 2,
                    "department": "engineering",
                    "salary": 110_000,
                }))
                .unwrap(),
                ..Default::default()
            },
            Event {
                event_type: "user-created".to_string(),
                datacontenttype: "application/json".to_string(),
                data: serde_json::to_vec(&serde_json::json!({
                    "id": 3,
                    "department": "engineering",
                    "salary": 88_000,
                }))
                .unwrap(),
                ..Default::default()
            },
            Event {
                event_type: "user-created".to_string(),
                datacontenttype: "application/json".to_string(),
                data: serde_json::to_vec(&serde_json::json!({
                    "id": 4,
                    "department": "sales",
                    "salary": 75_000,
                }))
                .unwrap(),
                ..Default::default()
            },
            Event {
                event_type: "user-created".to_string(),
                datacontenttype: "application/json".to_string(),
                data: serde_json::to_vec(&serde_json::json!({
                    "id": 5,
                    "department": "sales",
                    "salary": 82_000,
                }))
                .unwrap(),
                ..Default::default()
            },
            Event {
                event_type: "user-created".to_string(),
                datacontenttype: "application/json".to_string(),
                data: serde_json::to_vec(&serde_json::json!({
                    "id": 6,
                    "department": "marketing",
                    "salary": 70_000,
                }))
                .unwrap(),
                ..Default::default()
            },
        ],
    )
    .unwrap();

    let query = parse_query(include_str!("./resources/department-grouping.eql"))
        .unwrap()
        .run_static_analysis(&options)
        .unwrap();

    let mut result = db
        .run_query(&options, &query)
        .collect::<EvalResult<Vec<_>>>()
        .unwrap();

    result.sort_by_key(|v| {
        if let QueryValue::Record(props) = v {
            props
                .get("department")
                .unwrap()
                .as_str_or_panic()
                .to_string()
        } else {
            "const".to_string()
        }
    });

    insta::assert_yaml_snapshot!(result);
}
