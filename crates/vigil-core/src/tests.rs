use serde::Deserialize;
use uuid::uuid;

use crate::{databases::in_mem::InMemDb, eval::EvalResult, types::Event, values::QueryValue};

fn load_departments_dataset(db: &mut InMemDb) {
    #[derive(Deserialize)]
    struct Propose {
        subject: String,
        #[serde(rename = "type")]
        event_type: String,
        payload: serde_json::Value,
    }

    let proposes: Vec<Propose> =
        serde_json::from_str(include_str!("./resources/input/departments.json")).unwrap();

    for propose in proposes {
        db.append(
            &propose.subject,
            vec![Event {
                event_type: propose.event_type,
                datacontenttype: "application/json".to_string(),
                data: serde_json::to_vec(&propose.payload).unwrap(),
                ..Default::default()
            }],
        )
        .unwrap();
    }
}

#[test]
fn test_append() {
    let mut db = InMemDb::default();
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
    let mut db = InMemDb::default();
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
    let mut db = InMemDb::default();

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

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/query_from_events.eql"))
            .unwrap()
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_run_query_department_grouping() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    let mut result = db
        .run_query(include_str!("./resources/department-grouping.eql"))
        .unwrap()
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

#[test]
fn test_run_query_department_grouping_ordered() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/department-grouping-ordered.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}
#[test]
fn test_run_query_department_grouping_having() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/department-grouping-having.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}

#[test]
fn test_query_order_by() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/query_order_by.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}

#[test]
fn test_query_order_by_desc() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/query_order_by_desc.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}

#[test]
fn test_query_event_types() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/query_event_types.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}

#[test]
fn test_query_subjects() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/query_subjects.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}

#[test]
fn test_query_agg_distinct() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/query_agg_distinct.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}

#[test]
fn test_query_distinct() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/query_distinct.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}

#[test]
fn test_query_agg_functions() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    let mut result = db
        .run_query(include_str!("./resources/query_agg_functions.eql"))
        .unwrap()
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

#[test]
fn test_query_top() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/query_top.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}

#[test]
fn test_query_skip() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/query_skip.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}

#[test]
fn test_query_agg_top() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/query_agg_top.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}

#[test]
fn test_query_agg_skip() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/query_agg_skip.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}

#[test]
fn test_simple_sub_query() {
    let mut db = InMemDb::default();

    load_departments_dataset(&mut db);

    insta::assert_yaml_snapshot!(
        db.run_query(include_str!("./resources/simple-sub-query.eql"))
            .unwrap()
            .collect::<EvalResult<Vec<_>>>()
    );
}
