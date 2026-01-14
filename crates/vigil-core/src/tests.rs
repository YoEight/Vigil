use crate::db::{Db, Event};

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

    db.append(
        "companies/krispy",
        vec![Event {
            event_type: "user-created".to_string(),
            ..Default::default()
        }],
    )
    .unwrap();

    db.append(
        "companies/krispy",
        vec![Event {
            event_type: "user-deleted".to_string(),
            ..Default::default()
        }],
    )
    .unwrap();

    insta::assert_yaml_snapshot!(db.query(include_str!("./resources/query_from_events.eql")));
}
