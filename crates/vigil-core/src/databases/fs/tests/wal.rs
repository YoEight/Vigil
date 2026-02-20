use bytes::{Bytes, BytesMut};

use crate::databases::fs::wal::{WalContentType, WalOp, WalRecord, WalSegHeader};

#[test]
fn seg_header_round_trip() {
    let mut buf = BytesMut::new();

    let expected = WalSegHeader {
        version: 42,
        segment_id: 9,
    };

    expected.serialize_into(&mut buf);
    insta::assert_yaml_snapshot!(buf);

    let actual = WalSegHeader::try_deserialize_from(buf.freeze()).unwrap();

    assert_eq!(expected, actual);
}

#[test]
fn rec_round_trip() {
    let data = Bytes::from_static(&b"Hello, World!"[..]);
    let mut buf = BytesMut::new();

    let expected = WalRecord {
        lsn: 42,
        op: WalOp::Put,
        content_type: WalContentType::Unknown(123),
        data,
    };

    expected.serialize_into(&mut buf);
    insta::assert_yaml_snapshot!(buf);

    let actual = WalRecord::try_deserialize_from(buf.freeze()).unwrap();

    assert_eq!(expected, actual);
}
