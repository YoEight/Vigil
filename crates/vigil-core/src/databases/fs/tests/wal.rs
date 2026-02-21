use bytes::{Bytes, BytesMut};

use crate::databases::fs::{
    blocks::BlocksMut,
    wal::{LogContentType, LogOp, LogRecord, LogSegFooter, LogSegHeader},
};

#[test]
fn seg_header_round_trip() {
    let mut buf = BytesMut::new();

    let expected = LogSegHeader {
        version: 42,
        segment_id: 9,
    };

    expected.serialize_into(&mut buf);
    insta::assert_yaml_snapshot!(buf);

    let actual = LogSegHeader::try_deserialize_from(buf.freeze()).unwrap();

    assert_eq!(expected, actual);
}

#[test]
fn rec_round_trip() {
    let data = Bytes::from_static(&b"Hello, World!"[..]);
    let mut blocks = BlocksMut::new(128, 0, BytesMut::new());

    let expected = LogRecord {
        lsn: 42,
        op: LogOp::Put,
        content_type: LogContentType::Unknown(123),
        data,
    };

    expected.serialize_into(&mut blocks).unwrap();
    insta::assert_yaml_snapshot!(blocks.bytes_mut());

    let mut blocks = blocks.freeze();
    let block = blocks.next_block().unwrap().unwrap();
    let actual = LogRecord::try_deserialize_from(block).unwrap();

    assert_eq!(expected, actual);
}

#[test]
fn footer_round_trip() {
    let mut buf = BytesMut::new();
    let expected = LogSegFooter {
        sealed: true,
        first_lsn: 3,
        last_lsn: 53,
        checksum: 123,
    };

    expected.serialize_into(&mut buf);
    insta::assert_yaml_snapshot!(buf);

    let actual = LogSegFooter::try_deserialize_from(buf.freeze()).unwrap();

    assert_eq!(expected, actual);
}

#[test]
fn footer_serialization_when_not_sealed() {
    let mut buf = BytesMut::new();
    let footer = LogSegFooter {
        sealed: false,
        first_lsn: 123,
        last_lsn: 456,
        checksum: 789,
    };

    footer.serialize_into(&mut buf);
    insta::assert_yaml_snapshot!("buffer_content", buf);

    let actual = LogSegFooter::try_deserialize_from(buf.freeze());

    insta::assert_yaml_snapshot!("parsed_footer", actual);
}

#[test]
fn detect_not_enough_space_zero_offset() {
    let mut blocks = BlocksMut::new(128, 0, BytesMut::new());

    insta::assert_yaml_snapshot!(blocks.open(129));
}

#[test]
fn detect_not_enough_space_with_offset() {
    let mut blocks = BlocksMut::new(128, 64, BytesMut::new());

    insta::assert_yaml_snapshot!(blocks.open(64));
}

#[test]
fn detect_written_too_much() {
    let mut blocks = BlocksMut::new(128, 0, BytesMut::new());

    let mut buf = blocks.open(5).unwrap();

    insta::assert_yaml_snapshot!(buf.put_u64_le(123));
}

#[test]
fn detect_written_too_little() {
    let mut blocks = BlocksMut::new(128, 0, BytesMut::new());

    let mut buf = blocks.open(8).unwrap();

    buf.put_u32_le(123).unwrap();

    insta::assert_yaml_snapshot!(buf.finalize());
}
