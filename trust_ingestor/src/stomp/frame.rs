use bytes::{Bytes, BytesMut};

pub struct Frame {
    pub command: String,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

#[derive(Debug)]
pub struct FrameError;

pub fn parse_frame(buffer: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_complete_frame() {
        let mut buffer = BytesMut::from(&b"MESSAGE\nmessage-id:abc123\n\nhello\0"[..]);

        let frame = parse_frame(&mut buffer).unwrap().unwrap();

        assert_eq!(frame.command, "MESSAGE");
        assert_eq!(
            frame.headers,
            vec![("message-id".to_string(), "abc123".to_string())]
        );
        assert_eq!(frame.body, Bytes::from_static(b"hello"));
        assert!(buffer.is_empty());
    }

    #[test]
    fn incomplete_frame_does_not_consume_buffer() {
        let mut buffer = BytesMut::from(&b"MESSAGE\nmessage-id:abc123\n\nhel"[..]);

        assert!(parse_frame(&mut buffer).unwrap().is_none());
        assert_eq!(buffer.as_ref(), b"MESSAGE\nmessage-id:abc123\n\nhel");
    }

    #[test]
    fn parses_multiple_frames() {
        let mut buffer = BytesMut::from(&b"MESSAGE\n\nfirst\0MESSAGE\n\nsecond\0"[..]);

        let first = parse_frame(&mut buffer).unwrap().unwrap();
        assert_eq!(first.body, Bytes::from_static(b"first"));

        let second = parse_frame(&mut buffer).unwrap().unwrap();
        assert_eq!(second.body, Bytes::from_static(b"second"));

        assert!(buffer.is_empty());
    }
}
