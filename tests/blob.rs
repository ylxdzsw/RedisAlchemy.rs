use redis_alchemy::*;

#[test]
fn blob() {
    let client = TcpClient::new("127.0.0.1:6379");
    let mut blob = Blob::new(client, &b"fuck"[..]);
    blob.set(b"yes");
    assert_eq!(&blob.get()[..], &b"yes"[..])
}
