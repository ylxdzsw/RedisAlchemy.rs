use redis_alchemy::*;

#[test]
fn blob() {
    let client = TcpClient::new("127.0.0.1:6379");
    let blob: List<_, _, i32> = List::new(client, &b"list"[..]);
    blob.push(&-24).unwrap();
}
