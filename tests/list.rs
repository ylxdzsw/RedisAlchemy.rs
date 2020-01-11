use redis_alchemy::*;

#[test]
fn blob() {
    let client = TcpClient::new("127.0.0.1:6379");
    let list: List<_, _, i32> = List::new(client, &b"list"[..]);
    list.push(&-24).unwrap();
}
