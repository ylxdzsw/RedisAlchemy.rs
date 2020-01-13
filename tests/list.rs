use redis_alchemy::*;

#[test]
fn list_iter() {
    let client = TcpClient::new("127.0.0.1:6379");
    let list: List<_, _, i32> = List::new(client, &b"list_iter"[..]);
    list.push(&-24).unwrap();
    list.push(&257).unwrap();
    let x: Vec<_> = list.iter().collect();
    assert_eq!(&x, &[-24, 257])
}
