use redis_alchemy::*;

#[test]
fn cell() {
    let client = TcpClient::new("127.0.0.1:6379");
    let cell = Cell::new(&client, &b"fuck"[..], |x: &String| x.as_bytes().into(), |x| String::from_utf8(x.to_vec()).unwrap());
    cell.set("yes".to_string()).unwrap();
    assert_eq!(&cell.get().unwrap()[..], "yes")
}
