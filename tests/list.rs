use redis_alchemy::*;

#[test]
fn list() {
    let client = TcpClient::new("127.0.0.1:6379");
    let list: List<_, _, i32> = List::new(client, &b"list"[..]);
    list.clear().unwrap();
    let x = [-1, 0, 257];
    for i in &x {
        list.push(i).unwrap()
    }
    assert_eq!(list.get(0).unwrap().unwrap(), -1);
    assert_eq!(list.get(0).unwrap().unwrap(), -1);
    assert!(list.get(5).unwrap().is_none());
    assert_eq!(&list.range(0..1).unwrap()[..], &x[0..1]);
    assert_eq!(&list.range(0..-1).unwrap()[..], &x[0..2]);
    assert_eq!(&list.range(0..=-1).unwrap()[..], &x[0..]);
    assert_eq!(&list.range(0..10).unwrap()[..], &x[..]);
    assert_eq!(&list.range(-2..-1).unwrap()[..], &x[1..2]);
    assert_eq!(&list.range(1..).unwrap()[..], &x[1..]);
    assert_eq!(&list.range(..).unwrap()[..], &x[..]);
}

#[test]
fn list_iter() {
    let client = TcpClient::new("127.0.0.1:6379");
    let list: List<_, _, i32> = List::new(client, &b"list_iter"[..]);
    list.clear().unwrap();
    list.push(&-24).unwrap();
    list.push(&257).unwrap();
    let x: Vec<_> = list.iter().collect();
    assert_eq!(&x, &[-24, 257])
}
