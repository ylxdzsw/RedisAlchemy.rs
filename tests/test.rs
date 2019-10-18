use redis_alchemy::*;

#[test]
fn test_1() {
    let conn = std::net::TcpStream::connect("127.0.0.1:6379").expect("cannot connect to redis");
    let res = conn.as_redis()
        .arg(b"set")
        .arg(b"fuck")
        .arg(b"1")
        .fetch();
    assert_eq!(res.unwrap().into_text(), "OK")
}

#[test]
fn test_2() {
    let conn = std::net::TcpStream::connect("127.0.0.1:6379").expect("cannot connect to redis");
    for i in 0..5 {
        conn.as_redis()
            .arg(b"rpush")
            .arg(b"fucks")
            .arg(i.to_string().as_bytes())
            .fetch().unwrap();
    }

    let list: Vec<_> = conn.as_redis()
        .arg(b"lrange")
        .arg(b"fucks")
        .arg(b"0")
        .arg(b"-1")
        .fetch()
        .unwrap()
        .into_list()
        .into_iter()
        .map(|x| x.into_bytes()[0])
        .collect();

    assert_eq!(list, b"01234")
}