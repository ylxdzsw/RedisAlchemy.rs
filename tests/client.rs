use redis_alchemy::client;

#[test]
fn test_1() {
    let conn = std::os::unix::net::UnixStream::connect("/run/sayuri/redis/redis.sock").expect("cannot connect to redis");
    let mut client = client::Client::new("/run/sayuri/redis/redis.sock");
    client.arg(b"set");
    client.arg(b"fuck");
    client.arg(b"1");
    assert_eq!(client.fetch().unwrap().into_text(), "OK")
}

#[test]
fn test_2() {
    let conn = std::os::unix::net::UnixStream::connect("/run/sayuri/redis/redis.sock").expect("cannot connect to redis");
    let mut client = client::Client::new("/run/sayuri/redis/redis.sock");
    for i in 0..5 {
        client.arg(b"rpush");
        client.arg(b"fucks");
        client.arg(i.to_string().as_bytes());
        client.fetch().unwrap();
    }

    client.arg(b"lrange");
    client.arg(b"fucks");
    client.arg(b"0");
    client.arg(b"-1");

    let list = client.fetch().unwrap().into_list();
    let list: Vec<_> = list.into_iter().map(|x| x.into_bytes()[0]).collect();
    assert_eq!(list, b"01234")
}