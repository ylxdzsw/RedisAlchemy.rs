use redis_alchemy::*;

// #[test]
// fn test_1() {
//     let res = AsRedis::<std::net::TcpStream, _>::as_redis("127.0.0.1:6379")
//         .arg(b"set")
//         .arg(b"fuck")
//         .arg(b"1")
//         .fetch();
//     assert_eq!(res.unwrap().into_text(), "OK")
// }

// #[test]
// fn test_2() {
//     let mut conn = AsRedis::<std::net::TcpStream, _>::as_redis("127.0.0.1:6379");
//     for i in 0..5 {
//         conn.arg(b"rpush")
//             .arg(b"fucks")
//             .arg(i.to_string().as_bytes())
//             .fetch().unwrap();
//     }

//     let list: Vec<_> = conn
//         .arg(b"lrange")
//         .arg(b"fucks")
//         .arg(b"0")
//         .arg(b"-1")
//         .fetch()
//         .unwrap()
//         .into_list()
//         .into_iter()
//         .map(|x| x.into_bytes()[0])
//         .collect();

//     assert_eq!(list, b"01234")
// }

#[test]
fn test_3() {
    let client = std::cell::RefCell::new(std::os::unix::net::UnixStream::connect("/run/sayuri/redis/redis.sock").unwrap());
    let mut blob = Blob::new(client, &b"fuck"[..]);
    blob.set(b"yes");
}