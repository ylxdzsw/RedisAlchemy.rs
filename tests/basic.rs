use redis_alchemy::*;
use std::net::TcpStream;
use std::cell::RefCell;
use oh_my_rust::MonadExt;

#[test]
fn basic() {
    let res = TcpStream::connect("127.0.0.1:6379").unwrap()
        .arg(b"set")
        .arg(b"test")
        .arg(b"1")
        .fetch();
    assert_eq!(res.unwrap().text(), "OK")
}

#[test] #[ignore]
fn unix_socket() {
    let sock = std::os::unix::net::UnixStream::connect("/run/sayuri/redis/redis.sock").unwrap();
    let conn = RefCell::new(&sock);
    let res = conn
        .arg(b"set")
        .arg(b"test")
        .arg(b"1")
        .fetch();
    assert_eq!(res.unwrap().text(), "OK")
}

#[test]
fn reuse_session() {
    let conn = TcpClient::new("127.0.0.1:6379");
    let mut sess = Session::new(conn.as_redis());
    sess.arg(b"del").arg(b"test_reuse_session").run().unwrap();

    for i in 0..5 {
        sess.arg(b"rpush")
            .arg(b"test_reuse_session")
            .arg(i.to_string().as_bytes())
            .fetch().unwrap().ignore();
    }

    let list: Vec<_> = sess
        .arg(b"lrange")
        .arg(b"test_reuse_session")
        .arg(b"0")
        .arg(b"-1")
        .fetch()
        .unwrap()
        .list()
        .into_iter()
        .map(|x| x.bytes()[0])
        .collect();

    assert_eq!(list, b"01234")
}

#[test]
fn pool() {
    let client = TcpClient::new("127.0.0.1:6379");
    let pool: Pool<_> = (0..10).map(|_| client.as_redis()).collect();
    {
        let pool = pool.clone();
        std::thread::spawn(move || pool.arg(b"rpush").arg(b"pool").arg(b"1").fetch());
    }
    let res = pool.arg(b"blpop").arg(b"pool").arg(b"5").fetch().unwrap().list();
    assert_eq!(&res[0].clone().bytes()[..], &b"pool"[..]);
    assert_eq!(&res[1].clone().bytes()[..], &b"1"[..])
}

#[test] #[should_panic]
fn multiple_sessions_on_one_connection() {
    let conn = RefCell::new(TcpStream::connect("127.0.0.1:6379").unwrap());
    let _sess1 = conn.as_redis();
    let _sess2 = conn.as_redis();
}
