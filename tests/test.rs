use redis_alchemy::*;
use std::net::TcpStream;
use std::cell::RefCell;

#[test]
 fn test_1() {
     let conn = RefCell::new(TcpStream::connect("127.0.0.1:6379").unwrap());
     let res = Session::new(conn.as_redis())
         .arg(b"set")
         .arg(b"fuck")
         .arg(b"1")
         .fetch();
     assert_eq!(res.unwrap().text(), "OK")
 }

 #[test]
 fn test_2() {
     let conn = TcpClient::new("127.0.0.1:6379");
     let mut sess = Session::new(conn.as_redis());
     sess.arg(b"del").arg(b"fucks").run();

     for i in 0..5 {
         sess.arg(b"rpush")
             .arg(b"fucks")
             .arg(i.to_string().as_bytes())
             .fetch().unwrap();
     }

     let list: Vec<_> = sess
         .arg(b"lrange")
         .arg(b"fucks")
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

//#[test]
//fn test_3() {
//    let sock = std::os::unix::net::UnixStream::connect("/run/sayuri/redis/redis.sock").unwrap();
//    let client = std::cell::RefCell::new(&sock);
//    let mut blob = Blob::new(client, &b"fuck"[..]);
//    blob.set(b"yes");
//    assert_eq!(&blob.get()[..], &b"yes"[..])
//}

#[test]
fn test_4() {
    let client = TcpClient::new("127.0.0.1:6379");
    let mut blob = Blob::new(client, &b"fuck"[..]);
    blob.set(b"yes");
    assert_eq!(&blob.get()[..], &b"yes"[..])
}

#[test]
fn test_5() {
    let client = TcpClient::new("127.0.0.1:6379");
    let client = Pool::new(&client);
    let mut blob = Blob::new(client, &b"fuck"[..]);
    blob.set(b"yes");
}