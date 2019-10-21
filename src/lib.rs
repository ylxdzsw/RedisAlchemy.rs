#![allow(irrefutable_let_patterns)]
#![allow(dead_code, unused_imports)]
#![allow(non_camel_case_types)]
#![deny(bare_trait_objects)]
#![warn(clippy::all)]
#![allow(clippy::write_with_newline)]

// AsRedis: primary API. Anything that can initiate a valid redis session. Typically RefCell<&TcpStream>. An AsRedis may fail if it is already in use, or pending if it needs to wait before makeing a new connection.
// Session: a command builder buffer bound on an AsRedis.
// Collection: a collection represents the data behind a redis key.
// Response: an enum of possible non-error return types from Redis.

mod blob;
pub use blob::*;

use std::os::unix::net::UnixStream;
use std::net::TcpStream;
use std::io::prelude::*;
use std::sync::*;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::ops::DerefMut;
use std::cell::{RefCell, RefMut};
use detached_bufreader::BufReader;
use oh_my_rust::*;

// TODO: return a result instead?
pub trait AsRedis<'a, 'b> {
    type T: Read + Write;
    type P: std::ops::DerefMut<Target=Self::T> + 'a;
    fn as_redis(&'b self) -> Self::P;
}

impl<'a, 'b: 'a, T: Read + Write + 'a> AsRedis<'a, 'b> for RefCell<T> {
    type T = T;
    type P = RefMut<'a, T>;
    fn as_redis(&'b self) -> Self::P {
        self.borrow_mut()
    }
}

pub struct TcpClient<Addr: std::net::ToSocketAddrs> {
    addr: Addr
}

impl<Addr: std::net::ToSocketAddrs> TcpClient<Addr> {
    pub fn new(addr: Addr) -> TcpClient<Addr> {
        Self { addr }
    }
}

impl<'b, Addr: std::net::ToSocketAddrs> AsRedis<'static, 'b> for TcpClient<Addr> {
    type T = TcpStream;
    type P = Box<TcpStream>;
    fn as_redis(&self) -> Self::P {
        Box::new(TcpStream::connect(&self.addr).unwrap())
    }
}

pub struct UnixClient<Addr: AsRef<std::path::Path>> {
    addr: Addr
}

impl<Addr: AsRef<std::path::Path>> UnixClient<Addr> {
    pub fn new(addr: Addr) -> UnixClient<Addr> {
        Self { addr }
    }
}

impl<'b, Addr: AsRef<std::path::Path>> AsRedis<'static, 'b> for UnixClient<Addr> {
    type T = UnixStream;
    type P = Box<UnixStream>;
    fn as_redis(&self) -> Self::P {
        Box::new(UnixStream::connect(&self.addr).unwrap())
    }
}

pub struct Pool<'b, A: AsRedis<'static, 'b>> {
    send: Sender<A::P>,
    recv: Arc<Mutex<Receiver<A::P>>>
}

impl<'b, A: AsRedis<'static, 'b>> Pool<'b, A> {
    // default size 10
    pub fn new(client: &'b A) -> Self {
        Self::with_capacity(client, 10)
    }

    pub fn with_capacity(client: &'b A, num: usize) -> Self {
        let (send, recv) = channel();
        for _ in 0..num {
            send.send(client.as_redis()).unwrap();
        }
        Self { send, recv: Arc::new(Mutex::new(recv)) }
    }
}

impl<'inner, 'outer, A: AsRedis<'static, 'inner>> AsRedis<'static, 'outer> for Pool<'inner, A> {
    type T = A::T;
    type P = A::P;
    fn as_redis(&'outer self) -> Self::P {
        self.recv.lock().unwrap().recv().unwrap()
    }
} 


// impl<T: std::net::ToSocketAddrs + ?Sized> AsRedis<'static, TcpStream, Box<TcpStream>> for T {
//     fn as_redis(&self) -> Session<TcpStream, Box<TcpStream>> {
//         let conn = TcpStream::connect(self).expect("cannot connect to redis");
//         Session::new(Box::new(conn))
//     }
// }

pub struct Session<'a, 'b, A: AsRedis<'a, 'b>> {
    count: usize,
    buf: Vec<u8>,
    conn: &'b A,
    phantom: std::marker::PhantomData<&'a ()>
}

impl<'a,'b, A: AsRedis<'a, 'b>> Session<'a, 'b, A> {
    pub fn new(conn: &'b A) -> Self {
        Session { count: 0, buf: vec![], conn, phantom: std::marker::PhantomData }
    }

    pub fn arg(mut self, x: &[u8]) -> Self {
        self.count += 1;
        write!(self.buf, "${}\r\n", x.len()).expect("bug");
        self.buf.extend_from_slice(x);
        self.buf.extend_from_slice(b"\r\n");
        self // for chaining
    }

    // one should drop the connection if this errored
    pub fn fetch(&mut self) -> Result<Response, String> {
        let mut conn = self.conn.as_redis();

        // send
        write!(conn, "*{}\r\n", self.count).map_err(|_| "failed writing to socket")?;
        conn.write_all(&self.buf).map_err(|_| "failed writing to socket")?;

        // recv
        let mut reader = BufReader::with_capacity(64, &mut *conn);
        let res = parse_resp(&mut reader);
        if !reader.buffer().is_empty() {
            return Err("buffer not empty after read.".to_string())
        }

        // reset
        self.count = 0;
        self.buf.clear();

        res
    }
    
    pub fn run(&mut self) -> &mut Self {
        self.fetch().expect("redis failed");
        self // for chaining
    }
}

fn parse_resp(r: &mut impl BufRead) -> Result<Response, String> {
    let mut header = String::new();
    r.read_line(&mut header).unwrap();

    let magic = header.as_bytes()[0];
    let header = header[1..].trim_end();

    match magic {
        b'+' => Ok(Response::Text(header.to_string())),
        b'-' => Err(format!("redis error: {}", header)),
        b':' => Ok(Response::Integer(header.parse().msg("protocol error")?)),
        b'$' => if header.as_bytes()[0] == b'-' {
            Ok(Response::Nothing)
        } else {
            let len: u64 = header.parse().msg("protocol error")?;
            let mut buf = r.read_exact_alloc((len + 2) as usize).msg("protocol error")?;
            buf.truncate(len as usize); // remove the new line terminator
            Ok(Response::Bytes(buf.into_boxed_slice()))
        },
        b'*' => if header.as_bytes()[0] == b'-' {
            Ok(Response::Nothing)
        } else {
            let len: u32 = header.parse().msg("protocol error")?;
            Ok(Response::List((0..len).map(|_| {
                parse_resp(r)
            }).collect::<Result<Vec<_>, _>>()?))
        },
        _ => Err("protocol error".to_string())
    }
}

pub enum Response {
    Integer(i64), Text(String), Bytes(Box<[u8]>), List(Vec<Response>), Nothing
}

impl Response {
    pub fn unwrap_integer(self) -> i64 {
        if let Response::Integer(x) = self {
            x
        } else {
            panic!("not an integer")
        }
    }

    pub fn unwrap_text(self) -> String {
        if let Response::Text(x) = self {
            x
        } else {
            panic!("not a text")
        }
    }

    pub fn unwrap_bytes(self) -> Box<[u8]> {
        if let Response::Bytes(x) = self {
            x
        } else {
            panic!("not bytes")
        }
    }

    pub fn unwrap_list(self) -> Vec<Response> {
        if let Response::List(x) = self {
            x
        } else {
            panic!("not bytes")
        }
    }

    #[allow(clippy::unused_unit)]
    pub fn unwrap_nothing(self) -> () {
        if let Response::Nothing = self {
            ()
        } else {
            panic!("not nothing")
        }
    }
}