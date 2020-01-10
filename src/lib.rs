#![allow(irrefutable_let_patterns)]
#![allow(dead_code, unused_imports)]
#![allow(non_camel_case_types)]
#![deny(bare_trait_objects)]
#![warn(clippy::all)]
#![allow(clippy::write_with_newline)]

// AsRedis: anything that can initiate a valid redis session.
// Session: a command builder buffer that is bound to a single connection.
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

pub trait AsRedis<'a> {
    type T: Read + Write;
    type P: std::ops::DerefMut<Target=Self::T>;
    /// `as_redis` may panic if it is already in use, or block if it needs to wait before making a new connection.
    fn as_redis(&'a self) -> Self::P;
    // convenient method
    fn arg(&'a self, x: &[u8]) -> Session<Self::P> {
        Session::new(self.as_redis()).apply_owned(|s| s.arg(x).ignore())
    }
}

impl<'a, T: Read + Write + 'a> AsRedis<'a> for RefCell<T> {
    type T = T;
    type P = RefMut<'a, T>;
    fn as_redis(&'a self) -> Self::P {
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

impl<'a, Addr: std::net::ToSocketAddrs> AsRedis<'a> for TcpClient<Addr> {
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

impl<'a, Addr: AsRef<std::path::Path>> AsRedis<'a> for UnixClient<Addr> {
    type T = UnixStream;
    type P = Box<UnixStream>;
    fn as_redis(&self) -> Self::P {
        Box::new(UnixStream::connect(&self.addr).unwrap())
    }
}

pub struct Pool<'a, A: AsRedis<'a>> {
    send: Sender<A::P>,
    recv: Arc<Mutex<Receiver<A::P>>>
}

impl<'a, A: AsRedis<'a>> Pool<'a, A> {
    // default size 10
    pub fn new(client: &'a A) -> Self {
        Self::with_capacity(client, 10)
    }

    pub fn with_capacity(client: &'a A, num: usize) -> Self {
        let (send, recv) = channel();
        for _ in 0..num {
            send.send(client.as_redis()).unwrap();
        }
        Self { send, recv: Arc::new(Mutex::new(recv)) }
    }
}

impl<'inner, 'outer, A: AsRedis<'inner>> AsRedis<'outer> for Pool<'inner, A> {
    type T = A::T;
    type P = A::P;
    fn as_redis(&'outer self) -> Self::P {
        self.recv.lock().unwrap().recv().unwrap()
    }
}

pub struct Session<P> {
    count: usize,
    buf: Vec<u8>,
    conn: P
}

impl<T: Read + Write, P: std::ops::DerefMut<Target=T>> Session<P> {
    pub fn new(conn: P) -> Self {
        Self { count: 0, buf: vec![], conn }
    }

    pub fn arg(&mut self, x: &[u8]) -> &mut Self {
        self.count += 1;
        write!(self.buf, "${}\r\n", x.len()).expect("bug");
        self.buf.extend_from_slice(x);
        self.buf.extend_from_slice(b"\r\n");
        self // for chaining
    }

    /// execute the command and get response. one should drop the connection if this returns error
    pub fn fetch(&mut self) -> Result<Response, String> {
        self.send()?;
        self.recv()
    }

    /// execute command, panic if error, discard the result otherwise.
    pub fn run(&mut self) -> &mut Self {
        self.fetch().expect("redis failed");
        self // for chaining
    }

    /// low level instruction that only send the command without reading response. Note it also clears the buffer.
    pub fn send(&mut self) -> Result<(), String> {
        write!(self.conn, "*{}\r\n", self.count).msg("failed writing to socket")?;
        self.conn.write_all(&self.buf).msg("failed writing to socket")?;
        Ok(self.clear())
    }

    /// low level instruction that only read a response without sending request.
    pub fn recv(&mut self) -> Result<Response, String> {
        let mut reader = BufReader::with_capacity(64, &mut *self.conn);
        let res = parse_resp(&mut reader);
        if !reader.buffer().is_empty() {
            return Err("buffer not empty after read.".to_string())
        }
        res
    }

    fn clear(&mut self) {
        self.count = 0;
        self.buf.clear();
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
    pub fn integer(self) -> i64 {
        if let Response::Integer(x) = self {
            x
        } else {
            panic!("not an integer")
        }
    }

    pub fn text(self) -> String {
        if let Response::Text(x) = self {
            x
        } else {
            panic!("not a text")
        }
    }

    pub fn bytes(self) -> Box<[u8]> {
        if let Response::Bytes(x) = self {
            x
        } else {
            panic!("not bytes")
        }
    }

    pub fn list(self) -> Vec<Response> {
        if let Response::List(x) = self {
            x
        } else {
            panic!("not bytes")
        }
    }

    #[allow(clippy::unused_unit)]
    pub fn nothing(self) -> () {
        if let Response::Nothing = self {
            ()
        } else {
            panic!("not nothing")
        }
    }
}
